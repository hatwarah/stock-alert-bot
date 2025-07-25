import asyncio
import yfinance as yf
from datetime import datetime, time, timedelta
import pytz
import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import aiohttp
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
MONGO_URI = os.getenv("MONGODB_URI")

# MongoDB setup
client = AsyncIOMotorClient(MONGO_URI)
db = client["stock_zones"]
trade_collection = db["trades"]

IST = pytz.timezone("Asia/Kolkata")

def patch_symbol(symbol: str) -> str:
    """Appends '.NS' if no exchange suffix found (assumes NSE by default)."""
    if '.' not in symbol:
        return symbol + '.NS'
    return symbol

async def send_telegram_message(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    logger.info("Sending Telegram Message: %s", payload)
    
    async with aiohttp.ClientSession() as session:
        for attempt in range(3):
            async with session.post(url, data=payload) as resp:
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", 1))
                    logger.warning("Rate limit hit, retrying after %s seconds", retry_after)
                    await asyncio.sleep(retry_after)
                    continue
                if resp.status != 200:
                    raise Exception(f"Telegram API Error: {await resp.text()}")
                logger.info("Message sent successfully")
                return
        raise Exception("Max retries reached for Telegram API")

async def check_trades():
    # Check market hours
    now = datetime.now(IST)
    if now.weekday() >= 5 or now.time() < time(9, 15) or now.time() > time(15, 30):
        logger.info("Outside market hours (9:15 AM - 3:30 PM IST), exiting.")
        exit(0)

    logger.info("Checking Trades...")

    # Fetch all open trades
    trades = await trade_collection.find({"status": "OPEN"}).to_list(None)
    if not trades:
        logger.info("No open trades found.")
        return

    total_trades = len(trades)
    logger.info("Found %d open trades.", total_trades)

    # Get unique symbols
    tickers = list(set(patch_symbol(trade["symbol"]) for trade in trades))
    total_tickers = len(tickers)
    logger.info("Unique symbols to fetch: %s (%d total)", tickers, total_tickers)

    # Fetch data once per symbol
    price_data = {}
    logger.info("Fetching stock data...")
    try:
        data = yf.download(tickers, period="1d", group_by="ticker")
        for i, symbol in enumerate(tickers, 1):
            if symbol in data and not data[symbol].empty:
                price_data[symbol] = data[symbol]["Low"].iloc[-1]
                logger.info("Fetched data for %s: Day Low ₹%.2f (%d/%d)", 
                            symbol, price_data[symbol], i, total_tickers)
            else:
                logger.warning("No data for %s (%d/%d)", symbol, i, total_tickers)
    except Exception as e:
        logger.error("Error fetching batch data: %s", e)
        await send_telegram_message(f"⚠️ Error fetching stock data: {str(e)}")

    # Process trades using cached price data
    logger.info("Processing trades...")
    for i, trade in enumerate(trades, 1):
        raw_symbol = trade.get("symbol")
        trade_id = trade.get("_id")
        entry_price = trade.get("entry_price")
        alert_sent = trade.get("alert_sent", False)
        entry_alert_sent = trade.get("entry_alert_sent", False)
        last_alert_time = trade.get("last_alert_time")

        if not all([raw_symbol, trade_id, isinstance(entry_price, (int, float))]):
            logger.info("Skipping trade: Invalid data (symbol=%s, trade_id=%s, entry_price=%s) (%d/%d)", 
                        raw_symbol, trade_id, entry_price, i, total_trades)
            continue

        symbol = patch_symbol(raw_symbol)
        day_low = price_data.get(symbol)
        if day_low is None:
            logger.info("Skipping %s: No price data (%d/%d)", raw_symbol, i, total_trades)
            continue

        # Handle offset-naive last_alert_time
        if last_alert_time and not last_alert_time.tzinfo:
            last_alert_time = IST.localize(last_alert_time)

        # Prevent duplicate alerts within 30 minutes
        if last_alert_time and now - last_alert_time < timedelta(minutes=30):
            logger.info("Skipping alert for %s: Recent alert sent (%d/%d)", raw_symbol, i, total_trades)
            continue

        logger.info("Trade Check: %s | Trade ID: %s (%d/%d)", raw_symbol, trade_id, i, total_trades)
        logger.info("Entry Price ₹%.2f | Day Low ₹%.2f | Time: %s", 
                    entry_price, day_low, now.strftime('%H:%M'))

        try:
            # Approaching Alert (within 2% of entry price)
            if not alert_sent and 0 < abs(entry_price - day_low) / entry_price <= 0.02:
                msg = f"⚠️ *{raw_symbol}* is approaching entry price ₹{entry_price:.2f}\nDay Low: ₹{day_low:.2f}"
                await send_telegram_message(msg)
                await trade_collection.update_one(
                    {"_id": trade_id}, 
                    {"$set": {"alert_sent": True, "last_alert_time": now}}
                )
                logger.info("Sent approaching alert for %s (%d/%d)", raw_symbol, i, total_trades)

            # Entry Hit Alert
            elif not entry_alert_sent and day_low <= entry_price:
                msg = f"✅ *{raw_symbol}* has hit the entry price ₹{entry_price:.2f}\nDay Low: ₹{day_low:.2f}"
                await send_telegram_message(msg)
                await trade_collection.update_one(
                    {"_id": trade_id}, 
                    {"$set": {"entry_alert_sent": True, "last_alert_time": now}}
                )
                logger.info("Sent entry alert for %s (%d/%d)", raw_symbol, i, total_trades)

            # Reset alert after market close (3:30 PM IST)
            elif alert_sent and not entry_alert_sent and (now.hour > 15 or (now.hour == 15 and now.minute >= 30)):
                await trade_collection.update_one(
                    {"_id": trade_id}, 
                    {"$set": {"alert_sent": False, "last_alert_time": now}}
                )
                logger.info("Reset alert for %s at end of day (%d/%d)", raw_symbol, i, total_trades)

        except Exception as e:
            logger.error("Error processing trade %s: %s (%d/%d)", trade_id, e, i, total_trades)
            await send_telegram_message(f"⚠️ Error processing trade {raw_symbol}: {str(e)}")

async def main():
    # Validate environment variables
    if not all([TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, MONGO_URI]):
        error_msg = "Missing required environment variables: TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, or MONGODB_URI"
        logger.error(error_msg)
        await send_telegram_message(error_msg)
        exit(1)

    start_time = datetime.now(IST)
    try:
        await check_trades()
    except Exception as e:
        logger.error("Error in main: %s", e)
        await send_telegram_message(f"🔥 Error in trade alert: {str(e)}")
    finally:
        client.close()
        logger.info("MongoDB client closed")
        duration = (datetime.now(IST) - start_time).total_seconds()
        logger.info("Execution completed in %.2f seconds", duration)

if __name__ == "__main__":
    asyncio.run(main())
