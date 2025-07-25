import asyncio
import yfinance as yf
from datetime import datetime, time
import pytz
import os
from motor.motor_asyncio import AsyncIOMotorClient
import aiohttp
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
MONGO_URI = os.getenv("MONGODB_URI")

logging.info("Environment Loaded: TELEGRAM_TOKEN=%s, TELEGRAM_CHAT_ID=%s, MONGO_URI=%s", 
             TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, MONGO_URI)

# MongoDB setup
client = AsyncIOMotorClient(MONGO_URI)
db = client["stock_zones"]
zone_collection = db["demand_zones"]

IST = pytz.timezone("Asia/Kolkata")

def patch_symbol(symbol: str) -> str:
    """Appends '.NS' if no exchange suffix found (assumes NSE by default)."""
    if '.' not in symbol:
        return symbol + '.NS'
    return symbol

async def send_telegram_message(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }

    logging.info("Sending Telegram Message: URL=%s, Payload=%s", url, payload)

    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=payload) as resp:
            text = await resp.text()
            logging.info("Response Status: %s, Response Text: %s", resp.status, text)
            if resp.status != 200:
                raise Exception(f"Telegram API Error: {text}")

async def check_zones():
    # Check market hours
    now = datetime.now(IST)
    if now.weekday() >= 5 or now.time() < time(9, 15) or now.time() > time(15, 30):
        logging.info("Outside market hours (9:15 AM - 3:30 PM IST), skipping.")
        return

    logging.info("Checking Zones...")

    # Fetch all fresh zones
    zones = await zone_collection.find({"freshness": {"$gt": 0}}).to_list(None)
    if not zones:
        logging.info("No fresh zones found.")
        return

    # Get unique symbols
    tickers = list(set(patch_symbol(zone["ticker"]) for zone in zones))
    logging.info("Unique symbols to fetch: %s", tickers)

    # Fetch data once per symbol
    price_data = {}
    for symbol in tickers:
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="1d")
            if not hist.empty:
                price_data[symbol] = hist["Low"].iloc[-1]
                logging.info("Fetched data for %s: Day Low ₹%.2f", symbol, price_data[symbol])
            else:
                logging.warning("No data for %s", symbol)
        except Exception as e:
            logging.error("Error fetching data for %s: %s", symbol, e)

    # Process zones using cached price data
    for zone in zones:
        symbol_raw = zone["ticker"]
        symbol = patch_symbol(symbol_raw)
        zone_id = zone["zone_id"]
        proximal = zone["proximal_line"]
        distal = zone["distal_line"]
        day_low = price_data.get(symbol)

        if day_low is None:
            logging.info("Skipping %s (no price data)", symbol_raw)
            continue

        zone_alert_sent = zone.get("zone_alert_sent", False)
        zone_entry_sent = zone.get("zone_entry_sent", False)

        logging.info("Zone Check: %s | Zone ID: %s", symbol_raw, zone_id)
        logging.info("Proximal ₹%.2f | Distal ₹%.2f | Day Low ₹%.2f", proximal, distal, day_low)

        try:
            # Approaching alert
            if not zone_alert_sent and 0 < abs(proximal - day_low) / proximal <= 0.03:
                msg = f"📶 *{symbol_raw}* zone approaching entry\nZone ID: `{zone_id}`\nProximal: ₹{proximal:.2f}\nDay Low: ₹{day_low:.2f}"
                await send_telegram_message(msg)
                await zone_collection.update_one(
                    {"_id": zone["_id"]}, {"$set": {"zone_alert_sent": True}}
                )

            # Entry alert
            if not zone_entry_sent and day_low <= proximal:
                msg = f"🎯 *{symbol_raw}* zone entry hit!\nZone ID: `{zone_id}`\nProximal: ₹{proximal:.2f}\nDay Low: ₹{day_low:.2f}"
                await send_telegram_message(msg)
                await zone_collection.update_one(
                    {"_id": zone["_id"]}, {"$set": {"zone_entry_sent": True}}
                )

            # Distal breach → freshness = 0, trade_score = 0
            if day_low < distal:
                msg = f"🛑 *{symbol_raw}* zone breached distal!\nZone ID: `{zone_id}`\nDistal: ₹{distal:.2f}\nDay Low: ₹{day_low:.2f}\n⚠️ Marking as no longer fresh"
                await send_telegram_message(msg)
                await zone_collection.update_one(
                    {"_id": zone["_id"]},
                    {"$set": {"freshness": 0, "trade_score": 0}}
                )
                logging.info("Marked not fresh: %s", symbol_raw)

        except Exception as e:
            logging.error("Error processing zone %s: %s", zone_id, e)

async def main():
    try:
        await check_zones()
    except Exception as e:
        logging.error("Error in main: %s", e)
        await send_telegram_message(f"Error in stock alert: {e}")

if __name__ == "__main__":
    asyncio.run(main())