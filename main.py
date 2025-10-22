import requests
import json
from datetime import datetime

FILE_PATH = "nse_stock_list.json"

NSE_BASE = "https://www.nseindia.com"
NSE_API_URL = f"{NSE_BASE}/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "DNT": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1"
}

def get_all_stocks():
    session = requests.Session()
    # Step 1: visit homepage to get cookies
    home = session.get(NSE_BASE, headers=HEADERS, timeout=15)
    if home.status_code != 200:
        raise ValueError(f"Failed to initialize session, status {home.status_code}")

    # Step 2: get actual stock data
    api_headers = HEADERS.copy()
    api_headers["Referer"] = "https://www.nseindia.com/market-data/live-equity-market"
    response = session.get(NSE_API_URL, headers=api_headers, timeout=15)

    if response.status_code != 200:
        raise ValueError(f"Request failed with status {response.status_code}")

    data = response.json()
    stocks = [item["symbol"] for item in data.get("data", [])]
    if not stocks:
        raise ValueError("No stock data received.")
    return stocks

def save_stock_list(stocks):
    with open(FILE_PATH, "w") as f:
        json.dump({"timestamp": datetime.now().isoformat(), "symbols": stocks}, f, indent=2)

def load_old_stock_list():
    try:
        with open(FILE_PATH, "r") as f:
            return json.load(f)["symbols"]
    except (FileNotFoundError, KeyError, json.JSONDecodeError):
        return []

def main():
    try:
        current_stocks = get_all_stocks()
    except Exception as e:
        print("⚠️ Error fetching data from NSE:", e)
        return

    old_stocks = load_old_stock_list()
    new_listings = [s for s in current_stocks if s not in old_stocks]

    if new_listings:
        print("🆕 New listings in the last 24 hours:")
        for s in new_listings:
            print(" -", s)
    else:
        print("No new listings found.")

    save_stock_list(current_stocks)

if __name__ == "__main__":
    main()
