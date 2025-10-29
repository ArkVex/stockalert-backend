import requests
import json
from datetime import datetime
import time

NSE_BASE = "https://www.nseindia.com"
API_URL = f"{NSE_BASE}/api/corporate-announcements"
CACHE_FILE = "nse_cache.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-announcements"
}

def init_session():
    s = requests.Session()
    s.get(NSE_BASE, headers=HEADERS, timeout=15)
    time.sleep(2)
    return s

def fetch_announcements(session, size=50):
    resp = session.get(API_URL, params={"index": "0", "size": str(size)}, headers=HEADERS, timeout=20)
    
    if resp.status_code != 200:
        print(f"Error: {resp.status_code}")
        return None
    
    data = resp.json().get("data", [])
    announcements = []
    
    for item in data:
        att = item.get("attchmntFile", "")
        announcements.append({
            "symbol": item.get("symbol"),
            "company_name": item.get("companyName"),
            "subject": item.get("desc"),
            "details": item.get("smInf"),
            "broadcast_date": item.get("an_dt"),
            "attachment_link": f"https://nsearchives.nseindia.com{att}" if att else None,
            "attachment_size": item.get("attchmntSize")
        })
    
    return announcements

def save_cache(announcements):
    with open(CACHE_FILE, "w") as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "filings": announcements
        }, f, indent=2)

def load_cache():
    try:
        with open(CACHE_FILE, "r") as f:
            return json.load(f).get("filings", [])
    except:
        return []

def find_new(old, new):
    old_ids = {f"{f['symbol']}_{f['broadcast_date']}_{f['subject']}" for f in old}
    return [f for f in new if f"{f['symbol']}_{f['broadcast_date']}_{f['subject']}" not in old_ids]

def main():
    print("Fetching NSE announcements...")
    
    session = init_session()
    current = fetch_announcements(session, 50)
    
    if not current:
        print("Failed to fetch data")
        return
    
    old = load_cache()
    new = find_new(old, current)
    
    if new:
        print(f"\n🆕 {len(new)} NEW ANNOUNCEMENTS:\n")
        for f in new:
            print(f"{f['symbol']:12s} | {f['broadcast_date']} | {f['subject'][:60]}")
    else:
        print("No new announcements")
    
    print(f"\nLatest 5:")
    for f in current[:5]:
        print(f"{f['symbol']:12s} | {f['broadcast_date']} | {f['subject'][:60]}")
    
    save_cache(current)
    print(f"\nSaved {len(current)} announcements")

if __name__ == "__main__":
    main()