from flask import Flask, jsonify, request
import os
from datetime import datetime as dt

# Import the provided scraper
from nse_scrapper import NSEScraper

app = Flask(__name__)


# Optional MongoDB support: if MONGODB_URI is set the server will save
# scraped announcements into the `company` collection using Company name
# as the document _id and pushing announcements to an `announcements` array.
DB = None
MONGODB_URI = os.environ.get("MONGODB_URI")
MONGODB_DB = os.environ.get("MONGODB_DB", "stockalert")

if MONGODB_URI:
    try:
        from pymongo import MongoClient

        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        DB = client[MONGODB_DB]
        print(f"✓ Connected to MongoDB database: {MONGODB_DB}")
    except Exception as e:
        print(f"✗ MongoDB connection failed: {e}")
        DB = None
else:
    print("→ No MONGODB_URI configured. Database disabled.")


@app.route("/", methods=["GET"])
def index():
    return jsonify({"service": "nse_scraper", "status": "ready"})


@app.route("/scrape", methods=["GET"])
def scrape():
    """Call the NSEScraper, return JSON records and save to MongoDB if configured.

    Query params supported:
      - index (default: equities)
      - from_date (format: DD-MM-YYYY)
      - to_date (format: DD-MM-YYYY)
      - symbol
    """
    index = request.args.get("index", "equities")
    from_date = request.args.get("from_date")
    to_date = request.args.get("to_date")
    symbol = request.args.get("symbol")

    scraper = NSEScraper()

    data = scraper.fetch_corporate_filings(
        index=index,
        from_date=from_date,
        to_date=to_date,
        symbol=symbol,
    )

    if not data:
        return jsonify({"success": False, "error": "Failed to fetch data from NSE"}), 500

    # Normalize to list of records
    if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
        records = data["data"]
    elif isinstance(data, list):
        records = data
    else:
        df = scraper.parse_to_dataframe(data)
        if df is None:
            return jsonify({"success": False, "error": "No records found in response"}), 404
        records = df.to_dict(orient="records")

    saved = 0
    save_errors = []

    if DB:
        coll = DB["company"]
        now = dt.utcnow()
        for rec in records:
            # Use Company as _id per request; fallback to Symbol if Company not present
            company = (rec.get("Company") or rec.get("sm_name") or rec.get("Symbol"))
            if not company:
                continue

            announcement = {
                "Symbol": rec.get("Symbol"),
                "Subject": rec.get("Subject") or rec.get("desc"),
                "Description": rec.get("Description") or rec.get("attchmntText"),
                "Attachment_URL": rec.get("Attachment_URL") or rec.get("attchmntFile"),
                "File_Size": rec.get("File_Size") or rec.get("sm_size"),
                "Timestamp": rec.get("Timestamp") or rec.get("an_dt"),
                "XBRL_Link": rec.get("XBRL_Link") or rec.get("xbrl"),
                "scraped_at": now,
            }

            try:
                coll.update_one(
                    {"_id": company},
                    {
                        "$push": {"announcements": announcement},
                        "$set": {"last_updated": now, "symbol": rec.get("Symbol")},
                    },
                    upsert=True,
                )
                saved += 1
            except Exception as e:
                save_errors.append({"company": company, "error": str(e)})

    result = {"success": True, "count": len(records), "records": records}
    if DB:
        result["saved"] = saved
        if save_errors:
            result["save_errors"] = save_errors

    return jsonify(result)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    # Bind to 0.0.0.0 for external access when containerized/hosted
    app.run(host="0.0.0.0", port=port)
