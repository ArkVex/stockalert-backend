Minimal server for NSE scraper

This project contains a small Flask server that exposes the existing
`nse_scrapper.py` functionality as a JSON HTTP endpoint.

Files added:
- `server.py` : Minimal Flask app exposing `/scrape` and `/`.

How to run (Windows PowerShell):

1. Create and activate a virtual environment (recommended):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2. Install requirements:

```powershell
python -m pip install -r requirements.txt
```

3. (Optional) Set your MongoDB URI in the environment if you plan to use it
   (do not commit credentials into the repo):

```powershell
$env:MONGODB_URI = 'your-mongodb-uri-here'
```

4. Run the server:

```powershell
python server.py
```

Endpoints:
- GET /           -> {"service":"nse_scraper","status":"ready"}
- GET /scrape     -> accepts query params `index`, `from_date`, `to_date`, `symbol`
                    returns JSON {"success": true, "count": N, "records": [...]}

Notes:
- The server uses the provided `nse_scrapper.py` class `NSEScraper`.
- If you provided a MongoDB URI, you can (manually) modify `server.py` to store
  results in your database. I intentionally left out database code so the
  server stays minimal and avoids committing secrets.

Persistence with `nse_scrapper.py`:
- `nse_scrapper.py` now includes a `save_to_mongo(data, mongo_uri, db_name, collection_name)`
   method and `main()` will automatically call it if the environment variable
   `MONGODB_URI` (or `MONGO_URI`) is set. By default it writes to database
   `stockalert` and collection `company` where each document `_id` is the
   company name and announcements are stored in an `announcements` array.

   Example (PowerShell):

   ```powershell
   $env:MONGODB_URI = 'mongodb+srv://<user>:<pw>@cluster0.r5xgtsc.mongodb.net/'
   python nse_scrapper.py
   ```

   The script will upsert announcements and avoid duplicate inserts when an
   announcement with the same `Timestamp` already exists for a company.
 - If you set the environment variable `MONGODB_URI`, the server will automatically
    save scraped announcements into the `company` collection. Each company will
    be stored as a document where the document `_id` is the company name and
    announcements are appended into an `announcements` array.
