from flask import Flask, jsonify, request
import os
import sys
import subprocess
import logging
from datetime import datetime as dt

# Import the provided scraper
from nse_scrapper import NSEScraper

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)


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


def load_env_file(path='.env.local'):
    """Load simple KEY=VAL lines into environment if not present.
    This mirrors simple behavior used by other scripts in this repo.
    """
    try:
        if not os.path.exists(path):
            return
        with open(path, 'r', encoding='utf-8') as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                key, val = line.split('=', 1)
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = val
    except Exception:
        return


def run_script(path, args=None, timeout=300):
    """Run a python script via subprocess and return output dict."""
    cmd = [sys.executable, path]
    if args:
        cmd += args
    env = os.environ.copy()
    # ensure .env.local is loaded for credentials
    load_env_file('.env.local')
    try:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, text=True, timeout=timeout)
        return {
            'returncode': proc.returncode,
            'stdout': proc.stdout,
            'stderr': proc.stderr,
            'cmd': ' '.join(cmd),
        }
    except subprocess.TimeoutExpired as te:
        return {'returncode': 2, 'stdout': te.stdout or '', 'stderr': f'Timeout: {te}', 'cmd': ' '.join(cmd)}
    except Exception as e:
        return {'returncode': 3, 'stdout': '', 'stderr': str(e), 'cmd': ' '.join(cmd)}


def run_all_once():
    """Run scrapper -> summarizer -> send script sequentially and collect results."""
    results = {}
    logging.info('Running nse_scrapper.py')
    results['scrape'] = run_script(os.path.join(os.getcwd(), 'nse_scrapper.py'))
    logging.info('Running summarize_last_hour.py')
    results['summarize'] = run_script(os.path.join(os.getcwd(), 'scripts', 'summarize_last_hour.py'))
    logging.info('Running send_whatsapp_template.py')
    # default: dry-run off; you can modify args if you want dry-run
    results['send'] = run_script(os.path.join(os.getcwd(), 'scripts', 'send_whatsapp_template.py'))
    return results


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


@app.route('/api/scrape', methods=['POST', 'GET'])
def api_scrape():
    """Run nse_scrapper.py and return output."""
    result = run_script(os.path.join(os.getcwd(), 'nse_scrapper.py'))
    success = result['returncode'] == 0
    return jsonify({'success': success, 'result': result})


@app.route('/api/summarize', methods=['POST', 'GET'])
def api_summarize():
    """Run summarize_last_hour.py and return output."""
    result = run_script(os.path.join(os.getcwd(), 'scripts', 'summarize_last_hour.py'))
    success = result['returncode'] == 0
    return jsonify({'success': success, 'result': result})


@app.route('/api/send', methods=['POST', 'GET'])
def api_send():
    """Run send_whatsapp_template.py and return output.
    
    Query params:
      - company_id: Company _id to send (optional, if omitted script will use DB customers list)
      - to: Phone number to send to (optional)
      - dry_run: if 'true', adds --dry-run flag
    """
    company_id = request.args.get('company_id')
    to = request.args.get('to')
    dry_run = request.args.get('dry_run', '').lower() == 'true'
    
    args = []
    if company_id:
        args += ['--company-id', company_id]
    if to:
        args += ['--to', to]
    if dry_run:
        args.append('--dry-run')
    
    result = run_script(os.path.join(os.getcwd(), 'scripts', 'send_whatsapp_template.py'), args=args)
    success = result['returncode'] == 0
    return jsonify({'success': success, 'result': result})


@app.route('/api/summarize_hour', methods=['POST', 'GET'])
def api_summarize_hour():
    """Run summarize_hour.py and return output.
    
    Processes last_hour collection and saves summaries to hourly_summaries collection.
    Each document contains: company, price, current_price, update, update_summary.
    
    Query params:
      - limit: Limit number of companies to process (optional, default 0 = all)
      - verbose: if 'true', adds --verbose flag
      - send: if 'true', sends WhatsApp messages to recipients
      - recipients: comma-separated phone numbers (e.g., 918081489340,919999999999)
      - template: WhatsApp template name (default: stockupdate1)
    """
    limit = request.args.get('limit', '0')
    verbose = request.args.get('verbose', '').lower() == 'true'
    send = request.args.get('send', '').lower() == 'true'
    recipients = request.args.get('recipients', '')
    template = request.args.get('template', 'stockupdate1')
    
    args = ['--limit', str(limit), '--template', template]
    if verbose:
        args.append('--verbose')
    if send:
        args.append('--send')
    if recipients:
        args += ['--recipients', recipients]
    
    result = run_script(os.path.join(os.getcwd(), 'scripts', 'summarize_hour.py'), args=args)
    success = result['returncode'] == 0
    return jsonify({'success': success, 'result': result})


@app.route('/api/run_all', methods=['POST', 'GET'])
def api_run_all():
    """Run full pipeline: scrape -> summarize -> send. Returns all outputs."""
    results = run_all_once()
    # success if all return 0
    success = all(r.get('returncode') == 0 for r in results.values())
    return jsonify({'success': success, 'results': results})


@app.route('/api/broadcast', methods=['POST', 'GET'])
def api_broadcast():
    """Broadcast a template message to all contacts in database.
    
    Query params:
    - company (required): Company name
    - price (required): Price info
    - update (required): Update text
    - customer (optional): Default customer name
    - template (optional): Template name (default: stockupdate1)
    - dry_run (optional): If true, don't send messages
    - verbose (optional): Verbose output
    """
    company = request.args.get('company', '')
    price = request.args.get('price', '')
    update = request.args.get('update', '')
    customer = request.args.get('customer', 'Customer')
    template = request.args.get('template', 'stockupdate1')
    dry_run = request.args.get('dry_run', '').lower() == 'true'
    verbose = request.args.get('verbose', '').lower() == 'true'
    
    if not company or not price or not update:
        return jsonify({
            'success': False, 
            'error': 'Missing required parameters: company, price, update'
        }), 400
    
    args = [
        '--company', company,
        '--price', price,
        '--update', update,
        '--customer', customer,
        '--template', template
    ]
    if dry_run:
        args.append('--dry-run')
    if verbose:
        args.append('--verbose')
    
    result = run_script(os.path.join(os.getcwd(), 'scripts', 'broadcast_message.py'), args=args)
    success = result['returncode'] == 0
    return jsonify({'success': success, 'result': result})


if __name__ == "__main__":
    # Load env file early
    load_env_file('.env.local')
    
    port = int(os.environ.get("PORT", 5000))
    # Bind to 0.0.0.0 for external access when containerized/hosted
    app.run(host="0.0.0.0", port=port)
