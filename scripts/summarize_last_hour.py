"""Clean summarizer for last_hour documents.

This script reads the transient `last_hour` collection (created by
`nse_scrapper.py`), downloads PDF attachments, extracts text, creates a
short summary, and upserts structured fields required by the
WhatsApp template into three places:

- `company-map` (under `announcement.*`)
- `last_hour` (updates `latest.*` for the company)
- `summary-map` (a concise record for quick lookups)

The template fields added/updated are:
- announcement.update (short summary)
- announcement.summary (same as update)
- announcement.current_price (string or None)
- announcement.whatsapp (ready-to-send message using a `{{customer}}` placeholder)
- announcement.customers (empty list)

Environment variables (or use .env.local):
- MONGO_URI (or MONGODB_URI)
- OPENAI_API_KEY (optional — when present, uses OpenAI for summarization)
- FETCH_PRICE (optional; set to 1/true to attempt yfinance price lookup)

This file intentionally keeps logic small and readable.
"""

import os
import tempfile
import sys
import time
import requests
import traceback
from pymongo import MongoClient
from datetime import datetime
from urllib.parse import urljoin
from PyPDF2 import PdfReader
import argparse
import re


def load_env_file(path='.env.local'):
    try:
        if not os.path.exists(path):
            return
        with open(path, 'r', encoding='utf-8') as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' not in line:
                    continue
                key, val = line.split('=', 1)
                key = key.strip()
                val = val.strip()
                if len(val) >= 2 and ((val[0] == val[-1]) and val.startswith(("'", '"'))):
                    val = val[1:-1]
                if key and key not in os.environ:
                    os.environ[key] = val
    except Exception:
        return


def get_env(name, default=None, required=False):
    v = os.environ.get(name, None)
    if v is None:
        v = default
    if required and not v:
        raise RuntimeError(f"Environment variable {name} is required")
    return v


def connect_db(mongo_uri):
    client = MongoClient(mongo_uri)
    client.admin.command('ping')
    return client['nse_data']


def download_file(session, url, timeout=30):
    try:
        resp = session.get(url, timeout=timeout, stream=True)
        resp.raise_for_status()
        content_type = resp.headers.get('Content-Type', '')
        ext = '.pdf' if 'pdf' in content_type.lower() or url.lower().endswith('.pdf') else ''
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=ext)
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                tmp.write(chunk)
        tmp.flush()
        tmp.close()
        return tmp.name, content_type
    except Exception:
        return None, None


def extract_text_from_pdf(path, max_pages=10):
    try:
        reader = PdfReader(path)
        texts = []
        num_pages = len(reader.pages)
        page_count = min(num_pages, max_pages) if max_pages else num_pages
        for i in range(page_count):
            try:
                texts.append(reader.pages[i].extract_text() or '')
            except Exception:
                texts.append('')
        return '\n\n'.join(texts).strip()
    except Exception:
        return ''


def summarize_text(openai_key, text, company, model='gpt-4o-mini'):
    """Use OpenAI if key is present; otherwise produce a short fallback summary.

    This function is resilient: it first attempts the new `openai.OpenAI`
    client (openai>=1.0.0). If that fails for any reason it will fall back
    to a simple heuristic summary so the script can continue and still
    produce a usable `update` text.
    """
    if not text:
        return f"No extracted text for {company}. See attachment.", None

    # Try OpenAI new client first (works for openai>=1.0.0)
    if openai_key:
        prompt = (
            f"Summarize the filing for {company} in 2 short sentences, focus on the key point and an action.\n\n" +
            (text[:15000])
        )
        try:
            try:
                from openai import OpenAI
                client = OpenAI(api_key=openai_key)
                resp = client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": "You are a concise assistant summarizing corporate filings."},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=200,
                    temperature=0.2,
                )
                # New client returns choices with message content
                if resp and getattr(resp, 'choices', None) and len(resp.choices) > 0:
                    try:
                        return resp.choices[0].message.content.strip(), None
                    except Exception:
                        # some older wrappers may return dict-like
                        return resp['choices'][0]['message']['content'].strip(), None
            except Exception:
                # If new client failed, try legacy interface as a backup
                import openai as legacy_openai
                legacy_openai.api_key = openai_key
                resp = legacy_openai.ChatCompletion.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": "You are a concise assistant summarizing corporate filings."},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=200,
                    temperature=0.2,
                )
                if resp and 'choices' in resp and len(resp['choices']) > 0:
                    return resp['choices'][0]['message']['content'].strip(), None
        except Exception as e:
            # Log the error and continue to fallback heuristic summary below
            print(f"[warning] OpenAI call failed, falling back to heuristic summary: {e}")

    # Heuristic fallback: first 2 sentences or first 200 characters
    sents = re.split(r'(?<=[\.\!\?])\s+', text.strip())
    if len(sents) >= 2:
        summary = ' '.join(sents[:2]).strip()
    else:
        t = text.strip()
        summary = (t[:200] + '...') if len(t) > 200 else t
    return summary, None


def fetch_price(symbol):
    try:
        import yfinance as yf
    except Exception:
        return None
    if not symbol:
        return None
    ticker = symbol if '.' in symbol or symbol.endswith('.NS') else symbol + '.NS'
    try:
        t = yf.Ticker(ticker)
        fi = getattr(t, 'fast_info', None)
        price = None
        prev = None
        if fi:
            price = fi.get('lastPrice') or fi.get('last_price')
            prev = fi.get('previous_close')
        if price is None:
            hist = t.history(period='1d')
            if not hist.empty:
                last_row = hist.iloc[-1]
                price = last_row.get('Close') or last_row.get('close')
        if price is None:
            return None
        p = float(price)
        if prev:
            pct = (p - float(prev)) / float(prev) * 100.0
            return f"₹{p:.2f} ({pct:+.2f}%)"
        return f"₹{p:.2f}"
    except Exception:
        return None


def build_template_message(company, price_str, update_summary, attachment_url):
    # Keep customer as a placeholder to be replaced per-recipient later
    tpl = (
        "Hello {{customer}}, here is your latest stock update.\n\n"
        "stock: {{company}}\n"
        "current_price: {{price}}\n\n"
        "update_summary:\n"
        "{{update}}\n\n"
        "You can view the full details on the company’s official website.\n"
        "Reply STOP to unsubscribe."
    )
    # Also produce a pre-filled whatsapp message where customer placeholder remains
    filled = tpl.replace('{{company}}', company)
    filled = filled.replace('{{price}}', price_str or 'N/A')
    filled = filled.replace('{{update}}', update_summary or '(no summary)')
    # Optionally append attachment link
    if attachment_url:
        filled += f"\n\nDetails: {attachment_url}"
    return tpl, filled


def main():
    parser = argparse.ArgumentParser(description='Clean summarizer: update last_hour/company-map with template fields')
    parser.add_argument('--mongo-uri', help='MongoDB URI (overrides MONGO_URI env var)')
    parser.add_argument('--limit', type=int, default=0, help='Limit how many companies to process (0=all)')
    parser.add_argument('--model', default='gpt-4o-mini', help='OpenAI model to use if OPENAI_API_KEY is set')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose output')
    args = parser.parse_args()

    load_env_file('.env.local')
    mongo_uri = get_env('MONGO_URI', default=args.mongo_uri or os.environ.get('MONGODB_URI'))
    if not mongo_uri:
        print('ERROR: MONGO_URI or MONGODB_URI must be set in environment or .env.local')
        return

    openai_key = os.environ.get('OPENAI_API_KEY')
    fetch_price_flag = os.environ.get('FETCH_PRICE', '').lower() in ('1', 'true', 'yes')

    db = None
    try:
        db = connect_db(mongo_uri)
        print('✓ MongoDB connection successful')
    except Exception as e:
        print(f'ERROR: Could not connect to MongoDB: {e}')
        sys.exit(2)

    last_coll = db['last_hour']
    main_coll = db['company-map']
    summary_coll = db['summary-map']

    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0', 'Accept': '*/*'})

    try:
        docs = list(last_coll.find())
    except Exception as e:
        print(f'ERROR: Could not read last_hour collection: {e}')
        sys.exit(3)

    total_docs = len(docs)
    print(f'Found {total_docs} documents in last_hour')
    if args.limit and args.limit > 0:
        docs = docs[:args.limit]
    verbose = args.verbose

    # Counters for a final health summary
    counters = {
        'total': total_docs,
        'processed': 0,
        'skipped_no_attachment': 0,
        'download_fail': 0,
        'extraction_empty': 0,
        'summaries_success': 0,
        'summaries_failed': 0,
        'company_map_errors': 0,
        'last_hour_errors': 0,
        'summary_map_errors': 0,
    }

    for doc in docs:
        try:
            company = doc.get('_id') or doc.get('company') or doc.get('latest', {}).get('Company')
            if not company:
                print('Skipping doc with no company id')
                continue
            counters['processed'] += 1
            latest = doc.get('latest', {})
            attachment = latest.get('Attachment_URL') or latest.get('attchmntFile') or ''
            if not attachment:
                print(f'- {company}: no attachment URL, skipping')
                counters['skipped_no_attachment'] += 1
                continue
            if attachment.startswith('/'):
                attachment = urljoin('https://www.nseindia.com', attachment)

            print(f'- {company}: downloading {attachment}')
            tmp_path, content_type = download_file(session, attachment)
            if not tmp_path:
                print(f'  ✗ failed to download attachment for {company}')
                counters['download_fail'] += 1
                continue

            text = ''
            if tmp_path.lower().endswith('.pdf') or ('pdf' in (content_type or '').lower()):
                text = extract_text_from_pdf(tmp_path, max_pages=10)
            else:
                try:
                    with open(tmp_path, 'r', encoding='utf-8', errors='ignore') as fh:
                        text = fh.read()
                except Exception:
                    text = ''

            if not text:
                print(f'  → extracted text empty for {company}')
                counters['extraction_empty'] += 1

            summary, err = summarize_text(openai_key, text, company, model=args.model)
            if err:
                print(f'  ✗ summarization error for {company}: {err}')
                counters['summaries_failed'] += 1
            else:
                counters['summaries_success'] += 1

            price_str = None
            if fetch_price_flag:
                symbol = latest.get('Symbol') or latest.get('symbol')
                price_str = fetch_price(symbol)

            now = datetime.utcnow()

            # Build template message and filled whatsapp text
            tpl, whatsapp_msg = build_template_message(company, price_str, summary, attachment)

            # Upsert into company-map
            try:
                up = {
                    '$set': {
                        'announcement.summary': summary,
                        'announcement.update': summary,
                        # Do not store full filled WhatsApp message to DB (privacy/size).
                        # Store only the template with placeholders and structured fields.
                        'announcement.whatsapp_template': tpl,
                        'announcement.current_price': price_str,
                        'announcement.customers': [],
                        'announcement.summary_at': now,
                        'announcement.attachment_processed': True,
                        'announcement.attachment_url': attachment,
                    }
                }
                main_coll.update_one({'_id': company}, up, upsert=True)
                if verbose:
                    print(f'  ✓ updated company-map for {company}')
            except Exception as e:
                print(f'  ✗ failed to update company-map for {company}: {e}')
                counters['company_map_errors'] += 1

            # Update transient last_hour latest node
            try:
                last_up = {
                    '$set': {
                        'latest.summary': summary,
                        'latest.update': summary,
                        # Do not save the full filled WhatsApp message here.
                        'latest.price': price_str,
                        'latest.current_price': price_str,
                        'latest.customers': [],
                        'latest.summary_at': now,
                        'latest.attachment_processed': True,
                    }
                }
                last_coll.update_one({'_id': company}, last_up, upsert=True)
                if verbose:
                    print(f'  ✓ updated last_hour.latest for {company}')
            except Exception as e:
                print(f'  ✗ failed to update last_hour for {company}: {e}')
                counters['last_hour_errors'] += 1

            # Insert/replace into summary-map
            try:
                summary_doc = {
                    '_id': company,
                    'company': company,
                    'summary': summary,
                    'update': summary,
                    # Do not store the full filled WhatsApp message; keep template only
                    'whatsapp_template': tpl,
                    'attachment_url': attachment,
                    'current_price': price_str,
                    'customers': [],
                    'source_timestamp': latest.get('Timestamp'),
                    'processed_at': now,
                    'success': True if summary else False,
                }
                summary_coll.update_one({'_id': company}, {'$set': summary_doc}, upsert=True)
                if verbose:
                    print(f'  ✓ upserted summary-map for {company}')
            except Exception as e:
                print(f'  ✗ failed to update summary-map for {company}: {e}')
                counters['summary_map_errors'] += 1

        except Exception as e:
            print(f'Error processing company doc: {e}\n{traceback.format_exc()}')
            counters['summaries_failed'] += 1

    # Final summary and exit code
    print('\n=== Summary ===')
    print(f"Total documents found: {counters['total']}")
    print(f"Processed: {counters['processed']}")
    print(f"Skipped (no attachment): {counters['skipped_no_attachment']}")
    print(f"Download failures: {counters['download_fail']}")
    print(f"Empty extraction: {counters['extraction_empty']}")
    print(f"Summaries succeeded: {counters['summaries_success']}")
    print(f"Summaries failed: {counters['summaries_failed']}")
    print(f"company-map errors: {counters['company_map_errors']}")
    print(f"last_hour errors: {counters['last_hour_errors']}")
    print(f"summary-map errors: {counters['summary_map_errors']}")

    critical_failures = counters['download_fail'] + counters['summaries_failed'] + counters['company_map_errors'] + counters['last_hour_errors'] + counters['summary_map_errors']
    if critical_failures > 0:
        print('Exiting with error code 1 due to failures')
        sys.exit(1)
    else:
        print('Exiting with code 0 — all done')
        sys.exit(0)


if __name__ == '__main__':
    main()
