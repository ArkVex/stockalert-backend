"""
summarize_hour.py
Summarize PDFs and broadcast via WhatsApp to ALL contacts in the database.
"""

import os
import sys
import tempfile
import requests
import traceback
import json
import re
import argparse
from pymongo import MongoClient
from datetime import datetime
from urllib.parse import urljoin
from PyPDF2 import PdfReader

# Force UTF-8 encoding for Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')


def load_env_file(path='.env.local'):
    if not os.path.isfile(path):
        return
    try:
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
            page = reader.pages[i]
            text = page.extract_text()
            if text:
                texts.append(text)
        return '\n'.join(texts)
    except Exception:
        return ''


def summarize_text(openai_key, text, company, model='gpt-4o-mini'):
    if not text:
        return 'No text extracted from document.', None
    
    if openai_key:
        try:
            import openai
            client = openai.OpenAI(api_key=openai_key)
            prompt = f"Summarize this corporate filing for {company} in 2-3 sentences focusing on key financial or operational updates:\n\n{text[:4000]}"
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "You are a financial analyst summarizing corporate filings."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=150,
                temperature=0.3
            )
            return response.choices[0].message.content.strip(), None
        except Exception as e:
            pass
            
    sentences = text.replace('\n', ' ').split('.')
    clean = [s.strip() for s in sentences if len(s.strip()) > 20]
    summary = '. '.join(clean[:3]) + '.' if clean else 'Corporate filing update available.'
    return summary, None


def fetch_price(symbol):
    if not symbol:
        return None
    try:
        import yfinance as yf
        ticker = yf.Ticker(f"{symbol}.NS")
        hist = ticker.history(period='1d')
        if hist.empty:
            return None
        price = hist['Close'].iloc[-1]
        prev = None
        try:
            info = ticker.info
            prev = info.get('previousClose')
        except Exception:
            pass
        p = float(price)
        if prev:
            pct = (p - float(prev)) / float(prev) * 100.0
            return f"₹{p:.2f} ({pct:+.2f}%)"
        return f"₹{p:.2f}"
    except Exception:
        return None


def normalize_phone(phone):
    if not phone:
        return None
    s = str(phone).replace('+', '').replace('-', '').replace(' ', '')
    if not s.isdigit():
        return None
    if len(s) < 8 or len(s) > 15:
        return None
    return s


def build_template_payload(template_name, to, customer, company, price, update):
    """
    Build WhatsApp template payload using explicit 'parameter_name' keys.
    """
    # 1. Sanitize Inputs
    customer = str(customer).strip() if customer else 'Customer'
    company = str(company).strip() if company else 'N/A'
    price = str(price).strip() if price else 'N/A'
    update = str(update).strip() if update else 'No update available'
    
    # Truncate update to avoid payload size limits
    if len(update) > 1000:
        update = update[:997] + "..."

    # 2. Build Payload
    payload = {
        "messaging_product": "whatsapp",
        "to": str(to),
        "type": "template",
        "template": {
            "name": template_name,
            "language": {"code": "en"},
            "components": [
                {
                    "type": "body",
                    "parameters": [
                        {
                            "type": "text",
                            "parameter_name": "customer",
                            "text": customer
                        },
                        {
                            "type": "text",
                            "parameter_name": "company",
                            "text": company
                        },
                        {
                            "type": "text",
                            "parameter_name": "price",
                            "text": price
                        },
                        {
                            "type": "text",
                            "parameter_name": "update",
                            "text": update
                        }
                    ]
                }
            ]
        }
    }
    return payload


def send_message(token, phone_id, payload):
    url = f"https://graph.facebook.com/v22.0/{phone_id}/messages"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mongo-uri', help='MongoDB URI')
    parser.add_argument('--limit', type=int, default=0, help='Limit companies (0=all)')
    parser.add_argument('--model', default='gpt-4o-mini', help='OpenAI model')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--send', action='store_true', help='Send WhatsApp messages')
    parser.add_argument('--template', default='stockupdate1', help='WhatsApp template name')
    parser.add_argument('--recipients', help='Comma-separated phone numbers to force send (overrides DB)')
    args = parser.parse_args()

    load_env_file('.env.local')
    mongo_uri = os.environ.get('MONGO_URI') or os.environ.get('MONGODB_URI') or args.mongo_uri
    if not mongo_uri:
        print('✗ ERROR: MONGO_URI must be set')
        return

    openai_key = os.environ.get('OPENAI_API_KEY')
    fetch_price_flag = os.environ.get('FETCH_PRICE', '').lower() in ('1', 'true', 'yes')
    whatsapp_token = os.environ.get('WHATSAPP_TOKEN')
    whatsapp_phone_id = os.environ.get('WHATSAPP_PHONE_ID')
    send_messages = args.send and whatsapp_token and whatsapp_phone_id
    
    if args.send and not send_messages:
        print('WARNING: --send flag provided but WHATSAPP credentials missing.')

    # Force recipients logic (CLI override)
    force_recipients = []
    if args.recipients:
        for phone in args.recipients.split(','):
            norm = normalize_phone(phone)
            if norm:
                force_recipients.append({'phone': norm, 'name': 'Admin'})

    try:
        db = connect_db(mongo_uri)
        print('✓ MongoDB connection successful')
    except Exception as e:
        print(f'✗ ERROR: Could not connect to MongoDB: {e}')
        sys.exit(2)

    last_coll = db['last_hour']
    hourly_coll = db['hourly_summaries']
    contacts_coll = db['nse data']
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0'})

    try:
        docs = list(last_coll.find())
    except Exception:
        print('✗ ERROR: Could not read last_hour')
        sys.exit(3)

    print(f'Found {len(docs)} documents in last_hour')
    if args.limit and args.limit > 0:
        docs = docs[:args.limit]
    
    # === NEW: Load ALL valid contacts (Broadcasting Mode) ===
    global_db_recipients = []
    if send_messages and not force_recipients:
        try:
            all_contacts = list(contacts_coll.find())
            print(f'Found {len(all_contacts)} contacts in database')
            for contact in all_contacts:
                phone = normalize_phone(contact.get('phone') or contact.get('mobile'))
                if not phone: continue
                name = contact.get('name', 'Customer')
                
                # Simply add everyone found to the list
                global_db_recipients.append({'phone': phone, 'name': name})
                
            if args.verbose:
                print(f'Prepared to broadcast to {len(global_db_recipients)} total recipients')
        except Exception as e:
            print(f'WARNING: Could not load contacts: {e}')

    counters = {'processed': 0, 'messages_sent': 0, 'messages_failed': 0, 'summaries_success': 0}

    for doc in docs:
        try:
            company = doc.get('_id') or doc.get('company') or doc.get('latest', {}).get('Company')
            if not company: continue
            
            counters['processed'] += 1
            latest = doc.get('latest', {})
            attachment = latest.get('Attachment_URL') or latest.get('attchmntFile') or ''
            
            if not attachment:
                print(f'- {company}: No attachment, skipping')
                continue
                
            if attachment.startswith('/'):
                attachment = urljoin('https://www.nseindia.com', attachment)

            print(f'- {company}: Downloading PDF...')
            tmp_path, content_type = download_file(session, attachment)
            
            text = ''
            if tmp_path:
                text = extract_text_from_pdf(tmp_path)
            
            summary, err = summarize_text(openai_key, text, company, model=args.model)
            if not err:
                counters['summaries_success'] += 1

            price_str = None
            symbol = latest.get('Symbol') or latest.get('symbol')
            if fetch_price_flag:
                price_str = fetch_price(symbol)

            summary_doc = {
                'company': company, 
                'price': price_str or 'N/A', 
                'update': summary, 
                'symbol': symbol, 
                'timestamp': datetime.utcnow()
            }
            hourly_coll.update_one({'_id': company}, {'$set': summary_doc}, upsert=True)
            if args.verbose:
                print(f'  ✓ Saved summary for {company}')

            # === BROADCAST LOGIC ===
            if send_messages:
                # 1. Select the target list
                target_recipients = []
                if force_recipients:
                    target_recipients = force_recipients
                    if args.verbose: print("  [INFO] Using forced recipients list")
                else:
                    target_recipients = global_db_recipients
                    # No filter based on symbol anymore!

                # 2. Send loop
                for recipient in target_recipients:
                    phone = recipient['phone']
                    customer_name = recipient.get('name', 'Customer')

                    payload = build_template_payload(args.template, phone, customer_name, company, price_str, summary)

                    try:
                        send_message(whatsapp_token, whatsapp_phone_id, payload)
                        counters['messages_sent'] += 1
                        print(f'  ✓ Message sent to {phone}')
                    except requests.HTTPError as he:
                        counters['messages_failed'] += 1
                        err = he.response.text if he.response else str(he)
                        print(f'  ✗ HTTP error to {phone}: {err}')
                    except Exception as e:
                        counters['messages_failed'] += 1
                        print(f'  ✗ Error to {phone}: {e}')

        except Exception as e:
            print(f'Error processing {company}: {e}')
            if args.verbose:
                traceback.print_exc()

    print(f"\n=== Summary ===\nProcessed: {counters['processed']}\nMessages Sent: {counters['messages_sent']}\nMessages Failed: {counters['messages_failed']}")

if __name__ == '__main__':
    main()