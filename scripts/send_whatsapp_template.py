"""
send_whatsapp_template.py

Send a WhatsApp template message using the Meta/Facebook Graph API.

Reads configuration from environment variables or CLI arguments:
  - WHATSAPP_TOKEN (or --token)
  - WHATSAPP_PHONE_ID (or --phone-id)
  - TO (or --to)
  - TEMPLATE_NAME (or --template)

Example (PowerShell):
  $env:WHATSAPP_TOKEN = 'EAA...'
  $env:WHATSAPP_PHONE_ID = '918492424669959'
  python .\scripts\send_whatsapp_template.py --to 918081489340 \
    --customer Ayush --company "Symphony Ltd" --price "₹909.00 (-3.24%)" \
    --update "Earnings presentation shared; compliant with SEBI; no new financial data."

Or pass values on the command line:
  python .\scripts\send_whatsapp_template.py --token 'EAA...' --phone-id '918492424669959' --to 918081489340 ...

Warning: keep your token secret. Do not commit it into source control.
"""
import os
import json
import re
import sys
import argparse
import requests
from pymongo import MongoClient
from datetime import datetime
import os


def load_env_file(path='.env.local'):
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


def build_payload(template_name, to, customer, company, price, update_text):
    # Ensure all text values are non-empty
    customer = str(customer).strip() if customer else 'Customer'
    company = str(company).strip() if company else 'N/A'
    price = str(price).strip() if price else 'N/A'
    update_text = str(update_text).strip() if update_text else 'No update available'
    
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
                        {"type": "text", "text": customer},
                        {"type": "text", "text": company},
                        {"type": "text", "text": price},
                        {"type": "text", "text": update_text}
                    ]
                }
            ]
        }
    }
    return payload


def build_template_payload(template_name, to, parameters):
    # parameters: list of (name, text) tuples or dicts
    # Note: name is ignored - WhatsApp matches by position only
    params = []
    for p in parameters:
        if isinstance(p, dict):
            # Ensure text value is not empty
            text_value = str(p.get('text', 'N/A')).strip()
            if not text_value:
                text_value = 'N/A'
            params.append({"type": "text", "text": text_value})
        else:
            name, text = p
            # Ensure text value is not empty
            text_value = str(text).strip() if text else 'N/A'
            if not text_value:
                text_value = 'N/A'
            params.append({"type": "text", "text": text_value})

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
                    "parameters": params
                }
            ]
        }
    }
    return payload


def normalize_phone(phone):
    """Return digits-only phone string or None if invalid-looking."""
    if not phone:
        return None
    # remove common separators and plus sign
    s = re.sub(r"[^0-9]", "", str(phone))
    if not s:
        return None
    # basic sanity: length between 8 and 15 digits
    if len(s) < 8 or len(s) > 15:
        return None
    return s


def validate_recipients(recipients):
    """Validate recipients list. Returns (valid_list, invalid_entries).
    valid_list contains dicts with normalized 'phone' and 'name'.
    invalid_entries contains original items that failed validation.
    """
    valids = []
    invalids = []
    for r in recipients:
        phone = r.get('phone') if isinstance(r, dict) else r
        name = r.get('name') if isinstance(r, dict) else None
        norm = normalize_phone(phone)
        if norm:
            valids.append({'phone': norm, 'name': name or 'Customer'})
        else:
            invalids.append(r)
    return valids, invalids


def connect_db(mongo_uri):
    client = MongoClient(mongo_uri)
    client.admin.command('ping')
    return client['nse_data']


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
    parser = argparse.ArgumentParser(description='Send WhatsApp template via Meta Graph API')
    parser.add_argument('--token', help='WhatsApp API bearer token')
    parser.add_argument('--phone-id', help='WhatsApp Business Phone ID (numeric)')
    parser.add_argument('--to', help='Destination phone number with country code (e.g., 918081489340). If omitted, script will use customers list from DB')
    parser.add_argument('--template', default=os.environ.get('TEMPLATE_NAME', 'update1'), help='Template name')
    parser.add_argument('--company-id', help='Company _id to read from last_hour/company-map (required)')
    parser.add_argument('--customer', default=None, help='Customer display name to use when sending (optional)')
    parser.add_argument('--mongo-uri', help='MongoDB URI (overrides MONGO_URI env var)')
    parser.add_argument('--dry-run', action='store_true', help='Do not send messages; print payloads')
    parser.add_argument('--check-only', action='store_true', help='Only run validations and print status; do not send')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose output with debug info')
    args = parser.parse_args()

    # Load .env.local if present
    load_env_file('.env.local')

    token = args.token or os.environ.get('WHATSAPP_TOKEN')
    phone_id = args.phone_id or os.environ.get('WHATSAPP_PHONE_ID')
    to_arg = args.to or os.environ.get('TO')
    mongo_uri = args.mongo_uri or os.environ.get('MONGO_URI') or os.environ.get('MONGODB_URI')

    if not token:
        print('✗ Missing WhatsApp token. Set WHATSAPP_TOKEN or pass --token')
        return
    if not phone_id:
        print('✗ Missing WhatsApp phone id. Set WHATSAPP_PHONE_ID or pass --phone-id')
        return
    if not mongo_uri:
        print('✗ Missing MongoDB URI. Set MONGO_URI or pass --mongo-uri')
        return
    if not token:
        print('✗ Missing WhatsApp token. Set WHATSAPP_TOKEN or pass --token')
        return
    if not phone_id:
        print('✗ Missing WhatsApp phone id. Set WHATSAPP_PHONE_ID or pass --phone-id')
        return
    if not args.company_id:
        print('✗ Missing company id. Pass --company-id with the company _id from last_hour')
        return

    # Connect to DB and fetch data
    try:
        db = connect_db(mongo_uri)
    except Exception as e:
        print(f'✗ Could not connect to MongoDB: {e}')
        return

    last_coll = db['last_hour']
    main_coll = db['company-map']

    company_id = args.company_id
    last_doc = last_coll.find_one({'_id': company_id})
    if not last_doc:
        print(f'✗ No document found in last_hour with _id={company_id}')
        return

    latest = last_doc.get('latest', {})
    company = company_id
    price = latest.get('current_price') or latest.get('price') or 'N/A'
    update_text = latest.get('update') or latest.get('summary') or ''

    # Template: prefer company-map announcement template if present
    company_doc = main_coll.find_one({'_id': company_id}) or {}
    announcement = company_doc.get('announcement', {})
    template_name = announcement.get('whatsapp_template_name') or args.template
    whatsapp_template = announcement.get('whatsapp_template') or args.template

    # Determine recipients: use --to if provided, else use customers list from latest or announcement
    recipients = []
    if to_arg:
        recipients = [{'phone': to_arg, 'name': args.customer or 'Customer'}]
    else:
        # customers stored as list — support strings or dicts
        custs = latest.get('customers') or announcement.get('customers') or []
        for c in custs:
            if isinstance(c, dict):
                phone = c.get('phone') or c.get('number') or c.get('phone_number')
                name = c.get('name') or c.get('customer') or 'Customer'
            else:
                phone = str(c)
                name = 'Customer'
            if phone:
                recipients.append({'phone': phone, 'name': name})

    # Validate recipients (normalize phones)
    valid_recipients, invalid_recipients = validate_recipients(recipients)

    # Print validation summary
    if invalid_recipients:
        print('✗ Found invalid recipient entries:')
        for bad in invalid_recipients:
            print('  -', bad)

    if not valid_recipients:
        print('✗ No valid recipients found after normalization. Give --to or populate customers with valid numbers.')
        return

    # If check-only requested, print summary and exit
    if args.check_only:
        print('✔ Validation summary:')
        print('  Template:', template_name)
        print('  Company:', company)
        print('  Price:', price)
        print('  Update present:', bool(update_text))
        print(f'  Recipients found: {len(recipients)} (valid: {len(valid_recipients)}, invalid: {len(invalid_recipients)})')
        if invalid_recipients:
            print('  Invalid entries:')
            for bad in invalid_recipients:
                print('   -', bad)
        print('Run again with --dry-run to preview payloads or without flags to send (be careful).')
        # success exit code when at least one valid recipient
        sys.exit(0)

    if not recipients:
        print('✗ No recipients found: pass --to or populate customers in last_hour/company-map')
        return

    # Send to each recipient
    for r in valid_recipients:
        to = r['phone']
        customer_name = args.customer or r.get('name') or 'Customer'

        # Build parameters for template body
        params = [
            ('customer', customer_name),
            ('company', company),
            ('price', price),
            ('update', update_text)
        ]

        payload = build_template_payload(template_name, to, params)

        if args.verbose:
            print(f'→ Payload for {to}:')
            print(json.dumps(payload, indent=2))

        if args.dry_run:
            print('DRY RUN payload for', to)
            print(json.dumps(payload, indent=2))
            continue

        try:
            resp = send_message(token, phone_id, payload)
            print(f'✓ Message sent to {to}:')
            if args.verbose:
                print(json.dumps(resp, indent=2))
        except requests.HTTPError as he:
            print(f'✗ HTTP error sending message to {to}:')
            try:
                print(he.response.text)
            except Exception:
                print(str(he))
        except Exception as e:
            print(f'✗ Error sending message to {to}: {e}')


if __name__ == '__main__':
    main()
