#!/usr/bin/env python3
"""
Broadcast a WhatsApp template message to all contacts in the database.
"""
import sys
import os
import argparse
import json
import requests
from pymongo import MongoClient

# Force UTF-8 encoding on Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')


def load_env_file(path='.env.local'):
    """Load environment variables from .env.local if it exists."""
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


def build_template_payload(template_name, to, customer, company, price, update):
    """Build WhatsApp template payload."""
    # Ensure all text values are non-empty
    customer = str(customer).strip() if customer else 'Customer'
    company = str(company).strip() if company else 'N/A'
    price = str(price).strip() if price else 'N/A'
    update = str(update).strip() if update else 'No update available'
    
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
                        {"type": "text", "parameter_name": "customer", "text": customer},
                        {"type": "text", "parameter_name": "company", "text": company},
                        {"type": "text", "parameter_name": "price", "text": price},
                        {"type": "text", "parameter_name": "update", "text": update}
                    ]
                }
            ]
        }
    }
    return payload


def send_message(token, phone_id, payload):
    """Send WhatsApp message via Meta Graph API."""
    url = f"https://graph.facebook.com/v22.0/{phone_id}/messages"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def connect_db(mongo_uri):
    """Connect to MongoDB."""
    client = MongoClient(mongo_uri)
    client.admin.command('ping')
    return client['nse_data']


def main():
    parser = argparse.ArgumentParser(description='Broadcast WhatsApp template to all contacts')
    parser.add_argument('--mongo-uri', help='MongoDB URI (overrides MONGO_URI env var)')
    parser.add_argument('--token', help='WhatsApp API bearer token')
    parser.add_argument('--phone-id', help='WhatsApp Business Phone ID')
    parser.add_argument('--template', default='update1', help='Template name')
    parser.add_argument('--customer', default='Customer', help='Customer name')
    parser.add_argument('--company', required=True, help='Company name')
    parser.add_argument('--price', required=True, help='Price info')
    parser.add_argument('--update', required=True, help='Update text')
    parser.add_argument('--dry-run', action='store_true', help='Print payloads without sending')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    args = parser.parse_args()

    # Load environment from .env.local
    load_env_file()

    # Get credentials
    mongo_uri = args.mongo_uri or os.environ.get('MONGO_URI') or os.environ.get('MONGODB_URI')
    token = args.token or os.environ.get('WHATSAPP_TOKEN')
    phone_id = args.phone_id or os.environ.get('WHATSAPP_PHONE_ID')

    if not mongo_uri:
        print('✗ ERROR: MongoDB URI not provided (use --mongo-uri or set MONGO_URI env var)')
        sys.exit(1)

    if not args.dry_run:
        if not token:
            print('✗ ERROR: WhatsApp token not provided (use --token or set WHATSAPP_TOKEN env var)')
            sys.exit(1)
        if not phone_id:
            print('✗ ERROR: WhatsApp phone ID not provided (use --phone-id or set WHATSAPP_PHONE_ID env var)')
            sys.exit(1)

    # Connect to database
    try:
        db = connect_db(mongo_uri)
        print('✓ MongoDB connection successful')
    except Exception as e:
        print(f'✗ ERROR: Could not connect to MongoDB: {e}')
        sys.exit(2)

    # Get all contacts
    contacts_coll = db['nse data']
    try:
        all_contacts = list(contacts_coll.find())
        print(f'Found {len(all_contacts)} contacts in database')
    except Exception as e:
        print(f'✗ ERROR: Could not read nse data collection: {e}')
        sys.exit(3)

    if not all_contacts:
        print('✗ No contacts found in database')
        sys.exit(0)

    # Extract phone numbers and names
    recipients = []
    for contact in all_contacts:
        phone = contact.get('phone') or contact.get('mobile')
        if not phone:
            continue
        # Remove + prefix and any non-digits for WhatsApp API
        phone = str(phone).replace('+', '').replace('-', '').replace(' ', '')
        name = contact.get('name', 'Customer')
        recipients.append({'phone': phone, 'name': name})

    print(f'Found {len(recipients)} contacts with phone numbers')

    if not recipients:
        print('✗ No valid phone numbers found')
        sys.exit(0)

    # Counters
    sent = 0
    failed = 0

    # Send to each recipient
    for recipient in recipients:
        phone = recipient['phone']
        customer_name = recipient.get('name', args.customer)

        # Build payload
        payload = build_template_payload(
            args.template,
            phone,
            customer_name,
            args.company,
            args.price,
            args.update
        )

        if args.verbose or args.dry_run:
            print(f'\n→ Payload for {phone} ({customer_name}):')
            print(json.dumps(payload, indent=2))

        if args.dry_run:
            continue

        # Send message
        try:
            resp = send_message(token, phone_id, payload)
            sent += 1
            print(f'✓ Message sent to {phone} ({customer_name})')
            if args.verbose:
                print(f'  Response: {json.dumps(resp)}')
        except requests.HTTPError as he:
            failed += 1
            error_detail = he.response.text if he.response else str(he)
            print(f'✗ HTTP error sending to {phone}: {error_detail}')
        except Exception as e:
            failed += 1
            print(f'✗ Error sending to {phone}: {e}')

    # Summary
    print(f'\n=== Summary ===')
    print(f'Total contacts: {len(recipients)}')
    if args.dry_run:
        print('DRY RUN - no messages sent')
    else:
        print(f'Messages sent: {sent}')
        print(f'Failed: {failed}')

    sys.exit(0 if failed == 0 else 1)


if __name__ == '__main__':
    main()
