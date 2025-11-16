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
import argparse
import requests


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
                        {"type": "text", "parameter_name": "customer", "text": str(customer)},
                        {"type": "text", "parameter_name": "company", "text": str(company)},
                        {"type": "text", "parameter_name": "price", "text": str(price)},
                        {"type": "text", "parameter_name": "update", "text": str(update_text)}
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
    parser = argparse.ArgumentParser(description='Send WhatsApp template via Meta Graph API')
    parser.add_argument('--token', help='WhatsApp API bearer token')
    parser.add_argument('--phone-id', help='WhatsApp Business Phone ID (numeric)')
    parser.add_argument('--to', help='Destination phone number with country code (e.g., 918081489340)')
    parser.add_argument('--template', default=os.environ.get('TEMPLATE_NAME', 'stockupdate'), help='Template name')
    parser.add_argument('--customer', default='Customer', help='Customer name')
    parser.add_argument('--company', default='Company', help='Company name')
    parser.add_argument('--price', default='N/A', help='CMP / price string')
    parser.add_argument('--update', dest='update_text', default='', help='Update text')
    args = parser.parse_args()

    # Load .env.local if present
    load_env_file('.env.local')

    token = args.token or os.environ.get('WHATSAPP_TOKEN')
    phone_id = args.phone_id or os.environ.get('WHATSAPP_PHONE_ID')
    to = args.to or os.environ.get('TO')

    if not token:
        print('✗ Missing WhatsApp token. Set WHATSAPP_TOKEN or pass --token')
        return
    if not phone_id:
        print('✗ Missing WhatsApp phone id. Set WHATSAPP_PHONE_ID or pass --phone-id')
        return
    if not to:
        print('✗ Missing destination number. Pass --to or set TO in env')
        return

    payload = build_payload(args.template, to, args.customer, args.company, args.price, args.update_text)

    try:
        resp = send_message(token, phone_id, payload)
        print('✓ Message sent successfully!')
        print(json.dumps(resp, indent=2))
    except requests.HTTPError as he:
        print('✗ HTTP error sending message:')
        try:
            print(he.response.text)
        except Exception:
            print(str(he))
    except Exception as e:
        print('✗ Error sending message:')
        print(str(e))


if __name__ == '__main__':
    main()
