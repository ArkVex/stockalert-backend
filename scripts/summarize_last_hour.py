"""
summarize_last_hour.py

Connects to the existing MongoDB in the repository, reads the transient
`last_hour` collection created by `nse_scrapper.py`, downloads any PDF
attachment URLs, extracts text, and sends the text to the OpenAI API
to produce a short WhatsApp-style summary. The summary and the formatted
WhatsApp message are upserted into the main `company-map` collection for
each company under `announcement.summary` and `announcement.whatsapp`.

Security note: Do NOT paste your OpenAI API key into code. Revoke any
exposed keys (the key you pasted in the repo or chat) and set the key in
the environment variable `OPENAI_API_KEY` before running this script.

Environment variables required:
  - OPENAI_API_KEY : your OpenAI API key
  - MONGO_URI      : MongoDB connection URI (mongodb+srv://...)

Usage (Windows PowerShell):
  $env:OPENAI_API_KEY = 'sk-...'
  $env:MONGO_URI = 'mongodb+srv://user:pass@cluster0...'
  python .\scripts\summarize_last_hour.py

"""
import os
import tempfile
import requests
import traceback
from pymongo import MongoClient
from datetime import datetime
from urllib.parse import urljoin
import openai
from PyPDF2 import PdfReader
import argparse
import re


def load_env_file(path='.env.local'):
    """Load simple KEY=VALUE pairs from a local env file into os.environ.
    Does not overwrite existing environment variables.
    Lines beginning with # are ignored.
    """
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
                # remove surrounding quotes if present
                if len(val) >= 2 and ((val[0] == val[-1]) and val.startswith(("'", '"'))):
                    val = val[1:-1]
                if key and key not in os.environ:
                    os.environ[key] = val
    except Exception:
        # Fail silently — we'll show errors later if required envs are missing
        return


def build_whatsapp_message(company, summary, latest, attachment_url):
    """Build a WhatsApp-style formatted message using available fields.

    Template example:
    Stock: Symphony Ltd
    CMP: ₹909.00 (-3.24%)

    Gist of the update:
    
    > 📊 Earnings Presentation Update

    📍 Key Insight(s) of the update:
    - ...

    🕵 ScoutQuest | DeepDive
    🔗 Details: <attachment_url>
    """
    # Header
    header_lines = []
    header_lines.append(f"Stock: {company}")

    # CMP is not available from the filings; include placeholder
    cmp_line = "CMP: N/A"
    header_lines.append(cmp_line)

    # Gist: use the short summary (prefixed)
    gist_lines = []
    gist_lines.append("Gist of the update:")
    gist_lines.append("")
    gist_lines.append("> " + (summary.replace('\n', ' ').strip() if summary else "(no summary available)"))

    # Key insights: split the summary into sentences and use as bullets
    insights = []
    if summary:
        # Split on sentence endings for simple bullets
        sents = re.split(r'(?<=[\.\!\?])\s+', summary.strip())
        for s in sents:
            t = s.strip()
            if t:
                # Trim to reasonable length
                if len(t) > 240:
                    t = t[:237].rstrip() + '...'
                insights.append(f"- {t}")

    if not insights:
        # Fallback to subject or description
        subj = latest.get('Subject') or latest.get('Subject', '')
        if subj:
            insights.append(f"- {subj}")
        else:
            insights.append("- No key insights extracted.")

    # Details link (use provided attachment URL)
    details_line = f"🔗 Details: {attachment_url or 'N/A'}"

    # Combine all parts
    parts = []
    parts.extend(header_lines)
    parts.append("")
    parts.extend(gist_lines)
    parts.append("")
    parts.append("📍 Key Insight(s) of the update:")
    parts.extend(insights)
    parts.append("")
    parts.append("🕵 ScoutQuest | DeepDive")
    parts.append(details_line)

    return "\n".join(parts)


def get_env(name, default=None):
    v = os.environ.get(name)
    if not v and default is None:
        raise RuntimeError(f"Environment variable {name} is required")
    return v or default


def download_file(session, url, timeout=30):
    try:
        resp = session.get(url, timeout=timeout, stream=True)
        resp.raise_for_status()
        # determine file extension from headers or url
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


def extract_text_from_pdf(path, max_pages=None):
    try:
        reader = PdfReader(path)
        texts = []
        num_pages = len(reader.pages)
        page_count = num_pages if max_pages is None else min(max_pages, num_pages)
        for i in range(page_count):
            page = reader.pages[i]
            try:
                texts.append(page.extract_text() or '')
            except Exception:
                texts.append('')
        return "\n\n".join(texts)
    except Exception:
        return ''


def summarize_text_with_openai(openai_api_key, text, company, model='gpt-4o-mini', max_tokens=250, retries=3):
    system_prompt = (
        "You are a helpful assistant that summarizes corporate filings into a short, clear WhatsApp-style message."
    )

    user_prompt = (
        f"Summarize the following filing for {company} into a concise WhatsApp message (2-3 short sentences).\n"
        "Include the key point(s), whether there is a PDF attachment, and an action-oriented sentence (e.g., 'Check attachment for details').\n"
        "Keep it informal but professional, suitable for sending to a user over WhatsApp.\n\n"
        "Filing text:\n" + (text[:15000] if text else "(no extracted text)")
    )

    # Try with retries/backoff. Return tuple (summary, error). On success error is None.
    import time
    last_err = None

    # Try the new client first, then fallback
    for attempt in range(1, retries + 1):
        try:
            try:
                from openai import OpenAI
            except Exception:
                OpenAI = None

            if OpenAI is not None:
                client = OpenAI(api_key=openai_api_key)
                resp = client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    max_tokens=max_tokens,
                    temperature=0.2,
                )

                if resp and getattr(resp, 'choices', None) and len(resp.choices) > 0:
                    try:
                        return resp.choices[0].message.content.strip(), None
                    except Exception:
                        return resp['choices'][0]['message']['content'].strip(), None

            # Fallback to legacy interface if available
            openai.api_key = openai_api_key
            resp = openai.ChatCompletion.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                max_tokens=max_tokens,
                temperature=0.2,
            )

            if resp and 'choices' in resp and len(resp['choices']) > 0:
                return resp['choices'][0]['message']['content'].strip(), None

            # If we get here, something unexpected happened
            last_err = Exception('No choices in OpenAI response')

        except Exception as e:
            last_err = e
            # If it's an auth error (401) or invalid key, don't retry
            msg = str(e).lower()
            if 'invalid_api_key' in msg or 'incorrect api key' in msg or '401' in msg:
                break

            # Exponential backoff before retrying
            if attempt < retries:
                wait = 2 ** (attempt - 1)
                time.sleep(wait)
                continue
            else:
                break

    # All attempts failed
    err_text = str(last_err) if last_err is not None else 'Unknown error'
    print(f"✗ OpenAI summarization failed: {err_text}")
    return None, err_text


def main():
    print("→ Starting summarize_last_hour.py")

    # Parse CLI args (optional overrides for environment variables)
    parser = argparse.ArgumentParser(description='Summarize PDFs from last_hour and upsert summaries to company-map')
    parser.add_argument('--openai-key', help='OpenAI API key (overrides OPENAI_API_KEY env var)')
    parser.add_argument('--mongo-uri', help='MongoDB URI (overrides MONGO_URI env var)')
    parser.add_argument('--limit', type=int, default=0, help='Limit number of companies to process (0 = no limit)')
    parser.add_argument('--model', help='OpenAI model to use (overrides OPENAI_MODEL env var). E.g. gpt-4o-mini, gpt-4o, gpt-4o-mini-2024')
    args = parser.parse_args()

    # Load .env.local (if present) so env vars defined there are available.
    # CLI args still override because get_env uses the CLI-provided default.
    load_env_file('.env.local')

    # Accept MONGODB_URI as an alternative name for MONGO_URI (backwards compat)
    if 'MONGO_URI' not in os.environ and 'MONGODB_URI' in os.environ:
        os.environ['MONGO_URI'] = os.environ['MONGODB_URI']

    # Read environment (allow CLI overrides)
    try:
        openai_key = get_env('OPENAI_API_KEY', default=args.openai_key)
        mongo_uri = get_env('MONGO_URI', default=args.mongo_uri)
        limit = int(os.environ.get('LIMIT', args.limit or 0) or 0)
        model = os.environ.get('OPENAI_MODEL', args.model) or 'gpt-4o-mini'
    except RuntimeError as re:
        print(str(re))
        return

    # Connect to MongoDB
    try:
        client = MongoClient(mongo_uri)
        client.admin.command('ping')
        db = client['nse_data']
        last_coll = db['last_hour']
        main_coll = db['company-map']
        print("✓ Connected to MongoDB and collections available")
    except Exception as e:
        print(f"✗ MongoDB connection failed: {e}")
        return

    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': '*/*',
    })

    # Iterate over companies in last_hour
    try:
        docs = list(last_coll.find())
    except Exception as e:
        print(f"✗ Failed to read last_hour collection: {e}")
        return

    print(f"→ Found {len(docs)} companies in 'last_hour'")

    # Apply limit if provided via CLI or .env.local
    if 'limit' in locals() and limit > 0:
        docs = docs[:limit]
        print(f"→ Processing limited to first {limit} companies")

    for doc in docs:
        try:
            company = doc.get('_id') or doc.get('company') or doc.get('latest', {}).get('Company') or 'Unknown'
            latest = doc.get('latest', {})
            attachment = latest.get('Attachment_URL') or latest.get('attchmntFile') or ''

            if not attachment:
                print(f"- {company}: no attachment, skipping")
                continue

            # Ensure absolute URL
            if attachment.startswith('/'):
                attachment = urljoin('https://www.nseindia.com', attachment)

            print(f"- {company}: downloading attachment {attachment}")
            tmp_path, content_type = download_file(session, attachment)

            if not tmp_path:
                print(f"  ✗ Failed to download for {company}")
                continue

            extracted = ''
            if tmp_path.lower().endswith('.pdf') or (content_type and 'pdf' in content_type.lower()):
                extracted = extract_text_from_pdf(tmp_path, max_pages=10)
                if not extracted:
                    print(f"  ✗ No text extracted from PDF for {company}")
            else:
                # Not a PDF — try to read raw bytes as text
                try:
                    with open(tmp_path, 'r', encoding='utf-8', errors='ignore') as fh:
                        extracted = fh.read()
                except Exception:
                    extracted = ''

            if not extracted:
                print(f"  → Empty extraction for {company}; saving placeholder summary")

            # Call OpenAI to summarize (pass model)
            summary, err = summarize_text_with_openai(openai_key, extracted, company, model=model)

            now = datetime.utcnow()

            if summary:
                # Save to main collection under the company's announcement
                try:
                    # Prepare formatted WhatsApp message
                    whatsapp_msg = build_whatsapp_message(company, summary, latest, attachment)

                    update = {
                        '$set': {
                            'announcement.summary': summary,
                            'announcement.whatsapp': whatsapp_msg,
                            'announcement.summary_at': now,
                            'announcement.attachment_processed': True
                        }
                    }
                    res = main_coll.update_one({'_id': company}, update, upsert=True)
                    print(f"  ✓ Upserted summary for {company} into company-map")

                    # Also update the transient last_hour collection's latest node
                    try:
                        last_update = {
                            '$set': {
                                'latest.summary': summary,
                                'latest.whatsapp': summary,
                                'latest.summary_at': now,
                                'latest.attachment_processed': True
                            }
                        }
                        last_coll.update_one({'_id': company}, last_update, upsert=True)
                        print(f"  ✓ Updated last_hour.latest for {company}")
                    except Exception as le:
                        print(f"  ✗ Failed to update last_hour for {company}: {le}")

                    # Maintain a dedicated summary-map collection for quick lookup
                    try:
                        summary_coll = db['summary-map']
                        summary_doc = {
                                '_id': company,
                                'company': company,
                                'summary': summary,
                                'whatsapp': whatsapp_msg,
                                'attachment_url': attachment,
                                'source_timestamp': latest.get('Timestamp'),
                                'processed_at': now,
                                'model': model,
                                'success': True
                            }
                        summary_coll.update_one({'_id': company}, {'$set': summary_doc}, upsert=True)
                        print(f"  ✓ Upserted summary into summary-map for {company}")
                    except Exception as se:
                        print(f"  ✗ Failed to upsert into summary-map for {company}: {se}")

                except Exception as e:
                    print(f"  ✗ Failed to upsert summary for {company} into company-map: {e}")
            else:
                # Summarization failed — record error in summary-map but do not overwrite main/company or last_hour
                try:
                    summary_coll = db['summary-map']
                    summary_doc = {
                        '_id': company,
                        'company': company,
                        'summary': None,
                        'whatsapp': None,
                        'attachment_url': attachment,
                        'source_timestamp': latest.get('Timestamp'),
                        'processed_at': now,
                        'model': model,
                        'success': False,
                        'error': err
                    }
                    summary_coll.update_one({'_id': company}, {'$set': summary_doc}, upsert=True)
                    print(f"  ✗ Summarization failed for {company}; recorded error in summary-map")
                except Exception as se:
                    print(f"  ✗ Failed to record summarization error for {company}: {se}")

        except Exception as e:
            print(f"✗ Error processing doc: {e}\n{traceback.format_exc()}")
            continue

    print("→ Completed summarize_last_hour.py")


if __name__ == '__main__':
    main()
