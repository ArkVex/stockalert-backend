from flask import Flask, request, jsonify
import requests
from bs4 import BeautifulSoup
import re
import json
import os


def create_app(test_config=None):
    """Create a minimal Flask app exposing a /stock endpoint that proxies Firecrawl scrape API.

    The endpoint accepts query param `url` (optional) and will use a default NSE corporate
    filings page when not provided. It posts a JSON payload to the Firecrawl API and returns
    the parsed JSON response to the client.
    
    The response is filtered to only include the NSE corporate filings table data.
    """
    app = Flask(__name__, instance_relative_config=False)
    app.config.from_mapping(SECRET_KEY='dev')

    if test_config is not None:
        app.config.update(test_config)

    FIRECRAWL_API = "https://api.firecrawl.dev/v2/scrape"
    # Default target page (same as your snippet)
    DEFAULT_TARGET = "https://www.nseindia.com/companies-listing/corporate-filings-announcements"
    # Prefer token from environment for security; fallback to the value you provided
    FIRECRAWL_TOKEN = os.environ.get('FIRECRAWL_TOKEN', 'fc-a37b818bbd324923845c1251df2f13f5')
    FIRECRAWL_AUTH = f"Bearer {FIRECRAWL_TOKEN}"

    @app.route('/')
    def index():
        return jsonify({'status': 'ok', 'message': 'stockalert-backend (Firecrawl proxy)'})

    def parse_filings_table(html_content):
        """Extract only the corporate filings table data from the HTML content."""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find the table with the specific pattern
        # Looking for a table that has the header row with SYMBOL, COMPANY NAME, etc.
        filings = []
        
        # First find all tables
        tables = soup.find_all('table')
        
        for table in tables:
            headers = table.find_all('th')
            # Check if this table has the right headers
            header_texts = [h.get_text(strip=True) for h in headers]
            
            if 'SYMBOL' in header_texts and 'COMPANY NAME' in header_texts and 'SUBJECT' in header_texts:
                # This is our target table, extract rows
                rows = table.find_all('tr')
                
                # Skip header row
                for row in rows[1:]:
                    cells = row.find_all('td')
                    if len(cells) >= 7:  # Ensure we have enough cells
                        # Extract symbol from link or text
                        symbol_cell = cells[0]
                        symbol_link = symbol_cell.find('a')
                        symbol = symbol_link.get_text(strip=True) if symbol_link else symbol_cell.get_text(strip=True)
                        
                        # Extract company name
                        company_name = cells[1].get_text(strip=True)
                        
                        # Extract subject
                        subject = cells[2].get_text(strip=True)
                        
                        # Extract details
                        details = cells[3].get_text(strip=True)
                        
                        # Extract attachment link if present
                        attachment_cell = cells[4]
                        attachment_link = None
                        attachment_size = None
                        
                        attachment_tag = attachment_cell.find('a')
                        if attachment_tag and attachment_tag.has_attr('href'):
                            attachment_link = attachment_tag['href']
                            # Try to extract attachment size
                            size_text = attachment_cell.get_text(strip=True)
                            size_match = re.search(r'\((.+?)\)', size_text)
                            if size_match:
                                attachment_size = size_match.group(1)
                        
                        # Extract broadcast date/time
                        broadcast_date = cells[6].get_text(strip=True)
                        
                        filing = {
                            'symbol': symbol,
                            'company_name': company_name,
                            'subject': subject,
                            'details': details,
                            'attachment_link': attachment_link,
                            'attachment_size': attachment_size,
                            'broadcast_date': broadcast_date
                        }
                        filings.append(filing)
        
        return filings

    @app.route('/stock')
    def stock():
        # allow overriding the target URL via ?url=... but default to NSE filings page
        target_url = request.args.get('url', DEFAULT_TARGET)

        payload = {
            "url": target_url,
            "onlyMainContent": False,
            "maxAge": 172800000,
            "parsers": ["pdf"],
            "formats": ["markdown", "html"]
        }

        headers = {
            "Authorization": FIRECRAWL_AUTH,
            "Content-Type": "application/json"
        }

        try:
            resp = requests.post(FIRECRAWL_API, json=payload, headers=headers, timeout=30)
        except requests.RequestException as exc:
            app.logger.exception('Firecrawl request failed')
            return jsonify({'error': 'upstream_request_failed', 'message': str(exc)}), 502

        try:
            data = resp.json()
        except ValueError:
            return jsonify({'error': 'invalid_upstream_response', 'status_code': resp.status_code, 'text': resp.text}), 502

        # mirror upstream status codes: if upstream returned non-200, propagate
        if resp.status_code != 200:
            return jsonify({'error': 'upstream_error', 'status_code': resp.status_code, 'body': data}), resp.status_code
        
        # Extract HTML content from the Firecrawl response
        html_content = data.get('data', {}).get('html', '')
        
        # Parse the HTML to extract only the filings table
        filings = parse_filings_table(html_content)
        
        return jsonify({
            'source': 'firecrawl',
            'url': target_url,
            'last_updated': data.get('crawledAt', ''),
            'filings_count': len(filings),
            'filings': filings
        })

    return app
