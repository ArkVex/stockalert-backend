import requests
import json
import os
import sys
from pprint import pprint

# Get the backend URL from command line or use default
backend_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:5000"
stock_url = f"{backend_url}/stock"

print(f"Testing stock filings from: {stock_url}")

# Add debug=true to get HTML content for inspection if needed
params = {"force": "true"}

try:
    # Make the request to our backend
    print("Sending request...")
    response = requests.get(stock_url, params=params, timeout=90)
    print(f"Received response with status code: {response.status_code}")
    
    # Check if we got a successful response
    if response.status_code == 200:
        data = response.json()
        
        # Print cache info
        print("\nCache Info:")
        print(f"  - From cache: {data.get('cache_info', {}).get('from_cache', 'N/A')}")
        print(f"  - Crawled at: {data.get('cache_info', {}).get('crawled_at', 'N/A')}")
        
        # Print filings count
        filings_count = data.get('filings_count', 0)
        filings = data.get('filings', [])
        print(f"\nFound {filings_count} filings")
        
        # Show a sample of filings
        if filings:
            print("\nFirst 3 filings:")
            for i, filing in enumerate(filings[:3]):
                print(f"\nFiling {i+1}:")
                print(f"  - Symbol: {filing.get('symbol', 'N/A')}")
                print(f"  - Company: {filing.get('company_name', 'N/A')}")
                print(f"  - Subject: {filing.get('subject', 'N/A')}")
                print(f"  - Date: {filing.get('broadcast_date', 'N/A')}")
                if filing.get('attachment_link'):
                    print(f"  - Attachment: {filing.get('attachment_link')}")
        else:
            print("\nNo filings found!")
    else:
        print(f"Error: Received status code {response.status_code}")
        try:
            error_data = response.json()
            print("Error details:")
            pprint(error_data)
            
            # Extract more detailed error information if available
            if 'body' in error_data and isinstance(error_data['body'], dict):
                body = error_data['body']
                if 'details' in body:
                    print("\nAPI Error Details:")
                    for detail in body['details']:
                        if 'message' in detail:
                            print(f"- {detail['message']}")
                        if 'keys' in detail:
                            print(f"  Invalid keys: {', '.join(detail['keys'])}")
                elif 'error' in body:
                    print(f"\nAPI Error: {body['error']}")
        except Exception as e:
            print(f"Failed to parse error response: {e}")
            print(f"Raw response: {response.text[:1000]}")
            
except Exception as e:
    print(f"Error occurred: {e}")