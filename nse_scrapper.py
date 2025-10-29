import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime
import json
import brotli  # For Brotli decompression

class NSEScraper:
    def __init__(self):
        self.base_url = "https://www.nseindia.com"
        self.session = requests.Session()
        
        # Updated headers to better mimic browser
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://www.nseindia.com/companies-listing/corporate-filings-announcements',
            'X-Requested-With': 'XMLHttpRequest'
        }
        
    def get_cookies(self):
        """Get cookies by visiting the announcements page first"""
        try:
            url = f"{self.base_url}/companies-listing/corporate-filings-announcements"
            response = self.session.get(
                url,
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 200:
                print("✓ Cookies obtained successfully")
                print(f"✓ Session cookies: {len(self.session.cookies)} cookies set")
                return True
            else:
                print(f"✗ Failed to get cookies: {response.status_code}")
                return False
        except Exception as e:
            print(f"✗ Error getting cookies: {e}")
            return False
    
    def fetch_corporate_filings(self, index="equities", from_date=None, to_date=None, symbol=None):
        """Fetch corporate filings from NSE API"""
        
        if not self.get_cookies():
            return None
        
        time.sleep(3)
        
        api_url = f"{self.base_url}/api/corporate-announcements"
        
        params = {
            'index': index
        }
        
        if from_date:
            params['from_date'] = from_date
        if to_date:
            params['to_date'] = to_date
        if symbol:
            params['symbol'] = symbol
        
        try:
            print(f"→ Requesting: {api_url}")
            print(f"→ Params: {params}")
            
            response = self.session.get(
                api_url,
                headers=self.headers,
                params=params,
                timeout=15
            )
            
            print(f"→ Response status: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    # Handle Brotli compression manually if needed
                    content_encoding = response.headers.get('Content-Encoding', '')
                    print(f"→ Content encoding: {content_encoding}")
                    
                    if content_encoding == 'br' and response.content[:2] != b'{[':
                        # Manually decompress Brotli
                        try:
                            decompressed = brotli.decompress(response.content)
                            data = json.loads(decompressed.decode('utf-8'))
                            print(f"✓ Brotli decompression successful")
                        except:
                            # Fall back to response.json()
                            data = response.json()
                    else:
                        data = response.json()
                    
                    print(f"✓ Data fetched successfully")
                    print(f"→ Response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
                    
                    if isinstance(data, dict):
                        if 'data' in data:
                            print(f"✓ Found {len(data['data'])} records")
                        elif data:
                            print(f"→ Data structure: {list(data.keys())[:10]}")
                    elif isinstance(data, list):
                        print(f"✓ Found {len(data)} records (direct list)")
                    
                    return data
                    
                except Exception as e:
                    print(f"✗ Error parsing JSON: {e}")
                    return None
            else:
                print(f"✗ Error: Status code {response.status_code}")
                return None
                
        except Exception as e:
            print(f"✗ Error fetching data: {e}")
            return None
    
    def parse_to_dataframe(self, data):
        """Convert JSON data to pandas DataFrame"""
        if not data:
            print("✗ No data available in response")
            return None
        
        # Handle different response structures
        records_list = None
        
        if isinstance(data, dict):
            if 'data' in data:
                records_list = data['data']
            elif 'records' in data:
                records_list = data['records']
            else:
                # Print structure for debugging
                print(f"→ Available keys: {list(data.keys())}")
                # Try to find the list of records
                for key, value in data.items():
                    if isinstance(value, list) and len(value) > 0:
                        print(f"→ Found list in key '{key}' with {len(value)} items")
                        records_list = value
                        break
        elif isinstance(data, list):
            records_list = data
        
        if not records_list:
            print("✗ Could not find records in response")
            return None
        
        records = []
        for item in records_list:
            record = {
                'Symbol': item.get('symbol', ''),
                'Company': item.get('sm_name', ''),
                'Subject': item.get('desc', ''),
                'Description': item.get('attchmntText', ''),
                'Attachment_URL': item.get('attchmntFile', ''),
                'File_Size': item.get('sm_size', ''),
                'Timestamp': item.get('an_dt', ''),
                'XBRL_Link': item.get('xbrl', '')
            }
            records.append(record)
        
        df = pd.DataFrame(records)
        return df
    
    def download_pdf(self, pdf_url, filename):
        """Download PDF attachment"""
        try:
            if not pdf_url.startswith('http'):
                pdf_url = self.base_url + pdf_url
            
            response = self.session.get(pdf_url, headers=self.headers, timeout=15)
            
            if response.status_code == 200:
                with open(filename, 'wb') as f:
                    f.write(response.content)
                print(f"✓ Downloaded: {filename}")
                return True
            else:
                print(f"✗ Failed to download: {response.status_code}")
                return False
        except Exception as e:
            print(f"✗ Error downloading PDF: {e}")
            return False


def main():
    print("="*80)
    print("NSE Corporate Filings Scraper")
    print("="*80 + "\n")
    
    scraper = NSEScraper()
    
    # Get today's date
    today = datetime.now().strftime("%d-%m-%Y")
    
    print("Fetching today's corporate announcements...")
    print("-" * 80)
    
    # Try to fetch data
    data = scraper.fetch_corporate_filings(
        index="equities",
        from_date=today,
        to_date=today
    )
    
    if data:
        df = scraper.parse_to_dataframe(data)
        
        if df is not None and not df.empty:
            print("\n" + "="*80)
            print(f"✓ SUCCESS: Found {len(df)} announcements")
            print("="*80 + "\n")
            
            # Display first 10 rows
            print("Sample Data:")
            print("-" * 80)
            display_cols = ['Symbol', 'Company', 'Subject', 'Timestamp']
            print(df[display_cols].head(10).to_string(index=False))
            
            # Save to CSV
            csv_filename = f"nse_filings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
            print(f"\n✓ Full data saved to: {csv_filename}")
            
            # Save to Excel (optional)
            try:
                excel_filename = csv_filename.replace('.csv', '.xlsx')
                df.to_excel(excel_filename, index=False, engine='openpyxl')
                print(f"✓ Excel file saved to: {excel_filename}")
            except ImportError:
                print("(Install openpyxl for Excel export: pip install openpyxl)")
            except Exception as e:
                print(f"(Excel export failed: {e})")
            
            # Offer to download a PDF
            if not df.empty and df.iloc[0]['Attachment_URL']:
                print(f"\n→ First announcement PDF available")
                response = input("Download first PDF? (y/n): ").lower()
                if response == 'y':
                    pdf_url = df.iloc[0]['Attachment_URL']
                    pdf_filename = f"{df.iloc[0]['Symbol']}_announcement.pdf"
                    scraper.download_pdf(pdf_url, pdf_filename)
            
            return df
        else:
            print("\n✗ No announcements found")
            return None
    else:
        print("\n" + "="*80)
        print("✗ FAILED: Could not fetch data from NSE")
        print("="*80)
        print("\nTrying alternative: Install brotli library")
        print("Run: pip install brotli")
        print("\nOr use Selenium method for 100% reliability")
        return None


if __name__ == "__main__":
    main()