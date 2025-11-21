import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime
import json
import brotli  # For Brotli decompression
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError
import os
import sys

# Force UTF-8 encoding for stdout/stderr to prevent UnicodeEncodeError on Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
from urllib.parse import quote_plus

class NSEScraper:
    def __init__(self, mongo_uri=None, db_password=None):
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
        
        # MongoDB setup
        self.mongo_client = None
        self.db = None
        self.collection = None
        
        if mongo_uri and db_password:
            self.setup_mongodb(mongo_uri, db_password)
        
    def setup_mongodb(self, mongo_uri, db_password):
        """Setup MongoDB connection"""
        try:
            # URL encode the password to handle special characters
            encoded_password = quote_plus(db_password)
            
            # Replace password in URI
            connection_string = mongo_uri.replace('<db_password>', encoded_password)
            
            # Connect to MongoDB
            self.mongo_client = MongoClient(connection_string)
            
            # Test connection
            self.mongo_client.admin.command('ping')
            print("✓ MongoDB connection successful")
            
            # Select database and collection. Use company-keyed collection by default.
            self.db = self.mongo_client['nse_data']  # Database name
            # Use the new collection requested: 'company-map'
            self.collection = self.db['company-map']  # Collection name
            print(f"✓ Using collection: {self.db.name}.{self.collection.name}")
            
            return True
            
        except ConnectionFailure as e:
            print(f"✗ MongoDB connection failed: {e}")
            return False
        except Exception as e:
            print(f"✗ Error setting up MongoDB: {e}")
            return False
    
    def record_exists(self, symbol, timestamp):
        """Check if a record already exists in MongoDB"""
        if self.collection is None:
            return False
        
        try:
            existing = self.collection.find_one({
                'Symbol': symbol,
                'Timestamp': timestamp
            })
            return existing is not None
        except Exception as e:
            print(f"✗ Error checking record existence: {e}")
            return False
    
    def save_to_mongodb(self, df):
        """Save DataFrame to MongoDB - only insert if not already present"""
        if self.collection is None:
            print("✗ MongoDB not configured. Cannot save data.")
            return False
        
        try:
            if df is None or df.empty:
                print("✗ No data to save to MongoDB")
                return False
            
            # Convert DataFrame to list of dictionaries
            records = df.to_dict('records')

            upserted = 0
            errors = 0

            print(f"\n→ Processing {len(records)} records (company-keyed upserts)...")

            for record in records:
                try:
                    company = record.get('Company') or record.get('sm_name') or record.get('company') or 'Unknown'
                    if not company:
                        company = 'Unknown'

                    announcement = {
                        'Symbol': record.get('Symbol') or record.get('symbol', ''),
                        'Subject': record.get('Subject') or record.get('desc', ''),
                        'Description': record.get('Description') or record.get('attchmntText', ''),
                        'Attachment_URL': record.get('Attachment_URL') or record.get('attchmntFile', ''),
                        'File_Size': record.get('File_Size') or record.get('sm_size', ''),
                        'Timestamp': record.get('Timestamp') or record.get('an_dt', ''),
                        'XBRL_Link': record.get('XBRL_Link') or record.get('xbrl', ''),
                        'scraped_at': datetime.now()
                    }

                    # Upsert: replace the single announcement for the company
                    # Store it under 'announcement' so each company has only one announcement
                    res = self.collection.update_one(
                        {'_id': company},
                        {'$set': {'announcement': announcement, 'last_updated': datetime.now()}},
                        upsert=True
                    )

                    if getattr(res, 'modified_count', 0) > 0 or getattr(res, 'upserted_id', None):
                        upserted += 1

                except DuplicateKeyError as dk:
                    # A duplicate key error here most likely comes from existing unique
                    # constraints on other indexes. Log and continue.
                    errors += 1
                    print(f"✗ DuplicateKeyError upserting for '{company}': {dk}")
                except Exception as e:
                    errors += 1
                    print(f"✗ Error upserting announcement for company '{company}': {e}")

            print(f"\n{'='*80}")
            print("MongoDB Save Summary (company-keyed):")
            print(f"{'='*80}")
            print(f"✓ Upserted/Updated: {upserted}")
            if errors > 0:
                print(f"✗ Errors: {errors}")
            print(f"{'='*80}\n")

            return True
            
        except Exception as e:
            print(f"✗ Error saving to MongoDB: {e}")
            return False
    
    def get_records_from_mongodb(self, symbol=None, limit=10):
        """Retrieve records from MongoDB"""
        if self.collection is None:
            print("✗ MongoDB not configured")
            return None
        
        try:
            query = {}
            if symbol:
                query['Symbol'] = symbol
            
            records = list(self.collection.find(query).sort('Timestamp', -1).limit(limit))
            
            if records:
                # Remove MongoDB _id for display
                for record in records:
                    record.pop('_id', None)
                
                df = pd.DataFrame(records)
                return df
            else:
                print("✗ No records found in MongoDB")
                return None
                
        except Exception as e:
            print(f"✗ Error retrieving from MongoDB: {e}")
            return None
    
    def close_mongodb_connection(self):
        """Close MongoDB connection"""
        if self.mongo_client:
            self.mongo_client.close()
            print("✓ MongoDB connection closed")
        
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
                    
                    if isinstance(data, dict):
                        if 'data' in data:
                            print(f"✓ Found {len(data['data'])} records")
                    elif isinstance(data, list):
                        print(f"✓ Found {len(data)} records")
                    
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
                # Try to find the list of records
                for key, value in data.items():
                    if isinstance(value, list) and len(value) > 0:
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


def main():
    print("="*80)
    print("NSE Corporate Filings Scraper - Hourly Run")
    print(f"Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80 + "\n")
    
    # MongoDB credentials - password directly in URI
    MONGO_URI = "mongodb+srv://Arkvex:Ayushishan%401@cluster0.r5xgtsc.mongodb.net/"
    
    scraper = NSEScraper()
    
    # Connect to MongoDB
    try:
        from urllib.parse import quote_plus
        scraper.mongo_client = MongoClient(MONGO_URI)
        scraper.mongo_client.admin.command('ping')
        print("✓ MongoDB connection successful")

        scraper.db = scraper.mongo_client['nse_data']
        # Use the new default collection name requested by user
        scraper.collection = scraper.db['company-map']
        print(f"✓ Using collection: {scraper.db.name}.{scraper.collection.name}")
    except Exception as e:
        print(f"✗ MongoDB connection failed: {e}")
        return None
    
    if scraper.collection is None:
        print("✗ MongoDB connection failed. Cannot proceed.")
        return None
    
    # Get today's date (non-zero-padded day-month-year), e.g. 7-11-2025
    now = datetime.now()
    today = f"{now.day}-{now.month}-{now.year}"
    
    print(f"Fetching corporate announcements for {today}...")
    print("-" * 80)
    
    # Fetch data
    data = scraper.fetch_corporate_filings(
        index="equities",
        from_date=today,
        to_date=today
    )
    
    if data:
        df = scraper.parse_to_dataframe(data)
        
        if df is not None and not df.empty:
            print("\n" + "="*80)
            print(f"✓ SUCCESS: Found {len(df)} announcements from NSE")
            print("="*80 + "\n")
            
            # Display sample
            print("Sample Data (First 5 records):")
            print("-" * 80)
            display_cols = ['Symbol', 'Company', 'Subject', 'Timestamp']
            print(df[display_cols].head(5).to_string(index=False))
            print()

            # Create/replace a transient collection 'last_hour' that contains
            # the current scrape's latest announcement per company. We drop the
            # old collection and insert company-keyed documents for this run.
            try:
                last_coll_name = 'last_hour'
                # Drop existing last_hour collection if present
                if last_coll_name in scraper.db.list_collection_names():
                    scraper.db.drop_collection(last_coll_name)
                    print(f"→ Dropped existing transient collection: {last_coll_name}")

                last_coll = scraper.db[last_coll_name]

                records = df.to_dict('records')
                docs = []
                unknown_idx = 0
                for rec in records:
                    company = rec.get('Company') or rec.get('sm_name') or 'Unknown'
                    if not company:
                        company = f'Unknown_{unknown_idx}'
                        unknown_idx += 1

                    announcement = {
                        'Symbol': rec.get('Symbol') or rec.get('symbol', ''),
                        'Subject': rec.get('Subject') or rec.get('desc', ''),
                        'Description': rec.get('Description') or rec.get('attchmntText', ''),
                        'Attachment_URL': rec.get('Attachment_URL') or rec.get('attchmntFile', ''),
                        'File_Size': rec.get('File_Size') or rec.get('sm_size', ''),
                        'Timestamp': rec.get('Timestamp') or rec.get('an_dt', ''),
                        'XBRL_Link': rec.get('XBRL_Link') or rec.get('xbrl', ''),
                        'scraped_at': datetime.now()
                    }

                    docs.append({'_id': company, 'latest': announcement})

                if docs:
                    # Bulk insert (collection is new after drop)
                    last_coll.insert_many(docs)
                    print(f"✓ Inserted {len(docs)} documents into transient collection: {last_coll_name}")
                else:
                    print(f"→ No docs to insert into {last_coll_name}")
            except Exception as e:
                print(f"✗ Failed to refresh transient collection 'last_hour': {e}")

            # Save to MongoDB (only new records) into the main company-map style collection
            scraper.save_to_mongodb(df)
            
            # Close MongoDB connection
            scraper.close_mongodb_connection()
            
            print(f"✓ Scraping completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            return df
        else:
            print("\n✗ No announcements found for today")
            scraper.close_mongodb_connection()
            return None
    else:
        print("\n" + "="*80)
        print("✗ FAILED: Could not fetch data from NSE")
        print("="*80)
        scraper.close_mongodb_connection()
        return None


if __name__ == "__main__":
    main()