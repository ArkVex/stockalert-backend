#!/usr/bin/env python3
import os
from pymongo import MongoClient

# Load env
with open('.env.local', 'r') as f:
    for line in f:
        line = line.strip()
        if '=' in line and not line.startswith('#'):
            key, val = line.split('=', 1)
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key:
                os.environ[key] = val

uri = os.environ.get('MONGODB_URI')
print(f"URI: {uri[:50]}...")

client = MongoClient(uri)
db = client['nse_data']

print("\nCollections:")
for coll_name in db.list_collection_names():
    count = db[coll_name].count_documents({})
    print(f"  {coll_name}: {count} documents")
    
    # Check if this might be contacts
    if count > 0:
        sample = db[coll_name].find_one()
        if sample and ('phone' in sample or 'mobile' in sample):
            print(f"    -> Found phone/mobile field! Sample keys: {list(sample.keys())[:10]}")
