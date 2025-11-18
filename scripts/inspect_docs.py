#!/usr/bin/env python3
"""Small helper to inspect documents created by the scrapers.

Usage (PowerShell):
  $env:MONGO_URI = 'mongodb+srv://user:pass@cluster0...'
  python .\scripts\inspect_docs.py --id "GK Energy Limited"

Or pass a URI directly:
  python .\scripts\inspect_docs.py --id "GK Energy Limited" --uri "mongodb+srv://..."

This prints `last_hour.latest`, the `company-map` doc and the `summary-map` doc
for the provided company id.
"""
import argparse
import json
import sys
from pymongo import MongoClient
from bson import json_util


def main():
    parser = argparse.ArgumentParser(description='Inspect last_hour/company-map/summary-map for a company id')
    parser.add_argument('--uri', help='MongoDB URI (overrides MONGO_URI env var)')
    parser.add_argument('--id', required=True, help='Company _id to inspect (e.g. "GK Energy Limited")')
    args = parser.parse_args()

    mongo_uri = args.uri or (sys.environ.get('MONGO_URI') if hasattr(sys, 'environ') else None)
    # Attempt os.environ as fallback
    if not mongo_uri:
        try:
            import os
            mongo_uri = os.environ.get('MONGO_URI') or os.environ.get('MONGODB_URI')
        except Exception:
            mongo_uri = None

    if not mongo_uri:
        print('ERROR: MongoDB URI not provided. Set MONGO_URI env var or pass --uri')
        sys.exit(2)

    try:
        client = MongoClient(mongo_uri)
        db = client['nse_data']
    except Exception as e:
        print(f'ERROR: Could not connect to MongoDB: {e}')
        sys.exit(3)

    cid = args.id

    def pretty(obj):
        if obj is None:
            return '<not found>'
        return json.dumps(obj, default=json_util.default, indent=2)

    print('last_hour.latest:')
    try:
        print(pretty(db['last_hour'].find_one({'_id': cid}, {'latest': 1})))
    except Exception as e:
        print(f'  error reading last_hour: {e}')

    print('\ncompany-map:')
    try:
        print(pretty(db['company-map'].find_one({'_id': cid})))
    except Exception as e:
        print(f'  error reading company-map: {e}')

    print('\nsummary-map:')
    try:
        print(pretty(db['summary-map'].find_one({'_id': cid})))
    except Exception as e:
        print(f'  error reading summary-map: {e}')


if __name__ == '__main__':
    main()
