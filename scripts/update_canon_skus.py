#!/usr/bin/env python3
"""
Extract SKUs from Canon buying sheet and update database.
The buying sheet has a complex multi-column format that needs to be flattened.
"""

import csv
import sqlite3
from pathlib import Path

# Paths
BUYING_SHEET = Path("/Users/lee/Downloads/Buying sheet - Canon Printout.csv")
DB_PATH = Path("/Users/lee/Downloads/Proco Ebay Notifications Telelgram/database.db")

def parse_canon_buying_sheet():
    """Parse the Canon buying sheet and extract ASIN-SKU pairs."""
    asin_sku_pairs = {}
    
    with open(BUYING_SHEET, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        rows = list(reader)
    
    i = 0
    while i < len(rows):
        row = rows[i]
        
        # Look for ASIN row
        if row and row[0] == 'ASIN':
            # Next row should be SKU
            if i + 1 < len(rows) and rows[i + 1][0] == 'SKU':
                asin_row = row
                sku_row = rows[i + 1]
                
                # Process each column (skip first column which is the label)
                for col_idx in range(1, min(len(asin_row), len(sku_row))):
                    asin = asin_row[col_idx].strip() if col_idx < len(asin_row) else ''
                    sku = sku_row[col_idx].strip() if col_idx < len(sku_row) else ''
                    
                    # Skip invalid entries
                    if not asin or asin == 'N/A' or asin == 'No AMZ Listing':
                        continue
                    if not sku or sku == 'No AMZ Listing' or sku == 'N/A':
                        continue
                    
                    # Valid ASIN starts with B0
                    if asin.startswith('B0'):
                        asin_sku_pairs[asin] = sku
                        
                i += 2  # Skip past both rows
            else:
                i += 1
        else:
            i += 1
    
    return asin_sku_pairs

def update_database(asin_sku_pairs):
    """Update the database with SKUs."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get existing Canon products
    cursor.execute("SELECT asin, amazon_sku FROM products WHERE brand = 'canon'")
    existing = {row[0]: row[1] for row in cursor.fetchall()}
    
    updated = 0
    not_found = []
    already_set = 0
    
    for asin, sku in asin_sku_pairs.items():
        if asin in existing:
            current_sku = existing[asin]
            if current_sku != sku:
                cursor.execute(
                    "UPDATE products SET amazon_sku = ? WHERE asin = ?",
                    (sku, asin)
                )
                updated += 1
                print(f"  Updated {asin}: '{current_sku}' -> '{sku}'")
            else:
                already_set += 1
        else:
            not_found.append(asin)
    
    conn.commit()
    conn.close()
    
    return updated, already_set, not_found

def main():
    print("=" * 60)
    print("CANON SKU EXTRACTION AND UPDATE")
    print("=" * 60)
    
    # Parse buying sheet
    print("\n1. Parsing Canon buying sheet...")
    asin_sku_pairs = parse_canon_buying_sheet()
    print(f"   Found {len(asin_sku_pairs)} ASIN-SKU pairs")
    
    # Show sample
    print("\n   Sample pairs:")
    for i, (asin, sku) in enumerate(list(asin_sku_pairs.items())[:5]):
        print(f"   - {asin}: {sku}")
    
    # Update database
    print("\n2. Updating database...")
    updated, already_set, not_found = update_database(asin_sku_pairs)
    
    print(f"\n3. Results:")
    print(f"   - Updated: {updated}")
    print(f"   - Already set: {already_set}")
    print(f"   - ASINs not in DB: {len(not_found)}")
    
    if not_found:
        print(f"\n   ASINs from buying sheet not in database:")
        for asin in not_found[:10]:
            print(f"   - {asin}")
        if len(not_found) > 10:
            print(f"   ... and {len(not_found) - 10} more")
    
    print("\n" + "=" * 60)
    print("DONE")
    print("=" * 60)

if __name__ == "__main__":
    main()
