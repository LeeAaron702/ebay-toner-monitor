#!/usr/bin/env python3
"""
Compare Database vs Buying Sheet - Comprehensive Audit
"""

import csv
import sqlite3
from pathlib import Path

def main():
    # Read database products
    db_path = Path(__file__).parent.parent / 'database.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT asin, brand, model, color, capacity, variant_label, pack_size, sellable FROM products')
    db_products = {}
    for row in cursor.fetchall():
        db_products[row[0]] = {
            'asin': row[0], 
            'brand': row[1], 
            'model': row[2], 
            'color': row[3], 
            'capacity': row[4], 
            'variant_label': row[5], 
            'pack_size': row[6], 
            'sellable': row[7]
        }
    conn.close()

    # Read buying sheet
    buying_sheet_path = '/Users/lee/Downloads/Buying sheet - Analyzer Input.csv'
    buying_sheet = {}
    with open(buying_sheet_path, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip first header
        next(reader)  # Skip second header
        for row in reader:
            if row[0] and row[0].startswith('B'):
                asin = row[0]
                buying_sheet[asin] = {
                    'asin': asin,
                    'sku': row[2] if len(row) > 2 else '',
                    'sellable_status': row[3] if len(row) > 3 else '',
                    'brand': row[4] if len(row) > 4 else '',
                    'model': row[5] if len(row) > 5 else '',
                    'color': row[6] if len(row) > 6 else '',
                    'overstock': row[14] if len(row) > 14 else 'FALSE'
                }

    print("=" * 80)
    print("DATABASE vs BUYING SHEET COMPARISON AUDIT")
    print("=" * 80)
    print(f"\nTotal ASINs in Database: {len(db_products)}")
    print(f"Total ASINs in Buying Sheet: {len(buying_sheet)}")
    
    # Find ASINs in buying sheet but not in database (MISSING)
    missing_from_db = set(buying_sheet.keys()) - set(db_products.keys())
    print(f"\n{'=' * 80}")
    print(f"MISSING FROM DATABASE: {len(missing_from_db)} ASINs")
    print("=" * 80)
    for asin in sorted(missing_from_db):
        bs = buying_sheet[asin]
        print(f"  {asin} | {bs['brand']:<15} | {bs['model']:<10} | {bs['color']:<25} | Status: {bs['sellable_status']}")

    # Find ASINs in database but not in buying sheet (EXTRA)
    extra_in_db = set(db_products.keys()) - set(buying_sheet.keys())
    print(f"\n{'=' * 80}")
    print(f"IN DATABASE BUT NOT IN BUYING SHEET: {len(extra_in_db)} ASINs")
    print("=" * 80)
    for asin in sorted(extra_in_db):
        db = db_products[asin]
        print(f"  {asin} | {db['brand']:<15} | {db['model']:<10} | {db['color']:<25}")

    # Check sellable status mismatches
    print(f"\n{'=' * 80}")
    print("SELLABLE STATUS MISMATCHES")
    print("=" * 80)
    mismatches = []
    for asin in set(buying_sheet.keys()) & set(db_products.keys()):
        bs = buying_sheet[asin]
        db = db_products[asin]
        
        # Determine expected sellable from buying sheet
        # Blocked, Unsellable, or Overstock = not sellable (0)
        # Sellable = sellable (1)
        bs_status = bs['sellable_status'].lower()
        bs_overstock = bs['overstock'].upper() == 'TRUE'
        
        expected_sellable = 1
        if bs_status in ['blocked', 'unsellable'] or bs_overstock:
            expected_sellable = 0
        
        actual_sellable = db['sellable']
        
        if expected_sellable != actual_sellable:
            reason = bs_status
            if bs_overstock:
                reason = 'overstock'
            mismatches.append({
                'asin': asin,
                'brand': db['brand'],
                'model': db['model'],
                'color': db['color'],
                'bs_status': bs_status,
                'bs_overstock': bs_overstock,
                'expected': expected_sellable,
                'actual': actual_sellable,
                'reason': reason
            })
    
    print(f"Total mismatches: {len(mismatches)}")
    print()
    
    # Items that should be NOT sellable but are marked sellable
    should_be_blocked = [m for m in mismatches if m['expected'] == 0]
    print(f"\n--- Should be BLOCKED but marked SELLABLE: {len(should_be_blocked)} ---")
    for m in should_be_blocked:
        print(f"  {m['asin']} | {m['brand']:<15} | {m['model']:<10} | {m['color']:<20} | Reason: {m['reason']}")
    
    # Items that should be sellable but are marked blocked
    should_be_sellable = [m for m in mismatches if m['expected'] == 1]
    print(f"\n--- Should be SELLABLE but marked BLOCKED: {len(should_be_sellable)} ---")
    for m in should_be_sellable:
        print(f"  {m['asin']} | {m['brand']:<15} | {m['model']:<10} | {m['color']:<20}")

    # Summary
    print(f"\n{'=' * 80}")
    print("SUMMARY OF REQUIRED CHANGES")
    print("=" * 80)
    print(f"1. ADD to database: {len(missing_from_db)} ASINs")
    print(f"2. Update sellable to 0 (blocked): {len(should_be_blocked)} items")
    print(f"3. Update sellable to 1 (sellable): {len(should_be_sellable)} items")
    print(f"4. Extra in DB (not in buying sheet - review): {len(extra_in_db)} items")

if __name__ == '__main__':
    main()
