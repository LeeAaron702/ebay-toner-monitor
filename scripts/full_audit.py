#!/usr/bin/env python3
"""
Comprehensive Product Database Audit
Compares database against all three buying sheets (Canon, Xerox, Lexmark)
"""

import csv
import sqlite3
from pathlib import Path
from collections import defaultdict

# Paths
DB_PATH = Path("/Users/lee/Downloads/Proco Ebay Notifications Telelgram/database.db")
CANON_SHEET = Path("/Users/lee/Downloads/Buying sheet - Canon Printout (1).csv")
XEROX_SHEET = Path("/Users/lee/Downloads/Buying sheet - Xerox Printout.csv")
LEXMARK_SHEET = Path("/Users/lee/Downloads/Buying sheet - Lexmark Printout.csv")

def get_db_products():
    """Get all products from database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT asin, brand, model, color, capacity, variant_label, 
               pack_size, sellable, amazon_sku, part_number
        FROM products
    ''')
    products = {}
    for row in cursor.fetchall():
        products[row[0]] = {
            'asin': row[0],
            'brand': row[1],
            'model': row[2],
            'color': row[3],
            'capacity': row[4],
            'variant_label': row[5],
            'pack_size': row[6],
            'sellable': row[7],
            'amazon_sku': row[8],
            'part_number': row[9]
        }
    conn.close()
    return products

def parse_buying_sheet(filepath, brand):
    """Parse a buying sheet and extract ASIN-SKU-Sellable data."""
    products = {}
    
    if not filepath.exists():
        print(f"  WARNING: {filepath} not found")
        return products
    
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        rows = list(reader)
    
    i = 0
    while i < len(rows):
        row = rows[i]
        
        # Look for ASIN row
        if row and row[0] == 'ASIN':
            asin_row = row
            sku_row = rows[i + 1] if i + 1 < len(rows) and rows[i + 1][0] == 'SKU' else None
            
            # Find BSR and Sellable rows
            bsr_row = None
            sellable_row = None
            for j in range(i + 1, min(i + 5, len(rows))):
                if rows[j] and rows[j][0] == 'BSR':
                    bsr_row = rows[j]
                if rows[j] and rows[j][0] == 'Sellable?':
                    sellable_row = rows[j]
            
            # Process each column
            for col_idx in range(1, len(asin_row)):
                asin = asin_row[col_idx].strip() if col_idx < len(asin_row) else ''
                
                # Skip invalid entries
                if not asin or asin == 'N/A' or asin == 'No AMZ Listing' or not asin.startswith('B'):
                    continue
                
                sku = ''
                if sku_row and col_idx < len(sku_row):
                    sku = sku_row[col_idx].strip()
                    if sku in ['No AMZ Listing', 'N/A']:
                        sku = ''
                
                sellable_status = 'Sellable'
                if sellable_row and col_idx < len(sellable_row):
                    status = sellable_row[col_idx].strip()
                    if status in ['Blocked', 'Unsellable', 'Overstock']:
                        sellable_status = status
                    elif status == 'No AMZ Listing':
                        continue  # Skip if no AMZ listing
                
                products[asin] = {
                    'asin': asin,
                    'sku': sku,
                    'sellable_status': sellable_status,
                    'brand': brand
                }
            
            i += 4  # Skip past the block
        else:
            i += 1
    
    return products

def normalize_sellable(db_value, sheet_value):
    """Normalize sellable values for comparison."""
    db_sellable = bool(db_value)
    sheet_sellable = sheet_value in ['Sellable']
    return db_sellable, sheet_sellable

def main():
    print("=" * 80)
    print("COMPREHENSIVE PRODUCT DATABASE AUDIT")
    print("=" * 80)
    
    # Get database products
    print("\n1. Loading database products...")
    db_products = get_db_products()
    print(f"   Found {len(db_products)} products in database")
    
    # Count by brand
    brand_counts = defaultdict(int)
    for p in db_products.values():
        brand_counts[p['brand']] += 1
    for brand, count in sorted(brand_counts.items()):
        print(f"   - {brand}: {count}")
    
    # Parse buying sheets
    print("\n2. Parsing buying sheets...")
    
    canon_products = parse_buying_sheet(CANON_SHEET, 'canon')
    print(f"   Canon: {len(canon_products)} ASINs")
    
    xerox_products = parse_buying_sheet(XEROX_SHEET, 'xerox')
    print(f"   Xerox: {len(xerox_products)} ASINs")
    
    lexmark_products = parse_buying_sheet(LEXMARK_SHEET, 'lexmark')
    print(f"   Lexmark: {len(lexmark_products)} ASINs")
    
    # Combine all buying sheet products
    all_sheet_products = {}
    all_sheet_products.update(canon_products)
    all_sheet_products.update(xerox_products)
    all_sheet_products.update(lexmark_products)
    print(f"\n   Total in buying sheets: {len(all_sheet_products)} ASINs")
    
    # === AUDIT CHECKS ===
    
    print("\n" + "=" * 80)
    print("AUDIT RESULTS")
    print("=" * 80)
    
    # Check 1: ASINs in buying sheets but not in database
    print("\n3. ASINs in buying sheets but MISSING from database:")
    missing_from_db = []
    for asin, data in all_sheet_products.items():
        if asin not in db_products:
            missing_from_db.append(data)
    
    if missing_from_db:
        by_brand = defaultdict(list)
        for p in missing_from_db:
            by_brand[p['brand']].append(p)
        
        for brand in ['canon', 'xerox', 'lexmark']:
            if by_brand[brand]:
                print(f"\n   {brand.upper()} ({len(by_brand[brand])} missing):")
                for p in by_brand[brand][:10]:
                    print(f"   - {p['asin']}: {p.get('sku', 'no sku')}")
                if len(by_brand[brand]) > 10:
                    print(f"   ... and {len(by_brand[brand]) - 10} more")
    else:
        print("   ✓ None - all buying sheet ASINs are in database")
    
    # Check 2: ASINs in database but not in buying sheets
    print("\n4. ASINs in database but NOT in buying sheets:")
    extra_in_db = []
    for asin, data in db_products.items():
        if asin not in all_sheet_products:
            extra_in_db.append(data)
    
    if extra_in_db:
        by_brand = defaultdict(list)
        for p in extra_in_db:
            by_brand[p['brand']].append(p)
        
        for brand in ['canon', 'xerox', 'lexmark']:
            if by_brand[brand]:
                print(f"\n   {brand.upper()} ({len(by_brand[brand])} extra):")
                for p in by_brand[brand][:10]:
                    print(f"   - {p['asin']}: {p.get('variant_label', p.get('part_number', 'unknown'))}")
                if len(by_brand[brand]) > 10:
                    print(f"   ... and {len(by_brand[brand]) - 10} more")
    else:
        print("   ✓ None - database matches buying sheets exactly")
    
    # Check 3: Sellable status mismatches
    print("\n5. Sellable status MISMATCHES:")
    sellable_mismatches = []
    for asin in db_products:
        if asin in all_sheet_products:
            db_sellable = bool(db_products[asin]['sellable'])
            sheet_status = all_sheet_products[asin]['sellable_status']
            sheet_sellable = sheet_status == 'Sellable'
            
            if db_sellable != sheet_sellable:
                sellable_mismatches.append({
                    'asin': asin,
                    'brand': db_products[asin]['brand'],
                    'variant': db_products[asin].get('variant_label') or db_products[asin].get('part_number'),
                    'db_sellable': db_sellable,
                    'sheet_status': sheet_status
                })
    
    if sellable_mismatches:
        print(f"   Found {len(sellable_mismatches)} mismatches:\n")
        
        # Group by type
        db_says_sellable = [m for m in sellable_mismatches if m['db_sellable']]
        db_says_blocked = [m for m in sellable_mismatches if not m['db_sellable']]
        
        if db_says_sellable:
            print(f"   DB=Sellable but Sheet=Blocked ({len(db_says_sellable)}):")
            for m in db_says_sellable[:15]:
                print(f"   - [{m['brand']}] {m['asin']}: {m['variant']} (sheet: {m['sheet_status']})")
            if len(db_says_sellable) > 15:
                print(f"   ... and {len(db_says_sellable) - 15} more")
        
        if db_says_blocked:
            print(f"\n   DB=Blocked but Sheet=Sellable ({len(db_says_blocked)}):")
            for m in db_says_blocked[:15]:
                print(f"   - [{m['brand']}] {m['asin']}: {m['variant']} (sheet: {m['sheet_status']})")
            if len(db_says_blocked) > 15:
                print(f"   ... and {len(db_says_blocked) - 15} more")
    else:
        print("   ✓ All sellable statuses match")
    
    # Check 4: SKU mismatches (Canon only - we just updated these)
    print("\n6. SKU status check (Canon):")
    canon_missing_sku = []
    canon_sku_mismatch = []
    
    for asin in db_products:
        if db_products[asin]['brand'] == 'canon' and asin in canon_products:
            db_sku = db_products[asin].get('amazon_sku') or ''
            sheet_sku = canon_products[asin].get('sku') or ''
            
            if not db_sku and sheet_sku:
                canon_missing_sku.append({
                    'asin': asin,
                    'sheet_sku': sheet_sku,
                    'variant': db_products[asin].get('variant_label')
                })
            elif db_sku and sheet_sku and db_sku != sheet_sku:
                canon_sku_mismatch.append({
                    'asin': asin,
                    'db_sku': db_sku,
                    'sheet_sku': sheet_sku
                })
    
    if canon_missing_sku:
        print(f"   Missing SKUs in DB: {len(canon_missing_sku)}")
        for m in canon_missing_sku[:5]:
            print(f"   - {m['asin']}: should be '{m['sheet_sku']}'")
    else:
        print("   ✓ All Canon products have SKUs")
    
    if canon_sku_mismatch:
        print(f"   SKU mismatches: {len(canon_sku_mismatch)}")
        for m in canon_sku_mismatch[:5]:
            print(f"   - {m['asin']}: DB='{m['db_sku']}' vs Sheet='{m['sheet_sku']}'")
    
    # Check 5: Database integrity
    print("\n7. Database integrity checks:")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check for null ASINs
    cursor.execute("SELECT COUNT(*) FROM products WHERE asin IS NULL OR asin = ''")
    null_asins = cursor.fetchone()[0]
    print(f"   - Null/empty ASINs: {null_asins}")
    
    # Check for duplicate ASINs
    cursor.execute("SELECT asin, COUNT(*) as cnt FROM products GROUP BY asin HAVING cnt > 1")
    dupes = cursor.fetchall()
    print(f"   - Duplicate ASINs: {len(dupes)}")
    if dupes:
        for d in dupes[:5]:
            print(f"     {d[0]}: {d[1]} occurrences")
    
    # Check sellable distribution
    cursor.execute("SELECT sellable, COUNT(*) FROM products GROUP BY sellable")
    sellable_dist = cursor.fetchall()
    print(f"   - Sellable distribution:")
    for status, count in sellable_dist:
        label = "Sellable" if status else "Blocked"
        print(f"     {label}: {count}")
    
    conn.close()
    
    # Summary
    print("\n" + "=" * 80)
    print("AUDIT SUMMARY")
    print("=" * 80)
    
    total_issues = len(missing_from_db) + len(sellable_mismatches)
    
    print(f"\n   Database products: {len(db_products)}")
    print(f"   Buying sheet products: {len(all_sheet_products)}")
    print(f"   Missing from DB: {len(missing_from_db)}")
    print(f"   Extra in DB: {len(extra_in_db)}")
    print(f"   Sellable mismatches: {len(sellable_mismatches)}")
    
    if total_issues == 0:
        print("\n   ✅ DATABASE IS FULLY SYNCHRONIZED")
    else:
        print(f"\n   ⚠️  {total_issues} issues found - review above")
    
    print("\n" + "=" * 80)

if __name__ == "__main__":
    main()
