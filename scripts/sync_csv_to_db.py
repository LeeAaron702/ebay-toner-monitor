#!/usr/bin/env python3
"""
Sync products_audit.csv to database.
Updates sellable status and notes for all products.
"""
import csv
import sqlite3
import os

def main():
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    
    csv_path = os.path.join(project_dir, 'exports', 'products_audit.csv')
    db_path = os.path.join(project_dir, 'database.db')
    
    print(f'CSV path: {csv_path}')
    print(f'DB path: {db_path}')
    
    # Read CSV
    products = []
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            products.append(row)

    print(f'Loaded {len(products)} products from CSV')

    # Connect to DB
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Check current state
    cursor = conn.execute('SELECT COUNT(*) as cnt FROM products WHERE sellable = 0')
    print(f'Products with sellable=0 in DB before: {cursor.fetchone()["cnt"]}')

    # Update products by ASIN
    updated = 0

    for p in products:
        asin = p.get('asin')
        brand = p.get('brand', '').lower()
        sellable = int(p.get('sellable', 1))
        notes = p.get('notes') or None
        
        if not asin or not brand:
            continue
        
        cursor = conn.execute('''
            UPDATE products 
            SET sellable = ?,
                notes = ?
            WHERE asin = ? AND brand = ?
        ''', (sellable, notes, asin, brand))
        
        if cursor.rowcount > 0:
            updated += 1

    conn.commit()

    # Check state after
    cursor = conn.execute('SELECT COUNT(*) as cnt FROM products WHERE sellable = 0')
    print(f'Products with sellable=0 in DB after: {cursor.fetchone()["cnt"]}')
    print(f'Updated: {updated}')
    
    # Verify specific ASIN
    cursor = conn.execute('SELECT asin, sellable FROM products WHERE asin = ?', ('B06XY3WRVQ',))
    row = cursor.fetchone()
    if row:
        print(f'B06XY3WRVQ sellable = {row["sellable"]}')

    conn.close()
    print('Done!')

if __name__ == '__main__':
    main()
