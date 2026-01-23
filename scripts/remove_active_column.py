#!/usr/bin/env python3
"""Remove the 'active' column from the products table."""

import sqlite3
from pathlib import Path

DB_PATH = Path("/Users/lee/Downloads/Proco Ebay Notifications Telelgram/database.db")

def migrate():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("1. Creating new table without 'active' column...")
    cursor.execute('''
        CREATE TABLE products_new (
            id TEXT PRIMARY KEY,
            brand TEXT NOT NULL CHECK (brand IN ('canon', 'xerox', 'lexmark')),
            model TEXT,
            capacity TEXT,
            group_key TEXT,
            part_number TEXT,
            variant_label TEXT,
            color TEXT,
            pack_size INTEGER DEFAULT 1,
            asin TEXT,
            amazon_sku TEXT,
            net_cost REAL,
            bsr INTEGER,
            sellable INTEGER DEFAULT 1,
            source_tab TEXT,
            is_model_block INTEGER,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now')),
            UNIQUE(brand, asin)
        )
    ''')
    
    print("2. Copying data...")
    cursor.execute('''
        INSERT INTO products_new 
        SELECT id, brand, model, capacity, group_key, part_number, variant_label, 
               color, pack_size, asin, amazon_sku, net_cost, bsr, sellable, 
               source_tab, is_model_block, created_at, updated_at 
        FROM products
    ''')
    
    count = cursor.execute('SELECT COUNT(*) FROM products_new').fetchone()[0]
    print(f"   Copied {count} products")
    
    print("3. Dropping old table...")
    cursor.execute('DROP TABLE products')
    
    print("4. Renaming new table...")
    cursor.execute('ALTER TABLE products_new RENAME TO products')
    
    print("5. Recreating indexes...")
    cursor.execute('CREATE INDEX idx_products_brand ON products(brand)')
    cursor.execute('CREATE INDEX idx_products_model ON products(model)')
    cursor.execute('CREATE INDEX idx_products_part_number ON products(part_number)')
    cursor.execute('CREATE INDEX idx_products_part_number_lower ON products(lower(part_number))')
    cursor.execute('CREATE INDEX idx_products_asin ON products(asin)')
    cursor.execute('CREATE INDEX idx_products_group_key ON products(group_key)')
    cursor.execute('CREATE INDEX idx_products_canon_match ON products(brand, model, capacity, color, pack_size)')
    
    conn.commit()
    conn.close()
    
    print("\nDone! 'active' column has been removed from the database.")

if __name__ == "__main__":
    migrate()
