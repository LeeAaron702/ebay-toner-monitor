#!/usr/bin/env python3
"""
Apply Database Fixes from Audit
- Update sellable status for existing items
- Add missing products (staples, Canon blocked, Lexmark, Xerox)
"""

import sqlite3
import csv
from pathlib import Path

def main():
    db_path = Path(__file__).parent.parent / 'database.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("=" * 80)
    print("APPLYING DATABASE FIXES")
    print("=" * 80)
    
    # =========================================================================
    # PHASE 1: Update sellable status for existing items
    # =========================================================================
    print("\n--- PHASE 1: Updating sellable status ---\n")
    
    # Items that should be BLOCKED (sellable=0) - currently sellable=1
    should_be_blocked = [
        # Canon toner - blocked in buying sheet
        'B0041RRMQS',  # 128 Black
        'B001TOD3NM',  # 120 Black
        'B002GU5Y9E',  # 118 Yellow
        'B00BS6WWVA',  # 131 Cyan
        'B00BS6WYG8',  # 131 Black
        'B00S7K16MQ',  # 118 4 Color
        'B06XY1QTDP',  # 046 Yellow
        'B06XY2ZLVY',  # 046 Black
        'B06XY3FVCV',  # 046 Cyan
        'B06XXKNV2T',  # 046H Black
        'B06XXNXQ1L',  # 046H Magenta
        'B06XXVJ5Q4',  # 046 Magenta
        'B06XXZVXR6',  # 046H Yellow
        'B06XY3WRVQ',  # 046H Cyan
        'B06Y15LNX2',  # 045H Cyan
        'B06Y171NQT',  # 045 Magenta
        'B06Y176STB',  # 045H Yellow
        'B06Y1DTCNX',  # 045 Yellow
        'B06Y1MB5N6',  # 045 Cyan
        'B06Y1MPFX4',  # 045H Magenta
        'B06Y1MW1FR',  # 045H Black
        'B07N8LZ3R6',  # 121 Black
        'B07QDVH2RX',  # 054 Black
        'B07QF15RCH',  # 055 Cyan
        'B07QFYHY72',  # 054H Black
        'B07QFYJFRS',  # 054 Yellow
        'B07QG442C9',  # 055H Yellow (Unsellable)
        'B07QH3C22R',  # 054 Magenta
        'B07QH839HL',  # 055H Magenta
        'B07QJDC19D',  # 055H Black
        'B07QK9LF3H',  # 054 Cyan
        'B07QKGB3H5',  # 055H Cyan
        'B07QKG9KP7',  # 055 Yellow
        'B07QKNTRM3',  # 054H Yellow
        'B07QKPD5JC',  # 054H Cyan
        'B07QKWYBTQ',  # 055 Magenta
        'B09B3WP7TS',  # 045H 3 Color
        'B0BSR6DR4C',  # 069H Black
        'B0BSR6T22T',  # 069 Black
        'B0BSR7VC52',  # 069 Cyan
        # Xerox - overstock/unsellable
        'B006103DDY',  # 6700 Imaging Yellow (Overstock)
        'B0060S3GRU',  # 6700 Imaging Magenta (Overstock)
        'B0060TI1WO',  # 126K32220 Fuser (Overstock)
        'B00NOY7IP4',  # 5945 Black (Unsellable)
        'B07Q8WQW14',  # 6700 4 Color (Unsellable)
    ]
    
    for asin in should_be_blocked:
        cursor.execute("UPDATE products SET sellable = 0 WHERE asin = ?", (asin,))
        if cursor.rowcount > 0:
            print(f"  ✓ Set sellable=0 for {asin}")
        else:
            print(f"  - Skipped {asin} (not found)")
    
    # Items that should be SELLABLE (sellable=1) - currently sellable=0
    should_be_sellable = [
        'B00443J06E',  # Canon GPR-26 Black
        'B00SNDUPUG',  # Canon GPR-32 Yellow
    ]
    
    for asin in should_be_sellable:
        cursor.execute("UPDATE products SET sellable = 1 WHERE asin = ?", (asin,))
        if cursor.rowcount > 0:
            print(f"  ✓ Set sellable=1 for {asin}")
        else:
            print(f"  - Skipped {asin} (not found)")
    
    conn.commit()
    print(f"\nPhase 1 complete: Updated {len(should_be_blocked)} to blocked, {len(should_be_sellable)} to sellable")
    
    # =========================================================================
    # PHASE 2: Add missing products
    # =========================================================================
    print("\n--- PHASE 2: Adding missing products ---\n")
    
    # Products to add - format: (brand, asin, part_number, model, color, capacity, variant_label, pack_size, sellable)
    products_to_add = [
        # =====================================================================
        # CANON TONER - BLOCKED (sellable=0)
        # =====================================================================
        ('canon', 'B000B02BEM', '104', '104', 'Black', 'Standard', 'Black Standard', 1, 0),
        ('canon', 'B002GU5Y7Q', '118', '118', 'Black', 'Standard', 'Black Standard', 1, 0),
        ('canon', 'B002GU5Y80', '118', '118', 'Cyan', 'Standard', 'Cyan Standard', 1, 0),
        ('canon', 'B002GU5Y8K', '118', '118', 'Magenta', 'Standard', 'Magenta Standard', 1, 0),
        ('canon', 'B004HLOR1G', '125', '125', 'Black', 'Standard', 'Black Standard', 1, 0),
        ('canon', 'B007BNOX4S', 'GPR-31', 'GPR-31', 'Cyan', 'Standard', 'Cyan Standard', 1, 1),  # This one is sellable
        ('canon', 'B00N99DC8Q', '137', '137', 'Black', 'Standard', 'Black Standard', 1, 0),
        ('canon', 'B06XZ88THM', '045', '045', 'Black', 'Standard', 'Black Standard', 1, 0),
        ('canon', 'B07QH39PCP', '054H', '054H', 'Magenta', 'High', 'Magenta High', 1, 0),
        ('canon', 'B07T9G4L76', '055H', '055H', 'Black', 'High', '2 Black High', 2, 0),
        ('canon', 'B07WVFFTH7', '057H', '057H', 'Black', 'High', 'Black High', 1, 0),
        ('canon', 'B07WWK22KW', '057', '057', 'Black', 'Standard', 'Black Standard', 1, 0),
        ('canon', 'B0BSR5TSKR', '069', '069', 'Yellow', 'Standard', 'Yellow Standard', 1, 0),
        ('canon', 'B0BSR71ZF8', '069', '069', 'Magenta', 'Standard', 'Magenta Standard', 1, 0),
        ('canon', 'B0BSR7FJ8F', '069H', '069H', 'Cyan', 'High', 'Cyan High', 1, 0),
        ('canon', 'B0BSR85QTF', '069H', '069H', 'Magenta', 'High', 'Magenta High', 1, 0),
        
        # =====================================================================
        # LEXMARK PRODUCTS - SELLABLE (sellable=1)
        # =====================================================================
        ('lexmark', 'B000A4AWMW', '25A0013', '25A0', 'Black', '', 'Black', 1, 1),
        ('lexmark', 'B0044A4D5U', 'E460X11A', 'E460', 'Black', 'Extra High', 'Black Extra High', 1, 1),
        ('lexmark', 'B07F6JH7XH', '50G0Z00', '50G0', 'Black', '', 'Black Imaging Unit', 1, 1),
        ('lexmark', 'B07F6SZRBD', '50G0Z50', '50G0', 'Grey', '', 'Grey Imaging Unit', 1, 1),
        ('lexmark', 'B07F8M52V2', '24B6719', '24B6', 'Black', 'Standard', 'Black Standard', 1, 1),
        ('lexmark', 'B0CB8SPMPH', '38S0500', '38S0', 'Black', '', 'Black', 1, 1),
        ('lexmark', 'B0CB8TLFNS', '38S0400', '38S0', 'Black', '', 'Black', 1, 1),
        ('lexmark', 'B0CB8Z9LX2', '38S0310', '38S0', 'Tray', '', 'Sheet Tray', 1, 1),
        ('lexmark', 'B0CB99VBSB', '50M0310', '50M', 'Tray', '', 'Sheet Tray', 1, 1),
        
        # =====================================================================
        # XEROX PRODUCTS - SELLABLE (sellable=1)
        # =====================================================================
        ('xerox', 'B008BWK86K', '113R00726', '6180', 'Black', 'High', 'Black High Yield', 1, 1),
        ('xerox', 'B00PWVBMGI', '106R02244', '6600', 'Black', 'Standard', 'Black Standard', 1, 1),
        ('xerox', 'B0747Z742Z', '108R01486', '', 'Magenta', '', 'Magenta Drum', 1, 1),
        ('xerox', 'B0747Z7432', '108R01488', '', 'Black', '', 'Black Drum', 1, 1),
        ('xerox', 'B0779Q7PSQ', '106R03512', 'C400', 'Black', 'Standard', '2 Black Standard', 2, 1),
        ('xerox', 'B0779QCN9F', '106R03524', 'C400', 'Black', 'High', '2 Black High', 2, 1),
        ('xerox', 'B08XLK3L63', '006R01457', '7120', 'Black', 'Standard', '2 Black Standard', 2, 1),
        ('xerox', 'B08XLXRHCT', '006R01697', 'C8030', 'Black', 'Standard', '2 Black Standard', 2, 1),
        
        # =====================================================================
        # STAPLE CARTRIDGES - SELLABLE (sellable=1)
        # =====================================================================
        # Xerox Staples
        ('xerox', 'B00067ZETY', '008R12941', '', 'Staples', '', '3 Staple Cartridges', 1, 1),
        ('xerox', 'B000FQBBIY', '008R12925', '', 'Staples', '', 'Staples', 1, 1),
        ('xerox', 'B000SSJTHO', '008R12964', '', 'Staples', '', 'Staples', 1, 0),  # Overstock
        ('xerox', 'B0088EHTAO', '008R12941-2', '', 'Staples', '', '3 Staple Cartridges', 1, 0),  # Unsellable
        ('xerox', 'B00267ZY9M', '008R13041', '', 'Staples', '', '4 Staple Cartridges', 1, 1),
        ('xerox', 'B00AVKDVJG', '008R13041-2', '', 'Staples', '', '4 Staple Cartridges', 1, 1),
        ('xerox', 'B00BL3GCRK', '008R12964-2', '', 'Staples', '', 'Staples', 1, 1),
        ('xerox', 'B00KHVUDHM', '008R12925-2', '', 'Staples', '', 'Staples', 1, 0),  # Overstock
        ('xerox', 'B00PDDJAYG', '008R13177', '', 'Staples', '', 'Staples', 1, 1),
        
        # Canon Staples
        ('canon', 'B001EQSFRE', 'P1', '', 'Staples', '', '2 Staple Cartridges', 1, 1),
        ('canon', 'B00BBUS3K2', 'N1', '', 'Staples', '', '3 Staple Cartridges', 1, 1),
        ('canon', 'B00HKZPFMQ', 'J1', '', 'Staples', '', '3 Staple Cartridges', 1, 1),
        ('canon', 'B00HKZW52E', 'N1-2', '', 'Staples', '', '3 Staple Cartridges', 1, 1),
        ('canon', 'B00KHVMYPQ', 'N1-3', '', 'Staples', '', '3 Staple Cartridges', 1, 1),
        ('canon', 'B06XBXTW8R', 'X1', '', 'Staples', '', '3 Staple Cartridges', 1, 1),
        
        # Lexmark Staples
        ('lexmark', 'B000TS7B9G', '25A0013-2', '', 'Staples', '', '3 Staple Cartridges', 1, 1),
        ('lexmark', 'B003I7AG88', '25A0013', '', 'Staples', '', '3 Staple Cartridges', 1, 1),
        
        # NOTE: Kyocera, Ricoh, Sharp, Toshiba, Konica, Professor Color staples 
        # cannot be added - database only allows canon, xerox, lexmark brands
        # These would need a schema change to add:
        # B000VUPZE0, B00GOO5QEQ, B00I7TZ0F6, B0CNNJFQZD (Kyocera)
        # B002FYCNCC, B008RUXNNG, B00AQEJ6PU, B00BN3L4EE, B01AQ3EFZG, B09X127M3D (Ricoh)
        # B001E58B6K, B00BYD248S, B00UW3MOZO, B0141MLL1E (Sharp)
        # B00DB8HWPO (Toshiba)
        # B0CPP2612N (Konica Minolta)
        # B0DF9BXQW8, B0DQM1TWT1 (Professor Color)
    ]
    
    added_count = 0
    skipped_count = 0
    
    for product in products_to_add:
        brand, asin, part_number, model, color, capacity, variant_label, pack_size, sellable = product
        
        # Check if already exists
        cursor.execute("SELECT asin FROM products WHERE asin = ?", (asin,))
        if cursor.fetchone():
            print(f"  - Skipped {asin} (already exists)")
            skipped_count += 1
            continue
        
        # Generate group_key
        if model:
            group_key = f"{brand}:{model.lower()}:{capacity.lower() if capacity else 'standard'}"
        else:
            group_key = f"{brand}:part:{part_number.lower()}"
        
        cursor.execute("""
            INSERT INTO products (brand, asin, part_number, model, color, capacity, variant_label, pack_size, sellable, active, group_key)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
        """, (brand, asin, part_number, model, color, capacity, variant_label, pack_size, sellable, group_key))
        
        status = "sellable" if sellable == 1 else "BLOCKED"
        print(f"  ✓ Added {asin} | {brand:<15} | {variant_label:<25} | {status}")
        added_count += 1
    
    conn.commit()
    print(f"\nPhase 2 complete: Added {added_count} products, skipped {skipped_count} (already exist)")
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    cursor.execute("SELECT COUNT(*) FROM products")
    total_products = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM products WHERE sellable = 1")
    sellable_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM products WHERE sellable = 0")
    blocked_count = cursor.fetchone()[0]
    
    print("\n" + "=" * 80)
    print("FINAL DATABASE STATE")
    print("=" * 80)
    print(f"Total Products: {total_products}")
    print(f"Sellable: {sellable_count}")
    print(f"Blocked: {blocked_count}")
    
    conn.close()
    print("\n✅ All changes applied successfully!")

if __name__ == '__main__':
    main()
