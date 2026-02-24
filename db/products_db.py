"""
Products database operations.

Manages the products table for Canon, Xerox, and Lexmark toner cartridges.
Replaces Google Sheets as the source of truth for product catalog data.

- Schema creation
- CRUD operations
- Bulk import/upsert
- Metrics bulk update (from analyzer)
- Engine query helpers (returns DataFrames)
"""

import os
import sqlite3
import uuid
from datetime import datetime
from typing import Optional, List, Dict, Any
import pandas as pd

# Use same DB path as other modules
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.abspath(os.getenv("DB_PATH", os.path.join(REPO_ROOT, "database.db")))

PRODUCTS_TABLE = 'products'
SETTINGS_TABLE = 'settings'

# Valid brands
VALID_BRANDS = {'canon', 'xerox', 'lexmark'}

# Default settings
DEFAULT_OVERHEAD_PCT = 15.0  # 15% overhead deducted from amazon_price
DEFAULT_TARGET_PROFIT = 25.0  # $25 minimum profit for money emoji


def get_db_connection() -> sqlite3.Connection:
    """Get a database connection with row factory for dict-like access."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_products_db():
    """Create products table if not exists."""
    conn = get_db_connection()
    with conn:
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {PRODUCTS_TABLE} (
                -- Identity
                id TEXT PRIMARY KEY,
                brand TEXT NOT NULL CHECK (brand IN ('canon', 'xerox', 'lexmark')),
                
                -- Grouping (for printable blocks)
                model TEXT,
                capacity TEXT,
                group_key TEXT,
                
                -- Variant details
                part_number TEXT,
                variant_label TEXT,
                color TEXT,
                pack_size INTEGER DEFAULT 1,
                
                -- Amazon identifiers
                asin TEXT,
                amazon_sku TEXT,
                
                -- Amazon metrics (updated by analyzer import)
                net_cost REAL,
                amazon_price REAL,
                bsr INTEGER,
                bsr_current INTEGER,
                sellable INTEGER DEFAULT 1,
                
                -- User annotations
                notes TEXT,
                
                -- Source tracking
                source_tab TEXT,
                is_model_block INTEGER,
                
                -- Timestamps
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                
                -- Constraints
                UNIQUE(brand, asin)
            )
        ''')
        
        # Create indexes for efficient queries
        conn.execute(f'CREATE INDEX IF NOT EXISTS idx_products_brand ON {PRODUCTS_TABLE}(brand)')
        conn.execute(f'CREATE INDEX IF NOT EXISTS idx_products_model ON {PRODUCTS_TABLE}(model)')
        conn.execute(f'CREATE INDEX IF NOT EXISTS idx_products_part_number ON {PRODUCTS_TABLE}(part_number)')
        conn.execute(f'CREATE INDEX IF NOT EXISTS idx_products_part_number_lower ON {PRODUCTS_TABLE}(lower(part_number))')
        conn.execute(f'CREATE INDEX IF NOT EXISTS idx_products_asin ON {PRODUCTS_TABLE}(asin)')
        conn.execute(f'CREATE INDEX IF NOT EXISTS idx_products_group_key ON {PRODUCTS_TABLE}(group_key)')
        # Composite index for Canon matching
        conn.execute(f'CREATE INDEX IF NOT EXISTS idx_products_canon_match ON {PRODUCTS_TABLE}(brand, model, capacity, color, pack_size)')
        
        # Migration: add notes column if it doesn't exist
        cursor = conn.execute(f"PRAGMA table_info({PRODUCTS_TABLE})")
        columns = [row[1] for row in cursor.fetchall()]
        if 'notes' not in columns:
            conn.execute(f'ALTER TABLE {PRODUCTS_TABLE} ADD COLUMN notes TEXT')
        
        # Migration: add amazon_price column if it doesn't exist
        if 'amazon_price' not in columns:
            conn.execute(f'ALTER TABLE {PRODUCTS_TABLE} ADD COLUMN amazon_price REAL')

        # Migration: add bsr_current column if it doesn't exist
        if 'bsr_current' not in columns:
            conn.execute(f'ALTER TABLE {PRODUCTS_TABLE} ADD COLUMN bsr_current INTEGER')
        
        # Create settings table
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {SETTINGS_TABLE} (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT DEFAULT (datetime('now'))
            )
        ''')
        
        # Initialize default settings if not present
        conn.execute(f'''
            INSERT OR IGNORE INTO {SETTINGS_TABLE} (key, value)
            VALUES ('overhead_pct', ?)
        ''', (str(DEFAULT_OVERHEAD_PCT),))
        conn.execute(f'''
            INSERT OR IGNORE INTO {SETTINGS_TABLE} (key, value)
            VALUES ('target_profit', ?)
        ''', (str(DEFAULT_TARGET_PROFIT),))
    
    conn.close()


# =============================================================================
# Settings Functions
# =============================================================================

def get_setting(key: str, default: str = None) -> Optional[str]:
    """Get a setting value by key."""
    conn = get_db_connection()
    try:
        cursor = conn.execute(
            f'SELECT value FROM {SETTINGS_TABLE} WHERE key = ?',
            (key,)
        )
        row = cursor.fetchone()
        return row['value'] if row else default
    finally:
        conn.close()


def set_setting(key: str, value: str) -> bool:
    """Set a setting value (insert or update)."""
    conn = get_db_connection()
    try:
        with conn:
            conn.execute(f'''
                INSERT INTO {SETTINGS_TABLE} (key, value, updated_at)
                VALUES (?, ?, datetime('now'))
                ON CONFLICT(key) DO UPDATE SET value = ?, updated_at = datetime('now')
            ''', (key, value, value))
        return True
    except Exception:
        return False
    finally:
        conn.close()


def get_overhead_pct() -> float:
    """Get the overhead percentage setting."""
    value = get_setting('overhead_pct', str(DEFAULT_OVERHEAD_PCT))
    try:
        return float(value)
    except (ValueError, TypeError):
        return DEFAULT_OVERHEAD_PCT


def set_overhead_pct(pct: float) -> bool:
    """Set the overhead percentage setting (0-100)."""
    if not 0 <= pct <= 100:
        return False
    return set_setting('overhead_pct', str(pct))


def get_target_profit() -> float:
    """Get the target profit threshold setting (for money emoji)."""
    value = get_setting('target_profit', str(DEFAULT_TARGET_PROFIT))
    try:
        return float(value)
    except (ValueError, TypeError):
        return DEFAULT_TARGET_PROFIT


def set_target_profit(amount: float) -> bool:
    """Set the target profit threshold ($0-$500)."""
    if amount < 0 or amount > 500:
        return False
    return set_setting('target_profit', str(amount))


def get_all_settings() -> Dict[str, str]:
    """Get all settings as a dictionary."""
    conn = get_db_connection()
    try:
        cursor = conn.execute(f'SELECT key, value FROM {SETTINGS_TABLE}')
        return {row['key']: row['value'] for row in cursor.fetchall()}
    finally:
        conn.close()


def calculate_effective_net(net_cost: float, amazon_price: float, overhead_pct: float = None) -> float:
    """
    Calculate effective net after overhead deduction.
    
    Formula: effective_net = net_cost - (amazon_price × overhead_pct / 100)
    
    The overhead covers:
    - Inbound shipping to Amazon
    - Return allowances
    - Packaging/prep costs
    - Safety margin for price fluctuations
    
    Args:
        net_cost: Seller proceeds from analyzer (after Amazon fees)
        amazon_price: Amazon buybox/sale price
        overhead_pct: Overhead percentage (default: from settings)
    
    Returns:
        Effective net - the max you should pay on eBay to be profitable
    """
    if overhead_pct is None:
        overhead_pct = get_overhead_pct()
    
    if amazon_price is None or amazon_price <= 0:
        # If no amazon_price, can't calculate overhead - return net_cost as-is
        return net_cost
    
    overhead_amount = amazon_price * (overhead_pct / 100)
    return net_cost - overhead_amount


def generate_id() -> str:
    """Generate a UUID-style ID for new products."""
    return uuid.uuid4().hex


def normalize_capacity(capacity: str) -> str:
    """Normalize capacity string for group_key."""
    if not capacity:
        return 'standard'
    cap = capacity.lower().strip()
    if 'extra' in cap or 'super' in cap:
        return 'extra-high'
    elif 'high' in cap:
        return 'high'
    elif 'mixed' in cap:
        return 'mixed'
    return 'standard'


def is_xerox_model_block(identifier: str) -> bool:
    """
    Determine if a Xerox block identifier is a model (6020, C400) or a part number (001R00610).
    
    Part numbers follow pattern: 3 digits + R + 5 digits (e.g., 001R00610, 106R01512)
    Model names are typically: 4 digits (6020, 7500) or letter + digits (C400, C620)
    """
    import re
    if not identifier:
        return False
    # Part number pattern: XXX R XXXXX (e.g., 001R00610, 106R01512, 108R01124)
    if re.match(r'^\d{3}[rR]\d{5}$', identifier):
        return False  # It's a part number block
    # Otherwise assume it's a model (6020, 7500, C400, C620, etc.)
    return True


def generate_group_key(brand: str, model: str, capacity: str, part_number: str, is_model_block: bool = None) -> str:
    """
    Generate group_key for block grouping in printable sheets.
    
    IMPORTANT: Grouping rules differ by brand!
    - Canon: Groups by model + capacity (e.g., "054H High" is separate from "054H Standard")
    - Lexmark: Groups by model_family ONLY (blocks contain mixed capacities)
    - Xerox: TWO block types:
        - Part number blocks (001R00610, 008R13061): group by part_number
        - Model blocks (6020, 7500, C400): group by model + capacity (like Canon)
    """
    brand_lower = brand.lower() if brand else ''
    
    if brand_lower == 'canon':
        if model and capacity:
            cap_normalized = normalize_capacity(capacity)
            return f"canon:{model.lower()}:{cap_normalized}"
    
    elif brand_lower == 'lexmark':
        if model:
            return f"lexmark:{model.lower()}"
    
    elif brand_lower == 'xerox':
        # Determine block type if not specified
        if is_model_block is None:
            # Use model if available, otherwise check part_number pattern
            if model and is_xerox_model_block(model):
                is_model_block = True
            else:
                is_model_block = False
        
        if is_model_block and model:
            cap_normalized = normalize_capacity(capacity) if capacity else 'standard'
            return f"xerox:model:{model.lower()}:{cap_normalized}"
        elif part_number:
            return f"xerox:part:{part_number.lower()}"
    
    return None


def row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    """Convert sqlite3.Row to dict."""
    if row is None:
        return None
    return dict(row)


# =============================================================================
# CRUD Operations
# =============================================================================

def create_product(product: Dict[str, Any]) -> str:
    """
    Create a new product.
    
    Args:
        product: Dict with product fields (brand, model, etc.)
    
    Returns:
        The new product ID
    
    Raises:
        ValueError: If required fields missing or invalid brand
        sqlite3.IntegrityError: If duplicate (brand, asin) exists
    """
    # Validate required fields
    if 'brand' not in product:
        raise ValueError("brand is required")
    brand = product['brand'].lower()
    if brand not in VALID_BRANDS:
        raise ValueError(f"Invalid brand: {brand}. Must be one of {VALID_BRANDS}")
    
    # Generate ID and group_key
    product_id = product.get('id') or generate_id()
    group_key = generate_group_key(
        brand=brand,
        model=product.get('model'),
        capacity=product.get('capacity'),
        part_number=product.get('part_number'),
        is_model_block=product.get('is_model_block')
    )
    
    conn = get_db_connection()
    try:
        with conn:
            conn.execute(f'''
                INSERT INTO {PRODUCTS_TABLE} (
                    id, brand, model, capacity, group_key,
                    part_number, variant_label, color, pack_size,
                    asin, amazon_sku, net_cost, amazon_price, bsr, sellable, notes,
                    source_tab, is_model_block, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            ''', (
                product_id,
                brand,
                product.get('model'),
                product.get('capacity'),
                group_key,
                product.get('part_number'),
                product.get('variant_label'),
                product.get('color'),
                product.get('pack_size', 1),
                product.get('asin'),
                product.get('amazon_sku'),
                product.get('net_cost'),
                product.get('amazon_price'),
                product.get('bsr'),
                1 if product.get('sellable', True) else 0,
                product.get('notes'),
                product.get('source_tab'),
                1 if product.get('is_model_block') else 0 if product.get('is_model_block') is False else None
            ))
        return product_id
    finally:
        conn.close()


def get_product(product_id: str) -> Optional[Dict[str, Any]]:
    """Get a product by ID."""
    conn = get_db_connection()
    try:
        cursor = conn.execute(
            f'SELECT * FROM {PRODUCTS_TABLE} WHERE id = ?',
            (product_id,)
        )
        row = cursor.fetchone()
        return row_to_dict(row)
    finally:
        conn.close()


def get_product_by_asin(brand: str, asin: str) -> Optional[Dict[str, Any]]:
    """Get a product by brand and ASIN."""
    conn = get_db_connection()
    try:
        cursor = conn.execute(
            f'SELECT * FROM {PRODUCTS_TABLE} WHERE brand = ? AND asin = ?',
            (brand.lower(), asin)
        )
        row = cursor.fetchone()
        return row_to_dict(row)
    finally:
        conn.close()


def update_product(product_id: str, updates: Dict[str, Any]) -> bool:
    """
    Update a product's fields.
    
    Args:
        product_id: The product ID
        updates: Dict of fields to update
    
    Returns:
        True if product was found and updated, False otherwise
    """
    if not updates:
        return False
    
    # Don't allow updating id
    updates = {k: v for k, v in updates.items() if k != 'id'}
    
    # Recalculate group_key if relevant fields changed
    if any(k in updates for k in ('model', 'capacity', 'part_number', 'is_model_block')):
        existing = get_product(product_id)
        if existing:
            merged = {**existing, **updates}
            updates['group_key'] = generate_group_key(
                brand=merged.get('brand'),
                model=merged.get('model'),
                capacity=merged.get('capacity'),
                part_number=merged.get('part_number'),
                is_model_block=merged.get('is_model_block')
            )
    
    # Always update timestamp
    updates['updated_at'] = datetime.now().isoformat()
    
    # Build update query
    set_clause = ', '.join(f'{k} = ?' for k in updates.keys())
    values = list(updates.values()) + [product_id]
    
    conn = get_db_connection()
    try:
        with conn:
            cursor = conn.execute(
                f'UPDATE {PRODUCTS_TABLE} SET {set_clause} WHERE id = ?',
                values
            )
            return cursor.rowcount > 0
    finally:
        conn.close()


def delete_product(product_id: str, hard: bool = False) -> bool:
    """
    Delete a product (soft delete by default, hard delete if specified).
    
    Args:
        product_id: The product ID
        hard: If True, permanently delete. If False, set sellable=0 (soft delete).
    
    Returns:
        True if product was found and deleted/deactivated, False otherwise
    """
    conn = get_db_connection()
    try:
        with conn:
            if hard:
                # Permanent deletion
                cursor = conn.execute(
                    f'DELETE FROM {PRODUCTS_TABLE} WHERE id = ?',
                    (product_id,)
                )
            else:
                # Soft delete - set sellable to 0
                cursor = conn.execute(
                    f'UPDATE {PRODUCTS_TABLE} SET sellable = 0, updated_at = ? WHERE id = ?',
                    (datetime.now().isoformat(), product_id)
                )
            return cursor.rowcount > 0
    finally:
        conn.close()


def list_products(
    brand: str = None,
    group_key: str = None,
    model: str = None,
    search: str = None,
    include_inactive: bool = False,
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]:
    """
    List products with optional filters.
    
    Args:
        brand: Filter by brand
        group_key: Filter by group_key (for printable blocks)
        model: Filter by model
        search: Search in model, part_number, or asin
        include_inactive: If True, include products with sellable=0 (default False)
        limit: Max results (default 100)
        offset: Pagination offset
    
    Returns:
        List of product dicts
    """
    conditions = []
    params = []
    
    # By default, only show active (sellable) products
    if not include_inactive:
        conditions.append('(sellable IS NULL OR sellable = 1)')
    
    if brand:
        conditions.append('brand = ?')
        params.append(brand.lower())
    
    if group_key:
        conditions.append('group_key = ?')
        params.append(group_key)
    
    if model:
        conditions.append('model = ?')
        params.append(model)
    
    if search:
        conditions.append('(LOWER(model) LIKE ? OR LOWER(part_number) LIKE ? OR LOWER(asin) LIKE ? OR LOWER(variant_label) LIKE ? OR LOWER(color) LIKE ? OR LOWER(group_key) LIKE ?)')
        search_term = f'%{search.lower()}%'
        params.extend([search_term] * 6)

    where_clause = ' AND '.join(conditions) if conditions else '1=1'
    params.extend([limit, offset])
    
    conn = get_db_connection()
    try:
        cursor = conn.execute(
            f'''SELECT * FROM {PRODUCTS_TABLE} 
                WHERE {where_clause} 
                ORDER BY brand, group_key, pack_size DESC, color
                LIMIT ? OFFSET ?''',
            params
        )
        return [row_to_dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def count_products(brand: str = None, search: str = None, include_inactive: bool = False) -> int:
    """Count products with optional filters."""
    conditions = []
    params = []
    
    # By default, only count active (sellable) products
    if not include_inactive:
        conditions.append('(sellable IS NULL OR sellable = 1)')
    
    if brand:
        conditions.append('brand = ?')
        params.append(brand.lower())
    
    if search:
        conditions.append('(LOWER(model) LIKE ? OR LOWER(part_number) LIKE ? OR LOWER(asin) LIKE ? OR LOWER(variant_label) LIKE ? OR LOWER(color) LIKE ? OR LOWER(group_key) LIKE ?)')
        search_term = f'%{search.lower()}%'
        params.extend([search_term] * 6)

    where_clause = ' AND '.join(conditions) if conditions else '1=1'

    conn = get_db_connection()
    try:
        cursor = conn.execute(
            f'SELECT COUNT(*) FROM {PRODUCTS_TABLE} WHERE {where_clause}',
            params
        )
        return cursor.fetchone()[0]
    finally:
        conn.close()


# =============================================================================
# Bulk Operations
# =============================================================================

def bulk_upsert_products(products: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Bulk upsert products from CSV import.
    
    Uses (brand, asin) as the unique key for upsert logic:
    - If exists: update all fields
    - If not: insert new row
    
    Args:
        products: List of product dicts
    
    Returns:
        Dict with stats: {total, created, updated, skipped, errors}
    """
    stats = {'total': len(products), 'created': 0, 'updated': 0, 'skipped': 0, 'errors': []}
    
    conn = get_db_connection()
    try:
        with conn:
            for i, product in enumerate(products):
                try:
                    brand = product.get('brand', '').lower()
                    asin = product.get('asin')
                    
                    # Skip if missing required fields
                    if not brand or brand not in VALID_BRANDS:
                        stats['errors'].append({'row': i, 'error': f"Invalid or missing brand: {brand}"})
                        stats['skipped'] += 1
                        continue
                    
                    if not asin or asin.upper() in ('N/A', 'NO AMZ LISTING', ''):
                        stats['skipped'] += 1
                        continue
                    
                    # Check if exists
                    cursor = conn.execute(
                        f'SELECT id FROM {PRODUCTS_TABLE} WHERE brand = ? AND asin = ?',
                        (brand, asin)
                    )
                    existing = cursor.fetchone()
                    
                    # Generate group_key
                    group_key = generate_group_key(
                        brand=brand,
                        model=product.get('model'),
                        capacity=product.get('capacity'),
                        part_number=product.get('part_number'),
                        is_model_block=product.get('is_model_block')
                    )
                    
                    if existing:
                        # Update existing
                        conn.execute(f'''
                            UPDATE {PRODUCTS_TABLE} SET
                                model = ?,
                                capacity = ?,
                                group_key = ?,
                                part_number = ?,
                                variant_label = ?,
                                color = ?,
                                pack_size = ?,
                                amazon_sku = ?,
                                net_cost = ?,
                                bsr = ?,
                                sellable = ?,
                                source_tab = ?,
                                is_model_block = ?,
                                updated_at = datetime('now')
                            WHERE id = ?
                        ''', (
                            product.get('model'),
                            product.get('capacity'),
                            group_key,
                            product.get('part_number'),
                            product.get('variant_label'),
                            product.get('color'),
                            product.get('pack_size', 1),
                            product.get('amazon_sku'),
                            product.get('net_cost'),
                            product.get('bsr'),
                            1 if product.get('sellable', True) else 0,
                            product.get('source_tab'),
                            1 if product.get('is_model_block') else 0 if product.get('is_model_block') is False else None,
                            existing['id']
                        ))
                        stats['updated'] += 1
                    else:
                        # Insert new
                        product_id = generate_id()
                        conn.execute(f'''
                            INSERT INTO {PRODUCTS_TABLE} (
                                id, brand, model, capacity, group_key,
                                part_number, variant_label, color, pack_size,
                                asin, amazon_sku, net_cost, bsr, sellable,
                                source_tab, is_model_block, created_at, updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
                        ''', (
                            product_id,
                            brand,
                            product.get('model'),
                            product.get('capacity'),
                            group_key,
                            product.get('part_number'),
                            product.get('variant_label'),
                            product.get('color'),
                            product.get('pack_size', 1),
                            asin,
                            product.get('amazon_sku'),
                            product.get('net_cost'),
                            product.get('bsr'),
                            1 if product.get('sellable', True) else 0,
                            product.get('source_tab'),
                            1 if product.get('is_model_block') else 0 if product.get('is_model_block') is False else None
                        ))
                        stats['created'] += 1
                
                except Exception as e:
                    stats['errors'].append({'row': i, 'error': str(e)})
                    stats['skipped'] += 1
    
    finally:
        conn.close()
    
    return stats


def bulk_update_metrics(metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Bulk update BSR/net_cost/amazon_price/sellable from analyzer output.
    
    Matches by ASIN (across all brands).
    
    Args:
        metrics: List of dicts with asin, net_cost, amazon_price, bsr, sellable
    
    Returns:
        Dict with stats: {total, updated, not_found, errors}
    """
    stats = {'total': len(metrics), 'updated': 0, 'not_found': 0, 'errors': []}
    
    conn = get_db_connection()
    try:
        with conn:
            for i, m in enumerate(metrics):
                try:
                    asin = m.get('asin')
                    if not asin:
                        stats['errors'].append({'row': i, 'error': 'Missing ASIN'})
                        continue
                    
                    # Build dynamic update
                    updates = {}
                    if 'net_cost' in m and m['net_cost'] is not None:
                        updates['net_cost'] = m['net_cost']
                    if 'amazon_price' in m and m['amazon_price'] is not None:
                        updates['amazon_price'] = m['amazon_price']
                    if 'bsr' in m and m['bsr'] is not None:
                        updates['bsr'] = m['bsr']
                    if 'bsr_current' in m and m['bsr_current'] is not None:
                        updates['bsr_current'] = m['bsr_current']
                    if 'sellable' in m and m['sellable'] is not None:
                        updates['sellable'] = 1 if m['sellable'] else 0
                    
                    if not updates:
                        continue
                    
                    updates['updated_at'] = datetime.now().isoformat()
                    
                    set_clause = ', '.join(f'{k} = ?' for k in updates.keys())
                    values = list(updates.values()) + [asin]
                    
                    cursor = conn.execute(
                        f'UPDATE {PRODUCTS_TABLE} SET {set_clause} WHERE asin = ?',
                        values
                    )
                    
                    if cursor.rowcount > 0:
                        stats['updated'] += 1
                    else:
                        stats['not_found'] += 1
                
                except Exception as e:
                    stats['errors'].append({'row': i, 'error': str(e)})
    
    finally:
        conn.close()
    
    return stats


# =============================================================================
# Engine Query Helpers
# =============================================================================

def get_products_for_engine(brand: str) -> pd.DataFrame:
    """
    Get products as DataFrame for engine matching.
    
    Returns all products for the specified brand.
    
    Args:
        brand: 'canon', 'xerox', or 'lexmark'
    
    Returns:
        DataFrame with all product columns
    """
    conn = get_db_connection()
    try:
        query = f'''
            SELECT * FROM {PRODUCTS_TABLE}
            WHERE brand = ?
        '''
        df = pd.read_sql_query(query, conn, params=(brand.lower(),))
        
        # Convert boolean columns
        df['sellable'] = df['sellable'].astype(bool)
        if 'is_model_block' in df.columns:
            df['is_model_block'] = df['is_model_block'].fillna(False).astype(bool)
        
        return df
    finally:
        conn.close()


def get_canon_products() -> pd.DataFrame:
    """
    Get Canon products DataFrame for engine matching.
    
    Returns DataFrame with columns matching Canon engine expectations:
    - model, capacity, pack_size, variant, color, ASIN, BSR, net, amazon_price, sellable
    """
    df = get_products_for_engine('canon')
    if df.empty:
        return df
    
    # Rename columns to match Canon engine expectations
    column_map = {
        'asin': 'ASIN',
        'bsr': 'BSR',
        'bsr_current': 'BSR_current',
        'net_cost': 'net',
        'variant_label': 'variant',
    }
    df = df.rename(columns=column_map)
    
    # Convert sellable boolean to string format expected by Canon engine
    df['sellable'] = df['sellable'].apply(lambda x: 'Sellable' if x else 'Not Sellable')
    
    return df


def get_xerox_products() -> pd.DataFrame:
    """
    Get Xerox products DataFrame for engine matching.
    
    Returns DataFrame with columns matching Xerox engine expectations:
    - part_number, variant_label, capacity, net, amazon_price, asin, sku, bsr, sellable (bool)
    """
    df = get_products_for_engine('xerox')
    if df.empty:
        return df
    
    # Rename columns to match Xerox engine expectations
    column_map = {
        'net_cost': 'net',
        'amazon_sku': 'sku',
    }
    df = df.rename(columns=column_map)
    
    # Xerox engine expects sellable as boolean
    df['sellable'] = df['sellable'].astype(bool)
    
    return df


def get_lexmark_products() -> pd.DataFrame:
    """
    Get Lexmark products DataFrame for engine matching.
    
    Returns DataFrame with columns matching Lexmark engine expectations:
    - model_family, part_number, part_number_lower, variant_label, color,
      capacity, pack_size, net_cost, amazon_price, asin, amazon_sku, bsr, sellable (bool)
    """
    df = get_products_for_engine('lexmark')
    if df.empty:
        return df
    
    # Rename model to model_family for Lexmark
    df = df.rename(columns={'model': 'model_family'})
    
    # Add part_number_lower for Lexmark SKU matching
    df['part_number_lower'] = df['part_number'].str.lower().fillna('')
    
    # Lexmark engine expects sellable as boolean
    df['sellable'] = df['sellable'].astype(bool)
    
    return df


def build_part_number_index(brand: str) -> Dict[str, Dict[str, Any]]:
    """
    Build a part number lookup index for Xerox/Lexmark engines.
    
    Returns:
        Dict mapping lowercase part_number to product dict
    """
    df = get_products_for_engine(brand)
    index = {}
    for _, row in df.iterrows():
        pn = row.get('part_number')
        if pn:
            index[pn.lower()] = row.to_dict()
    return index


# =============================================================================
# Printable/Grouping Helpers
# =============================================================================

def get_all_group_keys(brand: str = None) -> List[str]:
    """Get all distinct group_keys, optionally filtered by brand."""
    conn = get_db_connection()
    try:
        if brand:
            cursor = conn.execute(
                f'SELECT DISTINCT group_key FROM {PRODUCTS_TABLE} WHERE brand = ? AND group_key IS NOT NULL ORDER BY group_key',
                (brand.lower(),)
            )
        else:
            cursor = conn.execute(
                f'SELECT DISTINCT group_key FROM {PRODUCTS_TABLE} WHERE group_key IS NOT NULL ORDER BY group_key'
            )
        return [row[0] for row in cursor.fetchall()]
    finally:
        conn.close()


def get_products_by_group_key(group_key: str) -> List[Dict[str, Any]]:
    """Get all products in a specific block/group."""
    conn = get_db_connection()
    try:
        cursor = conn.execute(
            f'''SELECT * FROM {PRODUCTS_TABLE} 
                WHERE group_key = ?
                ORDER BY pack_size DESC, color''',
            (group_key,)
        )
        return [row_to_dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()
