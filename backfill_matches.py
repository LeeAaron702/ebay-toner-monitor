#!/usr/bin/env python3
"""
backfill_matches.py
-------------------
Script to retroactively run the matching algorithm on existing order history.

Supports ALL brands: Canon, Xerox, Lexmark

This script:
1. Loads product data from SQL database for all brands
2. Reads orders from order_history table
3. Detects brand from item_title and runs appropriate matching
4. Creates messages and matches entries
5. Re-enriches order_history with match data
6. Populates purchased_units for analytics

Usage:
    python backfill_matches.py              # Process all unmatched orders
    python backfill_matches.py --full       # Reprocess ALL orders

After running, you can verify with:
    sqlite3 database.db "SELECT COUNT(*) FROM matches;"
    sqlite3 database.db "SELECT COUNT(*) FROM purchased_units;"
"""

import json
import sqlite3
import time
import sys
import re
from pathlib import Path
from typing import Dict, List, Optional, Any

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from db.listings_db import (
    DB_PATH,
    init_db,
    insert_message,
    insert_match,
    get_db_connection,
    ORDER_HISTORY_TABLE,
    MESSAGES_TABLE,
    MATCHES_TABLE,
    expand_order_to_purchased_units,
    insert_purchased_units_batch,
    get_unprocessed_orders,
    get_backfill_status,
)

# Canon imports
from engine.canon import (
    match_listing as canon_match_listing,
    is_mixed_lot_listing,
    build_lot_breakdown,
    calculate_lot_match,
    find_multi_pack_alternatives,
)

# Xerox imports
from engine.xerox import (
    build_sku_index as build_xerox_sku_index,
    resolve_listing_variants as xerox_resolve_variants,
)

# Lexmark imports  
from engine.lexmark import (
    build_part_number_index as build_lexmark_pn_index,
    resolve_listing_variants as lexmark_resolve_variants,
)

from db.products_db import (
    get_canon_products, 
    get_xerox_products,
    get_lexmark_products,
    get_overhead_pct, 
    calculate_effective_net,
)


def _log(message: str) -> None:
    """Log a message with script prefix."""
    print(f"LOG - backfill_matches.py - {message}")


def detect_brand(item_title: str) -> Optional[str]:
    """Detect brand from item title."""
    title_lower = item_title.lower()
    if 'canon' in title_lower or 'gpr-' in title_lower or 'gpr ' in title_lower:
        return 'canon'
    if 'xerox' in title_lower:
        return 'xerox'
    if 'lexmark' in title_lower:
        return 'lexmark'
    return None


def sanitize_for_json(obj):
    """Convert numpy types to Python native types for JSON serialization."""
    import numpy as np
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    elif isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, np.bool_):
        return bool(obj)
    else:
        return obj


def get_all_order_history_rows():
    """Fetch all rows from order_history table."""
    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM {ORDER_HISTORY_TABLE}')
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def check_message_exists(item_id: str) -> bool:
    """Check if a message already exists for this item_id."""
    conn = get_db_connection()
    cur = conn.cursor()
    # Messages store listing_id as 'v1|ITEM_ID|0' format
    pattern = f"%|{item_id}|%"
    cur.execute(f'SELECT COUNT(*) FROM {MESSAGES_TABLE} WHERE listing_id LIKE ?', (pattern,))
    count = cur.fetchone()[0]
    conn.close()
    return count > 0


def update_order_history_match_columns(order_id: str, transaction_id: str, match_data: dict):
    """Update order_history row with match columns."""
    conn = get_db_connection()
    
    # Build SET clause for match columns
    set_parts = []
    values = []
    for key, value in match_data.items():
        set_parts.append(f"{key} = ?")
        values.append(value)
    
    values.extend([order_id, transaction_id])
    
    query = f'''
        UPDATE {ORDER_HISTORY_TABLE}
        SET {', '.join(set_parts)}
        WHERE order_id = ? AND transaction_id = ?
    '''
    
    with conn:
        conn.execute(query, values)
    conn.close()


def backfill_matches_for_order(row: dict, products_data: dict) -> dict:
    """
    Run matching algorithm on an order row and return match data.
    
    Args:
        row: Order history row dict
        products_data: Dict with keys 'canon_df', 'xerox_sku_map', 'lexmark_pn_map'
        
    Returns dict with match1_*, match2_*, etc. columns.
    """
    item_id = row.get('item_id', '')
    item_title = row.get('item_title', '')
    
    if not item_title:
        return {}
    
    # Detect brand
    brand = detect_brand(item_title)
    if not brand:
        return {}
    
    # Get price info for profit calculations
    try:
        transaction_price = float(row.get('transaction_price', 0) or 0)
    except (ValueError, TypeError):
        transaction_price = 0.0
    
    try:
        shipping_cost = float(row.get('shipping_service_cost', 0) or 0)
    except (ValueError, TypeError):
        shipping_cost = 0.0
    
    total_sale = transaction_price + shipping_cost
    overhead_pct = get_overhead_pct()
    
    matches_to_insert = []
    
    if brand == 'canon':
        matches_to_insert = _match_canon_order(item_title, total_sale, products_data.get('canon_df'), overhead_pct)
    elif brand == 'xerox':
        matches_to_insert = _match_xerox_order(item_title, total_sale, products_data.get('xerox_sku_map'), overhead_pct)
    elif brand == 'lexmark':
        matches_to_insert = _match_lexmark_order(item_title, total_sale, products_data.get('lexmark_pn_map'), overhead_pct)
    
    # Build match columns for order_history
    match_columns = {}
    for i, m in enumerate(matches_to_insert[:4], start=1):
        match_columns[f'match{i}_title'] = m.get('title', '')
        match_columns[f'match{i}_asin'] = m.get('asin', '')
        match_columns[f'match{i}_bsr'] = str(m.get('bsr', '')) if m.get('bsr') else ''
        match_columns[f'match{i}_sellable'] = 'true' if m.get('sellable') else 'false'
        match_columns[f'match{i}_net_cost'] = str(m.get('net_cost', '')) if m.get('net_cost') is not None else ''
        match_columns[f'match{i}_profit'] = str(m.get('profit', '')) if m.get('profit') is not None else ''
        match_columns[f'match{i}_pack_size'] = str(m.get('pack_size', '')) if m.get('pack_size') else ''
        match_columns[f'match{i}_color'] = m.get('color', '')
        match_columns[f'match{i}_is_alternative'] = 'true' if m.get('is_alternative') else 'false'
        match_columns[f'match{i}_lot_breakdown'] = m.get('lot_breakdown', '')
        match_columns[f'match{i}_total_units'] = str(m.get('total_units', '')) if m.get('total_units') else ''
    
    return {
        'matches': matches_to_insert,
        'columns': match_columns,
        'brand': brand,
    }


def _match_canon_order(item_title: str, total_sale: float, sheet_df, overhead_pct: float) -> List[dict]:
    """Match a Canon order using the Canon engine."""
    if sheet_df is None or sheet_df.empty:
        return []
    
    matches_to_insert = []
    
    # Check if this is a mixed lot
    is_mixed = is_mixed_lot_listing(item_title)
    lot_breakdown = None
    
    if is_mixed:
        lot_breakdown = build_lot_breakdown(item_title, sheet_df)
    
    # Try standard matching first
    match = canon_match_listing(item_title, sheet_df)
    
    # CASE 1: Standard single match
    if match and not is_mixed:
        raw_net = match.get("net") or 0.0
        amazon_price = match.get("amazon_price")
        net_cost = calculate_effective_net(raw_net, amazon_price, overhead_pct)
        profit = net_cost - total_sale
        
        pack_size_int = int(match['pack_size'])
        single_color = match.get('color', '').lower() or 'unknown'
        single_lot_breakdown = {
            "model": str(match['model']),
            "capacity": str(match['capacity']),
            "color_quantities": {single_color: pack_size_int},
            "total_units": pack_size_int,
            "is_mixed_lot": False,
        }
        
        matches_to_insert.append({
            'is_alternative': 0,
            'title': f"Canon {match['model']} {match['variant']}",
            'asin': match['ASIN'],
            'bsr': int(match['BSR']) if match.get('BSR') else None,
            'sellable': int(match['sellable']),
            'net_cost': float(net_cost) if net_cost else 0.0,
            'profit': float(profit),
            'pack_size': pack_size_int,
            'color': match.get('color', ''),
            'lot_breakdown': json.dumps(sanitize_for_json(single_lot_breakdown)),
            'total_units': pack_size_int,
        })
        
        # Find alternatives for single-unit matches
        if match["pack_size"] == 1:
            alts = find_multi_pack_alternatives(match, sheet_df, total_sale)
            for alt in alts:
                matches_to_insert.append({
                    'is_alternative': 1,
                    'title': f"Canon {alt['model']} {alt['variant']}",
                    'asin': alt['ASIN'],
                    'bsr': int(alt['BSR']) if alt.get('BSR') else None,
                    'sellable': int(alt['unit_sellable']),
                    'net_cost': float(alt['unit_net']) if alt.get('unit_net') else 0.0,
                    'profit': float(alt['unit_profit']) if alt.get('unit_profit') else 0.0,
                    'pack_size': int(alt['pack_size']),
                    'color': alt.get('color', ''),
                })
    
    # CASE 2: Mixed lot
    elif is_mixed and lot_breakdown and lot_breakdown.model:
        lot_result = calculate_lot_match(lot_breakdown, sheet_df, total_sale)
        lot_breakdown_dict = sanitize_for_json(lot_breakdown.to_dict())
        
        # Store individual color matches
        for cm in lot_result.individual_matches:
            bsr_val = int(cm.bsr) if cm.bsr else None
            matches_to_insert.append({
                'is_alternative': 0,
                'title': f"Canon {lot_breakdown.model} {cm.color.capitalize()}",
                'asin': cm.asin,
                'bsr': bsr_val,
                'sellable': int(cm.sellable),
                'net_cost': float(cm.unit_net) if cm.unit_net else 0.0,
                'profit': float(cm.subtotal - (total_sale / lot_breakdown.total_units * cm.quantity)) if lot_breakdown.total_units > 0 else 0.0,
                'pack_size': 1,
                'color': cm.color,
                'lot_breakdown': json.dumps(lot_breakdown_dict),
                'total_units': int(cm.quantity),
            })
        
        # Store set alternatives
        for alt in lot_result.set_alternatives:
            bsr_val = int(alt.bsr) if alt.bsr else None
            matches_to_insert.append({
                'is_alternative': 1,
                'title': f"Canon {lot_breakdown.model} {alt.pack_type}",
                'asin': alt.asin,
                'bsr': bsr_val,
                'sellable': int(alt.sellable),
                'net_cost': float(alt.unit_net) if alt.unit_net else 0.0,
                'profit': float(alt.total_net - total_sale),
                'pack_size': int(alt.pack_size),
                'color': 'Color',
                'lot_breakdown': json.dumps(lot_breakdown_dict),
                'total_units': int(alt.total_units),
            })
    
    return matches_to_insert


def _match_xerox_order(item_title: str, total_sale: float, sku_map: dict, overhead_pct: float) -> List[dict]:
    """Match a Xerox order using SKU matching."""
    if not sku_map:
        return []
    
    matches_to_insert = []
    listing = {"title": item_title}
    
    # Use Xerox resolve function
    variant_matches = xerox_resolve_variants(listing, sku_map)
    
    for match in variant_matches:
        sku = match.get('sku', '')
        for variant in match.get('variants', []):
            raw_net = variant.get('net') or 0.0
            amazon_price = variant.get('amazon_price') or 0.0
            net_cost = calculate_effective_net(raw_net, amazon_price, overhead_pct)
            profit = net_cost - total_sale if net_cost else 0.0
            
            color = variant.get('color', '') or ''
            capacity = variant.get('capacity', '') or ''
            pack_size = int(variant.get('pack_size', 1) or 1)
            
            # lot_breakdown tracks what we PURCHASED (1 unit from eBay)
            # pack_size is the Amazon product configuration (for profit calculation)
            lot_breakdown = {
                "model": sku,
                "capacity": capacity,
                "color_quantities": {color.lower(): 1} if color else {"unknown": 1},
                "total_units": 1,
                "is_mixed_lot": False,
            }
            
            # Calculate per-unit profit: divide net_cost by pack_size
            net_per_unit = net_cost / pack_size if pack_size > 0 else net_cost
            profit_per_unit = net_per_unit - total_sale if net_per_unit else 0.0
            
            matches_to_insert.append({
                'is_alternative': 0,
                'title': f"Xerox {sku} {variant.get('variant_label', '')}".strip(),
                'asin': variant.get('asin', ''),
                'bsr': int(variant.get('bsr')) if variant.get('bsr') else None,
                'sellable': 1 if variant.get('sellable') else 0,
                'net_cost': float(net_per_unit) if net_per_unit else 0.0,
                'profit': float(profit_per_unit),
                'pack_size': pack_size,
                'color': color,
                'lot_breakdown': json.dumps(sanitize_for_json(lot_breakdown)),
                'total_units': 1,
            })
    
    return matches_to_insert


def _match_lexmark_order(item_title: str, total_sale: float, pn_map: dict, overhead_pct: float) -> List[dict]:
    """Match a Lexmark order using part number matching."""
    if not pn_map:
        return []
    
    matches_to_insert = []
    listing = {"title": item_title}
    
    # Use Lexmark resolve function
    variant_matches = lexmark_resolve_variants(listing, pn_map)
    
    for match in variant_matches:
        part_number = match.get('part_number', '')
        for variant in match.get('variants', []):
            raw_net = variant.get('net_cost') or 0.0
            amazon_price = variant.get('amazon_price') or 0.0
            net_cost = calculate_effective_net(raw_net, amazon_price, overhead_pct)
            profit = net_cost - total_sale if net_cost else 0.0
            
            color = variant.get('color', '') or ''
            capacity = variant.get('capacity', '') or ''
            pack_size = int(variant.get('pack_size', 1) or 1)
            model = variant.get('model_family', '') or part_number
            
            # lot_breakdown tracks what we PURCHASED (1 unit from eBay)
            # pack_size is the Amazon product configuration (for profit calculation)
            lot_breakdown = {
                "model": model,
                "capacity": capacity,
                "color_quantities": {color.lower(): 1} if color else {"unknown": 1},
                "total_units": 1,
                "is_mixed_lot": False,
            }
            
            # Calculate per-unit profit: divide net_cost by pack_size
            net_per_unit = net_cost / pack_size if pack_size > 0 else net_cost
            profit_per_unit = net_per_unit - total_sale if net_per_unit else 0.0
            
            matches_to_insert.append({
                'is_alternative': 0,
                'title': f"Lexmark {part_number} {variant.get('variant_label', '')}".strip(),
                'asin': variant.get('asin', ''),
                'bsr': int(variant.get('bsr')) if variant.get('bsr') else None,
                'sellable': 1 if variant.get('sellable') else 0,
                'net_cost': float(net_per_unit) if net_per_unit else 0.0,
                'profit': float(profit_per_unit),
                'pack_size': pack_size,
                'color': color,
                'lot_breakdown': json.dumps(sanitize_for_json(lot_breakdown)),
                'total_units': 1,
            })
    
    return matches_to_insert


def run_backfill():
    """Main backfill function - processes ALL orders. Use for initial setup."""
    print("=" * 60)
    print("BACKFILL MATCHES FROM ORDER HISTORY")
    print("=" * 60)
    print()
    
    # Initialize DB
    init_db()
    
    # Get all order history rows
    print("Fetching order history...")
    orders = get_all_order_history_rows()
    print(f"  Found {len(orders)} orders in database.")
    print()
    
    if not orders:
        print("No orders to process. Make sure order_history has been populated first.")
        print("Run the main.py or order history fetch before this script.")
        return 0
    
    # Run the backfill
    result = backfill_orders(orders, verbose=True)
    
    print()
    print("=" * 60)
    print("BACKFILL COMPLETE")
    print("=" * 60)
    
    # Print summary
    _print_db_stats()
    
    return result['matched']


def backfill_orders(orders: list, verbose: bool = False) -> dict:
    """
    Core backfill logic - processes a list of order rows for ALL brands.
    
    This is the reusable function for incremental backfill.
    
    Args:
        orders: List of order_history row dicts to process
        verbose: If True, print progress messages
        
    Returns:
        Dict with stats: {processed, matched, skipped, errors, by_brand}
    """
    if not orders:
        return {'processed': 0, 'matched': 0, 'skipped': 0, 'errors': 0, 'by_brand': {}}
    
    # Load products for ALL brands
    if verbose:
        print("Loading products from SQL database...")
    
    products_data = {}
    
    # Canon
    canon_df = get_canon_products()
    products_data['canon_df'] = canon_df
    if verbose:
        count = len(canon_df) if canon_df is not None and not canon_df.empty else 0
        print(f"  Canon: {count} products")
    
    # Xerox
    xerox_df = get_xerox_products()
    if xerox_df is not None and not xerox_df.empty:
        # Build SKU index for Xerox matching
        products_data['xerox_sku_map'] = build_xerox_sku_index(xerox_df)
        if verbose:
            print(f"  Xerox: {len(xerox_df)} products")
    else:
        products_data['xerox_sku_map'] = {}
        if verbose:
            print("  Xerox: 0 products")
    
    # Lexmark
    lexmark_df = get_lexmark_products()
    if lexmark_df is not None and not lexmark_df.empty:
        # Build part number index for Lexmark matching
        products_data['lexmark_pn_map'] = build_lexmark_pn_index(lexmark_df)
        if verbose:
            print(f"  Lexmark: {len(lexmark_df)} products")
    else:
        products_data['lexmark_pn_map'] = {}
        if verbose:
            print("  Lexmark: 0 products")
    
    if verbose:
        print()
        print("Processing orders...")
    
    processed = 0
    matched = 0
    skipped = 0
    errors = 0
    by_brand = {'canon': 0, 'xerox': 0, 'lexmark': 0, 'unknown': 0}
    
    for row in orders:
        item_id = row.get('item_id', '')
        item_title = row.get('item_title', '')
        order_id = row.get('order_id', '')
        transaction_id = row.get('transaction_id', '')
        
        if not item_title:
            skipped += 1
            continue
        
        # Skip if already has match data
        if row.get('match1_asin'):
            skipped += 1
            continue
        
        try:
            result = backfill_matches_for_order(row, products_data)
            brand = result.get('brand', 'unknown')
            
            if result.get('matches'):
                # Create a synthetic message entry if needed
                if item_id and not check_message_exists(item_id):
                    listing_id = f"v1|{item_id}|0"
                    
                    try:
                        price = float(row.get('transaction_price', 0) or 0)
                    except:
                        price = 0.0
                    
                    message_id = insert_message(
                        listing_id=listing_id,
                        timestamp=int(time.time()),
                        listed_time=row.get('created_time', ''),
                        link=f"https://www.ebay.com/itm/{item_id}",
                        type_="Backfill",
                        quantity="1",
                        price=price,
                        shipping=0.0,
                        total=price,
                        message=f"Backfilled from order history: {item_title}"
                    )
                    
                    # Insert matches
                    for m in result['matches']:
                        insert_match(
                            message_id=message_id,
                            is_alternative=m['is_alternative'],
                            title=m['title'],
                            asin=m['asin'],
                            bsr=m['bsr'],
                            sellable=m['sellable'],
                            net_cost=m['net_cost'],
                            profit=m['profit'],
                            pack_size=m['pack_size'],
                            color=m['color'],
                            lot_breakdown=m.get('lot_breakdown'),
                            total_units=m.get('total_units'),
                            is_mixed_lot=1 if m.get('lot_breakdown') else 0
                        )
                
                # Update order_history with match columns
                if result.get('columns'):
                    update_order_history_match_columns(order_id, transaction_id, result['columns'])
                
                matched += 1
                by_brand[brand] = by_brand.get(brand, 0) + 1
            else:
                by_brand['unknown'] = by_brand.get('unknown', 0) + 1
            
            processed += 1
            
            # Progress indicator
            if verbose and processed % 10 == 0:
                print(f"  Processed {processed}/{len(orders)} orders...")
                
        except Exception as e:
            if verbose:
                print(f"  ERROR processing order {order_id}: {e}")
                import traceback
                traceback.print_exc()
            errors += 1
    
    if verbose:
        print()
        print(f"Processing complete:")
        print(f"  Total orders: {len(orders)}")
        print(f"  Processed: {processed}")
        print(f"  Matched: {matched}")
        print(f"    - Canon: {by_brand.get('canon', 0)}")
        print(f"    - Xerox: {by_brand.get('xerox', 0)}")
        print(f"    - Lexmark: {by_brand.get('lexmark', 0)}")
        print(f"    - Unknown brand: {by_brand.get('unknown', 0)}")
        print(f"  Skipped (no title or already matched): {skipped}")
        print(f"  Errors: {errors}")
    
    # Populate purchased_units for the processed orders
    _populate_units_for_orders(orders, verbose)
    
    return {
        'processed': processed,
        'matched': matched,
        'skipped': skipped,
        'errors': errors,
        'by_brand': by_brand,
    }


def check_and_backfill(verbose: bool = False) -> dict:
    """
    Check for unprocessed orders and backfill if any found.
    
    This is the main entry point for incremental backfill, designed to be
    called after order history sync. It only processes orders that don't
    have purchased_units entries yet.
    
    Args:
        verbose: If True, print progress messages
        
    Returns:
        Dict with stats: {processed, matched, skipped, errors, status}
        status will be 'no_action' if no orders needed processing
    """
    # Initialize DB (idempotent)
    init_db()
    
    # Check current status
    status = get_backfill_status()
    unprocessed_count = status['unprocessed_orders']
    
    if unprocessed_count == 0:
        if verbose:
            _log("Backfill check: No unprocessed orders found")
        return {
            'processed': 0,
            'matched': 0,
            'skipped': 0,
            'errors': 0,
            'status': 'no_action',
        }
    
    # Get the unprocessed orders
    unprocessed = get_unprocessed_orders()
    
    if verbose:
        _log(f"Backfill check: Found {len(unprocessed)} unprocessed orders, starting backfill...")
    
    # Run backfill on just these orders
    result = backfill_orders(unprocessed, verbose=verbose)
    result['status'] = 'completed'
    
    if verbose:
        _log(f"Backfill complete: {result['matched']} orders matched, {result['errors']} errors")
    
    return result


def _populate_units_for_orders(orders: list, verbose: bool = False):
    """
    Populate purchased_units for a specific list of orders.
    
    For orders with lot_breakdown data, creates color-specific entries.
    For orders without matches, creates a sentinel entry with NULL ASIN
    to mark the order as "processed" for idempotency purposes.
    """
    if verbose:
        print()
        print("Populating purchased_units table...")
    
    # Re-fetch the orders to get updated match columns
    order_ids = {(r['order_id'], r['transaction_id']) for r in orders}
    
    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    all_units = []
    orders_with_units = set()
    
    for order_id, transaction_id in order_ids:
        cur.execute(
            f'SELECT * FROM {ORDER_HISTORY_TABLE} WHERE order_id = ? AND transaction_id = ?',
            (order_id, transaction_id)
        )
        row = cur.fetchone()
        if row:
            row_dict = dict(row)
            units = expand_order_to_purchased_units(row_dict)
            if units:
                all_units.extend(units)
                orders_with_units.add((order_id, transaction_id))
    
    conn.close()
    
    # Insert units for matched orders
    if all_units:
        inserted, skipped_units = insert_purchased_units_batch(all_units)
        if verbose:
            print(f"  Inserted {inserted} purchased units, {skipped_units} skipped (duplicates)")
    
    # For orders without lot_breakdown, insert sentinel entries
    # This marks them as "processed" so we don't keep retrying
    orders_without_match = order_ids - orders_with_units
    if orders_without_match:
        sentinel_units = []
        for order_id, transaction_id in orders_without_match:
            sentinel_units.append({
                'order_id': order_id,
                'transaction_id': transaction_id,
                'item_id': None,
                'asin': None,  # NULL ASIN marks as "no match"
                'color': 'unmatched',
                'quantity': 0,
                'unit_cost': 0.0,
                'purchased_date': None,
                'bsr': 0,
                'net_per_unit': 0.0,
                'model': None,
                'capacity': None,
                'lot_type': 'unmatched',
                'match_index': 0,
            })
        
        sentinel_inserted, sentinel_skipped = insert_purchased_units_batch(sentinel_units)
        if verbose:
            print(f"  Inserted {sentinel_inserted} sentinel entries for unmatched orders, {sentinel_skipped} skipped")


def _print_db_stats():
    """Print database statistics."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute(f'SELECT COUNT(*) FROM {MESSAGES_TABLE}')
    msg_count = cur.fetchone()[0]
    
    cur.execute(f'SELECT COUNT(*) FROM {MATCHES_TABLE}')
    match_count = cur.fetchone()[0]
    
    cur.execute('SELECT COUNT(*) FROM purchased_units')
    pu_count = cur.fetchone()[0]
    
    conn.close()
    
    print()
    print("Database stats:")
    print(f"  Messages: {msg_count}")
    print(f"  Matches: {match_count}")
    print(f"  Purchased Units: {pu_count}")


if __name__ == "__main__":
    run_backfill()
