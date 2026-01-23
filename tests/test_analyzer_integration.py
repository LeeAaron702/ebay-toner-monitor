#!/usr/bin/env python
"""Integration test for analyzer workflow with engine data."""

import sys
from pathlib import Path

# Add project root to path for pytest compatibility
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.products_db import get_canon_products, get_xerox_products, get_lexmark_products

def test_engine_data_integration():
    """Test that engine helper functions return correct data with BSR/net values."""
    
    # Load product data
    canon_df = get_canon_products()
    xerox_df = get_xerox_products()
    lexmark_df = get_lexmark_products()
    
    print("=== Engine Data Integration Test ===")
    print(f"Canon products: {len(canon_df)} rows")
    print(f"Xerox products: {len(xerox_df)} rows")
    print(f"Lexmark products: {len(lexmark_df)} rows")
    
    # Check required columns exist
    canon_cols = ['model', 'capacity', 'ASIN', 'BSR', 'net', 'sellable']
    xerox_cols = ['part_number', 'asin', 'net', 'bsr', 'sellable']
    lexmark_cols = ['model_family', 'part_number', 'asin', 'net_cost', 'bsr', 'sellable']
    
    canon_ok = all(c in canon_df.columns for c in canon_cols)
    xerox_ok = all(c in xerox_df.columns for c in xerox_cols)
    lexmark_ok = all(c in lexmark_df.columns for c in lexmark_cols)
    
    print(f"\nCanon columns present: {canon_ok}")
    print(f"Xerox columns present: {xerox_ok}")
    print(f"Lexmark columns present: {lexmark_ok}")
    
    # Check that we have BSR/net values from analyzer
    if not canon_df.empty:
        canon_with_bsr = canon_df['BSR'].notna().sum()
        canon_with_net = canon_df['net'].notna().sum()
        print(f"\nCanon: {canon_with_bsr}/{len(canon_df)} have BSR, {canon_with_net}/{len(canon_df)} have net")
        
        # Sample data
        sample = canon_df[['model', 'ASIN', 'BSR', 'net', 'sellable']].head(3)
        print(f"\nCanon sample:\n{sample.to_string()}")
    
    if not xerox_df.empty:
        xerox_with_bsr = xerox_df['bsr'].notna().sum()
        xerox_with_net = xerox_df['net'].notna().sum()
        print(f"\nXerox: {xerox_with_bsr}/{len(xerox_df)} have BSR, {xerox_with_net}/{len(xerox_df)} have net")
    
    # Assert all checks pass
    assert canon_ok, f"Missing Canon columns: {[c for c in canon_cols if c not in canon_df.columns]}"
    assert xerox_ok, f"Missing Xerox columns: {[c for c in xerox_cols if c not in xerox_df.columns]}"
    assert lexmark_ok, f"Missing Lexmark columns: {[c for c in lexmark_cols if c not in lexmark_df.columns]}"
    
    print("\n✓ All integration tests passed!")


if __name__ == "__main__":
    test_engine_data_integration()
