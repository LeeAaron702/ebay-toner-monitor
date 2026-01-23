"""
Analyzer Tools Excel parser.

Parses the Excel export from analyzer.tools and extracts metrics
for bulk updating the products database.

Key columns from analyzer.tools:
    - ASIN: Product identifier
    - Seller Proceeds: Net proceeds after Amazon fees (THIS IS NET_COST)
    - Avg Buybox 90d: Average buybox price (for reference)
    - Avg Sales Rank 30d / Avg Sales Rank 90d: Best Seller Rank
    - New FBA Offers / New FBM Offers: Competition data
"""

import os
from typing import List, Dict, Any, Optional
from pathlib import Path

import pandas as pd


# Column mappings from analyzer.tools Excel to our internal format
ASIN_COLUMN = "ASIN"
SELLER_PROCEEDS_COLUMN = "Seller Proceeds"  # This is what we actually get paid after Amazon fees
BUYBOX_COLUMN = "Avg Buybox 90d"
BSR_30D_COLUMN = "Avg Sales Rank 30d"
BSR_90D_COLUMN = "Avg Sales Rank 90d"
NEW_FBA_OFFERS_COLUMN = "New FBA Offers"


def parse_analyzer_excel(filepath: str) -> List[Dict[str, Any]]:
    """
    Parse analyzer.tools Excel export and extract metrics. 
    
    Args:
        filepath: Path to the Excel file (.xlsx)
        
    Returns:
        List of dicts with: asin, net_cost, bsr, sellable
        Ready for bulk_update_metrics()
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Excel file not found: {filepath}")
    
    # Load Excel file
    df = pd.read_excel(filepath, engine="openpyxl")
    
    # Normalize column names (strip whitespace)
    df.columns = df.columns.str.strip()
    
    # Validate required columns
    if ASIN_COLUMN not in df.columns:
        raise ValueError(f"Required column '{ASIN_COLUMN}' not found in Excel file")
    
    metrics = []
    
    for _, row in df.iterrows():
        asin = str(row.get(ASIN_COLUMN, "")).strip()
        
        # Skip empty ASINs
        if not asin or asin == "nan":
            continue
        
        metric = {"asin": asin}
        
        # Extract net_cost from Seller Proceeds - this is what we actually receive after Amazon fees
        # This is the TARGET PRICE we should pay on eBay to be profitable
        seller_proceeds = row.get(SELLER_PROCEEDS_COLUMN)
        if pd.notna(seller_proceeds):
            try:
                proceeds_val = float(seller_proceeds)
                # Use seller proceeds directly as net_cost (max we should pay)
                metric["net_cost"] = round(proceeds_val, 2)
            except (ValueError, TypeError):
                pass
        
        # Extract BSR (prefer 30d, fallback to 90d)
        bsr = row.get(BSR_30D_COLUMN)
        if pd.isna(bsr):
            bsr = row.get(BSR_90D_COLUMN)
        
        if pd.notna(bsr):
            try:
                metric["bsr"] = int(float(bsr))
            except (ValueError, TypeError):
                pass
        
        # Determine sellable status
        # Consider sellable if there are FBA offers or seller proceeds > 0
        seller_proceeds = row.get(SELLER_PROCEEDS_COLUMN)
        fba_offers = row.get(NEW_FBA_OFFERS_COLUMN)
        
        # Default to current status (don't change) if we can't determine
        # Only mark as not sellable if BSR is very high (>1,000,000) or no data
        if pd.notna(bsr):
            try:
                bsr_val = int(float(bsr))
                # High BSR (>1M) suggests low demand
                metric["sellable"] = bsr_val < 1_000_000
            except (ValueError, TypeError):
                pass
        
        metrics.append(metric)
    
    return metrics


def parse_and_summarize(filepath: str) -> Dict[str, Any]:
    """
    Parse Excel and return summary statistics.
    
    Returns:
        Dict with metrics list and summary stats.
    """
    metrics = parse_analyzer_excel(filepath)
    
    # Calculate summary
    with_net_cost = sum(1 for m in metrics if "net_cost" in m)
    with_bsr = sum(1 for m in metrics if "bsr" in m)
    sellable_count = sum(1 for m in metrics if m.get("sellable", True))
    
    return {
        "metrics": metrics,
        "summary": {
            "total_asins": len(metrics),
            "with_net_cost": with_net_cost,
            "with_bsr": with_bsr,
            "sellable": sellable_count,
            "not_sellable": len(metrics) - sellable_count,
        }
    }


def extract_full_data(filepath: str) -> pd.DataFrame:
    """
    Extract all data from analyzer Excel as DataFrame.
    
    Useful for debugging or when you need more than just metrics.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Excel file not found: {filepath}")
    
    df = pd.read_excel(filepath, engine="openpyxl")
    df.columns = df.columns.str.strip()
    
    return df


if __name__ == "__main__":
    import sys
    import json
    
    if len(sys.argv) < 2:
        print("Usage: python analyzer_parser.py <excel_path>")
        sys.exit(1)
    
    filepath = sys.argv[1]
    
    try:
        result = parse_and_summarize(filepath)
        print(f"\n=== Analyzer Excel Summary ===")
        print(f"Total ASINs: {result['summary']['total_asins']}")
        print(f"With net_cost: {result['summary']['with_net_cost']}")
        print(f"With BSR: {result['summary']['with_bsr']}")
        print(f"Sellable: {result['summary']['sellable']}")
        print(f"Not sellable: {result['summary']['not_sellable']}")
        
        # Print first 5 metrics as sample
        print(f"\nSample metrics (first 5):")
        for m in result['metrics'][:5]:
            print(f"  {m}")
            
    except Exception as e:
        print(f"Error parsing Excel: {e}")
        sys.exit(1)
