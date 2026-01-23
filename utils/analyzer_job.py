"""
Analyzer Tools daily job orchestrator.

Runs the complete analyzer.tools workflow:
1. Export ASINs from products database to CSV
2. Upload to analyzer.tools and wait for processing
3. Download Excel results
4. Parse metrics and update products database

Scheduled to run daily at 7am PST.
"""

import os
import csv
import tempfile
import traceback
from datetime import datetime
from typing import Optional

from db.products_db import list_products, bulk_update_metrics
from utils.analyzer_scraper import run_analyzer_sync
from utils.analyzer_parser import parse_analyzer_excel

# Timezone for scheduling
try:
    from pytz import timezone
    LOCAL_TZ = timezone("America/Los_Angeles")
except ImportError:
    import zoneinfo
    LOCAL_TZ = zoneinfo.ZoneInfo("America/Los_Angeles")


def export_asins_to_csv(output_path: Optional[str] = None) -> tuple[str, str]:
    """
    Export all ASINs to a CSV file for analyzer upload.
    
    Args:
        output_path: Path to save CSV (optional, uses temp file if not provided)
        
    Returns:
        Tuple of (path to the generated CSV file, unique identifier for this upload)
    """
    # Get all products
    products = list_products(limit=10000)
    asins = sorted(set(p['asin'] for p in products if p.get('asin')))
    
    if not asins:
        raise ValueError("No products with ASINs found in database")
    
    # Generate unique identifier with timestamp
    upload_id = datetime.now(LOCAL_TZ).strftime("%Y%m%d_%H%M%S")
    
    # Generate output path if not provided - include timestamp for uniqueness
    if not output_path:
        output_path = os.path.join(tempfile.gettempdir(), f"asins_{upload_id}.csv")
    
    # Write CSV
    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['asin'])
        for asin in asins:
            writer.writerow([asin])
    
    print(f"[Analyzer Job] Exported {len(asins)} ASINs to {output_path}")
    print(f"[Analyzer Job] Upload ID: {upload_id} (will look for this in results)")
    return output_path, upload_id


def run_analyzer_job() -> dict:
    """
    Run the complete analyzer workflow.
    
    Returns:
        Dict with stats: {asins_exported, excel_path, metrics_updated, not_found, errors}
    """
    stats = {
        'asins_exported': 0,
        'excel_path': None,
        'metrics_updated': 0,
        'not_found': 0,
        'errors': [],
        'success': False,
    }
    
    ts = datetime.now(LOCAL_TZ).strftime("%b %d %Y, %I:%M %p %Z")
    print(f"\n{'='*60}")
    print(f"[Analyzer Job] Starting at {ts}")
    print(f"{'='*60}")
    
    try:
        # Step 1: Export ASINs (with unique timestamp identifier)
        csv_path, upload_id = export_asins_to_csv()
        with open(csv_path, 'r') as f:
            stats['asins_exported'] = sum(1 for _ in f) - 1  # Subtract header
        
        # Step 2: Run analyzer scraper - save to analyzer_downloads folder for reference
        # Use env var if set, otherwise default to analyzer_downloads in project root
        download_dir = os.getenv("ANALYZER_DOWNLOAD_PATH")
        if not download_dir:
            # Default to analyzer_downloads folder in project directory
            download_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "analyzer_downloads")
        os.makedirs(download_dir, exist_ok=True)
        print(f"[Analyzer Job] Running scraper, downloads to: {download_dir}")
        
        excel_path = run_analyzer_sync(csv_path, download_dir, upload_id)
        
        if not excel_path:
            stats['errors'].append("Analyzer scraper failed - no Excel file returned")
            print(f"[Analyzer Job] ERROR: Scraper returned no file")
            return stats
        
        stats['excel_path'] = excel_path
        print(f"[Analyzer Job] Downloaded: {excel_path}")
        
        # Step 3: Parse Excel
        metrics = parse_analyzer_excel(excel_path)
        print(f"[Analyzer Job] Parsed {len(metrics)} metrics from Excel")
        
        # Step 4: Update database
        result = bulk_update_metrics(metrics)
        stats['metrics_updated'] = result['updated']
        stats['not_found'] = result['not_found']
        stats['errors'].extend(result.get('errors', []))
        
        print(f"[Analyzer Job] Database updated: {result['updated']} products, {result['not_found']} not found")
        
        stats['success'] = True
        
    except Exception as e:
        error_msg = f"Analyzer job failed: {str(e)}"
        stats['errors'].append(error_msg)
        print(f"[Analyzer Job] ERROR: {error_msg}")
        traceback.print_exc()
    
    finally:
        # Cleanup temp CSV file only (keep Excel downloads for reference)
        try:
            if 'csv_path' in locals() and csv_path and os.path.exists(csv_path):
                os.remove(csv_path)
                print(f"[Analyzer Job] Cleaned up temp CSV: {csv_path}")
        except Exception as e:
            print(f"[Analyzer Job] Warning: could not clean up CSV: {e}")
        
        # Note: Excel files are kept in analyzer_downloads/ for reference
        if 'excel_path' in locals() and excel_path:
            print(f"[Analyzer Job] Excel file saved for reference: {excel_path}")
    
    ts = datetime.now(LOCAL_TZ).strftime("%b %d %Y, %I:%M %p %Z")
    status = "SUCCESS" if stats['success'] else "FAILED"
    print(f"[Analyzer Job] {status} at {ts}")
    print(f"{'='*60}\n")
    
    return stats


if __name__ == "__main__":
    # Test run
    result = run_analyzer_job()
    print(f"\nResult: {result}")
