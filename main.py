# main.py

import os
import time
import base64
import traceback
import requests
import threading

from datetime import datetime, timedelta
from dotenv import load_dotenv

from engine.canon import canon, LOCAL_TZ
from engine.xerox import xerox
from engine.lexmark import lexmark
from order_history.ebay_order_history import run_order_history, send_stats_report, REPORT_SCHEDULE
from db.products_db import (
    init_products_db,
    get_canon_products,
    get_xerox_products,
    get_lexmark_products,
)
from utils.analyzer_job import run_analyzer_job

load_dotenv()

RUN_INTERVAL          = int(os.getenv("RUN_INTERVAL", "60"))  # default 1 min
CLIENT_ID              = os.getenv("EBAY_APP_ID")
CLIENT_SECRET          = os.getenv("EBAY_CLIENT_SECRET")
TOKEN_URL              = "https://api.ebay.com/identity/v1/oauth2/token"

ORDER_HISTORY_INTERVAL = 3600  # 1 hour between order history fetches
ANALYZER_HOUR = 7  # Run analyzer job at 7am PST daily

def get_token() -> str:
    auth = f"{CLIENT_ID}:{CLIENT_SECRET}".encode()
    enc  = base64.b64encode(auth).decode()
    headers = {
        "Content-Type":  "application/x-www-form-urlencoded",
        "Authorization": f"Basic {enc}"
    }
    data = {
        "grant_type": "client_credentials",
        "scope":      "https://api.ebay.com/oauth/api_scope"
    }
    resp = requests.post(TOKEN_URL, headers=headers, data=data)
    resp.raise_for_status()
    return resp.json().get("access_token", "")

def _run_canon_job(token: str, refresh_event: threading.Event) -> None:
    """
    Run Canon engine with products loaded from SQL database.
    """
    try:
        df = get_canon_products()
        if df.empty:
            print("LOG - Main.py - Canon: No products in database, skipping")
            return
        canon(token, df)
    except requests.HTTPError as e:
        if e.response.status_code == 401:
            print("Token expired; fetching new token")
            refresh_event.set()
        else:
            print(f"HTTP error in canon(): {e}")
    except Exception:
        print("Unexpected error in canon():")
        traceback.print_exc()


def _run_xerox_job(token: str, refresh_event: threading.Event) -> None:
    """
    Run Xerox engine with products loaded from SQL database.
    """
    try:
        df = get_xerox_products()
        if df.empty:
            print("LOG - Main.py - Xerox: No products in database, skipping")
            return
        xerox(token, df)
    except requests.HTTPError as e:
        if e.response.status_code == 401:
            print("Xerox token expired; fetching new token")
            refresh_event.set()
        else:
            print(f"HTTP error in xerox(): {e}")
    except Exception:
        print("Unexpected error in xerox():")
        traceback.print_exc()


def _run_lexmark_job(token: str, refresh_event: threading.Event) -> None:
    """
    Run Lexmark engine with products loaded from SQL database.
    """
    try:
        df = get_lexmark_products()
        if df.empty:
            print("LOG - Main.py - Lexmark: No products in database, skipping")
            return
        lexmark(token, df)
    except requests.HTTPError as e:
        if e.response.status_code == 401:
            print("Lexmark token expired; fetching new token")
            refresh_event.set()
        else:
            print(f"HTTP error in lexmark(): {e}")
    except Exception:
        print("Unexpected error in lexmark():")
        traceback.print_exc()


def _run_order_history_job() -> None:
    """Run order history fetch in a thread-safe manner."""
    try:
        new_count = run_order_history()
        ts = datetime.now(LOCAL_TZ).strftime("%b %d %Y, %I:%M %p %Z")
        print(f"LOG - Main.py - Order history: added {new_count} new rows at {ts}")
    except Exception:
        print("Unexpected error in order_history:")
        traceback.print_exc()


def _run_stats_report_job(report_hour: int, start_hour: int) -> None:
    """Send stats report to Telegram for a specific time range."""
    try:
        send_stats_report(report_hour, start_hour)
        ts = datetime.now(LOCAL_TZ).strftime("%b %d %Y, %I:%M %p %Z")
        print(f"LOG - Main.py - Stats report ({start_hour}:00-{report_hour}:00) sent at {ts}")
    except Exception:
        print("Unexpected error in stats_report:")
        traceback.print_exc()


def _run_analyzer_job(startup: bool = False) -> dict:
    """
    Run analyzer.tools workflow to update BSR/net_cost metrics.
    
    Args:
        startup: If True, this is the initial startup run (blocks engines until complete)
        
    Returns:
        Result dict from analyzer job
    """
    try:
        result = run_analyzer_job()
        ts = datetime.now(LOCAL_TZ).strftime("%b %d %Y, %I:%M %p %Z")
        if result['success']:
            print(f"LOG - Main.py - Analyzer job completed at {ts}: {result['metrics_updated']} products updated")
        else:
            print(f"LOG - Main.py - Analyzer job failed at {ts}: {result['errors']}")
        return result
    except Exception:
        print("Unexpected error in analyzer_job:")
        traceback.print_exc()
        return {'success': False, 'errors': ['Exception during analyzer job']}


def _get_next_report_times() -> dict:
    """
    Calculate the next scheduled report times for all reports.
    Returns a dict mapping report_hour to (timestamp, start_hour).
    """
    now = datetime.now(LOCAL_TZ)
    report_times = {}
    
    for report_hour, start_hour in REPORT_SCHEDULE:
        scheduled_time = now.replace(hour=report_hour, minute=0, second=0, microsecond=0)
        
        if now >= scheduled_time:
            # Already past this time today, schedule for tomorrow
            scheduled_time = scheduled_time + timedelta(days=1)
        
        report_times[report_hour] = (scheduled_time.timestamp(), start_hour)
    
    return report_times


def _get_next_analyzer_time() -> float:
    """
    Calculate the next scheduled time for analyzer job (7am PST daily).
    Returns a timestamp.
    """
    now = datetime.now(LOCAL_TZ)
    scheduled_time = now.replace(hour=ANALYZER_HOUR, minute=0, second=0, microsecond=0)
    
    if now >= scheduled_time:
        # Already past 7am today, schedule for tomorrow
        scheduled_time = scheduled_time + timedelta(days=1)
    
    return scheduled_time.timestamp()


def monitor():
    """
    Main monitoring loop - orchestrates Canon, Xerox, Lexmark engines.
    
    Architecture:
    - Loads product data from SQL database (no more Google Sheets)
    - BLOCKS on startup analyzer job to get fresh BSR/net_cost data before engines run
    - Runs each engine in parallel threads every RUN_INTERVAL seconds
    - Token refresh on 401 errors
    - Order history fetch hourly
    - Stats reports at 9am, 12pm, 5pm PST
    - Analyzer job at 7am PST daily
    """
    # Initialize the products database
    init_products_db()
    
    # =========================================================================
    # STARTUP ANALYZER JOB - ENGINES WILL NOT RUN UNTIL THIS COMPLETES
    # =========================================================================
    print("\n" + "="*70)
    print("  STARTUP PHASE: ANALYZER SCRAPE")
    print("="*70)
    print(f"  Started at: {datetime.now(LOCAL_TZ).strftime('%b %d %Y, %I:%M %p %Z')}")
    print("  Status: ENGINES BLOCKED - Waiting for analyzer to complete...")
    print("="*70 + "\n")
    
    startup_start = time.time()
    analyzer_result = _run_analyzer_job(startup=True)
    startup_elapsed = time.time() - startup_start
    
    print("\n" + "="*70)
    print("  STARTUP ANALYZER COMPLETE")
    print("="*70)
    if analyzer_result.get('success'):
        print(f"  ✓ Status: SUCCESS")
        print(f"  ✓ Products updated: {analyzer_result.get('metrics_updated', 0)}")
        print(f"  ✓ Not found: {analyzer_result.get('not_found', 0)}")
    else:
        print(f"  ✗ Status: FAILED")
        print(f"  ✗ Errors: {analyzer_result.get('errors', [])}")
        print(f"  ⚠ Engines will start with existing database values")
    print(f"  ⏱ Duration: {startup_elapsed:.1f} seconds")
    print("="*70 + "\n")
    
    # =========================================================================
    # NOW INITIALIZE ENGINES
    # =========================================================================
    print("="*70)
    print("  INITIALIZING MONITOR ENGINES")
    print("="*70)
    print(f"  Time: {datetime.now(LOCAL_TZ).strftime('%b %d %Y, %I:%M %p %Z')}")
    print("="*70 + "\n")
    
    token = get_token()
    print("[Monitor] ✓ eBay API token acquired")
    
    next_order_history = time.time()  # Run immediately on startup, then hourly
    next_reports = _get_next_report_times()  # Schedule for 9am, 12pm, 5pm PST
    next_analyzer = _get_next_analyzer_time()  # Schedule for 7am PST daily
    
    # Log initial product counts from SQL
    canon_count = len(get_canon_products())
    xerox_count = len(get_xerox_products())
    lexmark_count = len(get_lexmark_products())
    
    print(f"[Monitor] ✓ Canon products: {canon_count} rows loaded")
    print(f"[Monitor] ✓ Xerox products: {xerox_count} rows loaded")
    print(f"[Monitor] ✓ Lexmark products: {lexmark_count} rows loaded")
    
    # Log scheduled jobs
    print("\n[Monitor] Scheduled jobs:")
    for report_hour, (ts, start_hour) in sorted(next_reports.items()):
        next_dt = datetime.fromtimestamp(ts, tz=LOCAL_TZ)
        print(f"  • Stats report ({start_hour}:00-{report_hour}:00): {next_dt.strftime('%b %d %Y, %I:%M %p %Z')}")
    next_analyzer_dt = datetime.fromtimestamp(next_analyzer, tz=LOCAL_TZ)
    print(f"  • Analyzer job: {next_analyzer_dt.strftime('%b %d %Y, %I:%M %p %Z')}")
    
    print("\n" + "="*70)
    print("  ENGINES STARTING - Monitor loop active")
    print("="*70 + "\n")

    while True:
        loop_start = time.time()
        now = loop_start

        refresh_event = threading.Event()
        threads = []

        # Run engines - each loads fresh data from SQL
        canon_thread = threading.Thread(target=_run_canon_job, args=(token, refresh_event))
        canon_thread.start()
        threads.append(canon_thread)

        xerox_thread = threading.Thread(target=_run_xerox_job, args=(token, refresh_event))
        xerox_thread.start()
        threads.append(xerox_thread)

        lexmark_thread = threading.Thread(target=_run_lexmark_job, args=(token, refresh_event))
        lexmark_thread.start()
        threads.append(lexmark_thread)

        # Run order history on its own hourly schedule
        if now >= next_order_history:
            order_history_thread = threading.Thread(target=_run_order_history_job)
            order_history_thread.start()
            threads.append(order_history_thread)
            next_order_history = time.time() + ORDER_HISTORY_INTERVAL

        # Check each scheduled stats report
        for report_hour, (scheduled_ts, start_hour) in list(next_reports.items()):
            if now >= scheduled_ts:
                stats_thread = threading.Thread(target=_run_stats_report_job, args=(report_hour, start_hour))
                stats_thread.start()
                threads.append(stats_thread)
                
                # Schedule for tomorrow
                next_time = datetime.fromtimestamp(scheduled_ts, tz=LOCAL_TZ) + timedelta(days=1)
                next_reports[report_hour] = (next_time.timestamp(), start_hour)
                print(f"Next stats report ({start_hour}:00-{report_hour}:00) scheduled for: {next_time.strftime('%b %d %Y, %I:%M %p %Z')}")

        # Run analyzer job at 7am PST daily
        if now >= next_analyzer:
            analyzer_thread = threading.Thread(target=_run_analyzer_job, kwargs={'startup': False})
            analyzer_thread.start()
            threads.append(analyzer_thread)
            
            # Schedule for tomorrow
            next_analyzer_dt = datetime.fromtimestamp(next_analyzer, tz=LOCAL_TZ) + timedelta(days=1)
            next_analyzer = next_analyzer_dt.timestamp()
            print(f"Next analyzer job scheduled for: {next_analyzer_dt.strftime('%b %d %Y, %I:%M %p %Z')}")

        for t in threads:
            t.join()

        if refresh_event.is_set():
            token = get_token()

        elapsed = time.time() - loop_start
        sleep_for = max(0, RUN_INTERVAL - elapsed)
        if sleep_for:
            time.sleep(sleep_for)


if __name__ == "__main__":
    monitor()


