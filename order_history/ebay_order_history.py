import base64
import csv
import json
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from urllib.parse import urlparse, parse_qs, quote

import requests
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

# Import DB functions from listings_db
from db.listings_db import (
    DB_PATH as LISTINGS_DB_PATH,
    init_db,
    upsert_order_history,
    get_daily_order_stats,
    get_order_stats_for_time_range,
    get_order_item_match_counts,
    get_order_items_for_time_range,
    expand_order_to_purchased_units,
    insert_purchased_units_batch,
)

# Import Telegram service
from utils.telegram_service import send_telegram_message

# =====================================================================
# LOAD ENVIRONMENT VARIABLES
# =====================================================================

load_dotenv()

CLIENT_ID = os.getenv("EBAY_APP_ID")
CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET")
RU_NAME = os.getenv("RU_NAME")
DEV_ID = os.getenv("DEV_ID", "")
CERT_ID = os.getenv("CERT_ID", CLIENT_SECRET)
GETORDERS_DAYS = int(os.getenv("GETORDERS_DAYS", "30"))

# Multiple eBay accounts (each with its own token file)
ACCOUNTS = [
    {
        "id": "personal",
        "token_file": Path("ebay_tokens_personal.json"),
    },
    {
        "id": "business",
        "token_file": Path("ebay_tokens_business.json"),
    },
]

# Use repo root for output files to ensure consistent paths
REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_CSV = REPO_ROOT / "ebay_order_history.csv"
LISTINGS_DB = Path(LISTINGS_DB_PATH)
MAX_MATCHES = 4  # Number of match columns to include in CSV

EBAY_OAUTH_TOKEN_URL = "https://api.ebay.com/identity/v1/oauth2/token"
EBAY_AUTH_URL_BASE = "https://auth.ebay.com/oauth2/authorize"
EBAY_TRADING_ENDPOINT = "https://api.ebay.com/ws/api.dll"

# XML namespace for Trading API
EBAY_NAMESPACE = {"e": "urn:ebay:apis:eBLBaseComponents"}


# =====================================================================
# UTILS
# =====================================================================

def debug(msg: str) -> None:
    """Log debug messages to stderr."""
    print(msg, file=sys.stderr)


def save_tokens(tokens: Dict, token_file: Path) -> None:
    """Persist tokens (including refresh_token) to disk for a specific account."""
    token_file.write_text(json.dumps(tokens, indent=2), encoding="utf-8")
    debug(f"Saved tokens → {token_file}")


def load_tokens(token_file: Path) -> Dict:
    """Load tokens for a specific account from disk if present."""
    if not token_file.exists():
        return {}
    return json.loads(token_file.read_text(encoding="utf-8"))


def get_basic_auth_header() -> str:
    """Return Basic auth header value for OAuth (client_id:client_secret)."""
    creds = f"{CLIENT_ID}:{CLIENT_SECRET}".encode("utf-8")
    return base64.b64encode(creds).decode("ascii")


# PST timezone (UTC-8)
PST = timezone(timedelta(hours=-8))


def utc_to_pst(iso_timestamp: str) -> str:
    """
    Convert an ISO 8601 UTC timestamp to PST format.
    Input: '2025-12-03T18:02:41.536Z' or '2025-12-03T18:02:41.000Z'
    Output: '2025-12-03 10:02:41 PST'
    Returns empty string if input is empty or invalid.
    """
    if not iso_timestamp:
        return ""
    try:
        # Handle multiple timestamps separated by semicolons
        if ";" in iso_timestamp:
            converted = []
            for ts in iso_timestamp.split(";"):
                converted.append(utc_to_pst(ts.strip()))
            return ";".join(converted)
        
        # Parse the ISO timestamp (handles both .000Z and Z formats)
        iso_timestamp = iso_timestamp.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso_timestamp)
        # Convert to PST
        dt_pst = dt.astimezone(PST)
        return dt_pst.strftime("%Y-%m-%d %H:%M:%S PST")
    except (ValueError, AttributeError):
        return iso_timestamp  # Return original if parsing fails


def clean_status(value: str) -> str:
    """
    Clean up status values - replace 'NotApplicable' with empty string.
    """
    if value == "NotApplicable":
        return ""
    return value


# =====================================================================
# LISTINGS DB LOOKUP
# =====================================================================

def get_matches_for_item_id(item_id: str) -> List[Dict]:
    """
    Look up matches from database.db for a given eBay item_id.
    The listing_id in the DB has format like 'v1|136771205490|0', 
    so we search for item_id in the middle.
    Returns list of match dicts sorted by: is_alternative ASC, profit DESC.
    """
    if not LISTINGS_DB.exists():
        return []
    
    conn = sqlite3.connect(LISTINGS_DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Search for listing_id containing the item_id (format: v1|ITEM_ID|0)
    # Use LIKE to match the item_id portion
    pattern = f"%|{item_id}|%"
    
    cur.execute("""
        SELECT m.id as message_id, m.listing_id, m.type, m.price, m.shipping, m.total,
               ma.title, ma.asin, ma.bsr, ma.sellable, ma.net_cost, ma.profit, 
               ma.pack_size, ma.color, ma.is_alternative, ma.lot_breakdown, ma.total_units
        FROM messages m
        JOIN matches ma ON m.id = ma.message_id
        WHERE m.listing_id LIKE ?
        ORDER BY ma.is_alternative ASC, ma.profit DESC
    """, (pattern,))
    
    rows = cur.fetchall()
    conn.close()
    
    return [dict(row) for row in rows]


def build_match_columns() -> Dict[str, str]:
    """
    Build empty match columns for orders with no matches.
    Returns dict with all match column names set to empty string.
    """
    columns = {}
    for i in range(1, MAX_MATCHES + 1):
        columns[f"match{i}_title"] = ""
        columns[f"match{i}_asin"] = ""
        columns[f"match{i}_bsr"] = ""
        columns[f"match{i}_sellable"] = ""
        columns[f"match{i}_net_cost"] = ""
        columns[f"match{i}_profit"] = ""
        columns[f"match{i}_pack_size"] = ""
        columns[f"match{i}_color"] = ""
        columns[f"match{i}_is_alternative"] = ""
        columns[f"match{i}_lot_breakdown"] = ""
        columns[f"match{i}_total_units"] = ""
    return columns


def enrich_row_with_matches(row: Dict) -> Dict:
    """
    Look up matches for the item_id in the row and add match columns.
    Returns the enriched row with match1_*, match2_*, etc. columns.
    Now includes lot_breakdown and total_units for inventory tracking.
    """
    item_id = row.get("item_id", "")
    
    # Start with empty match columns
    match_cols = build_match_columns()
    
    if item_id:
        matches = get_matches_for_item_id(item_id)
        
        # Fill in up to MAX_MATCHES
        for i, match in enumerate(matches[:MAX_MATCHES], start=1):
            match_cols[f"match{i}_title"] = match.get("title", "")
            match_cols[f"match{i}_asin"] = match.get("asin", "")
            match_cols[f"match{i}_bsr"] = match.get("bsr", "") if match.get("bsr") else ""
            match_cols[f"match{i}_sellable"] = "true" if match.get("sellable") else "false" if match.get("sellable") is not None else ""
            match_cols[f"match{i}_net_cost"] = match.get("net_cost", "") if match.get("net_cost") is not None else ""
            match_cols[f"match{i}_profit"] = match.get("profit", "") if match.get("profit") is not None else ""
            
            # Handle pack_size - may be stored as bytes (blob) or int in DB
            pack_size = match.get("pack_size")
            if pack_size is not None:
                if isinstance(pack_size, bytes):
                    # Convert blob to int (little-endian)
                    pack_size = int.from_bytes(pack_size, byteorder='little') if pack_size else ""
                match_cols[f"match{i}_pack_size"] = pack_size if pack_size else ""
            else:
                match_cols[f"match{i}_pack_size"] = ""
            
            match_cols[f"match{i}_color"] = match.get("color", "")
            match_cols[f"match{i}_is_alternative"] = "true" if match.get("is_alternative") else "false"
            
            # Lot tracking fields for inventory
            match_cols[f"match{i}_lot_breakdown"] = match.get("lot_breakdown", "") or ""
            total_units = match.get("total_units")
            if total_units is not None:
                if isinstance(total_units, bytes):
                    total_units = int.from_bytes(total_units, byteorder='little') if total_units else ""
                match_cols[f"match{i}_total_units"] = total_units if total_units else ""
            else:
                match_cols[f"match{i}_total_units"] = ""
    
    # Merge match columns into the row
    row.update(match_cols)
    return row


# =====================================================================
# OAUTH
# =====================================================================

def build_authorize_url(scopes: List[str]) -> str:
    """Build the URL you open in a browser to authorize the app."""
    scope_str = " ".join(scopes)
    query = (
        f"client_id={quote(CLIENT_ID)}"
        f"&redirect_uri={quote(RU_NAME)}"
        f"&response_type=code"
        f"&scope={quote(scope_str)}"
    )
    return f"{EBAY_AUTH_URL_BASE}?{query}"


def interactive_authorization(scopes: List[str], token_file: Path) -> Dict:
    """
    First-time auth for a specific account:
      - prints URL
      - user logs into the desired eBay account and approves
      - user pastes redirect URL
      - script exchanges code → tokens and saves them to token_file
    """
    url = build_authorize_url(scopes)

    print("\n=== eBay OAuth Authorization Required ===")
    print("1) Open this URL in your browser and sign in with the eBay account you want to link:")
    print(url)
    print("\n2) After clicking 'Agree', eBay will redirect you to a URL.")
    print("3) Copy that FULL redirect URL from your browser's address bar.")
    print("4) Paste it below and press Enter.\n")

    redirect_url = input("Paste redirect URL: ").strip()

    parsed = urlparse(redirect_url)
    qs = parse_qs(parsed.query)
    if "code" not in qs:
        raise RuntimeError("Error: 'code' parameter missing in redirect URL.")

    auth_code = qs["code"][0]
    debug("Authorization code obtained.")

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {get_basic_auth_header()}",
    }
    data = {
        "grant_type": "authorization_code",
        "code": auth_code,
        "redirect_uri": RU_NAME,
    }

    resp = requests.post(EBAY_OAUTH_TOKEN_URL, headers=headers, data=data)
    resp.raise_for_status()

    tokens = resp.json()
    save_tokens(tokens, token_file)
    return tokens


def refresh_access_token(tokens: Dict, scopes: List[str], token_file: Path) -> Tuple[str, int]:
    """Use stored refresh_token to get a new access_token for a specific account."""
    if "refresh_token" not in tokens:
        raise RuntimeError("Refresh token missing — run interactive auth first.")

    scope_str = " ".join(scopes)

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {get_basic_auth_header()}",
    }
    data = {
        "grant_type": "refresh_token",
        "refresh_token": tokens["refresh_token"],
        "scope": scope_str,
    }

    resp = requests.post(EBAY_OAUTH_TOKEN_URL, headers=headers, data=data)
    resp.raise_for_status()

    j = resp.json()
    tokens["access_token"] = j["access_token"]
    tokens["expires_in"] = j.get("expires_in", 0)
    save_tokens(tokens, token_file)

    return j["access_token"], int(j.get("expires_in", 0))


def is_interactive() -> bool:
    """Check if we're running in an interactive terminal (not Docker/headless)."""
    return sys.stdin.isatty()


def get_access_token(scopes: List[str], token_file: Path) -> Optional[str]:
    """
    Master access token getter for a specific account:
      - If no tokens on disk → interactive auth (if available) or return None
      - Else → try refresh
      - If refresh fails → interactive auth (if available) or return None
    
    Returns None if tokens are missing and we can't run interactive auth.
    """
    tokens = load_tokens(token_file)

    if not tokens:
        if is_interactive():
            debug(f"No tokens found for {token_file} — starting interactive authorization.")
            tokens = interactive_authorization(scopes, token_file)
            return tokens["access_token"]
        else:
            debug(f"No tokens found for {token_file} and running headless. Skipping this account.")
            debug(f"To fix: run 'python order_history/ebay_order_history.py' locally first to authorize.")
            return None

    try:
        debug(f"Refreshing access token for {token_file}...")
        access_token, _ = refresh_access_token(tokens, scopes, token_file)
        return access_token
    except Exception as e:
        if is_interactive():
            debug(f"Refresh failed for {token_file}: {e}. Re-authorizing.")
            tokens = interactive_authorization(scopes, token_file)
            return tokens["access_token"]
        else:
            debug(f"Refresh failed for {token_file}: {e}. Running headless, cannot re-authorize.")
            debug(f"To fix: run 'python order_history/ebay_order_history.py' locally to re-authorize.")
            return None


# =====================================================================
# TRADING API
# =====================================================================

def _text(parent: ET.Element, path: str) -> str:
    """Utility: return element text or empty string."""
    el = parent.find(path, EBAY_NAMESPACE)
    return el.text if el is not None else ""


def build_getorders_xml(days: int, page: int) -> str:
    """
    Build a GetOrdersRequest that fetches buyer orders for the last `days` days.
    Fetches ALL order statuses (not just Completed) to track cancellations and returns.
    """
    root = ET.Element("GetOrdersRequest", xmlns=EBAY_NAMESPACE["e"])

    ET.SubElement(root, "DetailLevel").text = "ReturnAll"
    ET.SubElement(root, "NumberOfDays").text = str(days)
    ET.SubElement(root, "OrderRole").text = "Buyer"
    # Don't filter by OrderStatus - fetch all to track cancellations/returns
    # OrderStatus values: Active, Cancelled, Completed, etc.

    pagination = ET.SubElement(root, "Pagination")
    ET.SubElement(pagination, "EntriesPerPage").text = "100"
    ET.SubElement(pagination, "PageNumber").text = str(page)

    ET.SubElement(root, "SortingOrder").text = "Descending"

    return ET.tostring(root, encoding="utf-8", xml_declaration=True).decode("utf-8")


def call_getorders(access_token: str, days: int) -> List[ET.Element]:
    """
    Call GetOrders repeatedly until all pages are fetched.
    Returns a list of <Order> elements.
    """
    orders: List[ET.Element] = []
    page = 1
    more = True

    while more:
        xml_body = build_getorders_xml(days, page)

        headers = {
            "X-EBAY-API-CALL-NAME": "GetOrders",
            "X-EBAY-API-COMPATIBILITY-LEVEL": "1423",
            "X-EBAY-API-SITEID": "0",
            "X-EBAY-API-APP-NAME": CLIENT_ID,
            "X-EBAY-API-DEV-NAME": DEV_ID,
            "X-EBAY-API-CERT-NAME": CERT_ID,
            "X-EBAY-API-IAF-TOKEN": access_token,
            "Content-Type": "text/xml",
        }

        resp = requests.post(EBAY_TRADING_ENDPOINT, headers=headers, data=xml_body)

        try:
            resp.raise_for_status()
        except requests.HTTPError:
            debug("=== RAW GetOrders HTTP ERROR BODY ===")
            debug(resp.text)
            raise

        root = ET.fromstring(resp.content)

        ack_el = root.find("e:Ack", EBAY_NAMESPACE)
        ack = ack_el.text if ack_el is not None else "UNKNOWN"

        if ack not in ("Success", "Warning"):
            error_nodes = root.findall("e:Errors", EBAY_NAMESPACE)
            error_msgs = []
            for err in error_nodes:
                code = _text(err, "e:ErrorCode")
                short = _text(err, "e:ShortMessage")
                long = _text(err, "e:LongMessage")
                error_msgs.append(f"[{code}] {short} - {long}")

            debug("=== RAW GetOrders ERROR RESPONSE ===")
            debug(resp.text)

            raise RuntimeError(
                f"GetOrders failed. Ack={ack}. Errors: {' | '.join(error_msgs) or 'No <Errors> nodes found'}"
            )

        order_array = root.find("e:OrderArray", EBAY_NAMESPACE)
        if order_array is not None:
            orders.extend(order_array.findall("e:Order", EBAY_NAMESPACE))

        has_more_el = root.find("e:HasMoreOrders", EBAY_NAMESPACE)
        more = has_more_el is not None and has_more_el.text == "true"
        page += 1

    return orders


# =====================================================================
# PARSING
# =====================================================================

def extract_refunds(order_el: ET.Element) -> Dict:
    """
    Extract refund information from MonetaryDetails/Refunds.
    Returns dict with refund_amount, refund_time, refund_type, refund_to.
    """
    refund_info = {
        "refund_amount": "",
        "refund_time": "",
        "refund_type": "",
        "refund_to": "",
        "refund_status": "",
    }
    
    monetary = order_el.find("e:MonetaryDetails", EBAY_NAMESPACE)
    if monetary is None:
        return refund_info
    
    refunds_container = monetary.find("e:Refunds", EBAY_NAMESPACE)
    if refunds_container is None:
        return refund_info
    
    # Collect all refunds (there may be multiple)
    refund_amounts = []
    refund_times = []
    refund_types = []
    refund_tos = []
    
    for refund in refunds_container.findall("e:Refund", EBAY_NAMESPACE):
        amt_el = refund.find("e:RefundAmount", EBAY_NAMESPACE)
        if amt_el is not None and amt_el.text:
            refund_amounts.append(amt_el.text)
        
        time_el = refund.find("e:RefundTime", EBAY_NAMESPACE)
        if time_el is not None and time_el.text:
            refund_times.append(time_el.text)
        
        type_el = refund.find("e:RefundType", EBAY_NAMESPACE)
        if type_el is not None and type_el.text:
            refund_types.append(type_el.text)
        
        to_el = refund.find("e:RefundTo", EBAY_NAMESPACE)
        if to_el is not None and to_el.text:
            refund_tos.append(to_el.text)
    
    # Join multiple values with semicolons
    refund_info["refund_amount"] = ";".join(refund_amounts) if refund_amounts else ""
    refund_info["refund_time"] = ";".join(refund_times) if refund_times else ""
    refund_info["refund_type"] = ";".join(refund_types) if refund_types else ""
    refund_info["refund_to"] = ";".join(refund_tos) if refund_tos else ""
    
    # If we have any refund data, mark as refunded
    if refund_amounts:
        refund_info["refund_status"] = "REFUNDED"
    
    return refund_info


def extract_tracking(order_el: ET.Element, txn: Optional[ET.Element]) -> Tuple[str, str]:
    """
    Collect tracking numbers + carriers from both order- and transaction-level containers.
    Returns (tracking_numbers_csv, tracking_carriers_csv).
    """
    nums: List[str] = []
    cars: List[str] = []

    # Order-level
    ship_details = order_el.find("e:ShippingDetails", EBAY_NAMESPACE)
    if ship_details is not None:
        for td in ship_details.findall("e:ShipmentTrackingDetails", EBAY_NAMESPACE):
            nums.append(_text(td, "e:ShipmentTrackingNumber"))
            cars.append(_text(td, "e:ShippingCarrierUsed"))

    # Transaction-level
    if txn is not None:
        for td in txn.findall("e:ShipmentTrackingDetails", EBAY_NAMESPACE):
            nums.append(_text(td, "e:ShipmentTrackingNumber"))
            cars.append(_text(td, "e:ShippingCarrierUsed"))

        txn_ship = txn.find("e:ShippingDetails", EBAY_NAMESPACE)
        if txn_ship is not None:
            for td in txn_ship.findall("e:ShipmentTrackingDetails", EBAY_NAMESPACE):
                nums.append(_text(td, "e:ShipmentTrackingNumber"))
                cars.append(_text(td, "e:ShippingCarrierUsed"))

    # Clean up
    nums = [n for n in nums if n]
    cars = [c for c in cars if c]

    # Deduplicate while preserving order
    nums = list(dict.fromkeys(nums))
    cars = list(dict.fromkeys(cars))

    return ",".join(nums), ",".join(cars)


def parse_order_to_rows(order_el: ET.Element) -> List[Dict]:
    """
    Turn a single <Order> element into one row PER TRANSACTION (line item).
    Now includes cancellation and return status information.
    """
    rows: List[Dict] = []

    order_id = _text(order_el, "e:OrderID")
    order_status = _text(order_el, "e:OrderStatus")

    # Cancellation info (order-level)
    cancel_status = _text(order_el, "e:CancelStatus")
    cancel_reason = _text(order_el, "e:CancelReason")

    # Extract refund info from MonetaryDetails
    refund_info = extract_refunds(order_el)

    buyer_user_id = _text(order_el, "e:BuyerUserID")
    account_label = buyer_user_id.strip() or "unknown_account"

    seller_user_id = _text(order_el, "e:SellerUserID")
    checkout_status = _text(order_el, "e:CheckoutStatus/e:Status")

    created_time = _text(order_el, "e:CreatedTime")
    paid_time = _text(order_el, "e:PaidTime")
    shipped_time = _text(order_el, "e:ShippedTime")

    amount_paid_el = order_el.find("e:AmountPaid", EBAY_NAMESPACE)
    order_currency = amount_paid_el.get("currencyID") if amount_paid_el is not None else ""
    order_amount_paid = amount_paid_el.text if amount_paid_el is not None else ""

    subtotal_el = order_el.find("e:Subtotal", EBAY_NAMESPACE)
    order_subtotal = subtotal_el.text if subtotal_el is not None else ""

    total_el = order_el.find("e:Total", EBAY_NAMESPACE)
    order_total = total_el.text if total_el is not None else ""

    # Shipping info (order-level)
    ship_sel = order_el.find("e:ShippingServiceSelected", EBAY_NAMESPACE)
    if ship_sel is not None:
        shipping_service = _text(ship_sel, "e:ShippingService")
        shipping_service_cost = _text(ship_sel, "e:ShippingServiceCost")
    else:
        shipping_service = shipping_service_cost = ""

    transactions = order_el.findall("e:TransactionArray/e:Transaction", EBAY_NAMESPACE)

    # If no transactions are present, still emit one row
    if not transactions:
        tracking_nums, tracking_cars = extract_tracking(order_el, None)
        row = {
                "account_label": account_label,
                "order_id": order_id,
                "transaction_id": "",
                "order_status": order_status,
                "checkout_status": checkout_status,
                "cancel_status": clean_status(cancel_status),
                "cancel_reason": cancel_reason,
                "return_status": "",  # Return status is at transaction level
                "inquiry_status": "",  # Inquiry status is at transaction level
                "refund_amount": refund_info["refund_amount"],
                "refund_time": utc_to_pst(refund_info["refund_time"]),
                "refund_type": refund_info["refund_type"],
                "refund_status": refund_info["refund_status"],
                "created_time": utc_to_pst(created_time),
                "paid_time": utc_to_pst(paid_time),
                "shipped_time": utc_to_pst(shipped_time),
                "actual_delivery_time": utc_to_pst(_text(order_el, "e:ShippingServiceSelected/e:ShippingPackageInfo/e:ActualDeliveryTime")),
                "order_currency": order_currency,
                "order_amount_paid": order_amount_paid,
                "order_subtotal": order_subtotal,
                "order_total": order_total,
                "buyer_user_id": buyer_user_id,
                "seller_user_id": seller_user_id,
                "item_id": "",
                "item_title": "",
                "item_sku": "",
                "quantity_purchased": "",
                "transaction_price": "",
                "transaction_currency": "",
                "shipping_service": shipping_service,
                "shipping_service_cost": shipping_service_cost,
                "tracking_numbers": tracking_nums,
                "tracking_carriers": tracking_cars,
            }
        rows.append(enrich_row_with_matches(row))
        return rows

    # Normal case: one row per transaction
    for txn in transactions:
        txn_id = _text(txn, "e:TransactionID")
        qty = _text(txn, "e:QuantityPurchased")

        # Return status is at transaction level (Status/ReturnStatus)
        return_status = _text(txn, "e:Status/e:ReturnStatus")
        # Inquiry status (for Item Not Received cases)
        inquiry_status = _text(txn, "e:Status/e:InquiryStatus")

        # Actual delivery time from ShippingServiceSelected/ShippingPackageInfo
        actual_delivery_time = _text(txn, "e:ShippingServiceSelected/e:ShippingPackageInfo/e:ActualDeliveryTime")
        # Also check order-level if not at transaction level
        if not actual_delivery_time:
            actual_delivery_time = _text(order_el, "e:ShippingServiceSelected/e:ShippingPackageInfo/e:ActualDeliveryTime")

        item = txn.find("e:Item", EBAY_NAMESPACE)
        if item is not None:
            item_id = _text(item, "e:ItemID")
            item_title = _text(item, "e:Title")
            item_sku = _text(item, "e:SKU")
        else:
            item_id = item_title = item_sku = ""

        price_el = txn.find("e:TransactionPrice", EBAY_NAMESPACE)
        txn_currency = price_el.get("currencyID") if price_el is not None else ""
        txn_price = price_el.text if price_el is not None else ""

        tracking_nums, tracking_cars = extract_tracking(order_el, txn)

        row = {
                "account_label": account_label,
                "order_id": order_id,
                "transaction_id": txn_id,
                "order_status": order_status,
                "checkout_status": checkout_status,
                "cancel_status": clean_status(cancel_status),
                "cancel_reason": cancel_reason,
                "return_status": clean_status(return_status),
                "inquiry_status": clean_status(inquiry_status),
                "refund_amount": refund_info["refund_amount"],
                "refund_time": utc_to_pst(refund_info["refund_time"]),
                "refund_type": refund_info["refund_type"],
                "refund_status": refund_info["refund_status"],
                "created_time": utc_to_pst(created_time),
                "paid_time": utc_to_pst(paid_time),
                "shipped_time": utc_to_pst(shipped_time),
                "actual_delivery_time": utc_to_pst(actual_delivery_time),
                "order_currency": order_currency,
                "order_amount_paid": order_amount_paid,
                "order_subtotal": order_subtotal,
                "order_total": order_total,
                "buyer_user_id": buyer_user_id,
                "seller_user_id": seller_user_id,
                "item_id": item_id,
                "item_title": item_title,
                "item_sku": item_sku,
                "quantity_purchased": qty,
                "transaction_price": txn_price,
                "transaction_currency": txn_currency,
                "shipping_service": shipping_service,
                "shipping_service_cost": shipping_service_cost,
                "tracking_numbers": tracking_nums,
                "tracking_carriers": tracking_cars,
            }
        rows.append(enrich_row_with_matches(row))

    return rows


# =====================================================================
# CSV WRITE
# =====================================================================

def load_existing_orders(path: Path) -> Dict[Tuple[str, str], Dict]:
    """
    Load existing orders from CSV into a dict keyed by (order_id, transaction_id).
    Returns dict mapping keys to full row dicts.
    """
    if not path.exists():
        return {}
    orders = {}
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row["order_id"], row["transaction_id"])
            orders[key] = row
    return orders


def row_needs_update(existing: Dict, new: Dict) -> bool:
    """
    Check if the new row has important status changes that warrant an update.
    Compares cancel_status, return_status, refund_status, order_status, etc.
    """
    # Fields that indicate state changes we care about
    status_fields = [
        "order_status", "checkout_status", "cancel_status", "cancel_reason",
        "return_status", "inquiry_status", "refund_amount", "refund_time", 
        "refund_type", "refund_status", "shipped_time", "paid_time",
        "actual_delivery_time", "tracking_numbers", "tracking_carriers"
    ]
    
    for field in status_fields:
        old_val = existing.get(field, "")
        new_val = new.get(field, "")
        if old_val != new_val:
            return True
    return False


def upsert_csv(path: Path, rows: List[Dict]) -> Tuple[int, int]:
    """
    Upsert rows to CSV: insert new rows AND update existing rows if status changed.
    Returns tuple of (new_count, updated_count).
    """
    if not rows:
        debug("No rows to process.")
        return 0, 0

    existing_orders = load_existing_orders(path)
    
    new_rows = []
    updated_keys = set()
    
    for row in rows:
        key = (row["order_id"], row["transaction_id"])
        if key not in existing_orders:
            new_rows.append(row)
        elif row_needs_update(existing_orders[key], row):
            # Update the existing record with new data
            existing_orders[key] = row
            updated_keys.add(key)
    
    if not new_rows and not updated_keys:
        debug("No new orders and no status changes detected.")
        return 0, 0
    
    # Determine fieldnames from new rows or existing data
    if new_rows:
        fieldnames = list(new_rows[0].keys())
    elif existing_orders:
        fieldnames = list(next(iter(existing_orders.values())).keys())
    else:
        debug("No data to write.")
        return 0, 0
    
    # Merge existing and new rows
    all_rows = list(existing_orders.values()) + new_rows
    
    # Write everything back to CSV (overwrite)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(all_rows)
    
    return len(new_rows), len(updated_keys)


# Keep old function for backward compatibility
def append_csv(path: Path, rows: List[Dict]) -> int:
    """
    DEPRECATED: Use upsert_csv instead.
    Append rows to CSV, skipping duplicates based on (order_id, transaction_id).
    Returns count of new rows written.
    """
    new_count, _ = upsert_csv(path, rows)
    return new_count


def write_csv(path: Path, rows: List[Dict]) -> None:
    """Write all rows to CSV (overwrites existing file). DEPRECATED - use append_csv."""
    if not rows:
        debug("No rows to write.")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


# =====================================================================
# MAIN
# =====================================================================

def run_order_history() -> int:
    """
    Fetch order history for all accounts and save to both CSV and database.
    Returns the number of rows changed (new + updated).
    Raises RuntimeError if env vars are missing.
    """
    if not CLIENT_ID or not CLIENT_SECRET or not RU_NAME:
        raise RuntimeError("Missing env vars. Fill out .env with CLIENT_ID, CLIENT_SECRET, RU_NAME.")

    # Ensure database tables exist
    init_db()

    scopes = [
        "https://api.ebay.com/oauth/api_scope",
        "https://api.ebay.com/oauth/api_scope/sell.account",
    ]

    all_rows: List[Dict] = []
    accounts_processed = 0

    for account in ACCOUNTS:
        account_id = account["id"]
        token_file = account["token_file"]

        debug(f"\n=== Processing account id: {account_id} ({token_file}) ===")
        access_token = get_access_token(scopes, token_file)
        
        if access_token is None:
            debug(f"Skipping account '{account_id}' — no valid tokens available.")
            continue

        debug(f"Calling GetOrders() for account id '{account_id}'...")
        orders_xml = call_getorders(access_token, GETORDERS_DAYS)
        debug(f"Fetched {len(orders_xml)} orders for account id '{account_id}'.")

        for order_el in orders_xml:
            all_rows.extend(parse_order_to_rows(order_el))
        
        accounts_processed += 1

    if accounts_processed == 0:
        debug("No accounts were processed. Make sure token files exist.")
        return 0

    # Write to CSV
    csv_new, csv_updated = upsert_csv(OUTPUT_CSV, all_rows)
    debug(f"CSV: {csv_new} new rows, {csv_updated} updated rows → {OUTPUT_CSV.resolve()}")

    # Write to database
    db_new, db_updated = upsert_order_history(all_rows)
    debug(f"Database: {db_new} new rows, {db_updated} updated rows → {LISTINGS_DB}")

    # Populate purchased_units table for inventory analytics
    purchased_units = []
    for row in all_rows:
        units = expand_order_to_purchased_units(row)
        purchased_units.extend(units)
    
    if purchased_units:
        pu_inserted, pu_skipped = insert_purchased_units_batch(purchased_units)
        debug(f"Purchased units: {pu_inserted} inserted, {pu_skipped} skipped (already exist)")

    total_changes = csv_new + csv_updated
    return total_changes


# =====================================================================
# DAILY STATS REPORT
# =====================================================================

# Account label mapping for display names (eBay username -> display name)
ACCOUNT_DISPLAY_NAMES = {
    "buyinko_11": "Lee's Account",
    "surplusink": "Ira's Account",
}

# Report schedule configuration: (hour, start_hour) 
# e.g., 9am report covers midnight (0) to 9am
REPORT_SCHEDULE = [
    (9, 0),    # 9 AM report: 12:00 AM - 9:00 AM
    (12, 9),   # 12 PM report: 9:00 AM - 12:00 PM  
    (17, 12),  # 5 PM report: 12:00 PM - 5:00 PM
]


def send_daily_stats_report(date_str: str = None) -> None:
    """
    DEPRECATED: Use send_stats_report() instead.
    Send daily order stats to Telegram for a given date.
    """
    # Default to today in PST
    if date_str is None:
        now_pst = datetime.now(PST)
        date_str = now_pst.strftime("%Y-%m-%d")
    
    # Format date for display
    try:
        display_date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%b %d, %Y")
    except ValueError:
        display_date = date_str
    
    # Get stats from database
    stats = get_daily_order_stats(date_str)
    
    # Build message
    lines = [f"Daily Order Report - {display_date}", ""]
    
    total_orders = 0
    total_spent = 0.0
    
    # Process each account (use eBay usernames as stored in DB)
    for account_label in ["buyinko_11", "surplusink"]:
        display_name = ACCOUNT_DISPLAY_NAMES.get(account_label, account_label)
        
        # Find stats for this account
        account_stats = next((s for s in stats if s["account_label"] == account_label), None)
        
        if account_stats:
            order_count = account_stats["order_count"]
            spent = account_stats["total_spent"]
        else:
            order_count = 0
            spent = 0.0
        
        total_orders += order_count
        total_spent += spent
        
        lines.append(f"{display_name}")
        lines.append(f"  Orders: {order_count}")
        lines.append(f"  Spent: ${spent:.2f}")
        lines.append("")
    
    lines.append(f"Total: {total_orders} orders, ${total_spent:.2f}")
    
    message = "\n".join(lines)
    
    # Send to Telegram
    debug(f"Sending daily stats report for {date_str}")
    send_telegram_message(message)
    debug("Daily stats report sent.")


def send_stats_report(report_hour: int, start_hour: int) -> None:
    """
    Send order stats to Telegram for a specific time range.
    
    9 AM / 12 PM: Compact summary with order count, spend, matched vs unmatched items.
    5 PM: Full day summary (12 AM - 5 PM) with item-level purchase list and ASINs.
    
    Args:
        report_hour: The hour when this report is sent (9, 12, or 17)
        start_hour: The starting hour of the time range (0, 9, or 12)
    """
    now_pst = datetime.now(PST)
    date_str = now_pst.strftime("%Y-%m-%d")
    
    is_end_of_day = (report_hour == 17)
    
    # Build time range strings
    # For 5 PM report, cover the full day (12 AM - 5 PM)
    effective_start = 0 if is_end_of_day else start_hour
    start_time = f"{date_str} {effective_start:02d}:00:00"
    end_time = f"{date_str} {report_hour:02d}:00:00"
    
    # Format times for display
    start_display = datetime.strptime(f"{effective_start:02d}:00", "%H:%M").strftime("%I:%M %p").lstrip("0")
    end_display = datetime.strptime(f"{report_hour:02d}:00", "%H:%M").strftime("%I:%M %p").lstrip("0")
    if effective_start == 0:
        start_display = "12:00 AM"
    time_range_display = f"{start_display} - {end_display}"
    
    # Get stats from database
    stats = get_order_stats_for_time_range(start_time, end_time)
    match_counts = get_order_item_match_counts(start_time, end_time)
    items = get_order_items_for_time_range(start_time, end_time) if is_end_of_day else []
    
    # Build header
    header = "📊 Daily Summary" if is_end_of_day else "📊 Order Stats"
    lines = [f"{header} ({time_range_display})", ""]
    
    total_orders = 0
    total_spent = 0.0
    
    for account_label in ["buyinko_11", "surplusink"]:
        display_name = ACCOUNT_DISPLAY_NAMES.get(account_label, account_label)
        
        # Order count and spend
        account_stats = next((s for s in stats if s["account_label"] == account_label), None)
        order_count = account_stats["order_count"] if account_stats else 0
        spent = account_stats["total_spent"] if account_stats else 0.0
        
        total_orders += order_count
        total_spent += spent
        
        lines.append(f"{display_name}")
        lines.append(f"  Orders: {order_count} | Spent: ${spent:.2f}")
        
        # Match counts (skip if no orders)
        if order_count > 0:
            acct_matches = next((m for m in match_counts if m["account_label"] == account_label), None)
            total_items = acct_matches["total_items"] if acct_matches else 0
            matched_items = acct_matches["matched_items"] if acct_matches else 0
            lines.append(f"  Matched: {matched_items}/{total_items} items")
        
        lines.append("")
    
    # Total line
    lines.append(f"Total: {total_orders} orders, ${total_spent:.2f}")
    
    # For 5 PM report, append item-level detail
    if is_end_of_day and total_orders > 0:
        lines.append("")
        lines.append("━━━━━━━━━━━━━━━━")
        lines.append("📦 Purchases Today")
        
        for account_label in ["buyinko_11", "surplusink"]:
            acct_items = [i for i in items if i["account_label"] == account_label]
            if not acct_items:
                continue
            
            display_name = ACCOUNT_DISPLAY_NAMES.get(account_label, account_label)
            lines.append("")
            lines.append(f"━━ {display_name} ━━")
            lines.append("")
            
            for item in acct_items:
                title = item["item_title"] or "Unknown Item"
                # Escape HTML special chars in title since we use HTML parse mode
                title = title.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                price = item["transaction_price"] or "0.00"
                qty = item["quantity_purchased"] or "1"
                
                qty_suffix = f"{qty}x | " if str(qty) != "1" else ""
                
                lines.append(title)
                lines.append(f"{qty_suffix}${price}")
                
                # Collect all distinct ASINs from match1-4
                asins = []
                for i in range(1, 5):
                    asin = item.get(f"match{i}_asin")
                    if asin and asin.strip() and asin not in asins:
                        asins.append(asin)
                
                if asins:
                    asin_links = [f'<a href="https://www.amazon.com/dp/{a}">{a}</a>' for a in asins]
                    lines.append(", ".join(asin_links))
                else:
                    lines.append("No match found")
                
                lines.append("")
    
    # Split into multiple messages if needed to avoid Telegram truncation
    # Build the summary section (always send first)
    # Then build the purchases section separately if it exists
    summary_end_idx = None
    for idx, line in enumerate(lines):
        if line == "📦 Purchases Today":
            summary_end_idx = idx - 2  # Before the separator
            break
    
    if summary_end_idx is not None and is_end_of_day:
        # Send summary as first message
        summary_message = "\n".join(lines[:summary_end_idx + 1])
        debug(f"Sending stats summary for {time_range_display}")
        send_telegram_message(summary_message)
        
        # Send purchases as separate message(s) per account
        for account_label in ["buyinko_11", "surplusink"]:
            acct_items = [i for i in items if i["account_label"] == account_label]
            if not acct_items:
                continue
            
            display_name = ACCOUNT_DISPLAY_NAMES.get(account_label, account_label)
            acct_lines = [f"📦 {display_name} Purchases", ""]
            
            for item in acct_items:
                item_title = item["item_title"] or "Unknown Item"
                item_title = item_title.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                price = item["transaction_price"] or "0.00"
                qty = item["quantity_purchased"] or "1"
                
                qty_suffix = f"{qty}x | " if str(qty) != "1" else ""
                
                acct_lines.append(item_title)
                acct_lines.append(f"{qty_suffix}${price}")
                
                asins = []
                for i in range(1, 5):
                    asin = item.get(f"match{i}_asin")
                    if asin and asin.strip() and asin not in asins:
                        asins.append(asin)
                
                if asins:
                    asin_links = [f'<a href="https://www.amazon.com/dp/{a}">{a}</a>' for a in asins]
                    acct_lines.append(", ".join(asin_links))
                else:
                    acct_lines.append("No match found")
                
                acct_lines.append("")
            
            acct_message = "\n".join(acct_lines)
            send_telegram_message(acct_message)
    else:
        message = "\n".join(lines)
        debug(f"Sending stats report for {time_range_display}")
        send_telegram_message(message)
    debug("Stats report sent.")


def main() -> None:
    """Standalone entry point for manual runs."""
    try:
        change_count = run_order_history()
        print(f"\nProcessed {change_count} order rows (new + updated) → CSV: {OUTPUT_CSV.resolve()}, DB: {LISTINGS_DB}")
    except RuntimeError as e:
        print(f"ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
