"""Lexmark engine for eBay listing monitoring.

This engine:
- Loads Lexmark product data from SQL database
- Fetches eBay listings for Lexmark toner
- Matches listings to reference data via part number extraction
- Sends profitable matches to Telegram

Key differences from Xerox/Canon engines:
- Matching is based on Part Number (e.g., "500G", "501H", "601H")
- Simpler data model with flat rows
"""

from __future__ import annotations

import argparse
import base64
import os
import re
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv
from urllib.parse import quote
from pytz import timezone, utc

# Ensure parent directory is in path for db imports
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from db.exclusions_db import (
    list_sellers as db_list_sellers,
    list_lexmark_keywords as db_list_lexmark_keywords,
)
from db.listings_db import (
    add_seen_id,
    gc_old_ids,
    init_db,
    insert_match,
    insert_message,
    is_id_seen,
)
from db.products_db import get_lexmark_products, get_overhead_pct, calculate_effective_net, get_target_profit

load_dotenv()

# ─── Configuration ─────────────────────────────────────────────────────────────
ZIP_CODE = os.getenv("ZIP_CODE", "93012")
BASE_URL = "https://api.ebay.com/buy/browse/v1"
MARKETPLACE_ID = "EBAY_US"
CTX = quote(f"country=US,zip={ZIP_CODE}", safe="")
IDENTITY_URL = "https://api.ebay.com/identity/v1/oauth2/token"

EBAY_APP_ID = os.getenv("EBAY_APP_ID")
EBAY_CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_MAX_RETRIES = int(os.getenv("TELEGRAM_MAX_RETRIES", "4"))
TELEGRAM_SEND_MIN_INTERVAL_SEC = float(os.getenv("TELEGRAM_SEND_MIN_INTERVAL_SEC", "4"))
TELEGRAM_API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}" if TELEGRAM_BOT_TOKEN else ""
LEXMARK_MIN_NET_COST = float(os.getenv("LEXMARK_MIN_NET_COST", "50"))

LOCAL_TZ = timezone(os.getenv("LOCAL_TIMEZONE", "America/Los_Angeles"))
SEPARATOR = "-" * 10

# Lexmark part numbers are typically alphanumeric like "500G", "501H", "601H", "C540H1KG"
# They can also include longer codes like "50F1H00" or "52D0Z00"
# Pattern: 2-4 digits + 1-2 letters OR longer alphanumeric codes
PART_NUMBER_REGEX = re.compile(
    r"\b("
    r"[0-9]{2,4}[A-Z]{1,2}[0-9]{0,2}[A-Z]{0,2}"  # e.g., 500G, 501H, C540H1KG
    r"|"
    r"[A-Z][0-9]{3}[A-Z][0-9][A-Z]{2}"  # e.g., C540H1KG format
    r"|"
    r"[0-9]{2}[A-Z][0-9]{1,4}[A-Z]{0,2}[0-9]{0,2}"  # e.g., 50F1H00, 52D0Z00
    r")\b",
    re.IGNORECASE
)



def _log(message: str) -> None:
    """Log a message with engine prefix."""
    print(f"LOG - Lexmark.py - {message}")


def get_excluded_sellers() -> List[str]:
    """Get list of excluded sellers (lowercase)."""
    return [s.lower() for s in db_list_sellers()]


def get_excluded_keywords() -> List[str]:
    """Get list of excluded keywords for Lexmark (lowercase)."""
    return [k.lower() for k in db_list_lexmark_keywords()]


def _strip_currency(val: str) -> Optional[float]:
    """Strip currency symbols and convert to float."""
    try:
        clean = val.replace("$", "").replace(",", "").strip()
        if not clean:
            return None
        return float(clean)
    except Exception:
        return None


def _normalize_net_cost(val: Any) -> Optional[float]:
    """Normalize net cost value to float."""
    if isinstance(val, str):
        return _strip_currency(val)
    if isinstance(val, (int, float)):
        try:
            return float(val)
        except (TypeError, ValueError):
            return None
    return None


def _parse_bsr(val: str) -> Optional[int]:
    """Parse BSR value from string."""
    try:
        clean = str(val or "").replace(",", "").strip()
        if not clean:
            return None
        return int(float(clean))
    except Exception:
        return None


def _clean_field(val: Any) -> str:
    """Clean a field value to string."""
    return str(val or "").strip()


def _extract_color_from_variant(variant_label: str) -> str:
    """
    Extract color from a variant label like "Cyan Extra High Yield" or "Black Standard RP".
    
    Returns the color (Cyan, Magenta, Yellow, Black) or the full label if no color found.
    """
    label_lower = variant_label.lower()
    if "cyan" in label_lower:
        return "Cyan"
    if "magenta" in label_lower:
        return "Magenta"
    if "yellow" in label_lower:
        return "Yellow"
    if "black" in label_lower:
        return "Black"
    # Return the original for non-toner items (Fuser, Imaging Kit, etc.)
    return variant_label.split()[0] if variant_label.split() else ""


def _extract_capacity_from_variant(variant_label: str) -> str:
    """
    Extract capacity from a variant label.
    
    Returns: Standard, High, Extra High, Ultra High, or empty string.
    """
    label_lower = variant_label.lower()
    if "ultra high" in label_lower:
        return "Ultra High"
    if "extra high" in label_lower:
        return "Extra High"
    if "high" in label_lower:
        return "High"
    if "standard" in label_lower:
        return "Standard"
    return ""


def _extract_pack_size(variant_label: str) -> int:
    """
    Extract pack size from variant label like "2 Pack Black" or "2 Black Standard RP".
    Returns 1 if no pack size found.
    """
    match = re.match(r'^(\d+)\s*(?:pack|pk)?\s+', variant_label, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return 1


def build_part_number_index(df: pd.DataFrame) -> Dict[str, List[dict]]:
    """
    Build an index mapping normalized part numbers to their reference rows.
    
    This allows O(1) lookup when matching eBay listing titles against reference data.
    Multiple rows can have the same part number (e.g., multi-packs vs singles).
    """
    pn_map: Dict[str, List[dict]] = {}
    for row in df.to_dict(orient="records"):
        pn = row.get("part_number_lower") or ""
        if not pn:
            continue
        pn_map.setdefault(pn, []).append(row)
    return pn_map


def _normalize_part_number(value: Optional[str]) -> str:
    """Normalize a part number for matching."""
    return (value or "").strip().lower()


def extract_part_number_candidates(title: Optional[str]) -> List[str]:
    """
    Extract potential Lexmark part numbers from an eBay listing title.
    
    Lexmark part numbers have several formats:
    - Short: 500G, 501H, 601H, 520Z
    - Medium: C540H1KG, X463H11G
    - Long: 50F1H00, 52D0Z00, 50F000G
    
    Returns a list of normalized (lowercase) candidates.
    """
    if not title:
        return []
    
    candidates = []
    
    # First try the regex pattern
    for match in PART_NUMBER_REGEX.findall(title.upper()):
        normalized = _normalize_part_number(match)
        if normalized and normalized not in candidates:
            candidates.append(normalized)
    
    # Also try to find patterns in parentheses (common format: "Lexmark 500G (50F000G)")
    paren_matches = re.findall(r'\(([^)]+)\)', title)
    for paren_content in paren_matches:
        # Check if it looks like a part number
        paren_clean = paren_content.strip()
        if re.match(r'^[0-9A-Za-z]{4,10}$', paren_clean):
            normalized = _normalize_part_number(paren_clean)
            if normalized and normalized not in candidates:
                candidates.append(normalized)
    
    # Extract any alphanumeric tokens that might be part numbers
    # Pattern: starts with digit(s) followed by letter(s), or vice versa
    tokens = re.findall(r'\b([0-9]+[A-Za-z]+[0-9A-Za-z]*|[A-Za-z]+[0-9]+[0-9A-Za-z]*)\b', title)
    for token in tokens:
        # Filter out common false positives
        if len(token) < 3 or len(token) > 12:
            continue
        if token.upper() in ('OEM', 'NEW', 'BOX', 'USA', 'LOT', 'SET', 'PACK', '2PK', '3PK', '4PK'):
            continue
        normalized = _normalize_part_number(token)
        if normalized and normalized not in candidates:
            candidates.append(normalized)
    
    return candidates


def resolve_listing_variants(
    listing: Dict[str, Any], pn_map: Dict[str, List[dict]]
) -> List[Dict[str, Any]]:
    """
    Find all matching part numbers and their reference data for a listing.
    
    Returns a list of matches, each containing:
    - part_number: The matched part number
    - variants: List of reference rows that match
    """
    matches: List[Dict[str, Any]] = []
    title = listing.get("title") or ""
    if not title:
        return matches
    
    seen_tokens: set[str] = set()
    for token in extract_part_number_candidates(title):
        if token in seen_tokens:
            continue
        seen_tokens.add(token)
        variants = pn_map.get(token)
        if variants:
            matches.append({"part_number": token, "variants": variants})
    
    return matches


# ─── eBay API Integration ──────────────────────────────────────────────────────

def search_lexmark_listings(token: str, limit: int = 200) -> List[dict]:
    """
    Fetch Lexmark toner listings from eBay, filtering out excluded sellers/keywords.
    """
    url = (
        f"{BASE_URL}/item_summary/search"
        f"?q=lexmark+toner&category_ids=16204"
        f"&filter=conditionIds:1000,deliveryCountry:US,itemLocationCountry:US"
        f"&aspect_filter=categoryId:16204,Brand:{{Lexmark}}"
        f"&sort=newlyListed&limit={limit}&fieldgroups=FULL"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-ENDUSERCTX": f"contextualLocation={CTX}",
        "X-EBAY-C-MARKETPLACE-ID": MARKETPLACE_ID,
        "Content-Type": "application/json",
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()

    items = resp.json().get("itemSummaries", [])
    excluded_sellers = get_excluded_sellers()
    excluded_keywords = get_excluded_keywords()
    
    filtered = [
        it
        for it in items
        if it.get("seller", {}).get("username", "").lower() not in excluded_sellers
        and not any(kw in it.get("title", "").lower() for kw in excluded_keywords)
    ]
    
    _log(f"Fetched {len(items)} listings, {len(filtered)} after exclusion filters")
    return filtered


def obtain_lexmark_token() -> str:
    """Obtain an eBay OAuth token for API access."""
    if not EBAY_APP_ID or not EBAY_CLIENT_SECRET:
        raise RuntimeError("EBAY_APP_ID and EBAY_CLIENT_SECRET must be set")
    basic = base64.b64encode(f"{EBAY_APP_ID}:{EBAY_CLIENT_SECRET}".encode()).decode()
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {basic}",
    }
    data = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope",
    }
    resp = requests.post(IDENTITY_URL, headers=headers, data=data)
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError("Failed to obtain eBay OAuth token for Lexmark")
    return token


def fetch_lexmark_details(
    item_id: str,
    token: str,
    *,
    quantity_for_shipping_estimate: int = 1,
) -> Dict[str, Any]:
    """Retrieve quantity, shipping, imagery, and description for a listing."""
    endpoint = (
        f"{BASE_URL}/item/{item_id}?quantity_for_shipping_estimate="
        f"{quantity_for_shipping_estimate}"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-ENDUSERCTX": f"contextualLocation={CTX}",
        "X-EBAY-C-MARKETPLACE-ID": MARKETPLACE_ID,
        "Content-Type": "application/json",
    }
    resp = requests.get(endpoint, headers=headers)
    resp.raise_for_status()
    payload = resp.json()

    quantity_label: str | int = "Unavailable"
    for availability in payload.get("estimatedAvailabilities", []):
        if "estimatedAvailableQuantity" in availability:
            quantity_label = availability["estimatedAvailableQuantity"]
            break
        if availability.get("availabilityThresholdType") == "MORE_THAN":
            threshold = availability.get("availabilityThreshold", 0)
            quantity_label = f"More than {threshold}"
            break

    shipping_value: Optional[float] = None
    shipping_label = "N/A"
    ship_opts = payload.get("shippingOptions") or []
    if ship_opts:
        cost = ship_opts[0].get("shippingCost") or {}
        try:
            shipping_value = float(cost.get("value"))
            shipping_label = f"{shipping_value:.2f}"
        except (TypeError, ValueError):
            shipping_value = None

    image_urls: List[str] = []
    primary = payload.get("image", {}).get("imageUrl")
    if primary:
        image_urls.append(primary)
    for img in payload.get("additionalImages", []):
        url = img.get("imageUrl")
        if url:
            image_urls.append(url)
    image_urls = image_urls[:5]

    description_source = None
    description_text = ""
    if payload.get("description"):
        description_text = payload["description"]
        description_source = "description"
    elif payload.get("shortDescription"):
        description_text = payload["shortDescription"]
        description_source = "shortDescription"

    return {
        "quantity": quantity_label,
        "shipping_value": shipping_value,
        "shipping_label": shipping_label,
        "images": image_urls,
        "description": description_text,
        "description_source": description_source,
    }


# ─── Message Formatting ────────────────────────────────────────────────────────

def _format_listing_timestamp(raw_ts: Optional[str]) -> str:
    """Format an eBay timestamp to local time."""
    if not raw_ts:
        return "Unknown"
    patterns = ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"]
    dt_obj = None
    for fmt in patterns:
        try:
            dt_obj = datetime.strptime(raw_ts, fmt)
            break
        except ValueError:
            continue
    if dt_obj is None:
        return raw_ts
    aware = utc.localize(dt_obj)
    return aware.astimezone(LOCAL_TZ).strftime("%b %d %Y, %I:%M %p %Z")


def _format_currency(value: Optional[float]) -> str:
    """Format a value as currency."""
    if value is None:
        return "N/A"
    return f"${value:.2f}"


def _sale_type(listing: Dict[str, Any]) -> str:
    """Determine the sale type from listing buying options."""
    buying_opts = listing.get("buyingOptions", [])
    if "FIXED_PRICE" in buying_opts:
        label = "Fixed Price"
        if "BEST_OFFER" in buying_opts:
            label += " Or Best Offer"
        return label
    if any(opt in buying_opts for opt in ("AUCTION", "BID")):
        return "Auction"
    return ", ".join(buying_opts) or "Unknown"


def _bsr_marker(value: Optional[int]) -> Tuple[str, str]:
    """Return BSR indicator emoji and display value."""
    if value is None:
        return ("", "N/A")
    display = f"{value:,}"
    if value < 150_000:
        return ("🟢", display)
    if value < 250_000:
        return ("🟡", display)
    if value < 500_000:
        return ("🟠", display)
    return ("🔴", display)


def _profit_marker(profit: Optional[float], sellable: bool) -> str:
    """Return profit indicator emoji."""
    if profit is None or not sellable:
        return ""
    target_profit = get_target_profit()
    return "💰" if profit >= target_profit else ""


def _format_asin_line(asin: Optional[str]) -> str:
    """Format ASIN as clickable Amazon link."""
    if not asin:
        return "ASIN: N/A"
    return f'ASIN: <a href="https://amazon.com/d/{asin}">{asin}</a>'


def _format_variant_title(part_number: Optional[str], variant_label: Optional[str], pack_size: int = 1) -> str:
    """Format a variant title for display using part number and variant label."""
    pn = (part_number or "").strip().upper()
    vl = (variant_label or "").strip()
    pack_suffix = f" ({pack_size}-Pack)" if pack_size > 1 else ""
    # If we have a variant label from the sheet, use part number + variant label
    if vl:
        return f"{pn} - {vl}{pack_suffix}"[:80]  # Truncate long titles
    # Otherwise just use part number
    return f"Lexmark {pn}{pack_suffix}".strip()


def _primary_variant_net_cost(variant_matches: List[Dict[str, Any]]) -> Optional[float]:
    """Get the net cost from the first matching variant."""
    for match in variant_matches:
        for variant in match.get("variants", []):
            net_val = _normalize_net_cost(variant.get("net_cost"))
            if net_val is not None:
                return net_val
    return None


def _extract_lot_quantity(title: str) -> int:
    """
    Extract lot/multi-quantity from listing title.

    Detects patterns like:
    - "Lot of 3", "Lot of 4"
    - "3 Boxes", "2 Pack"
    - "x3", "x4"
    - "(3)" at start
    - Number words: "TWO", "THREE", etc.

    Returns the detected quantity (1 if none detected).
    """
    if not title:
        return 1

    title_upper = title.upper().strip()

    NUMBER_WORDS = {
        "TWO": 2, "THREE": 3, "FOUR": 4, "FIVE": 5,
        "SIX": 6, "SEVEN": 7, "EIGHT": 8, "NINE": 9, "TEN": 10,
    }

    # Named pack patterns: "Double Pack", "Triple Pack", "Dual Pack"
    NAMED_PACKS = {
        "DOUBLE": 2, "DUAL": 2, "TWIN": 2,
        "TRIPLE": 3, "QUAD": 4, "QUADRUPLE": 4,
    }
    for word, val in NAMED_PACKS.items():
        if re.search(rf'\b{word}\s+PACK\b', title_upper):
            return val

    # "Lot of X"
    m = re.search(r'\bLOT\s+OF\s+(\d+)\b', title_upper)
    if m:
        return int(m.group(1))

    # "Set of X" or "Set X" (e.g., "GENUINE SET 3 LEXMARK...")
    m = re.search(r'\bSET\s+(?:OF\s+)?(\d+)\b', title_upper)
    if m:
        return int(m.group(1))

    # "(X)" at start of title
    m = re.match(r'^\((\d+)\)', title_upper)
    if m:
        return int(m.group(1))

    # "X Boxes", "X Pack", "X Packs", "X-Pack"
    m = re.search(r'\b(\d+)\s*[-]?\s*(?:BOXES|BOX|PACKS?)\b', title_upper)
    if m:
        return int(m.group(1))

    # "xN" pattern (e.g., "x3", "x4") — only match small numbers to avoid model numbers like X725
    m = re.search(r'(?<![A-Z/])X(\d{1,2})\b', title_upper)
    if m:
        val = int(m.group(1))
        if 2 <= val <= 20:
            return val

    # Number words at start: "TWO Lexmark...", "THREE Lexmark..."
    for word, val in NUMBER_WORDS.items():
        if re.match(rf'^{word}\b', title_upper):
            return val

    return 1


def _detect_set_listing(title: str, variant_entries: List[Dict]) -> Tuple[bool, int]:
    """
    Detect if a listing is a set/full set of toner cartridges.

    Args:
        title: eBay listing title
        variant_entries: List of matched variant entry dicts

    Returns:
        Tuple of (is_set, num_matched_products)
    """
    if not title or not variant_entries:
        return (False, 0)

    title_upper = title.upper()
    num_matched = len(variant_entries)

    SET_KEYWORDS = {"SET", "FULL SET", "COMPLETE SET", "CMYK", "CMY"}
    has_set_keyword = any(kw in title_upper for kw in SET_KEYWORDS)

    # A set requires at least 2 matched products AND a keyword hint
    if num_matched >= 2 and has_set_keyword:
        return (True, num_matched)

    # If 3+ matches, likely a set even without keyword
    if num_matched >= 3:
        return (True, num_matched)

    return (False, num_matched)


# ─── Telegram Integration ──────────────────────────────────────────────────────

_last_telegram_post_ts = 0.0


def _ensure_telegram_ready(require_chat: bool = True) -> None:
    """Validate Telegram configuration."""
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
    if require_chat and not TELEGRAM_CHAT_ID:
        raise RuntimeError("TELEGRAM_CHAT_ID is not set")


def _pace_telegram_post() -> None:
    """Rate limit Telegram posts."""
    global _last_telegram_post_ts
    now = time.monotonic()
    elapsed = now - _last_telegram_post_ts
    wait = TELEGRAM_SEND_MIN_INTERVAL_SEC - elapsed
    if wait > 0:
        time.sleep(wait)
    _last_telegram_post_ts = time.monotonic()


def _telegram_request(method: str, payload: Dict[str, Any]) -> None:
    """Make a Telegram API request with retry logic."""
    _pace_telegram_post()
    url = f"{TELEGRAM_API_BASE}/{method}"
    last_exc: Optional[Exception] = None
    for attempt in range(1, TELEGRAM_MAX_RETRIES + 1):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            data = resp.json() if resp.headers.get("Content-Type", "").startswith("application/json") else {}
            if resp.status_code == 429:
                retry_after = data.get("parameters", {}).get("retry_after", 1)
                time.sleep(max(1.0, float(retry_after)))
                continue
            resp.raise_for_status()
            if not data.get("ok", True):
                raise RuntimeError(f"Telegram API error: {data}")
            return
        except Exception as exc:
            last_exc = exc
            time.sleep(min(10, 2 ** attempt))
    raise RuntimeError(f"Failed to call Telegram API: {last_exc}")


def send_lexmark_telegram_message(message: str, images: List[str]) -> None:
    """Send a Lexmark listing notification to Telegram."""
    _ensure_telegram_ready(require_chat=True)
    images = images[:20]  # Hard cap

    if not images:
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
        }
        _telegram_request("sendMessage", payload)
        return

    first_batch = True
    batch: List[str] = []
    for url in images:
        batch.append(url)
        if len(batch) == 10:
            _send_media_batch(batch, message if first_batch else None)
            first_batch = False
            batch = []
    if batch:
        _send_media_batch(batch, message if first_batch else None)


def _send_media_batch(urls: List[str], caption: Optional[str]) -> None:
    """Send a batch of media to Telegram."""
    # Validate and filter image URLs
    valid_urls = []
    for url in urls:
        if url and isinstance(url, str) and url.startswith(('http://', 'https://')):
            valid_urls.append(url)
        else:
            _log(f"Skipping invalid image URL: {url}")
    
    if not valid_urls:
        _log("No valid image URLs to send")
        return
    
    media = []
    for idx, url in enumerate(valid_urls):
        item = {"type": "photo", "media": url}
        if caption and idx == 0:
            # Telegram caption limit is 1024 characters
            truncated_caption = caption[:1024] if len(caption) > 1024 else caption
            item["caption"] = truncated_caption
            item["parse_mode"] = "HTML"
        media.append(item)
    payload = {"chat_id": TELEGRAM_CHAT_ID, "media": media}
    _telegram_request("sendMediaGroup", payload)


# ─── Message Building ──────────────────────────────────────────────────────────

def build_listing_message(record: Dict[str, Any]) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Build a formatted Telegram message for a Lexmark listing match.
    
    Returns:
        Tuple of (message_text, variant_summaries)
    """
    listing = record["listing"]
    details = record["details"]
    matches = record["matches"]

    # Price calculations
    price_obj = listing.get("price", {}) or {}
    try:
        sale_price = float(price_obj.get("value"))
    except (TypeError, ValueError):
        sale_price = 0.0
    shipping_cost = details.get("shipping_value") or 0.0
    total_sale = (sale_price or 0.0) + (shipping_cost or 0.0)
    price_str = f"{sale_price:.2f}" if sale_price else "N/A"
    ship_str = details.get("shipping_label") or "N/A"
    total_str = f"{total_sale:.2f}" if sale_price else "N/A"

    # Seller info
    seller = listing.get("seller", {})
    username = seller.get("username", "N/A")
    fb_score = seller.get("feedbackScore", "N/A")
    fb_pct = seller.get("feedbackPercentage", "N/A")

    # Listing metadata
    item_id = record.get("item_id") or listing.get("itemId") or "N/A"
    listed_time = _format_listing_timestamp(listing.get("itemCreationDate"))
    url = listing.get("itemWebUrl") or "n/a"
    sale_type = _sale_type(listing)
    quantity = details.get("quantity") or "N/A"

    # Lot quantity detection
    title = listing.get("title", "<missing title>")
    lot_qty = _extract_lot_quantity(title)

    # Detect if this is a set listing (multiple different part numbers)
    is_set, num_matched = _detect_set_listing(title, matches)
    
    # For set listings, the "pack" count refers to the number of different items,
    # not multiples of the same item. Always reset lot_qty when a set is detected —
    # we never know the per-part distribution in a mixed bundle.
    if is_set:
        lot_qty = 1
    
    per_unit_price = total_sale / lot_qty if lot_qty > 1 else total_sale

    # Build message header
    msg = f'<a href="{url}">{title}</a>\n\n'
    msg += f"Seller: {username} ({fb_score}, {fb_pct}%)\n"
    msg += f"{listed_time}\n"
    msg += f"{sale_type}\n\n"

    msg += (
        f"Qty: {quantity}\n"
        f"Price: ${price_str}\n"
        f"Shipping: ${ship_str}\n"
        f"Total: ${total_str}\n\n"
    )

    # Process variant matches
    variant_entries: List[Dict[str, Any]] = []
    overhead_pct = get_overhead_pct()  # Get current overhead setting
    for match in matches:
        part_number = (match.get("part_number") or "").upper()
        for variant in match.get("variants", []):
            net_cost = _normalize_net_cost(variant.get("net_cost"))
            amazon_price = variant.get("amazon_price")
            variant_entries.append({
                "part_number": part_number,
                "asin": variant.get("asin"),
                "amazon_sku": variant.get("amazon_sku"),
                "sellable": bool(variant.get("sellable")),
                "net_cost": net_cost,
                "amazon_price": amazon_price,
                "color": variant.get("color"),
                "variant_label": variant.get("variant_label"),
                "model_family": variant.get("model_family"),
                "capacity": variant.get("capacity"),
                "pack_size": variant.get("pack_size"),
                "bsr": variant.get("bsr"),
                "notes": variant.get("notes"),
            })

    # Sort variants: pack_size=1 first (primary), multi-packs after (alternatives)
    variant_entries.sort(key=lambda e: (e.get("pack_size") or 1, e.get("net_cost") or 0))

    # Calculate profit for each variant
    import math
    variant_summaries: List[Dict[str, Any]] = []
    for entry in variant_entries:
        net_cost = entry["net_cost"]
        amazon_price = entry.get("amazon_price")
        # Guard against NaN values from pandas
        if isinstance(net_cost, float) and math.isnan(net_cost):
            net_cost = None
        if isinstance(amazon_price, float) and math.isnan(amazon_price):
            amazon_price = None
        # Apply overhead deduction to get effective net
        pack_size = entry.get("pack_size") or 1
        if isinstance(net_cost, (int, float)) and net_cost > 0:
            effective_net = calculate_effective_net(net_cost, amazon_price, overhead_pct)
            # Guard against NaN result (e.g. from NaN amazon_price)
            if isinstance(effective_net, float) and math.isnan(effective_net):
                effective_net = net_cost  # fallback to raw net
            
            # For multi-packs, compute per-unit net
            per_unit_eff_net = effective_net / pack_size
            
            if is_set:
                # For sets: profit is calculated after summing all nets (deferred below)
                profit = None  # Will be computed after all variants are processed
            elif lot_qty > 1:
                # For lots (same part × N): per-unit net applies to each unit, profit = per_unit_net*qty - total
                profit = (per_unit_eff_net * lot_qty) - total_sale
            else:
                profit = per_unit_eff_net - per_unit_price
        else:
            effective_net = None
            per_unit_eff_net = None
            profit = None
        
        if is_set:
            margin = None  # Deferred
        else:
            margin = (
                (profit / (per_unit_eff_net * lot_qty if lot_qty > 1 else per_unit_eff_net) * 100)
                if profit is not None and per_unit_eff_net not in (None, 0)
                else None
            )
        entry["profit"] = profit
        entry["effective_net"] = effective_net
        entry["per_unit_eff_net"] = per_unit_eff_net
        entry["margin_pct"] = margin
        variant_summaries.append({
            "part_number": entry["part_number"],
            "net": per_unit_eff_net,  # Store per-unit net for display/DB
            "raw_net": net_cost,
            "profit": profit,
            "margin_pct": margin,
            "sellable": entry["sellable"],
            "asin": entry["asin"],
            "color": entry["color"],
            "title": _format_variant_title(entry.get("part_number"), entry.get("variant_label"), pack_size),
            "bsr": entry.get("bsr"),
            "capacity": entry.get("capacity"),
            "pack_size": entry.get("pack_size"),
            "is_alternative": False,
        })

    # For set listings: compute combined profit (sum of per-unit nets - total_sale)
    if is_set and variant_entries:
        combined_net = sum(
            e.get("per_unit_eff_net") or 0.0 for e in variant_entries
        )
        combined_profit = combined_net - total_sale if combined_net > 0 else None
        combined_margin = (combined_profit / combined_net * 100) if combined_profit is not None and combined_net > 0 else None
        # Update each variant with the combined profit info
        for entry, vs in zip(variant_entries, variant_summaries):
            entry["profit"] = combined_profit
            entry["margin_pct"] = combined_margin
            vs["profit"] = combined_profit
            vs["margin_pct"] = combined_margin

    # Mark alternatives (first entry is primary, rest are alternatives)
    for idx, vs in enumerate(variant_summaries):
        vs["is_alternative"] = idx > 0

    # ─── Helper to format a single match block ───
    def _format_match_block(entry: Dict[str, Any], show_profit: bool = True) -> str:
        pack_size = entry.get("pack_size") or 1
        # Use per-unit effective net for display
        eff_net = entry.get("per_unit_eff_net") or entry.get("effective_net")
        prof = entry.get("profit")
        mar = entry.get("margin_pct") or 0.0
        p_emoji = _profit_marker(prof, entry["sellable"])
        sell_str = "🟩 Yes" if entry["sellable"] else "⛔ No"
        net_disp = _format_currency(eff_net)
        if pack_size > 1:
            net_disp += f" (per unit from {pack_size}-Pack)"
        if prof is None:
            prof_disp = "N/A"
        else:
            prof_disp = f"${prof:.2f} ({mar:.1f}%)"
        t_line = _format_variant_title(entry.get("part_number"), entry.get("variant_label"), pack_size)
        b_emoji, b_disp = _bsr_marker(entry.get("bsr"))
        blk = (
            f"Title: {t_line}\n"
            f"{_format_asin_line(entry.get('asin'))}\n"
            f"BSR: {b_emoji} {b_disp} | Sellable: {sell_str}\n"
            f"Net: {net_disp}\n"
        )
        if show_profit:
            blk += f"Profit: {p_emoji} {prof_disp}\n"
        if entry.get("notes"):
            blk += f"⚠️ Note: {entry['notes']}\n"
        return blk

    # ─── Build match section of message ───
    if variant_entries:
        # SET LISTING: multiple distinct part numbers in one listing
        if is_set and len(variant_entries) >= 2:
            msg += f"Set ({num_matched} items):\n"
            combined_net = sum(e.get("effective_net") or 0.0 for e in variant_entries)
            combined_profit = combined_net - total_sale if combined_net > 0 else None
            combined_margin = (combined_profit / combined_net * 100) if combined_profit is not None and combined_net > 0 else None
            
            for entry in variant_entries:
                msg += _format_match_block(entry, show_profit=False)
                msg += "\n"
            
            # Combined profit summary
            p_emoji = _profit_marker(combined_profit, True)
            if combined_profit is not None:
                msg += f"Combined Net: ${combined_net:.2f}\n"
                msg += f"Profit: {p_emoji} ${combined_profit:.2f} ({combined_margin:.1f}%)\n"
            else:
                msg += "Profit: N/A\n"
        
        # LOT LISTING: same part number × N
        elif lot_qty > 1:
            msg += f"Match ({lot_qty}x):\n"
            primary = variant_entries[0]
            msg += _format_match_block(primary)

            alternatives = variant_entries[1:]
            if alternatives:
                msg += "\nAlternative Match(s):\n"
                for alt in alternatives:
                    msg += _format_match_block(alt)
        
        # STANDARD: single match
        else:
            primary = variant_entries[0]
            msg += "Match:\n"
            msg += _format_match_block(primary)

            alternatives = variant_entries[1:]
            if alternatives:
                msg += "\nAlternative Match(s):\n"
                for alt in alternatives:
                    msg += _format_match_block(alt)

    else:
        msg += "Match:\nNo matching product found\n"

    return msg, variant_summaries


# ─── Database Persistence ──────────────────────────────────────────────────────

def _persist_processed_record(record: Dict[str, Any]) -> None:
    """Persist a processed listing and its matches to the database."""
    listing = record["listing"]
    details = record["details"]
    listing_id = record["item_id"]
    timestamp = int(time.time())
    listed_time = _format_listing_timestamp(listing.get("itemCreationDate"))
    link = listing.get("itemWebUrl", "")
    sale_type = _sale_type(listing)
    quantity = str(details.get("quantity"))
    price_obj = listing.get("price", {})
    try:
        sale_price = float(price_obj.get("value"))
    except (TypeError, ValueError):
        sale_price = 0.0
    shipping = details.get("shipping_value") or 0.0
    total = sale_price + (shipping or 0.0)
    
    message_id = insert_message(
        listing_id=listing_id,
        timestamp=timestamp,
        listed_time=listed_time,
        link=link,
        type_=sale_type,
        quantity=quantity,
        price=sale_price,
        shipping=shipping,
        total=total,
        message=record["message"],
    )

    for idx, summary in enumerate(record["variant_summaries"]):
        insert_match(
            message_id=message_id,
            is_alternative=1 if idx else 0,
            title=summary.get("title"),
            asin=summary.get("asin"),
            bsr=summary.get("bsr"),  # Now available from flattened sheet
            sellable=int(bool(summary.get("sellable"))),
            net_cost=summary.get("net"),
            profit=summary.get("profit"),
            pack_size=summary.get("pack_size"),  # Now available from flattened sheet
            color=summary.get("color"),
        )


# ─── Main Engine Function ──────────────────────────────────────────────────────

def lexmark(
    token: str,
    sheet_df: pd.DataFrame,
    *,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    """
    Process Lexmark listings using a preloaded sheet DataFrame.
    
    This is the main entry point called by the monitor loop.
    """
    if sheet_df is None or sheet_df.empty:
        _log("Sheet unavailable; aborting run.")
        return []

    pn_index = build_part_number_index(sheet_df)
    if not pn_index:
        _log("Sheet loaded but contains no part number entries.")
        return []
    
    _log(f"Part number index contains {len(pn_index)} unique part numbers")

    init_db()
    gc_old_ids()

    try:
        listings = search_lexmark_listings(token, limit=limit)
    except requests.HTTPError as exc:
        _log(f"Listing search failed: {exc}")
        return []

    current_time = datetime.now(LOCAL_TZ).strftime("%I:%M:%S %p")
    _log(f"{current_time} {len(listings)} listings fetched")

    if not listings:
        _log("No listings returned from eBay.")
        return []

    # Filter out already-seen listings
    candidates: List[Dict[str, Any]] = []
    for item in listings:
        item_id = item.get("itemId")
        if not item_id:
            continue
        if is_id_seen(item_id):
            continue
        candidates.append(item)
    _log(f"{len(candidates)} new listings to process")

    if not candidates:
        return []

    # Process oldest first
    candidates.reverse()

    processed: List[Dict[str, Any]] = []
    skipped_without_match = 0
    skipped_not_new = 0

    for listing in candidates:
        item_id = listing.get("itemId")
        _log(f"Processing itemId={item_id}")
        
        # Check condition
        cond = (listing.get("condition") or "").lower()
        _log(f"condition='{cond}'")
        if cond != "new":
            _log(f"Skipping {item_id} (not new)")
            add_seen_id(item_id)
            skipped_not_new += 1
            continue
        
        # Try to match against reference data
        variant_matches = resolve_listing_variants(listing, pn_index)
        if not variant_matches:
            skipped_without_match += 1
            _log(f"Skipping {item_id} (no part number matches)")
            add_seen_id(item_id)
            continue

        # Check minimum net cost threshold
        # Only skip if we have a valid non-zero net cost that's below threshold
        # If net_cost is 0 or None, we proceed (missing pricing data shouldn't block notifications)
        primary_net_cost = _primary_variant_net_cost(variant_matches)
        if primary_net_cost is not None and primary_net_cost > 0 and primary_net_cost < LEXMARK_MIN_NET_COST:
            _log(f"Skipping {item_id} (net cost ${primary_net_cost:.2f} < ${LEXMARK_MIN_NET_COST:.2f})")
            add_seen_id(item_id)
            continue

        # Fetch detailed listing info
        try:
            details = fetch_lexmark_details(item_id, token)
        except requests.HTTPError as exc:
            _log(f"Detail fetch failed for {item_id}: {exc}")
            continue

        image_count = len(details.get("images") or [])
        _log(f"found {image_count} image(s)")

        # Build message and persist
        message, variant_summaries = build_listing_message({
            "item_id": item_id,
            "listing": listing,
            "matches": variant_matches,
            "details": details,
        })
        
        record_payload = {
            "item_id": item_id,
            "listing": listing,
            "matches": variant_matches,
            "details": details,
            "message": message,
            "variant_summaries": variant_summaries,
        }
        
        try:
            _persist_processed_record(record_payload)
        except Exception as exc:
            _log(f"Failed to persist record for {item_id}: {exc}")
            continue
        
        # Mark as seen BEFORE attempting Telegram send to prevent reprocessing on notification failure
        add_seen_id(item_id)
        processed.append(record_payload)
        
        # Telegram notification is now decoupled - failure doesn't affect enrichment pipeline
        try:
            send_lexmark_telegram_message(
                record_payload["message"],
                record_payload["details"].get("images", []),
            )
            _log(f"Sent album for {item_id}")
        except Exception as exc:
            _log(f"Telegram send failed for {item_id}: {exc} (listing still processed)")

    # Summary logging
    if skipped_not_new:
        _log(f"{skipped_not_new} listings skipped (not new condition)")
    if skipped_without_match:
        _log(f"{skipped_without_match} unseen listings lacked part number matches")

    _log(f"Returning {len(processed)} enriched listings.")
    return processed


# ─── CLI Interface ─────────────────────────────────────────────────────────────

def main() -> None:
    """CLI interface for testing the Lexmark engine (uses SQL database)."""
    parser = argparse.ArgumentParser(description="Lexmark engine CLI tool (SQL-backed)")
    parser.add_argument(
        "--preview-rows",
        type=int,
        default=15,
        help="How many product rows to print (default: 15)",
    )
    parser.add_argument(
        "--fetch-listings",
        action="store_true",
        help="Fetch Lexmark toner listings and print them with matches",
    )
    parser.add_argument(
        "--token",
        type=str,
        default=os.getenv("EBAY_OAUTH_TOKEN"),
        help="Optional eBay OAuth token",
    )
    parser.add_argument(
        "--process-once",
        action="store_true",
        help="Load products from SQL, fetch listings, and process once",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Maximum listings to request when processing (default: 200)",
    )
    args = parser.parse_args()

    # Load products from SQL database
    _log("Loading Lexmark products from SQL database...")
    df = get_lexmark_products()
    if df is None or df.empty:
        _log("No Lexmark products found in database.")
        return
    _log(f"Loaded {len(df)} Lexmark products from SQL.")

    if args.process_once:
        token = args.token
        if not token:
            try:
                token = obtain_lexmark_token()
            except Exception as exc:
                _log(f"Failed to obtain OAuth token: {exc}")
                return
        
        enriched = lexmark(token, df, limit=args.limit)
        
        if not enriched:
            _log("No enriched listings produced.")
            return
        
        for idx, record in enumerate(enriched, start=1):
            listing = record["listing"]
            matches = record["matches"]
            seller = listing.get("seller", {}).get("username", "<unknown>")
            price_obj = listing.get("price", {})
            title = listing.get("title", "<missing title>")
            item_url = listing.get("itemWebUrl", "")
            price_value = price_obj.get("value")
            currency = price_obj.get("currency", "USD")
            price_label = f"{price_value} {currency}" if price_value else "n/a"
            print(
                f"[Process {idx}] {record['item_id']} | {seller} | {price_label}\n"
                f"             Title: {title}\n"
                f"             URL: {item_url or 'n/a'}\n"
                f"             Matches: {', '.join(m['part_number'].upper() for m in matches)}\n"
                f"             Images: {len(record['details']['images'])} | Shipping: {record['details']['shipping_label']}\n"
                f"\n{record['message']}\n{'-' * 40}"
            )
        return

    if args.fetch_listings:
        if not args.token:
            _log("--fetch-listings requested but no token supplied (use --token).")
            return
        
        listings = search_lexmark_listings(args.token)
        _log(f"Fetched {len(listings)} listings from eBay.")
        
        for idx, item in enumerate(listings, start=1):
            title = item.get("title")
            item_id = item.get("itemId")
            seller = item.get("seller", {}).get("username")
            print(f"[{idx}] {item_id} | {seller} | {title}")

        pn_index = build_part_number_index(df)
        if not pn_index:
            _log("No part numbers found in product data.")
            return

        # Find matches
        match_count = 0
        for listing in listings:
            matches = resolve_listing_variants(listing, pn_index)
            if matches:
                match_count += 1
                title = listing.get("title", "<missing title>")
                seller = listing.get("seller", {}).get("username", "<unknown>")
                price = listing.get("price", {})
                price_value = price.get("value")
                price_currency = price.get("currency")
                item_url = listing.get("itemWebUrl", "")
                price_label = f"{price_value} {price_currency}" if price_value else "n/a"
                print(
                    f"  [Match] Part Numbers: {', '.join(m['part_number'].upper() for m in matches)} | "
                    f"Seller: {seller} | Price: {price_label}\n"
                    f"          Title: {title}\n"
                    f"          URL: {item_url or 'n/a'}"
                )
                for match in matches:
                    for variant in match["variants"]:
                        pn = variant.get("part_number")
                        net = variant.get("net_cost")
                        asin = variant.get("asin")
                        sellable = "yes" if variant.get("sellable") else "no"
                        net_label = f"${net:.2f}" if isinstance(net, (int, float)) else "n/a"
                        print(
                            f"          • Variant -> part: {pn}, net: {net_label}, "
                            f"asin: {asin or 'n/a'}, sellable: {sellable}"
                        )
        
        if match_count == 0:
            _log("No part number matches found in current listing batch.")
        else:
            _log(f"Found {match_count} listings with part number matches.")
        return

    # Default: preview products from SQL
    _log(f"Showing first {args.preview_rows} products from SQL database:")
    preview = df.head(args.preview_rows)
    print(preview.to_string(index=False))


if __name__ == "__main__":
    main()
