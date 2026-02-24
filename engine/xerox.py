"""Xerox engine for eBay listing monitoring.

Loads Xerox product data from SQL database and monitors eBay for profitable listings.
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
    list_xerox_keywords as db_list_xerox_keywords,
)
from db.listings_db import (
    add_seen_id,
    gc_old_ids,
    init_db,
    insert_match,
    insert_message,
    is_id_seen,
)
from db.products_db import get_xerox_products, get_overhead_pct, calculate_effective_net, get_target_profit

load_dotenv()

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
XEROX_MIN_NET_COST = float(os.getenv("XEROX_MIN_NET_COST", "50"))

LOCAL_TZ = timezone(os.getenv("LOCAL_TIMEZONE", "America/Los_Angeles"))
SEPARATOR = "-" * 10

# Xerox SKUs typically interleave digits with a single letter (e.g., 006R01512).
# Use a permissive regex to capture those sequences from eBay titles, then
# post-filter against the known SKU index to avoid false positives.
SKU_TOKEN_REGEX = re.compile(r"\b[0-9A-Z]{5,}\b", re.IGNORECASE)


def _log(message: str) -> None:
    print(f"LOG - Xerox.py - {message}")


def get_excluded_sellers() -> List[str]:
    return [s.lower() for s in db_list_sellers()]


def get_excluded_keywords() -> List[str]:
    return [k.lower() for k in db_list_xerox_keywords()]


def _strip_currency(val: str) -> Optional[float]:
    try:
        clean = val.replace("$", "").replace(",", "").strip()
        if not clean:
            return None
        return float(clean)
    except Exception:
        return None


def _normalize_net_cost(val: Any) -> Optional[float]:
    if isinstance(val, str):
        return _strip_currency(val)
    if isinstance(val, (int, float)):
        try:
            return float(val)
        except (TypeError, ValueError):
            return None
    return None


def _parse_bsr(val: str) -> Optional[int]:
    try:
        clean = str(val or "").replace(",", "").strip()
        if not clean:
            return None
        return int(float(clean))
    except Exception:
        return None


def _clean_field(val: str) -> str:
    return str(val or "").strip()





def build_sku_index(df: pd.DataFrame) -> dict[str, List[dict]]:
    """Return a mapping of normalized SKU -> list of variant rows."""

    sku_map: dict[str, List[dict]] = {}
    for row in df.to_dict(orient="records"):
        sku = row.get("sku") or ""
        key = sku.strip().lower()
        if not key:
            continue
        sku_map.setdefault(key, []).append(row)
    return sku_map


def _normalize_sku(value: Optional[str]) -> str:
    return (value or "").strip().lower()


def extract_sku_candidates(title: Optional[str]) -> List[str]:
    """Return normalized SKU-like tokens extracted from a listing title."""

    if not title:
        return []
    tokens = []
    for match in SKU_TOKEN_REGEX.findall(title.upper()):
        has_digit = any(ch.isdigit() for ch in match)
        has_alpha = any(ch.isalpha() for ch in match)
        if not has_digit or not has_alpha:
            continue
        normalized = _normalize_sku(match)
        tokens.append(normalized)
    return tokens


def match_listings_to_sheet(
    listings: List[dict], sku_map: dict[str, List[dict]]
) -> List[dict]:
    """Cross-reference listings against the flattened sheet via SKU."""

    matches: List[dict] = []
    for item in listings:
        title = item.get("title") or ""
        seen: set[str] = set()
        for token in extract_sku_candidates(title):
            if token in seen:
                continue
            seen.add(token)
            variants = sku_map.get(token)
            if not variants:
                continue
            matches.append({"sku": token, "listing": item, "variants": variants})
    return matches


def resolve_listing_variants(
    listing: Dict[str, Any], sku_map: dict[str, List[dict]]
) -> List[Dict[str, Any]]:
    """Return all matching SKU tokens + variants for a single listing."""

    matches: List[Dict[str, Any]] = []
    title = listing.get("title") or ""
    if not title:
        return matches
    seen_tokens: set[str] = set()
    for token in extract_sku_candidates(title):
        if token in seen_tokens:
            continue
        seen_tokens.add(token)
        variants = sku_map.get(token)
        if variants:
            matches.append({"sku": token, "variants": variants})
    return matches


def search_xerox_listings(token: str, limit: int = 200) -> List[dict]:
    """Fetch Xerox toner listings, filter out excluded sellers/keywords."""
    url = (
        f"{BASE_URL}/item_summary/search"
        f"?q=xerox+toner&category_ids=16204"
        f"&filter=conditionIds:1000,deliveryCountry:US,itemLocationCountry:US"
        f"&aspect_filter=categoryId:16204,Brand:{{Xerox}}"
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
    return [
        it
        for it in items
        if it.get("seller", {}).get("username", "").lower() not in excluded_sellers
        and not any(
            kw in it.get("title", "").lower() for kw in excluded_keywords
        )
    ]


def obtain_xerox_token() -> str:
    if not EBAY_APP_ID or not EBAY_CLIENT_SECRET:
        raise RuntimeError("EBAY_APP_ID and EBAY_CLIENT_SECRET must be set for Xerox token fetch")
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
        raise RuntimeError("Failed to obtain eBay OAuth token for Xerox")
    return token


def fetch_xerox_details(
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


def _format_listing_timestamp(raw_ts: Optional[str]) -> str:
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
    if value is None:
        return "N/A"
    return f"${value:.2f}"


def _sale_type(listing: Dict[str, Any]) -> str:
    buying_opts = listing.get("buyingOptions", [])
    if "FIXED_PRICE" in buying_opts:
        label = "Fixed Price"
        if "BEST_OFFER" in buying_opts:
            label += " (Best Offer)"
        return label
    if any(opt in buying_opts for opt in ("AUCTION", "BID")):
        return "Auction"
    return ", ".join(buying_opts) or "Unknown"


def _bsr_marker(value: Optional[int]) -> Tuple[str, str]:
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


def _format_bsr_display(bsr_current: Optional[int], bsr_avg: Optional[int]) -> str:
    """
    Format BSR as dual display (current | 30d avg) when both values are available.

    - Both available:    "🟢 15,234 | 30d: 25,432"
    - Only 30d avg:      "🟢 25,432 (30d avg)"
    - Only current:      "🟢 15,234"
    - Neither:           "N/A"

    The emoji colour is driven by the current BSR; falls back to avg when only avg present.
    """
    if bsr_current is not None and bsr_avg is not None:
        emoji, _ = _bsr_marker(bsr_current)
        return f"{emoji} {bsr_current:,} | 30d: {bsr_avg:,}"
    if bsr_avg is not None:
        emoji, _ = _bsr_marker(bsr_avg)
        return f"{emoji} {bsr_avg:,} (30d avg)"
    if bsr_current is not None:
        emoji, _ = _bsr_marker(bsr_current)
        return f"{emoji} {bsr_current:,}"
    return "N/A"


def _profit_marker(profit: Optional[float], sellable: bool) -> str:
    if profit is None or not sellable:
        return ""
    target_profit = get_target_profit()
    return "💰" if profit >= target_profit else ""


def _format_asin_line(asin: Optional[str]) -> str:
    if not asin:
        return "ASIN: N/A"
    return f'ASIN: <a href="https://amazon.com/d/{asin}">{asin}</a>'


def _sanitize_variant_label(label: Optional[str]) -> str:
    text = (label or "").strip()
    return text.replace("*", "").strip()


def _format_variant_title(part_number: Optional[str], variant_label: Optional[str]) -> str:
    sanitized_label = _sanitize_variant_label(variant_label)
    base = f"Xerox {part_number or ''} {sanitized_label}".strip()
    return re.sub(r"\s+", " ", base).strip()


_last_telegram_post_ts = 0.0


def _primary_variant_net_cost(variant_matches: List[Dict[str, Any]]) -> Optional[float]:
    for match in variant_matches:
        for variant in match.get("variants", []):
            net_val = _normalize_net_cost(variant.get("net"))
            if net_val is not None:
                return net_val
    return None


def _ensure_telegram_ready(require_chat: bool = True) -> None:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
    if require_chat and not TELEGRAM_CHAT_ID:
        raise RuntimeError("TELEGRAM_CHAT_ID is not set")


def _pace_telegram_post() -> None:
    global _last_telegram_post_ts
    now = time.monotonic()
    elapsed = now - _last_telegram_post_ts
    wait = TELEGRAM_SEND_MIN_INTERVAL_SEC - elapsed
    if wait > 0:
        time.sleep(wait)
    _last_telegram_post_ts = time.monotonic()


def _telegram_request(method: str, payload: Dict[str, Any]) -> None:
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


def send_xerox_telegram_message(message: str, images: List[str]) -> None:
    _ensure_telegram_ready(require_chat=True)
    images = images[:20]  # hard cap to limit spam

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


def build_listing_message(
    record: Dict[str, Any],
) -> Tuple[str, List[Dict[str, Any]]]:
    listing = record["listing"]
    details = record["details"]
    matches = record["matches"]

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

    seller = listing.get("seller", {})
    username = seller.get("username", "N/A")
    fb_score = seller.get("feedbackScore", "N/A")
    fb_pct = seller.get("feedbackPercentage", "N/A")

    item_id = record.get("item_id") or listing.get("itemId") or "N/A"
    listed_time = _format_listing_timestamp(listing.get("itemCreationDate"))
    url = listing.get("itemWebUrl") or "n/a"
    sale_type = _sale_type(listing)
    quantity = details.get("quantity") or "N/A"

    msg = (
        f"{listing.get('title', '<missing title>')}\n\n"
        f"Seller: {username}  Feedback: {fb_score}, {fb_pct}%\n\n"
        f"Listed: {listed_time}\n"
        f"Link: <a href=\"{url}\">{item_id}</a>\n\n"
        f"Type: {sale_type}\n\n"
        f"Quantity: {quantity}\n"
        f"Price: ${price_str}\n"
        f"Shipping: ${ship_str}\n"
        f"Total: ${total_str}\n\n"
    )

    variant_entries: List[Dict[str, Any]] = []
    overhead_pct = get_overhead_pct()  # Get current overhead setting
    for match in matches:
        sku = (match.get("sku") or "").upper()
        for variant in match.get("variants", []):
            net_cost = _normalize_net_cost(variant.get("net"))
            amazon_price = variant.get("amazon_price")
            variant_entries.append(
                {
                    "sku": sku,
                    "part_number": variant.get("part_number"),
                    "variant_label": _sanitize_variant_label(
                        variant.get("variant_label")
                    ),
                    "asin": variant.get("asin"),
                    "bsr": _parse_bsr(variant.get("bsr")),
                    "bsr_current": _parse_bsr(variant.get("bsr_current")),
                    "sellable": bool(variant.get("sellable")),
                    "net_cost": net_cost,
                    "amazon_price": amazon_price,
                    "pack_size": variant.get("pack_size"),
                    "color": variant.get("color"),
                    "notes": variant.get("notes"),
                }
            )

    variant_summaries: List[Dict[str, Any]] = []
    for entry in variant_entries:
        net_cost = entry["net_cost"]
        amazon_price = entry.get("amazon_price")
        # Apply overhead deduction to get effective net
        if isinstance(net_cost, (int, float)):
            effective_net = calculate_effective_net(net_cost, amazon_price, overhead_pct)
            profit = effective_net - total_sale
        else:
            effective_net = None
            profit = None
        margin = (
            (profit / effective_net * 100)
            if profit is not None and effective_net not in (None, 0)
            else None
        )
        entry["profit"] = profit
        entry["effective_net"] = effective_net
        entry["margin_pct"] = margin
        variant_summaries.append(
            {
                "sku": entry["sku"],
                "part_number": entry["part_number"],
                "variant_label": entry["variant_label"],
                "net": entry.get("effective_net"),  # Use effective net after overhead
                "raw_net": net_cost,  # Original seller proceeds
                "profit": profit,
                "margin_pct": margin,
                "bsr": entry["bsr"],
                "sellable": entry["sellable"],
                "asin": entry["asin"],
                "pack_size": entry["pack_size"],
                "color": entry["color"],
                "title": _format_variant_title(
                    entry.get("part_number"), entry.get("variant_label")
                ),
            }
        )

    if variant_entries:
        primary = variant_entries[0]
        effective_net = primary.get("effective_net")
        profit = primary.get("profit")
        margin = primary.get("margin_pct") or 0.0
        profit_emoji = _profit_marker(profit, primary["sellable"])
        bsr_display = _format_bsr_display(primary.get("bsr_current"), primary["bsr"])
        sellable_str = "🟩 Yes" if primary["sellable"] else "⛔ No"
        net_display = _format_currency(effective_net)
        if profit is None:
            profit_display = "N/A"
        else:
            profit_display = f"${profit:.2f} ({margin:.1f}%)"
        title_line = _format_variant_title(
            primary.get("part_number"), primary.get("variant_label")
        )
        msg += (
            "Product Match\n"
            f"Title: {title_line}\n"
            f"{_format_asin_line(primary.get('asin'))}\n"
            f"BSR: {bsr_display} | Sellable: {sellable_str}\n"
            f"Net: {net_display}\n"
            f"Profit: {profit_emoji} {profit_display}\n"
        )
        
        # Add notes if present (e.g., "DO NOT BUY", "known bad match")
        if primary.get("notes") and str(primary["notes"]).strip().lower() not in ("none", ""):
            msg += f"⚠️ Note: {primary['notes']}\n"

        alternatives = variant_entries[1:]
        if alternatives:
            _log(f"Found {len(alternatives)} alternative set(s)")
            msg += "\nAlternative Match(s):\n"
            for alt in alternatives:
                alt_bsr_display = _format_bsr_display(alt.get("bsr_current"), alt["bsr"])
                alt_sellable_str = "🟩 Yes" if alt["sellable"] else "⛔ No"
                alt_profit = alt.get("profit")
                alt_margin = alt.get("margin_pct") or 0.0
                alt_profit_emoji = _profit_marker(alt_profit, alt["sellable"])
                alt_profit_display = (
                    f"${alt_profit:.2f} ({alt_margin:.1f}%)"
                    if alt_profit is not None
                    else "N/A"
                )
                alt_title = _format_variant_title(
                    alt.get("part_number"), alt.get("variant_label")
                )
                msg += (
                    f"Title: {alt_title}\n"
                    f"{_format_asin_line(alt.get('asin'))}\n"
                    f"BSR: {alt_bsr_display} | Sellable: {alt_sellable_str}\n"
                    f"Net: {_format_currency(alt.get('effective_net'))}\n"
                    f"Profit: {alt_profit_emoji} {alt_profit_display}\n\n"
                )
    else:
        msg += "Product Match\nNo matching product found\n"

    return msg, variant_summaries


def _persist_processed_record(record: Dict[str, Any]) -> None:
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
            bsr=summary.get("bsr"),
            sellable=int(bool(summary.get("sellable"))),
            net_cost=summary.get("net"),
            profit=summary.get("profit"),
            pack_size=summary.get("pack_size"),
            color=summary.get("color"),
        )


def xerox(
    token: str,
    sheet_df: pd.DataFrame,
    *,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    """Process Xerox listings using a preloaded sheet dataframe."""

    if sheet_df is None or sheet_df.empty:
        _log("Sheet unavailable; aborting run.")
        return []

    sku_index = build_sku_index(sheet_df)
    if not sku_index:
        _log("Sheet loaded but contains no SKU entries.")
        return []

    init_db()
    gc_old_ids()

    try:
        listings = search_xerox_listings(token, limit=limit)
    except requests.HTTPError as exc:
        _log(f"Listing search failed: {exc}")
        return []

    current_time = datetime.now(LOCAL_TZ).strftime("%I:%M:%S %p")
    _log(f"{current_time} {len(listings)} listings fetched")

    if not listings:
        _log("No listings returned from eBay.")
        return []

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

    candidates.reverse()

    processed: List[Dict[str, Any]] = []
    skipped_without_match = 0

    for listing in candidates:
        item_id = listing.get("itemId")
        _log(f"Processing itemId={item_id}")
        cond = (listing.get("condition") or "").lower()
        _log(f"condition='{cond}'")
        if cond != "new":
            _log(f"Skipping {item_id} (not new)")
            add_seen_id(item_id)
            continue
        variant_matches = resolve_listing_variants(listing, sku_index)
        if not variant_matches:
            skipped_without_match += 1
            _log(f"Skipping {item_id} (no SKU matches)")
            add_seen_id(item_id)
            continue

        primary_net_cost = _primary_variant_net_cost(variant_matches)
        if (
            primary_net_cost is not None
            and primary_net_cost < XEROX_MIN_NET_COST
        ):
            _log(
                f"Skipping {item_id} (net cost ${primary_net_cost:.2f} < ${XEROX_MIN_NET_COST:.2f})"
            )
            add_seen_id(item_id)
            continue

        try:
            details = fetch_xerox_details(item_id, token)
        except requests.HTTPError as exc:
            _log(f"Detail fetch failed for {item_id}: {exc}")
            continue

        image_count = len(details.get("images") or [])
        _log(f"found {image_count} image(s)")

        message, variant_summaries = build_listing_message(
            {
                "item_id": item_id,
                "listing": listing,
                "matches": variant_matches,
                "details": details,
            }
        )
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
            send_xerox_telegram_message(
                record_payload["message"],
                record_payload["details"].get("images", []),
            )
            _log(f"Sent album for {item_id}")
        except Exception as exc:
            _log(f"Telegram send failed for {item_id}: {exc} (listing still processed)")

    if skipped_without_match:
        _log(
            f"{skipped_without_match} unseen listings lacked SKU matches and were skipped."
        )

    _log(f"Returning {len(processed)} enriched listings.")
    return processed


def main() -> None:
    """CLI interface for testing the Xerox engine (uses SQL database)."""
    parser = argparse.ArgumentParser(description="Xerox engine CLI tool (SQL-backed)")
    parser.add_argument(
        "--preview-rows",
        type=int,
        default=15,
        help="How many product rows to print (default: 15)",
    )
    parser.add_argument(
        "--fetch-listings",
        action="store_true",
        help="Fetch Xerox toner listings and print them with matches",
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
    _log("Loading Xerox products from SQL database...")
    df = get_xerox_products()
    if df is None or df.empty:
        _log("No Xerox products found in database.")
        return
    _log(f"Loaded {len(df)} Xerox products from SQL.")

    if args.process_once:
        token = args.token
        if not token:
            try:
                token = obtain_xerox_token()
            except Exception as exc:
                _log(f"Failed to obtain OAuth token: {exc}")
                return
        enriched = xerox(token, df, limit=args.limit)
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
                f"             Matches: {', '.join(m['sku'].upper() for m in matches)}\n"
                f"             Images: {len(record['details']['images'])} | Shipping: {record['details']['shipping_label']}\n"
                f"\n{record['message']}\n{'-' * 40}"
            )
        return

    if args.fetch_listings:
        if not args.token:
            _log("--fetch-listings requested but no token supplied (use --token).")
            return
        listings = search_xerox_listings(args.token)
        _log(f"Fetched {len(listings)} listings from eBay.")
        for idx, item in enumerate(listings, start=1):
            title = item.get("title")
            item_id = item.get("itemId")
            seller = item.get("seller", {}).get("username")
            print(f"[{idx}] {item_id} | {seller} | {title}")

        sku_index = build_sku_index(df)
        if not sku_index:
            _log("No SKUs found in product data.")
            return

        matches = match_listings_to_sheet(listings, sku_index)
        if not matches:
            _log("No Xerox SKU matches found in the current listing batch.")
            return

        _log(f"Found {len(matches)} listing ↔ product matches:")
        for idx, match in enumerate(matches, start=1):
            listing = match["listing"]
            sku = match["sku"]
            title = listing.get("title", "<missing title>")
            seller = listing.get("seller", {}).get("username", "<unknown>")
            price = listing.get("price", {})
            price_value = price.get("value")
            price_currency = price.get("currency")
            item_url = listing.get("itemWebUrl", "")
            price_label = f"{price_value} {price_currency}" if price_value else "n/a"
            print(
                f"  [Match {idx}] SKU {sku.upper()} | Seller: {seller} | Price: {price_label}\n"
                f"             Title: {title}\n"
                f"             URL: {item_url or 'n/a'}"
            )
            for variant in match["variants"]:
                part = variant.get("part_number")
                label = variant.get("variant_label")
                net = variant.get("net")
                asin = variant.get("asin")
                sellable = "yes" if variant.get("sellable") else "no"
                net_label = f"${net:.2f}" if isinstance(net, (int, float)) else "n/a"
                print(
                    f"             • Variant -> part: {part}, label: {label}, net: {net_label}, "
                    f"asin: {asin or 'n/a'}, sellable: {sellable}"
                )
        return

    # Default: preview products from SQL
    _log(f"Showing first {args.preview_rows} products from SQL database:")
    preview = df.head(args.preview_rows)
    print(preview.to_string(index=False))


if __name__ == "__main__":
    main()
