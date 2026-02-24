
"""
canon.py
---------
Main logic for fetching, filtering, and processing Canon toner eBay listings.

Key responsibilities:
- Loads Canon product data from SQL database
- Fetches eBay listings via eBay API
- Filters out excluded sellers/keywords (from database)
- Matches listings to Canon product data
- Summarizes and sends results to Telegram
- Persists processed listings and matches in the database

Key dependencies:
- eBay API (requests)
- Telegram bot utilities (telegram_utils)
- Exclusion logic (exclusions_db)
- Database logic (listings_db, products_db)

Main functions:
- search_listings: Fetches and filters eBay listings
- canon: Orchestrates the end-to-end process

"""
# canon.py

import os
import re
import time
import json
from dataclasses import dataclass, field
from datetime import datetime
import requests
import pandas as pd
from urllib.parse import quote
from pytz import timezone, utc
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional, Tuple
import sys

# Add repo root to path for imports
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)


# ─── Lot Tracking Data Structures ──────────────────────────────────────────────
@dataclass
class LotBreakdown:
    """
    Represents the actual contents of a lot listing.
    Used for mixed lots like "5-YELLOW, 3-MAGENTA, 3-CYAN" or "Lot of 3".
    """
    model: Optional[str] = None
    capacity: str = "Standard"
    color_quantities: Dict[str, int] = field(default_factory=dict)  # {"black": 2, "cyan": 1, ...}
    lot_multiplier: int = 1                    # For "2 Sets" type situations
    total_units: int = 1                       # Sum of all units
    is_mixed_lot: bool = False                 # True if multiple colors OR quantities > 1
    is_color_set: bool = False                 # True if it's a CMY or CMYK set
    confidence: str = "high"                   # "high", "medium", "low" - uncertainty flag
    confidence_notes: List[str] = field(default_factory=list)  # Reasons for lower confidence
    raw_title: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON storage."""
        return {
            "model": self.model,
            "capacity": self.capacity,
            "color_quantities": self.color_quantities,
            "lot_multiplier": self.lot_multiplier,
            "total_units": self.total_units,
            "is_mixed_lot": self.is_mixed_lot,
            "is_color_set": self.is_color_set,
            "confidence": self.confidence,
            "confidence_notes": self.confidence_notes,
        }


@dataclass
class ColorMatch:
    """Individual color match within a lot."""
    color: str
    quantity: int
    unit_net: float
    subtotal: float
    asin: str
    bsr: Optional[int] = None
    sellable: bool = True


@dataclass
class SetAlternative:
    """Alternative way to fulfill/sell a lot using multi-packs."""
    pack_type: str          # "4 Color", "2 Black", etc.
    pack_size: int
    sets_needed: int        # How many of this pack needed
    total_units: int
    unit_net: float
    total_net: float
    asin: str
    bsr: Optional[int] = None
    sellable: bool = True
    leftover_units: int = 0  # Units that don't fit in sets


@dataclass 
class LotMatchResult:
    """
    Complete match result for a lot listing.
    Shows both "split to singles" and "combine to sets" profitability.
    """
    lot_breakdown: LotBreakdown
    
    # Individual color matches (for splitting lot to singles)
    individual_matches: List[ColorMatch] = field(default_factory=list)
    total_net_if_split: float = 0.0      # Value if sold as individual units
    
    # Set alternatives (for selling as sets or comparing to set purchases)
    set_alternatives: List[SetAlternative] = field(default_factory=list)
    best_set_net: float = 0.0            # Best value if sold/bought as sets
    
    # Profit calculations
    total_sale_price: float = 0.0        # What we pay on eBay
    profit_if_split: float = 0.0         # Profit selling as singles
    profit_if_sets: float = 0.0          # Profit selling as sets
    
    # Uncertainty
    has_unmatched_colors: bool = False
    unmatched_colors: List[str] = field(default_factory=list)

# from telegram_utils import send_telegram_message, send_media_group
from db.listings_db import init_db, is_id_seen, add_seen_id, gc_old_ids, insert_message, insert_match
from db.products_db import get_overhead_pct, calculate_effective_net, get_target_profit
from utils.telegram_service import send_media_group_with_caption

# ─── Load environment ──────────────────────────────────────────────────────
load_dotenv()

# ─── Configuration ────────────────────────────────────────────────────────────
ZIP_CODE = os.getenv("ZIP_CODE", "93012")
BASE_URL = "https://api.ebay.com/buy/browse/v1"
LOCAL_TZ = timezone("America/Los_Angeles")
CTX = quote(f"country=US,zip={ZIP_CODE}", safe="")
QUANTITY_EST = 1
MARKETPLACE_ID = "EBAY_US"

# TARGET_PROFIT is now loaded dynamically from settings via get_target_profit()
SEPARATOR = "-" * 10


# Use exclusions_db for excluded sellers and keywords
from db.exclusions_db import (
    list_sellers as db_list_sellers,
    list_canon_keywords as db_list_canon_keywords,
)

def get_excluded_sellers():
    return [s.lower() for s in db_list_sellers()]

def get_excluded_keywords():
    return [k.lower() for k in db_list_canon_keywords()]

TELEGRAM_RETRY_WAIT = 2
TELEGRAM_MAX_RETRIES = 5
TELEGRAM_POST_DELAY = 2


def parse_model(header: str) -> str:
    hdr = re.sub(r"\bcannon\b", "Canon", header, flags=re.IGNORECASE)
    m = re.search(r"Canon\s*([\w\-]+)", hdr, re.IGNORECASE)
    return m.group(1).strip() if m else (hdr.split()[0].strip() if hdr.split() else "")


def parse_capacity(cap: str) -> str:
    s = cap.lower()
    if "standard" in s:
        return "Standard"
    if "extra" in s:
        return "Extra-High"
    if "high" in s:
        return "High"
    return cap.strip()


def parse_net(val: str) -> Optional[float]:
    try:
        return float(str(val).replace("$", "").replace(",", ""))
    except:
        return None


def infer_pack_size(label: str) -> int:
    m = re.match(r"(\d+)", label)
    return int(m.group(1)) if m else 1


# ─── eBay API ───────────────────────────────────────────────────────────────
def search_listings(token: str, limit: int = 200) -> List[Dict[str, Any]]:
    """
    Fetches Canon toner listings from eBay API, filters out excluded sellers/keywords.
    Returns a list of filtered item dicts.
    """
    url = (
        f"{BASE_URL}/item_summary/search"
        f"?q=canon+toner&category_ids=16204"
        f"&filter=conditionIds:1000,deliveryCountry:US,itemLocationCountry:US"
        f"&aspect_filter=categoryId:16204,Brand:{{Canon}}"
        f"&sort=newlyListed&limit={limit}&fieldgroups=FULL"
    )
    hdr = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-ENDUSERCTX": f"contextualLocation={CTX}",
        "X-EBAY-C-MARKETPLACE-ID": MARKETPLACE_ID,
        "Content-Type": "application/json",
    }
    resp = requests.get(url, headers=hdr)
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


def fetch_details(item_id: str, token: str) -> Dict[str, Any]:
    ep = f"{BASE_URL}/item/{item_id}?quantity_for_shipping_estimate={QUANTITY_EST}"
    hdr = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-ENDUSERCTX": f"contextualLocation={CTX}",
        "X-EBAY-C-MARKETPLACE-ID": MARKETPLACE_ID,
        "Content-Type": "application/json",
    }
    r = requests.get(ep, headers=hdr)
    r.raise_for_status()
    d = r.json()

    qty = "Unavailable"
    for a in d.get("estimatedAvailabilities", []):
        if "estimatedAvailableQuantity" in a:
            qty = a["estimatedAvailableQuantity"]
            break
        if a.get("availabilityThresholdType") == "MORE_THAN":
            qty = f"More than {a.get('availabilityThreshold',0)}"
            break

    ship_opts = d.get("shippingOptions", [])
    ship_val = None
    ship_str = "N/A"
    if ship_opts:
        try:
            ship_val = float(ship_opts[0]["shippingCost"]["value"])
            ship_str = f"{ship_val:.2f}"
        except:
            pass

    images = []
    primary = d.get("image", {}).get("imageUrl")
    if primary:
        images.append(primary)
    for img in d.get("additionalImages", []):
        url = img.get("imageUrl")
        if url:
            images.append(url)

    # Limit to first 5 images only
    images = images[:5]

    # Description (prefer full; fallback to short)
    raw_description = ""
    if d.get("description"):
        raw_description = d["description"]
        print("LOG - Canon.py - Found long description")
    elif d.get("shortDescription"):
        raw_description = d["shortDescription"]
        print("LOG - Canon.py - Found short description")
    else:
        print("LOG - Canon.py - No description retrieved")

    return {
        "quantity": qty,
        "ship_val": ship_val,
        "ship_str": ship_str,
        "images": images,
        "description": raw_description,
    }
def fmt_time(raw: str) -> str:
    try:
        dt = datetime.strptime(raw, "%Y-%m-%dT%H:%M:%S.%fZ")
        return utc.localize(dt).astimezone(LOCAL_TZ).strftime("%b %d %Y, %I:%M %p")
    except:
        return "Unknown"


# ─── Matching helpers ───────────────────────────────────────────────────────
def extract_model(title: str, models: List[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Find the best sheet model match in title, plus an optional color-suffix.
    Now supports single-letter suffixes (C, M, Y, K) and the two-letter 'BK' for Black.
    Also handles CRG/CRTDG/Cartridge prefix formats (e.g., CRG137 -> 137, CRTDG051H -> 051H).
    """
    norm_title = re.sub(r"[\s\-]", "", title).upper()
    
    # Pre-process: Extract model from CRG/CRTDG/Cartridge formats
    # Extended patterns capture optional color suffix (BK, C, M, Y, K) directly
    # This handles cases like CRTDG051HBK where model and color are joined
    cartridge_patterns = [
        (r'\bCRTDG[- ]?(\d{2,4}[A-Z]?)(BK|C|M|Y|K)?\b', 'CRTDG'),       # CRTDG051H, CRTDG051HBK
        (r'\bCartridge[- ]?(\d{2,4}[A-Z]?)(BK|C|M|Y|K)?\b', 'Cartridge'), # Cartridge 051H, Cartridge-051HBK
        (r'\bCRG[- ]?(\d{2,4}[A-Z]?)(BK|C|M|Y|K)?\b', 'CRG'),           # CRG137, CRG137BK
    ]
    
    cmap = {"C": "Cyan", "M": "Magenta", "Y": "Yellow", "K": "Black", "BK": "Black"}
    
    for pattern, prefix_name in cartridge_patterns:
        crg_match = re.search(pattern, title, re.IGNORECASE)
        if crg_match:
            crg_model = crg_match.group(1).upper()
            attached_suffix = crg_match.group(2).upper() if crg_match.group(2) else None
            
            # Check if this model exists in our sheet
            for m in models:
                if m and m.upper() == crg_model:
                    # If we captured a color suffix directly, use it
                    if attached_suffix and attached_suffix in cmap:
                        return m, cmap[attached_suffix]
                    
                    # Check for color word after or nearby (e.g., "CRTDG051H BK" or "Cartridge 051H Black")
                    color_match = re.search(rf'{crg_model}\s*(BK|black|cyan|magenta|yellow)\b', title, re.IGNORECASE)
                    if color_match:
                        color_word = color_match.group(1).upper()
                        if color_word == "BK":
                            return m, "Black"
                        return m, color_match.group(1).capitalize()
                    return m, None
    
    # Try longer model strings first
    for m in sorted(models, key=lambda x: len(x or ""), reverse=True):
        norm_m = re.sub(r"[\s\-]", "", m).upper()
        if norm_m not in norm_title:
            continue
        # allow optional spaces/hyphens between each char of norm_m
        pat = "[- ]?".join(map(re.escape, norm_m))
        # capture optional suffix BK (black), or one of C, M, Y, K
        regex = re.compile(rf"\b{pat}(?P<suffix>BK|C|M|Y|K)?\b", re.IGNORECASE)
        mo = regex.search(title)
        if not mo:
            continue
        suf = mo.group("suffix")
        if suf:
            suf = suf.upper()
        cmap = {
            "C": "Cyan",
            "M": "Magenta",
            "Y": "Yellow",
            "K": "Black",
            "BK": "Black",
        }
        return m, (cmap.get(suf) if suf else None)
    return None, None


def extract_capacity(title: str) -> str:
    t = title.lower()
    if re.search(r"\bextra[- ]?high\b", t):
        return "Extra-High"
    if re.search(r"\bhigh[- ]?yield\b", t):
        return "High"
    return "Standard"


def extract_pack_size(title: str, valid: List[int]) -> int:
    """
    0) Parenthetical enumeration: "(K,C,M,Y)" or "(Cyan, Black, ...)"
    0.5) "full set" or "complete set" → 4
    1) "X color", "set of X", "X-pack"/"X pk", "dual pack", "triple pack"
    1e) literal "cmyk" or "bcmy" → 4
    2) count distinct CMYK words
    3) fallback to 1
    """
    t = title.lower()

    # 0) Parenthesis group: either a pure number "(4)" or a comma-list "(Black,Cyan,...)"
    paren = re.search(r"\(([^)]+)\)", t)
    if paren:
        content = paren.group(1).strip()
        # pure number → direct pack size
        if re.fullmatch(r"\d+", content):
            cnt = int(content)
            if cnt in valid:
                return cnt
        # comma-separated list → count entries
        if "," in content:
            items = [i.strip() for i in content.split(",") if i.strip()]
            cnt = len(items)
            if cnt in valid:
                return cnt

    # 0.5) full/complete set implies 4
    if re.search(r"\b(full|complete)\s+set\b", t) and 4 in valid:
        return 4

    # 1a) "<number> color"
    m = re.search(r"\b(\d+)\s*[- ]?color\b", t)
    if m:
        cnt = int(m.group(1))
        if cnt in valid:
            return cnt

    # 1b) "set of <number>"
    m = re.search(r"\bset of\s*(\d+)\b", t)
    if m:
        cnt = int(m.group(1))
        if cnt in valid:
            return cnt

    # 1c) "<number>-pack" or "<number> pk"
    m = re.search(r"\b(\d+)\s*[- ]?(?:pack|pk)\b", t)
    if m:
        cnt = int(m.group(1))
        if cnt in valid:
            return cnt

    # 1d) dual/triple pack
    if re.search(r"\bdual pack\b", t):
        return 2 if 2 in valid else 1
    if re.search(r"\btriple pack\b", t):
        return 3 if 3 in valid else 1

    # 1e) literal "cmyk" or "bcmy" → 4
    if re.search(r"\b(?:cmyk|bcmy)\b", t) and 4 in valid:
        return 4

    # 2) count distinct CMYK words
    colors = re.findall(r"\b(black|cyan|magenta|yellow)\b", t, re.IGNORECASE)
    unique = set(c.lower() for c in colors)
    if 2 <= len(unique) <= 4 and len(unique) in valid:
        return len(unique)

    # 3) fallback
    return 1


# ─── Mixed Lot Extraction Functions ─────────────────────────────────────────
# These functions detect and extract information from complex lot listings
# like "5-YELLOW, 3-MAGENTA, 3-CYAN" or "Lot of 3" or "TWO Canon 137"

def extract_lot_multiplier(title: str) -> Tuple[int, str]:
    """
    Detect lot/bundle multipliers in title.
    Returns (multiplier, confidence) where confidence is 'high', 'medium', or 'low'.
    
    Patterns detected:
    - "Lot of 3", "Lot Of 2" → high confidence
    - "TWO", "THREE", "FOUR" (word numbers at start or before model) → high
    - "2 Sets", "3 Sets" → high (but note: this multiplies the set size)
    - "x2", "x3" → medium (could be model number)
    - Quantity words in middle of title → medium
    """
    t = title.lower()
    original = title
    
    # Word numbers mapping
    word_nums = {
        "two": 2, "three": 3, "four": 4, "five": 5, 
        "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10
    }
    
    # Pattern 0: "(X)" at very start of title - high confidence
    # e.g., "(9) Canon GPR-51 Toner Cartridges"
    m = re.match(r'^\s*\((\d+)\)\s*', t)
    if m:
        num = int(m.group(1))
        if 2 <= num <= 20:  # Reasonable lot size
            return num, "high"
    
    # Pattern 1: "Lot of X" - high confidence
    m = re.search(r'\blot\s+of\s+(\d+)\b', t)
    if m:
        return int(m.group(1)), "high"
    
    # Pattern 1a: "Lot of WORD" - e.g., "Lot Of Six (6)"
    for word, num in word_nums.items():
        if re.search(rf'\blot\s+of\s+{word}\b', t):
            return num, "high"
    
    # Pattern 1b: "Set of X" - high confidence (same as Lot of X)
    # e.g., "Canon 046 Set Of 3 Toners (Black, Magenta, Yellow)"
    m = re.search(r'\bset\s+of\s+(\d+)\b', t)
    if m:
        num = int(m.group(1))
        if 2 <= num <= 20:  # Reasonable set size
            return num, "high"
    
    # Pattern 1c: "Qty X" or "Qty. X" - e.g., "Bundle Deal Canon Qty 16 GPR-31"
    m = re.search(r'\bqty\.?\s*(\d+)\b', t)
    if m:
        num = int(m.group(1))
        if 2 <= num <= 50:  # Allow larger quantities for Qty pattern
            return num, "high"
    
    # Pattern 2: Word number at START of title or right before "Canon"/"Genuine"
    for word, num in word_nums.items():
        # At start: "TWO Canon..."
        if re.match(rf'^{word}\b', t):
            return num, "high"
        # Before Canon/Genuine: "New TWO Canon..."
        if re.search(rf'\b{word}\s+(?:canon|genuine)\b', t):
            return num, "high"
    
    # Pattern 3: "X Sets" - high confidence (multiplies entire set)
    # BUT: be careful not to match model numbers like "046 set" where 046 is the Canon model
    m = re.search(r'\b(\d+)\s+sets?\b', t)
    if m:
        num = int(m.group(1))
        # Canon model numbers are typically 3 digits (045, 046, 054, 055, etc.) or larger
        # Real lot multipliers are typically 2-10
        if 2 <= num <= 10:
            return num, "high"
        # If >= 11, this is likely a model number being matched, skip it
    
    # Pattern 3b: "X Pack" or "X-Pack" - high confidence
    m = re.search(r'\b(\d+)\s*-?\s*(?:pack|pk)\b', t)
    if m:
        num = int(m.group(1))
        if 2 <= num <= 10:  # Reasonable pack size
            return num, "high"
    
    # Pattern 3c: "Nx" or "NX" at start - e.g., "3x Sealed Canon 045"
    m = re.match(r'^(\d+)\s*x\s+', t)
    if m:
        num = int(m.group(1))
        if 2 <= num <= 10:
            return num, "high"
    
    # Pattern 3d: "N X MODEL" - e.g., "5 X CANON GPR-55" (digit, space, X, space)
    m = re.search(r'\b(\d+)\s+x\s+(?:canon|genuine)\b', t)
    if m:
        num = int(m.group(1))
        if 2 <= num <= 20:
            return num, "high"
    
    # Pattern 4: Digit at start followed by space and Canon/model
    m = re.match(r'^(\d+)\s+(?:canon|genuine|new|oem)\b', t)
    if m:
        num = int(m.group(1))
        if 2 <= num <= 10:  # Reasonable lot size
            return num, "high"
    
    # Pattern 5: "xN" suffix - medium confidence (could be model)
    m = re.search(r'\bx(\d+)\b', t)
    if m:
        num = int(m.group(1))
        if 2 <= num <= 10:
            return num, "medium"
    
    # Pattern 6: Word number in middle - medium confidence
    for word, num in word_nums.items():
        if re.search(rf'\b{word}\b', t):
            return num, "medium"
    
    return 1, "high"


def extract_color_quantities(title: str) -> Tuple[Dict[str, int], str, List[str]]:
    """
    Extract color breakdown from title.
    Returns (color_dict, confidence, notes) where:
    - color_dict: {"yellow": 5, "magenta": 3, ...}
    - confidence: 'high', 'medium', 'low'
    - notes: list of reasons for lower confidence
    
    Patterns detected (in priority order):
    1. "5-YELLOW, 3-MAGENTA, 3-CYAN" → explicit quantities per color
    2. "CMY" or "CMYK" → abbreviation sets
    3. "Black/Cyan/Magenta/Yellow" → slash-separated colors
    4. "Cyan, Magenta, Yellow" → comma-separated colors
    5. Single color mentions → one of each mentioned
    """
    colors = {}
    notes = []
    confidence = "high"
    t = title.lower()
    
    # Normalize "blue" to "cyan" (common mistake)
    t = re.sub(r'\bblue\b', 'cyan', t)
    
    # First, identify Canon model numbers to exclude them from color quantity matching
    # Common Canon model patterns: 045, 055, 067, 069, 116, 118, 119, 128, 137, etc.
    # These are typically 2-4 digit numbers that appear after "Canon", "Cartridge", or at start
    # We need to mask these out before looking for "X-COLOR" patterns
    
    # Build list of model number positions to exclude
    model_patterns = [
        r'\bcanon\s+(\d{2,4})\b',           # "Canon 045"
        r'\bcartridge\s+(\d{2,4})\b',       # "Cartridge 055"
        r'\bgpr[- ]?(\d{1,3})\b',           # "GPR-55", "GPR55"
        r'\bcrg[- ]?(\d{1,3})\b',           # "CRG-137", "CRG137"
        r'\b(\d{3})[hH]?\s+(?:toner|cartridge|black|cyan|magenta|yellow)\b',  # "045H toner", "045 Black"
    ]
    
    # Find all model number matches and their digit values
    model_numbers = set()
    for pattern in model_patterns:
        for match in re.finditer(pattern, t):
            # Get the digit group
            for g in match.groups():
                if g and g.isdigit():
                    model_numbers.add(g)
    
    # Pattern 0: "(X) COLOR" - parenthetical quantity format like "(2) Magenta"
    # This is the highest confidence pattern - explicit quantity in parentheses
    paren_qty_matches = re.findall(r'\((\d+)\)\s*(black|cyan|magenta|yellow)\b', t)
    if paren_qty_matches:
        for qty_str, color in paren_qty_matches:
            qty = int(qty_str)
            if qty <= 20:  # Reasonable quantity (not a model number)
                colors[color] = colors.get(color, 0) + qty
        if colors:
            return colors, "high", notes
    
    # Pattern 1: Explicit color quantities with various formats
    # Match patterns like:
    #   "5-YELLOW" or "3-MAGENTA" (number-hyphen-color)
    #   "-1 Cyan -5 Black 1 magenta" (hyphen-number-space-color OR number-space-color)
    # But NOT model numbers like "045 Yellow"
    
    # Pattern 1a: "-N COLOR" format (hyphen before number)
    hyphen_prefix_matches = re.findall(r'-\s*(\d+)\s+(black|cyan|magenta|yellow)\b', t)
    if hyphen_prefix_matches:
        for qty_str, color in hyphen_prefix_matches:
            qty = int(qty_str)
            if qty <= 20:
                colors[color] = colors.get(color, 0) + qty
    
    # Pattern 1b: "N-COLOR" format (hyphen after number)
    explicit_matches = re.findall(r'(?:^|[\s,])(\d+)\s*-\s*(black|cyan|magenta|yellow)\b', t)
    if explicit_matches:
        for qty_str, color in explicit_matches:
            # Skip if this number is a model number
            if qty_str in model_numbers or qty_str.lstrip('0') in model_numbers:
                continue
            qty = int(qty_str)
            # Skip quantities that look like model numbers (common 2-3 digit Canon models)
            # Canon models are typically: 045, 046, 054, 055, 067, 069, 116, 118, 119, 128, 137
            # Real quantities should be small (1-20 typically)
            if qty > 20:
                # This is likely a model number, not a quantity
                continue
            colors[color] = colors.get(color, 0) + qty
    
    # Pattern 1c: "N COLOR" format (no hyphen, single digit followed by color)
    # Only match single digits to avoid model numbers (054, 055, etc.)
    # e.g., "-1 Cyan -5 Black  1  magenta" - the "1 magenta" at end
    single_digit_matches = re.findall(r'\b(\d)\s+(black|cyan|magenta|yellow)\b', t)
    if single_digit_matches:
        for qty_str, color in single_digit_matches:
            qty = int(qty_str)
            
            # Check if this digit is part of "Set of N"
            # If "Set of 3 Cyan Yellow Magenta", the "3" is the set size, not "3 Cyan"
            # Look behind for "set of"
            if re.search(rf'set\s+of\s+{qty_str}\s+{color}', t):
                continue
                
            if color not in colors:  # Only add if not already found by hyphen patterns
                colors[color] = colors.get(color, 0) + qty

    # Pattern 1d: "NxColor" or "Nx Color" (e.g. "2xBlack", "2x Black")
    nx_matches = re.findall(r'\b(\d+)x\s*(black|cyan|magenta|yellow)\b', t)
    if nx_matches:
        for qty_str, color in nx_matches:
            qty = int(qty_str)
            if qty <= 20:
                colors[color] = colors.get(color, 0) + qty

    # Pattern 2: CMYK/CMY abbreviations
    if re.search(r'\bcmyk\b', t) or re.search(r'\bkcym\b', t):
        colors = {"cyan": 1, "magenta": 1, "yellow": 1, "black": 1}
        return colors, "high", notes
    
    if re.search(r'\bcmy\b', t):
        colors = {"cyan": 1, "magenta": 1, "yellow": 1}
        return colors, "high", notes
    
    # Also check for "C M Y" or "C/M/Y" pattern (single letters)
    if re.search(r'\b[cC]\s*[/,]?\s*[mM]\s*[/,]?\s*[yY]\s*[/,]?\s*[kK]?\b', title):
        if 'k' in t or 'K' in title:
            colors = {"cyan": 1, "magenta": 1, "yellow": 1, "black": 1}
        else:
            colors = {"cyan": 1, "magenta": 1, "yellow": 1}
        return colors, "high", notes
    
    # Pattern 3: Slash-separated colors "Black/Cyan/Magenta/Yellow"
    slash_pattern = r'(black|cyan|magenta|yellow)(?:\s*/\s*(black|cyan|magenta|yellow))+'
    slash_match = re.search(slash_pattern, t)
    if slash_match:
        # Extract all colors in the slash group
        slash_section = slash_match.group(0)
        found_colors = re.findall(r'(black|cyan|magenta|yellow)', slash_section)
        for c in found_colors:
            colors[c] = colors.get(c, 0) + 1
        return colors, "high", notes
    
    # Pattern 4: Comma-separated in parentheses "(Black, Cyan, Magenta, Yellow)"
    paren_match = re.search(r'\(([^)]+)\)', t)
    if paren_match:
        content = paren_match.group(1)
        found_colors = re.findall(r'(black|cyan|magenta|yellow)', content)
        if found_colors:
            for c in found_colors:
                colors[c] = colors.get(c, 0) + 1
            return colors, "high", notes
    
    # Pattern 5: Just find all color mentions
    found_colors = re.findall(r'\b(black|cyan|magenta|yellow)\b', t)
    if found_colors:
        # Check if colors are repeated for emphasis vs indicating quantity
        color_set = set(found_colors)
        for c in color_set:
            colors[c] = 1  # Default to 1 each unless explicitly stated
        
        # Lower confidence if only finding scattered mentions
        if len(color_set) > 1:
            confidence = "medium"
            notes.append("Colors detected from scattered mentions, quantities assumed 1 each")
        
        return colors, confidence, notes
    
    return colors, "high", notes


def detect_set_type(title: str) -> Tuple[Optional[str], int]:
    """
    Detect if the listing is explicitly a "set" type.
    Returns (set_type, expected_colors) or (None, 0).
    
    Examples:
    - "Color Set" → ("color", 3 or 4)
    - "4 Color Set" → ("4color", 4)
    - "CMY Set" → ("cmy", 3)
    """
    t = title.lower()
    
    # "4 Color Set" or "4-Color Set"
    m = re.search(r'\b(\d+)\s*[-]?\s*color\s+set\b', t)
    if m:
        return f"{m.group(1)}color", int(m.group(1))
    
    # "Color Set" (generic - could be 3 or 4)
    if re.search(r'\bcolor\s+set\b', t):
        # Check if black is mentioned
        if re.search(r'\bblack\b', t):
            return "color", 4
        return "color", 3
    
    # "CMY Set" or "CMYK Set"
    if re.search(r'\bcmyk\s+set\b', t):
        return "cmyk", 4
    if re.search(r'\bcmy\s+set\b', t):
        return "cmy", 3
    
    # "Full Set" or "Complete Set"
    if re.search(r'\b(full|complete)\s+set\b', t):
        return "full", 4
    
    return None, 0


def build_lot_breakdown(title: str, sheet_df: pd.DataFrame) -> LotBreakdown:
    """
    Build a complete LotBreakdown from a listing title.
    
    CONSERVATIVE APPROACH: Only create breakdowns when we can be CERTAIN about quantities.
    If we can't determine exact color/quantity breakdown, we return a low-confidence result
    that should be EXCLUDED from purchased_units analytics.
    
    High confidence cases (will be included in purchased_units):
    - Single item with explicit color: "Canon 137 Black" → 1 black
    - Explicit quantities: "5-YELLOW, 3-MAGENTA, 3-CYAN" → exact counts
    - Single color lot: "Lot of 3 Canon 137 Black" → 3 black
    - Color set with matching count: "Canon 045 Set Of 3 (Black, Magenta, Yellow)" → 1 each
    
    Low confidence cases (will be EXCLUDED from purchased_units):
    - Multi-color lot without explicit per-color quantities: "(9) Canon GPR-51 CMYK"
    - Ambiguous multipliers: "5 X CANON GPR-55 Cyan Magenta Yellow"
    - Missing color info: "Lot of 16 GPR-31 Toner Set"
    """
    models = sheet_df["model"].dropna().unique().tolist()
    
    # Extract base information
    model, suffix_color = extract_model(title, models)
    capacity = extract_capacity(title)
    
    # Get lot multiplier
    lot_mult, mult_confidence = extract_lot_multiplier(title)
    
    # Get color quantities
    color_qtys, color_confidence, color_notes = extract_color_quantities(title)
    
    # Detect if it's a set
    set_type, expected_set_size = detect_set_type(title)
    
    confidence_notes = list(color_notes)
    
    # ─── Capacity adjustment based on model suffix ───────────────────────────
    if capacity == "Standard" and model:
        if re.search(r"XH$", model, re.IGNORECASE):
            capacity = "Extra-High"
        elif re.search(r"H$", model, re.IGNORECASE):
            capacity = "High"
    
    # ─── CONSERVATIVE LOT BREAKDOWN LOGIC ───────────────────────────────────
    # Only proceed with high confidence when we have explicit information
    
    overall_confidence = "high"
    
    # CASE 1: Explicit per-color quantities found (e.g., "5-YELLOW, 3-MAGENTA")
    # These are already in color_qtys with correct values - no modification needed
    if color_qtys and any(q > 1 for q in color_qtys.values()):
        # We have explicit quantities > 1, trust them
        # Check if lot_mult confirms or conflicts
        current_total = sum(color_qtys.values())
        if lot_mult > 1 and current_total != lot_mult:
            # Conflict between explicit colors and lot multiplier - mark as uncertain
            confidence_notes.append(f"Lot multiplier ({lot_mult}) conflicts with color sum ({current_total})")
            overall_confidence = "low"
        lot_mult = 1  # Don't apply multiplier, trust explicit quantities
    
    # CASE 2: Single color with lot multiplier (e.g., "Lot of 3 Canon 137 Black")
    elif lot_mult > 1 and len(color_qtys) == 1:
        color = list(color_qtys.keys())[0]
        color_qtys[color] = lot_mult
        lot_mult = 1
        # This is high confidence - single color, explicit count
    
    # CASE 3: Color set where lot_mult matches color count (e.g., "Set Of 3 (Black, Magenta, Yellow)")
    elif lot_mult > 1 and color_qtys and lot_mult == len(color_qtys) and all(q == 1 for q in color_qtys.values()):
        # lot_mult confirms "1 of each color"
        lot_mult = 1
        # High confidence - the numbers match up
    
    # CASE 4: No explicit quantities, just colors listed (e.g., "Canon 045 Black/Cyan/Magenta/Yellow")
    elif color_qtys and all(q == 1 for q in color_qtys.values()) and lot_mult == 1:
        # Single unit of each color mentioned - this is high confidence
        pass
    
    # CASE 5: Lot multiplier with multiple colors but no explicit per-color quantities
    # This is AMBIGUOUS - we don't know the distribution
    elif lot_mult > 1 and color_qtys and len(color_qtys) > 1:
        # Check if lot_mult is divisible by number of colors (e.g. Lot of 8, 4 colors -> 2 each)
        if lot_mult % len(color_qtys) == 0:
            per_color = lot_mult // len(color_qtys)
            for c in color_qtys:
                color_qtys[c] = per_color
            lot_mult = 1
            confidence_notes.append(f"Distributed lot of {lot_mult * len(color_qtys) * per_color} evenly across {len(color_qtys)} colors")
            # This is a reasonable assumption for "Lot of 8 KCYM"
        else:
            # Examples: "(9) Canon GPR-51 CMYK", "5 X CANON GPR-55 CMY", "Qty 16 GPR-31 Set"
            # We cannot determine if it's "9 of each" or "9 total distributed somehow"
            confidence_notes.append(f"Cannot determine per-color quantities for lot of {lot_mult} with {len(color_qtys)} colors")
            overall_confidence = "low"
            # Don't guess - leave color_qtys as-is (1 each) and set total to lot_mult
            # The consumer of this data should check confidence before using color_quantities
    
    # CASE 6: No colors detected
    elif not color_qtys:
        if suffix_color:
            # We have a color from model suffix (e.g., "137 Black" → black)
            color_qtys = {suffix_color.lower(): lot_mult if lot_mult > 1 else 1}
            lot_mult = 1
        elif lot_mult > 1:
            # Lot detected but no colors - cannot break down
            confidence_notes.append("Lot detected but colors not identified")
            overall_confidence = "low"
    
    # ─── Calculate totals ───────────────────────────────────────────────────
    if color_qtys:
        total_units = sum(color_qtys.values())
        # For low confidence multi-color lots, use lot_mult as total if higher
        if overall_confidence == "low" and lot_mult > 1:
            total_units = lot_mult
    else:
        total_units = lot_mult if lot_mult > 1 else 1
    
    # Determine if this is a mixed lot
    is_mixed = (
        len(color_qtys) > 1 or
        (len(color_qtys) == 1 and list(color_qtys.values())[0] > 1) or
        total_units > 1
    )
    
    # Is it a color set?
    is_color_set = set_type is not None or (len(color_qtys) >= 3 and all(q == 1 for q in color_qtys.values()))
    
    # Downgrade confidence based on extraction confidence
    if mult_confidence == "low" or color_confidence == "low":
        overall_confidence = "low"
    elif overall_confidence == "high" and (mult_confidence == "medium" or color_confidence == "medium"):
        overall_confidence = "medium"
        overall_confidence = "high"
    
    # Add notes for uncertainty
    if not model:
        confidence_notes.append("Could not identify Canon model")
        overall_confidence = "low"
    
    if not color_qtys and total_units > 1:
        confidence_notes.append("Lot detected but colors not identified")
        overall_confidence = "medium" if overall_confidence == "high" else overall_confidence
    
    return LotBreakdown(
        model=model,
        capacity=capacity,
        color_quantities=color_qtys,
        lot_multiplier=lot_mult if lot_mult > 1 else 1,
        total_units=total_units,
        is_mixed_lot=is_mixed,
        is_color_set=is_color_set,
        confidence=overall_confidence,
        confidence_notes=confidence_notes,
        raw_title=title,
    )


def is_mixed_lot_listing(title: str) -> bool:
    """
    Quick check to determine if a listing needs mixed lot processing.
    Returns True if the listing appears to be a complex lot that
    the standard match_listing() might not handle correctly.
    """
    t = title.lower()
    
    # Check for explicit lot indicators
    if re.search(r'\blot\s+of\s+\d+\b', t):
        return True
    
    # Check for word numbers at start
    if re.match(r'^(two|three|four|five|six|seven|eight|nine|ten)\b', t):
        return True
    
    # Check for "X Sets"
    if re.search(r'\b\d+\s+sets?\b', t):
        return True
    
    # Check for "X Pack" or "X-Pack" (e.g., "2 pack", "3-pack")
    m = re.search(r'\b(\d+)\s*-?\s*(?:pack|pk)\b', t)
    if m:
        num = int(m.group(1))
        if 2 <= num <= 10:  # Reasonable pack size
            return True
    
    # Check for explicit color quantities "5-YELLOW" or "(2) Yellow"
    # We need to be careful not to match model numbers like "Canon 045 Yellow"
    # Real quantity patterns:
    # - "(2) Yellow" or "(5) Magenta"
    # - "5-YELLOW" with hyphen (explicit quantity notation)
    # - Numbers under 20 followed by color (but NOT right after Canon/Cartridge/model pattern)
    
    # Pattern: "(X) COLOR" - high confidence
    if re.search(r'\(\d+\)\s*(black|cyan|magenta|yellow)', t):
        return True
    
    # Pattern: "X-COLOR" with hyphen - explicit quantity notation
    if re.search(r'\b\d+-\s*(black|cyan|magenta|yellow)', t):
        return True
    
    # Check for CMY/CMYK without "pack" (might be a color set)
    if re.search(r'\bcmy[k]?\b', t) and not re.search(r'pack', t):
        return True
    
    # Check for slash-separated colors indicating a set
    if re.search(r'(black|cyan|magenta|yellow)\s*/\s*(black|cyan|magenta|yellow)', t):
        return True
    
    # Check for hyphen-separated colors "Black-Cyan-Magenta-Yellow"
    if re.search(r'(black|cyan|magenta|yellow)\s*-\s*(black|cyan|magenta|yellow)\s*-\s*(black|cyan|magenta|yellow)', t):
        return True
    
    # Check for color enumeration in parentheses
    paren = re.search(r'\(([^)]+)\)', t)
    if paren:
        content = paren.group(1).lower()
        color_count = len(re.findall(r'(black|cyan|magenta|yellow)', content))
        if color_count >= 2:
            return True
    
    # Check for "Set" with color words nearby
    if re.search(r'\bset\b', t):
        color_count = len(set(re.findall(r'\b(black|cyan|magenta|yellow)\b', t)))
        if color_count >= 2:
            return True
    
    # Check for multiple colors with "and" or "&" or commas
    # e.g., "Black and Cyan and Magenta and Yellow"
    # e.g., "Black, Cyan, Magenta, Yellow"
    colors_found = set(re.findall(r'\b(black|cyan|magenta|yellow)\b', t))
    if len(colors_found) >= 3:
        # 3+ distinct colors mentioned = likely a set
        return True
    
    return False


def match_listing(title: str, sheet_df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    models = sheet_df["model"].dropna().unique().tolist()
    pack_sizes = sheet_df["pack_size"].dropna().astype(int).unique().tolist()

    # 1) model + suffix
    model, suffix = extract_model(title, models)
    if not model:
        return None

    # 2) capacity adjust
    capacity = extract_capacity(title)
    if capacity == "Standard":
        if re.search(r"XH$", model, re.IGNORECASE):
            capacity = "Extra-High"
        elif re.search(r"H$", model, re.IGNORECASE):
            capacity = "High"
    if capacity in ("High", "Extra-High") and not re.search(
        r"(?:XH|H)$", model, re.IGNORECASE
    ):
        up = model + ("XH" if capacity == "Extra-High" else "H")
        if up in models:
            model = up

    # 3) pack size
    pack_size = extract_pack_size(title, pack_sizes)

    # 4) filter sheet
    cand = sheet_df[
        (sheet_df["model"] == model)
        & (sheet_df["capacity"] == capacity)
        & (sheet_df["pack_size"] == pack_size)
    ]
    
    # 4a) Capacity fallback: if no match found for High/Extra-High, try Standard
    # This handles cases like GPR-55 where title says "High Yield" but DB only has Standard
    if cand.empty and capacity in ("High", "Extra-High"):
        cand = sheet_df[
            (sheet_df["model"] == model)
            & (sheet_df["capacity"] == "Standard")
            & (sheet_df["pack_size"] == pack_size)
        ]
    
    if cand.empty:
        return None

    # 5) single-color filter only if exactly one color word
    colors = re.findall(r"\b(black|cyan|magenta|yellow)\b", title, re.IGNORECASE)
    unique = set(c.lower() for c in colors)
    if pack_size == 1 and len(unique) == 1:
        desired = unique.pop()
        cand = cand[cand["color"].str.lower() == desired]
        if cand.empty:
            return None

    # 6) multi-pack prefer generic "Color"
    if pack_size > 1 and len(cand) > 1:
        colv = cand[cand["color"].str.lower() == "color"]
        if not colv.empty:
            cand = colv

    # 7) pick best candidate — prefer sellable with highest net_cost
    if len(cand) > 1:
        sellable = cand[cand["sellable"].str.strip().str.lower() == "sellable"]
        if not sellable.empty:
            cand = sellable
        cand = cand.sort_values("net", ascending=False, na_position="last")

    if len(cand) >= 1:
        row = cand.iloc[0]
        try:
            bsr_val = int(float(str(row["BSR"]).replace(",", "")))
        except:
            bsr_val = None
        try:
            net_val = float(row["net"])
        except:
            net_val = None
        try:
            amazon_price_val = float(row["amazon_price"]) if row.get("amazon_price") else None
        except:
            amazon_price_val = None
        return {
            "model": row["model"],
            "capacity": row["capacity"],
            "pack_size": row["pack_size"],
            "variant": row["variant"],
            "color": row["color"],
            "ASIN": row["ASIN"],
            "BSR": bsr_val,
            "net": net_val,
            "amazon_price": amazon_price_val,
            "sellable": str(row["sellable"]).strip().lower() == "sellable",
            "notes": row.get("notes", None),
        }
    return None


def find_multi_pack_alternatives(
    original: Dict[str, Any],
    sheet_df: pd.DataFrame,
    total_sale: float
) -> List[Dict[str, Any]]:
    """
    For a single-unit match (or pseudo-original) in `original`, return ALL sellable multi-packs:
      • if original.color=='black' → sizes [2,4,5]
      • else (Cyan/Magenta/Yellow) → sizes [3,4,5]
    Enforce:
      – same model & capacity
      – only 'sellable'
      – for size==2: color=='Black'
      – for size>=3: color=='Color'
      – for ps==4:
         • High-capacity: include “4 Color High” (and if original.color=='black', also “4 Color Mix”)
         • Standard-capacity: include only “4 Color Standard” (no Mix on same model)
    """
    model      = original['model']
    capacity   = original['capacity']
    orig_color = original['color'].lower()
    target_ps  = [2,4,5] if orig_color == 'black' else [3,4,5]
    overhead_pct = get_overhead_pct()  # Get current overhead setting

    results = []
    for ps in target_ps:
        # base filter
        cand = sheet_df[
            (sheet_df['model']    == model) &
            (sheet_df['capacity'] == capacity) &
            (sheet_df['pack_size'].astype(int) == ps) &
            (sheet_df['sellable'].str.lower() == 'sellable')
        ]
        # color rule
        if ps == 2:
            cand = cand[cand['color'].str.lower() == 'black']
        else:
            cand = cand[cand['color'].str.lower() == 'color']

        # special 4-pack logic
        if ps == 4:
            high_rows     = cand[cand['variant'].str.contains(r'\bHigh\b',    case=False)]
            standard_rows = cand[cand['variant'].str.contains(r'\bStandard\b',case=False)]
            mix_rows      = cand[cand['variant'].str.contains(r'\bMix\b',     case=False)]

            if capacity == 'High':
                selection = high_rows.copy()
                # if orig_color == 'black':
                #     selection = pd.concat([selection, mix_rows]).drop_duplicates()
                # The above logic for including 'Mix' variants is commented out as it is not working correctly and needs to be revisited later.
            else:
                selection = standard_rows.copy()
            cand = selection

        if cand.empty:
            continue

        # compute metrics
        for _, row in cand.iterrows():
            raw_net = float(row['net'])
            amazon_price = float(row['amazon_price']) if row.get('amazon_price') else None
            # Apply overhead to get effective net
            net = calculate_effective_net(raw_net, amazon_price, overhead_pct)
            unit_net    = net / ps
            unit_profit = unit_net - total_sale
            try:
                bsr_val = int(float(str(row['BSR']).replace(',', '')))
            except:
                bsr_val = None

            results.append({
                'model':        row['model'],
                'variant':      row['variant'],
                'ASIN':         row['ASIN'],
                'BSR':          bsr_val,
                'unit_net':     unit_net,
                'unit_profit':  unit_profit,
                'unit_sellable': True,
                'pack_size':    ps,
                'color':        row['color'],
            })

    return results


# ─── Mixed Lot Profit Calculator ────────────────────────────────────────────
# Calculates profit for complex lots in both directions:
# 1. Split lot → sell as singles (arbitrage: buy lot, sell individuals)
# 2. Combine into sets → sell as multi-packs (arbitrage: buy lot, resell as sets)

def calculate_lot_match(
    lot: LotBreakdown,
    sheet_df: pd.DataFrame,
    total_sale: float
) -> LotMatchResult:
    """
    Calculate comprehensive profit analysis for a mixed lot.
    
    This function handles the arbitrage calculation in both directions:
    1. SPLIT strategy: Buy the lot, sell each unit individually
       - Finds single-unit prices for each color
       - Sums up total value if sold as singles
       
    2. SET strategy: Buy the lot, combine into multi-packs to sell
       - Finds best matching set configurations
       - Calculates value if sold as sets (2-pack, 4-pack, etc.)
    
    Returns LotMatchResult with all calculations and alternatives.
    """
    result = LotMatchResult(
        lot_breakdown=lot,
        total_sale_price=total_sale,
    )
    
    if not lot.model or not lot.color_quantities:
        result.has_unmatched_colors = True
        result.unmatched_colors = ["Unknown - could not parse lot"]
        return result
    
    # ─── SPLIT STRATEGY: Value if sold as singles ───────────────────────────
    individual_matches = []
    unmatched_colors = []
    total_net_split = 0.0
    overhead_pct = get_overhead_pct()  # Get current overhead setting
    
    for color, qty in lot.color_quantities.items():
        # Find single-unit match for this model/capacity/color
        cand = sheet_df[
            (sheet_df['model'] == lot.model) &
            (sheet_df['capacity'] == lot.capacity) &
            (sheet_df['pack_size'] == 1) &
            (sheet_df['color'].str.lower() == color.lower())
        ]
        
        if not cand.empty:
            row = cand.iloc[0]
            raw_net = float(row['net']) if row['net'] else 0.0
            amazon_price = float(row['amazon_price']) if row.get('amazon_price') else None
            sellable = str(row['sellable']).strip().lower() == 'sellable'
            
            # Apply overhead deduction to get effective net
            unit_net = calculate_effective_net(raw_net, amazon_price, overhead_pct)
            
            try:
                bsr_val = int(float(str(row['BSR']).replace(',', '')))
            except:
                bsr_val = None
            
            # If not sellable, value is 0 for profit calculation
            effective_unit_net = unit_net if sellable else 0.0
            
            match = ColorMatch(
                color=color,
                quantity=qty,
                unit_net=unit_net,
                subtotal=effective_unit_net * qty,
                asin=row['ASIN'],
                bsr=bsr_val,
                sellable=sellable,
            )
            individual_matches.append(match)
            total_net_split += effective_unit_net * qty
        else:
            unmatched_colors.append(color)
    
    result.individual_matches = individual_matches
    result.total_net_if_split = total_net_split
    result.profit_if_split = total_net_split - total_sale
    result.has_unmatched_colors = len(unmatched_colors) > 0
    result.unmatched_colors = unmatched_colors
    
    # ─── SET STRATEGY: Value if sold as multi-packs ─────────────────────────
    set_alternatives = []
    
    # Analyze what sets could be formed from this lot
    color_qtys = lot.color_quantities.copy()
    has_black = 'black' in color_qtys
    has_cmy = all(c in color_qtys for c in ['cyan', 'magenta', 'yellow'])
    has_cmyk = has_cmy and has_black
    
    # Try 4-color sets (CMYK)
    if has_cmyk:
        min_cmyk_qty = min(color_qtys.get('black', 0), 
                          color_qtys.get('cyan', 0),
                          color_qtys.get('magenta', 0),
                          color_qtys.get('yellow', 0))
        if min_cmyk_qty > 0:
            # Find 4-pack in sheet - first get base candidates
            cand_4pack = sheet_df[
                (sheet_df['model'] == lot.model) &
                (sheet_df['capacity'] == lot.capacity) &
                (sheet_df['pack_size'] == 4) &
                (sheet_df['color'].str.lower() == 'color')
            ]
            
            # Apply variant filtering for High vs Standard capacity (like find_multi_pack_alternatives)
            if lot.capacity == 'High':
                high_rows = cand_4pack[cand_4pack['variant'].str.contains(r'\bHigh\b', case=False, na=False)]
                if not high_rows.empty:
                    cand_4pack = high_rows
            elif lot.capacity == 'Standard':
                standard_rows = cand_4pack[cand_4pack['variant'].str.contains(r'\bStandard\b', case=False, na=False)]
                if not standard_rows.empty:
                    cand_4pack = standard_rows
            
            # Prefer sellable, but keep non-sellable if that's all we have
            sellable_4pack = cand_4pack[cand_4pack['sellable'].str.lower() == 'sellable']
            if not sellable_4pack.empty:
                cand_4pack = sellable_4pack
            
            if not cand_4pack.empty:
                row = cand_4pack.iloc[0]
                raw_pack_net = float(row['net']) if row['net'] else 0.0
                amazon_price = float(row['amazon_price']) if row.get('amazon_price') else None
                # Apply overhead to pack net
                pack_net = calculate_effective_net(raw_pack_net, amazon_price, overhead_pct)
                sellable = str(row['sellable']).strip().lower() == 'sellable'
                try:
                    bsr_val = int(float(str(row['BSR']).replace(',', '')))
                except:
                    bsr_val = None
                
                # Calculate leftover units
                leftover = sum(color_qtys.values()) - (min_cmyk_qty * 4)
                
                # If not sellable, value is 0
                effective_total_net = (pack_net * min_cmyk_qty) if sellable else 0.0
                
                alt = SetAlternative(
                    pack_type="4 Color",
                    pack_size=4,
                    sets_needed=min_cmyk_qty,
                    total_units=min_cmyk_qty * 4,
                    unit_net=pack_net / 4,
                    total_net=effective_total_net,
                    asin=row['ASIN'],
                    bsr=bsr_val,
                    sellable=sellable,
                    leftover_units=leftover,
                )
                set_alternatives.append(alt)
    
    # Try 3-color sets (CMY only)
    if has_cmy:
        min_cmy_qty = min(color_qtys.get('cyan', 0),
                         color_qtys.get('magenta', 0),
                         color_qtys.get('yellow', 0))
        if min_cmy_qty > 0:
            # Find 3-pack in sheet
            cand_3pack = sheet_df[
                (sheet_df['model'] == lot.model) &
                (sheet_df['capacity'] == lot.capacity) &
                (sheet_df['pack_size'] == 3) &
                (sheet_df['color'].str.lower() == 'color')
            ]
            
            # Prefer sellable
            sellable_3pack = cand_3pack[cand_3pack['sellable'].str.lower() == 'sellable']
            if not sellable_3pack.empty:
                cand_3pack = sellable_3pack
                
            if not cand_3pack.empty:
                row = cand_3pack.iloc[0]
                raw_pack_net = float(row['net']) if row['net'] else 0.0
                amazon_price = float(row['amazon_price']) if row.get('amazon_price') else None
                # Apply overhead to pack net
                pack_net = calculate_effective_net(raw_pack_net, amazon_price, overhead_pct)
                sellable = str(row['sellable']).strip().lower() == 'sellable'
                try:
                    bsr_val = int(float(str(row['BSR']).replace(',', '')))
                except:
                    bsr_val = None
                
                leftover = sum(color_qtys.values()) - (min_cmy_qty * 3)
                
                effective_total_net = (pack_net * min_cmy_qty) if sellable else 0.0
                
                alt = SetAlternative(
                    pack_type="3 Color (CMY)",
                    pack_size=3,
                    sets_needed=min_cmy_qty,
                    total_units=min_cmy_qty * 3,
                    unit_net=pack_net / 3,
                    total_net=effective_total_net,
                    asin=row['ASIN'],
                    bsr=bsr_val,
                    sellable=sellable,
                    leftover_units=leftover,
                )
                set_alternatives.append(alt)
    
    # Try 2-pack Black sets
    if has_black and color_qtys.get('black', 0) >= 2:
        black_qty = color_qtys['black']
        sets_of_2 = black_qty // 2
        
        cand_2pack = sheet_df[
            (sheet_df['model'] == lot.model) &
            (sheet_df['capacity'] == lot.capacity) &
            (sheet_df['pack_size'] == 2) &
            (sheet_df['color'].str.lower() == 'black')
        ]
        
        # Prefer sellable
        sellable_2pack = cand_2pack[cand_2pack['sellable'].str.lower() == 'sellable']
        if not sellable_2pack.empty:
            cand_2pack = sellable_2pack
            
        if not cand_2pack.empty and sets_of_2 > 0:
            row = cand_2pack.iloc[0]
            raw_pack_net = float(row['net']) if row['net'] else 0.0
            amazon_price = float(row['amazon_price']) if row.get('amazon_price') else None
            # Apply overhead to pack net
            pack_net = calculate_effective_net(raw_pack_net, amazon_price, overhead_pct)
            sellable = str(row['sellable']).strip().lower() == 'sellable'
            try:
                bsr_val = int(float(str(row['BSR']).replace(',', '')))
            except:
                bsr_val = None
            
            leftover = black_qty % 2 + sum(v for k, v in color_qtys.items() if k != 'black')
            
            effective_total_net = (pack_net * sets_of_2) if sellable else 0.0
            
            alt = SetAlternative(
                pack_type="2 Black",
                pack_size=2,
                sets_needed=sets_of_2,
                total_units=sets_of_2 * 2,
                unit_net=pack_net / 2,
                total_net=effective_total_net,
                asin=row['ASIN'],
                bsr=bsr_val,
                sellable=sellable,
                leftover_units=leftover,
            )
            set_alternatives.append(alt)
    
    result.set_alternatives = set_alternatives
    
    # Find best set net value (considering we might still need to sell leftovers as singles)
    if set_alternatives:
        # For each set alternative, calculate total value including leftover singles
        best_set_value = 0.0
        for alt in set_alternatives:
            set_value = alt.total_net
            # Add value of leftover units if they can be sold as singles
            # This is approximate - in reality leftovers depend on which colors remain
            if alt.leftover_units > 0 and individual_matches:
                avg_single_value = total_net_split / lot.total_units if lot.total_units > 0 else 0
                set_value += avg_single_value * alt.leftover_units
            
            if set_value > best_set_value:
                best_set_value = set_value
        
        result.best_set_net = best_set_value
        result.profit_if_sets = best_set_value - total_sale
    
    return result


def format_lot_match_message(
    lot_result: LotMatchResult,
    title: str,
) -> str:
    """
    Format a LotMatchResult into a Telegram message section.
    Shows both split and set strategies for arbitrage decisions.
    Compact format to avoid Telegram message length limits.
    """
    lot = lot_result.lot_breakdown
    target_profit = get_target_profit()
    lines = []

    # Header with inline contents - e.g. "Mixed Lot: 1B 2C 2M 2Y (4 units)"
    color_abbrev = {"black": "B", "cyan": "C", "magenta": "M", "yellow": "Y"}
    if lot.color_quantities:
        contents_str = " ".join(
            f"{qty}{color_abbrev.get(color, color[0].upper())}"
            for color, qty in sorted(lot.color_quantities.items())
        )
        lines.append(f"Mixed Lot: {contents_str} ({lot.total_units} units)")
    else:
        lines.append("Mixed Lot Match")
    
    if lot.confidence != "high" and lot.confidence_notes:
        lines.append(f"⚠️ {'; '.join(lot.confidence_notes)}")
    
    # ─── SPLIT STRATEGY ─────────────────────────────────────────────────────
    if lot_result.individual_matches:
        lines.append("")
        lines.append("Singles:")
        
        # One line per color: "1x Black: $45.07 | 🟡161K | B003EHEKBG"
        for match in lot_result.individual_matches:
            bsr_emoji, _ = get_bsr_emoji(match.bsr)
            bsr_short = f"{match.bsr // 1000}K" if match.bsr and match.bsr >= 1000 else str(match.bsr or "N/A")
            sellable_mark = " ⛔" if not match.sellable else ""
            asin_link = f"<a href=\"https://amazon.com/d/{match.asin}\">{match.asin}</a>"
            lines.append(f"{match.quantity}x {match.color.capitalize()}: ${match.unit_net:.2f} | {bsr_emoji}{bsr_short} | {asin_link}{sellable_mark}")

        # Singles summary - Value, Profit, Unmatched
        lines.append(f"Value: ${lot_result.total_net_if_split:.2f}")

        profit = lot_result.profit_if_split
        profit_pct = (profit / lot_result.total_net_if_split * 100) if lot_result.total_net_if_split > 0 else 0
        unit_profit = profit / lot.total_units if lot.total_units > 0 else 0
        is_profitable = unit_profit >= target_profit
        profit_emoji = "💰" if is_profitable else ""

        lines.append(f"Profit: {profit_emoji}${profit:.2f} ({profit_pct:.1f}%) | unit: ${unit_profit:.2f}")

        # Show unmatched colors under Singles section
        if lot_result.has_unmatched_colors:
            lines.append(f"Unmatched: {', '.join(lot_result.unmatched_colors)}")
    
    # ─── SET STRATEGY ───────────────────────────────────────────────────────
    if lot_result.set_alternatives:
        lines.append("")
        lines.append("Sets:")
        
        for alt in lot_result.set_alternatives:
            bsr_emoji, _ = get_bsr_emoji(alt.bsr)
            bsr_short = f"{alt.bsr // 1000}K" if alt.bsr and alt.bsr >= 1000 else str(alt.bsr or "N/A")
            sellable_mark = " ⛔" if not alt.sellable else ""
            asin_link = f"<a href=\"https://amazon.com/d/{alt.asin}\">{alt.asin}</a>"
            
            leftover_str = f" (Color and leftover amount)" if alt.leftover_units > 0 else ""
            lines.append(f"{alt.sets_needed}x {alt.pack_type}: ${alt.total_net:.2f} | {bsr_emoji}{bsr_short} | {asin_link}{sellable_mark}{leftover_str}")
        
        # Sets profit summary
        profit = lot_result.profit_if_sets
        profit_pct = (profit / lot_result.best_set_net * 100) if lot_result.best_set_net > 0 else 0
        unit_profit = profit / lot.total_units if lot.total_units > 0 else 0
        is_profitable = unit_profit >= target_profit
        profit_emoji = "💰" if is_profitable else ""
        
        lines.append(f"Profit: {profit_emoji}${profit:.2f} ({profit_pct:.1f}%)")
        lines.append(f"unit: ${unit_profit:.2f}")
    
    return "\n".join(lines)


def safe_send_media_group(msg: str, images: List[str]) -> None:
    for attempt in range(1, TELEGRAM_MAX_RETRIES + 1):
        try:
            send_media_group_with_caption(msg, images)
            time.sleep(TELEGRAM_POST_DELAY)
            return
        except Exception as e:
            if "429" in str(e) and attempt < TELEGRAM_MAX_RETRIES:
                wait = TELEGRAM_RETRY_WAIT * (2 ** (attempt - 1))
                time.sleep(wait)
                continue
            raise


def get_bsr_emoji(bsr_val: Optional[int]) -> Tuple[str, str]:
    if bsr_val is None:
        return "", "N/A"
    bsr_str = f"{bsr_val:,}"
    if bsr_val < 150_000:
        emoji = "🟢"
    elif bsr_val < 250_000:
        emoji = "🟡"
    elif bsr_val < 500_000:
        emoji = "🟠"
    else:
        emoji = "🔴"
    return emoji, bsr_str


def get_profit_emoji(profit: float, sellable: bool, target_profit: float = None) -> str:
    if target_profit is None:
        target_profit = get_target_profit()
    return "💰" if sellable and profit >= target_profit else ""


# ─── Orchestrator ───────────────────────────────────────────────────────────

# first_run = True

def canon(token: str, lookup_df: pd.DataFrame, limit: int = 200) -> None:
    """
    Orchestrates the full Canon listing process:
    - Initializes DB, cleans up old IDs
    - Fetches and filters new eBay listings
    - Matches listings to Canon sheet
    - Sends results to Telegram
    - Persists results in DB
    """
    
    # global first_run
    # Initialize DB and garbage collect old IDs
    init_db()
    gc_old_ids()
    current_time = datetime.now(LOCAL_TZ).strftime("%I:%M:%S %p")
    listings = search_listings(token, limit)
    print(f"LOG - Canon.py - {current_time} {len(listings)} listings fetched")


    # --- Old logic (in-memory, commented out) ---
    # if first_run:
    #     candidates = listings[:limit]
    #     print(f"LOG - Canon.py - First run — processing first {len(candidates)} listings")
    # else:
    #     candidates = [it for it in listings if not is_id_seen(it["itemId"])]
    #     print(f"LOG - Canon.py - {len(candidates)} new listings to process")
    # candidates.reverse()
    # first_run = False

    # --- New logic: always filter by persistent DB ---
    candidates = [it for it in listings if not is_id_seen(it["itemId"])]
    print(f"LOG - Canon.py - {len(candidates)} new listings to process")
    candidates.reverse()

    # Cache settings once per cycle to avoid repeated DB reads
    cached_target_profit = get_target_profit()


    for it in candidates:
        # --- Process each new listing ---
        item_id = it["itemId"]
        print(f"LOG - Canon.py - Processing itemId={item_id}")

        cond = it.get("condition", "").lower()
        print(f"LOG - Canon.py - condition='{cond}'")
        if cond != "new":
            print(f"LOG - Canon.py - Skipping {item_id} (not new)")
            add_seen_id(item_id)
            continue

        dets    = fetch_details(item_id, token)
        images  = dets.get("images", [])
        print(f"LOG - Canon.py - found {len(images)} image(s)")

        title       = it.get("title", "")
        pack_sizes  = lookup_df["pack_size"].dropna().astype(int).tolist()
        ps          = extract_pack_size(title, pack_sizes)
        seller      = it.get("seller", {})
        username    = seller.get("username", "N/A")
        fb_score    = seller.get("feedbackScore", "N/A")
        fb_pct      = seller.get("feedbackPercentage", "N/A")
        listed_time = fmt_time(it.get("itemCreationDate", ""))
        url         = it.get("itemWebUrl", "")

        try:
            sale_price = float(it["price"]["value"])
        except:
            sale_price = 0.0
        ship_val   = dets.get("ship_val") or 0.0
        total_sale = sale_price + ship_val
        price_str  = f"{sale_price:.2f}" if sale_price else "N/A"
        ship_str   = dets.get("ship_str", "N/A")
        total_str  = f"{total_sale:.2f}" if sale_price else "N/A"

        opts = it.get("buyingOptions", [])
        if "FIXED_PRICE" in opts:
            sale_type = "Fixed Price" + (" (Best Offer)" if "BEST_OFFER" in opts else "")
        elif any(o in opts for o in ("AUCTION", "BID")):
            sale_type = "Auction"
        else:
            sale_type = ", ".join(opts) or "Unknown"


        msg = (
            f"{title}\n\n"
            f"{username} ({fb_score}, {fb_pct}%) | {listed_time}\n"
            f'<a href="{url}">{item_id}</a>\n'
            f"Type: {sale_type}\n"
            f"Qty: {dets.get('quantity')}\n"
            f"${price_str} + ${ship_str} = ${total_str}\n\n"
        )

        # ─── Check if this is a mixed lot that needs special handling ───────
        is_mixed = is_mixed_lot_listing(title)
        lot_breakdown = None
        lot_result = None
        
        if is_mixed:
            print(f"LOG - Canon.py - Detected mixed lot: {title[:50]}...")
            lot_breakdown = build_lot_breakdown(title, lookup_df)
            print(f"LOG - Canon.py - Lot breakdown: {lot_breakdown.total_units} units, colors: {lot_breakdown.color_quantities}")
        
        match = match_listing(title, lookup_df)
        matches_to_insert = []
        
        # ─── CASE 1: Standard single match found ────────────────────────────
        if match and not is_mixed:
            raw_net      = match["net"] or 0.0
            amazon_price = match.get("amazon_price")
            overhead_pct = get_overhead_pct()
            # Apply overhead deduction to get effective net
            net_cost     = calculate_effective_net(raw_net, amazon_price, overhead_pct)
            profit       = net_cost - total_sale
            profit_margin_pct = (profit / net_cost * 100) if net_cost > 0 else 0.0
            bsr_emoji, bsr = get_bsr_emoji(match["BSR"])
            prof_emoji   = get_profit_emoji(profit, match["sellable"], cached_target_profit)
            sellable_str = "🟩 Yes" if match["sellable"] else "⛔ No"

            msg += (
                "Matching Sheet Data\n"
                f"Title: Canon {match['model']} {match['variant']}\n"
                f"ASIN: <a href=\"https://amazon.com/d/{match['ASIN']}\">{match['ASIN']}</a>\n"
                f"BSR: {bsr_emoji} {bsr} | Sellable: {sellable_str}\n"
                f"Net: ${net_cost:.2f}\n"
                f"Profit: {prof_emoji} ${profit:.2f} ({profit_margin_pct:.1f}%)\n"
            )
            
            # Add notes if present (e.g., "DO NOT BUY", "known bad match")
            if match.get("notes") and str(match["notes"]).strip().lower() not in ("none", ""):
                msg += f"⚠️ Note: {match['notes']}\n"

            # Create lot_breakdown for single items to enable analytics
            # Convert numpy int64 to Python int for JSON serialization
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
                'bsr': match['BSR'],
                'sellable': int(match['sellable']),
                'net_cost': net_cost,
                'profit': profit,
                'pack_size': pack_size_int,
                'color': match.get('color', ''),
                'lot_breakdown': json.dumps(single_lot_breakdown),
                'total_units': pack_size_int,
            })

            if match["pack_size"] == 1:
                alts = find_multi_pack_alternatives(match, lookup_df, total_sale)
                print(f"LOG - Canon.py - Found {len(alts)} alternative set(s)")
                if alts:
                    msg += "\nAlternative Match(s):\n"
                    for alt in alts:
                        alt_emoji, alt_bsr   = get_bsr_emoji(alt["BSR"])
                        alt_prof_emoji       = get_profit_emoji(alt["unit_profit"], alt["unit_sellable"], cached_target_profit)
                        alt_sellable_str = "🟩 Yes" if alt["unit_sellable"] else "⛔ No"
                        alt_margin_pct = (alt["unit_profit"] / alt["unit_net"] * 100) if alt["unit_net"] > 0 else 0.0
                        msg += (
                            f"Title: Canon {alt['model']} {alt['variant']}\n"
                            f"ASIN: <a href=\"https://amazon.com/d/{alt['ASIN']}\">{alt['ASIN']}</a>\n"
                            f"BSR: {alt_emoji} {alt_bsr} | Sellable: {alt_sellable_str}\n"
                            f"Net: ${alt['unit_net']:.2f}\n"
                            f"Profit: {alt_prof_emoji} ${alt['unit_profit']:.2f} ({alt_margin_pct:.1f}%)\n\n"
                        )
                        matches_to_insert.append({
                            'is_alternative': 1,
                            'title': f"Canon {alt['model']} {alt['variant']}",
                            'asin': alt['ASIN'],
                            'bsr': alt['BSR'],
                            'sellable': int(alt['unit_sellable']),
                            'net_cost': alt['unit_net'],
                            'profit': alt['unit_profit'],
                            'pack_size': alt['pack_size'],
                            'color': alt.get('color', ''),
                        })

            msg += "\n"

        # ─── CASE 2: Mixed lot detected - use lot analysis ─────────────────
        elif is_mixed and lot_breakdown and lot_breakdown.model:
            lot_result = calculate_lot_match(lot_breakdown, lookup_df, total_sale)
            
            # Add the lot analysis message
            msg += format_lot_match_message(lot_result, title)
            msg += "\n\n"
            
            # Store individual color matches
            for cm in lot_result.individual_matches:
                matches_to_insert.append({
                    'is_alternative': 0,
                    'title': f"Canon {lot_breakdown.model} {cm.color.capitalize()}",
                    'asin': cm.asin,
                    'bsr': cm.bsr,
                    'sellable': int(cm.sellable),
                    'net_cost': cm.unit_net,
                    'profit': cm.subtotal - (total_sale / lot_breakdown.total_units * cm.quantity) if lot_breakdown.total_units > 0 else 0,
                    'pack_size': 1,
                    'color': cm.color,
                    'lot_breakdown': json.dumps(lot_breakdown.to_dict()),
                    'total_units': cm.quantity,
                })
            
            # Store set alternatives
            for alt in lot_result.set_alternatives:
                matches_to_insert.append({
                    'is_alternative': 1,
                    'title': f"Canon {lot_breakdown.model} {alt.pack_type}",
                    'asin': alt.asin,
                    'bsr': alt.bsr,
                    'sellable': int(alt.sellable),
                    'net_cost': alt.unit_net,
                    'profit': alt.total_net - total_sale,
                    'pack_size': alt.pack_size,
                    'color': 'Color',
                    'lot_breakdown': json.dumps(lot_breakdown.to_dict()),
                    'total_units': alt.total_units,
                })
            
            print(f"LOG - Canon.py - Mixed lot: {len(lot_result.individual_matches)} color matches, {len(lot_result.set_alternatives)} set options")

        # ─── CASE 3: No match found - try fallback matching ─────────────────
        else:
            msg += "Product Match\nNo matching product found\n"
            
            # If it was detected as mixed lot but we couldn't parse it
            if is_mixed and lot_breakdown:
                confidence_emoji = {"high": "🟢", "medium": "🟡", "low": "🔴"}.get(lot_breakdown.confidence, "⚪")
                msg += f"\n⚠️ Mixed Lot Detected {confidence_emoji}\n"
                if lot_breakdown.confidence_notes:
                    msg += f"Notes: {'; '.join(lot_breakdown.confidence_notes)}\n"
                if lot_breakdown.color_quantities:
                    color_parts = [f"{q}× {c.capitalize()}" for c, q in lot_breakdown.color_quantities.items()]
                    msg += f"Detected contents: {', '.join(color_parts)}\n"
                msg += f"Total units: {lot_breakdown.total_units}\n"
            
            # fallback for any true single-unit title (including High-capacity)
            detected = re.findall(r"(black|cyan|magenta|yellow|blue)", title, re.IGNORECASE)
            unique = {c.lower() for c in detected}
            # Relaxed check: if ps is 1 OR we have a single color mentioned and no obvious pack size > 1
            if (ps == 1 or ps is None) and len(unique) == 1:
                color = unique.pop()
                if color == "blue":
                    color = "cyan"
                model_ex, _ = extract_model(title, lookup_df["model"].dropna().tolist())
                capacity_ex = extract_capacity(title)
                
                if model_ex:
                    pseudo_orig = {
                        "model": model_ex,
                        "capacity": capacity_ex,
                        "color": color.capitalize(),
                    }
                    alts = find_multi_pack_alternatives(pseudo_orig, lookup_df, total_sale)
                    print(f"LOG - Canon.py - Found {len(alts)} alternative set(s) for no-match case")
                    if alts:
                        msg += "\nAlternative Match(s):\n"
                        for alt in alts:
                            alt_emoji, alt_bsr = get_bsr_emoji(alt["BSR"])
                            alt_prof = get_profit_emoji(alt["unit_profit"], alt["unit_sellable"], cached_target_profit)
                            alt_sellable_str = "🟩 Yes" if alt["unit_sellable"] else "⛔ No"
                            alt_margin_pct = (alt["unit_profit"] / alt["unit_net"] * 100) if alt["unit_net"] > 0 else 0.0
                            msg += (
                                f"Title: Canon {alt['model']} {alt['variant']}\n"
                                f"ASIN: <a href=\"https://amazon.com/d/{alt['ASIN']}\">{alt['ASIN']}</a>\n"
                                f"BSR: {alt_emoji} {alt_bsr} | Sellable: {alt_sellable_str}\n"
                                f"Net: ${alt['unit_net']:.2f}\n"
                                f"Profit: {alt_prof} ${alt['unit_profit']:.2f} ({alt_margin_pct:.1f}%)\n\n"
                            )
                            matches_to_insert.append({
                                'is_alternative': 1,
                                'title': f"Canon {alt['model']} {alt['variant']}",
                                'asin': alt['ASIN'],
                                'bsr': alt['BSR'],
                                'sellable': int(alt['unit_sellable']),
                                'net_cost': alt['unit_net'],
                                'profit': alt['unit_profit'],
                                'pack_size': alt['pack_size'],
                                'color': alt.get('color', ''),
                            })
            msg += "\n"

        try:
            # Send results to Telegram and persist in DB
            safe_send_media_group(msg, images)
            print(f"LOG - Canon.py - Sent album for {item_id}")
            add_seen_id(item_id)
            # Insert into messages and matches tables
            message_id = insert_message(
                listing_id=item_id,
                timestamp=int(time.time()),
                listed_time=listed_time,
                link=url,
                type_=sale_type,
                quantity=str(dets.get('quantity')),
                price=sale_price,
                shipping=ship_val,
                total=total_sale,
                message=msg
            )
            for m in matches_to_insert:
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
        except Exception as e:
            print(f"ERROR sending media for {item_id}: {e}")

# ---------------------------------------------------------------------------
# File summary:
# - Loads Canon sheet and eBay listings, filters, matches, and sends results
# - Calls: exclusions_db, listings_db, telegram_utils, summarize_description
# - Main entry: canon(token, lookup_df, limit)
# ---------------------------------------------------------------------------




