# Canon Title Parsing and Matching

This document provides a comprehensive explanation of how Canon toner cartridge titles are parsed, matched to product catalog data (stored in SQLite), and processed for profitability analysis.

---

## Table of Contents

1. [Purpose and Scope](#purpose-and-scope)
2. [Input Formats and Edge Cases](#input-formats-and-edge-cases)
3. [Parsing Rules](#parsing-rules)
4. [Matching Algorithm](#matching-algorithm)
5. [Lot Breakdown Examples](#lot-breakdown-examples)
6. [How to Modify and Extend](#how-to-modify-and-extend)
7. [Manual Validation Checklist](#manual-validation-checklist)

---

## Purpose and Scope

### What Title Parsing Accomplishes

**Goal:** Extract structured information from unstructured eBay listing titles to match against a known product catalog and calculate profitability.

**Challenges:**

1. **Title inconsistency:** Sellers use varied formats (e.g., "Canon 137 Black", "CRG-137BK", "Canon Cartridge 137 for ImageCLASS MF236n - Black")
2. **Mixed lots:** Bundles with multiple colors/quantities (e.g., "5-YELLOW, 3-MAGENTA, 3-CYAN", "Lot of 9 GPR-51 CMYK")
3. **Ambiguous multipliers:** "2 Sets", "TWO Canon 137", "(9) Toner Cartridges"
4. **Model number vs. quantity confusion:** "Canon 045 Yellow" (045 is model) vs. "5-Yellow" (5 is quantity)
5. **Color abbreviations:** "BK", "C", "M", "Y", "K", "CMYK", "BCMY"
6. **Pack size inference:** "4 Color Set", "Dual Pack", "Set of 3"

**Output:** Structured `LotBreakdown` dataclass with:
- `model` (e.g., "137")
- `capacity` ("Standard", "High", "Extra-High")
- `color_quantities` (e.g., `{"yellow": 5, "magenta": 3, "cyan": 3}`)
- `lot_multiplier` (e.g., 2 for "2 Sets")
- `total_units` (sum of all units)
- `is_mixed_lot` (True/False)
- `confidence` ("high", "medium", "low")
- `confidence_notes` (list of ambiguity warnings)

---

## Input Formats and Edge Cases

### Standard Single-Item Titles

**Format:** `<Brand> <Model> <Capacity?> <Color?> <Keywords>`

**Examples:**

1. `Canon 137 Black Toner Cartridge`
2. `Canon CRG-137BK Toner for ImageCLASS MF236n`
3. `Canon 045H Yellow High Yield`
4. `GPR-51 Cyan Toner Cartridge`
5. `Canon 055 Magenta Standard Yield`

**Expected Parse:**

| Title | Model | Capacity | Color | Pack Size |
|-------|-------|----------|-------|-----------|
| Canon 137 Black Toner | 137 | Standard | Black | 1 |
| CRG-137BK | 137 | Standard | Black | 1 |
| Canon 045H Yellow High Yield | 045H | High | Yellow | 1 |
| GPR-51 Cyan | GPR-51 | Standard | Cyan | 1 |

---

### Multi-Pack Titles (Sets)

**Format:** `<Model> <Pack Size> <Colors> Set/Pack`

**Examples:**

1. `Canon 137 4 Color Set (Black, Cyan, Magenta, Yellow)`
2. `Canon 045 Set of 3 - Cyan, Magenta, Yellow`
3. `Canon GPR-51 CMYK Full Set`
4. `Canon 055 Dual Pack - Black`

**Expected Parse:**

| Title | Model | Colors | Pack Size | Is Color Set |
|-------|-------|--------|-----------|--------------|
| 137 4 Color Set (B/C/M/Y) | 137 | black:1, cyan:1, magenta:1, yellow:1 | 4 | True |
| 045 Set of 3 (C/M/Y) | 045 | cyan:1, magenta:1, yellow:1 | 3 | True |
| GPR-51 CMYK Full Set | GPR-51 | cyan:1, magenta:1, yellow:1, black:1 | 4 | True |
| 055 Dual Pack Black | 055 | black:2 | 2 | False |

---

### Lot Multiplier Titles

**Format:** `Lot of <N>` or `<Word Number>` or `<N> Sets`

**Examples:**

1. `Lot of 3 Canon 137 Black`
2. `TWO Canon 045 Yellow High Yield`
3. `(5) Canon GPR-51 Cyan Toner Cartridges`
4. `2 Sets of Canon 137 CMYK (4 Colors)`

**Expected Parse:**

| Title | Lot Mult | Colors | Total Units | Confidence |
|-------|----------|--------|-------------|------------|
| Lot of 3 Canon 137 Black | 3 | black:3 | 3 | high |
| TWO Canon 045 Yellow | 2 | yellow:2 | 2 | high |
| (5) GPR-51 Cyan | 5 | cyan:5 | 5 | high |
| 2 Sets of 137 CMYK | 2 | black:2, cyan:2, magenta:2, yellow:2 | 8 | high |

---

### Complex Mixed Lot Titles (Explicit Quantities)

**Format:** `<Qty>-<Color>, <Qty>-<Color>, ...`

**Examples:**

1. `5-YELLOW, 3-MAGENTA, 3-CYAN Canon 137`
2. `(2) Black (1) Cyan Canon 045H`
3. `-1 Cyan -5 Black 1 magenta Canon 055`

**Expected Parse:**

| Title | Color Quantities | Total Units | Confidence |
|-------|------------------|-------------|------------|
| 5-YELLOW, 3-MAGENTA, 3-CYAN | yellow:5, magenta:3, cyan:3 | 11 | high |
| (2) Black (1) Cyan | black:2, cyan:1 | 3 | high |
| -1 Cyan -5 Black 1 magenta | cyan:1, black:5, magenta:1 | 7 | high |

---

### Ambiguous Mixed Lot Titles (Low Confidence)

**Format:** Lot multiplier + multiple colors without explicit per-color quantities

**Examples:**

1. `(9) Canon GPR-51 CMYK Toner Cartridges`
2. `5 X CANON GPR-55 Cyan Magenta Yellow`
3. `Lot of 16 GPR-31 Toner Set`

**Interpretation Issue:**

- Does "(9) CMYK" mean "9 of each color" (36 total) or "9 total distributed across 4 colors"?
- Does "5 X CMY" mean "5 sets of 3" (15 total) or "5 total"?

**Expected Parse:**

| Title | Lot Mult | Colors | Total Units | Confidence | Notes |
|-------|----------|--------|-------------|------------|-------|
| (9) GPR-51 CMYK | 9 | black:1, cyan:1, magenta:1, yellow:1 | 9 | low | Cannot determine per-color quantities for lot of 9 with 4 colors |
| 5 X GPR-55 CMY | 5 | cyan:1, magenta:1, yellow:1 | 5 | low | Cannot determine per-color quantities for lot of 5 with 3 colors |
| Lot of 16 GPR-31 Set | 16 | (none) | 16 | low | Lot detected but colors not identified |

**Conservative Approach:** Mark as low confidence, exclude from `purchased_units` analytics to avoid skewing per-color inventory data.

---

## Parsing Rules

### Rule 1: Model Extraction

**Function:** `extract_model(title, models)`

**Priority:** Longest model string first (avoids partial matches)

**Patterns:**

1. **CRG format:** `CRG-137`, `CRG137`, `CRG 137` → model = "137"
2. **GPR format:** `GPR-51`, `GPR51` → model = "GPR-51"
3. **Direct number:** `Canon 045`, `Cartridge 055` → model = "045"
4. **With suffix:** `137BK`, `137C`, `137M`, `137Y`, `137K` → model = "137", color from suffix

**Suffix Mapping:**

- `BK` or `K` → Black
- `C` → Cyan
- `M` → Magenta
- `Y` → Yellow

**Algorithm:**

```python
# Normalize title: remove spaces/hyphens
norm_title = re.sub(r"[\s\-]", "", title).upper()

# Check for CRG prefix first (e.g., CRG137 -> 137)
crg_match = re.search(r'\bCRG[- ]?(\d{2,4}[A-Z]?)\b', title, re.IGNORECASE)
if crg_match:
    model = crg_match.group(1)
    # Check for color suffix (BK, C, M, Y, K)
    if re.search(rf'\bCRG[- ]?{model}(BK|C|M|Y|K)\b', title, re.IGNORECASE):
        suffix_color = map_suffix_to_color(suffix)
        return model, suffix_color

# Try exact model match with optional suffix
for model in sorted(models, key=len, reverse=True):
    pattern = f"{model}(?P<suffix>BK|C|M|Y|K)?"
    if re.search(pattern, norm_title, re.IGNORECASE):
        return model, suffix_color_or_none
```

**Edge Cases:**

- **Partial matches:** "137" in "1370" → Use word boundaries `\b137\b`
- **Spaces in model:** "GPR 51" vs. "GPR-51" → Normalize to "GPR51" or "GPR-51" (sheet must be consistent)
- **Multiple models in title:** "Compatible with Canon 137, 137H" → Return first match

---

### Rule 2: Capacity Extraction

**Function:** `extract_capacity(title)`

**Patterns:**

1. **Explicit keywords:**
   - "High Yield" → "High"
   - "Extra High" → "Extra-High"
   - "Standard Yield" → "Standard"
   - (No keyword) → "Standard" (default)

2. **Model suffix inference:**
   - Model ends with "H" (e.g., "137H") → "High" (if not already set)
   - Model ends with "XH" (e.g., "045XH") → "Extra-High"

**Algorithm:**

```python
capacity = "Standard"  # default

if re.search(r"\bextra[- ]?high\b", title, re.IGNORECASE):
    capacity = "Extra-High"
elif re.search(r"\bhigh[- ]?yield\b", title, re.IGNORECASE):
    capacity = "High"

# Adjust based on model suffix
if capacity == "Standard" and model:
    if re.search(r"XH$", model, re.IGNORECASE):
        capacity = "Extra-High"
    elif re.search(r"H$", model, re.IGNORECASE):
        capacity = "High"

# If capacity is High/Extra-High but model doesn't have suffix, try adding it
if capacity in ("High", "Extra-High") and not re.search(r"(X?H)$", model, re.IGNORECASE):
    # Check if model+"H" or model+"XH" exists in sheet
    # If so, update model to include suffix
```

---

### Rule 3: Pack Size Extraction

**Function:** `extract_pack_size(title, valid_pack_sizes)`

**Priority order (first match wins):**

0. **Parenthetical enumeration:** `(K,C,M,Y)` or `(Cyan, Black, Magenta, Yellow)` → count items
1. **Full/Complete Set:** `Full Set` or `Complete Set` → 4 (assumes CMYK)
2. **Explicit number with "color":** `4 Color`, `3-Color` → extract number
3. **"Set of N":** `Set of 3` → 3
4. **"N-pack" or "N pk":** `4-pack`, `3 pk` → extract number
5. **"Dual Pack":** → 2
6. **"Triple Pack":** → 3
7. **Literal "CMYK" or "BCMY":** → 4
8. **Count distinct color words:** "Black Cyan Magenta Yellow" → 4
9. **Fallback:** → 1

**Algorithm:**

```python
# 0) Parenthesis: count items
paren = re.search(r"\(([^)]+)\)", title)
if paren:
    content = paren.group(1)
    if re.fullmatch(r"\d+", content):  # Pure number
        return int(content) if int(content) in valid_pack_sizes else 1
    if "," in content:  # Comma-separated list
        items = content.split(",")
        count = len([i for i in items if i.strip()])
        return count if count in valid_pack_sizes else 1

# 1-7) Explicit patterns (see code in canon.py lines 490-520)

# 8) Count distinct color words
colors = re.findall(r"\b(black|cyan|magenta|yellow)\b", title, re.IGNORECASE)
unique_colors = set(c.lower() for c in colors)
if 2 <= len(unique_colors) <= 4 and len(unique_colors) in valid_pack_sizes:
    return len(unique_colors)

# 9) Default
return 1
```

**Edge Cases:**

- **"2 Pack" vs. "2-Pack":** Both handled (regex accepts optional hyphen)
- **"Dual Pack Black" vs. "Dual Pack CMYK":** Pack size = 2 (first match), but sheet matching uses color count
- **Valid pack sizes:** Constrained by Google Sheet (typically [1, 2, 3, 4], sometimes includes 6, 10)

---

### Rule 4: Lot Multiplier Extraction

**Function:** `extract_lot_multiplier(title)`

**Returns:** `(multiplier: int, confidence: str)`

**Patterns (descending confidence):**

1. **"(X)" at title start (high):** `(9) Canon GPR-51` → 9
2. **"Lot of X" (high):** `Lot of 3 Canon 137` → 3
3. **Word numbers at start (high):** `TWO Canon 045`, `THREE Canon 137` → 2, 3
4. **"X Sets" (high):** `2 Sets of Canon 137 CMYK` → 2
5. **"xX" notation (medium):** `Canon 137 x3` → 3 (could be model number ambiguity)
6. **Word numbers mid-title (medium):** `Canon FIVE 137 Black` → 5

**Word-to-Number Mapping:**

```python
word_nums = {
    "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10
}
```

**Algorithm:**

```python
# Pattern 0: (X) at start
m = re.match(r'^\s*\((\d+)\)\s*', title)
if m and 2 <= int(m.group(1)) <= 20:
    return int(m.group(1)), "high"

# Pattern 1: Lot of X
m = re.search(r'\blot\s+of\s+(\d+)\b', title, re.IGNORECASE)
if m:
    return int(m.group(1)), "high"

# Pattern 2: Word numbers at start
for word, num in word_nums.items():
    if re.match(rf'^{word}\b', title, re.IGNORECASE):
        return num, "high"

# Pattern 3: X Sets
m = re.search(r'\b(\d+)\s+sets?\b', title, re.IGNORECASE)
if m:
    return int(m.group(1)), "high"

# Pattern 4: xX notation
m = re.search(r'\bx(\d+)\b', title, re.IGNORECASE)
if m and 2 <= int(m.group(1)) <= 10:
    return int(m.group(1)), "medium"

# Default: no multiplier
return 1, "high"
```

**Edge Cases:**

- **"(045) Canon Yellow":** Parenthetical number is model, not quantity → Avoid by checking if number > 20 or if it appears near "Canon"
- **"X Sets of Y Colors":** Multiplier = X, pack size = Y (handle separately)

---

### Rule 5: Color Quantity Extraction

**Function:** `extract_color_quantities(title)`

**Returns:** `(color_quantities: Dict[str, int], confidence: str, notes: List[str])`

**Patterns (descending priority):**

0. **"(X) COLOR" (high):** `(2) Magenta`, `(5) Black` → {"magenta": 2}, {"black": 5}
1. **"X-COLOR" with hyphen (high):** `5-YELLOW`, `3-MAGENTA` → {"yellow": 5, "magenta": 3}
2. **"-X COLOR" (high):** `-1 Cyan -5 Black` → {"cyan": 1, "black": 5}
3. **"XxCOLOR" (high):** `2xBlack`, `3x Yellow` → {"black": 2, "yellow": 3}
4. **"CMYK" or "CMY" literals (high):** → {"cyan": 1, "magenta": 1, "yellow": 1, "black": 1 (if K)}
5. **Slash-separated (high):** `Black/Cyan/Magenta/Yellow` → 1 each
6. **Comma-separated in parentheses (high):** `(Black, Cyan, Magenta, Yellow)` → 1 each
7. **Single digit followed by color (medium):** `1 Cyan`, `5 Black` → {"cyan": 1, "black": 5} (only if not part of "Set of X")
8. **Scattered mentions (low):** Just finding color words → 1 each (downgrade confidence to "medium")

**Model Number Exclusion:**

Before applying patterns, identify Canon model numbers (045, 055, 067, 069, 116, 118, 119, 128, 137) to avoid confusing them with quantities.

**Algorithm:**

```python
# Normalize "blue" to "cyan" (common mistake)
title_lower = re.sub(r'\bblue\b', 'cyan', title.lower())

# Identify model numbers to exclude
model_patterns = [
    r'\bcanon\s+(\d{2,4})\b',
    r'\bcartridge\s+(\d{2,4})\b',
    r'\bgpr[- ]?(\d{1,3})\b',
    r'\bcrg[- ]?(\d{1,3})\b',
]
model_numbers = set()
for pattern in model_patterns:
    for match in re.finditer(pattern, title_lower):
        model_numbers.add(match.group(1))

# Pattern 0: (X) COLOR
matches = re.findall(r'\((\d+)\)\s*(black|cyan|magenta|yellow)\b', title_lower)
if matches:
    for qty_str, color in matches:
        qty = int(qty_str)
        if qty <= 20 and qty_str not in model_numbers:
            colors[color] = colors.get(color, 0) + qty
    return colors, "high", []

# Pattern 1: X-COLOR with hyphen
matches = re.findall(r'(?:^|[\s,])(\d+)\s*-\s*(black|cyan|magenta|yellow)\b', title_lower)
if matches:
    for qty_str, color in matches:
        if qty_str in model_numbers:
            continue  # Skip model numbers
        qty = int(qty_str)
        if qty <= 20:
            colors[color] = colors.get(color, 0) + qty

# ... (continue with patterns 2-8)

# If no explicit quantities found, default to 1 each
if not colors:
    colors_found = re.findall(r'\b(black|cyan|magenta|yellow)\b', title_lower)
    for c in set(colors_found):
        colors[c] = 1
    confidence = "medium" if len(colors) > 1 else "high"
    notes = ["Colors detected from scattered mentions, quantities assumed 1 each"]

return colors, confidence, notes
```

**Edge Cases:**

- **"Canon 045 Yellow" vs. "5-Yellow":** 045 is model (excluded via model_numbers), 5 is quantity
- **"2 Sets of 3 Colors":** Don't extract "2" as color quantity (it's lot multiplier), don't extract "3" (it's pack size)
- **"C/M/Y/K" single letters:** Handle as color abbreviations

---

### Rule 6: Set Type Detection

**Function:** `detect_set_type(title)`

**Returns:** `(set_type: Optional[str], expected_colors: int)`

**Patterns:**

1. **"4 Color Set" or "4-Color Set":** → ("4color", 4)
2. **"Color Set" (generic):** → ("color", 4 if "black" mentioned else 3)
3. **"CMYK Set":** → ("cmyk", 4)
4. **"CMY Set":** → ("cmy", 3)
5. **"Full Set" or "Complete Set":** → ("full", 4)

**Used for:** Validation (does expected_colors match extracted colors?), confidence adjustment

---

## Matching Algorithm

### Overview

Once parsing extracts structured data, match it to product catalog rows (from SQLite `products` table) to get net cost, ASIN, BSR, sellability.

**Two paths:**

1. **Standard matching** (`match_listing()`) - Single item or simple multi-pack
2. **Mixed lot matching** (`build_lot_breakdown()` + `match_lot_listing()`) - Complex lots

---

### Path 1: Standard Matching

**Function:** `match_listing(title, sheet_df)`

**Steps:**

1. **Extract:** model, capacity, pack_size
2. **Filter sheet:** `sheet_df[(model == X) & (capacity == Y) & (pack_size == Z)]`
3. **If pack_size == 1 and one color mentioned:** Filter to that color
4. **If pack_size > 1 and multiple candidates:** Prefer "Color" variant (generic)
5. **Return:** Single row if exactly one match, else None

**Decision Tree:**

```
START
  ├─ Model found? NO → Return None
  │                YES ↓
  ├─ Capacity adjusted? (check suffix)
  │                      ↓
  ├─ Pack size extracted
  │                      ↓
  ├─ Filter sheet: model + capacity + pack_size
  │                      ↓
  ├─ Any candidates? NO → Return None
  │                   YES ↓
  ├─ Single color mentioned? YES → Filter to that color
  │                           NO ↓
  ├─ Multiple candidates? YES → Prefer "Color" variant
  │                        NO ↓
  └─ Return match (ASIN, BSR, net, etc.)
```

**Example Walkthrough:**

**Input:** `"Canon 137 Black Toner Cartridge"`

1. Extract model: "137"
2. Extract capacity: "Standard" (default)
3. Extract pack_size: 1 (default)
4. Filter sheet:
   ```python
   sheet_df[(sheet_df["model"] == "137") &
            (sheet_df["capacity"] == "Standard") &
            (sheet_df["pack_size"] == 1)]
   ```
   Results: [
     {model: "137", capacity: "Standard", pack_size: 1, color: "Black", ASIN: "B00X...", net: 12.50},
     {model: "137", capacity: "Standard", pack_size: 1, color: "Cyan", ASIN: "B00Y...", net: 13.00},
     ...
   ]
5. One color mentioned: "Black"
6. Filter to color: `sheet_df[sheet_df["color"].str.lower() == "black"]`
7. Result: Single row → Return match

**Output:** `{"model": "137", "capacity": "Standard", "pack_size": 1, "color": "Black", "ASIN": "B00X...", "BSR": 15000, "net": 12.50, "sellable": True}`

---

### Path 2: Mixed Lot Matching

**Function:** `build_lot_breakdown(title, sheet_df)` → `match_lot_listing(breakdown, sheet_df, ebay_price, shipping)`

**Steps:**

1. **Parse lot breakdown** (see Rule 4-6 above)
   - Result: `LotBreakdown(model="137", color_quantities={"yellow": 5, "magenta": 3}, lot_multiplier=1, total_units=8, confidence="high")`

2. **For each color in `color_quantities`:**
   - Find sheet row: `(model, capacity, color, pack_size=1)`
   - If found: `ColorMatch(color="yellow", quantity=5, unit_net=12.50, subtotal=62.50, asin="B00X...")`
   - If not found: Mark as unmatched, add to `unmatched_colors`

3. **Calculate "split to singles" net cost:**
   - `total_net_if_split = sum(ColorMatch.subtotal for all matches)`

4. **Find "set alternatives" (multi-packs that could fulfill the lot):**
   - Example: 5 Yellow could be bought as:
     - Option A: 5x "1-pack Yellow" = 5 * $12.50 = $62.50
     - Option B: 1x "4-pack Yellow" + 1x "1-pack Yellow" = $45.00 + $12.50 = $57.50 (cheaper!)
   - Search sheet for: `(model, capacity, color, pack_size > 1)`
   - For each multi-pack, calculate: `sets_needed = ceil(quantity / pack_size)`, `total_net = sets_needed * unit_net`
   - Keep best (lowest net) alternative per color

5. **Calculate "best set net":**
   - `best_set_net = sum(best_alternative_net for all colors)`

6. **Calculate profit:**
   - `profit_if_split = total_net_if_split - (ebay_price + shipping + TARGET_PROFIT)`
   - `profit_if_sets = best_set_net - (ebay_price + shipping + TARGET_PROFIT)`

7. **Return `LotMatchResult`:**
   - Includes: `individual_matches`, `set_alternatives`, `profit_if_split`, `profit_if_sets`, `has_unmatched_colors`, `unmatched_colors`

**Example Walkthrough:**

**Input:** `"5-YELLOW, 3-MAGENTA, 3-CYAN Canon 137"`

eBay price: $50, Shipping: $10, Total: $60

1. **Parse lot breakdown:**
   - Model: "137"
   - Capacity: "Standard"
   - Color quantities: {"yellow": 5, "magenta": 3, "cyan": 3}
   - Lot multiplier: 1
   - Total units: 11
   - Confidence: "high"

2. **Match colors to sheet (pack_size=1):**

| Color | Quantity | Unit Net | Subtotal | ASIN | Match Found? |
|-------|----------|----------|----------|------|--------------|
| Yellow | 5 | $12.50 | $62.50 | B00X1 | Yes |
| Magenta | 3 | $13.00 | $39.00 | B00X2 | Yes |
| Cyan | 3 | $13.00 | $39.00 | B00X3 | Yes |

3. **Total net if split:** $62.50 + $39.00 + $39.00 = **$140.50**

4. **Find set alternatives:**

**Yellow (need 5):**
   - Option A: 5x "1-pack Yellow" = $62.50
   - Option B: 1x "4-pack Yellow" ($45) + 1x "1-pack Yellow" ($12.50) = **$57.50** ← best
   - Option C: 2x "3-pack Yellow" ($36 each) = $72.00

**Magenta (need 3):**
   - Option A: 3x "1-pack Magenta" = $39.00
   - Option B: 1x "3-pack Magenta" = **$36.00** ← best

**Cyan (need 3):**
   - Option A: 3x "1-pack Cyan" = $39.00
   - Option B: 1x "3-pack Cyan" = **$36.00** ← best

5. **Best set net:** $57.50 + $36.00 + $36.00 = **$129.50** (saves $11 vs. singles!)

6. **Calculate profit:**
   - eBay total: $60
   - Target profit: $25
   - Net needed: $60 + $25 = $85
   - **Profit if split:** $140.50 - $85 = **$55.50** ✅ Profitable!
   - **Profit if sets:** $129.50 - $85 = **$44.50** ✅ Still profitable, but less

7. **Telegram message includes:**
   - Main profit: $55.50 (split to singles)
   - Alternative: "Or buy as sets for $129.50 net (profit $44.50)"
   - Color breakdown: "5 Yellow, 3 Magenta, 3 Cyan"

---

## Lot Breakdown Examples

### Example 1: Single Color Lot (High Confidence)

**Input:** `"Lot of 3 Canon 137 Black Toner Cartridges"`

**Parse:**

- Model: "137"
- Capacity: "Standard"
- Lot multiplier: 3 (from "Lot of 3")
- Colors: {"black": 3} (single color + multiplier)
- Total units: 3
- Confidence: "high"

**Match:**

- Sheet row: `{model: "137", capacity: "Standard", color: "Black", pack_size: 1, net: 12.50}`
- Total net: 3 * $12.50 = $37.50

**Profitability:**

- eBay: $20 + $5 shipping = $25
- Net needed: $25 + $25 target = $50
- Actual net: $37.50
- **Profit: $50 - $37.50 = $12.50** ✅

**Confidence: HIGH** (single color, explicit lot size)

---

### Example 2: Explicit Multi-Color Quantities (High Confidence)

**Input:** `"(2) Yellow (1) Cyan Canon 045H High Yield"`

**Parse:**

- Model: "045H"
- Capacity: "High" (from "High Yield" + model suffix)
- Lot multiplier: 1
- Colors: {"yellow": 2, "cyan": 1} (from "(2) Yellow" and "(1) Cyan")
- Total units: 3
- Confidence: "high"

**Match:**

- Yellow: 2 * $15.00 = $30.00
- Cyan: 1 * $16.00 = $16.00
- Total net: $46.00

**Profitability:**

- eBay: $25 + $8 shipping = $33
- Net needed: $33 + $25 target = $58
- Actual net: $46.00
- **Profit: $58 - $46 = $12** ✅

**Confidence: HIGH** (explicit per-color quantities)

---

### Example 3: Set with Matching Count (High Confidence)

**Input:** `"Canon 045 Set Of 3 (Cyan, Magenta, Yellow)"`

**Parse:**

- Model: "045"
- Capacity: "Standard"
- Lot multiplier: 3 (from "Set of 3")
- Colors: {"cyan": 1, "magenta": 1, "yellow": 1} (from parenthesis enumeration)
- Set type: "3color"
- Expected set size: 3 (matches actual)
- Total units: 3
- Confidence: "high"

**Match:**

- Cyan: 1 * $13.00 = $13.00
- Magenta: 1 * $13.00 = $13.00
- Yellow: 1 * $13.00 = $13.00
- Total net: $39.00

**Alternative (3-pack):**

- If sheet has "3-pack Color" variant: $36.00 (saves $3)

**Profitability:**

- eBay: $22 + $6 shipping = $28
- Net needed: $28 + $25 target = $53
- Actual net (singles): $39.00
- Actual net (3-pack): $36.00
- **Profit: $53 - $36 = $17** ✅

**Confidence: HIGH** (set size matches color count)

---

### Example 4: CMYK Literal (High Confidence)

**Input:** `"Canon GPR-51 CMYK Full Set"`

**Parse:**

- Model: "GPR-51"
- Capacity: "Standard"
- Colors: {"cyan": 1, "magenta": 1, "yellow": 1, "black": 1} (from "CMYK")
- Set type: "cmyk"
- Total units: 4
- Confidence: "high"

**Match:**

- Black: $14.00
- Cyan: $15.00
- Magenta: $15.00
- Yellow: $15.00
- Total net (singles): $59.00

**Alternative (4-pack):**

- If sheet has "4 Color" variant: $52.00 (saves $7)

**Confidence: HIGH** (CMYK is explicit, well-understood abbreviation)

---

### Example 5: Ambiguous Lot Multiplier (Low Confidence)

**Input:** `"(9) Canon GPR-51 CMYK Toner Cartridges"`

**Parse:**

- Model: "GPR-51"
- Capacity: "Standard"
- Lot multiplier: 9 (from "(9)")
- Colors: {"cyan": 1, "magenta": 1, "yellow": 1, "black": 1} (from "CMYK")
- Total units: 9 (not 36! We can't determine if it's "9 total" or "9 of each")
- Confidence: "low"
- Notes: "Cannot determine per-color quantities for lot of 9 with 4 colors"

**Interpretation Issue:**

- Does "(9) CMYK" mean:
  - **Option A:** 9 total cartridges distributed across 4 colors (but how? 3+2+2+2? 2+2+2+3?)
  - **Option B:** 9 of each color (36 total)

**Conservative Approach:**

- Mark as **low confidence**
- Send Telegram notification (could still be profitable!)
- **DO NOT expand to `purchased_units` table** (would skew per-color inventory analytics)
- Message includes: "⚠️ Lot breakdown uncertain - verify per-color quantities before purchase"

**Match:**

- If we assume 1 of each color (conservative):
  - Total net (singles): $59.00
  - eBay: $45 + $10 shipping = $55
  - Net needed: $55 + $25 = $80
  - Actual net: $59.00
  - **Profit: $80 - $59 = $21** ✅ (but uncertain!)

- If actual distribution is "9 of each" (36 total):
  - Total net: 36 * $14.75 avg = $531
  - eBay: $55 total
  - **Massive profit!** (but unlikely)

**Action:** Send notification with warning, let human verify before purchasing.

**Confidence: LOW** (ambiguous, requires manual validation)

---

### Example 6: Lot Without Color Info (Low Confidence)

**Input:** `"Lot of 16 Canon GPR-31 Toner Set"`

**Parse:**

- Model: "GPR-31"
- Capacity: "Standard"
- Lot multiplier: 16 (from "Lot of 16")
- Colors: {} (none identified)
- Total units: 16
- Confidence: "low"
- Notes: "Lot detected but colors not identified"

**Interpretation Issue:**

- We know there are 16 cartridges, but not which colors
- Could be: 4 Black + 4 Cyan + 4 Magenta + 4 Yellow (balanced)
- Could be: 16 Black (single color)
- Could be: Random mix

**Conservative Approach:**

- Mark as **low confidence**
- Cannot match to sheet (no color info)
- **Skip Telegram notification** (no profitability calc possible)

**Action:** Log as "unmatched" in debug logs, move on.

**Confidence: LOW** (insufficient data)

---

### Example 7: Nested Multipliers (Medium Confidence)

**Input:** `"2 Sets of 4 Canon 137 CMYK"`

**Parse:**

- Model: "137"
- Capacity: "Standard"
- Lot multiplier: 2 (from "2 Sets")
- Colors: {"cyan": 1, "magenta": 1, "yellow": 1, "black": 1} (from "CMYK")
- Pack size: 4 (from "4 Canon 137" or CMYK = 4 colors)
- Total units: 2 * 4 = 8
- Confidence: "high" (numbers match: 2 sets of 4 colors)

**Match:**

- Apply multiplier to each color:
  - Black: 2 * $12.50 = $25.00
  - Cyan: 2 * $13.00 = $26.00
  - Magenta: 2 * $13.00 = $26.00
  - Yellow: 2 * $13.00 = $26.00
- Total net (singles): $103.00

**Alternative (4-packs):**

- If sheet has "4 Color Set": 2 * $48.00 = $96.00 (saves $7)

**Profitability:**

- eBay: $60 + $12 shipping = $72
- Net needed: $72 + $25 = $97
- Actual net (4-packs): $96.00
- **Profit: $97 - $96 = $1** ❌ (marginal, skip)

**Confidence: HIGH** (explicit "2 Sets", clear CMYK)

---

### Example 8: Single-Letter Color Abbreviations (High Confidence)

**Input:** `"Canon 055 C/M/Y/K Set"`

**Parse:**

- Model: "055"
- Colors: {"cyan": 1, "magenta": 1, "yellow": 1, "black": 1} (from "C/M/Y/K")
- Set type: "cmyk"
- Total units: 4
- Confidence: "high"

**Match:** (same as Example 4, different notation)

**Confidence: HIGH** (C/M/Y/K is standard abbreviation)

---

### Example 9: Model Number Confusion (Avoided)

**Input:** `"Canon 045 Yellow Toner"`

**Parse:**

- Model: "045" (not 45, not "0-Yellow")
- Capacity: "Standard"
- Colors: {"yellow": 1}
- Total units: 1
- Confidence: "high"

**Edge Case Avoided:**

- Regex `(\d+)-Yellow` would match "045-Yellow" if hyphen present
- But "045" is identified as model number via `model_numbers` set
- Exclusion logic prevents treating "045" as quantity

**Confidence: HIGH** (correct model extraction)

---

### Example 10: Multiple Patterns in One Title (High Confidence)

**Input:** `"5-YELLOW, 3-MAGENTA, 3-CYAN Canon 137 High Yield (11 Total)"`

**Parse:**

- Model: "137"
- Capacity: "High" (from "High Yield")
- Colors: {"yellow": 5, "magenta": 3, "cyan": 3} (from explicit quantities)
- Total units: 11 (validated by "(11 Total)")
- Confidence: "high"

**Validation:**

- Sum of colors: 5 + 3 + 3 = 11 ✅ Matches "(11 Total)"
- Increases confidence (internal consistency check)

**Match:** (same as mixed lot example above)

**Confidence: HIGH** (explicit quantities, validated by total)

---

## How to Modify and Extend

### Adding New Model Patterns

**Location:** `engine/canon.py::extract_model()`

**Steps:**

1. Identify new model format (e.g., "IR-1024" for new Canon line)
2. Add regex pattern to model extraction:

```python
# After existing CRG/GPR checks:
ir_match = re.search(r'\bIR[- ]?(\d{4})\b', title, re.IGNORECASE)
if ir_match:
    model = ir_match.group(1)
    # Check for color suffix if needed
    return model, suffix_color_or_none
```

3. Update Google Sheet with new model entries
4. Test with sample titles:

```python
from engine.canon import extract_model, load_canon_sheet
df = load_canon_sheet().df
models = df["model"].dropna().unique().tolist()

test_titles = [
    "Canon IR-1024 Black Toner",
    "IR1024C Cyan Cartridge",
    "Canon IR 1024 Yellow",
]

for title in test_titles:
    model, color = extract_model(title, models)
    print(f"{title} -> Model: {model}, Color: {color}")
```

5. Deploy and monitor for false positives

---

### Adding New Color Abbreviations

**Location:** `engine/canon.py::extract_color_quantities()`

**Example:** Support "Blk" as Black abbreviation

**Steps:**

1. Add to color normalization at start of function:

```python
# After existing "blue" -> "cyan" normalization:
title_lower = re.sub(r'\bblk\b', 'black', title_lower)
```

2. No other changes needed (all patterns work with "black" keyword)

3. Test:

```python
from engine.canon import extract_color_quantities
test_titles = [
    "2-Blk, 1-Cyan",
    "(3) Blk Canon 137",
]
for title in test_titles:
    colors, conf, notes = extract_color_quantities(title)
    print(f"{title} -> {colors}, confidence={conf}")
```

---

### Adding New Pack Size Patterns

**Location:** `engine/canon.py::extract_pack_size()`

**Example:** Support "Trio Pack" (3-pack)

**Steps:**

1. Add pattern before fallback:

```python
# After "Triple Pack" check:
if re.search(r"\btrio pack\b", title, re.IGNORECASE):
    return 3 if 3 in valid_pack_sizes else 1
```

2. Test with real titles:

```python
from engine.canon import extract_pack_size
valid = [1, 2, 3, 4]
test_titles = [
    "Canon 137 Trio Pack Black",
    "Trio Pack CMYK",
]
for title in test_titles:
    size = extract_pack_size(title, valid)
    print(f"{title} -> Pack size: {size}")
```

3. Monitor for conflicts (ensure "Trio" doesn't appear in model names)

---

### Adjusting Confidence Thresholds

**Location:** `engine/canon.py::build_lot_breakdown()`

**Example:** Lower confidence for "xX" notation (e.g., "x3")

**Steps:**

1. Find the `extract_lot_multiplier()` function
2. Change return value:

```python
# Before:
m = re.search(r'\bx(\d+)\b', title, re.IGNORECASE)
if m and 2 <= int(m.group(1)) <= 10:
    return int(m.group(1)), "medium"

# After:
    return int(m.group(1)), "low"  # Changed from "medium" to "low"
```

3. Rebuild Docker container or restart app
4. Monitor Telegram notifications for "⚠️ Low confidence" warnings
5. Adjust back if too many false negatives

---

### Extending to New Brands (e.g., HP)

**High-level steps:**

1. Create `engine/hp.py` (copy `xerox.py` as template)
2. Update eBay API query:
   - `aspect_filter=categoryId:16204,Brand:{{HP}}`
3. Adjust title parsing:
   - HP model format: "HP 26A", "CF226A", "LaserJet 4000"
   - Extract model from HP-specific patterns
4. Create Google Sheet for HP products
5. Add `hp_excluded_keywords` table to exclusions DB
6. Add HP thread to `main.py::monitor()`:

```python
hp_thread = threading.Thread(target=_run_hp_job, args=(token, hp_lookup_data, refresh_event))
hp_thread.start()
threads.append(hp_thread)
```

7. Register Telegram commands: `/add_hp_keyword`, `/list_hp_keywords`

---

### Debugging Title Parsing Issues

**Tools:**

1. **Interactive testing:**

```python
from engine.canon import build_lot_breakdown, load_canon_sheet
df = load_canon_sheet().df

problem_title = "(9) Canon GPR-51 CMYK"
breakdown = build_lot_breakdown(problem_title, df)

print("Model:", breakdown.model)
print("Colors:", breakdown.color_quantities)
print("Total units:", breakdown.total_units)
print("Confidence:", breakdown.confidence)
print("Notes:", breakdown.confidence_notes)
```

2. **Add debug logging:**

```python
# In extract_color_quantities():
print(f"DEBUG: Extracted colors from '{title}': {colors}")
print(f"DEBUG: Confidence: {confidence}, Notes: {notes}")
```

3. **Check database for edge cases:**

```bash
sqlite3 database.db "
SELECT title, lot_breakdown
FROM matches
WHERE json_extract(lot_breakdown, '$.confidence') = 'low'
ORDER BY id DESC
LIMIT 10;
"
```

4. **Log all unparsed titles:**

Add to `engine/canon.py::canon()`:

```python
if not lot_breakdown.model:
    with open("unparsed_titles.log", "a") as f:
        f.write(f"{datetime.now()}: {title}\n")
```

---

## Manual Validation Checklist

Since automated tests have been removed, use this checklist to validate title parsing changes:

### Basic Parsing Tests

- [ ] **Single color, single unit:** `"Canon 137 Black"` → 1 Black
- [ ] **Single color, lot:** `"Lot of 3 Canon 137 Black"` → 3 Black
- [ ] **CMYK set:** `"Canon 045 CMYK Set"` → 1 each (4 total)
- [ ] **Explicit quantities:** `"5-YELLOW, 3-MAGENTA"` → 5 Yellow, 3 Magenta
- [ ] **Pack size:** `"Canon 137 4 Color Set"` → 4-pack, 1 each color

### Edge Case Tests

- [ ] **CRG format:** `"CRG-137BK"` → Model 137, Black
- [ ] **Model number not quantity:** `"Canon 045 Yellow"` → Model 045 (not 0 and 45)
- [ ] **Nested multipliers:** `"2 Sets of 4 CMYK"` → 8 total (2x4)
- [ ] **Ambiguous lot:** `"(9) GPR-51 CMYK"` → Low confidence, 9 total (not 36)
- [ ] **No colors:** `"Lot of 10 Canon Toner"` → Low confidence, skip match

### Regression Tests

After modifying parsing code, test against these historical problematic titles:

1. `"(2) Yellow (1) Cyan Canon 045H"` → Should parse as 2 Yellow + 1 Cyan (not 2+1=3 of mixed)
2. `"5 X CANON GPR-55 Cyan Magenta Yellow"` → Low confidence (ambiguous multiplier)
3. `"Canon 137 Black/Cyan/Magenta/Yellow"` → 1 each (4 total), high confidence
4. `"TWO Canon 137 Sets Black Cyan Magenta Yellow"` → 2x4=8, high confidence
5. `"-1 Cyan -5 Black 1 magenta Canon 055"` → 1 Cyan, 5 Black, 1 Magenta (hyphen prefix handled)

### Profitability Validation

- [ ] Verify profit calculation: `net_cost - (ebay_price + shipping + TARGET_PROFIT) >= 0`
- [ ] Check set alternatives: Multi-packs should be cheaper than sum of singles
- [ ] Confirm "sellable" filter: Only "Sellable" rows from sheet are matched

### Telegram Notification Validation

- [ ] Message includes: Title (truncated 100 chars), Link, Price, Shipping, Profit
- [ ] Images attached (up to 5)
- [ ] For mixed lots: Color breakdown visible
- [ ] For low confidence: "⚠️" warning emoji present
- [ ] Set alternatives listed (if applicable)

### Database Validation

```bash
# Check seen_ids is being populated
sqlite3 database.db "SELECT COUNT(*) FROM seen_ids WHERE seen_ts > strftime('%s', 'now', '-1 hour')"
# Should be >0 if monitor ran in last hour

# Check matches have lot_breakdown JSON
sqlite3 database.db "SELECT lot_breakdown FROM matches WHERE is_mixed_lot = 1 LIMIT 5"
# Should show valid JSON objects

# Verify purchased_units expansion (after order history fetch)
sqlite3 database.db "SELECT unit_color, SUM(unit_quantity) FROM purchased_units GROUP BY unit_color"
# Should show color distribution (no colors should be missing)
```

---

**End of CANON_TITLE_PARSING.md**
