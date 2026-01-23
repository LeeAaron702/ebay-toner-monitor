# eBay Toner Arbitrage Monitor - AI Agent Instructions

## Overview

Multi-threaded eBay listing monitor for Canon, Xerox, and Lexmark toner cartridges. Matches listings against product catalog in SQLite, sends profitable opportunities to Telegram.

**Tech Stack:** Python 3.13, FastAPI, SQLite, python-telegram-bot (webhook mode), Docker

## Architecture

```
FastAPI Server (api/api_server.py)
  ├── Telegram webhook receiver
  ├── Products API (/api/v1/products)
  └── Exclude API (/exclude/*)
        │
        └── spawns subprocess
              │
              ▼
Monitor Loop (main.py) - runs every 60s
  ├── Canon Thread  → engine/canon.py
  ├── Xerox Thread  → engine/xerox.py
  ├── Lexmark Thread → engine/lexmark.py
  └── Order History Thread (hourly)
              │
              ▼
SQLite Database (database.db)
  ├── products      - Product catalog (471 items)
  ├── seen_ids      - Deduplication (72-hour window)
  ├── messages      - Raw listing data
  ├── matches       - Profitable matches
  ├── excluded_*    - Seller/keyword blacklists
  └── order_history - Purchase tracking
```

## Key Files

| File | Purpose |
|------|---------|
| `main.py` | Monitor orchestrator, spawns engine threads |
| `engine/canon.py` | Canon lot parser (~1800 lines), handles "5-YELLOW, 3-MAGENTA" |
| `engine/xerox.py` | Xerox SKU matcher, extracts "006R01512" patterns |
| `engine/lexmark.py` | Lexmark part number matcher |
| `db/products_db.py` | Product CRUD, `get_*_products()` helpers |
| `db/listings_db.py` | Listings, matches, seen_ids tables |
| `db/exclusions_db.py` | Seller/keyword exclusion tables |
| `api/api_server.py` | FastAPI entrypoint, Telegram webhook |
| `utils/telegram_service.py` | Bot command handlers |

## Database Access

- **No ORM** — Raw `sqlite3` with context managers
- **Thread safety** — Each thread gets own connection via `get_db_connection()`
- **Garbage collection** — `gc_old_ids()` trims seen_ids older than 72 hours

```python
from db.listings_db import get_db_connection

conn = get_db_connection()
try:
    cursor = conn.execute("SELECT * FROM products WHERE brand = ?", ("Canon",))
    rows = cursor.fetchall()
finally:
    conn.close()
```

## Engine Pattern

All engines follow this flow:
1. `search_listings(token)` → eBay Browse API
2. Filter by seen_ids, excluded sellers/keywords
3. Parse title → extract model, capacity, color, quantity
4. Match to products table → get net_cost, ASIN
5. Calculate profit → `net_cost - (price + shipping)`
6. Send to Telegram if profitable

## Lot Parsing (Canon)

Canon engine uses structured lot decomposition:

```python
LotBreakdown(
    model="046",
    color_quantities={"cyan": 2, "magenta": 2, "yellow": 2, "black": 2},
    lot_multiplier=2,
    total_units=8,
    is_mixed_lot=True,
    is_color_set=True,
    confidence="high",
    confidence_notes=["Explicit color quantities found"]
)
```

**Important:** Preserve `confidence` and `confidence_notes` when modifying lot logic.

## Products Table

```sql
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    brand TEXT NOT NULL,        -- 'Canon', 'Xerox', 'Lexmark'
    model TEXT,                 -- Printer model (Canon)
    capacity TEXT,              -- 'Standard', 'High', 'Super High'
    part_number TEXT,           -- OEM part number (Xerox, Lexmark)
    variant_label TEXT,         -- Display name
    color TEXT,                 -- 'Black', 'Cyan', etc.
    pack_size INTEGER DEFAULT 1,
    asin TEXT,                  -- Amazon ASIN
    amazon_sku TEXT,            -- Internal SKU
    net_cost REAL,              -- Target purchase price
    bsr INTEGER,                -- Best Seller Rank
    sellable INTEGER DEFAULT 1,
    group_key TEXT,             -- For block grouping
    UNIQUE(brand, asin)
);
```

## Environment Variables

Required:
- `EBAY_APP_ID`, `EBAY_CLIENT_SECRET` — eBay OAuth
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` — Telegram bot
- `TELEGRAM_WEBHOOK_URL`, `TELEGRAM_WEBHOOK_SECRET` — Webhook mode

Optional:
- `RUN_INTERVAL` (default: 60) — Polling interval seconds
- `DB_PATH` (default: database.db) — SQLite path
- `EXCLUDE_API_KEY` — Auth for /exclude/* endpoints
- `OLLAMA_BASE_URL` — AI description parsing

## Common Commands

```bash
# Run locally
uvicorn api.api_server:app --host 0.0.0.0 --port 8000 --reload

# Docker
docker-compose up --build

# Run tests
python -m pytest tests/ -v

# Check product counts
python -c "from db.products_db import get_canon_products; print(len(get_canon_products()))"
```

## Conventions

1. **Logging:** Use `from utils.base import _log` for timestamped output
2. **Imports:** Standard library → Third-party → Local
3. **Error handling:** Log and continue, don't crash the loop
4. **Telegram:** Engines use direct HTTP (not PTB) since they run in subprocess

## Gotchas

1. **Token expiration:** eBay OAuth tokens last ~2 hours. `refresh_event` triggers cross-thread refresh.
2. **Database connections:** Never share connections across threads.
3. **Timezone:** All timestamps use `America/Los_Angeles` (PST/PDT).
4. **Mixed lots:** Test regex changes against real titles in `seen_ids` table.
