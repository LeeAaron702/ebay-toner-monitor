# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

eBay Toner Arbitrage Monitor — monitors eBay for Canon, Xerox, and Lexmark toner cartridge listings, matches them against a product catalog in SQLite, calculates profit using Amazon resale values, and sends alerts to Telegram.

## Commands

```bash
# Run locally (starts FastAPI + spawns monitor subprocess)
source .venv/bin/activate
uvicorn api.api_server:app --reload --port 8000

# Docker
docker compose up -d --build
docker compose logs -f

# Tests (no tests/ directory yet — create as needed)
pytest tests/ -v

# Install dependencies
pip install -r requirements.txt

# Manual backfill (match orders to products)
python backfill_matches.py

# Manual analyzer run (scrape Amazon pricing)
python -c "from utils.analyzer_job import run_analyzer_job; print(run_analyzer_job())"
```

## Architecture

**Two-process model:** `uvicorn` runs FastAPI (`api/api_server.py`), which spawns `main.py` as a subprocess on startup. They share `database.db`.

**FastAPI Server** (`api/api_server.py`):
- Telegram webhook receiver at `/telegram/webhook`
- Admin panel at `/admin/*` (Jinja2 + HTMX, session cookie auth)
- Products REST API at `/api/v1/products`
- Exclusions API at `/exclude/*`
- Root `/` redirects to `/admin/`

**Monitor Loop** (`main.py`):
- Runs Canon/Xerox/Lexmark engines in parallel threads every 60s
- Order history sync hourly
- Stats reports at 9am, 12pm, 5pm PST
- Analyzer job (Amazon scrape via Playwright) at 7am PST daily
- Blocks engines on startup until first analyzer run completes

**Engine pipeline** (each brand follows this flow):
1. Search eBay Browse API (200 listings)
2. Filter by seen_ids, excluded sellers/keywords
3. Parse title (regex/NLP) → extract model, capacity, color, quantity
4. Match to products table → get net_cost, ASIN
5. Calculate profit: `effective_net - (price + shipping)` where `effective_net = net_cost - (amazon_price × overhead_pct / 100)`
6. Send Telegram alert if profitable

**Key differences between engines:**
- Canon (`engine/canon.py`, ~1900 LOC): Complex lot decomposition ("5-YELLOW, 3-MAGENTA"), uses `LotBreakdown` dataclass with confidence scoring. Matches by model + capacity + color + pack_size.
- Xerox (`engine/xerox.py`): SKU regex extraction ("006R01512"). Matches by part_number.
- Lexmark (`engine/lexmark.py`): Part number extraction in various formats. Matches by part_number.

## Database

SQLite at `database.db`. No ORM — raw `sqlite3` with context managers. Thread safety via per-thread connections from `get_db_connection()`.

**Two `get_db_connection()` functions exist** — one in `db/listings_db.py` (used by engines, admin queries) and one in `db/products_db.py` (used by product CRUD). Both connect to the same `database.db`.

Key tables: `products` (471 SKUs, unique on `(brand, asin)`), `seen_ids` (72-hour dedup window), `messages`, `matches`, `excluded_sellers`, `canon_excluded_keywords`, `xerox_excluded_keywords`, `lexmark_excluded_keywords`, `order_history`, `purchased_units`, `settings`.

`database.db` is **tracked in git** (unusual — the gitignore only excludes journal files). This is intentional for syncing product catalog across machines.

The `settings` table stores `overhead_pct` (default 15%) and `target_profit` (default $25).

## Engine Column Conventions

Each engine expects different DataFrame column names from `db/products_db.py`:
- Canon: `ASIN`, `BSR`, `net`, `variant` (renamed from `asin`, `bsr`, `net_cost`, `variant_label`)
- Xerox: `net`, `sku` (renamed from `net_cost`, `amazon_sku`), `sellable` as bool
- Lexmark: `model_family` (renamed from `model`), adds `part_number_lower`

## Important Conventions

- **Logging:** Each file defines `_log()` locally for consistent prefixed output
- **Timezone:** All timestamps use `America/Los_Angeles` (PST/PDT), imported as `LOCAL_TZ` from `engine/canon.py`
- **Telegram:** Engines use direct HTTP requests (not PTB library) since they run in the subprocess
- **Error handling:** Log and continue — never crash the monitor loop
- **Token refresh:** eBay OAuth tokens expire ~2 hours; `refresh_event` (threading.Event) signals cross-thread refresh
- **Imports:** Standard library → Third-party → Local
- **Sensitive files in .gitignore:** `.env`, `ebay_tokens_*.json`, `utils/base.py` (contains hardcoded credentials)

## Gotchas

- `utils/base.py` is gitignored — it contains hardcoded eBay credentials for manual testing. Do not commit.
- `net_cost` and `bsr` on products are **only updated by the analyzer pipeline**, not by admin UI edits.
- Canon's `LotBreakdown` has `confidence` and `confidence_notes` fields — preserve these when modifying lot logic.
- Database connections must **never be shared across threads**.
- The `group_key` generation differs by brand (see `generate_group_key()` in `db/products_db.py`): Canon groups by model+capacity, Lexmark by model only, Xerox has two block types (part number blocks vs model blocks).
- Mixed lots in Canon should be tested against real titles from the `seen_ids` table.
- Admin panel auth uses `ADMIN_USERNAME`/`ADMIN_PASSWORD` env vars (default: admin/changeme).
- `scripts/` contains one-off DB maintenance scripts (audit, migration, sync). Not part of the runtime — run manually as needed.
- Each engine can be run standalone with `--token` flag for testing (see `argparse` at bottom of each engine file).

## Environment Variables

Required in `.env`:
- `EBAY_APP_ID`, `EBAY_CLIENT_SECRET` — eBay API credentials
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` — Telegram alerts

Optional:
- `TELEGRAM_WEBHOOK_URL`, `TELEGRAM_WEBHOOK_SECRET` — for webhook mode
- `EXCLUDE_API_KEY` — protects `/exclude/*` endpoints
- `ADMIN_USERNAME`, `ADMIN_PASSWORD` — admin panel login (default: admin/changeme)
- `SECRET_KEY` — session signing (auto-generated if not set)
- `ADMIN_PANEL_URL` — linked in Telegram messages for quick admin access
- `ZIP_CODE` — eBay location filter (default: 93012)
- `RUN_INTERVAL` — seconds between engine runs (default: 60)
- `DB_PATH` — override database location (default: `database.db` in repo root)
- `ANALYZER_USERNAME`, `ANALYZER_PASSWORD` — analyzer.tools login for Amazon scraping
- `ANALYZER_DOWNLOAD_PATH` — where Playwright downloads analyzer Excel files
- `{BRAND}_MIN_NET_COST` — minimum net cost filter per engine (e.g., `LEXMARK_MIN_NET_COST`, default: 50)
