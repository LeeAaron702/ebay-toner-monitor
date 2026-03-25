# eBay Toner Arbitrage Monitor

Multi-threaded eBay monitor for Canon/Xerox/Lexmark toner. Matches listings against SQLite catalog, sends profitable opportunities to Telegram.

## Architecture (Two-Process Model)

```
uvicorn (api/api_server.py)          main.py (subprocess)
├── Telegram webhook                 ├── Canon/Xerox/Lexmark threads (every 60s)
├── Admin panel /admin/*             ├── Order history sync (hourly)
├── Products API /api/v1/products    ├── Stats reports (9am/12pm/5pm PST)
└── Exclude API /exclude/*           └── Analyzer job (7am PST daily)
                    │                                │
                    └──────── database.db ───────────┘
```

**Key insight:** FastAPI spawns `main.py` as subprocess on startup. They share `database.db` but each thread uses its own connection.

## Commands

```bash
# Local dev
source .venv/bin/activate && uvicorn api.api_server:app --reload --port 8000

# Docker
docker compose up -d --build && docker compose logs -f

# Manual backfill (match orders to products)
python backfill_matches.py

# Test single engine with token
python engine/canon.py --token "YOUR_EBAY_TOKEN"
```

## Engine Differences (Critical for Matching)

Each engine expects specific DataFrame columns from `db/products_db.py`:

| Engine | Match By | Key Columns | Example Pattern |
|--------|----------|-------------|-----------------|
| Canon | model + capacity + color + pack_size | `ASIN`, `BSR`, `net`, `variant` | "046H", "055H" |
| Xerox | part_number | `net`, `sku`, `sellable` | "006R01512" |
| Lexmark | part_number | `model_family`, `part_number_lower` | "500G", "50F1H00" |

**Canon is special:** Uses `LotBreakdown` dataclass with `confidence` scoring for mixed lots like "5-YELLOW, 3-MAGENTA". Always preserve `confidence` and `confidence_notes` fields.

## Database Patterns

- **No ORM** — Raw `sqlite3` with `conn.row_factory = sqlite3.Row`
- **Two `get_db_connection()` functions** — one in `db/listings_db.py`, one in `db/products_db.py` (both connect to same DB)
- **Thread safety** — Never share connections across threads
- **`database.db` is git-tracked** (intentional for syncing catalog)

```python
from db.listings_db import get_db_connection
conn = get_db_connection()
try:
    cursor = conn.execute("SELECT * FROM products WHERE brand = ?", ("Canon",))
finally:
    conn.close()
```

## Profit Calculation

```python
effective_net = net_cost - (amazon_price × overhead_pct / 100)
profit = effective_net - (ebay_price + shipping)
```

`overhead_pct` (default 15%) and `target_profit` (default $25) are in `settings` table, editable via Admin Panel.

## Conventions

1. **Logging:** Define `_log()` locally: `def _log(msg): print(f"LOG - {__file__} - {msg}")`
2. **Timezone:** Always use `America/Los_Angeles`, import `LOCAL_TZ` from `engine/canon.py`
3. **Telegram in engines:** Use direct HTTP requests (not PTB library) since engines run in subprocess
4. **Error handling:** Log and continue — never crash the monitor loop

## Gotchas

- **`utils/base.py` is gitignored** — contains hardcoded credentials for manual testing
- **`net_cost` and `bsr` only updated by analyzer pipeline**, not admin UI
- **eBay tokens expire ~2 hours** — `refresh_event` (threading.Event) triggers cross-thread refresh
- **Admin auth:** `ADMIN_USERNAME`/`ADMIN_PASSWORD` env vars (default: admin/changeme)
- **`scripts/` folder:** One-off DB maintenance scripts, not part of runtime
- **`group_key` differs by brand** — Canon: model+capacity, Lexmark: model only, Xerox: two block types

## Environment Variables

Required: `EBAY_APP_ID`, `EBAY_CLIENT_SECRET`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `TELEGRAM_WEBHOOK_URL`, `TELEGRAM_WEBHOOK_SECRET`

Optional: `RUN_INTERVAL` (60), `DB_PATH`, `ADMIN_USERNAME`/`ADMIN_PASSWORD`, `{BRAND}_MIN_NET_COST` (50), `ANALYZER_USERNAME`/`ANALYZER_PASSWORD`