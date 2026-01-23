# Architecture

This document describes the system architecture, data flow, and key design decisions.

## System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                       FastAPI Server                            │
│                     (api/api_server.py)                         │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │ Telegram Webhook │  │  Products API   │  │   Exclude API   │  │
│  │ /telegram/webhook│  │ /api/v1/products│  │    /exclude/*   │  │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘  │
└───────────┼─────────────────────┼─────────────────────┼─────────┘
            │                     │                     │
            │         spawns subprocess on startup      │
            ▼                     ▼                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Monitor Loop (main.py)                       │
│                    Runs every 60 seconds                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌───────────────┐   │
│  │  Canon   │  │  Xerox   │  │ Lexmark  │  │ Order History │   │
│  │  Thread  │  │  Thread  │  │  Thread  │  │    Thread     │   │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └───────┬───────┘   │
└───────┼─────────────┼─────────────┼────────────────┼───────────┘
        │             │             │                │
        └─────────────┴─────────────┴────────────────┘
                              │
                              ▼
                 ┌────────────────────────┐
                 │    SQLite Database     │
                 │    (database.db)       │
                 │  ┌──────────────────┐  │
                 │  │    products      │  │  ◄── Product catalog
                 │  │    seen_ids      │  │  ◄── Processed listings
                 │  │    messages      │  │  ◄── Raw listing data
                 │  │    matches       │  │  ◄── Profitable matches
                 │  │  excluded_sellers│  │  ◄── Seller blacklist
                 │  │  *_keywords      │  │  ◄── Keyword filters
                 │  │  order_history   │  │  ◄── Purchase records
                 │  └──────────────────┘  │
                 └────────────────────────┘

External Services:
  • eBay Browse API ──► Search listings, get item details
  • eBay Trading API ──► Fetch purchase history (XML)
  • Telegram Bot API ──► Send notifications, receive commands
  • Ollama (optional) ──► AI-powered description parsing
```

## Component Details

### FastAPI Server (`api/api_server.py`)

The main entry point that:
1. Initializes the Telegram bot in webhook mode
2. Spawns `main.py` as a subprocess for the monitoring loop
3. Mounts API routers for products and exclusions
4. Handles health checks at `/healthz`

### Monitor Loop (`main.py`)

Orchestrates the scraping engines:
1. Loads product catalogs from SQLite on each iteration
2. Spawns parallel threads for Canon, Xerox, and Lexmark
3. Manages OAuth token refresh (tokens expire every ~2 hours)
4. Runs order history sync hourly
5. Triggers scheduled stats reports (9am, 12pm, 5pm PST)

### Engine Architecture

Each engine follows the same pattern:

```
┌─────────────────────────────────────────────────────────────┐
│                        Engine Flow                          │
├─────────────────────────────────────────────────────────────┤
│  1. Search eBay ──► Browse API returns 200 listings         │
│  2. Filter ──► Remove seen IDs, excluded sellers/keywords   │
│  3. Parse ──► Extract model, capacity, color, quantity      │
│  4. Match ──► Find product in catalog, get net_cost         │
│  5. Calculate ──► profit = net_cost - (price + shipping)    │
│  6. Notify ──► Send to Telegram if profit > threshold       │
│  7. Store ──► Save match to database                        │
└─────────────────────────────────────────────────────────────┘
```

**Canon Engine** (`engine/canon.py`)
- Parses complex lot titles ("5-YELLOW, 3-MAGENTA")
- Uses `LotBreakdown` dataclass for structured decomposition
- Matches by model + capacity + color + pack_size

**Xerox Engine** (`engine/xerox.py`)
- Uses regex to extract SKUs like "006R01512"
- Matches by part_number against product catalog

**Lexmark Engine** (`engine/lexmark.py`)
- Extracts part numbers in various formats (500G, C540H1KG, 50F1H00)
- Matches by part_number against product catalog

### Database Schema

```sql
-- Product catalog (source of truth)
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    brand TEXT NOT NULL,           -- 'Canon', 'Xerox', 'Lexmark'
    model TEXT,                    -- Printer model
    capacity TEXT,                 -- 'Standard', 'High', 'Super High'
    part_number TEXT,              -- OEM part number
    variant_label TEXT,            -- Display name
    color TEXT,                    -- 'Black', 'Cyan', etc.
    pack_size INTEGER DEFAULT 1,
    asin TEXT,                     -- Amazon ASIN
    amazon_sku TEXT,               -- Internal SKU
    net_cost REAL,                 -- Target purchase price
    bsr INTEGER,                   -- Best Seller Rank
    sellable INTEGER DEFAULT 1,
    is_model_block INTEGER,        -- For Canon grouping
    group_key TEXT,                -- Block grouping key
    UNIQUE(brand, asin)
);

-- Deduplication tracking (72-hour window)
CREATE TABLE seen_ids (
    item_id TEXT PRIMARY KEY,
    seen_ts INTEGER NOT NULL       -- Unix timestamp
);

-- Profitable matches
CREATE TABLE matches (
    id INTEGER PRIMARY KEY,
    message_id INTEGER,            -- FK to messages
    title TEXT,
    asin TEXT,
    net_cost REAL,
    profit REAL,
    total_units INTEGER,
    is_mixed_lot INTEGER,
    lot_breakdown TEXT,            -- JSON
    timestamp INTEGER
);

-- Exclusion lists
CREATE TABLE excluded_sellers (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL      -- Case-insensitive
);

CREATE TABLE canon_excluded_keywords (
    id INTEGER PRIMARY KEY,
    keyword TEXT UNIQUE NOT NULL
);

CREATE TABLE xerox_excluded_keywords (
    id INTEGER PRIMARY KEY,
    keyword TEXT UNIQUE NOT NULL
);
```

## Data Flow

### Listing Processing

```
eBay Browse API
      │
      ▼
┌─────────────────┐
│  Raw Listings   │  200 items from search
└────────┬────────┘
         │ Filter by seen_ids
         ▼
┌─────────────────┐
│  New Listings   │  ~50-150 items
└────────┬────────┘
         │ Filter by excluded sellers
         ▼
┌─────────────────┐
│ Filtered List   │  ~30-100 items
└────────┬────────┘
         │ Filter by excluded keywords
         ▼
┌─────────────────┐
│ Candidates      │  ~20-80 items
└────────┬────────┘
         │ Parse title, match to products
         ▼
┌─────────────────┐
│   Matches       │  ~1-10 items
└────────┬────────┘
         │ Calculate profit, filter by threshold
         ▼
┌─────────────────┐
│  Notifications  │  ~0-5 items sent to Telegram
└─────────────────┘
```

### Product Catalog Updates

```
CSV Upload ──► /api/v1/products/import
                      │
                      ▼
              ┌───────────────┐
              │  Validation   │  Check required fields
              └───────┬───────┘
                      │
                      ▼
              ┌───────────────┐
              │   Upsert      │  Insert or update by (brand, asin)
              └───────┬───────┘
                      │
                      ▼
              ┌───────────────┐
              │  Reindex      │  Regenerate group_keys
              └───────────────┘
```

## Threading Model

```
main.py
   │
   ├── Thread: Canon Engine
   │      └── HTTP: eBay API, Telegram API
   │
   ├── Thread: Xerox Engine
   │      └── HTTP: eBay API, Telegram API
   │
   ├── Thread: Lexmark Engine
   │      └── HTTP: eBay API, Telegram API
   │
   └── Thread: Order History (hourly)
          └── HTTP: eBay Trading API (XML)
```

**Thread Safety:**
- Each thread gets its own SQLite connection via `get_db_connection()`
- `refresh_event` (threading.Event) signals token refresh across threads
- No shared mutable state between engines

## Error Handling

| Error | Handling |
|-------|----------|
| eBay 401 | Set `refresh_event`, retry with new token |
| eBay 429 | Exponential backoff, up to 5 retries |
| Telegram rate limit | Sleep 1s between messages |
| SQLite locked | Retry with exponential backoff |
| Network timeout | Log and skip, retry next cycle |

## Performance Characteristics

- **Polling interval:** 60 seconds (configurable via `RUN_INTERVAL`)
- **Listings per cycle:** ~200 per engine
- **Typical cycle time:** 15-45 seconds
- **Database size:** ~50MB after 6 months
- **Memory usage:** ~200MB typical
