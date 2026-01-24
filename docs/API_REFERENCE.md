# API Reference

This document covers REST API endpoints and database operations.

## Products API

Base path: `/api/v1/products`

### List Products

```http
GET /api/v1/products
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `brand` | string | Filter by brand: `Canon`, `Xerox`, `Lexmark` |
| `limit` | integer | Max results (default: 100) |
| `offset` | integer | Pagination offset |

**Response:**
```json
{
  "products": [
    {
      "id": 1,
      "brand": "Canon",
      "model": "MF731Cdw",
      "capacity": "High",
      "part_number": "1254C001",
      "variant_label": "046H Black",
      "color": "Black",
      "pack_size": 1,
      "asin": "B01N5VJF4P",
      "amazon_sku": null,
      "net_cost": 85.50,
      "bsr": 7182,
      "sellable": true,
      "group_key": "Canon|MF731Cdw|High"
    }
  ],
  "total": 471,
  "limit": 100,
  "offset": 0
}
```

### Get Product

```http
GET /api/v1/products/{id}
```

### Create Product

```http
POST /api/v1/products
Content-Type: application/json
```

**Body:**
```json
{
  "brand": "Canon",
  "model": "MF731Cdw",
  "capacity": "High",
  "part_number": "1254C001",
  "variant_label": "046H Black",
  "color": "Black",
  "pack_size": 1,
  "asin": "B01N5VJF4P",
  "net_cost": 85.50,
  "bsr": 7182,
  "sellable": true
}
```

### Update Product

```http
PUT /api/v1/products/{id}
Content-Type: application/json
```

### Delete Product

```http
DELETE /api/v1/products/{id}
```

### Bulk Import

```http
POST /api/v1/products/import
Content-Type: multipart/form-data
```

**Form Data:**
- `file`: CSV or Excel file

**Required CSV Columns (by brand):**

*Canon:*
- brand, model, capacity, color, pack_size, asin, net_cost

*Xerox:*
- brand, part_number, variant_label, capacity, asin, net_cost

*Lexmark:*
- brand, part_number, variant_label, color, capacity, pack_size, asin, net_cost

**Optional Columns (all brands):**
- `sellable`: 0/1, true/false, yes/no (default: 1) - Controls whether product is included in monitoring
- `notes`: Free text notes displayed in Telegram alerts

**Response:**
```json
{
  "created": 45,
  "updated": 12,
  "skipped": 3,
  "errors": ["Row 5: Invalid brand 'Epson'"]
}
```

### Update Metrics

```http
POST /api/v1/products/metrics
Content-Type: application/json
```

**Body:**
```json
{
  "updates": [
    {"asin": "B01N5VJF4P", "bsr": 5432, "net_cost": 82.00},
    {"asin": "B07QXYZ123", "bsr": 8901}
  ]
}
```

### Download Template

```http
GET /api/v1/products/template?brand=Canon
```

Returns CSV template with correct columns for the specified brand.

### List Group Keys

```http
GET /api/v1/products/groups?brand=Canon
```

**Response:**
```json
{
  "group_keys": [
    "Canon|MF731Cdw|High",
    "Canon|MF731Cdw|Standard",
    "Canon|LBP612Cdw|High"
  ]
}
```

### Get Products by Group

```http
GET /api/v1/products/groups/{group_key}
```

---

## Exclusions API

Base path: `/exclude`

> **Note:** If `EXCLUDE_API_KEY` is set, include header: `X-API-Key: <key>`

### Sellers

```http
GET /exclude/sellers                    # List all
POST /exclude/sellers                   # Add {"name": "seller_name"}
DELETE /exclude/sellers/{name}          # Remove
```

### Canon Keywords

```http
GET /exclude/canon_keywords             # List all
POST /exclude/canon_keywords            # Add {"keyword": "compatible"}
DELETE /exclude/canon_keywords/{kw}     # Remove
```

### Xerox Keywords

```http
GET /exclude/xerox_keywords             # List all
POST /exclude/xerox_keywords            # Add {"keyword": "refurbished"}
DELETE /exclude/xerox_keywords/{kw}     # Remove
```

---

## Telegram Webhook

```http
POST /telegram/webhook
X-Telegram-Bot-Api-Secret-Token: {TELEGRAM_WEBHOOK_SECRET}
```

Receives updates from Telegram. Handled automatically by python-telegram-bot.

---

## Health Check

```http
GET /healthz
```

**Response:**
```json
{"status": "ok"}
```

---

## Database Schema

### Products Table

```sql
CREATE TABLE products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    brand TEXT NOT NULL CHECK(brand IN ('Canon', 'Xerox', 'Lexmark')),
    model TEXT,
    capacity TEXT,
    part_number TEXT,
    variant_label TEXT,
    color TEXT,
    pack_size INTEGER DEFAULT 1,
    asin TEXT,
    amazon_sku TEXT,
    net_cost REAL,
    bsr INTEGER,
    sellable INTEGER DEFAULT 1,
    is_model_block INTEGER DEFAULT 0,
    group_key TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(brand, asin)
);

CREATE INDEX idx_products_brand ON products(brand);
CREATE INDEX idx_products_group_key ON products(group_key);
CREATE INDEX idx_products_asin ON products(asin);
CREATE INDEX idx_products_part_number ON products(part_number);
```

### Seen IDs Table

```sql
CREATE TABLE seen_ids (
    item_id TEXT PRIMARY KEY,
    seen_ts INTEGER NOT NULL
);

CREATE INDEX idx_seen_ids_ts ON seen_ids(seen_ts);
```

Garbage collected every 72 hours via `gc_old_ids()`.

### Messages Table

```sql
CREATE TABLE messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    link TEXT,
    item_id TEXT,
    price REAL,
    shipping REAL,
    listed_time TEXT,
    timestamp INTEGER
);
```

### Matches Table

```sql
CREATE TABLE matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER,
    title TEXT,
    asin TEXT,
    net_cost REAL,
    profit REAL,
    total_units INTEGER,
    is_mixed_lot INTEGER DEFAULT 0,
    lot_breakdown TEXT,
    confidence TEXT,
    confidence_notes TEXT,
    timestamp INTEGER,
    FOREIGN KEY (message_id) REFERENCES messages(id)
);
```

### Exclusion Tables

```sql
CREATE TABLE excluded_sellers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL COLLATE NOCASE
);

CREATE TABLE canon_excluded_keywords (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword TEXT UNIQUE NOT NULL COLLATE NOCASE
);

CREATE TABLE xerox_excluded_keywords (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword TEXT UNIQUE NOT NULL COLLATE NOCASE
);
```

### Order History Tables

```sql
CREATE TABLE order_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT,
    item_id TEXT,
    item_title TEXT,
    quantity_purchased INTEGER,
    transaction_price REAL,
    order_total REAL,
    created_time TEXT,
    account_id TEXT,
    UNIQUE(order_id, item_id)
);

CREATE TABLE purchased_units (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT,
    unit_color TEXT,
    unit_quantity INTEGER,
    unit_cost REAL,
    purchased_time INTEGER
);
```

---

## Common SQL Queries

### Check System Health

```sql
-- Recent activity
SELECT 
    'seen_ids' AS table_name,
    COUNT(*) AS rows,
    datetime(MAX(seen_ts), 'unixepoch', 'localtime') AS latest
FROM seen_ids
UNION ALL
SELECT 'messages', COUNT(*), datetime(MAX(timestamp), 'unixepoch', 'localtime')
FROM messages
UNION ALL
SELECT 'matches', COUNT(*), datetime(MAX(timestamp), 'unixepoch', 'localtime')
FROM matches;
```

### Product Counts by Brand

```sql
SELECT brand, COUNT(*) AS count
FROM products
GROUP BY brand;
```

### Recent Matches

```sql
SELECT m.title, m.profit, m.total_units, datetime(m.timestamp, 'unixepoch', 'localtime') AS time
FROM matches m
ORDER BY m.timestamp DESC
LIMIT 20;
```

### Exclusion Lists

```sql
SELECT 'sellers' AS type, COUNT(*) FROM excluded_sellers
UNION ALL SELECT 'canon_keywords', COUNT(*) FROM canon_excluded_keywords
UNION ALL SELECT 'xerox_keywords', COUNT(*) FROM xerox_excluded_keywords;
```
