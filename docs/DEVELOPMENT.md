# Development Guide

This guide covers local development setup, testing, and contribution guidelines.

## Local Setup

### Prerequisites

- Python 3.11+ (3.13 recommended)
- Git
- SQLite (included with Python)

### Installation

```bash
# Clone repository
git clone <repo-url>
cd "Proco Ebay Notifications Telelgram"

# Create virtual environment
python -m venv .venv

# Activate (choose your OS)
source .venv/bin/activate      # Linux/macOS
.venv\Scripts\activate         # Windows CMD
.venv\Scripts\Activate.ps1     # Windows PowerShell

# Install dependencies
pip install -r requirements.txt

# Create environment file
cp .env.example .env
# Edit .env with your credentials
```

### Running Locally

```bash
# Start FastAPI server (spawns monitor automatically)
uvicorn api.api_server:app --host 0.0.0.0 --port 8000 --reload

# Or run monitor directly (without Telegram webhook)
python main.py
```

---

## Testing

### Running Tests

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test file
python -m pytest tests/test_products_db.py -v

# Run with coverage
pip install pytest-cov
python -m pytest tests/ --cov=db --cov=api
```

### Test Structure

```
tests/
├── test_products_db.py      # Database CRUD operations
├── test_products_api.py     # API endpoint tests
└── test_analyzer_integration.py  # Engine integration tests
```

### Manual Testing

**Test engine imports:**
```python
from engine.canon import canon
from engine.xerox import xerox
from engine.lexmark import lexmark
from db.products_db import get_canon_products, get_xerox_products, get_lexmark_products

# Verify product loading
print(f"Canon: {len(get_canon_products())} products")
print(f"Xerox: {len(get_xerox_products())} products")
print(f"Lexmark: {len(get_lexmark_products())} products")
```

**Test lot parsing:**
```python
from engine.canon import build_lot_breakdown
import pandas as pd

# Create minimal test data
df = get_canon_products()
breakdown = build_lot_breakdown("5-YELLOW, 3-MAGENTA Canon 046", df)
print(breakdown.color_quantities)  # {'yellow': 5, 'magenta': 3}
```

---

## Code Style

### Formatting

- **PEP 8** compliance
- 4-space indentation
- 100-character line limit (flexible for long strings)
- Type hints encouraged for new code

```bash
# Install formatters
pip install black isort

# Format code
black .
isort .

# Check style (no auto-fix)
pip install flake8
flake8 engine/ api/ db/ utils/
```

### Conventions

**Imports:**
```python
# Standard library
import os
import re
from typing import List, Dict, Optional

# Third-party
import pandas as pd
import requests
from fastapi import FastAPI

# Local
from db.products_db import get_canon_products
from utils.base import _log
```

**Logging:**
```python
from utils.base import _log

def my_function():
    _log(f"Processing {count} items")  # Prints with timestamp
```

**Database access:**
```python
from db.listings_db import get_db_connection

def my_query():
    conn = get_db_connection()
    try:
        cursor = conn.execute("SELECT * FROM products")
        return cursor.fetchall()
    finally:
        conn.close()
```

---

## Project Structure

```
├── api/                        # FastAPI application
│   ├── __init__.py
│   ├── api_server.py           # Main entrypoint
│   └── routers/
│       ├── __init__.py
│       ├── exclude_api.py      # Exclusion REST endpoints
│       ├── products_api.py     # Product CRUD endpoints
│       └── telegram_exclude_commands.py
│
├── db/                         # Database layer
│   ├── __init__.py
│   ├── exclusions_db.py        # Seller/keyword exclusions
│   ├── listings_db.py          # Messages, matches, seen_ids
│   ├── products_db.py          # Product catalog
│   └── seen_ids_db.py          # Deduplication helpers
│
├── engine/                     # Scraping engines
│   ├── __init__.py
│   ├── canon.py                # Canon lot parser (~1800 lines)
│   ├── xerox.py                # Xerox SKU matcher
│   └── lexmark.py              # Lexmark part number matcher
│
├── order_history/              # Purchase tracking
│   └── ebay_order_history.py   # Trading API integration
│
├── utils/                      # Shared utilities
│   ├── __init__.py
│   ├── base.py                 # Common helpers (_log, etc.)
│   ├── telegram_service.py     # Bot command handlers
│   ├── analyzer_job.py         # Scheduled analyzer scraper
│   ├── analyzer_scraper.py     # Playwright scraper
│   └── analyzer_parser.py      # Parse analyzer results
│
├── tests/                      # Test suite
├── docs/                       # Documentation
│
├── main.py                     # Monitor orchestrator
├── requirements.txt            # Python dependencies
├── Dockerfile                  # Container definition
├── docker-compose.yml          # Docker Compose config
└── .env                        # Environment variables (not in git)
```

---

## Key Files

### `main.py` - Monitor Orchestrator

```python
def monitor():
    """Main loop that runs every RUN_INTERVAL seconds."""
    while True:
        # Load fresh product data
        canon_df = get_canon_products()
        xerox_df = get_xerox_products()
        lexmark_df = get_lexmark_products()
        
        # Spawn parallel threads
        threads = [
            Thread(target=_run_canon_job, args=(token, canon_df)),
            Thread(target=_run_xerox_job, args=(token, xerox_df)),
            Thread(target=_run_lexmark_job, args=(token, lexmark_df)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        time.sleep(RUN_INTERVAL)
```

### `engine/canon.py` - Canon Engine

Key functions:
- `canon(token, df)` - Main entry point
- `search_listings(token)` - eBay API call
- `build_lot_breakdown(title, df)` - Parse complex lots
- `match_listing(title, df)` - Find product match
- `send_telegram_message(...)` - Format and send notification

### `db/products_db.py` - Product Database

Key functions:
- `get_canon_products()` - Returns DataFrame for engine
- `get_xerox_products()` - Returns DataFrame for engine
- `get_lexmark_products()` - Returns DataFrame for engine
- `create_product(data)` - Insert new product
- `bulk_upsert_products(rows)` - Batch import
- `bulk_update_metrics(updates)` - Update BSR/net_cost

---

## Adding a New Brand

1. **Create engine file** (`engine/newbrand.py`):
   ```python
   from db.products_db import get_products_by_brand
   
   def newbrand(token: str, df: pd.DataFrame):
       listings = search_listings(token)
       for listing in listings:
           # Parse, match, notify
           pass
   ```

2. **Add product loader** (`db/products_db.py`):
   ```python
   def get_newbrand_products() -> pd.DataFrame:
       return get_products_by_brand('NewBrand')
   ```

3. **Register in main.py**:
   ```python
   from engine.newbrand import newbrand
   
   # In monitor():
   newbrand_df = get_newbrand_products()
   Thread(target=_run_newbrand_job, args=(token, newbrand_df))
   ```

4. **Add tests** (`tests/test_newbrand.py`)

---

## Debugging Tips

### Enable verbose logging

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Inspect database

```bash
# Open SQLite shell
sqlite3 database.db

# Common queries
.tables
SELECT COUNT(*) FROM products WHERE brand='Canon';
SELECT * FROM matches ORDER BY timestamp DESC LIMIT 5;
```

### Test eBay API

```python
from utils.base import get_oauth_token

token = get_oauth_token()
print(f"Token: {token[:20]}...")
```

### Test Telegram

```python
import requests

BOT_TOKEN = "your_token"
CHAT_ID = "your_chat_id"

requests.post(
    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
    json={"chat_id": CHAT_ID, "text": "Test message"}
)
```

---

## Pull Request Guidelines

1. **One feature per PR** — Easier to review
2. **Update docs** — If changing APIs or data flow
3. **Add tests** — For new functionality
4. **Test locally** — Run `pytest` before submitting
5. **Clear commit messages** — Describe what and why

### Commit Message Format

```
feat: Add Lexmark engine support

- Create engine/lexmark.py with part number parsing
- Add get_lexmark_products() to products_db
- Register in main.py monitor loop
- Add tests for part number extraction
```

Prefixes: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`
