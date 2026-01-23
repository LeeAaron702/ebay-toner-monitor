<div align="center">

# 🖨️ eBay Toner Arbitrage Monitor

### Real-time eBay listing intelligence for printer toner resellers

[![Python 3.13](https://img.shields.io/badge/Python-3.13-3776AB?logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.109-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white)](https://docker.com)
[![Telegram](https://img.shields.io/badge/Telegram-Bot-26A5E4?logo=telegram&logoColor=white)](https://core.telegram.org/bots)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Automated profit detection** • **Multi-threaded scanning** • **Instant Telegram alerts**

[Features](#-features) • [Architecture](#-architecture) • [Quick Start](#-quick-start) • [Configuration](#%EF%B8%8F-configuration) • [API Reference](#-api-reference)

</div>

---

## 🎯 What It Does

This system continuously monitors eBay for **Canon, Xerox, and Lexmark** toner cartridge listings, automatically identifying arbitrage opportunities by comparing listing prices against a **dynamically-updated product catalog** with real-time Amazon resale values.

The catalog is automatically refreshed daily via a **headless browser automation pipeline** that scrapes Amazon seller metrics through a third-party analytics proxy, ensuring profit calculations always reflect current market conditions.

```
📦 eBay Listing Found: "Canon 046 Toner Set CMYK - $89.99"
    ↓
🔍 Matched to catalog: 4 units × $35 net each = $140 value
    ↓
💰 Profit detected: $140 - $89.99 = $50.01
    ↓
📱 Telegram alert sent in <2 seconds
```

---

## ✨ Features

<table>
<tr>
<td width="50%">

### 🔄 Real-Time Monitoring
- Polls eBay Browse API every 60 seconds
- Parallel processing with dedicated threads per brand
- Smart deduplication (72-hour sliding window)
- Automatic OAuth token refresh

</td>
<td width="50%">

### 🧠 Intelligent Parsing
- Complex lot decomposition ("5-YELLOW, 3-MAGENTA")
- Multi-pack detection and set matching
- Capacity recognition (Standard/High/Super High)
- Confidence scoring for uncertain matches

</td>
</tr>
<tr>
<td width="50%">

### 📱 Telegram Integration
- Instant profit alerts with item images
- Interactive bot commands (`/stats`, `/exclude`)
- Webhook mode for reliability
- Scheduled daily reports (9am, 12pm, 5pm PST)

</td>
<td width="50%">

### 🛡️ Filtering System
- Seller blacklist management
- Keyword exclusion filters
- Per-brand filter support
- REST API for remote management

</td>
</tr>
<tr>
<td width="50%">

### 📊 Analytics Dashboard
- Web-based admin panel
- Order history tracking
- Profit/loss reporting
- Product catalog management

</td>
<td width="50%">

### 🐳 Production Ready
- Docker Compose deployment
- Health check endpoints
- Graceful shutdown handling
- Comprehensive logging

</td>
</tr>
</table>

---

## 🔄 Automated Amazon Data Pipeline

One of the most critical components of this system is the **daily automated pricing pipeline** that keeps product valuations accurate and market-responsive.

### How It Works

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    DAILY AMAZON DATA PIPELINE (7am PST)                             │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  1. EXPORT                                                                          │
│     ┌──────────────┐                                                                │
│     │   SQLite DB  │ ──▶ Extract 471 ASINs ──▶ Generate CSV                        │
│     │  (products)  │     with unique timestamp                                      │
│     └──────────────┘                                                                │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  2. HEADLESS BROWSER AUTOMATION (Playwright)                                        │
│     ┌────────────┐    ┌────────────┐    ┌────────────┐    ┌─────────────┐          │
│     │   Launch   │───▶│   Login    │───▶│  Upload    │───▶│   Wait for  │          │
│     │  Chromium  │    │  via SPA   │    │    CSV     │    │  Processing │          │
│     └────────────┘    └────────────┘    └────────────┘    └─────────────┘          │
│                                                                  │                  │
│     Handles: JavaScript SPAs, dynamic forms, file uploads        │                  │
│              authentication flows, download triggers             ▼                  │
│                                                          ┌─────────────┐           │
│                                                          │  Download   │           │
│                                                          │   Excel     │           │
│                                                          └─────────────┘           │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  3. PARSE & UPDATE                                                                  │
│     ┌────────────────┐         ┌──────────────────┐         ┌─────────────────┐    │
│     │  Excel Parser  │────────▶│  Extract Metrics │────────▶│  Bulk Update    │    │
│     │   (pandas)     │         │                  │         │    Database     │    │
│     └────────────────┘         │  • Seller Proceeds (net)   └─────────────────┘    │
│                                │  • BSR (30d/90d avg)                              │
│                                │  • FBA competition                                │
│                                │  • Sellability score                              │
│                                └──────────────────┘                                │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Key Technical Achievements

| Challenge | Solution |
|-----------|----------|
| **JavaScript SPA Authentication** | Playwright handles dynamic login forms, waits for SPA navigation |
| **Anti-Bot Detection** | Headless browser with realistic viewport, user-agent, and timing |
| **Async File Processing** | Polling mechanism waits for server-side analysis completion |
| **Data Extraction** | pandas parses complex Excel with 50+ columns, extracts 4 key metrics |
| **Bulk Updates** | Single transaction updates 471 products in <1 second |

### Metrics Extracted

```python
# From analyzer.tools Excel export:
{
    "net_cost": 32.50,      # Seller Proceeds after Amazon fees (our max buy price)
    "bsr": 15420,           # Best Seller Rank (30-day average)
    "sellable": True,       # Derived from BSR threshold (<1M)
    "fba_offers": 12        # Competition count
}
```

### Why This Matters

- **Real-time Accuracy**: Profit calculations use yesterday's actual Amazon data, not stale estimates
- **Market Responsiveness**: BSR changes and fee adjustments are captured within 24 hours  
- **Zero Manual Work**: Fully autonomous—runs at 7am PST daily without intervention
- **Resilient Design**: Handles network failures, login issues, and processing delays gracefully

---

## 🏗 Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              FastAPI Server                                  │
│                            (api/api_server.py)                               │
│                                                                              │
│   ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────────────┐   │
│   │    Telegram     │   │   Products API  │   │     Admin Panel         │   │
│   │    Webhook      │   │ /api/v1/products│   │       /admin/*          │   │
│   │/telegram/webhook│   │                 │   │                         │   │
│   └────────┬────────┘   └────────┬────────┘   └────────────┬────────────┘   │
└────────────┼─────────────────────┼──────────────────────────┼────────────────┘
             │                     │                          │
             │              spawns on startup                 │
             ▼                     ▼                          ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                         Monitor Loop (main.py)                               │
│                         Runs every 60 seconds                                │
│                                                                              │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐   │
│   │    Canon     │  │    Xerox     │  │   Lexmark    │  │ Order History │   │
│   │    Thread    │  │    Thread    │  │    Thread    │  │    Thread     │   │
│   │  ~1900 LOC   │  │   ~600 LOC   │  │   ~500 LOC   │  │    Hourly     │   │
│   └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └───────┬───────┘   │
│          │                 │                 │                  │           │
└──────────┼─────────────────┼─────────────────┼──────────────────┼───────────┘
           │                 │                 │                  │
           └─────────────────┴────────┬────────┴──────────────────┘
                                      │
                                      ▼
                    ┌─────────────────────────────────┐
                    │        SQLite Database          │
                    │         (database.db)           │
                    │                                 │
                    │  • products (471 SKUs)          │
                    │  • seen_ids (deduplication)     │
                    │  • matches (profitable finds)   │
                    │  • excluded_sellers             │
                    │  • order_history                │
                    └─────────────────────────────────┘

                         External Services
           ┌────────────────────────────────────────────┐
           │  eBay Browse API ←── Search & item data    │
           │  eBay Trading API ←── Order history (XML)  │
           │  Telegram Bot API ←── Notifications        │
           │  Ollama (optional) ←── AI parsing          │
           └────────────────────────────────────────────┘
```

### Engine Pipeline

Each brand engine follows the same processing flow:

```
┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐
│ Search  │───▶│ Filter  │───▶│  Parse  │───▶│  Match  │───▶│ Calculate│───▶│ Notify  │
│  eBay   │    │Exclusions│   │  Title  │    │ Catalog │    │  Profit  │    │Telegram │
└─────────┘    └─────────┘    └─────────┘    └─────────┘    └─────────┘    └─────────┘
     │              │              │              │              │              │
  Browse API   seen_ids      Regex/NLP      products      net_cost -       Bot API
  200 items    blacklists    extraction      table       (price+ship)     if profit
```

---

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- eBay Developer Account ([developer.ebay.com](https://developer.ebay.com))
- Telegram Bot Token ([@BotFather](https://t.me/botfather))

### 1. Clone & Configure

```bash
git clone https://github.com/yourusername/ebay-toner-monitor.git
cd ebay-toner-monitor

# Copy example environment file
cp .env.example .env

# Edit with your credentials
nano .env
```

### 2. Deploy with Docker

```bash
# Build and start
docker compose up -d --build

# View logs
docker compose logs -f

# Check health
curl http://localhost:8000/healthz
```

### 3. Set Telegram Webhook

```bash
# The app auto-registers the webhook on startup
# Verify it's working:
curl https://api.telegram.org/bot<TOKEN>/getWebhookInfo
```

---

## ⚙️ Configuration

### Environment Variables

Create a `.env` file with the following:

```bash
# ═══════════════════════════════════════════════════════════
# eBay API (Required)
# ═══════════════════════════════════════════════════════════
EBAY_APP_ID=YourApp-ID-PRD-xxxxxxxx-xxxxxxxx
EBAY_CLIENT_SECRET=PRD-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

# ═══════════════════════════════════════════════════════════
# Telegram Bot (Required)
# ═══════════════════════════════════════════════════════════
TELEGRAM_BOT_TOKEN=123456789:ABCDefGHIjklMNOPQRstUVWxyz
TELEGRAM_CHAT_ID=-1001234567890
TELEGRAM_WEBHOOK_URL=https://yourdomain.com/telegram/webhook
TELEGRAM_WEBHOOK_SECRET=your_random_32_char_secret_string

# ═══════════════════════════════════════════════════════════
# Optional Settings
# ═══════════════════════════════════════════════════════════
RUN_INTERVAL=60                    # Polling interval (seconds)
EXCLUDE_API_KEY=your_api_key       # Protect /exclude/* endpoints
OLLAMA_BASE_URL=http://ollama:11434  # AI parsing (optional)
```

### Product Catalog

The system matches listings against a SQLite product catalog:

| Field | Description | Example |
|-------|-------------|---------|
| `brand` | Manufacturer | Canon, Xerox, Lexmark |
| `model` | Printer model (Canon) | 046, 055H |
| `part_number` | OEM part (Xerox/Lexmark) | 006R01512 |
| `capacity` | Yield tier | Standard, High, Super High |
| `color` | Toner color | Black, Cyan, Magenta, Yellow |
| `net_cost` | Target buy price | 25.00 |
| `asin` | Amazon ASIN | B07XXXXXX |
| `bsr` | Best Seller Rank | 15000 |

---

## 📡 API Reference

### Health Check

```http
GET /healthz
```

### Products API

```http
GET    /api/v1/products              # List all products
GET    /api/v1/products/{id}         # Get single product
POST   /api/v1/products              # Create product
PUT    /api/v1/products/{id}         # Update product
DELETE /api/v1/products/{id}         # Delete product
```

### Exclusions API

```http
GET    /exclude/sellers              # List excluded sellers
POST   /exclude/sellers              # Add seller exclusion
DELETE /exclude/sellers/{seller}     # Remove exclusion

GET    /exclude/keywords/{brand}     # List keyword filters
POST   /exclude/keywords/{brand}     # Add keyword filter
```

### Telegram Bot Commands

| Command | Description |
|---------|-------------|
| `/start` | Initialize bot |
| `/stats` | Today's match statistics |
| `/exclude <seller>` | Add seller to blacklist |
| `/unexclude <seller>` | Remove from blacklist |
| `/help` | Show all commands |

---

## 📁 Project Structure

```
├── api/
│   ├── api_server.py          # FastAPI entrypoint
│   └── routers/
│       ├── admin.py           # Admin panel routes
│       ├── exclude_api.py     # Exclusion management
│       └── products_api.py    # Product CRUD
├── db/
│   ├── products_db.py         # Product catalog operations
│   ├── listings_db.py         # Matches & seen_ids
│   ├── exclusions_db.py       # Blacklist management
│   └── seen_ids_db.py         # Deduplication
├── engine/
│   ├── canon.py               # Canon lot parser (~1900 LOC)
│   ├── xerox.py               # Xerox SKU matcher
│   └── lexmark.py             # Lexmark part matcher
├── order_history/
│   └── ebay_order_history.py  # Purchase tracking
├── templates/                 # Jinja2 admin templates
├── utils/
│   ├── telegram_service.py    # Bot handlers
│   └── analyzer_*.py          # External tool integration
├── main.py                    # Monitor orchestrator
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

---

## 🔧 Development

### Local Setup

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run locally
uvicorn api.api_server:app --reload --port 8000
```

### Running Tests

```bash
pytest tests/ -v
```

### Code Statistics

| Component | Lines of Code | Purpose |
|-----------|---------------|---------|
| Canon Engine | ~1,920 | Complex lot parsing, CMYK set detection |
| Xerox Engine | ~600 | SKU pattern matching (006R01512) |
| Lexmark Engine | ~500 | Part number extraction |
| API Layer | ~500 | REST endpoints, webhooks |
| Database Layer | ~800 | SQLite operations, CRUD |
| Utilities | ~850 | Telegram, scrapers, helpers |
| **Total** | **~7,500** | |

---

## 📈 Sample Telegram Alert

```
🟢 CANON MATCH

📦 Canon 046 High Yield CMYK Set
💰 Price: $89.99 + $0.00 shipping

┌─────────────────────────────┐
│ Color    Qty   Net    Total │
├─────────────────────────────┤
│ Black     1   $32.00  $32.00│
│ Cyan      1   $28.00  $28.00│
│ Magenta   1   $28.00  $28.00│
│ Yellow    1   $28.00  $28.00│
├─────────────────────────────┤
│ TOTAL     4          $116.00│
└─────────────────────────────┘

📊 Profit: $26.01 (22.4% margin)
🔗 ebay.com/itm/123456789
```

---

## 🔄 Multi-Machine Sync

This project supports deployment across multiple machines with automatic git synchronization:

```bash
# Runs automatically 2x daily inside Docker container
# Or manually:
python scripts/git_sync.py
```

The sync script:
- Pulls latest changes before pushing (handles multi-machine updates)
- Auto-resolves simple conflicts
- Commits code changes with timestamps
- Excludes sensitive files (credentials, database)

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

<div align="center">

</div>
