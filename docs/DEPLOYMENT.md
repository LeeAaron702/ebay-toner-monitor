# Deployment Guide

This guide covers production deployment, Docker configuration, and troubleshooting.

## Docker Deployment (Recommended)

### Prerequisites

- Docker Engine 20.10+
- Docker Compose v2+
- `.env` file with credentials
- Public URL for Telegram webhook (Cloudflare Tunnel, ngrok, etc.)

### Quick Start

```bash
# 1. Configure environment
cp .env.example .env
# Edit .env with your credentials

# 2. Build and start
docker-compose up --build -d

# 3. Check logs
docker logs proco-ebay-notifier --follow

# 4. Verify
curl http://localhost:8000/healthz
```

### Docker Compose Configuration

```yaml
services:
  proco-ebay-notifier:
    build: .
    container_name: proco-ebay-notifier
    restart: unless-stopped
    env_file:
      - .env
    environment:
      - PYTHONUNBUFFERED=1
    volumes:
      - .:/app
    ports:
      - "8000:8000"
    command: ["uvicorn", "api.api_server:app", "--host", "0.0.0.0", "--port", "8000"]
```

### Dockerfile

```dockerfile
FROM python:3.13-slim

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# For Playwright (analyzer scraper)
RUN apt-get update && apt-get install -y --no-install-recommends libnss3 libnspr4 \
    libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 libxkbcommon0 libxcomposite1 \
    libxdamage1 libxfixes3 libxrandr2 libgbm1 libasound2 && rm -rf /var/lib/apt/lists/*
RUN playwright install chromium

COPY . .
CMD ["uvicorn", "api.api_server:app", "--host", "0.0.0.0", "--port", "8000"]
```

---

## Environment Variables

### Required

| Variable | Description | Example |
|----------|-------------|---------|
| `EBAY_APP_ID` | eBay OAuth client ID | `ProcoEba-Notifier-PRD-...` |
| `EBAY_CLIENT_SECRET` | eBay OAuth client secret | `PRD-abc123...` |
| `TELEGRAM_BOT_TOKEN` | Bot token from @BotFather | `123456789:ABCDef...` |
| `TELEGRAM_CHAT_ID` | Target chat/group ID | `-1001234567890` |
| `TELEGRAM_WEBHOOK_URL` | Public webhook endpoint | `https://ebay.example.com/telegram/webhook` |
| `TELEGRAM_WEBHOOK_SECRET` | Webhook validation secret | `my_secret_32_chars_or_more` |

### Optional

| Variable | Default | Description |
|----------|---------|-------------|
| `RUN_INTERVAL` | `60` | Polling interval in seconds |
| `DB_PATH` | `database.db` | SQLite database path |
| `EXCLUDE_API_KEY` | *(none)* | API key for `/exclude/*` endpoints |
| `OLLAMA_BASE_URL` | `http://ollama:11434` | Ollama server for AI parsing |
| `ZIP_CODE` | `93012` | Zip code for eBay shipping estimates |

---

## Webhook Setup

### Cloudflare Tunnel (Recommended)

```bash
# Install cloudflared
brew install cloudflare/cloudflare/cloudflared

# Login
cloudflared login

# Create tunnel
cloudflared tunnel create ebay-telegram

# Configure (~/.cloudflared/config.yml)
tunnel: <tunnel-id>
credentials-file: ~/.cloudflared/<tunnel-id>.json
ingress:
  - hostname: ebay-telegram.yourdomain.com
    service: http://localhost:8000
  - service: http_status:404

# Run tunnel
cloudflared tunnel run ebay-telegram
```

### Verifying Webhook

```bash
# Check Telegram webhook info
curl "https://api.telegram.org/bot<TOKEN>/getWebhookInfo"

# Expected response
{
  "ok": true,
  "result": {
    "url": "https://ebay-telegram.yourdomain.com/telegram/webhook",
    "has_custom_certificate": false,
    "pending_update_count": 0
  }
}
```

---

## Monitoring

### Log Patterns

**Healthy startup:**
```
MONITOR STARTUP - Jan 21 2026, 07:27 PM PST
[Monitor] ✓ Canon products: 198 rows from SQL
[Monitor] ✓ Xerox products: 113 rows from SQL
[Monitor] ✓ Lexmark products: 160 rows from SQL
```

**Successful cycle:**
```
LOG - Canon.py - 07:27:57 PM 130 listings fetched
LOG - Canon.py - 130 new listings to process
LOG - Canon.py - Sent album for v1|206009139062|0
```

**Token refresh:**
```
Refreshing access token for ebay_tokens_personal.json...
Saved tokens → ebay_tokens_personal.json
```

### Health Endpoints

```bash
# Application health
curl http://localhost:8000/healthz
# {"status": "ok"}

# Database status
docker exec proco-ebay-notifier sqlite3 /app/database.db \
  "SELECT 'seen_ids', COUNT(*) FROM seen_ids UNION SELECT 'matches', COUNT(*) FROM matches"
```

---

## Troubleshooting

### Container Won't Start

**Symptom:** Container exits immediately

**Check logs:**
```bash
docker logs proco-ebay-notifier
```

**Common causes:**
- Missing `.env` file → Create from `.env.example`
- Invalid `TELEGRAM_BOT_TOKEN` → Verify with @BotFather
- Missing eBay credentials → Check `EBAY_APP_ID` and `EBAY_CLIENT_SECRET`

### Bot Not Responding

**Symptom:** Telegram commands return no response

**Checks:**
1. Verify webhook is set:
   ```bash
   curl "https://api.telegram.org/bot<TOKEN>/getWebhookInfo"
   ```
2. Check webhook URL is publicly accessible
3. Verify `TELEGRAM_WEBHOOK_SECRET` matches in bot settings
4. Check FastAPI logs for incoming webhook requests

### No Listings Found

**Symptom:** Monitor runs but no Telegram notifications

**Checks:**
1. Verify eBay tokens are valid (check for "401" errors in logs)
2. Check exclusion lists aren't over-filtering:
   ```bash
   docker exec proco-ebay-notifier sqlite3 /app/database.db \
     "SELECT COUNT(*) FROM excluded_sellers"
   ```
3. Verify products have `net_cost` values set
4. Check profit threshold (default: matches need positive profit)

### Database Locked

**Symptom:** `sqlite3.OperationalError: database is locked`

**Fix:**
```bash
# Stop all instances
docker-compose down

# Check for orphaned processes
ps aux | grep python

# Remove lock file if present
rm database.db-journal

# Restart
docker-compose up
```

### Token Refresh Failures

**Symptom:** Repeated "401 Unauthorized" errors

**Fix:**
1. Delete token files:
   ```bash
   rm ebay_tokens_*.json
   ```
2. Restart application (will re-authenticate)

---

## Backups

### Database Backup

```bash
# Create backup
cp database.db "database_$(date +%Y%m%d_%H%M%S).db"

# Or with Docker
docker exec proco-ebay-notifier cp /app/database.db /app/backups/
```

### Automated Backups (cron)

```bash
# Add to crontab
0 * * * * docker exec proco-ebay-notifier sqlite3 /app/database.db ".backup '/app/backups/db_hourly.db'"
0 0 * * * docker exec proco-ebay-notifier cp /app/database.db "/app/backups/db_$(date +\%Y\%m\%d).db"
```

---

## Git Auto-Sync (Multi-Machine)

The project includes automatic git synchronization for running across multiple machines.

### Enable Auto-Sync

Add to your `.env`:
```bash
GIT_AUTO_SYNC=true
MACHINE_ID=production-server  # Unique name for this machine
GIT_SYNC_BRANCH=main
GIT_USER_NAME=Toner Monitor Bot
GIT_USER_EMAIL=bot@toner-monitor.local
```

### How It Works

The `git-sync` Docker service runs at **6:00 AM** and **6:00 PM**:

1. **Pull** latest changes from GitHub
2. **Commit** local changes (database updates)
3. **Push** back to GitHub

### Docker Compose Services

```yaml
services:
  # Main application
  proco-ebay-notifier:
    # ...

  # Git sync (runs 2x daily)
  git-sync:
    build: .
    container_name: proco-git-sync
    restart: unless-stopped
    environment:
      - GIT_AUTO_SYNC=true
    volumes:
      - .:/app
      - ~/.ssh:/root/.ssh:ro  # SSH keys for git push
    command: ["python", "scripts/git_sync.py", "--daemon"]
```

### SSH Key Setup

Each machine needs SSH access to GitHub:

```bash
# Generate key (if not exists)
ssh-keygen -t ed25519 -C "your-email@example.com"

# Add public key to GitHub
cat ~/.ssh/id_ed25519.pub
# Go to: https://github.com/settings/keys → Add SSH key
```

### Manual Sync

```bash
# Run sync manually
python scripts/git_sync.py

# Or inside Docker
docker exec proco-git-sync python scripts/git_sync.py
```

---

## Scaling Considerations

### Current Limits

- **SQLite:** Single-writer, suitable for 1 instance
- **Polling interval:** 60 seconds minimum (eBay rate limits)
- **Listings per cycle:** ~200 per engine (API limit)

### Future Improvements

- **PostgreSQL:** For multi-instance deployment
- **Redis:** For shared seen_ids cache
- **Message queue:** For async notification delivery
