# Operations Guide

Runtime commands and SQL queries for operating the eBay Toner Arbitrage Monitor.

## Monitoring Health

### Check if Monitor is Running

```sql
-- Should see timestamps within last 60 seconds
SELECT 
    'messages' AS source,
    datetime(MAX(timestamp), 'unixepoch', 'localtime') AS latest,
    (strftime('%s', 'now') - MAX(timestamp)) AS seconds_ago
FROM messages
WHERE timestamp > 0
UNION ALL
SELECT 'seen_ids', datetime(MAX(seen_ts), 'unixepoch', 'localtime'),
       (strftime('%s', 'now') - MAX(seen_ts))
FROM seen_ids WHERE seen_ts > 0;
```

If `seconds_ago` > 300, the monitor may be stalled.

### Table Row Counts

```sql
SELECT 'seen_ids' AS tbl, COUNT(*) AS rows FROM seen_ids
UNION ALL SELECT 'messages', COUNT(*) FROM messages
UNION ALL SELECT 'matches', COUNT(*) FROM matches
UNION ALL SELECT 'products', COUNT(*) FROM products
UNION ALL SELECT 'order_history', COUNT(*) FROM order_history;
```

---

## Analytics Queries

### Daily Match Summary (Last 7 Days)

```sql
SELECT 
    DATE(msg.timestamp, 'unixepoch') AS date,
    COUNT(*) AS matches,
    ROUND(SUM(m.profit), 2) AS total_profit,
    ROUND(AVG(m.profit), 2) AS avg_profit
FROM matches m
JOIN messages msg ON m.message_id = msg.id
WHERE msg.timestamp > strftime('%s', 'now', '-7 days')
GROUP BY date ORDER BY date DESC;
```

### Top Profitable ASINs

```sql
SELECT asin, COUNT(*) AS hits,
       ROUND(SUM(profit), 2) AS total_profit,
       ROUND(AVG(profit), 2) AS avg_profit
FROM matches
WHERE asin IS NOT NULL
GROUP BY asin ORDER BY total_profit DESC LIMIT 15;
```

### Match Rate (Last 24h)

```sql
SELECT 
    COUNT(DISTINCT msg.id) AS listings,
    COUNT(DISTINCT m.message_id) AS matched,
    ROUND(100.0 * COUNT(DISTINCT m.message_id) / COUNT(DISTINCT msg.id), 1) AS match_pct
FROM messages msg
LEFT JOIN matches m ON msg.id = m.message_id
WHERE msg.timestamp > strftime('%s', 'now', '-1 day');
```

### Profit Distribution

```sql
SELECT 
    CASE 
        WHEN profit < 0 THEN 'Loss'
        WHEN profit < 10 THEN '$0-10'
        WHEN profit < 25 THEN '$10-25'
        WHEN profit < 50 THEN '$25-50'
        ELSE '$50+'
    END AS range,
    COUNT(*) AS count
FROM matches GROUP BY range;
```

### Unmatched Listings (Debug)

```sql
SELECT msg.title, msg.link
FROM messages msg
LEFT JOIN matches m ON msg.id = m.message_id
WHERE m.id IS NULL
  AND msg.timestamp > strftime('%s', 'now', '-1 day')
LIMIT 20;
```

---

## Exclusion Management

### View Current Exclusions

```sql
SELECT 'Sellers' AS type, COUNT(*) AS count FROM excluded_sellers
UNION ALL SELECT 'Canon Keywords', COUNT(*) FROM canon_excluded_keywords
UNION ALL SELECT 'Xerox Keywords', COUNT(*) FROM xerox_excluded_keywords
UNION ALL SELECT 'Lexmark Keywords', COUNT(*) FROM lexmark_excluded_keywords;
```

### List All Excluded Sellers

```sql
SELECT name FROM excluded_sellers ORDER BY name;
```

---

## Data Cleanup

> ⚠️ **Warning:** These commands permanently delete data.

### Clear Processing Data (Keep Products & Exclusions)

```sql
DELETE FROM seen_ids;
DELETE FROM messages;
DELETE FROM matches;
DELETE FROM sqlite_sequence WHERE name IN ('messages', 'matches');
```

### Clear Order History

```sql
DELETE FROM order_history;
DELETE FROM purchased_units;
```

### Full Reset (Keep Schema)

```sql
DELETE FROM seen_ids;
DELETE FROM messages;
DELETE FROM matches;
DELETE FROM order_history;
DELETE FROM purchased_units;
DELETE FROM sqlite_sequence;
```

---

## Troubleshooting

### Database Locked

```bash
# Find zombie processes
ps aux | grep python

# Kill and remove lock
pkill -f "python.*main.py"
rm -f database.db-journal
```

### Rebuild Database

```bash
# Backup, delete, restart (schema auto-creates)
cp database.db database.db.backup
rm database.db
docker-compose up
```

### Check Database Integrity

```sql
-- Orphaned matches (should be 0)
SELECT COUNT(*) FROM matches m
LEFT JOIN messages msg ON m.message_id = msg.id
WHERE msg.id IS NULL;

-- Stale seen_ids (should be 0, GC runs hourly)
SELECT COUNT(*) FROM seen_ids
WHERE seen_ts < strftime('%s', 'now', '-72 hours');
```

### Vacuum Database

```sql
VACUUM;
ANALYZE;
```

---

## Quick Reference

| Task | Command |
|------|---------|
| Access DB (macOS) | `sqlite3 database.db` |
| Access DB (Docker) | `docker exec -it proco-ebay-notifier sqlite3 /app/database.db` |
| Show tables | `.tables` |
| Show schema | `.schema tablename` |
| Pretty output | `.headers on` then `.mode column` |
| Exit | `.quit` |
