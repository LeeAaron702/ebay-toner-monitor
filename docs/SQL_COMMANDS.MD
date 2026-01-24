## Use this to clear database

Clears operational tables (matches, messages, order_history, purchased_units, seen_ids) while preserving the products table.

```bash
sqlite3 "/Users/lee/Downloads/Proco Ebay Notifications Telelgram/database.db" "DELETE FROM matches; DELETE FROM messages; DELETE FROM order_history; DELETE FROM purchased_units; DELETE FROM seen_ids; DELETE FROM sqlite_sequence WHERE name IN ('matches', 'messages', 'order_history', 'purchased_units', 'seen_ids'); VACUUM;"
```

## Sync products from CSV audit

Updates sellable status and notes from the exported CSV file:

```bash
python3 scripts/sync_csv_to_db.py
```

## Verify sellable status

```bash
# Count blocked products
sqlite3 database.db "SELECT COUNT(*) FROM products WHERE sellable = 0"

# List blocked products
sqlite3 database.db "SELECT brand, model, asin, variant_label FROM products WHERE sellable = 0"

# Check specific ASIN
sqlite3 database.db "SELECT asin, sellable, notes FROM products WHERE asin = 'B06XY3WRVQ'"
```

## Important: Commit database before running Docker

To prevent git sync from overwriting local database changes:

```bash
git add -f database.db && git commit -m "Update database" && git push
```
