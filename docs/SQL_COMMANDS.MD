## Use this to clear database

sqlite3 "/Users/lee/Downloads/Proco Ebay Notifications Telelgram/database.db" "DELETE FROM matches; DELETE FROM messages; DELETE FROM order_history; DELETE FROM purchased_units; DELETE FROM seen_ids; DELETE FROM sqlite_sequence WHERE name IN ('matches', 'messages', 'order_history', 'purchased_units', 'seen_ids'); VACUUM;"
