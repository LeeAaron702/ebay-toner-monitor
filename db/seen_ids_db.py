import sqlite3
import time
from typing import Optional

DB_PATH = 'database.db'
SEEN_IDS_TABLE = 'seen_ids'
ROLLING_WINDOW_SEC = 72 * 3600  # 72 hours

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    return conn

def init_db():
    conn = get_db_connection()
    with conn:
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {SEEN_IDS_TABLE} (
                id TEXT PRIMARY KEY,
                seen_ts INTEGER
            )
        ''')
    conn.close()

def add_seen_id(listing_id: str, timestamp: Optional[int] = None):
    if timestamp is None:
        timestamp = int(time.time())
    conn = get_db_connection()
    with conn:
        conn.execute(f'''
            INSERT OR REPLACE INTO {SEEN_IDS_TABLE} (id, seen_ts)
            VALUES (?, ?)
        ''', (listing_id, timestamp))
    conn.close()

def is_id_seen(listing_id: str) -> bool:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f'SELECT 1 FROM {SEEN_IDS_TABLE} WHERE id = ?', (listing_id,))
    result = cur.fetchone()
    conn.close()
    return result is not None

def gc_old_ids():
    cutoff = int(time.time()) - ROLLING_WINDOW_SEC
    conn = get_db_connection()
    with conn:
        conn.execute(f'DELETE FROM {SEEN_IDS_TABLE} WHERE seen_ts < ?', (cutoff,))
    conn.close()
