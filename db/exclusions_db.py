import sqlite3
import os
from typing import List

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.abspath(os.getenv("DB_PATH", os.path.join(REPO_ROOT, "database.db")))

# Make sure the parent directory exists (useful if DB_PATH points to /data/ in Docker)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


SELLERS_TABLE = "excluded_sellers"
LEGACY_KEYWORDS_TABLE = "excluded_keywords"
CANON_KEYWORDS_TABLE = "canon_excluded_keywords"
XEROX_KEYWORDS_TABLE = "xerox_excluded_keywords"
LEXMARK_KEYWORDS_TABLE = "lexmark_excluded_keywords"

def get_db_connection():
    return sqlite3.connect(DB_PATH)

def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    return cur.fetchone() is not None

def _migrate_legacy_keywords_table(conn: sqlite3.Connection) -> None:
    if _table_exists(conn, CANON_KEYWORDS_TABLE):
        return
    if not _table_exists(conn, LEGACY_KEYWORDS_TABLE):
        return
    conn.execute(
        f"ALTER TABLE {LEGACY_KEYWORDS_TABLE} RENAME TO {CANON_KEYWORDS_TABLE}"
    )

def init_exclusions_db():
    conn = get_db_connection()
    with conn:
        _migrate_legacy_keywords_table(conn)
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {SELLERS_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )
        ''')
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {CANON_KEYWORDS_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phrase TEXT UNIQUE NOT NULL
            )
        ''')
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {XEROX_KEYWORDS_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phrase TEXT UNIQUE NOT NULL
            )
        ''')
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {LEXMARK_KEYWORDS_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phrase TEXT UNIQUE NOT NULL
            )
        ''')
    conn.close()

def add_seller(name: str):
    conn = get_db_connection()
    with conn:
        conn.execute(f'INSERT OR IGNORE INTO {SELLERS_TABLE} (name) VALUES (?)', (name,))
    conn.close()

def remove_seller(name: str):
    conn = get_db_connection()
    with conn:
        conn.execute(f'DELETE FROM {SELLERS_TABLE} WHERE name = ?', (name,))
    conn.close()

def list_sellers() -> List[str]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f'SELECT name FROM {SELLERS_TABLE}')
    sellers = [row[0] for row in cur.fetchall()]
    conn.close()
    return sellers

def add_canon_keyword(phrase: str):
    conn = get_db_connection()
    with conn:
        conn.execute(
            f'INSERT OR IGNORE INTO {CANON_KEYWORDS_TABLE} (phrase) VALUES (?)',
            (phrase,),
        )
    conn.close()

def remove_canon_keyword(phrase: str):
    conn = get_db_connection()
    with conn:
        conn.execute(
            f'DELETE FROM {CANON_KEYWORDS_TABLE} WHERE phrase = ?', (phrase,)
        )
    conn.close()

def list_canon_keywords() -> List[str]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f'SELECT phrase FROM {CANON_KEYWORDS_TABLE}')
    keywords = [row[0] for row in cur.fetchall()]
    conn.close()
    return keywords

def add_xerox_keyword(phrase: str):
    conn = get_db_connection()
    with conn:
        conn.execute(
            f'INSERT OR IGNORE INTO {XEROX_KEYWORDS_TABLE} (phrase) VALUES (?)',
            (phrase,),
        )
    conn.close()

def remove_xerox_keyword(phrase: str):
    conn = get_db_connection()
    with conn:
        conn.execute(
            f'DELETE FROM {XEROX_KEYWORDS_TABLE} WHERE phrase = ?', (phrase,)
        )
    conn.close()

def list_xerox_keywords() -> List[str]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f'SELECT phrase FROM {XEROX_KEYWORDS_TABLE}')
    keywords = [row[0] for row in cur.fetchall()]
    conn.close()
    return keywords

def add_lexmark_keyword(phrase: str):
    conn = get_db_connection()
    with conn:
        conn.execute(
            f'INSERT OR IGNORE INTO {LEXMARK_KEYWORDS_TABLE} (phrase) VALUES (?)',
            (phrase,),
        )
    conn.close()

def remove_lexmark_keyword(phrase: str):
    conn = get_db_connection()
    with conn:
        conn.execute(
            f'DELETE FROM {LEXMARK_KEYWORDS_TABLE} WHERE phrase = ?', (phrase,)
        )
    conn.close()

def list_lexmark_keywords() -> List[str]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f'SELECT phrase FROM {LEXMARK_KEYWORDS_TABLE}')
    keywords = [row[0] for row in cur.fetchall()]
    conn.close()
    return keywords

# Ensure tables exist (and migrations run) as soon as the module is imported.
init_exclusions_db()
