import sqlite3
from config.paths import DB_FILE
from config.logger import log

def get_db_connection():
    try:
        conn = sqlite3.connect(DB_FILE, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn
    except Exception as e:
        log(f"DB CONNECTION FAILED: {e}")
        raise

def close_db_connection(conn):
    try:
        if conn:
            conn.close()
    except Exception as e:
        log(f"DB CLOSE FAILED: {e}")