# db_app.py — FIXED VERSION - Creates fresh connections each time

import sqlite3
import os
from config import Config


def get_appdata_conn():
    """Create a fresh connection to appdata.db with WAL mode enabled."""
    db_path = Config.APP_SQLITE_PATH
    if not db_path:
        db_path = os.path.join(os.path.dirname(__file__), 'appdata.db')
    
    # Create a fresh connection each time
    conn = sqlite3.connect(db_path, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def get_conn():
    """Alias for get_appdata_conn(). Creates a fresh connection each call."""
    return get_appdata_conn()


def close_conn(conn):
    """Close a specific connection."""
    if conn:
        try:
            conn.close()
        except Exception:
            pass