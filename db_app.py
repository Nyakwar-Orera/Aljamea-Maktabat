# db_app.py
import sqlite3
from config import Config

def get_appdata_conn():
    """Return connection to local appdata.db (users, mappings, settings)."""
    conn = sqlite3.connect(Config.APP_SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# Alias for backward compatibility
def get_conn():
    return get_appdata_conn()
