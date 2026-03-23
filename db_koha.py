# db_koha.py
import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool
from config import Config
from contextlib import contextmanager
import logging
import os

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# OFFLINE / DEV MODE  (set KOHA_OFFLINE=true)
# Lets the app run without a Koha DB connection.
# All Koha queries return empty results gracefully.
# ─────────────────────────────────────────────
_OFFLINE = os.getenv("KOHA_OFFLINE", "false").strip().lower() in ("true", "1", "yes")

if _OFFLINE:
    logger.warning("⚠️  KOHA_OFFLINE=true — running without Koha database (dev/laptop mode)")
    _pool = None
else:
    # Increased pool size to 20 for better handling of concurrent requests
    try:
        _pool = MySQLConnectionPool(
            pool_name="koha_pool",
            pool_size=20,
            host=Config.KOHA_DB_HOST,
            user=Config.KOHA_DB_USER,
            password=Config.KOHA_DB_PASS,
            database=Config.KOHA_DB_NAME,
            charset="utf8mb4",
            autocommit=True,
        )
    except Exception as e:
        logger.error(f"Failed to initialize Koha connection pool: {e}")
        _pool = None


from collections import defaultdict

class _MockRow(defaultdict):
    """A dictionary that returns 0 for any missing key."""
    def __init__(self):
        super().__init__(int)
    
    def __getitem__(self, key):
        return 0

class _MockCursor:
    """Fake cursor that returns empty results for all queries."""
    def __init__(self, dictionary=True):
        self._dictionary = dictionary
        self.rowcount = 0
        self.lastrowid = None

    def execute(self, *a, **kw): pass
    def executemany(self, *a, **kw): pass

    def fetchone(self):
        return _MockRow() if self._dictionary else (0,)

    def fetchall(self):
        return []

    def fetchmany(self, *a, **kw):
        return []

    def close(self): pass
    def __iter__(self): return iter([])
    def __enter__(self): return self
    def __exit__(self, *a): pass


class _MockConnection:
    """Fake MySQL connection used in offline / dev mode."""
    def cursor(self, dictionary=True, **kw):
        return _MockCursor(dictionary=dictionary)

    def commit(self): pass
    def rollback(self): pass
    def close(self): pass
    def __enter__(self): return self
    def __exit__(self, *a): pass


def get_conn():
    """Get a pooled MySQL connection to Koha DB (or a mock if offline)."""
    if _OFFLINE or not _pool:
        if _OFFLINE:
            return _MockConnection()
        raise Exception(
            "Koha connection pool not initialized. "
            "Set KOHA_OFFLINE=true in .env to run without Koha."
        )
    return _pool.get_connection()


# ✅ Alias for backward compatibility
def get_koha_conn():
    """Alias to match other parts of the app that expect get_koha_conn."""
    return get_conn()


@contextmanager
def koha_conn():
    """Context manager for Koha DB connections to ensure they are always closed."""
    conn = None
    try:
        conn = get_conn()
        yield conn
    finally:
        if conn and not isinstance(conn, _MockConnection):
            try:
                conn.close()
            except Exception as e:
                logger.error(f"Error closing Koha connection: {e}")
