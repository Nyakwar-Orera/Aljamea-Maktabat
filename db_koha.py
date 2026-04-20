# db_koha.py — MULTI-CAMPUS EDITION v3.0
"""
Multi-Campus Koha Connection Manager.

Each campus branch (AJSN, AJSS, AJSK, AJSM, AJSI) has its own
MySQL connection pool, initialized lazily on first access.

Usage:
    from db_koha import get_branch_conn, get_conn
    
    # Get connection for a specific branch
    conn = get_branch_conn("AJSN")
    
    # Get connection for default branch (AJSN / env-configured)
    conn = get_conn()
"""
import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool
from config import Config
from contextlib import contextmanager
import logging
import os
import threading

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# OFFLINE / DEV MODE  (set KOHA_OFFLINE=true)
# ─────────────────────────────────────────────
_OFFLINE = os.getenv("KOHA_OFFLINE", "false").strip().lower() in ("true", "1", "yes")

if _OFFLINE:
    logger.warning("⚠️  KOHA_OFFLINE=true — running without Koha database (dev/laptop mode)")


# ─────────────────────────────────────────────
# MOCK OBJECTS (for offline/dev mode)
# ─────────────────────────────────────────────
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


# ─────────────────────────────────────────────
# MULTI-CAMPUS CONNECTION POOL REGISTRY
# ─────────────────────────────────────────────
_pools: dict = {}
_pools_lock = threading.Lock()
_branch_locks: dict = {}

def _get_branch_lock(branch_code: str):
    with _pools_lock:
        if branch_code not in _branch_locks:
            _branch_locks[branch_code] = threading.Lock()
        return _branch_locks[branch_code]

def _create_pool(branch_code: str) -> MySQLConnectionPool | None:
    """
    Create and return a MySQL connection pool for the given branch.
    Returns None if configuration is incomplete or connection fails.
    """
    branch_cfg = Config.CAMPUS_REGISTRY.get(branch_code)
    if not branch_cfg:
        logger.error(f"Unknown branch code: {branch_code}")
        return None

    host = branch_cfg.get("koha_host", "")
    user = branch_cfg.get("koha_user", "")
    password = branch_cfg.get("koha_pass", "")
    database = branch_cfg.get("koha_db", "")

    if not host or not database:
        logger.warning(
            f"⚠️  Branch {branch_code}: Koha DB not configured "
            f"(host={host!r}, db={database!r}). This branch will run offline."
        )
        return None

    try:
        pool = MySQLConnectionPool(
            pool_name=f"koha_{branch_code.lower()}",
            pool_size=10,  # Increased to prevent exhaustion during heavy dashboard loads
            host=host,
            user=user,
            password=password,
            database=database,
            charset="utf8mb4",
            autocommit=True,
            connect_timeout=10, # Longer wait for a connection slot
        )
        logger.info(f"✅ Koha pool initialized for branch {branch_code} ({host}/{database})")
        return pool
    except Exception as e:
        logger.error(f"❌ Failed to initialize Koha pool for {branch_code}: {e}")
        return None


def _get_pool(branch_code: str) -> MySQLConnectionPool | None:
    """
    Get or lazily create a connection pool for the given branch.
    Thread-safe via lock.
    """
    if _OFFLINE:
        return None

    branch_lock = _get_branch_lock(branch_code)
    with branch_lock:
        if branch_code not in _pools:
            _pools[branch_code] = _create_pool(branch_code)
        return _pools[branch_code]


def get_branch_conn(branch_code: str):
    """
    Get a MySQL connection for the specified branch.
    Returns a MockConnection if offline or pool unavailable.
    """
    if _OFFLINE:
        return _MockConnection()

    pool = _get_pool(branch_code)
    if not pool:
        logger.warning(f"No pool for {branch_code} — returning mock connection")
        return _MockConnection()

    try:
        return pool.get_connection()
    except Exception as e:
        logger.error(f"Failed to get connection from {branch_code} pool: {e}")
        return _MockConnection()


def is_branch_online(branch_code: str) -> bool:
    """Check if a branch's Koha database is reachable."""
    if _OFFLINE:
        return False
    pool = _get_pool(branch_code)
    if not pool:
        return False
    try:
        conn = pool.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        return True
    except Exception:
        return False


def get_branch_status() -> dict:
    """
    Return connectivity status for all configured branches.
    Returns dict: {branch_code: True/False}
    """
    status = {}
    for code, cfg in Config.CAMPUS_REGISTRY.items():
        if not cfg.get("active", False):
            status[code] = None  # Not configured
        else:
            status[code] = is_branch_online(code)
    return status


# ─────────────────────────────────────────────
# DEFAULT CONNECTION (AJSN / env-configured)
# ─────────────────────────────────────────────

# Initialize the primary Nairobi pool on module load (backward compat)
_primary_pool: MySQLConnectionPool | None = None

if not _OFFLINE:
    try:
        _primary_pool = MySQLConnectionPool(
            pool_name="koha_pool",
            pool_size=10,
            host=Config.KOHA_DB_HOST,
            user=Config.KOHA_DB_USER,
            password=Config.KOHA_DB_PASS,
            database=Config.KOHA_DB_NAME,
            charset="utf8mb4",
            autocommit=True,
        )
        logger.info("✅ Primary Koha pool (AJSN) initialized")
    except Exception as e:
        logger.error(f"Failed to initialize primary Koha connection pool: {e}")
        _primary_pool = None


def get_conn():
    """
    Smart connection selector.
    If within a Flask request context and session has 'branch_code',
    returns connection to that branch. Otherwise, returns primary AJSN pool.
    """
    if _OFFLINE:
        return _MockConnection()

    # Try session-based routing for multi-campus support
    try:
        from flask import session as _flask_session, has_request_context as _has_request_context
        if _has_request_context():
            # Honor branch from session (assigned at login or by Head Office)
            bc = _flask_session.get("branch_code")
            if bc in Config.CAMPUS_REGISTRY:
                return get_branch_conn(bc)
    except (RuntimeError, ImportError):
        pass

    # Fallback to primary pool (AJSN)
    if not _primary_pool:
        # Check if we should lazily create it for AJSN if not already
        pool = _get_pool("AJSN")
        if pool:
            return pool.get_connection()
        return _MockConnection()

    try:
        return _primary_pool.get_connection()
    except Exception as e:
        logger.error(f"Failed to get connection from primary pool: {e}")
        return _MockConnection()


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


@contextmanager
def branch_conn(branch_code: str):
    """Context manager for branch-specific Koha connections."""
    conn = None
    try:
        conn = get_branch_conn(branch_code)
        yield conn
    finally:
        if conn and not isinstance(conn, _MockConnection):
            try:
                conn.close()
            except Exception as e:
                logger.error(f"Error closing branch {branch_code} connection: {e}")
