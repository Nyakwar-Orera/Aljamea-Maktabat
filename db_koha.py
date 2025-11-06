import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool
from config import Config

# Small connection pool (read-only user)
_pool = MySQLConnectionPool(
    pool_name="koha_pool",
    pool_size=5,
    host=Config.KOHA_DB_HOST,
    user=Config.KOHA_DB_USER,
    password=Config.KOHA_DB_PASS,
    database=Config.KOHA_DB_NAME,
    charset="utf8mb4",
    autocommit=True,
)

def get_conn():
    """Get a pooled MySQL connection to Koha DB."""
    return _pool.get_connection()

# ✅ Alias for backward compatibility
def get_koha_conn():
    """Alias to match other parts of the app that expect get_koha_conn."""
    return get_conn()
