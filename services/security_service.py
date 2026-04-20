import time
import sqlite3
from flask import request, current_app
from config import Config

from db_app import get_conn as get_db_conn

class RateLimiter:
    """
    A simple SQLite-based rate limiter for production use.
    Tracks failed attempts by IP and Username.
    """
    def __init__(self, db_path=None):
        self.db_path = db_path or Config.APP_SQLITE_PATH
        self._init_db()

    def _get_conn(self):
        """Unified connection with proper timeout and WAL mode."""
        return get_db_conn(self.db_path)

    def _init_db(self):
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS rate_limits (
                    key TEXT PRIMARY KEY,
                    attempts INTEGER DEFAULT 0,
                    last_attempt REAL
                )
            """)
            conn.commit()

    def is_locked(self, key, max_attempts=5, lock_duration=300):
        """Check if a specific key (IP or Username) is currently blocked."""
        with self._get_conn() as conn:
            row = conn.execute("SELECT attempts, last_attempt FROM rate_limits WHERE key = ?", (key,)).fetchone()
            if not row:
                return False
            
            attempts, last_attempt = row
            if attempts >= max_attempts:
                if time.time() - last_attempt < lock_duration:
                    return True
                else:
                    # Lock expired, reset
                    self.reset(key)
            return False

    def log_attempt(self, key):
        """Increment attempt count for a key."""
        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO rate_limits (key, attempts, last_attempt)
                VALUES (?, 1, ?)
                ON CONFLICT(key) DO UPDATE SET
                    attempts = attempts + 1,
                    last_attempt = excluded.last_attempt
            """, (key, time.time()))
            conn.commit()

    def reset(self, key):
        """Reset attempts for a key (on successful login)."""
        with self._get_conn() as conn:
            conn.execute("DELETE FROM rate_limits WHERE key = ?", (key,))
            conn.commit()

# Singleton instance
limiter = RateLimiter()
