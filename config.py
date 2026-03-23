# config.py
import os
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ✅ Ensure .env is loaded immediately when config.py is imported
load_dotenv()


def _get_bool(name: str, default: str = "False") -> bool:
    """Read an environment variable as a boolean."""
    value = os.getenv(name, default)
    return str(value).strip().lower() in ("true", "1", "yes", "y")


class Config:
    # ---- Flask / Security ----
    SECRET_KEY = os.getenv("SECRET_KEY", "dev_secret_key_change_me")

    # ---- Admin login ----
    ADMIN_USER = os.getenv("ADMIN_USER", "admin")
    ADMIN_PASS = os.getenv("ADMIN_PASS", "adminpass")

    # ---- Email (Flask-Mail) ----
    MAIL_SERVER = os.getenv("MAIL_SERVER", "smtp.gmail.com")
    MAIL_PORT = int(os.getenv("MAIL_PORT", "587"))
    MAIL_USE_TLS = _get_bool("MAIL_USE_TLS", "True")
    MAIL_USE_SSL = _get_bool("MAIL_USE_SSL", "False")
    MAIL_USERNAME = os.getenv("MAIL_USERNAME", "")
    MAIL_PASSWORD = os.getenv("MAIL_PASSWORD", "")

    MAIL_DEFAULT_SENDER = os.getenv(
        "MAIL_DEFAULT_SENDER",
        f"Maktabat al-Jamea <{MAIL_USERNAME}>",
    )
    MAIL_SUPPRESS_SEND = _get_bool("MAIL_SUPPRESS_SEND", "False")

    ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", MAIL_USERNAME)
    ADMINS = [a.strip() for a in os.getenv("ADMINS", ADMIN_EMAIL).split(",")]

    # ---- Koha DB (read-only user) ----
    KOHA_DB_HOST = os.getenv("KOHA_DB_HOST", os.getenv("DB_HOST", "197.211.6.51"))
    KOHA_DB_USER = os.getenv("KOHA_DB_USER", os.getenv("DB_USER", "library_read"))
    KOHA_DB_PASS = os.getenv("KOHA_DB_PASS", os.getenv("DB_PASS", ""))
    KOHA_DB_NAME = os.getenv("KOHA_DB_NAME", os.getenv("DB_NAME", "koha_library"))

    # ---- Koha OPAC Base URL ----
    KOHA_OPAC_BASE_URL = os.getenv("KOHA_OPAC_BASE_URL", "https://library-nairobi.jameasaifiyah.org")

    # ---- Local App DB ----
    APP_SQLITE_PATH = os.getenv(
        "APP_SQLITE_PATH",
        os.path.join(BASE_DIR, "appdata.db"),
    )

    # ---- Scheduler defaults ----
    REPORT_SEND_DAY = int(os.getenv("REPORT_SEND_DAY", "1"))
    REPORT_SEND_HOUR = int(os.getenv("REPORT_SEND_HOUR", "8"))
    REPORT_SEND_MINUTE = int(os.getenv("REPORT_SEND_MINUTE", "0"))

    # ✅ APScheduler config
    SCHEDULER_API_ENABLED = True
    SCHEDULER_TIMEZONE = os.getenv("TZ", "Africa/Nairobi")

    # ---- UI ----
    PAGE_SIZE = int(os.getenv("PAGE_SIZE", "25"))

    # ---- Session / cookie security ----
    SESSION_COOKIE_HTTPONLY = True
    REMEMBER_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = os.getenv("SESSION_COOKIE_SAMESITE", "Lax")
    # In production, set SESSION_COOKIE_SECURE to True
    SESSION_COOKIE_SECURE = _get_bool("SESSION_COOKIE_SECURE", "False")

    # ---- Campus Branches ----
    # Institutional branches for filtering and mark adjustments.
    CAMPUS_BRANCHES = ["Nairobi", "Mombasa", "Dar es Salaam", "Surat", "Karachi", "Mumbai", "Global"]

    # ---- Profile picture uploads ----
    PROFILE_UPLOAD_FOLDER = os.path.join(BASE_DIR, "static", "images", "profiles")

    # ---- Academic Year (Dynamic) ----
    @classmethod
    def CURRENT_ACADEMIC_YEAR(cls):
        """
        Dynamically calculate the current Hijri Academic Year.
        The year starts April 1st. Example: April 2025 -> 1447 H
        """
        from datetime import date
        today = date.today()
        # Hijri year offset (Approximate but institutional standard for 1447/1448)
        # 2025 Apr -> 1447 H, 2026 Apr -> 1448 H etc.
        hijri_offset = 1422 
        year = today.year + hijri_offset - 2000 + 25 # Institutional specific mapping
        
        # Simple Logic: If before April, we are in the previous AY
        if today.month < 4:
            year -= 1
        
        return os.getenv("CURRENT_ACADEMIC_YEAR", f"{year} H")
