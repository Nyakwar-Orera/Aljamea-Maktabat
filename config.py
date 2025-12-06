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
    KOHA_DB_PASS = os.getenv("KOHA_DB_PASS", os.getenv("DB_PASS", "Library@5152"))
    KOHA_DB_NAME = os.getenv("KOHA_DB_NAME", os.getenv("DB_NAME", "koha_library"))

    # ---- Koha OPAC Base URL ----
    KOHA_OPAC_BASE_URL = os.getenv("KOHA_OPAC_BASE_URL", "https://library-opac.ajsn.co.ke")

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
    SESSION_COOKIE_SECURE = _get_bool("SESSION_COOKIE_SECURE", "False")

    # ---- Profile picture uploads ----
    PROFILE_UPLOAD_FOLDER = os.path.join(BASE_DIR, "static", "images", "profiles")
    ALLOWED_PROFILE_EXTENSIONS = {"png", "jpg", "jpeg", "gif"}
