import os

class Config:
    # ---- Flask / Security ----
    SECRET_KEY = os.environ.get("SECRET_KEY", "dev_secret_key_change_me")

    # ---- Admin login ----
    ADMIN_USER = os.environ.get("ADMIN_USER", "admin")
    ADMIN_PASS = os.environ.get("ADMIN_PASS", "adminpass")

    # ---- Email (Flask-Mail) ----
    MAIL_SERVER = os.environ.get("MAIL_SERVER", "smtp.gmail.com")
    MAIL_PORT = int(os.environ.get("MAIL_PORT", "587"))
    MAIL_USE_TLS = os.environ.get("MAIL_USE_TLS", "True") == "True"
    MAIL_USE_SSL = os.environ.get("MAIL_USE_SSL", "False") == "True"
    MAIL_USERNAME = os.environ.get("MAIL_USERNAME", "")
    MAIL_PASSWORD = os.environ.get("MAIL_PASSWORD", "")

    # ✅ Default sender & behavior
    MAIL_DEFAULT_SENDER = (
        os.environ.get("MAIL_DEFAULT_NAME", "Maktabat al-Jamea"),
        os.environ.get("MAIL_USERNAME", ""),
    )
    MAIL_SUPPRESS_SEND = os.environ.get("MAIL_SUPPRESS_SEND", "False") == "True"

    ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", MAIL_USERNAME)
    ADMINS = [a.strip() for a in os.environ.get("ADMINS", ADMIN_EMAIL).split(",")]

    # ---- Koha DB (read-only user) ----
    KOHA_DB_HOST = os.environ.get("DB_HOST", "197.211.6.51")
    KOHA_DB_USER = os.environ.get("DB_USER", "library_read")
    KOHA_DB_PASS = os.environ.get("DB_PASS", "Library@5152")
    KOHA_DB_NAME = os.environ.get("DB_NAME", "koha_library")

    # ---- Local App DB ----
    APP_SQLITE_PATH = os.environ.get(
        "APP_SQLITE_PATH",
        os.path.join(os.path.dirname(__file__), "appdata.db")
    )

    # ---- Scheduler defaults ----
    REPORT_SEND_DAY = int(os.environ.get("REPORT_SEND_DAY", "1"))
    REPORT_SEND_HOUR = int(os.environ.get("REPORT_SEND_HOUR", "8"))
    REPORT_SEND_MINUTE = int(os.environ.get("REPORT_SEND_MINUTE", "0"))

    # ✅ APScheduler config
    SCHEDULER_API_ENABLED = True
    SCHEDULER_TIMEZONE = os.environ.get("TZ", "Africa/Nairobi")

    # ---- UI ----
    PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "25"))
