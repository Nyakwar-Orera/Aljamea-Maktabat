# config.py — MULTI-CAMPUS EDITION v3.0
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
    ADMIN_USER = os.getenv("ADMIN_USER")
    ADMIN_PASS = os.getenv("ADMIN_PASS")

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

    # ---- Koha DB (Nairobi — Primary / Default) ----
    KOHA_DB_HOST = os.getenv("KOHA_DB_HOST", os.getenv("DB_HOST"))
    KOHA_DB_USER = os.getenv("KOHA_DB_USER", os.getenv("DB_USER", "library_read"))
    KOHA_DB_PASS = os.getenv("KOHA_DB_PASS", os.getenv("DB_PASS", ""))
    KOHA_DB_NAME = os.getenv("KOHA_DB_NAME", os.getenv("DB_NAME", "koha_library"))

    # ---- Koha OPAC Base URL (Nairobi default) ----
    KOHA_OPAC_BASE_URL = os.getenv("KOHA_OPAC_BASE_URL", "https://library-nairobi.jameasaifiyah.org")

    # =========================================================
    # 🌐 MULTI-CAMPUS BRANCH REGISTRY
    # Each campus entry maps to its own Koha MySQL instance.
    # Per-branch env vars (e.g., KOHA_AJSS_HOST) override defaults.
    # =========================================================
    CAMPUS_REGISTRY = {
        "AJSN": {
            "code":       "AJSN",
            "name":       "Al-Jamea tus-Saifiyah Nairobi",
            "short_name": "Nairobi",
            "country":    "Kenya",
            "flag":       "🇰🇪",
            "timezone":   "Africa/Nairobi",
            "koha_host":  os.getenv("KOHA_AJSN_HOST", os.getenv("DB_HOST", "")),
            "koha_user":  os.getenv("KOHA_AJSN_USER", os.getenv("DB_USER", "library_read")),
            "koha_pass":  os.getenv("KOHA_AJSN_PASS", os.getenv("DB_PASS", "")),
            "koha_db":    os.getenv("KOHA_AJSN_DB",   os.getenv("DB_NAME", "koha_library")),
            "opac_url":   os.getenv("KOHA_AJSN_OPAC", "https://library-nairobi.jameasaifiyah.org"),
            "active":     True,
            "color":      "#00875A",   # Emerald green
            "icon":       "bi-geo-alt-fill",
            "currency":   "KES",
            "fine_rate":   10,         # KES per day
        },
        "AJSS": {
            "code":       "AJSS",
            "name":       "Al-Jamea tus-Saifiyah Surat",
            "short_name": "Surat",
            "country":    "India",
            "flag":       "🇮🇳",
            "timezone":   "Asia/Kolkata",
            "koha_host":  os.getenv("KOHA_AJSS_HOST", ""),
            "koha_user":  os.getenv("KOHA_AJSS_USER", "library_read"),
            "koha_pass":  os.getenv("KOHA_AJSS_PASS", ""),
            "koha_db":    os.getenv("KOHA_AJSS_DB",   "koha_ajss"),
            "opac_url":   os.getenv("KOHA_AJSS_OPAC", "https://library-surat.jameasaifiyah.org"),
            "active":     _get_bool("AJSS_ACTIVE", "False"),
            "color":      "#0052CC",   # Royal blue
            "icon":       "bi-building",
            "currency":   "INR",
            "fine_rate":   5,          # INR per day
        },
        "AJSK": {
            "code":       "AJSK",
            "name":       "Al-Jamea tus-Saifiyah Karachi",
            "short_name": "Karachi",
            "country":    "Pakistan",
            "flag":       "🇵🇰",
            "timezone":   "Asia/Karachi",
            "koha_host":  os.getenv("KOHA_AJSK_HOST", ""),
            "koha_user":  os.getenv("KOHA_AJSK_USER", "library_read"),
            "koha_pass":  os.getenv("KOHA_AJSK_PASS", ""),
            "koha_db":    os.getenv("KOHA_AJSK_DB",   "koha_ajsk"),
            "opac_url":   os.getenv("KOHA_AJSK_OPAC", "https://library-karachi.jameasaifiyah.org"),
            "active":     _get_bool("AJSK_ACTIVE", "False"),
            "color":      "#403294",   # Deep purple
            "icon":       "bi-bank",
            "currency":   "PKR",
            "fine_rate":   20,         # PKR per day
        },
        "AJSM": {
            "code":       "AJSM",
            "name":       "Al-Jamea tus-Saifiyah Marol",
            "short_name": "Marol",
            "country":    "India",
            "flag":       "🇮🇳",
            "timezone":   "Asia/Kolkata",
            "koha_host":  os.getenv("KOHA_AJSM_HOST", ""),
            "koha_user":  os.getenv("KOHA_AJSM_USER", "library_read"),
            "koha_pass":  os.getenv("KOHA_AJSM_PASS", ""),
            "koha_db":    os.getenv("KOHA_AJSM_DB",   "koha_ajsm"),
            "opac_url":   os.getenv("KOHA_AJSM_OPAC", "https://library-marol.jameasaifiyah.org"),
            "active":     _get_bool("AJSM_ACTIVE", "False"),
            "color":      "#FF5630",   # Coral red
            "icon":       "bi-mortarboard",
            "currency":   "INR",
            "fine_rate":   5,          # INR per day
        },
        "AJSI": {
            "code":       "AJSI",
            "name":       "Al-Jamea tus-Saifiyah Sidhpur",
            "short_name": "Sidhpur",
            "country":    "India",
            "flag":       "🇮🇳",
            "timezone":   "Asia/Kolkata",
            "koha_host":  os.getenv("KOHA_AJSI_HOST", ""),
            "koha_user":  os.getenv("KOHA_AJSI_USER", "library_read"),
            "koha_pass":  os.getenv("KOHA_AJSI_PASS", ""),
            "koha_db":    os.getenv("KOHA_AJSI_DB",   "koha_ajsi"),
            "opac_url":   os.getenv("KOHA_AJSI_OPAC", "https://library-sidhpur.jameasaifiyah.org"),
            "active":     _get_bool("AJSI_ACTIVE", "False"),
            "color":      "#FF8B00",   # Amber
            "icon":       "bi-journal-bookmark",
            "currency":   "INR",
            "fine_rate":   5,          # INR per day
        },
    }

    # ---- Branch ordered list (for display) ----
    BRANCH_ORDER = ["AJSN", "AJSS", "AJSK", "AJSM", "AJSI"]

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

    # ---- Legacy Campus Branches (kept for backward compat) ----
    CAMPUS_BRANCHES = ["AJSN", "AJSS", "AJSK", "AJSM", "AJSI", "Global"]

    # ---- Profile picture uploads ----
    PROFILE_UPLOAD_FOLDER = os.path.join(BASE_DIR, "static", "images", "profiles")

    # ---- Academic Year (Dynamic) ----
    @classmethod
    def CURRENT_ACADEMIC_YEAR(cls):
        """
        Dynamically calculate the current Hijri Academic Year.
        The AY starts on 1st Shawwal (month 10) of the Hijri year.
        """
        from datetime import date
        today = date.today()

        try:
            from hijri_converter import convert
            h_today = convert.Gregorian(today.year, today.month, today.day).to_hijri()
            hijri_year = h_today.year
            if h_today.month < 10:
                hijri_year -= 1
            return os.getenv("CURRENT_ACADEMIC_YEAR", f"{hijri_year} H")
        except Exception:
            if today >= date(2026, 3, 20):
                return os.getenv("CURRENT_ACADEMIC_YEAR", "1447 H")
            elif today >= date(2025, 3, 21):
                return os.getenv("CURRENT_ACADEMIC_YEAR", "1446 H")
            else:
                return os.getenv("CURRENT_ACADEMIC_YEAR", "1445 H")

    @classmethod
    def CLEAN_ACADEMIC_YEAR(cls):
        """Standardized version without H or spaces."""
        ay = cls.CURRENT_ACADEMIC_YEAR()
        if not ay: return ""
        return str(ay).replace('H', '').strip()

    @classmethod
    def get_branch_config(cls, branch_code: str) -> dict:
        """Get configuration for a specific branch. Returns None if not found."""
        return cls.CAMPUS_REGISTRY.get(branch_code)

    @classmethod
    def get_active_branches(cls) -> list:
        """Return list of active branch configs in display order."""
        return [
            cls.CAMPUS_REGISTRY[code]
            for code in cls.BRANCH_ORDER
            if code in cls.CAMPUS_REGISTRY and cls.CAMPUS_REGISTRY[code].get("active", False)
        ]
