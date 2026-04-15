# services/koha_queries.py - COMPLETELY UPDATED WITH WEEKLY TREND SUPPORT AND CACHING

from typing import Dict, List, Optional, Tuple, Any, Union
from contextlib import contextmanager
from db_koha import get_conn, get_koha_conn 
import os
from datetime import date, datetime, timedelta
import re
import logging
from functools import lru_cache
from time import time

logger = logging.getLogger(__name__)

try:
    from hijri_converter import convert as hijri_convert
except ImportError:
    hijri_convert = None

# ---------- Simple Cache Decorator ----------
HIJRI_MONTHS = [
    "Muḥarram al-Harām", "Safar al-Muzaffar", "Rabi al-Awwal", "Rabī al-Akhar",
    "Jamādil Awwal", "Jamādā al-ʾŪkhrā", "Rajab al-Asab", "Shabān al-Karim",
    "Shehrullah al-Moazzam", "Shawwāl al-Mukarram", "Zilqādah al-Harām", "Zilhijjatil Harām",
]

def get_hijri_month_year_label(d: date) -> str:
    """Safely convert Gregorian date to Hijri month label."""
    try:
        from hijri_converter import convert
        h = convert.Gregorian(d.year, d.month, d.day).to_hijri()
        return f"{HIJRI_MONTHS[h.month-1]} {h.year} H"
    except Exception:
        return d.strftime("%b %Y")

class SimpleCache:
    """Simple time-based cache for function results, now branch-aware."""
    def __init__(self, ttl_seconds=300):
        self.cache = {}
        self.ttl = ttl_seconds
    
    def _get_branch_key(self, key):
        """Prepend branch_code from session to cache key."""
        from flask import session, has_request_context
        # Use session value if in request, else fallback
        bc = "AJSN"
        if has_request_context():
            bc = session.get("branch_code", "AJSN")
        return f"{bc}_{key}"

    def get(self, key):
        full_key = self._get_branch_key(key)
        if full_key in self.cache:
            value, timestamp = self.cache[full_key]
            if time() - timestamp < self.ttl:
                return value
            else:
                del self.cache[full_key]
        return None
    
    def set(self, key, value):
        full_key = self._get_branch_key(key)
        self.cache[full_key] = (value, time())
    
    def clear(self):
        self.cache.clear()

# Initialize caches for different functions
summary_cache = SimpleCache(ttl_seconds=300)  # 5 minutes
trend_cache = SimpleCache(ttl_seconds=300)
marhala_stats_cache = SimpleCache(ttl_seconds=600)  # 10 minutes
top_titles_cache = SimpleCache(ttl_seconds=600)
darajah_cache = SimpleCache(ttl_seconds=300)

# ---------- Connection Context Manager ----------
@contextmanager
def get_db_cursor(dictionary=True):
    """Context manager for database connections to ensure proper cleanup."""
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=dictionary)
        yield cur
        conn.commit()
    finally:
        cur.close()
        conn.close()


# ---------- SQL Loader ----------
_SQL_CACHE = None

def _load_sql_file() -> dict:
    """Load and cache named SQL sections from sql/koha.sql."""
    global _SQL_CACHE
    if _SQL_CACHE is not None:
        return _SQL_CACHE

    here = os.path.dirname(os.path.dirname(__file__))
    path = os.path.join(here, "sql", "koha.sql")

    sections: Dict[str, str] = {}
    key: Optional[str] = None
    buf: List[str] = []

    if not os.path.exists(path):
        _SQL_CACHE = sections
        return sections

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip().lower().startswith("-- name:"):
                if key and buf:
                    sections[key] = "".join(buf).strip()
                    buf = []
                key = line.split(":", 1)[1].strip()
                continue
            buf.append(line)

    if key and buf:
        sections[key] = "".join(buf).strip()

    _SQL_CACHE = sections
    return sections


def sql_named(name: str) -> str:
    """Return SQL text for a named section from sql/koha.sql."""
    q = _load_sql_file().get(name)
    if not q:
        raise KeyError(f"SQL section '{name}' not found in sql/koha.sql")
    return q


# -------------------------------
# ACADEMIC YEAR HELPER
# -------------------------------

def get_available_academic_years() -> List[Dict[str, Any]]:
    """
    Return a list of available Hijri academic years for selection.
    Currently includes 1444, 1445, 1446, and 1447.
    """
    return [
        {"hijri_year": 1447, "label": "1447 H (2026-27)"},
        {"hijri_year": 1446, "label": "1446 H (2025-26)"},
        {"hijri_year": 1445, "label": "1445 H (2024-25)"},
        {"hijri_year": 1444, "label": "1444 H (2023-24)"},
    ]


# Cache for academic year bounds (changes once per day)
_ay_bounds_cache = None
_ay_bounds_timestamp = None

def get_ay_bounds(hijri_year: Optional[int] = None) -> Tuple[Optional[date], Optional[date]]:
    """
    Academic Year follows the Hijri cycle starting from 1st Shawwal.
    Dynamically calculates start/end dates using the Hijri calendar.
    If hijri_year is provided, returns bounds for that specific year.
    """
    global _ay_bounds_cache, _ay_bounds_timestamp
    today = date.today()
    
    # If a specific year is requested, we use a separate caching strategy or no cache for simplicity in this call
    if hijri_year:
        return get_ay_bounds_for_hijri_year(hijri_year)

    if _ay_bounds_cache and _ay_bounds_timestamp:
        if time() - _ay_bounds_timestamp < 86400:
            return _ay_bounds_cache

    try:
        from hijri_converter import convert

        # Convert today to Hijri
        h_today = convert.Gregorian(today.year, today.month, today.day).to_hijri()
        current_hijri_year = h_today.year

        # Get 1st Shawwal of the current Hijri year
        start_g = convert.Hijri(current_hijri_year, 10, 1).to_gregorian()
        start_date = date(start_g.year, start_g.month, start_g.day)

        if today < start_date:
            # Before this year's Shawwal → we're in the PREVIOUS AY
            prev_year = current_hijri_year - 1
            start_g = convert.Hijri(prev_year, 10, 1).to_gregorian()
            start_date = date(start_g.year, start_g.month, start_g.day)
            end_g = convert.Hijri(current_hijri_year, 9, 29).to_gregorian()
            end_date = date(end_g.year, end_g.month, end_g.day)
        else:
            # On or after Shawwal → we're in the CURRENT AY
            next_year = current_hijri_year + 1
            end_g = convert.Hijri(next_year, 9, 29).to_gregorian()
            end_date = date(end_g.year, end_g.month, end_g.day)

        logger.info(f"Hijri AY {current_hijri_year}: {start_date} → {end_date}")
        _ay_bounds_cache = (start_date, end_date)
        _ay_bounds_timestamp = time()
        return _ay_bounds_cache

    except Exception as e:
        logger.error(f"Error calculating Hijri AY bounds: {e}")
        return _fallback_ay_bounds()


def _fallback_ay_bounds() -> Tuple[date, date]:
    """
    Fallback calculation for Academic Year when Hijri conversion fails.
    Uses the hardcoded dates for 1447 H (March 20, 2026 – March 9, 2027 approx).
    """
    today = date.today()
    ay_start = date(2026, 3, 20)
    ay_end = date(2027, 3, 9)

    if today >= ay_start:
        return ay_start, ay_end
    else:
        # Before 1447H: use April-1 based approximate
        yr = today.year if today.month >= 4 else today.year - 1
        return date(yr, 4, 1), date(yr + 1, 3, 31)



# -------------------------------
# HIJRI HELPER FUNCTIONS (Cached)
# -------------------------------

@lru_cache(maxsize=1024)
def _get_hijri_conversion(year: int, month: int, day: int) -> Tuple[str, str]:
    """Cached Hijri conversion for date objects."""
    d = date(year, month, day)
    if not hijri_convert:
        return d.strftime("%B %Y"), d.strftime("%d %B %Y")
    try:
        h = hijri_convert.Gregorian(year, month, day).to_hijri()
        month_label = f"{HIJRI_MONTHS[h.month - 1]} {h.year} H"
        full_label = f"{h.day} {HIJRI_MONTHS[h.month - 1]} {h.year} H"
        return month_label, full_label
    except Exception:
        return d.strftime("%B %Y"), d.strftime("%d %B %Y")


def get_hijri_month_year_label(d: date) -> str:
    """Get professional Hijri month-year label for the given date."""
    if not d:
        return ""
    month_label, _ = _get_hijri_conversion(d.year, d.month, d.day)
    return month_label


def get_hijri_date_label(d: date) -> str:
    """Get full Hijri date label (Day Month Year H)."""
    if not d:
        return ""
    _, full_label = _get_hijri_conversion(d.year, d.month, d.day)
    return full_label


# -------------------------------
# PATRON COUNT QUERIES (Cached)
# -------------------------------

def get_patron_counts(marhala_name: Optional[str] = None) -> Dict[str, int]:
    """
    Get all patron counts in a single optimized query.
    Results are cached for 5 minutes.
    """
    cache_key = f"patron_counts_{marhala_name}"
    cached = summary_cache.get(cache_key)
    if cached is not None:
        return cached
    
    with get_db_cursor() as cur:
        query = """
            SELECT 
                COUNT(*) AS total_all,
                SUM(CASE 
                    WHEN (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                        AND (b.debarred IS NULL OR b.debarred = 0)
                        AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
                    THEN 1 ELSE 0 
                END) AS active_total,
                SUM(CASE 
                    WHEN b.categorycode LIKE 'S%%'
                        AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                        AND (b.debarred IS NULL OR b.debarred = 0)
                        AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
                    THEN 1 ELSE 0 
                END) AS active_students,
                SUM(CASE 
                    WHEN (b.categorycode NOT LIKE 'S%%' OR b.categorycode IS NULL)
                        AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                        AND (b.debarred IS NULL OR b.debarred = 0)
                        AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
                    THEN 1 ELSE 0 
                END) AS active_non_students
            FROM borrowers b
            LEFT JOIN categories c ON b.categorycode = c.categorycode
            WHERE 1=1
        """
        params = []
        if marhala_name:
            query += " AND (c.description = %s OR b.categorycode = %s)"
            params.extend([marhala_name, marhala_name])

        cur.execute(query, params)
        result = cur.fetchone()
        
        # Use active_total as the main total_patrons to "remove bared and expire"
        active_total = int(result.get("active_total") or 0)
        
        output = {
            "total_all": int(result.get("total_all") or 0),
            "total_patrons": active_total, 
            "active_patrons": active_total,
            "expired_patrons": int(result.get("total_all") or 0) - active_total,
            "student_patrons": int(result.get("active_students") or 0),
            "non_student_patrons": int(result.get("active_non_students") or 0),
        }
        
        summary_cache.set(cache_key, output)
        return output

def get_patron_bifurcation(marhala_name: Optional[str] = None) -> Dict[str, Any]:
    """Retrieve count of active, expired, and category-wise patrons."""
    counts = get_patron_counts(marhala_name)
    
    with get_db_cursor() as cur:
        # Category bifurcation (Active vs Expired)
        # All categories regardless of status
        query = """
            SELECT 
                COALESCE(c.description, b.categorycode, 'Other') AS category, 
                COUNT(*) AS count,
                SUM(CASE 
                    WHEN (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                        AND (b.debarred IS NULL OR b.debarred = 0)
                        AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
                    THEN 1 ELSE 0 
                END) AS active_count
            FROM borrowers b
            LEFT JOIN categories c ON b.categorycode = c.categorycode
            WHERE 1=1
        """
        params = []
        if marhala_name:
            query += " AND (c.description = %s OR b.categorycode = %s)"
            params.extend([marhala_name, marhala_name])
        
        query += " GROUP BY category ORDER BY count DESC"
        cur.execute(query, params)
        category_stats = cur.fetchall()
        
        # Student vs Teacher bifurcation (Active)
        query_st = """
            SELECT 
                SUM(CASE WHEN b.categorycode LIKE 'S%%' THEN 1 ELSE 0 END) AS students,
                SUM(CASE WHEN b.categorycode = 'T-KG' THEN 1 ELSE 0 END) AS teachers,
                SUM(CASE WHEN b.categorycode NOT LIKE 'S%%' AND b.categorycode <> 'T-KG' THEN 1 ELSE 0 END) AS others
            FROM borrowers b
            LEFT JOIN categories c ON b.categorycode = c.categorycode
            WHERE (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
        """
        st_params = []
        if marhala_name:
            query_st += " AND (c.description = %s OR b.categorycode = %s)"
            st_params.extend([marhala_name, marhala_name])

        cur.execute(query_st, st_params)
        st_result = cur.fetchone()


    return {
        "counts": counts,
        "category_stats": category_stats,
        "st_bifurcation": {
            "students": int(st_result.get("students") or 0),
            "teachers": int(st_result.get("teachers") or 0),
            "others": int(st_result.get("others") or 0)
        }
    }



def get_total_patrons_count() -> int:
    """Legacy wrapper - get total active patrons count."""
    return get_patron_counts()["active_patrons"]


def get_student_patrons_count() -> int:
    """Legacy wrapper - get active student patrons count."""
    return get_patron_counts()["student_patrons"]


def get_issues_bifurcation(marhala_name: Optional[str] = None, hijri_year: Optional[int] = None) -> Dict[str, Any]:
    """Get detailed issues bifurcation for the 'Books Issued' card popup."""
    start, end = get_ay_bounds(hijri_year)
    if not start:
        return {"category_stats": [], "monthly_stats": []}

    with get_db_cursor() as cur:
        # Category bifurcation
        query = """
            SELECT c.description AS category, COUNT(*) AS count
            FROM statistics s
            JOIN borrowers b ON s.borrowernumber = b.borrowernumber
            LEFT JOIN categories c ON b.categorycode = c.categorycode
            WHERE s.type = 'issue'
              AND DATE(s.`datetime`) BETWEEN %s AND %s
        """
        params = [start, end]
        if marhala_name:
            query += " AND (c.description = %s OR b.categorycode = %s)"
            params.extend([marhala_name, marhala_name])
            
        cur.execute(query + " GROUP BY c.description ORDER BY count DESC", params)
        category_stats = cur.fetchall()

        # Monthly stats (approximated by Gregorian for chart continuity)
        query_m = """
            SELECT DATE_FORMAT(s.`datetime`, '%b %Y') AS month, COUNT(*) AS count
            FROM statistics s
            JOIN borrowers b ON s.borrowernumber = b.borrowernumber
            LEFT JOIN categories c ON b.categorycode = c.categorycode
            WHERE s.type = 'issue'
              AND DATE(s.`datetime`) BETWEEN %s AND %s
        """
        if marhala_name:
            query_m += " AND (c.description = %s OR b.categorycode = %s)"
        
        cur.execute(query_m + " GROUP BY DATE_FORMAT(s.`datetime`, '%Y-%m') ORDER BY MIN(s.`datetime`)", params)
        monthly_stats = cur.fetchall()

    return {
        "category_stats": category_stats,
        "monthly_stats": monthly_stats
    }

def get_fines_bifurcation(marhala_name: Optional[str] = None, hijri_year: Optional[int] = None) -> Dict[str, Any]:
    """Get detailed fines bifurcation for the 'Garamat' card popup."""
    start, end = get_ay_bounds(hijri_year)
    if not start:
        return {"total_fine": 0, "daily_stats": []}

    with get_db_cursor() as cur:
        # 7-day stats
        cur.execute("""
            SELECT DATE(date) AS d, SUM(-amount) AS count
            FROM accountlines
            WHERE credit_type_code = 'PAYMENT'
              AND (status IS NULL OR status <> 'VOID')
              AND DATE(date) >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
            GROUP BY d ORDER BY d
        """)
        daily_stats = cur.fetchall()
        
        # Monthly stats for AY
        query_m = """
            SELECT DATE_FORMAT(date, '%b %Y') AS month, SUM(-amount) AS count
            FROM accountlines al
            JOIN borrowers b ON al.borrowernumber = b.borrowernumber
            LEFT JOIN categories c ON b.categorycode = c.categorycode
            WHERE al.credit_type_code = 'PAYMENT'
              AND (al.status IS NULL OR al.status <> 'VOID')
              AND DATE(al.date) BETWEEN %s AND %s
        """
        params = [start, end]
        if marhala_name:
            query_m += " AND (c.description = %s OR b.categorycode = %s)"
            params.extend([marhala_name, marhala_name])

        cur.execute(query_m + " GROUP BY DATE_FORMAT(date, '%Y-%m') ORDER BY MIN(date)", params)
        monthly_stats = cur.fetchall()

    return {
        "daily_stats": daily_stats,
        "monthly_stats": monthly_stats
    }


# -------------------------------
# DASHBOARD SUMMARY QUERIES (Cached)
# -------------------------------

def get_summary(marhala_name: Optional[str] = None, hijri_year: Optional[int] = None) -> Dict[str, Any]:
    """
    Return library summary stats with optimized single-pass approach.
    Results are cached for 5 minutes.
    """
    cache_key = f"summary_{marhala_name}_{hijri_year}"
    cached = summary_cache.get(cache_key)
    if cached is not None:
        return cached
    
    patron_counts = get_patron_counts(marhala_name)
    start, end = get_ay_bounds(hijri_year)
    
    with get_db_cursor() as cur:
        # Get total titles count
        cur.execute("SELECT COUNT(*) AS c FROM biblio")
        total_titles_all = int(cur.fetchone()["c"] or 0)
        
        # Get overdue count (Filtered by AY and Marhala)
        overdue_query = """
            SELECT COUNT(*) AS c
            FROM issues i
            JOIN borrowers b ON i.borrowernumber = b.borrowernumber
            LEFT JOIN categories c ON b.categorycode = c.categorycode
            WHERE i.returndate IS NULL 
              AND i.date_due < CURDATE()
              AND DATE(i.issuedate) BETWEEN %s AND %s
              AND b.categorycode LIKE 'S%%'
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
        """
        overdue_params = [start, end]
        if marhala_name:
            overdue_query += " AND (c.description = %s OR b.categorycode = %s)"
            overdue_params.extend([marhala_name, marhala_name])
        
        cur.execute(overdue_query, overdue_params)
        overdue = int(cur.fetchone()["c"] or 0)
        
        # 21-DAY GRACE PERIOD: If AY started < 21 days ago, ignore overdues
        # as it's the beginning of the institutional calendar.
        if start and (date.today() - start).days < 21:
            overdue = 0
        
        # Get currently issued count (Filtered by AY and Marhala)
        issued_query = """
            SELECT COUNT(*) AS c
            FROM issues i
            JOIN borrowers b ON i.borrowernumber = b.borrowernumber
            LEFT JOIN categories c ON b.categorycode = c.categorycode
            WHERE i.returndate IS NULL
              AND DATE(i.issuedate) BETWEEN %s AND %s
              AND b.categorycode LIKE 'S%%'
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
        """
        issued_params = [start, end]
        if marhala_name:
            issued_query += " AND (c.description = %s OR b.categorycode = %s)"
            issued_params.extend([marhala_name, marhala_name])

        cur.execute(issued_query, issued_params)
        currently_issued = int(cur.fetchone()["c"] or 0)
        
        # Initialize AY metrics
        active_patrons_ay = 0
        total_issues = 0
        total_titles_issued = 0
        fees_paid = 0.0
        
        # Get AY metrics if AY has started
        if start and end:
            # Active patrons in AY
            active_q = """
                SELECT COUNT(DISTINCT s.borrowernumber) AS c
                FROM statistics s
                JOIN borrowers b ON s.borrowernumber = b.borrowernumber
                LEFT JOIN categories c ON b.categorycode = c.categorycode
                WHERE s.type = 'issue'
                  AND DATE(s.`datetime`) BETWEEN %s AND %s
                  AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                  AND (b.debarred IS NULL OR b.debarred = 0)
                  AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            """
            active_p = [start, end]
            if marhala_name:
                active_q += " AND (c.description = %s OR b.categorycode = %s)"
                active_p.extend([marhala_name, marhala_name])
            
            cur.execute(active_q, active_p)
            active_patrons_ay = int(cur.fetchone()["c"] or 0)
            
            # Total issues in AY (Total Loans count as requested: "use issues count not the books")
            issues_q = """
                SELECT COUNT(*) AS c
                FROM statistics s
                JOIN borrowers b ON s.borrowernumber = b.borrowernumber
                LEFT JOIN categories c ON b.categorycode = c.categorycode
                WHERE s.type = 'issue'
                  AND DATE(s.`datetime`) BETWEEN %s AND %s
                  AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                  AND (b.debarred IS NULL OR b.debarred = 0)
                  AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            """
            issues_p = [start, end]
            if marhala_name:
                issues_q += " AND (c.description = %s OR b.categorycode = %s)"
                issues_p.extend([marhala_name, marhala_name])
                
            cur.execute(issues_q, issues_p)
            total_issues = int(cur.fetchone()["c"] or 0)
            
            # Distinct titles issued in AY
            titles_q = """
                SELECT COUNT(DISTINCT it.biblionumber) AS c
                FROM statistics s
                JOIN items it ON s.itemnumber = it.itemnumber
                JOIN borrowers b ON s.borrowernumber = b.borrowernumber
                LEFT JOIN categories c ON b.categorycode = c.categorycode
                WHERE s.type = 'issue'
                  AND DATE(s.`datetime`) BETWEEN %s AND %s
            """
            titles_p = [start, end]
            if marhala_name:
                titles_q += " AND (c.description = %s OR b.categorycode = %s)"
                titles_p.extend([marhala_name, marhala_name])

            cur.execute(titles_q, titles_p)
            total_titles_issued = int(cur.fetchone()["c"] or 0)
            
            # Fees paid in AY
            fees_q = """
                SELECT COALESCE(SUM(
                    CASE
                        WHEN al.credit_type_code = 'PAYMENT'
                             AND (al.status IS NULL OR al.status <> 'VOID')
                             AND DATE(al.`date`) BETWEEN %s AND %s
                        THEN -al.amount
                        ELSE 0
                    END
                ), 0) AS fees_paid
                FROM accountlines al
                JOIN borrowers b ON al.borrowernumber = b.borrowernumber
                LEFT JOIN categories c ON b.categorycode = c.categorycode
            """
            fees_p = [start, end]
            if marhala_name:
                fees_q += " WHERE (c.description = %s OR b.categorycode = %s)"
                fees_p.extend([marhala_name, marhala_name])
                
            cur.execute(fees_q, fees_p)
            fees_paid = float(cur.fetchone()["fees_paid"] or 0.0)

    result = {
        "active_patrons": patron_counts["active_patrons"],
        "total_patrons": patron_counts["total_patrons"],
        "student_patrons": patron_counts["student_patrons"],
        "non_student_patrons": patron_counts["non_student_patrons"],
        "active_patrons_ay": active_patrons_ay,
        "total_titles": total_titles_all,
        "total_titles_issued": total_titles_issued,
        "total_issues": total_issues,
        "overdue": overdue,
        "currently_issued": currently_issued,
        "fees_paid": fees_paid,
    }
    
    summary_cache.set(cache_key, result)
    return result


def get_summary_with_updated_terms() -> Dict[str, Any]:
    """Legacy wrapper - summary with currently_issued already included."""
    return get_summary()


# -------------------------------
# DARAJAH GROUP CONSTANTS
# -------------------------------

DARAJAH_GROUPS = {
    "Darajah 1–2": (1, 2),
    "Darajah 3–4": (3, 4),
    "Darajah 5–7": (5, 7),
    "Darajah 8–11": (8, 11),
}

DARAJAH_GROUP_ORDER = {
    "Darajah 1–2": 1,
    "Darajah 3–4": 2,
    "Darajah 5–7": 3,
    "Darajah 8–11": 4,
    "Unassigned": 5,
    "Other": 6,
}


def get_darajah_group_from_std(std_attr: Optional[str]) -> str:
    """Convert STD attribute to Darajah group name."""
    if not std_attr:
        return "Unassigned"
    
    match = re.search(r'\d+', str(std_attr))
    if not match:
        return "Unassigned"
    
    n = int(match.group())
    
    for group_name, (low, high) in DARAJAH_GROUPS.items():
        if low <= n <= high:
            return group_name
    
    return "Other"


def get_department_currently_issued(marhala_name: Optional[str] = None, hijri_year: Optional[int] = None) -> Dict[str, Any]:
    """
    Get currently issued books by marhala — ONLY active (non-expired, non-debarred) patrons.
    """
    start, end = get_ay_bounds(hijri_year)
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        
        query = """
        SELECT
            COALESCE(c.description, b.categorycode, 'Unknown') AS Marhala,
            COUNT(*) AS CurrentlyIssued,
            SUM(CASE
                WHEN i.date_due < CURDATE()
                     AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                     AND (b.debarred IS NULL OR b.debarred = 0)
                THEN 1 ELSE 0
            END) AS Overdue,
            GROUP_CONCAT(DISTINCT it.ccode ORDER BY it.ccode SEPARATOR ', ') AS Collections
        FROM issues i
        JOIN borrowers b ON i.borrowernumber = b.borrowernumber
        LEFT JOIN categories c ON c.categorycode = b.categorycode
        JOIN items it ON i.itemnumber = it.itemnumber
        WHERE i.returndate IS NULL
          AND DATE(i.issuedate) BETWEEN %s AND %s
          AND b.categorycode LIKE 'S%%'
          AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
          AND (b.debarred IS NULL OR b.debarred = 0)
          AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
        """
        
        params = [start, end]
        
        if marhala_name:
            query += " AND (c.description = %s OR b.categorycode = %s)"
            params.extend([marhala_name, marhala_name])
        
        query += " GROUP BY Marhala ORDER BY CurrentlyIssued DESC;"
        
        cur.execute(query, params)
        rows = cur.fetchall()
        cur.close()
        
        total_issued = sum(row['CurrentlyIssued'] for row in rows) if rows else 0
        
        # 21-DAY GRACE PERIOD: If AY started < 21 days ago, zero out overdues
        if start and (date.today() - start).days < 21:
            for row in rows:
                row['Overdue'] = 0
        
        return {
            "marhalas": rows,
            "total_currently_issued": total_issued
        }
    finally:
        conn.close()


# -------------------------------
# MARHALA & DARAJAH FUNCTIONS (Cached)
# -------------------------------

def get_marhala_distribution_with_dars_burhani(hijri_year: Optional[int] = None) -> Tuple[List[str], List[int]]:
    """Return Marhala distribution including Dars Burhani."""
    cache_key = f"marhala_distribution_{hijri_year}"
    cached = marhala_stats_cache.get(cache_key)
    if cached is not None:
        return cached
    
    start, end = get_ay_bounds(hijri_year)
    if not start:
        return [], []

    academic_marhalas = get_academic_marhalas()
    
    with get_db_cursor() as cur:
        placeholders = ', '.join(['%s'] * len(academic_marhalas))
        
        cur.execute(f"""
            SELECT 
                c.categorycode,
                c.description AS marhala_name,
                COUNT(*) AS total_issues
            FROM statistics s
            JOIN borrowers b ON s.borrowernumber = b.borrowernumber
            JOIN categories c ON b.categorycode = c.categorycode
            WHERE s.type = 'issue'
              AND DATE(s.`datetime`) BETWEEN %s AND %s
              AND b.categorycode IN ({placeholders})
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            GROUP BY c.categorycode, c.description
        """, (start, end, *academic_marhalas))
        
        rows = cur.fetchall()
        result = {}
        for row in rows:
            name = row["marhala_name"]
            issues = int(row["total_issues"] or 0)
            result[name] = result.get(name, 0) + issues
    
    all_marhalas = [
        'Collegiate I (5-7)',
        'Culture Générale (Std 3-4)',
        'Culture Générale (Std 1-2)',
        'Collegiate II & Higher Studies (Std 8-11)',
        'Dars Burhani'
    ]
    
    labels = []
    values = []
    
    for marhala in all_marhalas:
        labels.append(marhala)
        values.append(result.get(marhala, 0))
    
    output = (labels, values)
    marhala_stats_cache.set(cache_key, output)
    return output


def get_darajah_summary_by_marhala(marhala_name: Optional[str] = None, hijri_year: Optional[int] = None) -> List[Dict]:
    """Get Darajah summary filtered by Marhala - OPTIMIZED."""
    cache_key = f"darajah_summary_{marhala_name}_{hijri_year}"
    cached = darajah_cache.get(cache_key)
    if cached is not None:
        return cached
    
    start, end = get_ay_bounds(hijri_year)
    if not start:
        return []

    academic_codes = get_academic_marhalas()
    non_academic_codes = get_non_academic_marhalas()
    all_codes = academic_codes + non_academic_codes
    
    with get_db_cursor() as cur:
        placeholders = ', '.join(['%s'] * len(all_codes))
        
        query = f"""
            WITH darajah_issues AS (
                SELECT
                    b.borrowernumber,
                    COALESCE(std.attribute, b.branchcode, 'Unknown') AS darajah,
                    c.description AS marhala,
                    it.ccode
                FROM statistics s
                JOIN borrowers b ON b.borrowernumber = s.borrowernumber
                LEFT JOIN categories c ON c.categorycode = b.categorycode
                LEFT JOIN borrower_attributes std
                    ON std.borrowernumber = b.borrowernumber
                    AND std.code IN ('Class', 'STD', 'CLASS', 'DAR', 'CLASS_STD')
                JOIN items it ON s.itemnumber = it.itemnumber
                WHERE s.type = 'issue'
                  AND DATE(s.`datetime`) BETWEEN %s AND %s
                  AND b.categorycode IN ({placeholders})
            )
            SELECT
                darajah,
                marhala,
                COUNT(*) AS BooksIssued,
                COUNT(DISTINCT borrowernumber) AS ActiveStudents,
                GROUP_CONCAT(DISTINCT ccode ORDER BY ccode SEPARATOR ', ') AS Collections
            FROM darajah_issues
        """
        
        params = [start, end, *all_codes]
        
        if marhala_name:
            query += " WHERE marhala = %s"
            params.append(marhala_name)
        
        query += " GROUP BY darajah, marhala ORDER BY BooksIssued DESC"
        
        cur.execute(query, params)
        rows = cur.fetchall()
    
    for r in rows:
        name = (r.get("darajah") or "").strip()
        if name.upper() == "AJSN":
            name = "Asateza"
        r["Darajah"] = name or "Unknown"
        r["Collections"] = r.get("Collections") or "—"
        
        active_students = r.get("ActiveStudents") or 0
        books_issued = r.get("BooksIssued") or 0
        r["IssuesPerStudent"] = round(books_issued / active_students, 2) if active_students else 0.0
    
    darajah_cache.set(cache_key, rows)
    return rows


def get_issues_by_language(marhala_name: Optional[str] = None, hijri_year: Optional[int] = None) -> Tuple[List[str], List[int]]:
    """Get issues distribution by language from MARC 041$a metadata."""
    cache_key = f"issues_by_language_{marhala_name}_{hijri_year}"
    cached = marhala_stats_cache.get(cache_key)
    if cached is not None:
        return cached
        
    start, end = get_ay_bounds(hijri_year)
    if not start:
        return [], []
        
    with get_db_cursor() as cur:
        query = """
            SELECT 
                COALESCE(
                    ExtractValue(bmd.metadata, '//datafield[@tag="041"]/subfield[@code="a"]'),
                    'Unknown'
                ) AS language,
                COUNT(*) AS count
            FROM statistics s
            JOIN items it ON s.itemnumber = it.itemnumber
            JOIN biblio_metadata bmd ON it.biblionumber = bmd.biblionumber
            JOIN borrowers b ON s.borrowernumber = b.borrowernumber
            LEFT JOIN categories c ON b.categorycode = c.categorycode
            WHERE s.type = 'issue'
              AND DATE(s.`datetime`) BETWEEN %s AND %s
        """
        params = [start, end]
        if marhala_name:
            query += " AND (c.description = %s OR b.categorycode = %s)"
            params.extend([marhala_name, marhala_name])
            
        cur.execute(query + " GROUP BY language ORDER BY count DESC", params)
        rows = cur.fetchall()
        
    labels = []
    values = []
    for row in rows:
        lang = row['language'].strip() or 'Unknown'
        labels.append(lang)
        values.append(int(row['count']))
        
    output = (labels, values)
    marhala_stats_cache.set(cache_key, output)
    return output


# -------------------------------
# ACADEMIC YEAR HISTORY FUNCTIONS
# -------------------------------

def get_ay_bounds_for_hijri_year(hijri_year: int) -> Tuple[Optional[date], Optional[date]]:
    """
    Get academic year bounds for a specific Hijri year.
    AY starts on 1st Shawwal of hijri_year and ends on last of Sha'ban of hijri_year+1.
    """
    try:
        from hijri_converter import convert
        from datetime import timedelta
        
        start_obj = convert.Hijri(hijri_year, 10, 1).to_gregorian()
        start = date(start_obj.year, start_obj.month, start_obj.day)
        
        try:
            end_obj = convert.Hijri(hijri_year + 1, 8, 30).to_gregorian()
            end = date(end_obj.year, end_obj.month, end_obj.day)
        except Exception:
            end = start + timedelta(days=354)
        
        return start, end
    except Exception as e:
        logger.error(f"Error getting AY bounds for hijri year {hijri_year}: {e}")
        return None, None


def get_available_academic_years() -> List[Dict]:
    """
    Get list of available academic years (Hijri) that have data in the statistics table.
    Returns list of dicts with hijri_year, start, end, label.
    """
    cache_key = "available_academic_years"
    cached = summary_cache.get(cache_key)
    if cached is not None:
        return cached
    
    try:
        with get_db_cursor() as cur:
            # Get earliest and latest issue dates from statistics
            cur.execute("""
                SELECT 
                    MIN(DATE(datetime)) AS earliest,
                    MAX(DATE(datetime)) AS latest
                FROM statistics
                WHERE type = 'issue'
            """)
            row = cur.fetchone()
            if not row or not row.get('earliest'):
                return []
            
            earliest = row['earliest']
            latest = row['latest']
        
        # Convert earliest and latest to Hijri years
        try:
            from hijri_converter import convert as hc
            h_earliest = hc.Gregorian(earliest.year, earliest.month, earliest.day).to_hijri()
            h_latest = hc.Gregorian(latest.year, latest.month, latest.day).to_hijri()
            
            # The AY starts on Shawwal (month 10), so if earliest is before Shawwal,
            # the AY it belongs to is the previous Hijri year
            start_hijri_year = h_earliest.year if h_earliest.month >= 10 else h_earliest.year - 1
            end_hijri_year = h_latest.year if h_latest.month >= 10 else h_latest.year - 1
            # Always include at most 5 previous years
            start_hijri_year = max(start_hijri_year, end_hijri_year - 4)
        except Exception:
            return []
        
        years = []
        for hy in range(end_hijri_year, start_hijri_year - 1, -1):
            start, end = get_ay_bounds_for_hijri_year(hy)
            if not start:
                continue
            
            # Check if there's actual data for this year
            with get_db_cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) AS cnt FROM statistics
                    WHERE type = 'issue' AND DATE(datetime) BETWEEN %s AND %s
                """, (start, min(end, date.today())))
                result = cur.fetchone()
                if not result or not result.get('cnt'):
                    continue
            
            # Format label
            try:
                start_month = HIJRI_MONTHS[9]  # Shawwal is index 9
                end_month = HIJRI_MONTHS[7]    # Sha'ban is index 7
                label = f"AY {hy} H ({start_month[:7]} {hy} – {end_month[:7]} {hy + 1})"
            except Exception:
                label = f"AY {hy} H"
            
            years.append({
                'hijri_year': hy,
                'start': start.isoformat(),
                'end': min(end, date.today()).isoformat(),
                'label': label,
                'is_current': (hy == end_hijri_year)
            })
        
        summary_cache.set(cache_key, years)
        return years
    except Exception as e:
        logger.error(f"Error getting available academic years: {e}")
        return []


def get_monthly_trend_for_period(
    start: date, end: date,
    marhala_code: Optional[str] = None,
    darajah_name: Optional[str] = None
) -> Tuple[List[str], List[int]]:
    """
    Get monthly borrowing trend data for an explicit date range.
    Returns 12 Hijri month points for a consistent trend view.
    """
    if not start or not end:
        return [], []

    try:
        from hijri_converter import convert
        # Identify the base Hijri year from the start date (1st Shawwal)
        start_hijri = convert.Gregorian(start.year, start.month, start.day).to_hijri()
        base_h_year = start_hijri.year
        
        # Define the 12 month labels for the AY cycle starting from Shawwal (10)
        labels = []
        month_ranges = [] # list of (h_year, h_month)
        m = 10
        y = base_h_year
        for _ in range(12):
            labels.append(f"{HIJRI_MONTHS[m-1]} {y} H")
            month_ranges.append((y, m))
            m += 1
            if m > 12:
                m = 1
                y += 1

        # Fetch all statistics between start and end with filters
        with get_db_cursor() as cur:
            query = """
                SELECT s.datetime
                FROM statistics s
                JOIN borrowers b ON s.borrowernumber = b.borrowernumber
                LEFT JOIN categories c ON b.categorycode = c.categorycode
                WHERE s.type = 'issue'
                  AND DATE(s.datetime) BETWEEN %s AND %s
            """
            params: List = [start, end]
            
            if marhala_code:
                query += " AND (c.description = %s OR b.categorycode = %s)"
                params.extend([marhala_code, marhala_code])
            elif darajah_name:
                query += """
                    AND EXISTS (
                        SELECT 1 FROM borrower_attributes std
                        WHERE std.borrowernumber = b.borrowernumber
                        AND std.code IN ('Class','STD','CLASS','DAR','CLASS_STD')
                        AND std.attribute = %s
                    )
                """
                params.append(darajah_name)
            
            cur.execute(query, params)
            rows = cur.fetchall()

        # Group rows by Hijri month
        counts = [0] * 12
        for row in rows:
            dt = row['datetime']
            try:
                h = convert.Gregorian(dt.year, dt.month, dt.day).to_hijri()
                # Find which month slot this falls into
                for i, (ry, rm) in enumerate(month_ranges):
                    if h.year == ry and h.month == rm:
                        counts[i] += 1
                        break
            except:
                continue
                
        # To make it professional, we only show months up to the current Hijri month + 1 padding 
        # OR just show all 12 if we want the full year view.
        # User wants to update "trend", so full 12 month view is usually better.
        return labels, counts
    except Exception as e:
        logger.error(f"Error in trend generation: {e}")
        return [], []


# -------------------------------
# IMPROVED TREND DATA FUNCTION - WITH CACHING
# -------------------------------

def get_ay_trend_data(marhala_code: Optional[str] = None, darajah_name: Optional[str] = None, hijri_year: Optional[int] = None) -> Tuple[List[str], List[int]]:
    """
    Get borrowing trends for the current Academic Year — always monthly grouping.
    Results cached for 5 minutes.
    """
    cache_key = f"trend_{marhala_code}_{darajah_name}_{hijri_year}"
    cached = trend_cache.get(cache_key)
    if cached is not None:
        return cached

    start, end = get_ay_bounds(hijri_year)
    if not start:
        return [], []

    today = date.today()
    effective_end = min(end, today)

    # Always use monthly grouping for a clean trend line
    labels, values = get_monthly_trend_for_period(start, effective_end, marhala_code, darajah_name)

    output = (labels, values)
    trend_cache.set(cache_key, output)
    return output


# -------------------------------
# MARHALA HELPER FUNCTIONS (Cached)
# -------------------------------

@lru_cache(maxsize=1)
def get_academic_marhalas() -> List[str]:
    """Get academic marhala category codes."""
    return [
        'S-CO',   # Collegiate I (5-7)
        'S-CGB',  # Culture Générale (Std 3-4)
        'S-CGA',  # Culture Générale (Std 1-2)
        'S-CT',   # Collegiate II & Higher Studies (Std 8-11)
        'S-DARS'  # Dars Burhani
    ]


@lru_cache(maxsize=1)
def get_non_academic_marhalas() -> List[str]:
    """Get non-academic marhala category codes."""
    return [
        'T-KG',  # Asateza Kiram
        'L',     # Library Staff
        'T',     # Teaching Staff
        'S',     # Library Staff (alternate)
        'M-KG',  # Mukhayyam Khidmat Guzar
        'HO'     # Sighat ul Jamea
    ]


def get_non_academic_marhala_display_name(code: str) -> str:
    """Get display name for non-academic marhala."""
    display_names = {
        'T-KG': 'Asateza Kiram',
        'L': 'Library Staffs',
        'T': 'Teaching Staffs',
        'S': 'Library Staffs',
        'M-KG': 'Mukhayyam Khidmat Guzar',
        'HO': 'Sighat ul Jamea'
    }
    return display_names.get(code, code)


def is_academic_marhala(marhala_code: str) -> bool:
    """Check if marhala is academic."""
    return marhala_code in get_academic_marhalas()


def is_non_academic_marhala(marhala_code: str) -> bool:
    """Check if marhala is non-academic."""
    return marhala_code in get_non_academic_marhalas()


@lru_cache(maxsize=1)
def get_all_marhalas() -> List[str]:
    """
    Get all Marhala names for filter dropdown.
    Returns actual Koha category descriptions.
    """
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        
        academic_codes = get_academic_marhalas()
        non_academic_codes = get_non_academic_marhalas()
        all_codes = academic_codes + non_academic_codes
        
        if not all_codes:
            return []
        
        placeholders = ', '.join(['%s'] * len(all_codes))
        
        cur.execute(f"""
            SELECT DISTINCT c.description
            FROM categories c
            WHERE c.categorycode IN ({placeholders})
            ORDER BY c.description;
        """, tuple(all_codes))
        
        rows = cur.fetchall()
        cur.close()
        
        return [row['description'] for row in rows if row.get('description')]
    finally:
        conn.close()


# -------------------------------
# KEY INSIGHTS (Cached)
# -------------------------------

def get_key_insights(hijri_year: Optional[int] = None) -> List[str]:
    """Generate key insights with optimized data fetching."""
    cache_key = f"key_insights_{hijri_year}"
    cached = summary_cache.get(cache_key)
    if cached is not None:
        return cached
    
    insights = []
    start, end = get_ay_bounds(hijri_year)
    
    if start:
        with get_db_cursor() as cur:
            # Get top darajah (excluding Asateza)
            cur.execute("""
                SELECT
                    COALESCE(std.attribute, b.branchcode, 'Unknown') AS Darajah,
                    COUNT(*) AS BooksIssued,
                    COUNT(DISTINCT s.borrowernumber) AS ActiveStudents
                FROM statistics s
                JOIN borrowers b ON b.borrowernumber = s.borrowernumber
                LEFT JOIN borrower_attributes std
                    ON std.borrowernumber = b.borrowernumber
                    AND std.code IN ('Class', 'STD', 'CLASS', 'DAR', 'CLASS_STD')
                WHERE s.type = 'issue'
                  AND DATE(s.`datetime`) BETWEEN %s AND %s
                  AND (std.attribute != 'AJSN' OR std.attribute IS NULL)
                  AND b.branchcode != 'AJSN'
                GROUP BY Darajah
                ORDER BY BooksIssued DESC
                LIMIT 1
            """, (start, end))
            
            top_darajah = cur.fetchone()
            if top_darajah:
                darajah_name = top_darajah["Darajah"]
                books = top_darajah["BooksIssued"]
                active = top_darajah["ActiveStudents"] or 1
                per_student = round(books / active, 1)
                insights.append(
                    f"Top performing Darajah (excluding Asateza): {darajah_name} "
                    f"with {books:,} books issued ({per_student} per student)."
                )
            
            # Get top academic marhala
            academic_codes = get_academic_marhalas()
            if academic_codes:
                placeholders = ', '.join(['%s'] * len(academic_codes))
                cur.execute(f"""
                    SELECT
                        c.description AS Marhala,
                        COUNT(*) AS Issues
                    FROM statistics s
                    JOIN borrowers b ON s.borrowernumber = b.borrowernumber
                    JOIN categories c ON b.categorycode = c.categorycode
                    WHERE s.type = 'issue'
                      AND DATE(s.`datetime`) BETWEEN %s AND %s
                      AND b.categorycode IN ({placeholders})
                    GROUP BY c.description
                    ORDER BY Issues DESC
                    LIMIT 1
                """, (start, end, *academic_codes))
                
                top_academic = cur.fetchone()
                if top_academic:
                    insights.append(
                        f"Top academic Marhala: {top_academic['Marhala']} "
                        f"({top_academic['Issues']:,} issues)."
                    )
            
            # Get currently issued books
            cur.execute("""
                SELECT COUNT(*) AS cnt
                FROM issues
                WHERE returndate IS NULL
                  AND DATE(issuedate) BETWEEN %s AND %s
            """, (start, end))
            currently_issued = cur.fetchone()["cnt"] or 0
            insights.append(f"Currently issued books (AY): {currently_issued:,}.")
    
    # Gender distribution
    darajah_names, male_counts, female_counts = get_gender_darajah_distribution()
    if darajah_names:
        total_male = sum(male_counts)
        total_female = sum(female_counts)
        total = total_male + total_female
        if total > 0:
            male_pct = round((total_male / total) * 100, 1)
            female_pct = round((total_female / total) * 100, 1)
            insights.append(
                f"Gender distribution: {male_pct}% male, {female_pct}% female "
                f"(Girls: Darajah 1-7 only, Boys: Darajah 1-11)."
            )
    
    summary_cache.set(cache_key, insights)
    return insights


# -------------------------------
# TOP TITLES FUNCTIONS (Cached)
# -------------------------------

def top_titles(
    limit: int = 25, arabic: bool = False, non_arabic: bool = False, hijri_year: Optional[int] = None
) -> List[Tuple[str, int, str]]:
    """Top borrowed titles for current AY with secure parameterization."""
    cache_key = f"top_titles_{limit}_{arabic}_{non_arabic}_{hijri_year}"
    cached = top_titles_cache.get(cache_key)
    if cached is not None:
        return cached
    
    start, end = get_ay_bounds(hijri_year)
    if not start:
        return []
    
    lang_condition = ""
    params = [start, end]
    
    if arabic:
        lang_condition = "AND bib.title REGEXP %s"
        params.append('[ء-ي]')
    elif non_arabic:
        lang_condition = "AND bib.title NOT REGEXP %s"
        params.append('[ء-ي]')
    
    params.append(int(limit))
    
    with get_db_cursor(dictionary=False) as cur:
        cur.execute(f"""
            SELECT
                bib.title,
                COUNT(*) AS cnt,
                MAX(all_iss.issuedate) AS last_issued
            FROM statistics all_iss
            JOIN items it ON all_iss.itemnumber = it.itemnumber
            JOIN biblio bib ON it.biblionumber = bib.biblionumber
            WHERE all_iss.type = 'issue'
              AND DATE(all_iss.datetime) BETWEEN %s AND %s
            {lang_condition}
            GROUP BY bib.biblionumber, bib.title
            ORDER BY cnt DESC
            LIMIT %s
        """, params)
        
        rows = cur.fetchall()
    
    top_titles_cache.set(cache_key, rows)
    return rows


def _top_titles_by_language(language_code: str, limit: int = 25, marhala_name: Optional[str] = None, hijri_year: Optional[int] = None) -> List[Dict]:
    """Generic function for top titles by MARC 041$a language code."""
    cache_key = f"top_titles_lang_{language_code}_{limit}_{marhala_name}_{hijri_year}"
    cached = top_titles_cache.get(cache_key)
    if cached is not None:
        return cached
    
    start, end = get_ay_bounds(hijri_year)
    if not start:
        return []
    
    with get_db_cursor() as cur:
        query = """
            SELECT
                bib.biblionumber AS BiblioNumber,
                bib.title AS Title,
                bib.author AS Author,
                bib.notes AS Notes,
                bib.abstract AS Abstract,
                bi.isbn AS ISBN,
                MAX(ci.imagenumber) AS LocalImageNumber,
                GROUP_CONCAT(DISTINCT it.ccode ORDER BY it.ccode SEPARATOR ', ') AS Collections,
                COUNT(*) AS Times_Issued
            FROM (
                SELECT borrowernumber, itemnumber, issuedate
                FROM issues
                UNION ALL
                SELECT borrowernumber, itemnumber, issuedate
                FROM old_issues
            ) all_iss
            JOIN borrowers b ON b.borrowernumber = all_iss.borrowernumber
            LEFT JOIN categories c ON b.categorycode = c.categorycode
            JOIN items it ON all_iss.itemnumber = it.itemnumber
            JOIN biblio bib ON it.biblionumber = bib.biblionumber
            JOIN biblioitems bi ON bib.biblionumber = bi.biblionumber
            LEFT JOIN cover_images ci ON bib.biblionumber = ci.biblionumber
            JOIN biblio_metadata bmd ON bib.biblionumber = bmd.biblionumber
            WHERE DATE(all_iss.issuedate) BETWEEN %s AND %s
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
              AND ExtractValue(
                    bmd.metadata,
                    '//datafield[@tag="041"]/subfield[@code="a"]'
                  ) = %s
        """
        params = [start, end, language_code]
        
        if marhala_name:
            query += " AND (c.description = %s OR b.categorycode = %s)"
            params.extend([marhala_name, marhala_name])
            
        query += """
            GROUP BY bib.biblionumber, bib.title, bib.author, bib.notes, bib.abstract, bi.isbn
            ORDER BY Times_Issued DESC
            LIMIT %s
        """
        params.append(limit)
        
        cur.execute(query, params)
        rows = cur.fetchall()

    from routes.dashboard import get_opac_base
    opac_base = get_opac_base()

    for row in rows:
        row["Title"] = row.get("Title") or "Untitled"
        row["Author"] = row.get("Author") or "Unknown"
        row["Times_Issued"] = int(row.get("Times_Issued", 0))
        
        synopsis = row.get("Abstract") or row.get("Notes") or ""
        row["Synopsis"] = str(synopsis).strip()
        
        # Priority 1: Local Koha Cover Image
        local_img = row.get("LocalImageNumber")
        if local_img:
            # Use OPAC image URL
            row["CoverURL"] = f"{opac_base}/cgi-bin/koha/opac-image.pl?biblionumber={row['BiblioNumber']}&imagenumber={local_img}"
        else:
            # Clean ISBN for external lookups
            isbn = str(row.get("ISBN") or "").replace("-", "").replace(" ", "").replace(".", "").split(' ')[0]
            
            if isbn and len(isbn) >= 10:
                # Priority 2: Amazon Jackets (Highly reliable for published titles)
                row["CoverURL"] = f"https://images-na.ssl-images-amazon.com/images/P/{isbn}.01.MZZZZZZZ.jpg"
                
                # Priority 3: Google Books via ISBN (Used via onerror in template if Amazon fails)
                # We'll stick to one URL here for simplicity, but Amazon is preferred by user
            else:
                # Fallback: Koha internal detail cover or generic placeholder
                row["CoverURL"] = f"/students/book_cover/{row['BiblioNumber']}"
    
    top_titles_cache.set(cache_key, rows)
    return rows


def get_language_top25(marhala_name: Optional[str] = None, hijri_year: Optional[int] = None) -> Dict[str, Dict[str, List]]:
    """Combine Arabic and English top 25 into expected structure."""
    def pack(rows: List[Dict]) -> Dict[str, List]:
        titles = [r.get("Title") for r in rows]
        counts = [int(r.get("Times_Issued") or 0) for r in rows]
        return {
            "titles": titles,
            "counts": counts,
            "records": rows,
        }
    
    return {
        "arabic": pack(arabic_top25(marhala_name, hijri_year=hijri_year)),
        "english": pack(english_top25(marhala_name, hijri_year=hijri_year)),
    }


def arabic_top25(marhala_name: Optional[str] = None, hijri_year: Optional[int] = None) -> List[Dict]:
    """Top 25 Arabic titles."""
    return _top_titles_by_language('Arabic', 25, marhala_name, hijri_year=hijri_year)


def english_top25(marhala_name: Optional[str] = None, hijri_year: Optional[int] = None) -> List[Dict]:
    """Top 25 English titles."""
    return _top_titles_by_language('English', 25, marhala_name, hijri_year=hijri_year)


# -------------------------------
# OPTIMIZED MARHALA STATS FUNCTIONS (Cached)
# -------------------------------

def get_all_marhalas_with_stats() -> List[Dict]:
    """Get all marhalas with detailed statistics using optimized batch queries."""
    cache_key = "all_marhalas_with_stats"
    cached = marhala_stats_cache.get(cache_key)
    if cached is not None:
        return cached
    
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT 
                c.categorycode AS marhala_code,
                c.description AS marhala_name
            FROM categories c
            WHERE c.description IS NOT NULL AND c.description != ''
            ORDER BY c.description
        """)
        
        marhalas = cur.fetchall()
        marhala_map = {m["marhala_code"]: m for m in marhalas}
        
        for m in marhalas:
            m.update({
                "total_borrowers": 0,
                "active_borrowers": 0,
                "ay_issues": 0,
                "currently_issued": 0,
                "overdues": 0
            })
        
        if marhalas:
            placeholders = ', '.join(['%s'] * len(marhala_map.keys()))
            
            cur.execute(f"""
                SELECT 
                    b.categorycode,
                    COUNT(*) AS total,
                    COUNT(DISTINCT CASE 
                        WHEN trno.attribute IS NOT NULL AND trno.attribute != ''
                        THEN b.borrowernumber 
                    END) AS active
                FROM borrowers b
                LEFT JOIN borrower_attributes trno 
                    ON trno.borrowernumber = b.borrowernumber AND trno.code = 'TRNO'
                WHERE b.categorycode IN ({placeholders})
                GROUP BY b.categorycode
            """, tuple(marhala_map.keys()))
            
            for row in cur.fetchall():
                code = row["categorycode"]
                if code in marhala_map:
                    marhala_map[code]["total_borrowers"] = row["total"]
                    marhala_map[code]["active_borrowers"] = row["active"]
        
        start, end = get_ay_bounds()
        if start and end and marhalas:
            placeholders = ', '.join(['%s'] * len(marhala_map.keys()))
            
            cur.execute(f"""
                SELECT 
                    b.categorycode,
                    SUM(COALESCE(s_agg.cnt, 0)) AS ay_issues,
                    SUM(COALESCE(i_agg.cnt, 0)) AS currently_issued,
                    SUM(COALESCE(i_agg.overdue_cnt, 0)) AS overdues
                FROM borrowers b
                LEFT JOIN (
                    SELECT borrowernumber, COUNT(*) AS cnt
                    FROM statistics
                    WHERE type = 'issue' AND DATE(`datetime`) BETWEEN %s AND %s
                    GROUP BY borrowernumber
                ) s_agg ON b.borrowernumber = s_agg.borrowernumber
                LEFT JOIN (
                    SELECT borrowernumber, 
                           COUNT(*) AS cnt,
                           SUM(CASE 
                                WHEN date_due < CURDATE() 
                                AND DATE(issuedate) BETWEEN %s AND %s
                                THEN 1 ELSE 0 
                           END) AS overdue_cnt
                    FROM issues
                    WHERE returndate IS NULL
                    GROUP BY borrowernumber
                ) i_agg ON b.borrowernumber = i_agg.borrowernumber
                WHERE b.categorycode IN ({placeholders})
                GROUP BY b.categorycode
            """, (start, end, start, end, *marhala_map.keys()))
            
            for row in cur.fetchall():
                code = row["categorycode"]
                if code in marhala_map:
                    marhala_map[code]["ay_issues"] = int(row["ay_issues"] or 0)
                    marhala_map[code]["currently_issued"] = int(row["currently_issued"] or 0)
                    marhala_map[code]["overdues"] = int(row["overdues"] or 0)
    
    aggregated = {}
    academic_codes = set(get_academic_marhalas())
    non_academic_codes = set(get_non_academic_marhalas())

    for m in marhalas:
        orig_name = m.get("marhala_name", "Unknown")
        display_name = format_marhala_display_name(orig_name)
        code = m.get("marhala_code")

        if display_name not in aggregated:
            m_type = "Other"
            m_icon = "info-circle"
            
            if code in academic_codes:
                m_type = "Academic"
                m_icon = "graduation-cap"
            elif code in non_academic_codes:
                if display_name == "Library Staff":
                    m_type = "Library"
                    m_icon = "book"
                elif display_name == "Teaching Staff":
                    m_type = "Staff"
                    m_icon = "user-tie"
                else:
                    m_type = "Staff"
                    m_icon = "user-tie"
            
            aggregated[display_name] = {
                "marhala_name": display_name,
                "total_borrowers": 0,
                "active_borrowers": 0,
                "ay_issues": 0,
                "currently_issued": 0,
                "overdues": 0,
                "type": m_type,
                "icon": m_icon
            }
        
        agg = aggregated[display_name]
        agg["total_borrowers"] += int(m.get("total_borrowers", 0))
        agg["active_borrowers"] += int(m.get("active_borrowers", 0))
        agg["ay_issues"] += int(m.get("ay_issues", 0))
        agg["currently_issued"] += int(m.get("currently_issued", 0))
        agg["overdues"] += int(m.get("overdues", 0))

    result = list(aggregated.values())
    marhala_stats_cache.set(cache_key, result)
    return result


def get_marhala_summary(selected_marhala: Optional[str] = None) -> List[Dict]:
    """Get marhala summary stats using optimized data."""
    all_marhalas = get_all_marhalas_with_stats()
    summary = []
    
    for m in all_marhalas:
        m_name = m.get("marhala_name", "Unknown")
        
        if selected_marhala and m_name != selected_marhala:
            continue
        
        active_patrons = int(m.get("active_borrowers", 0))
        issues = int(m.get("ay_issues", 0))
        avg = round(issues / active_patrons, 1) if active_patrons > 0 else 0.0
        
        summary.append({
            "Marhala": m_name,
            "BooksIssued": issues,
            "ActivePatrons": active_patrons,
            "CurrentlyIssued": m.get("currently_issued", 0),
            "Overdues": m.get("overdues", 0),
            "IssuesPerPatron": avg,
            "Type": m.get("type", "Other"),
            "Icon": m.get("icon", "users"),
            "Collections": "—"
        })
    
    summary.sort(key=lambda x: x["BooksIssued"], reverse=True)
    return summary


def get_marhala_engagement_stats() -> Dict[str, int]:
    """Get engagement statistics for all marhalas."""
    marhalas = get_all_marhalas_with_stats()
    
    counts = {
        "total_marhalas": len(marhalas),
        "academic_marhalas": 0,
        "staff_marhalas": 0,
        "library_marhalas": 0,
        "admin_marhalas": 0,
        "other_marhalas": 0,
        "total_borrowers": 0,
        "active_borrowers": 0,
        "total_ay_issues": 0
    }
    
    for marhala in marhalas:
        m_type = marhala.get("type", "Other")
        
        if m_type == "Academic":
            counts["academic_marhalas"] += 1
        elif m_type == "Staff":
            counts["staff_marhalas"] += 1
        elif m_type == "Library":
            counts["library_marhalas"] += 1
        elif m_type == "Administration":
            counts["admin_marhalas"] += 1
        else:
            counts["other_marhalas"] += 1
        
        counts["total_borrowers"] += marhala.get("total_borrowers", 0)
        counts["active_borrowers"] += marhala.get("active_borrowers", 0)
        counts["total_ay_issues"] += marhala.get("ay_issues", 0)
    
    return counts


# -------------------------------
# GENDER-SPECIFIC DARAJAH FUNCTIONS (Cached)
# -------------------------------

def get_gender_darajah_distribution(hijri_year: Optional[int] = None) -> Tuple[List[str], List[int], List[int]]:
    """
    Get Darajah distribution by total issues:
    - Females: Darajah 1-7 only
    - Males: Darajah 1-11
    """
    cache_key = f"gender_darajah_distribution_{hijri_year}"
    cached = darajah_cache.get(cache_key)
    if cached is not None:
        return cached
    
    start, end = get_ay_bounds(hijri_year)
    if not start:
        return [], [], []

    with get_db_cursor() as cur:
        cur.execute("""
            SELECT
                COALESCE(std.attribute, b.branchcode, 'Unknown') AS darajah_name,
                UPPER(COALESCE(b.sex, '')) AS gender,
                COUNT(*) AS cnt
            FROM statistics s
            JOIN borrowers b ON b.borrowernumber = s.borrowernumber
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('Class', 'STD', 'CLASS', 'DAR', 'CLASS_STD')
            WHERE s.type = 'issue'
              AND DATE(s.`datetime`) BETWEEN %s AND %s
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
              AND COALESCE(std.attribute, b.branchcode) != 'AJSN'
            GROUP BY darajah_name, gender
        """, (start, end))
        
        rows = cur.fetchall()
    
    darajah_data = {}
    
    for row in rows:
        darajah_name = (row["darajah_name"] or "Unknown").strip()
        gender = row["gender"]
        cnt = int(row["cnt"])
        
        if darajah_name not in darajah_data:
            darajah_data[darajah_name] = {'M': 0, 'F': 0}
        
        match = re.search(r'\d+', darajah_name)
        if not match:
            continue
        
        darajah_num = int(match.group())
        
        if gender == 'M' and 1 <= darajah_num <= 11:
            darajah_data[darajah_name]['M'] += cnt
        elif gender == 'F' and 1 <= darajah_num <= 7:
            darajah_data[darajah_name]['F'] += cnt
        elif not gender:
            darajah_name_upper = darajah_name.upper()
            if "F" in darajah_name_upper and 1 <= darajah_num <= 7:
                darajah_data[darajah_name]['F'] += cnt
            elif "M" in darajah_name_upper and 1 <= darajah_num <= 11:
                darajah_data[darajah_name]['M'] += cnt
    
    def darajah_sort_key(name: str):
        match = re.search(r'\d+', name)
        return (0, int(match.group())) if match else (1, name)
    
    darajah_names = sorted(darajah_data.keys(), key=darajah_sort_key)
    
    male_counts = [darajah_data[name]['M'] for name in darajah_names]
    female_counts = [darajah_data[name]['F'] for name in darajah_names]
    
    result = (darajah_names, male_counts, female_counts)
    darajah_cache.set(cache_key, result)
    return result


# -------------------------------
# ADDITIONAL FUNCTIONS (Remain unchanged but add caching where beneficial)
# -------------------------------

def format_marhala_display_name(marhala_name: str) -> str:
    """Format Marhala name for consistent display."""
    if not marhala_name:
        return "Unknown"
    
    marhala_name = str(marhala_name).strip()
    
    display_map = {
        "Teacher": "Teaching Staff",
        "Library": "Library Staff",
        "Asateza Kiram": "Asateza Kiram",
        "Sighat ul jamea": "Sighat ul Jamea",
        "Mukhayyam Khidmat Guzar": "Mukhayyam Khidmat Guzar",
        "Collegiate I (5-7)": "Collegiate I (5-7)",
        "Culture Générale (Std 3-4)": "Culture Générale (Std 3-4)",
        "Culture Générale (Std 1-2)": "Culture Générale (Std 1-2)",
        "Collegiate II & Higher Studies (Std 8-11)": "Collegiate II & Higher Studies (Std 8-11)",
        "Collegiate II and Higher Studies": "Collegiate II & Higher Studies (Std 8-11)",
        "Dars Burhani": "Dars Burhani",
        "Staff": "Library Staff"
    }
    
    return display_map.get(marhala_name, marhala_name)


def get_darajahs_in_marhala(marhala_code: str) -> List[Dict]:
    """Get all darajahs belonging to a specific marhala."""
    start, end = get_ay_bounds()
    
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT 
                COALESCE(std.attribute, b.branchcode) AS darajah_name,
                COUNT(DISTINCT b.borrowernumber) AS total_students,
                SUM(COALESCE(s_agg.active_cnt, 0)) AS active_students,
                SUM(COALESCE(s_agg.ay_issues, 0)) AS ay_issues
            FROM borrowers b
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('Class', 'STD', 'CLASS', 'DAR', 'CLASS_STD')
            LEFT JOIN (
                SELECT borrowernumber, 
                       COUNT(DISTINCT borrowernumber) AS active_cnt,
                       COUNT(*) AS ay_issues
                FROM statistics
                WHERE type = 'issue' AND DATE(`datetime`) BETWEEN %s AND %s
                GROUP BY borrowernumber
            ) s_agg ON b.borrowernumber = s_agg.borrowernumber
            WHERE b.categorycode = %s
                AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                AND (b.debarred IS NULL OR b.debarred = 0)
                AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
                AND COALESCE(std.attribute, b.branchcode) IS NOT NULL
                AND COALESCE(std.attribute, b.branchcode) != ''
            GROUP BY darajah_name
            ORDER BY darajah_name
        """, (start, end, start, end, marhala_code))
        
        darajahs = cur.fetchall()
    
    for darajah in darajahs:
        name = darajah.get("darajah_name", "")
        name_str = str(name)
        
        if " M" in name_str or name_str.endswith("M"):
            darajah["gender"] = "Boys"
            darajah["icon"] = "male"
        elif " F" in name_str or name_str.endswith("F"):
            darajah["gender"] = "Girls"
            darajah["icon"] = "female"
        else:
            darajah["gender"] = "Mixed"
            darajah["icon"] = "users"
    
    return darajahs


def get_top_students(limit: int = 10, marhala_name: Optional[str] = None, hijri_year: Optional[int] = None, sex: Optional[str] = None) -> List[Dict]:
    """Get top students by number of books issued in the current AY."""
    start, end = get_ay_bounds(hijri_year)
    if not start:
        return []
    
    with get_db_cursor() as cur:
        query = """
            SELECT 
                b.borrowernumber,
                CASE 
                    WHEN (b.surname IS NOT NULL AND b.surname != '' AND b.surname != 'None')
                         AND (b.firstname IS NOT NULL AND b.firstname != '' AND b.firstname != 'None')
                    THEN CONCAT(b.surname, ' ', b.firstname)
                    WHEN (b.surname IS NOT NULL AND b.surname != '' AND b.surname != 'None')
                    THEN b.surname
                    WHEN (b.firstname IS NOT NULL AND b.firstname != '' AND b.firstname != 'None')
                    THEN b.firstname
                    ELSE CONCAT('Student #', b.cardnumber)
                END AS StudentName,
                b.cardnumber,
                COALESCE(std.attribute, b.branchcode) AS Class,
                c.description AS Department,
                COUNT(*) AS BooksIssued,
                GROUP_CONCAT(DISTINCT it.ccode ORDER BY it.ccode SEPARATOR ', ') AS CollectionsUsed
            FROM statistics s
            JOIN borrowers b ON b.borrowernumber = s.borrowernumber
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('Class', 'STD', 'CLASS', 'DAR', 'CLASS_STD')
            JOIN items it ON s.itemnumber = it.itemnumber
            WHERE s.type = 'issue'
              AND DATE(s.`datetime`) BETWEEN %s AND %s
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
              AND c.description IS NOT NULL
        """
        params = [start, end]
        if marhala_name:
            query += " AND (c.description = %s OR b.categorycode = %s)"
            params.extend([marhala_name, marhala_name])

        if sex:
            query += " AND b.sex = %s\n"
            params.append(sex)
            
        academic_codes = get_academic_marhalas()
        if academic_codes:
            placeholders = ', '.join(['%s'] * len(academic_codes))
            query += f" AND b.categorycode IN ({placeholders})\n"
            params.extend(academic_codes)

        query += """
            GROUP BY b.borrowernumber, b.surname, b.firstname, b.cardnumber, 
                     std.attribute, b.branchcode, c.description
            ORDER BY BooksIssued DESC
            LIMIT %s
        """
        params.append(limit)
        
        cur.execute(query, params)
        
        rows = cur.fetchall()
    
    for row in rows:
        if row.get("Class") and "AJSN" in str(row["Class"]).upper():
            row["Class"] = "Asateza"
        
        if not row.get("StudentName") or str(row["StudentName"]).strip() in ("", "None"):
            row["StudentName"] = f"Student #{row['cardnumber']}"
    
    return rows


def get_department_performance(category_codes: List[str], hijri_year: Optional[int] = None) -> List[Dict]:
    """Generic function to get performance for a list of category codes."""
    start, end = get_ay_bounds(hijri_year)
    if not start or not category_codes:
        return []
    
    with get_db_cursor() as cur:
        placeholders = ', '.join(['%s'] * len(category_codes))
        
        cur.execute(f"""
            WITH marhala_stats AS (
                SELECT
                    c.categorycode,
                    c.description AS Marhala,
                    COUNT(s.borrowernumber) AS BooksIssued,
                    COUNT(DISTINCT s.borrowernumber) AS ActivePatrons,
                    GROUP_CONCAT(DISTINCT it.ccode ORDER BY it.ccode SEPARATOR ', ') AS Collections
                FROM categories c
                LEFT JOIN borrowers b ON c.categorycode = b.categorycode
                    AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                    AND (b.debarred IS NULL OR b.debarred = 0)
                    AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
                LEFT JOIN statistics s ON s.borrowernumber = b.borrowernumber
                    AND s.type = 'issue'
                    AND DATE(s.`datetime`) BETWEEN %s AND %s
                LEFT JOIN items it ON s.itemnumber = it.itemnumber
                WHERE c.categorycode IN ({placeholders})
                GROUP BY c.categorycode, c.description
            )
            SELECT * FROM marhala_stats
            ORDER BY BooksIssued DESC
        """, (start, end, *category_codes))
        
        rows = cur.fetchall()
    
    for r in rows:
        patrons = r.get("ActivePatrons") or 0
        issues = r.get("BooksIssued") or 0
        r["IssuesPerPatron"] = round(issues / patrons, 2) if patrons else 0.0
        r["Collections"] = r.get("Collections") or "—"
        r["Marhala"] = r.get("Marhala") or "Unknown"
        r["MarhalaCode"] = r.get("categorycode") or ""
    
    return rows


def get_academic_departments_performance(hijri_year: Optional[int] = None) -> List[Dict]:
    """Get performance for academic marhalas."""
    return get_department_performance(get_academic_marhalas(), hijri_year=hijri_year)


def get_non_academic_departments_performance(hijri_year: Optional[int] = None) -> List[Dict]:
    """Get performance for non-academic marhalas."""
    return get_department_performance(get_non_academic_marhalas(), hijri_year=hijri_year)


def get_top_darajah_summary(limit: int = 10, exclude_asateza: bool = False, hijri_year: Optional[int] = None) -> List[Dict]:
    """Unified function to get top darajah summary with optional Asateza exclusion."""
    start, end = get_ay_bounds(hijri_year)
    if not start:
        return []

    with get_db_cursor() as cur:
        query = """
            SELECT
                COALESCE(std.attribute, b.branchcode, 'Unknown') AS Darajah,
                COUNT(*) AS BooksIssued,
                COUNT(DISTINCT s.borrowernumber) AS ActiveStudents,
                GROUP_CONCAT(DISTINCT it.ccode ORDER BY it.ccode SEPARATOR ', ') AS Collections
            FROM statistics s
            JOIN borrowers b ON b.borrowernumber = s.borrowernumber
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('Class', 'STD', 'CLASS', 'DAR', 'CLASS_STD')
            JOIN items it ON s.itemnumber = it.itemnumber
            WHERE s.type = 'issue'
              AND DATE(s.`datetime`) BETWEEN %s AND %s
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
        """
        
        params = [start, end]
        
        if exclude_asateza:
            query += """
                AND (std.attribute != 'AJSN' OR std.attribute IS NULL)
                AND b.branchcode != 'AJSN'
            """
        
        query += " GROUP BY Darajah ORDER BY BooksIssued DESC LIMIT %s"
        params.append(limit)
        
        cur.execute(query, params)
        rows = cur.fetchall()
    
    for r in rows:
        name = (r.get("Darajah") or "").strip()
        if name.upper() == "AJSN":
            name = "Asateza"
        r["Darajah"] = name or "Unknown"
        r["Collections"] = r.get("Collections") or "—"
        
        active_students = r.get("ActiveStudents") or 0
        books_issued = r.get("BooksIssued") or 0
        r["IssuesPerStudent"] = round(books_issued / active_students, 2) if active_students else 0.0
    
    return rows


def get_top_darajah_summary_excluding_asateza(limit: int = 10, hijri_year: Optional[int] = None) -> List[Dict]:
    """Wrapper for top darajah excluding Asateza."""
    return get_top_darajah_summary(limit=limit, exclude_asateza=True, hijri_year=hijri_year)


def get_top_darajah_summary_with_asateza_last(limit: int = 10, hijri_year: Optional[int] = None) -> List[Dict]:
    """Top Darajah summary with Asateza placed last."""
    rows = get_top_darajah_summary(limit=limit + 5, hijri_year=hijri_year)
    
    asateza_rows = []
    other_rows = []
    
    for row in rows:
        darajah_name = (row.get("Darajah") or "").strip().upper()
        if darajah_name in ("ASATEZA", "AJSN"):
            asateza_rows.append(row)
        else:
            other_rows.append(row)
    
    result = other_rows[:limit]
    if asateza_rows:
        result.append(asateza_rows[0])
    
    return result


# -------------------------------
# LEGACY FUNCTIONS (Unchanged)
# -------------------------------

def darajah_issues() -> List[Tuple[str, int]]:
    """Issues grouped by Darajah for current AY."""
    start, end = get_ay_bounds()
    if not start:
        return []
    
    with get_db_cursor(dictionary=False) as cur:
        cur.execute("""
            SELECT
                COALESCE(std.attribute, b.branchcode, 'Unknown') AS darajah_name,
                COUNT(*) AS cnt
            FROM statistics s
            JOIN borrowers b ON b.borrowernumber = s.borrowernumber
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('Class', 'STD', 'CLASS', 'DAR', 'CLASS_STD')
            WHERE s.type = 'issue'
              AND DATE(s.`datetime`) BETWEEN %s AND %s
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            GROUP BY darajah_name
            ORDER BY cnt DESC
        """, (start, end))
        
        rows = cur.fetchall()
    
    return rows


def marhalas_breakdown() -> List[Tuple[str, int]]:
    """Distribution of patrons by marhala (category) in current AY."""
    start, end = get_ay_bounds()
    if not start:
        return []
    
    with get_db_cursor(dictionary=False) as cur:
        cur.execute("""
            SELECT
                COALESCE(c.description, b.categorycode, 'Unknown') AS marhala,
                COUNT(DISTINCT s.borrowernumber) AS cnt
            FROM statistics s
            JOIN borrowers b ON b.borrowernumber = s.borrowernumber
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            WHERE s.type = 'issue'
              AND DATE(s.`datetime`) BETWEEN %s AND %s
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            GROUP BY marhala
            ORDER BY cnt DESC
        """, (start, end))
        
        rows = cur.fetchall()
    
    return rows


def borrowing_trend_monthly() -> List[Tuple[str, int]]:
    """Issues per month (YYYY-MM) for current AY."""
    start, end = get_ay_bounds()
    if not start:
        return []
    
    with get_db_cursor(dictionary=False) as cur:
        cur.execute("""
            SELECT DATE_FORMAT(s.`datetime`, '%Y-%m') AS ym,
                   COUNT(*) AS cnt
            FROM statistics s
            WHERE s.type = 'issue'
              AND DATE(s.`datetime`) BETWEEN %s AND %s
            GROUP BY ym
            ORDER BY ym ASC
        """, (start, end))
        
        rows = cur.fetchall()
    
    return rows


def darajah_buckets() -> List[Tuple[str, int]]:
    """Bucket counts of patrons per Darajah group based on STD attribute."""
    with get_db_cursor(dictionary=False) as cur:
        cur.execute("""
            SELECT
                CASE
                    WHEN CAST(std.attribute AS UNSIGNED) BETWEEN 1 AND 2 THEN 'Darajah 1–2'
                    WHEN CAST(std.attribute AS UNSIGNED) BETWEEN 3 AND 4 THEN 'Darajah 3–4'
                    WHEN CAST(std.attribute AS UNSIGNED) BETWEEN 5 AND 7 THEN 'Darajah 5–7'
                    WHEN CAST(std.attribute AS UNSIGNED) BETWEEN 8 AND 11 THEN 'Darajah 8–11'
                    ELSE 'Unassigned'
                END AS darajah_group,
                COUNT(*) AS patrons
            FROM borrowers b
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber AND std.code IN ('Class', 'STD')
            WHERE (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            GROUP BY darajah_group
            ORDER BY MIN(CASE darajah_group
                WHEN 'Darajah 1–2' THEN 1
                WHEN 'Darajah 3–4' THEN 2
                WHEN 'Darajah 5–7' THEN 3
                WHEN 'Darajah 8–11' THEN 4
                ELSE 9 END)
        """)
        
        rows = cur.fetchall()
    
    return rows


def darajah_max_books() -> List[Tuple[str, int]]:
    """Return maximum allowed books per Darajah group."""
    return [
        ('Darajah 1–2', 3),
        ('Darajah 3–4', 4),
        ('Darajah 5–7', 5),
        ('Darajah 8–11', 6),
    ]


def verify_patron_counts() -> Dict[str, Any]:
    """Verify patron counts match Koha database."""
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT 
                COUNT(*) AS total_in_database,
                SUM(CASE 
                    WHEN (dateexpiry IS NULL OR dateexpiry >= CURDATE())
                        AND (debarred IS NULL OR debarred = 0)
                        AND (gonenoaddress IS NULL OR gonenoaddress = 0)
                    THEN 1 ELSE 0 
                END) AS active_patrons,
                SUM(CASE 
                    WHEN dateexpiry < CURDATE() AND dateexpiry IS NOT NULL 
                    THEN 1 ELSE 0 
                END) AS expired_patrons,
                SUM(CASE 
                    WHEN debarred = 1 THEN 1 ELSE 0 
                END) AS debarred_patrons,
                SUM(CASE 
                    WHEN gonenoaddress = 1 THEN 1 ELSE 0 
                END) AS gonenoaddress_patrons
            FROM borrowers
        """)
        
        summary = cur.fetchone()
        
        cur.execute("""
            SELECT 
                b.categorycode,
                c.description AS category,
                COUNT(*) AS count
            FROM borrowers b
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            WHERE (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            GROUP BY b.categorycode, c.description
            ORDER BY count DESC
        """)
        
        summary["category_breakdown"] = cur.fetchall()
        summary["verification_note"] = (
            "Active patrons should match Koha's count of non-expired, "
            "non-debarred, non-gonenoaddress patrons."
        )
    
    return summary


def get_all_active_patrons(limit: int = 1000) -> List[Dict]:
    """Get all active patrons with basic info."""
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT 
                b.borrowernumber,
                b.cardnumber,
                b.surname,
                b.firstname,
                CONCAT(b.surname, ' ', b.firstname) AS FullName,
                b.categorycode,
                c.description AS category,
                b.dateexpiry,
                b.debarred,
                b.gonenoaddress,
                b.branchcode,
                std.attribute AS std_attribute,
                trno.attribute AS trno_attribute
            FROM borrowers b
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            LEFT JOIN borrower_attributes std 
                ON std.borrowernumber = b.borrowernumber AND std.code IN ('Class', 'STD')
            LEFT JOIN borrower_attributes trno 
                ON trno.borrowernumber = b.borrowernumber AND trno.code = 'TRNO'
            WHERE (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            ORDER BY b.surname, b.firstname
            LIMIT %s
        """, (limit,))
        
        rows = cur.fetchall()
    
    return rows


def sip_stats(days: int = 90) -> List[Tuple[str, int]]:
    """SIP2 issue/return/renew counts in the last N days."""
    with get_db_cursor(dictionary=False) as cur:
        cur.execute(sql_named("sip_activity_counts"), (int(days),))
        rows = cur.fetchall()
    
    return rows


def today_activity() -> Tuple[int, int]:
    """Return (today_checkouts, today_checkins) using statistics.type ('issue'/'return')."""
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT
              SUM(CASE WHEN type='issue' THEN 1 ELSE 0 END) AS checkouts,
              SUM(CASE WHEN type='return' THEN 1 ELSE 0 END) AS checkins
            FROM statistics
            WHERE DATE(`datetime`) = CURDATE();
        """)
        row = cur.fetchone()
        cur.close()
        
        checkouts = int(row.get("checkouts") or 0) if row else 0
        checkins = int(row.get("checkins") or 0) if row else 0
        return checkouts, checkins
    finally:
        conn.close()


def find_student_by_identifier(identifier: str) -> Optional[dict]:
    """Find student by cardnumber / userid / borrowernumber / TRNO."""
    conn = None
    cur = None
    try:
        conn = get_koha_conn()
        cur = conn.cursor(dictionary=True)
        
        cur.execute("""
            SELECT
                b.borrowernumber,
                b.cardnumber,
                b.userid,
                b.surname,
                b.firstname,
                b.email,
                b.categorycode,
                c.description AS category,
                COALESCE(std.attribute, b.branchcode) AS darajah,
                b.dateexpiry,
                b.debarred,
                b.gonenoaddress,
                trno.attribute AS trno
            FROM borrowers b
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber AND std.code IN ('Class', 'STD', 'CLASS', 'DAR')
            LEFT JOIN borrower_attributes trno
                ON trno.borrowernumber = b.borrowernumber AND trno.code IN ('TRNO', 'TRN', 'TR_NUMBER', 'TR')
            WHERE (LOWER(b.cardnumber) = LOWER(%s)
               OR LOWER(b.userid) = LOWER(%s)
               OR CAST(b.borrowernumber AS CHAR) = %s
               OR LOWER(trno.attribute) = LOWER(%s))
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            LIMIT 1
        """, (identifier, identifier, identifier, identifier))
        
        row = cur.fetchone()
        return row
        
    except Exception as e:
        logger.error(f"Error finding student by identifier {identifier}: {e}")
        return None
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def borrowed_books_for(borrowernumber: int) -> List[dict]:
    """Get active loans + past borrowed books for a student for history view."""
    start, end = get_ay_bounds()
    with get_db_cursor() as cur:
        # Fetch Active Loans (regardless of date)
        cur.execute("""
            SELECT bi.title, i.issuedate AS date_issued, 
                   i.date_due AS date_due,
                   0 AS returned
            FROM issues i
            JOIN items it ON i.itemnumber = it.itemnumber
            JOIN biblio bi ON it.biblionumber = bi.biblionumber
            WHERE i.borrowernumber = %s
        """, (borrowernumber,))
        active = cur.fetchall()

        # Fetch Statistics (within current AY)
        cur.execute("""
            SELECT bi.title, s.datetime AS date_issued, 
                   NULL AS date_due,
                   1 AS returned
            FROM statistics s
            JOIN items it ON s.itemnumber = it.itemnumber
            JOIN biblio bi ON it.biblionumber = bi.biblionumber
            WHERE s.borrowernumber = %s
              AND s.type = 'issue'
              AND DATE(s.datetime) BETWEEN %s AND %s
            ORDER BY s.datetime DESC
        """, (borrowernumber, start, end))
        past = cur.fetchall()
        
        history = active + past
        history.sort(key=lambda x: x["date_issued"] or datetime.min, reverse=True)
    
    return history


def darajah_dataframe(darajah_std: str) -> list:
    """Return list of students in a darajah (STD attribute) with totals."""
    start, end = get_ay_bounds()
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT
                b.borrowernumber,
                b.cardnumber,
                CONCAT(b.surname, ' ', b.firstname) AS FullName,
                b.email AS EduEmail,
                b.categorycode,
                c.description AS category,
                COALESCE(std.attribute, b.branchcode) AS darajah,
                COALESCE(x.total_issues_ay, 0) AS TotalIssues,
                COALESCE(x.fees_paid_ay, 0) AS TotalFeesPaid,
                b.dateexpiry,
                b.debarred,
                b.gonenoaddress,
                trno.attribute AS trno
            FROM borrowers b
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber AND std.code IN ('Class', 'STD')
            LEFT JOIN borrower_attributes trno
                ON trno.borrowernumber = b.borrowernumber AND trno.code = 'TRNO'
            LEFT JOIN (
                SELECT s.borrowernumber,
                       COUNT(*) AS total_issues_ay,
                       COALESCE(SUM(CASE WHEN al.credit_type_code = 'PAYMENT' AND DATE(al.date) BETWEEN %s AND %s THEN al.amount END), 0) AS fees_paid_ay
                FROM statistics s
                LEFT JOIN accountlines al ON s.borrowernumber = al.borrowernumber
                WHERE s.type = 'issue' AND DATE(s.datetime) BETWEEN %s AND %s
                GROUP BY s.borrowernumber
            ) x ON x.borrowernumber = b.borrowernumber
            WHERE (std.attribute = %s OR b.branchcode = %s)
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            ORDER BY FullName ASC
        """, (start, end, start, end, darajah_std, darajah_std))
        
        rows = cur.fetchall()
    
    return rows


def marhala_dataframe(marhala: str) -> list:
    """Return list of students in a marhala (by category)."""
    start, end = get_ay_bounds()
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT
                b.borrowernumber,
                b.cardnumber,
                CONCAT(b.surname, ' ', b.firstname) AS FullName,
                b.email AS EduEmail,
                b.categorycode,
                c.description AS category,
                COALESCE(std.attribute, b.branchcode) AS darajah,
                COALESCE(x.total_issues_ay, 0) AS TotalIssues,
                COALESCE(x.fees_paid_ay, 0) AS TotalFeesPaid,
                b.dateexpiry,
                b.debarred,
                b.gonenoaddress,
                trno.attribute AS trno
            FROM borrowers b
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber AND std.code IN ('Class', 'STD')
            LEFT JOIN borrower_attributes trno
                ON trno.borrowernumber = b.borrowernumber AND trno.code = 'TRNO'
            LEFT JOIN (
                SELECT s.borrowernumber,
                       COUNT(*) AS total_issues_ay,
                       COALESCE(SUM(CASE WHEN al.credit_type_code = 'PAYMENT' AND DATE(al.date) BETWEEN %s AND %s THEN al.amount END), 0) AS fees_paid_ay
                FROM statistics s
                LEFT JOIN accountlines al ON s.borrowernumber = al.borrowernumber
                WHERE s.type = 'issue' AND DATE(s.datetime) BETWEEN %s AND %s
                GROUP BY s.borrowernumber
            ) x ON x.borrowernumber = b.borrowernumber
            WHERE (c.description = %s OR b.categorycode = %s)
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            ORDER BY FullName ASC
        """, (start, end, start, end, marhala, marhala))
        
        rows = cur.fetchall()
    
    return rows


def patron_title_agg(from_date, to_date, exclude_category: str = "T-KG") -> list:
    """Python wrapper over koha.sql:patron_title_agg_between_dates."""
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT
                b.borrowernumber,
                b.cardnumber,
                CONCAT_WS(' ', b.surname, b.firstname) AS patron_name,
                std.attribute AS darajah_std,
                trno.attribute AS trno,
                COALESCE(COUNT(d.biblionumber), 0) AS issued_count,
                GROUP_CONCAT(
                    CONCAT(bib.title, ' (', DATE_FORMAT(d.first_issued, '%d-%b-%Y'), ')')
                    ORDER BY bib.title SEPARATOR ' • '
                ) AS titles_list
            FROM borrowers b
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber AND std.code IN ('Class', 'STD')
            LEFT JOIN borrower_attributes trno
                ON trno.borrowernumber = b.borrowernumber AND trno.code = 'TRNO'
            LEFT JOIN (
                SELECT s.borrowernumber, it.biblionumber, MIN(DATE(s.datetime)) AS first_issued
                FROM statistics s
                JOIN items it ON it.itemnumber = s.itemnumber
                WHERE s.type = 'issue'
                    AND DATE(s.datetime) BETWEEN %s AND %s
                GROUP BY s.borrowernumber, it.biblionumber
            ) d ON d.borrowernumber = b.borrowernumber
            LEFT JOIN biblio bib ON bib.biblionumber = d.biblionumber
            WHERE b.categorycode <> %s
                AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                AND (b.debarred IS NULL OR b.debarred = 0)
                AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            GROUP BY b.borrowernumber
            ORDER BY darajah_std, patron_name
        """, (from_date, to_date, exclude_category))
        
        rows = cur.fetchall()
    
    return rows


def get_all_darajahs() -> List[Dict]:
    """Get all distinct darajahs from Koha database."""
    start, end = get_ay_bounds()
    
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT 
                COALESCE(std.attribute, b.branchcode) AS darajah_name,
                COUNT(DISTINCT b.borrowernumber) AS total_students,
                COUNT(DISTINCT CASE 
                    WHEN (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                        AND (b.debarred IS NULL OR b.debarred = 0)
                        AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
                    THEN b.borrowernumber 
                END) AS active_students,
                SUM(COALESCE(s_agg.ay_issues, 0)) AS ay_issues
            FROM borrowers b
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('Class', 'STD', 'CLASS', 'DAR', 'CLASS_STD')
            LEFT JOIN (
                SELECT borrowernumber, 
                       COUNT(*) AS ay_issues
                FROM statistics
                WHERE type = 'issue' AND DATE(`datetime`) BETWEEN %s AND %s
                GROUP BY borrowernumber
            ) s_agg ON b.borrowernumber = s_agg.borrowernumber
            WHERE (std.attribute IS NOT NULL OR b.branchcode IS NOT NULL)
                AND (std.attribute != '' OR b.branchcode != '')
            GROUP BY darajah_name
            ORDER BY 
                CAST(SUBSTRING_INDEX(darajah_name, ' ', 1) AS UNSIGNED),
                darajah_name
        """, (start, end, start, end))
        
        darajahs = cur.fetchall()
    
    for darajah in darajahs:
        name = darajah.get("darajah_name", "")
        name_str = str(name)
        
        if " M" in name_str or name_str.endswith("M"):
            darajah["gender"] = "Boys"
            darajah["icon"] = "male"
        elif " F" in name_str or name_str.endswith("F"):
            darajah["gender"] = "Girls"
            darajah["icon"] = "female"
        else:
            darajah["gender"] = "Mixed"
            darajah["icon"] = "users"
        
        if " A" in name_str:
            darajah["section"] = "A"
        elif " B" in name_str:
            darajah["section"] = "B"
        elif " C" in name_str:
            darajah["section"] = "C"
        else:
            darajah["section"] = ""
        
        year_match = re.search(r'\d+', name_str)
        darajah["year"] = year_match.group() if year_match else ""
    
    return darajahs


def get_marhala_distribution() -> Tuple[List[str], List[int]]:
    """Return (labels, values) for Marhala/Darajah distribution based on issue counts."""
    labels, values = get_marhala_distribution_with_dars_burhani()
    
    if not labels:
        buckets = darajah_buckets()
        labels = [b[0] for b in buckets if b[1] > 0]
        values = [b[1] for b in buckets if b[1] > 0]
    
    return labels, values