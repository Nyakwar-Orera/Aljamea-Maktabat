# services/koha_queries.py - OPTIMIZED VERSION
from typing import Dict, List, Optional, Tuple, Any, Union
from contextlib import contextmanager
from db_koha import get_conn
import os
from datetime import date, datetime
import re

try:
    from hijri_converter import convert as hijri_convert
except ImportError:
    hijri_convert = None

HIJRI_MONTHS = [
    "Muḥarram al-Harām", "Safar al-Muzaffar", "Rabi al-Awwal", "Rabī al-Akhar",
    "Jamādil Awwal", "Jamādā al-ʾŪkhrā", "Rajab al-Asab", "Shabān al-Karim",
    "Shehrullah al-Moazzam", "Shawwāl al-Mukarram", "Zilqādah al-Harām", "Zilhijjatil Harām",
]

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


# ---------- SQL Loader (unchanged) ----------
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
# ACADEMIC YEAR HELPER - FIXED
# -------------------------------


def get_ay_bounds() -> Tuple[Optional[date], Optional[date]]:
    """
    Academic Year follows the Hijri cycle starting from 1st Shawwal.
    For 1447 H, this started on March 20, 2026.
    """
    today = date.today()
    
    # For now, let's align with the user's current session (1447 H)
    # which we know started in March 2026.
    # In a production system, this would be computed by converting 1-10-HYear to Gregorian.
    
    if today.year >= 2026:
        # User is in the 1447 H session
        current_start = date(2026, 3, 20)
        current_end = date(2027, 3, 10) # Approx end of 1447 H
    else:
        # Fallback to previous logic for other years
        current_start = date(today.year, 4, 1)
        current_end = date(today.year, 12, 31)
        if today.month < 4:
            current_start = date(today.year - 1, 4, 1)
            current_end = date(today.year - 1, 12, 31)
            
    return current_start, min(today, current_end)



# -------------------------------
# HIJRI HELPER FUNCTIONS (unchanged)
# -------------------------------

def get_hijri_month_year_label(d: date) -> str:
    """Get professional Hijri month-year label for the given date."""
    if not hijri_convert or not d:
        return d.strftime("%B %Y") if d else ""
    try:
        h = hijri_convert.Gregorian(d.year, d.month, d.day).to_hijri()
        return f"{HIJRI_MONTHS[h.month - 1]} {h.year} H"
    except Exception:
        return d.strftime("%B %Y")


def get_hijri_date_label(d: date) -> str:
    """Get full Hijri date label (Day Month Year H)."""
    if not hijri_convert or not d:
        return d.strftime("%d %B %Y") if d else ""
    try:
        h = hijri_convert.Gregorian(d.year, d.month, d.day).to_hijri()
        return f"{h.day} {HIJRI_MONTHS[h.month - 1]} {h.year} H"
    except Exception:
        return d.strftime("%d %B %Y")


# -------------------------------
# PATRON COUNT QUERIES - OPTIMIZED
# -------------------------------

def get_patron_counts(marhala_name: Optional[str] = None) -> Dict[str, int]:
    """
    Get all patron counts in a single optimized query.
    Returns dict with total, student, non-student, and active counts.
    """
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
        
        return {
            "total_patrons": int(result.get("total_all", 0)),
            "active_patrons": int(result.get("active_total", 0)),
            "student_patrons": int(result.get("active_students", 0)),
            "non_student_patrons": int(result.get("active_non_students", 0)),
        }


def get_total_patrons_count() -> int:
    """Legacy wrapper - get total active patrons count."""
    return get_patron_counts()["active_patrons"]


def get_student_patrons_count() -> int:
    """Legacy wrapper - get active student patrons count."""
    return get_patron_counts()["student_patrons"]


def get_non_student_patrons_count() -> int:
    """Legacy wrapper - get active non-student patrons count."""
    return get_patron_counts()["non_student_patrons"]


# -------------------------------
# DASHBOARD SUMMARY QUERIES - OPTIMIZED
# -------------------------------

def get_summary(marhala_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Return library summary stats with optimized single-pass approach.
    Uses multiple focused queries instead of one giant query.
    """
    # Get patron counts in one query
    patron_counts = get_patron_counts(marhala_name)
    
    start, end = get_ay_bounds()
    
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
              AND i.date_due < NOW()
              AND DATE(i.issuedate) BETWEEN %s AND %s
        """
        overdue_params = [start, end]
        if marhala_name:
            overdue_query += " AND (c.description = %s OR b.categorycode = %s)"
            overdue_params.extend([marhala_name, marhala_name])
        
        cur.execute(overdue_query, overdue_params)
        overdue = int(cur.fetchone()["c"] or 0)
        
        # Get currently issued count (Filtered by AY and Marhala)
        issued_query = """
            SELECT COUNT(*) AS c
            FROM issues i
            JOIN borrowers b ON i.borrowernumber = b.borrowernumber
            LEFT JOIN categories c ON b.categorycode = c.categorycode
            WHERE i.returndate IS NULL
              AND DATE(i.issuedate) BETWEEN %s AND %s
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
            """
            active_p = [start, end]
            if marhala_name:
                active_q += " AND (c.description = %s OR b.categorycode = %s)"
                active_p.extend([marhala_name, marhala_name])
            
            cur.execute(active_q, active_p)
            active_patrons_ay = int(cur.fetchone()["c"] or 0)
            
            # Total issues in AY
            issues_q = """
                SELECT COUNT(*) AS c
                FROM statistics s
                JOIN borrowers b ON s.borrowernumber = b.borrowernumber
                LEFT JOIN categories c ON b.categorycode = c.categorycode
                WHERE s.type = 'issue'
                  AND DATE(s.`datetime`) BETWEEN %s AND %s
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

    return {
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


def get_summary_with_updated_terms() -> Dict[str, Any]:
    """Legacy wrapper - summary with currently_issued already included."""
    return get_summary()


# -------------------------------
# DARAJAH GROUP CONSTANTS - CENTRALIZED
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
    
    # Extract numeric part
    match = re.search(r'\d+', str(std_attr))
    if not match:
        return "Unassigned"
    
    n = int(match.group())
    
    for group_name, (low, high) in DARAJAH_GROUPS.items():
        if low <= n <= high:
            return group_name
    
    return "Other"

def get_department_currently_issued(marhala_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Get currently issued books by marhala.
    
    Args:
        marhala_name: Optional marhala name/code to filter by
        
    Returns:
        Dict with 'marhalas' list and 'total_currently_issued' count
    """
    start, end = get_ay_bounds()
    conn = get_conn()
    try:
        cur = conn.cursor()
        
        query = """
        SELECT
            COALESCE(c.description, b.categorycode, 'Unknown') AS Marhala,
            COUNT(*) AS CurrentlyIssued,
            GROUP_CONCAT(DISTINCT it.ccode ORDER BY it.ccode SEPARATOR ', ') AS Collections
        FROM issues i
        JOIN borrowers b ON i.borrowernumber = b.borrowernumber
        LEFT JOIN categories c ON c.categorycode = b.categorycode
        JOIN items it ON i.itemnumber = it.itemnumber
        WHERE i.returndate IS NULL
          AND DATE(i.issuedate) BETWEEN %s AND %s
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
        
        total_issued = sum(row['CurrentlyIssued'] for row in rows)
        
        return {
            "marhalas": rows,
            "total_currently_issued": total_issued
        }
    finally:
        conn.close()

# -------------------------------
# MARHALA & DARAJAH FUNCTIONS - OPTIMIZED
# -------------------------------

def get_marhala_distribution_with_dars_burhani() -> Tuple[List[str], List[int]]:
    """Return Marhala distribution including Dars Burhani."""
    start, end = get_ay_bounds()
    if not start:
        return [], []

    academic_marhalas = get_academic_marhalas()
    
    with get_db_cursor() as cur:
        # Get issues per category code
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
            GROUP BY c.categorycode, c.description
        """, (start, end, *academic_marhalas))
        
        result = {row["marhala_name"]: row["total_issues"] for row in cur.fetchall()}
    
    # Standard marhala list in desired order
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
    
    return labels, values


def get_darajah_summary_by_marhala(marhala_name: Optional[str] = None) -> List[Dict]:
    """Get Darajah summary filtered by Marhala - OPTIMIZED."""
    start, end = get_ay_bounds()
    if not start:
        return []

    academic_codes = get_academic_marhalas()
    non_academic_codes = get_non_academic_marhalas()
    all_codes = academic_codes + non_academic_codes
    
    with get_db_cursor() as cur:
        # Build parameterized query with IN clause
        placeholders = ', '.join(['%s'] * len(all_codes))
        
        # Base query without WHERE filter
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
                    AND std.code IN ('STD', 'CLASS', 'DAR', 'CLASS_STD')
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
        
        # Add WHERE clause if marhala_name is provided
        if marhala_name:
            query += " WHERE marhala = %s"
            params.append(marhala_name)
        
        # Add GROUP BY and ORDER BY
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
    
    return rows

def get_top_darajah_summary(limit: int = 10, exclude_asateza: bool = False) -> List[Dict]:
    """
    Unified function to get top darajah summary with optional Asateza exclusion.
    """
    start, end = get_ay_bounds()
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
                AND std.code IN ('STD', 'CLASS', 'DAR', 'CLASS_STD')
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


def get_top_darajah_summary_excluding_asateza(limit: int = 10) -> List[Dict]:
    """Wrapper for top darajah excluding Asateza."""
    return get_top_darajah_summary(limit=limit, exclude_asateza=True)


def get_top_darajah_summary_with_asateza_last(limit: int = 10) -> List[Dict]:
    """Top Darajah summary with Asateza placed last."""
    rows = get_top_darajah_summary(limit=limit + 5)
    
    # Separate Asateza
    asateza_rows = []
    other_rows = []
    
    for row in rows:
        darajah_name = (row.get("Darajah") or "").strip().upper()
        if darajah_name in ("ASATEZA", "AJSN"):
            asateza_rows.append(row)
        else:
            other_rows.append(row)
    
    # Return others first, then Asateza
    result = other_rows[:limit]
    if asateza_rows:
        result.append(asateza_rows[0])
    
    return result


# -------------------------------
# DEPARTMENT PERFORMANCE FUNCTIONS - OPTIMIZED
# -------------------------------

def get_department_performance(category_codes: List[str]) -> List[Dict]:
    """
    Generic function to get performance for a list of category codes.
    Used by both academic and non-academic department queries.
    """
    start, end = get_ay_bounds()
    if not start or not category_codes:
        return []
    
    with get_db_cursor() as cur:
        placeholders = ', '.join(['%s'] * len(category_codes))
        
        # Use a single CTE for efficiency
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


def get_academic_departments_performance() -> List[Dict]:
    """Get performance for academic marhalas."""
    return get_department_performance(get_academic_marhalas())


def get_non_academic_departments_performance() -> List[Dict]:
    """Get performance for non-academic marhalas."""
    return get_department_performance(get_non_academic_marhalas())


# -------------------------------
# GENDER-SPECIFIC DARAJAH FUNCTIONS - OPTIMIZED
# -------------------------------

def get_gender_darajah_distribution() -> Tuple[List[str], List[int], List[int]]:
    """
    Get Darajah distribution by gender with ranges:
    - Females: Darajah 1-7 only
    - Males: Darajah 1-11
    """
    start, end = get_ay_bounds()
    if not start:
        return [], [], []

    with get_db_cursor() as cur:
        # Get all darajahs with their gender info in one query
        cur.execute("""
            SELECT
                COALESCE(std.attribute, b.branchcode, 'Unknown') AS darajah_name,
                UPPER(COALESCE(b.sex, '')) AS gender,
                COUNT(*) AS cnt
            FROM statistics s
            JOIN borrowers b ON b.borrowernumber = s.borrowernumber
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('STD', 'CLASS', 'DAR', 'CLASS_STD')
            WHERE s.type = 'issue'
              AND DATE(s.`datetime`) BETWEEN %s AND %s
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
              AND COALESCE(std.attribute, b.branchcode) != 'AJSN'
            GROUP BY darajah_name, gender
        """, (start, end))
        
        rows = cur.fetchall()
    
    # Process data with gender-specific ranges
    darajah_data = {}
    
    for row in rows:
        darajah_name = (row["darajah_name"] or "Unknown").strip()
        gender = row["gender"]
        cnt = int(row["cnt"])
        
        if darajah_name not in darajah_data:
            darajah_data[darajah_name] = {'M': 0, 'F': 0}
        
        # Extract darajah number
        match = re.search(r'\d+', darajah_name)
        if not match:
            continue
        
        darajah_num = int(match.group())
        
        # Apply gender-specific ranges
        if gender == 'M' and 1 <= darajah_num <= 11:
            darajah_data[darajah_name]['M'] += cnt
        elif gender == 'F' and 1 <= darajah_num <= 7:
            darajah_data[darajah_name]['F'] += cnt
        elif not gender:  # Unknown gender - try to infer from name
            darajah_name_upper = darajah_name.upper()
            if "F" in darajah_name_upper and 1 <= darajah_num <= 7:
                darajah_data[darajah_name]['F'] += cnt
            elif "M" in darajah_name_upper and 1 <= darajah_num <= 11:
                darajah_data[darajah_name]['M'] += cnt
    
    # Sort darajahs numerically
    def darajah_sort_key(name: str):
        match = re.search(r'\d+', name)
        return (0, int(match.group())) if match else (1, name)
    
    darajah_names = sorted(darajah_data.keys(), key=darajah_sort_key)
    
    male_counts = [darajah_data[name]['M'] for name in darajah_names]
    female_counts = [darajah_data[name]['F'] for name in darajah_names]
    
    return darajah_names, male_counts, female_counts


# -------------------------------
# MARHALA HELPER FUNCTIONS - UPDATED
# -------------------------------

def get_academic_marhalas() -> List[str]:
    """Get academic marhala category codes."""
    return [
        'S-CO',   # Collegiate I (5-7)
        'S-CGB',  # Culture Générale (Std 3-4)
        'S-CGA',  # Culture Générale (Std 1-2)
        'S-CT',   # Collegiate II & Higher Studies (Std 8-11)
        'S-DARS'  # Dars Burhani
    ]


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


# -------------------------------
# KEY INSIGHTS - OPTIMIZED
# -------------------------------

def get_key_insights() -> List[str]:
    """Generate key insights with optimized data fetching."""
    insights = []
    
    # Get all data in parallel where possible
    start, end = get_ay_bounds()
    
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
                    AND std.code IN ('STD', 'CLASS', 'DAR', 'CLASS_STD')
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
            
            # Get currently issued books (Filtered by AY)
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
    
    return insights


# -------------------------------
# TOP TITLES - SECURE VERSION
# -------------------------------

def top_titles(
    limit: int = 25, arabic: bool = False, non_arabic: bool = False
) -> List[Tuple[str, int, str]]:
    """
    Top borrowed titles for current AY with secure parameterization.
    """
    start, end = get_ay_bounds()
    if not start:
        return []
    
    # Build language filter securely
    lang_condition = ""
    params = [start, end]
    
    if arabic:
        lang_condition = "AND bib.title REGEXP ?"
        params.append('[ء-ي]')
    elif non_arabic:
        lang_condition = "AND bib.title NOT REGEXP ?"
        params.append('[ء-ي]')
    
    params.append(int(limit))
    
    with get_db_cursor(dictionary=False) as cur:
        cur.execute(f"""
            SELECT
                bib.title,
                COUNT(*) AS cnt,
                MAX(all_iss.issuedate) AS last_issued
            FROM (
                SELECT borrowernumber, itemnumber, issuedate
                FROM issues
                UNION ALL
                SELECT borrowernumber, itemnumber, issuedate
                FROM old_issues
            ) all_iss
            JOIN items it ON all_iss.itemnumber = it.itemnumber
            JOIN biblio bib ON it.biblionumber = bib.biblionumber
            WHERE DATE(all_iss.issuedate) BETWEEN ? AND ?
            {lang_condition}
            GROUP BY bib.biblionumber, bib.title
            ORDER BY cnt DESC
            LIMIT ?
        """, params)
        
        rows = cur.fetchall()
    
    return rows


# -------------------------------
# ARABIC/ENGLISH TOP 25 - OPTIMIZED
# -------------------------------

def _top_titles_by_language(language_code: str, limit: int = 25, marhala_name: Optional[str] = None) -> List[Dict]:
    """
    Generic function for top titles by MARC 041$a language code.
    """
    start, end = get_ay_bounds()
    if not start:
        return []
    
    with get_db_cursor() as cur:
        query = """
            SELECT
                bib.biblionumber AS BiblioNumber,
                bib.title AS Title,
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
            GROUP BY bib.biblionumber, bib.title
            ORDER BY Times_Issued DESC
            LIMIT %s
        """
        params.append(limit)
        
        cur.execute(query, params)
        rows = cur.fetchall()
    
    return rows


def get_language_top25(marhala_name: Optional[str] = None) -> Dict[str, Dict[str, List]]:
    """Combine Arabic and English top 25 into expected structure with optional mapping."""
    def pack(rows: List[Dict]) -> Dict[str, List]:
        titles = [r.get("Title") for r in rows]
        counts = [int(r.get("Times_Issued") or 0) for r in rows]
        return {
            "titles": titles,
            "counts": counts,
            "records": rows,
        }
    
    return {
        "arabic": pack(arabic_top25(marhala_name)),
        "english": pack(english_top25(marhala_name)),
    }


def arabic_top25(marhala_name: Optional[str] = None) -> List[Dict]:
    """Top 25 Arabic titles."""
    return _top_titles_by_language('Arabic', 25, marhala_name)


def english_top25(marhala_name: Optional[str] = None) -> List[Dict]:
    """Top 25 English titles."""
    return _top_titles_by_language('English', 25, marhala_name)


# -------------------------------
# OPTIMIZED MARHALA STATS FUNCTIONS
# -------------------------------

def get_all_marhalas_with_stats() -> List[Dict]:
    """
    Get all marhalas with detailed statistics using optimized batch queries.
    """
    with get_db_cursor() as cur:
        # Get all categories
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
        
        # Get borrower counts
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
        
        # Get AY issues and current issues in one pass
        start, end = get_ay_bounds()
        if start and end and marhalas:
            placeholders = ', '.join(['%s'] * len(marhala_map.keys()))
            
            cur.execute(f"""
                SELECT 
                    b.categorycode,
                    SUM(CASE 
                        WHEN s.type = 'issue' 
                            AND DATE(s.`datetime`) BETWEEN %s AND %s
                        THEN 1 ELSE 0 
                    END) AS ay_issues,
                    SUM(CASE 
                        WHEN i.returndate IS NULL THEN 1 ELSE 0 
                    END) AS currently_issued,
                    SUM(CASE 
                        WHEN i.returndate IS NULL AND i.date_due < CURDATE() 
                        THEN 1 ELSE 0 
                    END) AS overdues
                FROM borrowers b
                LEFT JOIN statistics s ON b.borrowernumber = s.borrowernumber
                LEFT JOIN issues i ON b.borrowernumber = i.borrowernumber
                WHERE b.categorycode IN ({placeholders})
                GROUP BY b.categorycode
            """, (start, end, *marhala_map.keys()))
            
            for row in cur.fetchall():
                code = row["categorycode"]
                if code in marhala_map:
                    marhala_map[code]["ay_issues"] = int(row["ay_issues"] or 0)
                    marhala_map[code]["currently_issued"] = int(row["currently_issued"] or 0)
                    marhala_map[code]["overdues"] = int(row["overdues"] or 0)
    
    # Add type metadata (in Python for performance)
    academic_codes = set(get_academic_marhalas())
    non_academic_codes = set(get_non_academic_marhalas())
    
    for marhala in marhalas:
        code = marhala.get("marhala_code")
        name = marhala.get("marhala_name", "").lower()
        
        if code in academic_codes:
            marhala["type"] = "Academic"
            marhala["icon"] = "graduation-cap"
        elif code in non_academic_codes:
            if code == 'L' or code == 'S':
                marhala["type"] = "Library"
                marhala["icon"] = "book"
            else:
                marhala["type"] = "Staff"
                marhala["icon"] = "user-tie"
        else:
            # Fallback detection
            if any(k in name for k in ['student', 'std', 'darajah', 'grade', 'collegiate', 'culture', 'dars']):
                marhala["type"] = "Academic"
                marhala["icon"] = "graduation-cap"
            elif any(k in name for k in ['faculty', 'staff', 'teacher', 'asateza']):
                marhala["type"] = "Staff"
                marhala["icon"] = "user-tie"
            elif any(k in name for k in ['library', 'maktabat']):
                marhala["type"] = "Library"
                marhala["icon"] = "book"
            else:
                marhala["type"] = "Other"
                marhala["icon"] = "users"
    
    return marhalas


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


def get_darajahs_in_marhala(marhala_code: str) -> List[Dict]:
    """Get all darajahs belonging to a specific marhala."""
    start, end = get_ay_bounds()
    
    with get_db_cursor() as cur:
        # Get darajah list and stats in one query
        cur.execute("""
            SELECT 
                COALESCE(std.attribute, b.branchcode) AS darajah_name,
                COUNT(DISTINCT b.borrowernumber) AS total_students,
                COUNT(DISTINCT CASE 
                    WHEN s.type = 'issue' 
                        AND DATE(s.`datetime`) BETWEEN %s AND %s
                    THEN s.borrowernumber 
                END) AS active_students,
                COUNT(CASE 
                    WHEN s.type = 'issue' 
                        AND DATE(s.`datetime`) BETWEEN %s AND %s
                    THEN 1 
                END) AS ay_issues
            FROM borrowers b
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('STD', 'CLASS', 'DAR', 'CLASS_STD')
            LEFT JOIN statistics s ON b.borrowernumber = s.borrowernumber
            WHERE b.categorycode = %s
                AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                AND (b.debarred IS NULL OR b.debarred = 0)
                AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
                AND COALESCE(std.attribute, b.branchcode) IS NOT NULL
                AND COALESCE(std.attribute, b.branchcode) != ''
            GROUP BY COALESCE(std.attribute, b.branchcode)
            ORDER BY COALESCE(std.attribute, b.branchcode)
        """, (start, end, start, end, marhala_code))
        
        darajahs = cur.fetchall()
    
    for darajah in darajahs:
        name = darajah.get("darajah_name", "")
        name_str = str(name)
        
        # Set gender based on naming pattern
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


def get_top_students(limit: int = 10, marhala_name: Optional[str] = None) -> List[Dict]:
    """Get top students by number of books issued in the current AY."""
    start, end = get_ay_bounds()
    if not start:
        return []
    
    with get_db_cursor() as cur:
        query = """
            SELECT 
                b.borrowernumber,
                CASE 
                    WHEN b.surname IS NULL OR b.surname = '' OR b.surname = 'None' 
                         AND b.firstname IS NULL OR b.firstname = '' OR b.firstname = 'None'
                    THEN CONCAT('Student #', b.cardnumber)
                    WHEN b.surname IS NULL OR b.surname = '' OR b.surname = 'None'
                    THEN b.firstname
                    WHEN b.firstname IS NULL OR b.firstname = '' OR b.firstname = 'None'
                    THEN b.surname
                    ELSE CONCAT(b.surname, ' ', b.firstname)
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
                AND std.code IN ('STD', 'CLASS', 'DAR', 'CLASS_STD')
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
        
        # Ensure name is never empty/None
        if not row.get("StudentName") or str(row["StudentName"]).strip() in ("", "None"):
            row["StudentName"] = f"Student #{row['cardnumber']}"
    
    return rows


# -------------------------------
# LEGACY FUNCTIONS - PRESERVED WITH OPTIMIZATIONS
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
                AND std.code IN ('STD', 'CLASS', 'DAR', 'CLASS_STD')
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
            SELECT DATE_FORMAT(s.`datetime`, '%%Y-%%m') AS ym,
                   COUNT(*) AS cnt
            FROM statistics s
            WHERE s.type = 'issue'
              AND DATE(s.`datetime`) BETWEEN %s AND %s
            GROUP BY ym
            ORDER BY ym ASC
        """, (start, end))
        
        rows = cur.fetchall()
    
    return rows


def get_ay_trend_data(marhala_code: str = None, darajah_name: str = None) -> Tuple[List[str], List[int]]:
    """
    Get zero-filled monthly trend data for the current Academic Year.
    Optionally filters by marhala_code or darajah_name.
    """
    start, end = get_ay_bounds()
    if not start:
        return [], []

    # Get raw data
    query = """
        SELECT DATE_FORMAT(s.`datetime`, '%%Y-%%m') AS ym, COUNT(*) AS cnt
        FROM statistics s
    """
    params = [start, end]
    
    # Add filters
    join_clause = ""
    where_clause = "WHERE s.type = 'issue' AND DATE(s.`datetime`) BETWEEN %s AND %s"
    
    if marhala_code or darajah_name:
        join_clause += " JOIN borrowers b ON s.borrowernumber = b.borrowernumber"
        if darajah_name:
            join_clause += " LEFT JOIN borrower_attributes std ON std.borrowernumber = b.borrowernumber AND std.code IN ('STD','CLASS','DAR','CLASS_STD')"
            where_clause += " AND (std.attribute = %s OR b.branchcode = %s)"
            params.extend([darajah_name, darajah_name])
        elif marhala_code:
            where_clause += " AND b.categorycode = %s"
            params.append(marhala_code)

    full_query = f"{query} {join_clause} {where_clause} GROUP BY ym"
    
    with get_db_cursor(dictionary=False) as cur:
        cur.execute(full_query, tuple(params))
        rows = cur.fetchall()
    
    by_month = {ym: int(cnt) for ym, cnt in (rows or [])}
    
    labels = []
    values = []
    
    # Start from April of the AY start year
    # Loop through months until 'today' or end of AY
    curr = date(start.year, 4, 1)
    # If today is before start (unlikely but safe), use start
    target_end = min(date.today(), end)
    
    # We want a continuous sequence of months from April to target_end
    # The loop should handle the year transition nicely
    while curr <= target_end:
        ym = curr.strftime("%Y-%m")
        labels.append(get_hijri_month_year_label(curr))  # Always Hijri display
        values.append(by_month.get(ym, 0))
        
        # Increment month
        if curr.month == 12:
            curr = date(curr.year + 1, 1, 1)
        else:
            curr = date(curr.year, curr.month + 1, 1)

    return labels, values


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
                ON std.borrowernumber = b.borrowernumber AND std.code = 'STD'
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
        # Single query for all counts
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
        
        # Get category breakdown
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


# -------------------------------
# ADDITIONAL LEGACY FUNCTIONS - UNCHANGED STRUCTURE BUT OPTIMIZED
# -------------------------------

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
                ON std.borrowernumber = b.borrowernumber AND std.code = 'STD'
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
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
              SUM(CASE WHEN type='issue'  THEN 1 ELSE 0 END) AS checkouts,
              SUM(CASE WHEN type='return' THEN 1 ELSE 0 END) AS checkins
            FROM statistics
            WHERE DATE(`datetime`) = CURDATE();
            """
        )
        row = cur.fetchone() or (0, 0)
        cur.close()
        # Handle None values safely
        if isinstance(row, tuple):
            checkouts = int(row[0]) if row[0] is not None else 0
            checkins = int(row[1]) if row[1] is not None else 0
        else:  # dictionary cursor
            checkouts = int(row.get("checkouts") or 0)
            checkins = int(row.get("checkins") or 0)
        return checkouts, checkins
    finally:
        conn.close()


def find_student_by_identifier(identifier: str) -> Optional[dict]:
    """Find student by cardnumber / userid / borrowernumber / TRNO."""
    with get_db_cursor() as cur:
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
                ON std.borrowernumber = b.borrowernumber AND std.code = 'STD'
            LEFT JOIN borrower_attributes trno
                ON trno.borrowernumber = b.borrowernumber AND trno.code = 'TRNO'
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


def borrowed_books_for(borrowernumber: int) -> List[dict]:
    """Get active + past borrowed books for a student."""
    with get_db_cursor() as cur:
        # Active issues
        cur.execute("""
            SELECT bi.title, iss.issuedate AS date_issued, iss.date_due, 0 AS returned
            FROM issues iss
            JOIN items it ON iss.itemnumber = it.itemnumber
            JOIN biblio bi ON it.biblionumber = bi.biblionumber
            WHERE iss.borrowernumber = %s
            ORDER BY iss.issuedate DESC
        """, (borrowernumber,))
        
        active = cur.fetchall()
        
        # Past issues (limited)
        cur.execute("""
            SELECT bi.title, oi.issuedate AS date_issued, oi.returndate AS date_due, 1 AS returned
            FROM old_issues oi
            JOIN items it ON oi.itemnumber = it.itemnumber
            JOIN biblio bi ON it.biblionumber = bi.biblionumber
            WHERE oi.borrowernumber = %s
            ORDER BY oi.issuedate DESC
            LIMIT 200
        """, (borrowernumber,))
        
        old = cur.fetchall()
    
    return active + old


def darajah_dataframe(darajah_std: str) -> list:
    """Return list of students in a darajah (STD attribute) with totals."""
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
                COALESCE(x.total_issues, 0) AS TotalIssues,
                COALESCE(x.fees_paid, 0) AS TotalFeesPaid,
                b.dateexpiry,
                b.debarred,
                b.gonenoaddress,
                trno.attribute AS trno
            FROM borrowers b
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber AND std.code = 'STD'
            LEFT JOIN borrower_attributes trno
                ON trno.borrowernumber = b.borrowernumber AND trno.code = 'TRNO'
            LEFT JOIN (
                SELECT iss.borrowernumber,
                       COUNT(*) AS total_issues,
                       COALESCE(SUM(CASE WHEN al.credit_type_code = 'PAYMENT' THEN al.amount END), 0) AS fees_paid
                FROM issues iss
                LEFT JOIN accountlines al ON iss.borrowernumber = al.borrowernumber
                GROUP BY iss.borrowernumber
            ) x ON x.borrowernumber = b.borrowernumber
            WHERE (std.attribute = %s OR b.branchcode = %s)
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            ORDER BY FullName ASC
        """, (darajah_std, darajah_std))
        
        rows = cur.fetchall()
    
    return rows

def get_all_marhalas() -> List[str]:
    """
    Get all Marhala names for filter dropdown.
    Returns actual Koha category descriptions.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        
        # Get descriptions for both academic and non-academic categories
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
        
        return [row[0] for row in rows if row[0]]
    finally:
        conn.close()

def marhala_dataframe(marhala: str) -> list:
    """Return list of students in a marhala (by category)."""
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
                COALESCE(x.total_issues, 0) AS TotalIssues,
                COALESCE(x.fees_paid, 0) AS TotalFeesPaid,
                b.dateexpiry,
                b.debarred,
                b.gonenoaddress,
                trno.attribute AS trno
            FROM borrowers b
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber AND std.code = 'STD'
            LEFT JOIN borrower_attributes trno
                ON trno.borrowernumber = b.borrowernumber AND trno.code = 'TRNO'
            LEFT JOIN (
                SELECT iss.borrowernumber,
                       COUNT(*) AS total_issues,
                       COALESCE(SUM(CASE WHEN al.credit_type_code = 'PAYMENT' THEN al.amount END), 0) AS fees_paid
                FROM issues iss
                LEFT JOIN accountlines al ON iss.borrowernumber = al.borrowernumber
                GROUP BY iss.borrowernumber
            ) x ON x.borrowernumber = b.borrowernumber
            WHERE (c.description = %s OR b.categorycode = %s)
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            ORDER BY FullName ASC
        """, (marhala, marhala))
        
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
                ON std.borrowernumber = b.borrowernumber AND std.code = 'STD'
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
        # Single combined query for efficiency
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
                COUNT(DISTINCT CASE 
                    WHEN s.type = 'issue' 
                        AND DATE(s.`datetime`) BETWEEN %s AND %s
                    THEN s.borrowernumber 
                END) AS ay_active,
                COUNT(CASE 
                    WHEN s.type = 'issue' 
                        AND DATE(s.`datetime`) BETWEEN %s AND %s
                    THEN 1 
                END) AS ay_issues
            FROM borrowers b
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('STD', 'CLASS', 'DAR', 'CLASS_STD')
            LEFT JOIN statistics s ON b.borrowernumber = s.borrowernumber
            WHERE (std.attribute IS NOT NULL OR b.branchcode IS NOT NULL)
                AND (std.attribute != '' OR b.branchcode != '')
            GROUP BY COALESCE(std.attribute, b.branchcode)
            ORDER BY 
                CAST(REGEXP_SUBSTR(COALESCE(std.attribute, b.branchcode), '^[0-9]+') AS UNSIGNED),
                COALESCE(std.attribute, b.branchcode)
        """, (start, end, start, end))
        
        darajahs = cur.fetchall()
    
    for darajah in darajahs:
        name = darajah.get("darajah_name", "")
        name_str = str(name)
        
        # Determine gender
        if " M" in name_str or name_str.endswith("M"):
            darajah["gender"] = "Boys"
            darajah["icon"] = "male"
        elif " F" in name_str or name_str.endswith("F"):
            darajah["gender"] = "Girls"
            darajah["icon"] = "female"
        else:
            darajah["gender"] = "Mixed"
            darajah["icon"] = "users"
        
        # Determine section
        if " A" in name_str:
            darajah["section"] = "A"
        elif " B" in name_str:
            darajah["section"] = "B"
        elif " C" in name_str:
            darajah["section"] = "C"
        else:
            darajah["section"] = ""
    
    return darajahs


def get_marhala_distribution() -> Tuple[List[str], List[int]]:
    """Return (labels, values) for Marhala/Darajah distribution based on issue counts."""
    # First try marhala-based distribution
    labels, values = get_marhala_distribution_with_dars_burhani()
    
    # If no marhala data, fall back to darajah buckets
    if not labels:
        buckets = darajah_buckets()
        labels = [b[0] for b in buckets if b[1] > 0]
        values = [b[1] for b in buckets if b[1] > 0]
    
    return labels, values


def get_top_darajah_summary_with_asateza_last() -> List[Dict]:
    """
    Legacy function - delegates to optimized version.
    """
    return get_top_darajah_summary_with_asateza_last(limit=10)
