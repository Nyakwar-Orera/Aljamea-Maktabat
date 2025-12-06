# services/koha_queries.py
from typing import Dict, List, Optional, Tuple, Any
from db_koha import get_conn
import os
from datetime import date, datetime
import re

# ---------- tiny SQL loader for sql/koha.sql ----------
# Each query section must start with: "-- name: <key>"
_SQL_CACHE = None


def _load_sql_file() -> dict:
    """Load and cache named SQL sections from sql/koha.sql."""
    global _SQL_CACHE
    if _SQL_CACHE is not None:
        return _SQL_CACHE

    # Resolve path relative to project root (place file at ./sql/koha.sql)
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
                # flush previous
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

def _ay_bounds():
    """
    Academic Year window:
      - From 1 April of the current calendar year
      - To today (capped at 31 Dec of that year)

    Returns (start_date, end_date) or (None, None) if AY hasn't started yet.
    """
    today = date.today()
    year = today.year
    if today.month < 4:
        # If before April, use previous year's AY
        ay_year = year - 1
        start = date(ay_year, 4, 1)
        end = min(date.today(), date(ay_year, 12, 31))
        return start, end
    start = date(year, 4, 1)
    end = min(today, date(year, 12, 31))
    return start, end


# -------------------------------
# PATRON COUNT QUERIES (IMPROVED)
# -------------------------------

def get_total_patrons_count() -> int:
    """
    Get total count of ALL active patrons in Koha database.
    Includes all categories (students, teachers, staff, etc.)
    Excludes expired, debarred, and gone-no-address patrons.
    """
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute(
        """
        SELECT COUNT(*) AS total_patrons
        FROM borrowers
        WHERE (dateexpiry IS NULL OR dateexpiry >= CURDATE())
          AND (debarred IS NULL OR debarred = 0)
          AND (gonenoaddress IS NULL OR gonenoaddress = 0);
        """
    )
    
    result = cur.fetchone()
    cur.close()
    conn.close()
    
    return int(result[0] if result else 0)


def get_student_patrons_count() -> int:
    """
    Get count of active student patrons (category codes starting with 'S').
    Excludes expired, debarred, and gone-no-address patrons.
    """
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute(
        """
        SELECT COUNT(*) AS student_patrons
        FROM borrowers
        WHERE categorycode LIKE 'S%%'
          AND (dateexpiry IS NULL OR dateexpiry >= CURDATE())
          AND (debarred IS NULL OR debarred = 0)
          AND (gonenoaddress IS NULL OR gonenoaddress = 0);
        """
    )
    
    result = cur.fetchone()
    cur.close()
    conn.close()
    
    return int(result[0] if result else 0)


def get_non_student_patrons_count() -> int:
    """
    Get count of active non-student patrons (category codes NOT starting with 'S').
    Excludes expired, debarred, and gone-no-address patrons.
    """
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute(
        """
        SELECT COUNT(*) AS non_student_patrons
        FROM borrowers
        WHERE (categorycode NOT LIKE 'S%%' OR categorycode IS NULL)
          AND (dateexpiry IS NULL OR dateexpiry >= CURDATE())
          AND (debarred IS NULL OR debarred = 0)
          AND (gonenoaddress IS NULL OR gonenoaddress = 0);
        """
    )
    
    result = cur.fetchone()
    cur.close()
    conn.close()
    
    return int(result[0] if result else 0)


# -------------------------------
# DASHBOARD SUMMARY QUERIES (AY) - UPDATED
# -------------------------------

def get_summary() -> Dict[str, Any]:
    """
    Return library summary stats with accurate patron counts.

    active_patrons:   ALL currently active patrons in Koha (not AY-limited):
                      - in borrowers table
                      - NOT expired (dateexpiry >= today or NULL)
                      - NOT debarred
                      - NOT gone-no-address

    total_patrons:    Same as active_patrons (for backward compatibility)
    student_patrons:  Active patrons with category codes starting with 'S'
    non_student_patrons: Active patrons with category codes NOT starting with 'S'
    
    total_titles:      total number of biblio records (lifetime)
    total_titles_issued: DISTINCT titles that were issued in AY
    total_issues:     total issue events in AY (from statistics)
    overdue:          current overdue issues (not AY-limited)
    fines_paid:       total fines PAID in AY (PAYMENT, not VOID)
    """
    # Get accurate patron counts
    total_patrons = get_total_patrons_count()
    student_patrons = get_student_patrons_count()
    non_student_patrons = get_non_student_patrons_count()
    
    start, end = _ay_bounds()
    conn = get_conn()
    cur = conn.cursor(dictionary=True)

    # --- Lifetime count of titles (unchanged) ---
    cur.execute("SELECT COUNT(*) AS c FROM biblio;")
    total_titles_all = int(cur.fetchone()["c"] or 0)

    # If AY hasn't started yet, we still return patron counts + titles + overdue
    if not start:
        # Overdue still makes sense without AY
        cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM issues
            WHERE returndate IS NULL
              AND date_due < NOW();
            """
        )
        overdue = int(cur.fetchone()["c"] or 0)

        cur.close()
        conn.close()
        return {
            "active_patrons": total_patrons,
            "total_patrons": total_patrons,
            "student_patrons": student_patrons,
            "non_student_patrons": non_student_patrons,
            "total_titles": total_titles_all,
            "total_titles_issued": 0,
            "total_issues": 0,
            "overdue": overdue,
            "fines_paid": 0.0,
            "active_patrons_ay": 0,
        }

    # ---------- AY-SCOPED METRICS ----------

    # Distinct borrowers who issued at least once in AY
    cur.execute(
        """
        SELECT COUNT(DISTINCT borrowernumber) AS c
        FROM statistics
        WHERE type='issue'
          AND DATE(`datetime`) BETWEEN %s AND %s;
        """,
        (start, end),
    )
    active_patrons_ay = int(cur.fetchone()["c"] or 0)

    # Total issues in AY
    cur.execute(
        """
        SELECT COUNT(*) AS c
        FROM statistics
        WHERE type='issue'
          AND DATE(`datetime`) BETWEEN %s AND %s;
        """,
        (start, end),
    )
    total_issues = int(cur.fetchone()["c"] or 0)

    # Distinct titles issued in AY
    cur.execute(
        """
        SELECT COUNT(DISTINCT bib.biblionumber) AS c
        FROM statistics s
        JOIN items it   ON s.itemnumber   = it.itemnumber
        JOIN biblio bib ON it.biblionumber = bib.biblionumber
        WHERE s.type='issue'
          AND DATE(s.`datetime`) BETWEEN %s AND %s;
        """,
        (start, end),
    )
    total_titles_issued = int(cur.fetchone()["c"] or 0)

    # Current overdue (not AY-limited)
    cur.execute(
        """
        SELECT COUNT(*) AS c
        FROM issues
        WHERE returndate IS NULL
          AND date_due < NOW();
        """
    )
    overdue = int(cur.fetchone()["c"] or 0)

    # Fines paid in AY (PAYMENT credits, not VOID; amount stored negative in Koha)
    cur.execute(
        """
        SELECT COALESCE(
                 SUM(
                   CASE
                     WHEN credit_type_code='PAYMENT'
                          AND (status IS NULL OR status <> 'VOID')
                          AND DATE(`date`) BETWEEN %s AND %s
                     THEN -amount
                     ELSE 0
                   END
                 ), 0
               ) AS fines_paid
        FROM accountlines;
        """,
        (start, end),
    )
    fines_paid = float(cur.fetchone()["fines_paid"] or 0.0)

    cur.close()
    conn.close()

    return {
        # Accurate patron counts
        "active_patrons": total_patrons,
        "total_patrons": total_patrons,
        "student_patrons": student_patrons,
        "non_student_patrons": non_student_patrons,
        "active_patrons_ay": active_patrons_ay,
        "total_titles": total_titles_all,
        "total_titles_issued": total_titles_issued,
        "total_issues": total_issues,
        "overdue": overdue,
        "fines_paid": fines_paid,
    }


def get_all_active_patrons(limit: int = 1000) -> List[Dict]:
    """
    Get all active patrons (including non-students) with basic info.
    Useful for verifying patron counts match Koha database.
    """
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    
    cur.execute(
        """
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
        LIMIT %s;
        """,
        (limit,)
    )
    
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def class_issues() -> List[Tuple[str, int]]:
    """
    Issues grouped by class (Darajah) using borrower attributes
    for the current Academic Year (AY) only.
    """
    start, end = _ay_bounds()
    if not start:
        return []

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
          COALESCE(std.attribute, b.branchcode, 'Unknown') AS class_name,
          COUNT(*) AS cnt
        FROM statistics s
        JOIN borrowers b
             ON b.borrowernumber = s.borrowernumber
        LEFT JOIN borrower_attributes std
             ON std.borrowernumber = b.borrowernumber
            AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
        WHERE s.type='issue'
          AND DATE(s.`datetime`) BETWEEN %s AND %s
          AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
          AND (b.debarred IS NULL OR b.debarred = 0)
          AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
        GROUP BY class_name
        ORDER BY cnt DESC;
        """,
        (start, end),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def departments_breakdown() -> List[Tuple[str, int]]:
    """
    Distribution of patrons by department (Koha category description),
    based on DISTINCT borrowers who issued at least 1 item in AY.
    """
    start, end = _ay_bounds()
    if not start:
        return []

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
          COALESCE(c.description, b.categorycode, 'Unknown') AS dept,
          COUNT(DISTINCT s.borrowernumber) AS cnt
        FROM statistics s
        JOIN borrowers b ON b.borrowernumber = s.borrowernumber
        LEFT JOIN categories c ON c.categorycode = b.categorycode
        WHERE s.type='issue'
          AND DATE(s.`datetime`) BETWEEN %s AND %s
          AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
          AND (b.debarred IS NULL OR b.debarred = 0)
          AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
        GROUP BY dept
        ORDER BY cnt DESC;
        """,
        (start, end),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def borrowing_trend_monthly() -> List[Tuple[str, int]]:
    """
    Issues per month (YYYY-MM) for the current Academic Year (AY),
    using statistics.type='issue'.
    """
    start, end = _ay_bounds()
    if not start:
        return []

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DATE_FORMAT(s.`datetime`, '%%Y-%%m') AS ym,
               COUNT(*) AS cnt
        FROM statistics s
        WHERE s.type='issue'
          AND DATE(s.`datetime`) BETWEEN %s AND %s
        GROUP BY ym
        ORDER BY ym ASC;
        """,
        (start, end),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def top_titles(
    limit: int = 25, arabic: bool = False, non_arabic: bool = False
) -> List[Tuple[str, int, str]]:
    """
    Top borrowed titles for the current Academic Year (AY) using
    issues + old_issues / items / biblio with optional Arabic / Non-Arabic filter.

    Filters:
      - arabic=True       → title contains Arabic letters
      - non_arabic=True   → title does NOT contain Arabic letters
      - both False        → no language filter (ALL)

    Returns rows of (title, count, last_issued).
    """
    start, end = _ay_bounds()
    if not start:
        return []

    conn = get_conn()
    cur = conn.cursor()

    # MySQL/MariaDB Arabic block: [ء-ي]
    lang_filter = ""
    if arabic:
        lang_filter = "AND bib.title REGEXP '[ء-ي]'"
    elif non_arabic:
        lang_filter = "AND bib.title NOT REGEXP '[ء-ي]'"

    cur.execute(
        f"""
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
        JOIN items it
             ON all_iss.itemnumber = it.itemnumber
        JOIN biblio bib
             ON it.biblionumber = bib.biblionumber
        WHERE DATE(all_iss.issuedate) BETWEEN %s AND %s
          {lang_filter}
        GROUP BY bib.biblionumber, bib.title
        ORDER BY cnt DESC
        LIMIT %s;
        """,
        (start, end, int(limit)),
    )

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def sip_stats(days: int = 90) -> List[Tuple[str, int]]:
    """SIP2 issue/return/renew counts in the last N days (not AY-specific)."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql_named("sip_activity_counts"), (int(days),))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def today_activity() -> Tuple[int, int]:
    """Return (today_checkouts, today_checkins) using statistics.type ('issue'/'return')."""
    conn = get_conn()
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
    conn.close()
    # row may be a tuple or dict depending on cursor; normalize:
    if isinstance(row, tuple):
        return int(row[0] or 0), int(row[1] or 0)
    return int(row.get("checkouts") or 0), int(row.get("checkins") or 0)


# -------------------------------
# TOP 25 TITLES BY MARC LANGUAGE (AY) - UPDATED
# -------------------------------

def arabic_top25() -> List[Dict]:
    """
    Top 25 Arabic titles (MARC 041$a LIKE 'ar%') in the current AY.
    Returns dict rows including: BiblioNumber, Title, Times_Issued, Collections.
    """
    start, end = _ay_bounds()
    if not start:
        return []

    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT
            bib.biblionumber AS BiblioNumber,
            bib.title        AS Title,
            GROUP_CONCAT(DISTINCT it.ccode ORDER BY it.ccode SEPARATOR ', ') AS Collections,
            COUNT(*) AS Times_Issued
        FROM (
            SELECT borrowernumber, itemnumber, issuedate
            FROM issues
            UNION ALL
            SELECT borrowernumber, itemnumber, issuedate
            FROM old_issues
        ) all_iss
        JOIN borrowers b
             ON b.borrowernumber = all_iss.borrowernumber
        JOIN items it
             ON all_iss.itemnumber = it.itemnumber
        JOIN biblio bib
             ON it.biblionumber = bib.biblionumber
        JOIN biblio_metadata bmd
             ON bib.biblionumber = bmd.biblionumber
        WHERE DATE(all_iss.issuedate) BETWEEN %s AND %s
          AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
          AND (b.debarred IS NULL OR b.debarred = 0)
          AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
          AND ExtractValue(
                bmd.metadata,
                '//datafield[@tag="041"]/subfield[@code="a"]'
              ) LIKE 'ar%%'
        GROUP BY bib.biblionumber, bib.title
        ORDER BY Times_Issued DESC
        LIMIT 25;
        """,
        (start, end),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def english_top25() -> List[Dict]:
    """
    Top 25 English titles (MARC 041$a LIKE 'eng%') in the current AY.
    Returns dict rows including: BiblioNumber, Title, Times_Issued, Collections.
    """
    start, end = _ay_bounds()
    if not start:
        return []

    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT
            bib.biblionumber AS BiblioNumber,
            bib.title        AS Title,
            GROUP_CONCAT(DISTINCT it.ccode ORDER BY it.ccode SEPARATOR ', ') AS Collections,
            COUNT(*) AS Times_Issued
        FROM (
            SELECT borrowernumber, itemnumber, issuedate
            FROM issues
            UNION ALL
            SELECT borrowernumber, itemnumber, issuedate
            FROM old_issues
        ) all_iss
        JOIN borrowers b
             ON b.borrowernumber = all_iss.borrowernumber
        JOIN items it
             ON all_iss.itemnumber = it.itemnumber
        JOIN biblio bib
             ON it.biblionumber = bib.biblionumber
        JOIN biblio_metadata bmd
             ON bib.biblionumber = bmd.biblionumber
        WHERE DATE(all_iss.issuedate) BETWEEN %s AND %s
          AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
          AND (b.debarred IS NULL OR b.debarred = 0)
          AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
          AND ExtractValue(
                bmd.metadata,
                '//datafield[@tag="041"]/subfield[@code="a"]'
              ) LIKE 'eng%%'
        GROUP BY bib.biblionumber, bib.title
        ORDER BY Times_Issued DESC
        LIMIT 25;
        """,
        (start, end),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# -------------------------------
# STUDENT / PATRON QUERIES - UPDATED
# -------------------------------

def find_student_by_identifier(identifier: str) -> Optional[dict]:
    """Find student by cardnumber / userid / borrowernumber."""
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT
          b.borrowernumber,
          b.cardnumber,
          b.userid,
          b.surname,
          b.firstname,
          b.email,
          b.categorycode,
          c.description AS category,
          COALESCE(std.attribute, b.branchcode) AS class,
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
           OR CAST(b.borrowernumber AS CHAR) = %s)
          AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
          AND (b.debarred IS NULL OR b.debarred = 0)
          AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
        LIMIT 1;
        """,
        (identifier, identifier, identifier),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def borrowed_books_for(borrowernumber: int) -> List[dict]:
    """Get active + past borrowed books for a student."""
    conn = get_conn()
    cur = conn.cursor(dictionary=True)

    cur.execute(
        """
        SELECT bi.title, iss.issuedate AS date_issued, iss.date_due, 0 AS returned
        FROM issues iss
        JOIN items it ON iss.itemnumber = it.itemnumber
        JOIN biblio bi ON it.biblionumber = bi.biblionumber
        WHERE iss.borrowernumber = %s
        ORDER BY iss.issuedate DESC;
        """,
        (borrowernumber,),
    )
    active = cur.fetchall()

    try:
        cur.execute(
            """
            SELECT bi.title, oi.issuedate AS date_issued, oi.returndate AS date_due, 1 AS returned
            FROM old_issues oi
            JOIN items it ON oi.itemnumber = it.itemnumber
            JOIN biblio bi ON it.biblionumber = bi.biblionumber
            WHERE oi.borrowernumber = %s
            ORDER BY oi.issuedate DESC
            LIMIT 200;
            """,
            (borrowernumber,),
        )
        old = cur.fetchall()
    except Exception:
        old = []

    cur.close()
    conn.close()
    return active + old


# -------------------------------
# CLASS & DEPARTMENT REPORTS - UPDATED
# -------------------------------

def class_dataframe(class_std: str) -> list:
    """Return list of students in a class (STD attribute) with totals."""
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT
          b.borrowernumber,
          b.cardnumber,
          CONCAT(b.surname, ' ', b.firstname) AS FullName,
          b.email AS EduEmail,
          b.categorycode,
          c.description AS category,
          COALESCE(std.attribute, b.branchcode) AS class,
          COALESCE(x.total_issues, 0) AS TotalIssues,
          COALESCE(x.fines_paid, 0) AS TotalFinesPaid,
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
                 COALESCE(SUM(CASE WHEN al.credit_type_code='PAYMENT' THEN al.amount END),0) AS fines_paid
          FROM issues iss
          LEFT JOIN accountlines al ON iss.borrowernumber = al.borrowernumber
          GROUP BY iss.borrowernumber
        ) x ON x.borrowernumber = b.borrowernumber
        WHERE (std.attribute = %s OR b.branchcode = %s)
          AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
          AND (b.debarred IS NULL OR b.debarred = 0)
          AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
        ORDER BY FullName ASC;
        """,
        (class_std, class_std),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def department_dataframe(dept: str) -> list:
    """Return list of students in a department (by category)."""
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT
          b.borrowernumber,
          b.cardnumber,
          CONCAT(b.surname, ' ', b.firstname) AS FullName,
          b.email AS EduEmail,
          b.categorycode,
          c.description AS category,
          COALESCE(std.attribute, b.branchcode) AS class,
          COALESCE(x.total_issues, 0) AS TotalIssues,
          COALESCE(x.fines_paid, 0) AS TotalFinesPaid,
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
                 COALESCE(SUM(CASE WHEN al.credit_type_code='PAYMENT' THEN al.amount END),0) AS fines_paid
          FROM issues iss
          LEFT JOIN accountlines al ON iss.borrowernumber = al.borrowernumber
          GROUP BY iss.borrowernumber
        ) x ON x.borrowernumber = b.borrowernumber
        WHERE (c.description = %s OR b.categorycode = %s)
          AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
          AND (b.debarred IS NULL OR b.debarred = 0)
          AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
        ORDER BY FullName ASC;
        """,
        (dept, dept),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def patron_title_agg(from_date, to_date, exclude_category: str = "T-KG") -> list:
    """Python wrapper over koha.sql:patron_title_agg_between_dates (date range passed by caller)."""
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT
          b.borrowernumber,
          b.cardnumber,
          CONCAT_WS(' ', b.surname, b.firstname)        AS patron_name,
          std.attribute                                  AS class_std,
          trno.attribute                                 AS trno,
          COALESCE(COUNT(d.biblionumber),0)              AS issued_count,
          GROUP_CONCAT(
              CONCAT(bib.title,' (',DATE_FORMAT(d.first_issued,'%d-%b-%Y'),')')
              ORDER BY bib.title SEPARATOR ' • '
          )                                              AS titles_list
        FROM borrowers b
        LEFT JOIN borrower_attributes std
               ON std.borrowernumber = b.borrowernumber AND std.code = 'STD'
        LEFT JOIN borrower_attributes trno
               ON trno.borrowernumber = b.borrowernumber AND trno.code = 'TRNO'
        LEFT JOIN (
          /* one row per (borrower, title) issued in period */
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
        ORDER BY class_std, patron_name;
        """,
        (from_date, to_date, exclude_category),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def daraja_buckets() -> List[Tuple[str, int]]:
    """Bucket counts of patrons per Daraja group based on STD attribute."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
          CASE
            WHEN CAST(std.attribute AS UNSIGNED) BETWEEN 1 AND 2  THEN 'Daraja 1–2'
            WHEN CAST(std.attribute AS UNSIGNED) BETWEEN 3 AND 4  THEN 'Daraja 3–4'
            WHEN CAST(std.attribute AS UNSIGNED) BETWEEN 5 AND 7  THEN 'Daraja 5–7'
            WHEN CAST(std.attribute AS UNSIGNED) BETWEEN 8 AND 11 THEN 'Daraja 8–11'
            ELSE 'Unassigned'
          END AS daraja_group,
          COUNT(*) AS patrons
        FROM borrowers b
        LEFT JOIN borrower_attributes std
          ON std.borrowernumber = b.borrowernumber AND std.code = 'STD'
        WHERE (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
          AND (b.debarred IS NULL OR b.debarred = 0)
          AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
        GROUP BY daraja_group
        ORDER BY MIN(CASE daraja_group
          WHEN 'Daraja 1–2' THEN 1
          WHEN 'Daraja 3–4' THEN 2
          WHEN 'Daraja 5–7' THEN 3
          WHEN 'Daraja 8–11' THEN 4
          ELSE 9 END);
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def darajah_max_books() -> List[Tuple[str, int]]:
    """
    Return maximum allowed books per Darajah group.
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 'Darajah 1–4'  AS darajah_group, 4 AS max_books
        UNION ALL
        SELECT 'Darajah 5–7', 5 AS max_books
        UNION ALL
        SELECT 'Darajah 8–11', 6 AS max_books;
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# -------------------------------
# ADMIN DASHBOARD HELPERS (AY) - UPDATED
# -------------------------------

def get_department_summary(selected_dept: Optional[str] = None) -> list:
    """
    Department-wise issues for the current Academic Year (AY).

    Returns list of dict rows with:
      - Department
      - BooksIssued
      - ActivePatrons
      - IssuesPerPatron
      - Collections
    """
    start, end = _ay_bounds()
    if not start:
        return []

    conn = get_conn()
    cur = conn.cursor(dictionary=True)

    query = """
    SELECT
      COALESCE(c.description, b.categorycode, 'Unknown') AS Department,
      COUNT(*) AS BooksIssued,
      COUNT(DISTINCT s.borrowernumber) AS ActivePatrons,
      GROUP_CONCAT(DISTINCT it.ccode ORDER BY it.ccode SEPARATOR ', ') AS Collections
    FROM statistics s
    JOIN borrowers b ON b.borrowernumber = s.borrowernumber
    LEFT JOIN categories c ON c.categorycode = b.categorycode
    JOIN items it ON s.itemnumber = it.itemnumber
    WHERE s.type = 'issue'
      AND DATE(s.`datetime`) BETWEEN %s AND %s
      AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
      AND (b.debarred IS NULL OR b.debarred = 0)
      AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
    """
    params = [start, end]

    if selected_dept:
        # Match either by category description or category code
        query += " AND (c.description = %s OR b.categorycode = %s)"
        params.extend([selected_dept, selected_dept])

    query += """
    GROUP BY Department
    ORDER BY BooksIssued DESC;
    """

    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    for r in rows:
        patrons = r.get("ActivePatrons") or 0
        issues = r.get("BooksIssued") or 0
        r["IssuesPerPatron"] = (
            round(float(issues) / float(patrons), 2) if patrons else 0.0
        )
        r["Collections"] = r.get("Collections") or "—"

    return rows


def _is_marhala_department(dept_name: str) -> bool:
    """
    Heuristic to decide if a Koha department is actually a 'Marhala'
    (stage) for students.

    We treat anything with 'Std', 'Collegiate', or 'Culture Générale'
    in the description as Marhala.
    """
    if not dept_name:
        return False
    n = dept_name.lower()
    keywords = [
        "std",                 # e.g. "Std 1-2", "Std 8-11"
        "collegiate",          # "Collegiate I (5-7)", etc.
        "culture générale",    # French spelling
        "culture generale",    # in case accents are missing
    ]
    return any(k in n for k in keywords)


def get_marhala_distribution() -> Tuple[List[str], List[int]]:
    """
    Return (labels, values) for Marhala / Darajah distribution based on
    *issue counts* in the current AY.
    """
    start, end = _ay_bounds()
    if not start:
        return [], []

    # ---------- 1) Try department-based Marhala (preferred) ----------
    labels: List[str] = []
    values: List[int] = []

    try:
        dept_rows = get_department_summary(selected_dept=None)
    except Exception:
        dept_rows = []

    for r in dept_rows:
        dept_name = (r.get("Department") or "").strip()
        if not _is_marhala_department(dept_name):
            continue

        labels.append(dept_name)
        values.append(int(r.get("BooksIssued") or 0))

    # If we got at least one Marhala row, use it directly
    if labels:
        return labels, values

    # ---------- 2) Fallback: STD-based buckets (issue counts) ----------
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
          std.attribute AS std_attr,
          COUNT(*)      AS cnt
        FROM statistics s
        JOIN borrowers b
              ON b.borrowernumber = s.borrowernumber
        LEFT JOIN borrower_attributes std
              ON std.borrowernumber = b.borrowernumber
             AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
        WHERE s.type = 'issue'
          AND DATE(s.`datetime`) BETWEEN %s AND %s
          AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
          AND (b.debarred IS NULL OR b.debarred = 0)
          AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
        GROUP BY std.attribute;
        """,
        (start, end),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    bucket_counts = {
        "Darajah 1–4": 0,
        "Darajah 5–7": 0,
        "Darajah 8–11": 0,
        "Unassigned": 0,
    }

    for std_attr, cnt in rows:
        cnt = int(cnt or 0)

        if not std_attr:
            bucket_counts["Unassigned"] += cnt
            continue

        m = re.search(r"\d+", str(std_attr))
        if not m:
            bucket_counts["Unassigned"] += cnt
            continue

        n = int(m.group(0))
        if 1 <= n <= 4:
            bucket_counts["Darajah 1–4"] += cnt
        elif 5 <= n <= 7:
            bucket_counts["Darajah 5–7"] += cnt
        elif 8 <= n <= 11:
            bucket_counts["Darajah 8–11"] += cnt
        else:
            bucket_counts["Unassigned"] += cnt

    labels = []
    values = []
    for label in ["Darajah 1–4", "Darajah 5–7", "Darajah 8–11", "Unassigned"]:
        if bucket_counts[label] > 0:
            labels.append(label)
            values.append(bucket_counts[label])

    return labels, values


def get_language_top25() -> Dict[str, Dict[str, List]]:
    """
    Combine arabic_top25() and english_top25() into the structure
    expected by the Admin dashboard:
    """
    def pack(rows: List[Dict]) -> Dict[str, List]:
        titles = [r.get("Title") for r in rows]
        counts = [int(r.get("Times_Issued") or 0) for r in rows]
        return {
            "titles": titles,
            "counts": counts,
            "records": rows,
        }

    ar_rows = arabic_top25()
    en_rows = english_top25()

    return {
        "arabic": pack(ar_rows),
        "english": pack(en_rows),
    }


def get_top_darajah_summary(limit: int = 10) -> List[Dict]:
    """
    Darajah-wise (class-wise) summary for current AY,
    ordered by BooksIssued (descending).
    """
    start, end = _ay_bounds()
    if not start:
        return []

    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT
          COALESCE(std.attribute, b.branchcode, 'Unknown') AS Darajah,
          COUNT(*) AS BooksIssued,
          COUNT(DISTINCT s.borrowernumber) AS ActiveStudents,
          GROUP_CONCAT(DISTINCT it.ccode ORDER BY it.ccode SEPARATOR ', ') AS Collections
        FROM statistics s
        JOIN borrowers b ON b.borrowernumber = s.borrowernumber
        LEFT JOIN borrower_attributes std
             ON std.borrowernumber = b.borrowernumber
            AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
        JOIN items it ON s.itemnumber = it.itemnumber
        WHERE s.type = 'issue'
          AND DATE(s.`datetime`) BETWEEN %s AND %s
          AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
          AND (b.debarred IS NULL OR b.debarred = 0)
          AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
        GROUP BY Darajah
        ORDER BY BooksIssued DESC
        LIMIT %s;
        """,
        (start, end, int(limit)),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    for r in rows:
        name = (r.get("Darajah") or "").strip()
        if name.upper() == "AJSN":
            name = "Asateza"
        r["Darajah"] = name or "Unknown"
        r["Collections"] = r.get("Collections") or "—"

    return rows


# -------------------------------
# NEW: VERIFICATION QUERIES
# -------------------------------

def verify_patron_counts() -> Dict[str, Any]:
    """
    Verify patron counts match Koha database.
    Returns detailed breakdown for debugging.
    """
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    
    # Total patrons in database (including inactive)
    cur.execute("SELECT COUNT(*) AS total FROM borrowers;")
    total_in_db = int(cur.fetchone()["total"] or 0)
    
    # Active patrons (not expired, not debarred, not gone-no-address)
    cur.execute(
        """
        SELECT COUNT(*) AS active
        FROM borrowers
        WHERE (dateexpiry IS NULL OR dateexpiry >= CURDATE())
          AND (debarred IS NULL OR debarred = 0)
          AND (gonenoaddress IS NULL OR gonenoaddress = 0);
        """
    )
    active_in_db = int(cur.fetchone()["active"] or 0)
    
    # Expired patrons
    cur.execute(
        """
        SELECT COUNT(*) AS expired
        FROM borrowers
        WHERE dateexpiry < CURDATE() AND dateexpiry IS NOT NULL;
        """
    )
    expired_in_db = int(cur.fetchone()["expired"] or 0)
    
    # Debarred patrons
    cur.execute(
        """
        SELECT COUNT(*) AS debarred
        FROM borrowers
        WHERE debarred = 1;
        """
    )
    debarred_in_db = int(cur.fetchone()["debarred"] or 0)
    
    # Gone-no-address patrons
    cur.execute(
        """
        SELECT COUNT(*) AS gonenoaddress
        FROM borrowers
        WHERE gonenoaddress = 1;
        """
    )
    gonenoaddress_in_db = int(cur.fetchone()["gonenoaddress"] or 0)
    
    # Category breakdown of active patrons
    cur.execute(
        """
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
        ORDER BY count DESC;
        """
    )
    category_breakdown = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return {
        "total_in_database": total_in_db,
        "active_patrons": active_in_db,
        "expired_patrons": expired_in_db,
        "debarred_patrons": debarred_in_db,
        "gonenoaddress_patrons": gonenoaddress_in_db,
        "category_breakdown": category_breakdown,
        "verification_note": "Active patrons should match Koha's count of non-expired, non-debarred, non-gonenoaddress patrons."
    }

def get_all_classes() -> List[Dict]:
    """
    Get all distinct classes from Koha database.
    Returns list of dicts with class info including stats.
    """
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    
    # Get current AY bounds for stats
    start, end = _ay_bounds()
    
    query = """
    SELECT DISTINCT
        COALESCE(std.attribute, b.branchcode) AS class_name,
        COUNT(DISTINCT b.borrowernumber) AS total_students,
        COUNT(DISTINCT CASE 
            WHEN (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                AND (b.debarred IS NULL OR b.debarred = 0)
                AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            THEN b.borrowernumber 
        END) AS active_students
    FROM borrowers b
    LEFT JOIN borrower_attributes std
        ON std.borrowernumber = b.borrowernumber
        AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
    WHERE (std.attribute IS NOT NULL OR b.branchcode IS NOT NULL)
        AND (std.attribute != '' OR b.branchcode != '')
    GROUP BY COALESCE(std.attribute, b.branchcode)
    ORDER BY 
        CAST(REGEXP_SUBSTR(COALESCE(std.attribute, b.branchcode), '^[0-9]+') AS UNSIGNED),
        COALESCE(std.attribute, b.branchcode);
    """
    
    cur.execute(query)
    classes = cur.fetchall()
    
    # Add AY issue counts for each class
    for cls in classes:
        class_name = cls.get("class_name")
        
        if start and end:
            # Get issues count for this class in AY
            cur.execute("""
                SELECT COUNT(*) AS ay_issues
                FROM statistics s
                JOIN borrowers b ON b.borrowernumber = s.borrowernumber
                LEFT JOIN borrower_attributes std
                    ON std.borrowernumber = b.borrowernumber
                    AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
                WHERE s.type = 'issue'
                    AND DATE(s.`datetime`) BETWEEN %s AND %s
                    AND (std.attribute = %s OR b.branchcode = %s)
            """, (start, end, class_name, class_name))
            
            result = cur.fetchone()
            cls["ay_issues"] = result["ay_issues"] if result else 0
        else:
            cls["ay_issues"] = 0
        
        # Determine class type based on naming pattern
        class_name_str = str(class_name)
        if " M" in class_name_str:
            cls["gender"] = "Boys"
            cls["icon"] = "male"
        elif " F" in class_name_str:
            cls["gender"] = "Girls"
            cls["icon"] = "female"
        else:
            cls["gender"] = "Mixed"
            cls["icon"] = "users"
        
        # Determine section/stream
        if " A" in class_name_str:
            cls["section"] = "A"
        elif " B" in class_name_str:
            cls["section"] = "B"
        elif " C" in class_name_str:
            cls["section"] = "C"
        else:
            cls["section"] = ""
    
    cur.close()
    conn.close()
    
    return classes


# -------------------------------
# DEPARTMENT EXPLORER FUNCTIONS
# -------------------------------

def get_all_departments_with_stats() -> List[Dict]:
    """
    Get all departments with detailed statistics.
    Returns list of dicts with department info including accurate stats.
    """
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    
    # Get current AY bounds for stats
    start, end = _ay_bounds()
    
    # Get all distinct departments/categories
    query = """
    SELECT DISTINCT
        c.categorycode AS dept_code,
        c.description AS dept_name,
        c.categorytype,
        c.enrolmentperiod,
        c.overduefinescap,
        c.reservefee,
        c.rentalcharge,
        c.fee,
        c.default_privacy
    FROM categories c
    WHERE c.description IS NOT NULL
        AND c.description != ''
        AND c.categorytype IN ('A', 'C')  # Patron categories
    ORDER BY c.description;
    """
    
    cur.execute(query)
    departments = cur.fetchall()
    
    # Add detailed stats for each department
    for dept in departments:
        dept_code = dept.get("dept_code")
        
        # Get accurate patron counts
        cur.execute("""
            SELECT COUNT(*) AS total_borrowers
            FROM borrowers b
            WHERE b.categorycode = %s
                AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                AND (b.debarred IS NULL OR b.debarred = 0)
                AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
        """, (dept_code,))
        total_row = cur.fetchone()
        dept["total_borrowers"] = total_row["total_borrowers"] if total_row else 0
        
        # Count borrowers with TR numbers (active students)
        cur.execute("""
            SELECT COUNT(DISTINCT trno.attribute) AS active_borrowers
            FROM borrowers b
            LEFT JOIN borrower_attributes trno
                ON trno.borrowernumber = b.borrowernumber
                AND trno.code = 'TRNO'
            WHERE b.categorycode = %s
                AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                AND (b.debarred IS NULL OR b.debarred = 0)
                AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
                AND trno.attribute IS NOT NULL
                AND trno.attribute != ''
        """, (dept_code,))
        active_row = cur.fetchone()
        dept["active_borrowers"] = active_row["active_borrowers"] if active_row else 0
        
        # Get AY issues count
        if start and end:
            cur.execute("""
                SELECT COUNT(*) AS ay_issues
                FROM statistics s
                JOIN borrowers b ON s.borrowernumber = b.borrowernumber
                WHERE s.type = 'issue'
                    AND DATE(s.`datetime`) BETWEEN %s AND %s
                    AND b.categorycode = %s
            """, (start, end, dept_code))
            issues_row = cur.fetchone()
            dept["ay_issues"] = issues_row["ay_issues"] if issues_row else 0
        else:
            dept["ay_issues"] = 0
        
        # Get active loans count
        cur.execute("""
            SELECT COUNT(*) AS active_loans
            FROM issues i
            JOIN borrowers b ON i.borrowernumber = b.borrowernumber
            WHERE b.categorycode = %s
                AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                AND (b.debarred IS NULL OR b.debarred = 0)
                AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
                AND i.returndate IS NULL
        """, (dept_code,))
        loans_row = cur.fetchone()
        dept["active_loans"] = loans_row["active_loans"] if loans_row else 0
        
        # Get overdue count
        cur.execute("""
            SELECT COUNT(*) AS overdues
            FROM issues i
            JOIN borrowers b ON i.borrowernumber = b.borrowernumber
            WHERE b.categorycode = %s
                AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                AND (b.debarred IS NULL OR b.debarred = 0)
                AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
                AND i.date_due < CURDATE()
                AND i.returndate IS NULL
        """, (dept_code,))
        overdues_row = cur.fetchone()
        dept["overdues"] = overdues_row["overdues"] if overdues_row else 0
        
        # Determine department type and icon
        dept_name_lower = str(dept["dept_name"]).lower()
        
        # Department type classification
        if any(keyword in dept_name_lower for keyword in ['student', 'std', 'class', 'darajah', 'grade']):
            dept["type"] = "Student"
            dept["icon"] = "graduation-cap"
        elif any(keyword in dept_name_lower for keyword in ['faculty', 'staff', 'teacher', 'asateza']):
            dept["type"] = "Faculty"
            dept["icon"] = "user-tie"
        elif any(keyword in dept_name_lower for keyword in ['library', 'maktabat']):
            dept["type"] = "Library"
            dept["icon"] = "book"
        elif any(keyword in dept_name_lower for keyword in ['admin', 'administration', 'management']):
            dept["type"] = "Administration"
            dept["icon"] = "building"
        else:
            dept["type"] = "Other"
            dept["icon"] = "users"
    
    cur.close()
    conn.close()
    
    return departments


def get_department_engagement_stats() -> Dict[str, int]:
    """
    Get engagement statistics for all departments.
    Returns counts of different department types.
    """
    departments = get_all_departments_with_stats()
    
    counts = {
        "total_departments": len(departments),
        "student_departments": 0,
        "faculty_departments": 0,
        "library_departments": 0,
        "admin_departments": 0,
        "other_departments": 0,
        "total_borrowers": 0,
        "active_borrowers": 0,
        "total_ay_issues": 0
    }
    
    for dept in departments:
        dept_type = dept.get("type", "Other")
        
        # Count department types
        if dept_type == "Student":
            counts["student_departments"] += 1
        elif dept_type == "Faculty":
            counts["faculty_departments"] += 1
        elif dept_type == "Library":
            counts["library_departments"] += 1
        elif dept_type == "Administration":
            counts["admin_departments"] += 1
        else:
            counts["other_departments"] += 1
        
        # Sum up totals
        counts["total_borrowers"] += dept.get("total_borrowers", 0)
        counts["active_borrowers"] += dept.get("active_borrowers", 0)
        counts["total_ay_issues"] += dept.get("ay_issues", 0)
    
    return counts


def get_classes_in_department(dept_code: str) -> List[Dict]:
    """
    Get all classes belonging to a specific department.
    """
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    
    # Get current AY bounds for stats
    start, end = _ay_bounds()
    
    cur.execute("""
        SELECT DISTINCT 
            COALESCE(std.attribute, b.branchcode) AS class_name,
            COUNT(DISTINCT b.borrowernumber) AS total_students
        FROM borrowers b
        LEFT JOIN borrower_attributes std
            ON std.borrowernumber = b.borrowernumber
            AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
        WHERE b.categorycode = %s
          AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
          AND (b.debarred IS NULL OR b.debarred = 0)
          AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
          AND COALESCE(std.attribute, b.branchcode) IS NOT NULL
          AND COALESCE(std.attribute, b.branchcode) != ''
        GROUP BY COALESCE(std.attribute, b.branchcode)
        ORDER BY COALESCE(std.attribute, b.branchcode)
    """, (dept_code,))
    
    classes = cur.fetchall()
    
    # Add AY issues count for each class
    for cls in classes:
        class_name = cls.get("class_name")
        
        if start and end and class_name:
            cur.execute("""
                SELECT COUNT(*) AS ay_issues
                FROM statistics s
                JOIN borrowers b ON s.borrowernumber = b.borrowernumber
                LEFT JOIN borrower_attributes std
                    ON std.borrowernumber = b.borrowernumber
                    AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
                WHERE s.type = 'issue'
                    AND DATE(s.`datetime`) BETWEEN %s AND %s
                    AND (std.attribute = %s OR b.branchcode = %s)
                    AND b.categorycode = %s
            """, (start, end, class_name, class_name, dept_code))
            
            result = cur.fetchone()
            cls["ay_issues"] = result["ay_issues"] if result else 0
        else:
            cls["ay_issues"] = 0
        
        # Determine class gender
        class_name_str = str(class_name)
        if " M" in class_name_str or class_name_str.endswith("M"):
            cls["gender"] = "Boys"
            cls["icon"] = "male"
        elif " F" in class_name_str or class_name_str.endswith("F"):
            cls["gender"] = "Girls"
            cls["icon"] = "female"
        else:
            cls["gender"] = "Mixed"
            cls["icon"] = "users"
    
    cur.close()
    conn.close()
    
    return classes