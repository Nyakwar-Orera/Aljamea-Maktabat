# services/koha_queries.py
from typing import Dict, List, Optional, Tuple
from db_koha import get_conn
import os

# ---------- tiny SQL loader for sql/koha.sql ----------
# Each query section must start with: "-- name: <key>"
_SQL_CACHE = None

def _load_sql_file() -> dict:
    global _SQL_CACHE
    if _SQL_CACHE is not None:
        return _SQL_CACHE
    # Resolve path relative to project root (place file at ./sql/koha.sql)
    here = os.path.dirname(os.path.dirname(__file__))
    path = os.path.join(here, 'sql', 'koha.sql')
    sections = {}
    key = None
    buf: List[str] = []
    if not os.path.exists(path):
        _SQL_CACHE = sections
        return sections
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip().lower().startswith('-- name:'):
                # flush previous
                if key and buf:
                    sections[key] = ''.join(buf).strip()
                    buf = []
                key = line.split(':', 1)[1].strip()
                continue
            buf.append(line)
    if key and buf:
        sections[key] = ''.join(buf).strip()
    _SQL_CACHE = sections
    return sections

def sql_named(name: str) -> str:
    q = _load_sql_file().get(name)
    if not q:
        raise KeyError(f"SQL section '{name}' not found in sql/koha.sql")
    return q

# -------------------------------
# DASHBOARD SUMMARY QUERIES
# -------------------------------

def get_summary() -> Dict[str, int]:
    """Return library summary stats: patrons, titles, issues, overdue, fines paid."""
    conn = get_conn()
    cur = conn.cursor(dictionary=True)

    cur.execute("SELECT COUNT(*) AS c FROM borrowers;")
    total_patrons = cur.fetchone()["c"]

    cur.execute("SELECT COUNT(*) AS c FROM biblio;")
    total_titles = cur.fetchone()["c"]

    cur.execute("SELECT COUNT(*) AS c FROM issues;")
    total_active_issues = cur.fetchone()["c"]

    cur.execute("SELECT COUNT(*) AS c FROM issues WHERE date_due < NOW();")
    overdue = cur.fetchone()["c"]

    # payments only
    cur.execute(
        """
        SELECT COALESCE(SUM(amount), 0) AS fines_paid
        FROM accountlines
        WHERE credit_type_code = 'PAYMENT';
        """
    )
    fines_paid = float(cur.fetchone()["fines_paid"] or 0)

    cur.close(); conn.close()
    return {
        "total_patrons": total_patrons,
        "total_titles": total_titles,
        "total_issues": total_active_issues,
        "overdue": overdue,
        "fines_paid": fines_paid,
    }


def class_issues() -> List[Tuple[str, int]]:
    """Issues grouped by class using borrower attribute STD (fallback to Unknown)."""
    conn = get_conn(); cur = conn.cursor()
    cur.execute(sql_named('class_issue_counts_by_std'))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


def departments_breakdown() -> List[Tuple[str, int]]:
    """Distribution of patrons by department (Koha category description)."""
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        """
        SELECT COALESCE(c.description, b.categorycode, 'Unknown') AS dept,
               COUNT(*) AS cnt
        FROM borrowers b
        LEFT JOIN categories c ON c.categorycode = b.categorycode
        GROUP BY dept
        ORDER BY cnt DESC;
        """
    )
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


def borrowing_trend_monthly() -> List[Tuple[str, int]]:
    """Issues per month (YYYY-MM) from issues table."""
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        """
        SELECT DATE_FORMAT(issuedate, '%%Y-%%m') AS ym, COUNT(*) AS cnt
        FROM issues
        GROUP BY ym
        ORDER BY ym ASC;
        """
    )
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


def top_titles(limit: int = 25, arabic: bool = False, non_arabic: bool = False) -> List[Tuple[str, int, str]]:
    """
    Top borrowed titles using issues/items/biblio with optional Arabic / Non-Arabic filter.

    Filters:
      - arabic=True       → title contains Arabic letters
      - non_arabic=True   → title does NOT contain Arabic letters
      - both False        → no language filter (ALL)

    Returns rows of (title, count, last_issued).
    """
    conn = get_conn()
    cur = conn.cursor()

    # MySQL/MariaDB Arabic block: [ء-ي]
    lang_filter = ""
    if arabic:
        lang_filter = "AND bi.title REGEXP '[ء-ي]'"
    elif non_arabic:
        lang_filter = "AND bi.title NOT REGEXP '[ء-ي]'"

    cur.execute(f"""
        SELECT bi.title,
               COUNT(*) AS cnt,
               MAX(iss.issuedate) AS last_issued
        FROM issues iss
        JOIN items it  ON iss.itemnumber   = it.itemnumber
        JOIN biblio bi ON it.biblionumber  = bi.biblionumber
        WHERE 1=1 {lang_filter}
        GROUP BY bi.biblionumber, bi.title
        ORDER BY cnt DESC
        LIMIT %s;
    """, (int(limit),))

    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


def sip_stats(days: int = 90) -> List[Tuple[str, int]]:
    """SIP2 issue/return/renew counts in the last N days."""
    conn = get_conn(); cur = conn.cursor()
    cur.execute(sql_named('sip_activity_counts'), (int(days),))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


def today_activity() -> Tuple[int, int]:
    """Return (today_checkouts, today_checkins) using statistics.type ('issue'/'return')."""
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
        SELECT
          SUM(CASE WHEN type='issue'  THEN 1 ELSE 0 END) AS checkouts,
          SUM(CASE WHEN type='return' THEN 1 ELSE 0 END) AS checkins
        FROM statistics
        WHERE DATE(`datetime`) = CURDATE();
    """)
    row = cur.fetchone() or (0, 0)
    cur.close(); conn.close()
    # row may be a tuple or dict depending on cursor; normalize:
    if isinstance(row, tuple):
        return int(row[0] or 0), int(row[1] or 0)
    return int(row.get('checkouts') or 0), int(row.get('checkins') or 0)

# -------------------------------
# STUDENT / PATRON QUERIES
# -------------------------------

def find_student_by_identifier(identifier: str) -> Optional[dict]:
    """Find student by cardnumber / userid / borrowernumber."""
    conn = get_conn(); cur = conn.cursor(dictionary=True)
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
          COALESCE(std.attribute, b.branchcode) AS class
        FROM borrowers b
        LEFT JOIN categories c ON c.categorycode = b.categorycode
        LEFT JOIN borrower_attributes std
          ON std.borrowernumber = b.borrowernumber AND std.code = 'STD'
        WHERE LOWER(b.cardnumber) = LOWER(%s)
           OR LOWER(b.userid) = LOWER(%s)
           OR CAST(b.borrowernumber AS CHAR) = %s
        LIMIT 1;
        """,
        (identifier, identifier, identifier),
    )
    row = cur.fetchone()
    cur.close(); conn.close()
    return row


def borrowed_books_for(borrowernumber: int) -> List[dict]:
    """Get active + past borrowed books for a student."""
    conn = get_conn(); cur = conn.cursor(dictionary=True)

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

    cur.close(); conn.close()
    return active + old

# -------------------------------
# CLASS & DEPARTMENT REPORTS
# -------------------------------

def class_dataframe(class_std: str) -> list:
    """Return list of students in a class (STD attribute) with totals."""
    conn = get_conn(); cur = conn.cursor(dictionary=True)
    cur.execute(sql_named('patron_list_by_class_std'), (class_std, class_std))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


def department_dataframe(dept: str) -> list:
    """Return list of students in a department (by category)."""
    conn = get_conn(); cur = conn.cursor(dictionary=True)
    cur.execute(sql_named('patron_list_by_department'), (dept, dept))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


def patron_title_agg(from_date, to_date, exclude_category='T-KG') -> list:
    """Python wrapper over koha.sql:patron_title_agg_between_dates."""
    conn = get_conn(); cur = conn.cursor(dictionary=True)
    cur.execute(sql_named('patron_title_agg_between_dates'), (from_date, to_date, exclude_category))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


def daraja_buckets() -> List[Tuple[str, int]]:
    conn = get_conn(); cur = conn.cursor()
    cur.execute(sql_named('daraja_buckets_from_std'))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows
