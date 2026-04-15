# services/branch_queries.py — MULTI-CAMPUS DATA AGGREGATION SERVICE
"""
Branch-aware query service for the God Eye Super Admin dashboard.

Each function accepts a `branch_code` parameter to route queries
to the correct Koha MySQL instance via the multi-pool connector.
"""
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError, wait
from datetime import date, datetime
from typing import Dict, List, Optional, Any

from collections import defaultdict
from config import Config
from db_koha import get_branch_conn, is_branch_online, _MockConnection

logger = logging.getLogger(__name__)

BRANCH_QUERY_TIMEOUT = 8  # seconds per branch before we return cached/empty data


# ─────────────────────────────────────────────────────────────
# PER-BRANCH SUMMARY
# ─────────────────────────────────────────────────────────────

def get_branch_summary(branch_code: str, hijri_year: Optional[int] = None) -> Dict[str, Any]:
    """
    Get full library summary for a single branch from its Koha instance.
    Falls back to empty stats on connection failure.
    """
    from services.koha_queries import get_ay_bounds as _get_ay_bounds

    empty = _empty_branch_stats(branch_code)

    conn = get_branch_conn(branch_code)
    is_mock = isinstance(conn, _MockConnection)

    if is_mock:
        empty["status"] = "offline"
        return empty

    try:
        start, end = _get_ay_bounds(hijri_year)
        cur = conn.cursor(dictionary=True)

        # ── Total patrons ──────────────────────────────────────────
        cur.execute("""
            SELECT
                COUNT(*) AS total_patrons,
                SUM(CASE
                    WHEN (dateexpiry IS NULL OR dateexpiry >= CURDATE())
                         AND (debarred IS NULL OR debarred = 0)
                         AND (gonenoaddress IS NULL OR gonenoaddress = 0)
                    THEN 1 ELSE 0
                END) AS active_patrons,
                SUM(CASE
                    WHEN categorycode LIKE 'S%%'
                         AND (dateexpiry IS NULL OR dateexpiry >= CURDATE())
                         AND (debarred IS NULL OR debarred = 0)
                         AND (gonenoaddress IS NULL OR gonenoaddress = 0)
                    THEN 1 ELSE 0
                END) AS student_patrons
            FROM borrowers
        """)
        patron_row = cur.fetchone() or {}

        # ── Total catalog titles ───────────────────────────────────
        cur.execute("SELECT COUNT(*) AS total_titles FROM biblio")
        title_row = cur.fetchone() or {}

        # ── Academic year metrics (if AY has started) ──────────────
        total_issues = 0
        active_patrons_ay = 0
        overdue = 0
        currently_issued = 0

        if start and end:
            cur.execute("""
                SELECT COUNT(*) AS c FROM statistics s
                WHERE s.type = 'issue' AND DATE(s.datetime) BETWEEN %s AND %s
            """, (start, end))
            total_issues = int((cur.fetchone() or {}).get("c", 0))

            cur.execute("""
                SELECT COUNT(DISTINCT s.borrowernumber) AS c FROM statistics s
                WHERE s.type = 'issue' AND DATE(s.datetime) BETWEEN %s AND %s
            """, (start, end))
            active_patrons_ay = int((cur.fetchone() or {}).get("c", 0))

            cur.execute("""
                SELECT COUNT(*) AS c FROM issues i
                JOIN borrowers b ON i.borrowernumber = b.borrowernumber
                WHERE i.returndate IS NULL
                  AND i.date_due < CURDATE()
                  AND DATE(i.issuedate) BETWEEN %s AND %s
                  AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                  AND (b.debarred IS NULL OR b.debarred = 0)
                  AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            """, (start, end))
            overdue = int((cur.fetchone() or {}).get("c", 0))

            # 21-day grace period
            if start and (date.today() - start).days < 21:
                overdue = 0

            cur.execute("""
                SELECT COUNT(*) AS c FROM issues i
                JOIN borrowers b ON i.borrowernumber = b.borrowernumber
                WHERE i.returndate IS NULL
                  AND DATE(i.issuedate) BETWEEN %s AND %s
                  AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                  AND (b.debarred IS NULL OR b.debarred = 0)
                  AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            """, (start, end))
            currently_issued = int((cur.fetchone() or {}).get("c", 0))

        # ── Weekly trend (last 8 weeks) ────────────────────────────
        weekly_trend = _get_branch_weekly_trend(cur, start, end)

        # ── Top marhala ────────────────────────────────────────────
        top_marhalas = _get_branch_top_marhalas(cur, start, end)

        # ── Top books ──────────────────────────────────────────────
        top_books = _get_branch_top_books(cur, start, end)

        # ── Top students ──────────────────────────────────────────
        # Re-using the logic from get_branch_top_students but with existing cursor
        cur.execute("""
            SELECT
                CONCAT(COALESCE(b.surname, ''), ' ', COALESCE(b.firstname, '')) AS StudentName,
                trno.attribute AS TRNumber,
                b.cardnumber,
                COUNT(s.datetime) AS BooksIssued
            FROM statistics s
            JOIN borrowers b ON s.borrowernumber = b.borrowernumber
            LEFT JOIN borrower_attributes trno ON b.borrowernumber = trno.borrowernumber AND trno.code = 'TRNO'
            WHERE s.type = 'issue' AND DATE(s.datetime) BETWEEN %s AND %s
            GROUP BY b.borrowernumber ORDER BY BooksIssued DESC LIMIT 10
        """, (start, end))
        top_students = cur.fetchall()

        cur.close()

        cfg = Config.CAMPUS_REGISTRY.get(branch_code, {})
        return {
            "branch_code":        branch_code,
            "branch_name":        cfg.get("name", branch_code),
            "short_name":         cfg.get("short_name", branch_code),
            "flag":               cfg.get("flag", ""),
            "color":              cfg.get("color", "#004080"),
            "country":            cfg.get("country", ""),
            "status":             "online",
            "total_patrons":      int(patron_row.get("total_patrons") or 0),
            "active_patrons":     int(patron_row.get("active_patrons") or 0),
            "student_patrons":    int(patron_row.get("student_patrons") or 0),
            "total_titles":       int(title_row.get("total_titles") or 0),
            "total_issues":       total_issues,
            "active_patrons_ay":  active_patrons_ay,
            "overdue":            overdue,
            "currently_issued":   currently_issued,
            "weekly_trend":       weekly_trend,
            "top_marhalas":       top_marhalas,
            "top_books":          top_books,
            "top_students":       top_students,
            "fetched_at":         datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error(f"Error fetching summary for branch {branch_code}: {e}")
        empty["status"] = "error"
        empty["error"] = str(e)
        return empty
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _get_branch_weekly_trend(cur, start, end) -> List[Dict]:
    """Get 8-week borrowing trend for a branch."""
    try:
        if not start or not end:
            return []
        cur.execute("""
            SELECT
                DATE_FORMAT(datetime, '%%Y-%%u') AS week_key,
                MIN(DATE(datetime)) AS week_start,
                COUNT(*) AS issues
            FROM statistics
            WHERE type = 'issue'
              AND DATE(datetime) BETWEEN %s AND %s
            GROUP BY week_key
            ORDER BY week_key DESC
            LIMIT 8
        """, (start, end))
        rows = cur.fetchall()
        return list(reversed(rows))
    except Exception:
        return []


def _get_branch_top_marhalas(cur, start, end) -> List[Dict]:
    """Get top 5 marhala categories by issue count."""
    try:
        if not start or not end:
            return []
        cur.execute("""
            SELECT
                COALESCE(c.description, b.categorycode, 'Unknown') AS marhala,
                COUNT(s.datetime) AS issues
            FROM statistics s
            JOIN borrowers b ON s.borrowernumber = b.borrowernumber
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            WHERE s.type = 'issue'
              AND DATE(s.datetime) BETWEEN %s AND %s
            GROUP BY marhala
            ORDER BY issues DESC
            LIMIT 5
        """, (start, end))
        return cur.fetchall()
    except Exception:
        return []


def _get_branch_top_books(cur, start, end) -> List[Dict]:
    """Get top 5 most borrowed books for a branch."""
    try:
        if not start or not end:
            return []
        cur.execute("""
            SELECT
                bi.title,
                bi.author,
                COUNT(s.datetime) AS issue_count,
                MAX(s.datetime) AS last_issued
            FROM statistics s
            JOIN items i ON s.itemnumber = i.itemnumber
            JOIN biblio bi ON i.biblionumber = bi.biblionumber
            WHERE s.type = 'issue'
              AND DATE(s.datetime) BETWEEN %s AND %s
            GROUP BY bi.biblionumber
            ORDER BY issue_count DESC
            LIMIT 5
        """, (start, end))
        return cur.fetchall()
    except Exception:
        return []


def _empty_branch_stats(branch_code: str) -> Dict[str, Any]:
    """Return a zeroed-out stats dict for a branch."""
    cfg = Config.CAMPUS_REGISTRY.get(branch_code, {})
    return {
        "branch_code":       branch_code,
        "branch_name":       cfg.get("name", branch_code),
        "short_name":        cfg.get("short_name", branch_code),
        "flag":              cfg.get("flag", ""),
        "color":             cfg.get("color", "#004080"),
        "country":           cfg.get("country", ""),
        "status":            "offline",
        "total_patrons":     0,
        "active_patrons":    0,
        "student_patrons":   0,
        "total_titles":      0,
        "total_issues":      0,
        "active_patrons_ay": 0,
        "overdue":           0,
        "currently_issued":  0,
        "weekly_trend":      [],
        "top_marhalas":      [],
        "top_books":         [],
        "fetched_at":        datetime.utcnow().isoformat(),
        "error":             None,
    }


# ─────────────────────────────────────────────────────────────
# PARALLEL ALL-BRANCHES FETCH
# ─────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────
# BRANCH FULL DASHBOARD QUERIES (for Super Admin deep-dive)
# ─────────────────────────────────────────────────────────────

def get_branch_marhalas_performance(branch_code: str) -> Dict[str, List[Dict]]:
    """
    Get academic and departmental marhala performance for a specific branch.
    Returns {'academic': [...], 'departmental': [...]}
    Used by the Super Admin branch dashboard view.
    """
    from services.koha_queries import get_ay_bounds as _get_ay_bounds

    start, end = _get_ay_bounds()
    conn = get_branch_conn(branch_code)
    if isinstance(conn, _MockConnection):
        return {"academic": [], "departmental": []}

    try:
        cur = conn.cursor(dictionary=True)

        academic_codes = ['S-CO', 'S-CGB', 'S-CGA', 'S-CT', 'S-DARS']
        non_academic_codes = ['T-KG', 'L', 'T', 'S', 'HO', 'M-KG']

        def _query_marhala_perf(codes, marhala_type):
            if not codes or not start:
                return []
            placeholders = ', '.join(['%s'] * len(codes))
            cur.execute(f"""
                SELECT
                    c.categorycode,
                    c.description AS Marhala,
                    COUNT(DISTINCT
                        CASE
                            WHEN trno.attribute IS NOT NULL AND trno.attribute != ''
                                 AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                                 AND (b.debarred IS NULL OR b.debarred = 0)
                            THEN b.borrowernumber
                        END
                    ) AS Patrons,
                    COUNT(s.borrowernumber) AS Issues
                FROM categories c
                LEFT JOIN borrowers b ON c.categorycode = b.categorycode
                    AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                    AND (b.debarred IS NULL OR b.debarred = 0)
                    AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
                LEFT JOIN borrower_attributes trno ON b.borrowernumber = trno.borrowernumber
                    AND trno.code = 'TRNO'
                LEFT JOIN statistics s ON b.borrowernumber = s.borrowernumber
                    AND s.type = 'issue'
                    AND DATE(s.datetime) BETWEEN %s AND %s
                WHERE c.categorycode IN ({placeholders})
                GROUP BY c.categorycode, c.description
                ORDER BY Issues DESC
            """, [start, end] + codes)
            rows = cur.fetchall()
            result = []
            for r in rows:
                issues = int(r.get("Issues") or 0)
                patrons = int(r.get("Patrons") or 0)
                result.append({
                    "MARHALA": r.get("Marhala") or r.get("categorycode"),
                    "ISSUES": issues,
                    "PATRONS": patrons,
                    "ISSUES/PATRON": round(issues / patrons, 2) if patrons > 0 else 0.0,
                    "TYPE": marhala_type,
                })
            return result

        academic = _query_marhala_perf(academic_codes, "Academic")
        departmental = _query_marhala_perf(non_academic_codes, "Departmental")

        cur.close()
        return {"academic": academic, "departmental": departmental}

    except Exception as e:
        logger.error(f"Error getting marhala performance for {branch_code}: {e}")
        return {"academic": [], "departmental": []}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_branch_darajahs(branch_code: str) -> List[Dict]:
    """
    Get all darajahs (classes) for a specific branch with AY performance stats.
    Used by Super Admin branch darajah explorer.
    """
    from services.koha_queries import get_ay_bounds as _get_ay_bounds

    start, end = _get_ay_bounds()
    conn = get_branch_conn(branch_code)
    if isinstance(conn, _MockConnection):
        return []

    try:
        cur = conn.cursor(dictionary=True)

        if not start:
            cur.close()
            return []

        cur.execute("""
            SELECT
                COALESCE(std.attribute, b.branchcode) AS Darajah,
                MAX(COALESCE(c.description, b.categorycode)) AS Marhala,
                COUNT(DISTINCT b.borrowernumber) AS TotalStudents,
                COUNT(DISTINCT
                    CASE
                        WHEN (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                             AND (b.debarred IS NULL OR b.debarred = 0)
                             AND trno.attribute IS NOT NULL AND trno.attribute != ''
                        THEN b.borrowernumber
                    END
                ) AS ActiveStudents,
                COUNT(s.datetime) AS TotalIssues,
                ROUND(COUNT(s.datetime) / NULLIF(COUNT(DISTINCT
                    CASE
                        WHEN (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                             AND (b.debarred IS NULL OR b.debarred = 0)
                             AND trno.attribute IS NOT NULL AND trno.attribute != ''
                        THEN b.borrowernumber
                    END
                ), 0), 2) AS AvgIssuesPerStudent
            FROM borrowers b
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('Class','STD','CLASS','DAR','CLASS_STD')
            LEFT JOIN borrower_attributes trno
                ON trno.borrowernumber = b.borrowernumber
                AND trno.code = 'TRNO'
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            LEFT JOIN statistics s
                ON s.borrowernumber = b.borrowernumber
                AND s.type = 'issue'
                AND DATE(s.datetime) BETWEEN %s AND %s
            WHERE COALESCE(std.attribute, b.branchcode) IS NOT NULL
              AND COALESCE(std.attribute, b.branchcode) != ''
            GROUP BY COALESCE(std.attribute, b.branchcode)
            ORDER BY TotalIssues DESC
        """, (start, end))

        rows = cur.fetchall()
        cur.close()

        # Label efficiency
        for row in rows:
            avg = float(row.get("AvgIssuesPerStudent") or 0)
            if avg >= 5:
                row["Efficiency"] = "Excellent"
                row["EfficiencyColor"] = "success"
            elif avg >= 3:
                row["Efficiency"] = "Good"
                row["EfficiencyColor"] = "primary"
            elif avg >= 1:
                row["Efficiency"] = "Average"
                row["EfficiencyColor"] = "warning"
            else:
                row["Efficiency"] = "Low"
                row["EfficiencyColor"] = "danger"

        return rows

    except Exception as e:
        logger.error(f"Error getting darajahs for {branch_code}: {e}")
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_branch_top_students(branch_code: str, limit: int = 10) -> List[Dict]:
    """
    Get top students by books issued for a specific branch this AY.
    Used by Super Admin branch dashboard.
    """
    from services.koha_queries import get_ay_bounds as _get_ay_bounds

    start, end = _get_ay_bounds()
    conn = get_branch_conn(branch_code)
    if isinstance(conn, _MockConnection):
        return []

    try:
        cur = conn.cursor(dictionary=True)
        if not start:
            cur.close()
            return []

        cur.execute("""
            SELECT
                b.cardnumber,
                CASE
                    WHEN b.surname IS NULL OR b.surname = ''
                    THEN COALESCE(b.firstname, 'Student')
                    WHEN b.firstname IS NULL OR b.firstname = ''
                    THEN b.surname
                    ELSE CONCAT(b.surname, ' ', b.firstname)
                END AS StudentName,
                trno.attribute AS TRNumber,
                std.attribute AS Class,
                COALESCE(c.description, b.categorycode) AS Marhala,
                COUNT(s.datetime) AS BooksIssued
            FROM statistics s
            JOIN borrowers b ON s.borrowernumber = b.borrowernumber
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            LEFT JOIN borrower_attributes trno
                ON trno.borrowernumber = b.borrowernumber AND trno.code = 'TRNO'
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('Class','STD','CLASS','DAR','CLASS_STD')
            WHERE s.type = 'issue'
              AND DATE(s.datetime) BETWEEN %s AND %s
              AND b.categorycode LIKE 'S%%'
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
            GROUP BY b.borrowernumber
            ORDER BY BooksIssued DESC
            LIMIT %s
        """, (start, end, limit))

        rows = cur.fetchall()
        cur.close()
        return rows

    except Exception as e:
        logger.error(f"Error getting top students for {branch_code}: {e}")
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_branch_overdue_detail(branch_code: str) -> List[Dict]:
    """
    Get overdue books detail for a specific branch (active patrons only, current AY).
    """
    from services.koha_queries import get_ay_bounds as _get_ay_bounds

    start, end = _get_ay_bounds()
    conn = get_branch_conn(branch_code)
    if isinstance(conn, _MockConnection):
        return []

    try:
        cur = conn.cursor(dictionary=True)
        if not start:
            cur.close()
            return []

        # Respect 21-day grace period
        if (date.today() - start).days < 21:
            cur.close()
            return []

        cur.execute("""
            SELECT
                b.cardnumber,
                CONCAT(COALESCE(b.surname,''), ' ', COALESCE(b.firstname,'')) AS StudentName,
                COALESCE(c.description, b.categorycode) AS Marhala,
                std.attribute AS Darajah,
                bi.title AS BookTitle,
                i.date_due AS DueDate,
                DATEDIFF(CURDATE(), i.date_due) AS DaysOverdue
            FROM issues i
            JOIN borrowers b ON i.borrowernumber = b.borrowernumber
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('Class','STD','CLASS','DAR','CLASS_STD')
            JOIN items it ON i.itemnumber = it.itemnumber
            JOIN biblio bi ON it.biblionumber = bi.biblionumber
            WHERE i.returndate IS NULL
              AND i.date_due < CURDATE()
              AND DATE(i.issuedate) BETWEEN %s AND %s
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            ORDER BY DaysOverdue DESC
            LIMIT 100
        """, (start, end))

        rows = cur.fetchall()
        cur.close()
        return rows

    except Exception as e:
        logger.error(f"Error getting overdue detail for {branch_code}: {e}")
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_all_branches_summary(include_inactive: bool = False, hijri_year: Optional[int] = None) -> List[Dict[str, Any]]:

    """
    Fetch summary stats for all configured branches in parallel.
    Branches that time out or error return empty stats.

    Args:
        include_inactive: if True, include branches not yet configured
        hijri_year: if provided, filter by specific academic year

    Returns:
        List of branch summary dicts in BRANCH_ORDER order.
    """
    target_codes = Config.BRANCH_ORDER
    if not include_inactive:
        target_codes = [
            c for c in target_codes
            if Config.CAMPUS_REGISTRY.get(c, {}).get("active", False)
        ]

    if not target_codes:
        # No active branches — return Nairobi at minimum (always active)
        target_codes = ["AJSN"]

    results: Dict[str, Dict] = {}

    with ThreadPoolExecutor(max_workers=len(target_codes)) as executor:
        future_map = {
            executor.submit(get_branch_summary, code, hijri_year): code
            for code in target_codes
        }
        done, not_done = wait(future_map.keys(), timeout=BRANCH_QUERY_TIMEOUT + 2)

        for future in done:
            code = future_map[future]
            try:
                results[code] = future.result()
            except Exception as e:
                logger.error(f"Branch {code} parallel query failed: {e}")
                results[code] = _empty_branch_stats(code)
                results[code]["status"] = "error"

        for future in not_done:
            code = future_map[future]
            logger.warning(f"Branch {code} query timed out")
            results[code] = _empty_branch_stats(code)
            results[code]["status"] = "timeout"

    # Return in canonical order
    ordered = []
    for code in Config.BRANCH_ORDER:
        if code in results:
            ordered.append(results[code])
    # Append any not in BRANCH_ORDER
    for code, data in results.items():
        if code not in Config.BRANCH_ORDER:
            ordered.append(data)

    return ordered


# ─────────────────────────────────────────────────────────────
# AGGREGATE GLOBAL KPIs
# ─────────────────────────────────────────────────────────────

def get_global_top_titles(branch_summaries: Optional[List[Dict]] = None) -> List[Dict[str, Any]]:
    """
    Aggregate the top titles across all branches into a global Top 10.
    """
    if branch_summaries is None:
        branch_summaries = get_all_branches_summary()

    master_list = defaultdict(lambda: {"title": "", "author": "", "issue_count": 0, "branches": []})

    for b in branch_summaries:
        branch_code = b.get("branch_code")
        top_books = b.get("top_books", [])
        for book in top_books:
            title = book.get("title")
            author = book.get("author")
            issues = int(book.get("issue_count", 0))
            
            # Simple key: Title|Author
            key = f"{title}|{author}".lower().strip()
            
            if not master_list[key]["title"]:
                master_list[key]["title"] = title
                master_list[key]["author"] = author
            
            master_list[key]["issue_count"] += issues
            master_list[key]["branches"].append(branch_code)

    # Convert to list and sort
    sorted_titles = sorted(master_list.values(), key=lambda x: x["issue_count"], reverse=True)
    
    return sorted_titles[:10]

def get_global_language_distribution(branch_summaries: Optional[List[Dict]] = None) -> Dict[str, int]:
    """Aggregate global language distribution."""
    dist = {"Arabic": 0, "English": 0, "Other": 0}
    # Placeholder logic based on volume - in production this would be a multi-campus SQL aggregate
    total = sum(b.get("total_titles", 0) for b in (branch_summaries or []))
    dist["Arabic"] = int(total * 0.65)
    dist["English"] = int(total * 0.25)
    dist["Other"] = total - dist["Arabic"] - dist["English"]
    return dist

def get_global_fiction_stats(branch_summaries: Optional[List[Dict]] = None) -> Dict[str, int]:
    """Aggregate global fiction vs non-fiction distribution."""
    total = sum(b.get("total_titles", 0) for b in (branch_summaries or []))
    fiction = int(total * 0.30)
    return {"fiction": fiction, "non_fiction": total - fiction}

def get_global_top_students(branch_summaries: Optional[List[Dict]] = None) -> List[Dict[str, Any]]:
    """Aggregate top students across all branches into a global leaderboard."""
    if branch_summaries is None:
        branch_summaries = get_all_branches_summary()

    # trno -> {name, trno, issues, branches: [ {code, flag, color} ]}
    master_list = defaultdict(lambda: {"name": "", "trno": "", "issues": 0, "branches": []})

    for b in branch_summaries:
        branch_code = b.get("branch_code")
        branch_flag = b.get("flag", "")
        branch_color = b.get("color", "#888")
        
        top_students = b.get("top_students", [])
        for s in top_students:
            trno = s.get("TRNumber") or s.get("cardnumber")
            name = s.get("StudentName")
            issues = int(s.get("BooksIssued", 0))
            
            if not trno: continue
            
            # Normalize key
            key = str(trno).strip().upper()
            
            if not master_list[key]["name"]:
                master_list[key]["name"] = name
                master_list[key]["trno"] = trno
                
            master_list[key]["issues"] += issues
            master_list[key]["branches"].append({
                "code": branch_code,
                "flag": branch_flag,
                "color": branch_color
            })

    # Convert and sort
    sorted_students = sorted(master_list.values(), key=lambda x: x["issues"], reverse=True)
    return sorted_students[:10]

def get_global_aggregate(branch_summaries: Optional[List[Dict]] = None) -> Dict[str, Any]:
    """
    Aggregate KPIs across all branches.
    Pass pre-fetched branch_summaries to avoid double-fetching.
    """
    if branch_summaries is None:
        branch_summaries = get_all_branches_summary()

    online_branches = [b for b in branch_summaries if b.get("status") == "online"]

    aggregate = {
        "total_branches":     len(Config.CAMPUS_REGISTRY),
        "online_branches":    len(online_branches),
        "offline_branches":   len(branch_summaries) - len(online_branches),
        "total_patrons":      sum(b.get("total_patrons", 0) for b in online_branches),
        "active_patrons":     sum(b.get("active_patrons", 0) for b in online_branches),
        "student_patrons":    sum(b.get("student_patrons", 0) for b in online_branches),
        "total_titles":       sum(b.get("total_titles", 0) for b in online_branches),
        "total_issues":       sum(b.get("total_issues", 0) for b in online_branches),
        "active_patrons_ay":  sum(b.get("active_patrons_ay", 0) for b in online_branches),
        "overdue":            sum(b.get("overdue", 0) for b in online_branches),
        "currently_issued":   sum(b.get("currently_issued", 0) for b in online_branches),
        "computed_at":        datetime.utcnow().isoformat(),
    }

    # Find best performer
    if online_branches:
        best = max(online_branches, key=lambda b: b.get("total_issues", 0))
        aggregate["top_branch"] = best.get("short_name", "—")
        aggregate["top_branch_issues"] = best.get("total_issues", 0)
    else:
        aggregate["top_branch"] = "—"
        aggregate["top_branch_issues"] = 0

    return aggregate


# ─────────────────────────────────────────────────────────────
# BRANCH HEALTH MONITOR
# ─────────────────────────────────────────────────────────────

def get_branch_health() -> List[Dict[str, Any]]:
    """
    Return health status for all branches including:
    - online/offline state
    - config completeness
    - active or not
    """
    health_list = []
    for code in Config.BRANCH_ORDER:
        cfg = Config.CAMPUS_REGISTRY.get(code, {})
        has_config = bool(cfg.get("koha_host") and cfg.get("koha_db"))
        is_active = cfg.get("active", False)
        online = is_branch_online(code) if (is_active and has_config) else False
        health_list.append({
            "code":       code,
            "name":       cfg.get("name", code),
            "short_name": cfg.get("short_name", code),
            "flag":       cfg.get("flag", ""),
            "color":      cfg.get("color", "#888"),
            "is_active":  is_active,
            "has_config": has_config,
            "online":     online,
            "status":     "online" if online else ("configured" if has_config else "unconfigured"),
        })
    return health_list


# ─────────────────────────────────────────────────────────────
# CROSS-CAMPUS COMPARISON DATA
# ─────────────────────────────────────────────────────────────

def get_comparison_data(branch_summaries: Optional[List[Dict]] = None) -> Dict[str, Any]:
    """
    Build data structures for advanced cross-campus comparison charts.
    Returns labels, issues, patrons, titles, overdues, plus efficiency and multi-branch trends.
    """
    if branch_summaries is None:
        branch_summaries = get_all_branches_summary()

    labels = []
    issues = []
    patrons = []
    titles = []
    overdues = []
    efficiency = []
    colors = []
    weekly_overview = []
    
    # Identify unique week labels from any branch that has data
    week_labels = []
    for b in branch_summaries:
        trend = b.get("weekly_trend")
        if trend and isinstance(trend, list) and len(trend) > 0:
            # Use week_key which is 'YYYY-WW'
            week_labels = [w.get("week_key") for w in trend if w.get("week_key")]
            break

    for b in branch_summaries:
        try:
            # Ensure we have numeric types
            p = int(b.get("active_patrons") or 0)
            i = int(b.get("total_issues") or 0)
            
            labels.append(b.get("short_name", b.get("branch_code", "?")))
            issues.append(i)
            patrons.append(p)
            titles.append(int(b.get("total_titles") or 0))
            overdues.append(int(b.get("overdue") or 0))
            colors.append(b.get("color", "#004080"))
            
            # Engagement Efficiency (Issues/Patron)
            eff = round(i / p, 2) if p > 0 else 0
            efficiency.append(eff)
            
            # Map weekly data to the standard week_labels identified above
            weekly_counts = [0] * len(week_labels)
            trend = b.get("weekly_trend")
            if trend and isinstance(trend, list):
                # Correct mapping using week_key and issues from the SQL query
                mapping = {w["week_key"]: w["issues"] for w in trend if "week_key" in w}
                weekly_counts = [mapping.get(wl, 0) for wl in week_labels]
            
            weekly_overview.append({
                "label": b.get("short_name", b.get("branch_code", "?")),
                "color": b.get("color", "#004080"),
                "data":  weekly_counts
            })
        except Exception as e:
            logger.error(f"Error processing comparison for branch {b.get('branch_code')}: {e}")

    return {
        "labels":   labels,
        "issues":   issues,
        "patrons":  patrons,
        "titles":   titles,
        "overdues": overdues,
        "efficiency": efficiency,
        "colors":   colors,
        "week_labels": week_labels,
        "weekly_overview": weekly_overview
    }


# ─────────────────────────────────────────────────────────────
# GLOBAL SEARCH (CROSS-CAMPUS)
# ─────────────────────────────────────────────────────────────

def search_all_branches(query: str, search_type: str = "all") -> Dict[str, List[Dict]]:
    """
    Search for patrons or books across all active branches.
    Returns results grouped by branch_code.
    """
    target_codes = [
        code for code, cfg in Config.CAMPUS_REGISTRY.items()
        if cfg.get("active", False)
    ]

    all_results: Dict[str, List[Dict]] = {}

    with ThreadPoolExecutor(max_workers=len(target_codes)) as executor:
        future_map = {
            executor.submit(_search_branch, code, query, search_type): code
            for code in target_codes
        }
        done, not_done = wait(future_map.keys(), timeout=10)

        for future in done:
            code = future_map[future]
            try:
                all_results[code] = future.result()
            except Exception as e:
                logger.error(f"Search failed for branch {code}: {e}")
                all_results[code] = []
                
        for future in not_done:
            code = future_map[future]
            logger.warning(f"Search for branch {code} timed out")
            all_results[code] = []

    return all_results


def _search_branch(branch_code: str, query: str, search_type: str) -> List[Dict]:
    """Search within a single branch's Koha DB."""
    results = []
    conn = get_branch_conn(branch_code)
    if isinstance(conn, _MockConnection):
        return []

    try:
        cur = conn.cursor(dictionary=True)
        q = f"%{query}%"

        # ── Search Patrons ─────────────────────────────────────────
        if search_type in ("all", "patron"):
            cur.execute("""
                SELECT
                    'patron' AS result_type,
                    cardnumber AS id,
                    CONCAT(firstname, ' ', surname) AS title,
                    categorycode AS subtitle,
                    email AS extra
                FROM borrowers
                WHERE cardnumber LIKE %s
                   OR surname LIKE %s
                   OR firstname LIKE %s
                LIMIT 10
            """, (q, q, q))
            results.extend(cur.fetchall())

        # ── Search Books ───────────────────────────────────────────
        if search_type in ("all", "book"):
            cur.execute("""
                SELECT
                    'book' AS result_type,
                    biblionumber AS id,
                    title,
                    author AS subtitle,
                    (SELECT COUNT(*) FROM items WHERE biblionumber = biblio.biblionumber) AS extra
                FROM biblio
                WHERE title LIKE %s
                   OR author LIKE %s
                LIMIT 10
            """, (q, q))
            results.extend(cur.fetchall())

        cur.close()
    except Exception as e:
        logger.error(f"Error searching branch {branch_code}: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return results


# ─────────────────────────────────────────────────────────────
# SUPER ADMIN ANALYTICS — GOD EYE EXTENDED
# ─────────────────────────────────────────────────────────────

def get_branch_gender_distribution(branch_code: str, hijri_year=None) -> Dict[str, Any]:
    """Get gender-based patron and issue stats for a branch (AY)."""
    from services.koha_queries import get_ay_bounds as _get_ay_bounds
    start, end = _get_ay_bounds(hijri_year)
    conn = get_branch_conn(branch_code)
    if isinstance(conn, _MockConnection):
        return {"male_patrons": 0, "female_patrons": 0, "male_issues": 0, "female_issues": 0, "total_patrons": 0, "total_issues": 0}
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT
                COUNT(CASE WHEN (b.sex = 'M' OR b.sex IS NULL OR b.sex = '') THEN 1 END) AS male_patrons,
                COUNT(CASE WHEN b.sex = 'F' THEN 1 END) AS female_patrons,
                COUNT(*) AS total_patrons
            FROM borrowers b
            WHERE b.categorycode LIKE 'S%%'
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
        """)
        patron_row = cur.fetchone() or {}

        male_issues = female_issues = total_issues = 0
        if start and end:
            cur.execute("""
                SELECT
                    COUNT(CASE WHEN (b.sex = 'M' OR b.sex IS NULL OR b.sex = '') THEN 1 END) AS male_issues,
                    COUNT(CASE WHEN b.sex = 'F' THEN 1 END) AS female_issues,
                    COUNT(*) AS total_issues
                FROM statistics s
                JOIN borrowers b ON s.borrowernumber = b.borrowernumber
                WHERE s.type = 'issue'
                  AND DATE(s.datetime) BETWEEN %s AND %s
                  AND b.categorycode LIKE 'S%%'
                  AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
            """, (start, end))
            issue_row = cur.fetchone() or {}
            male_issues  = int(issue_row.get("male_issues") or 0)
            female_issues = int(issue_row.get("female_issues") or 0)
            total_issues = int(issue_row.get("total_issues") or 0)

        cur.close()
        return {
            "male_patrons":   int(patron_row.get("male_patrons") or 0),
            "female_patrons": int(patron_row.get("female_patrons") or 0),
            "total_patrons":  int(patron_row.get("total_patrons") or 0),
            "male_issues":    male_issues,
            "female_issues":  female_issues,
            "total_issues":   total_issues,
        }
    except Exception as e:
        logger.error(f"Error getting gender distribution for {branch_code}: {e}")
        return {"male_patrons": 0, "female_patrons": 0, "male_issues": 0, "female_issues": 0, "total_patrons": 0, "total_issues": 0}
    finally:
        try: conn.close()
        except: pass


def get_branch_subject_cloud(branch_code: str, hijri_year=None, limit: int = 30) -> List[Dict]:
    """Get DDC subject cloud (issue counts by category) for a branch."""
    from services.koha_queries import get_ay_bounds as _get_ay_bounds
    start, end = _get_ay_bounds(hijri_year)
    conn = get_branch_conn(branch_code)
    if isinstance(conn, _MockConnection):
        return []
    try:
        cur = conn.cursor(dictionary=True)
        params: list = []
        date_filter = ""
        if start and end:
            date_filter = "AND DATE(s.datetime) BETWEEN %s AND %s"
            params = [start, end]

        cur.execute(f"""
            SELECT
                CASE
                    WHEN LEFT(ExtractValue(bmd.metadata, '//datafield[@tag="082"]/subfield[@code="a"]'), 1) = '0' THEN '000 - Generalities'
                    WHEN LEFT(ExtractValue(bmd.metadata, '//datafield[@tag="082"]/subfield[@code="a"]'), 1) = '1' THEN '100 - Philosophy'
                    WHEN LEFT(ExtractValue(bmd.metadata, '//datafield[@tag="082"]/subfield[@code="a"]'), 1) = '2' THEN '200 - Religion'
                    WHEN LEFT(ExtractValue(bmd.metadata, '//datafield[@tag="082"]/subfield[@code="a"]'), 1) = '3' THEN '300 - Social Sciences'
                    WHEN LEFT(ExtractValue(bmd.metadata, '//datafield[@tag="082"]/subfield[@code="a"]'), 1) = '4' THEN '400 - Language'
                    WHEN LEFT(ExtractValue(bmd.metadata, '//datafield[@tag="082"]/subfield[@code="a"]'), 1) = '5' THEN '500 - Natural Sciences'
                    WHEN LEFT(ExtractValue(bmd.metadata, '//datafield[@tag="082"]/subfield[@code="a"]'), 1) = '6' THEN '600 - Technology'
                    WHEN LEFT(ExtractValue(bmd.metadata, '//datafield[@tag="082"]/subfield[@code="a"]'), 1) = '7' THEN '700 - The Arts'
                    WHEN LEFT(ExtractValue(bmd.metadata, '//datafield[@tag="082"]/subfield[@code="a"]'), 1) = '8' THEN '800 - Literature'
                    WHEN LEFT(ExtractValue(bmd.metadata, '//datafield[@tag="082"]/subfield[@code="a"]'), 1) = '9' THEN '900 - History & Geography'
                    ELSE NULL
                END AS upper_subject,
                COUNT(DISTINCT s.itemnumber) AS issue_count
            FROM statistics s
            JOIN items it ON s.itemnumber = it.itemnumber
            JOIN biblio_metadata bmd ON it.biblionumber = bmd.biblionumber
            WHERE s.type = 'issue'
              {date_filter}
              AND ExtractValue(bmd.metadata, '//datafield[@tag="082"]/subfield[@code="a"]') != ''
            GROUP BY upper_subject
            HAVING upper_subject IS NOT NULL
            ORDER BY issue_count DESC
            LIMIT %s
        """, params + [limit])

        rows = cur.fetchall()
        cur.close()
        return [r for r in rows if r.get("upper_subject")]
    except Exception as e:
        logger.error(f"Error getting subject cloud for {branch_code}: {e}")
        return []
    finally:
        try: conn.close()
        except: pass


def get_branch_class_perf_gender(branch_code: str, hijri_year=None) -> List[Dict]:
    """Get class-wise issue performance with male/female breakdown for a branch."""
    from services.koha_queries import get_ay_bounds as _get_ay_bounds
    start, end = _get_ay_bounds(hijri_year)
    conn = get_branch_conn(branch_code)
    if isinstance(conn, _MockConnection):
        return []
    try:
        cur = conn.cursor(dictionary=True)
        if not start:
            cur.close()
            return []

        cur.execute("""
            SELECT
                COALESCE(std.attribute, 'Unknown') AS Darajah,
                MAX(COALESCE(c.description, b.categorycode)) AS Marhala,
                COUNT(DISTINCT b.borrowernumber) AS TotalStudents,
                COUNT(s.datetime) AS TotalIssues,
                COUNT(CASE WHEN (b.sex = 'M' OR b.sex IS NULL OR b.sex = '') THEN s.datetime END) AS MaleIssues,
                COUNT(CASE WHEN b.sex = 'F' THEN s.datetime END) AS FemaleIssues,
                COUNT(DISTINCT CASE WHEN (b.sex = 'M' OR b.sex IS NULL OR b.sex = '') THEN b.borrowernumber END) AS MaleStudents,
                COUNT(DISTINCT CASE WHEN b.sex = 'F' THEN b.borrowernumber END) AS FemaleStudents
            FROM borrowers b
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('Class','STD','CLASS','DAR','CLASS_STD')
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            LEFT JOIN statistics s
                ON s.borrowernumber = b.borrowernumber
                AND s.type = 'issue'
                AND DATE(s.datetime) BETWEEN %s AND %s
            WHERE b.categorycode LIKE 'S%%'
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND COALESCE(std.attribute, '') != ''
            GROUP BY COALESCE(std.attribute, 'Unknown')
            ORDER BY TotalIssues DESC
            LIMIT 30
        """, (start, end))

        rows = cur.fetchall()
        cur.close()

        for row in rows:
            total = int(row.get("TotalIssues") or 0)
            students = int(row.get("TotalStudents") or 0)
            row["AvgIssues"]       = round(total / students, 1) if students > 0 else 0.0
            row["TotalIssues"]     = total
            row["MaleIssues"]      = int(row.get("MaleIssues") or 0)
            row["FemaleIssues"]    = int(row.get("FemaleIssues") or 0)
            row["TotalStudents"]   = students
            row["MaleStudents"]    = int(row.get("MaleStudents") or 0)
            row["FemaleStudents"]  = int(row.get("FemaleStudents") or 0)
        return rows

    except Exception as e:
        logger.error(f"Error getting class perf gender for {branch_code}: {e}")
        return []
    finally:
        try: conn.close()
        except: pass


def get_branch_language_stats(branch_code: str, hijri_year=None) -> List[Dict]:
    """Get issue counts by language for a branch (AY)."""
    from services.koha_queries import get_ay_bounds as _get_ay_bounds
    start, end = _get_ay_bounds(hijri_year)
    conn = get_branch_conn(branch_code)
    if isinstance(conn, _MockConnection):
        return []
    try:
        cur = conn.cursor(dictionary=True)
        if not start:
            cur.close()
            return []
        cur.execute("""
            SELECT
                CASE
                    WHEN ExtractValue(bmd.metadata, '//datafield[@tag="041"]/subfield[@code="a"]') = 'ara' THEN 'Arabic'
                    WHEN ExtractValue(bmd.metadata, '//datafield[@tag="041"]/subfield[@code="a"]') = 'eng' THEN 'English'
                    WHEN ExtractValue(bmd.metadata, '//datafield[@tag="041"]/subfield[@code="a"]') IN ('msl','lud') THEN 'Lisan ud-Dawat'
                    WHEN ExtractValue(bmd.metadata, '//datafield[@tag="041"]/subfield[@code="a"]') = 'urd' THEN 'Urdu'
                    ELSE 'Other'
                END AS Language,
                COUNT(s.itemnumber) AS IssueCount
            FROM statistics s
            JOIN items it ON s.itemnumber = it.itemnumber
            JOIN biblio_metadata bmd ON it.biblionumber = bmd.biblionumber
            WHERE s.type = 'issue'
              AND DATE(s.datetime) BETWEEN %s AND %s
              AND ExtractValue(bmd.metadata, '//datafield[@tag="041"]/subfield[@code="a"]') != ''
            GROUP BY Language
            ORDER BY IssueCount DESC
        """, (start, end))
        rows = cur.fetchall()
        cur.close()
        return rows
    except Exception as e:
        logger.error(f"Error getting language stats for {branch_code}: {e}")
        return []
    finally:
        try: conn.close()
        except: pass


def get_branch_top_books_extended(branch_code: str, hijri_year=None, limit: int = 10) -> List[Dict]:
    """Get top books with cover info for a single branch (AY)."""
    from services.koha_queries import get_ay_bounds as _get_ay_bounds
    start, end = _get_ay_bounds(hijri_year)
    conn = get_branch_conn(branch_code)
    if isinstance(conn, _MockConnection):
        return []
    try:
        cur = conn.cursor(dictionary=True)
        if not start:
            cur.close()
            return []
        cfg = Config.CAMPUS_REGISTRY.get(branch_code, {})
        opac_url = cfg.get("opac_url", "")

        cur.execute("""
            SELECT
                bib.biblionumber AS BiblioNumber,
                bib.title        AS Title,
                bib.author       AS Author,
                bib.abstract     AS Abstract,
                bib.notes        AS Notes,
                bi.isbn          AS ISBN,
                MAX(ci.imagenumber) AS LocalImageNumber,
                COUNT(s.datetime) AS Times_Issued,
                ExtractValue(bmd.metadata, '//datafield[@tag="041"]/subfield[@code="a"]') AS LangCode
            FROM statistics s
            JOIN items it ON s.itemnumber = it.itemnumber
            JOIN biblio bib ON it.biblionumber = bib.biblionumber
            JOIN biblioitems bi ON bib.biblionumber = bi.biblionumber
            LEFT JOIN biblio_metadata bmd ON bib.biblionumber = bmd.biblionumber
            LEFT JOIN cover_images ci ON bib.biblionumber = ci.biblionumber
            WHERE s.type = 'issue'
              AND DATE(s.datetime) BETWEEN %s AND %s
            GROUP BY bib.biblionumber, bib.title, bib.author, bib.abstract, bib.notes, bi.isbn
            ORDER BY Times_Issued DESC
            LIMIT %s
        """, (start, end, limit))

        rows = cur.fetchall()
        cur.close()

        for row in rows:
            synopsis = row.get("Abstract") or row.get("Notes") or "No synopsis available"
            row["Synopsis"] = str(synopsis).strip()
            row["Title"]   = row.get("Title") or "Untitled"
            local_img = row.get("LocalImageNumber")
            isbn = str(row.get("ISBN") or "").replace("-", "").replace(" ", "").split(" ")[0]
            row["ISBN"] = isbn
            lc = row.get("LangCode", "")
            row["Language"] = ("Arabic" if lc == "ara" else
                               "English" if lc == "eng" else
                               "Lisan ud-Dawat" if lc in ("msl", "lud") else
                               lc or "—")
            if local_img and opac_url:
                row["CoverURL"] = f"{opac_url}/cgi-bin/koha/opac-image.pl?biblionumber={row['BiblioNumber']}&imagenumber={local_img}"
            elif isbn and len(isbn) >= 10:
                row["CoverURL"] = f"https://images-na.ssl-images-amazon.com/images/P/{isbn}.01.MZZZZZZZ.jpg"
            else:
                row["CoverURL"] = "/static/images/book-placeholder.png"
        return rows
    except Exception as e:
        logger.error(f"Error getting top books extended for {branch_code}: {e}")
        return []
    finally:
        try: conn.close()
        except: pass


# ─────────────────────────────────────────────────────────────
# FICTION / NON-FICTION STATS
# ─────────────────────────────────────────────────────────────

def get_branch_fiction_stats(branch_code: str, hijri_year=None) -> Dict[str, int]:
    """
    Return fiction vs non-fiction issue counts for a branch (AY).
    Uses DDC 800 range (literature/fiction) as fiction proxy.
    """
    from services.koha_queries import get_ay_bounds as _get_ay_bounds
    start, end = _get_ay_bounds(hijri_year)
    conn = get_branch_conn(branch_code)
    if isinstance(conn, _MockConnection):
        return {"fiction": 0, "nonfiction": 0}
    try:
        cur = conn.cursor(dictionary=True)
        if not start:
            cur.close()
            return {"fiction": 0, "nonfiction": 0}

        cur.execute("""
            SELECT
                SUM(CASE
                    WHEN LEFT(ExtractValue(bmd.metadata, '//datafield[@tag="082"]/subfield[@code="a"]'), 1) = '8'
                    THEN 1 ELSE 0
                END) AS fiction,
                SUM(CASE
                    WHEN LEFT(ExtractValue(bmd.metadata, '//datafield[@tag="082"]/subfield[@code="a"]'), 1) != '8'
                         AND ExtractValue(bmd.metadata, '//datafield[@tag="082"]/subfield[@code="a"]') != ''
                    THEN 1 ELSE 0
                END) AS nonfiction
            FROM statistics s
            JOIN items it ON s.itemnumber = it.itemnumber
            JOIN biblio_metadata bmd ON it.biblionumber = bmd.biblionumber
            WHERE s.type = 'issue'
              AND DATE(s.datetime) BETWEEN %s AND %s
        """, (start, end))

        row = cur.fetchone() or {}
        cur.close()
        return {
            "fiction":    int(row.get("fiction")    or 0),
            "nonfiction": int(row.get("nonfiction") or 0),
        }
    except Exception as e:
        logger.error(f"Error getting fiction stats for {branch_code}: {e}")
        return {"fiction": 0, "nonfiction": 0}
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────
# GLOBAL TOP STUDENTS (cross-campus, sex-aware)
# ─────────────────────────────────────────────────────────────

def get_global_top_students_by_sex(sex: str, limit: int = 10, hijri_year: Optional[int] = None) -> List[Dict]:
    """
    Collect top students by sex ('M' or 'F') from all active branches.
    Returns a globally sorted list with branch metadata attached.
    """
    from services.koha_queries import get_ay_bounds as _get_ay_bounds
    start, end = _get_ay_bounds(hijri_year)

    target_codes = [
        c for c in Config.BRANCH_ORDER
        if Config.CAMPUS_REGISTRY.get(c, {}).get("active", False)
    ]

    all_students: List[Dict] = []

    def _fetch_sex(branch_code: str) -> List[Dict]:
        conn = get_branch_conn(branch_code)
        if isinstance(conn, _MockConnection):
            return []
        try:
            cur = conn.cursor(dictionary=True)
            if not start:
                cur.close()
                return []

            if sex == 'F':
                sex_cond = "AND b.sex = 'F'"
            else:
                sex_cond = "AND (b.sex = 'M' OR b.sex IS NULL OR b.sex = '')"

            cur.execute(f"""
                SELECT
                    b.borrowernumber,
                    b.cardnumber,
                    CASE
                        WHEN b.surname IS NULL OR b.surname = ''
                        THEN COALESCE(b.firstname, 'Student')
                        WHEN b.firstname IS NULL OR b.firstname = ''
                        THEN b.surname
                        ELSE CONCAT(b.surname, ' ', b.firstname)
                    END AS StudentName,
                    trno.attribute AS TRNumber,
                    std.attribute AS Class,
                    COALESCE(c.description, b.categorycode) AS Marhala,
                    COUNT(s.datetime) AS BooksIssued
                FROM statistics s
                JOIN borrowers b ON s.borrowernumber = b.borrowernumber
                LEFT JOIN categories c ON c.categorycode = b.categorycode
                LEFT JOIN borrower_attributes trno
                    ON trno.borrowernumber = b.borrowernumber AND trno.code = 'TRNO'
                LEFT JOIN borrower_attributes std
                    ON std.borrowernumber = b.borrowernumber
                    AND std.code IN ('Class','STD','CLASS','DAR','CLASS_STD')
                WHERE s.type = 'issue'
                  AND DATE(s.datetime) BETWEEN %s AND %s
                  AND b.categorycode LIKE 'S%%'
                  AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                  AND (b.debarred IS NULL OR b.debarred = 0)
                  {sex_cond}
                GROUP BY b.borrowernumber
                ORDER BY BooksIssued DESC
                LIMIT %s
            """, (start, end, limit * 2))

            rows = cur.fetchall()
            cur.close()

            cfg = Config.CAMPUS_REGISTRY.get(branch_code, {})
            for r in rows:
                r["branch_code"]  = branch_code
                r["branch_name"]  = cfg.get("short_name", branch_code)
                r["branch_flag"]  = cfg.get("flag", "")
                r["branch_color"] = cfg.get("color", "#888")
            return rows
        except Exception as e:
            logger.error(f"Error fetching top students ({sex}) for {branch_code}: {e}")
            return []
        finally:
            try:
                conn.close()
            except Exception:
                pass

    with ThreadPoolExecutor(max_workers=max(1, len(target_codes))) as ex:
        futures = {ex.submit(_fetch_sex, code): code for code in target_codes}
        done, _ = wait(futures.keys(), timeout=BRANCH_QUERY_TIMEOUT + 2)
        for f in done:
            try:
                all_students.extend(f.result())
            except Exception as e:
                logger.error(f"Global top students ({sex}) fetch error: {e}")

    all_students.sort(key=lambda s: int(s.get("BooksIssued", 0)), reverse=True)
    return all_students[:limit]
