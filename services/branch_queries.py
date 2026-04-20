# services/branch_queries.py — MULTI-CAMPUS DATA AGGREGATION SERVICE
"""
Branch-aware query service for the Head Office Sighat ul-Jamea Super Admin dashboard.

Each function accepts a `branch_code` parameter to route queries
to the correct Koha MySQL instance via the multi-pool connector.
"""
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
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

        # ── Monthly trend (Hijri months) ───────────────────────────
        monthly_trend = _get_branch_monthly_trend(cur, start, end)

        # ── Top marhala ────────────────────────────────────────────
        top_marhalas = _get_branch_top_marhalas(cur, start, end)

        # ── All Darajahs (Performance Breakdown) ───────────────────
        all_darajahs = _get_branch_all_darajahs(cur, start, end)
        
        # ── Top Darajahs (Performance - first 5) ───────────────────
        top_darajahs = all_darajahs[:5]

        # ── Top books ──────────────────────────────────────────────
        top_books = _get_branch_top_books(cur, start, end, branch_code)
        
        cfg = Config.CAMPUS_REGISTRY.get(branch_code, {})
        opac_url = cfg.get("opac_url", "")
        
        # Attach URLs to top books
        for book in top_books:
            isbn = str(book.get("isbn") or "").replace("-", "").replace(" ", "").split(" ")[0]
            local_img = book.get("local_imagenumber")
            
            if local_img and opac_url:
                book["CoverURL"] = f"{opac_url}/cgi-bin/koha/opac-image.pl?biblionumber={book['biblionumber']}&imagenumber={local_img}"
            elif isbn and len(isbn) >= 10:
                book["CoverURL"] = f"https://images-na.ssl-images-amazon.com/images/P/{isbn}.01.MZZZZZZZ.jpg"
            else:
                book["CoverURL"] = "/static/images/book-placeholder.png"
                
            if opac_url and book.get('biblionumber'):
                book["OPAC_URL"] = f"{opac_url}/cgi-bin/koha/opac-detail.pl?biblionumber={book['biblionumber']}"
            else:
                book["OPAC_URL"] = "#"

        # ── Today's activity ───────────────────────────────────────
        today_activity = _get_branch_today_activity(cur)
        
        # ── Total fees (AY) ────────────────────────────────────────
        total_fees = _get_branch_ay_fees(cur, start, end)

        # ── Top students ──────────────────────────────────────────
        top_students = _get_branch_top_students(cur, start, end)
        
        # ── Subject Cloud ──────────────────────────────────────────
        subject_cloud = _get_branch_subject_cloud(cur, start, end, limit=30)
        
        # ── Language Stats ─────────────────────────────────────────
        language_stats = _get_branch_language_stats(cur, start, end)
        
        # ── Fiction Stats ──────────────────────────────────────────
        fiction_stats = _get_branch_fiction_stats(cur, start, end)
        
        # ── Top Books by Language ──────────────────────────────────
        top_books_arabic = _get_branch_top_books_by_lang(cur, start, end, 'Arabic', limit=10)
        top_books_english = _get_branch_top_books_by_lang(cur, start, end, 'English', limit=10)
        
        # Attach URLs to language-specific book lists
        for book in top_books_arabic + top_books_english:
            isbn = str(book.get("isbn") or "").replace("-", "").replace(" ", "").split(" ")[0]
            local_img = book.get("local_imagenumber")
            
            if local_img and opac_url:
                book["CoverURL"] = f"{opac_url}/cgi-bin/koha/opac-image.pl?biblionumber={book['biblionumber']}&imagenumber={local_img}"
            elif isbn and len(isbn) >= 10:
                book["CoverURL"] = f"https://images-na.ssl-images-amazon.com/images/P/{isbn}.01.MZZZZZZZ.jpg"
            else:
                book["CoverURL"] = "/static/images/book-placeholder.png"

            if opac_url and book.get('biblionumber'):
                book["OPAC_URL"] = f"{opac_url}/cgi-bin/koha/opac-detail.pl?biblionumber={book['biblionumber']}"
            else:
                book["OPAC_URL"] = "#"

        cur.close()

        return {
            "branch_code":        branch_code,
            "branch_name":        cfg.get("name", branch_code),
            "short_name":         cfg.get("short_name", branch_code),
            "flag":               cfg.get("flag", ""),
            "color":              cfg.get("color", "#004080"),
            "country":            cfg.get("country", ""),
            "city":               cfg.get("city", ""),
            "status":             "online",
            "total_patrons":      int(patron_row.get("total_patrons") or 0),
            "active_patrons":     int(patron_row.get("active_patrons") or 0),
            "student_patrons":    int(patron_row.get("student_patrons") or 0),
            "total_titles":       int(title_row.get("total_titles") or 0),
            "total_issues":       total_issues,
            "active_patrons_ay":  active_patrons_ay,
            "overdue":            overdue,
            "currently_issued":   currently_issued,
            "today_activity":     today_activity,
            "total_fees":         total_fees,
            "weekly_trend":       weekly_trend,
            "monthly_trend":      monthly_trend,
            "top_marhalas":       top_marhalas,
            "top_darajahs":       top_darajahs,
            "all_darajahs":       all_darajahs,
            "top_books":          top_books,
            "top_books_arabic":   top_books_arabic,
            "top_books_english":  top_books_english,
            "top_students":       top_students,
            "subject_cloud":      subject_cloud,
            "language_stats":     language_stats,
            "fiction_stats":      fiction_stats,
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


def _get_branch_monthly_trend(cur, start, end) -> Dict[str, Any]:
    """Get full AY monthly trend using Hijri months."""
    from services.koha_queries import HIJRI_MONTHS, get_current_ay_year
    from hijri_converter import convert
    
    if not start or not end:
        return {"labels": [], "values": []}

    try:
        cur.execute("SELECT datetime FROM statistics WHERE type='issue' AND DATE(datetime) BETWEEN %s AND %s", (start, end))
        rows = cur.fetchall()
        
        base_h = get_current_ay_year()
        labels = []
        month_ranges = []
        m, y = 10, base_h  # Shawwal start
        for _ in range(12):
            labels.append(f"{HIJRI_MONTHS[m-1]} {y} H")
            month_ranges.append((y, m))
            m += 1
            if m > 12: 
                m = 1
                y += 1
            
        values = [0] * 12
        for r in rows:
            dt = r['datetime']
            try:
                h = convert.Gregorian(dt.year, dt.month, dt.day).to_hijri()
                for i, (ry, rm) in enumerate(month_ranges):
                    if h.year == ry and h.month == rm:
                        values[i] += 1
                        break
            except: 
                continue
        return {"labels": labels, "values": values}
    except Exception as e:
        logger.error(f"Error in _get_branch_monthly_trend: {e}")
        return {"labels": [], "values": []}


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


def _get_branch_top_books(cur, start, end, branch_code) -> List[Dict]:
    """Get top 5 most borrowed books for a branch, including cover data."""
    try:
        if not start or not end:
            return []
        
        cur.execute("""
            SELECT
                bi.title AS title,
                bi.author AS author,
                bi.biblionumber AS biblionumber,
                biti.isbn AS isbn,
                ExtractValue(bmd.metadata, '//datafield[@tag="520"]/subfield[@code="a"]') AS synopsis,
                MAX(ci.imagenumber) AS local_imagenumber,
                COUNT(s.datetime) AS issue_count,
                MAX(s.datetime) AS last_issued
            FROM statistics s
            JOIN items i ON s.itemnumber = i.itemnumber
            JOIN biblio bi ON i.biblionumber = bi.biblionumber
            JOIN biblioitems biti ON bi.biblionumber = biti.biblionumber
            JOIN biblio_metadata bmd ON bi.biblionumber = bmd.biblionumber
            LEFT JOIN cover_images ci ON bi.biblionumber = ci.biblionumber
            WHERE s.type = 'issue'
              AND DATE(s.datetime) BETWEEN %s AND %s
            GROUP BY bi.biblionumber
            ORDER BY issue_count DESC
            LIMIT 5
        """, (start, end))
        return cur.fetchall()
    except Exception:
        return []


def _get_branch_top_books_by_lang(cur, start, end, lang_code: str, limit: int = 10) -> List[Dict]:
    """Get top books by language for a branch."""
    try:
        if not start or not end:
            return []
        cur.execute("""
            SELECT
                bi.biblionumber AS biblionumber,
                bi.title AS title,
                bi.author AS author,
                biti.isbn AS isbn,
                ExtractValue(bmd.metadata, '//datafield[@tag="520"]/subfield[@code="a"]') AS synopsis,
                MAX(ci.imagenumber) AS local_imagenumber,
                COUNT(s.datetime) AS issues
            FROM statistics s
            JOIN items i ON s.itemnumber = i.itemnumber
            JOIN biblio bi ON i.biblionumber = bi.biblionumber
            JOIN biblioitems biti ON bi.biblionumber = biti.biblionumber
            LEFT JOIN cover_images ci ON bi.biblionumber = ci.biblionumber
            JOIN biblio_metadata bmd ON bi.biblionumber = bmd.biblionumber
            WHERE s.type = 'issue'
              AND DATE(s.datetime) BETWEEN %s AND %s
              AND ExtractValue(bmd.metadata, '//datafield[@tag="041"]/subfield[@code="a"]') = %s
            GROUP BY bi.biblionumber
            ORDER BY issues DESC
            LIMIT %s
        """, (start, end, lang_code, limit))
        return cur.fetchall()
    except Exception as e:
        logger.error(f"Error in _get_branch_top_books_by_lang ({lang_code}): {e}")
        return []


def _get_branch_top_students(cur, start, end, limit: int = 10) -> List[Dict]:
    """Get top students by books issued for a branch."""
    try:
        if not start or not end:
            return []
        cur.execute("""
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
            GROUP BY b.borrowernumber
            ORDER BY BooksIssued DESC
            LIMIT %s
        """, (start, end, limit))
        return cur.fetchall()
    except Exception:
        return []


def _get_branch_all_darajahs(cur, start, end) -> List[Dict]:
    """Get all darajahs for a branch with full statistics (AY)."""
    try:
        if not start or not end:
            return []
        
        # 1. Performance data (Issues and Active Borrowers)
        cur.execute("""
            SELECT
                COALESCE(std.attribute, 'General') AS darajah,
                COUNT(s.datetime) AS issues,
                COUNT(DISTINCT b.borrowernumber) AS active_borrowers
            FROM statistics s
            JOIN borrowers b ON s.borrowernumber = b.borrowernumber
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('Class','STD','CLASS','DAR','CLASS_STD')
            WHERE s.type = 'issue'
              AND DATE(s.datetime) BETWEEN %s AND %s
              AND b.categorycode LIKE 'S%%'
            GROUP BY darajah
        """, (start, end))
        perf_rows = cur.fetchall()
        perf_map = {r['darajah']: r for r in perf_rows}
        
        # 2. Total student counts (Total Enrolled)
        cur.execute("""
            SELECT
                COALESCE(std.attribute, 'General') AS darajah,
                COUNT(DISTINCT b.borrowernumber) AS total_students
            FROM borrowers b
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('Class','STD','CLASS','DAR','CLASS_STD')
            WHERE b.categorycode LIKE 'S%%'
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
            GROUP BY darajah
        """)
        enroll_rows = cur.fetchall()
        enroll_map = {r['darajah']: r['total_students'] for r in enroll_rows}
        
        # Merge
        result = []
        all_keys = set(list(perf_map.keys()) + list(enroll_map.keys()))
        for k in all_keys:
            if k == 'General' and k not in enroll_map and k not in perf_map:
                continue
            
            issues = int(perf_map.get(k, {}).get('issues', 0))
            active = int(perf_map.get(k, {}).get('active_borrowers', 0))
            total = int(enroll_map.get(k, 0))
            
            # If we have active borrowers but no total, use active as total
            if total == 0 and active > 0:
                total = active
            
            result.append({
                "darajah": k,
                "issues": issues,
                "active_borrowers": active,
                "total_students": total,
                "borrowing_rate": round(issues / total, 2) if total > 0 else 0
            })
            
        return sorted(result, key=lambda x: x["issues"], reverse=True)
    except Exception as e:
        logger.error(f"Error in _get_branch_all_darajahs: {e}")
        return []


def _get_branch_subject_cloud(cur, start, end, limit: int = 30) -> List[Dict]:
    """Internal helper to get DDC subject cloud using an existing cursor."""
    try:
        if not start or not end:
            return []
            
        cur.execute("""
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
                    ELSE 'Other'
                END AS upper_subject,
                COUNT(DISTINCT s.itemnumber) AS issue_count
            FROM statistics s
            JOIN items it ON s.itemnumber = it.itemnumber
            JOIN biblio_metadata bmd ON it.biblionumber = bmd.biblionumber
            WHERE s.type = 'issue'
              AND DATE(s.datetime) BETWEEN %s AND %s
              AND ExtractValue(bmd.metadata, '//datafield[@tag="082"]/subfield[@code="a"]') != ''
            GROUP BY upper_subject
            HAVING upper_subject IS NOT NULL AND upper_subject != 'Other'
            ORDER BY issue_count DESC
            LIMIT %s
        """, (start, end, limit))
        rows = cur.fetchall()
        return [r for r in rows if r.get("upper_subject")]
    except Exception as e:
        logger.error(f"Error in _get_branch_subject_cloud: {e}")
        return []


def _get_branch_language_stats(cur, start, end) -> List[Dict]:
    """Internal helper to get issue counts by language for a branch (AY)."""
    try:
        if not start or not end:
            return []
        cur.execute("""
            SELECT
                CASE
                    WHEN ExtractValue(bmd.metadata, '//datafield[@tag="041"]/subfield[@code="a"]') IN ('ara', 'Arabic') THEN 'Arabic'
                    WHEN ExtractValue(bmd.metadata, '//datafield[@tag="041"]/subfield[@code="a"]') IN ('eng', 'English') THEN 'English'
                    WHEN ExtractValue(bmd.metadata, '//datafield[@tag="041"]/subfield[@code="a"]') IN ('msl','lud','msa', 'Lisan-ud-Dawat') THEN 'Lisan-ud-Dawat'
                    WHEN ExtractValue(bmd.metadata, '//datafield[@tag="041"]/subfield[@code="a"]') IN ('urd', 'Urdu') THEN 'Urdu'
                    WHEN ExtractValue(bmd.metadata, '//datafield[@tag="041"]/subfield[@code="a"]') IN ('fre', 'French') THEN 'French'
                    WHEN ExtractValue(bmd.metadata, '//datafield[@tag="041"]/subfield[@code="a"]') IN ('per', 'Farsi') THEN 'Farsi'
                    WHEN ExtractValue(bmd.metadata, '//datafield[@tag="041"]/subfield[@code="a"]') IN ('mul', 'Multilingual') THEN 'Multilingual'
                    WHEN ExtractValue(bmd.metadata, '//datafield[@tag="041"]/subfield[@code="a"]') IN ('Gujarati') THEN 'Gujarati'
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
        return cur.fetchall()
    except Exception as e:
        logger.error(f"Error in _get_branch_language_stats: {e}")
        return []


def _get_branch_fiction_stats(cur, start, end) -> Dict[str, int]:
    """Internal helper to get fiction vs non-fiction stats for a branch (AY)."""
    try:
        if not start or not end:
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
        return {
            "fiction": int(row.get("fiction") or 0),
            "nonfiction": int(row.get("nonfiction") or 0)
        }
    except Exception as e:
        logger.error(f"Error in _get_branch_fiction_stats: {e}")
        return {"fiction": 0, "nonfiction": 0}


def _get_branch_today_activity(cur) -> Dict[str, int]:
    """Get checkout and checkin counts for today for a branch."""
    try:
        cur.execute("""
            SELECT 
                SUM(CASE WHEN type = 'issue' THEN 1 ELSE 0 END) AS checkouts,
                SUM(CASE WHEN type = 'return' THEN 1 ELSE 0 END) AS checkins
            FROM statistics
            WHERE DATE(datetime) = CURDATE()
        """)
        row = cur.fetchone() or {}
        return {
            "checkouts": int(row.get("checkouts") or 0),
            "checkins": int(row.get("checkins") or 0)
        }
    except Exception as e:
        logger.error(f"Error in _get_branch_today_activity: {e}")
        return {"checkouts": 0, "checkins": 0}


def _get_branch_ay_fees(cur, start, end) -> float:
    """Get total amount of fees paid in the academic year for a branch."""
    try:
        if not start or not end:
            return 0.0
        cur.execute("""
            SELECT COALESCE(SUM(-amount), 0) AS fees_paid
            FROM accountlines
            WHERE credit_type_code = 'PAYMENT'
              AND (status IS NULL OR status <> 'VOID')
              AND DATE(date) BETWEEN %s AND %s
        """, (start, end))
        row = cur.fetchone() or {}
        return float(row.get("fees_paid") or 0.0)
    except Exception as e:
        logger.error(f"Error in _get_branch_ay_fees: {e}")
        return 0.0


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
        "today_activity":    {"checkouts": 0, "checkins": 0},
        "total_fees":        0,
        "weekly_trend":      [],
        "monthly_trend":     {"labels": [], "values": []},
        "top_marhalas":      [],
        "top_books":         [],
        "top_books_arabic":  [],
        "top_books_english": [],
        "top_darajahs":      [],
        "all_darajahs":      [],
        "top_students":      [],
        "subject_cloud":     [],
        "language_stats":    [],
        "fiction_stats":     {"fiction": 0, "nonfiction": 0},
        "fetched_at":        datetime.utcnow().isoformat(),
        "error":             None,
    }


# ─────────────────────────────────────────────────────────────
# PARALLEL ALL-BRANCHES FETCH
# ─────────────────────────────────────────────────────────────

def get_all_branches_summary(include_inactive: bool = False, hijri_year: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Fetch summary stats for all configured branches in parallel.
    """
    target_codes = Config.BRANCH_ORDER
    if not include_inactive:
        target_codes = [
            c for c in target_codes
            if Config.CAMPUS_REGISTRY.get(c, {}).get("active", False)
        ]

    results = {}
    
    # Sequential fallback if parallel isn't working
    for code in target_codes:
        try:
            results[code] = get_branch_summary(code, hijri_year=hijri_year)
        except Exception as e:
            logger.error(f"Error fetching branch {code}: {e}")
            results[code] = _empty_branch_stats(code)
            results[code]["status"] = "error"

    # Process and fill gaps
    ordered = []
    for code in Config.BRANCH_ORDER:
        if code in results:
            data = results[code]
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

    master_list = defaultdict(lambda: {"title": "", "author": "", "issue_count": 0, "branches": [], "CoverURL": ""})

    for b in branch_summaries:
        branch_code = b.get("branch_code")
        branch_name = b.get("short_name", branch_code)
        branch_flag = b.get("flag", "")
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
                master_list[key]["CoverURL"] = book.get("CoverURL", "")
                master_list[key]["synopsis"] = book.get("Synopsis") or book.get("synopsis", "")
                master_list[key]["OPAC_URL"] = book.get("OPAC_URL", "#")
            
            master_list[key]["issue_count"] += issues
            master_list[key]["branches"].append({
                "code": branch_code,
                "name": branch_name,
                "flag": branch_flag
            })

    # Convert to list and sort
    sorted_titles = sorted(master_list.values(), key=lambda x: x["issue_count"], reverse=True)
    
    return sorted_titles[:10]


def get_global_language_distribution(branch_summaries: Optional[List[Dict]] = None) -> Dict[str, Any]:
    """Aggregate real language distribution from branch summaries with branch-wise breakdown."""
    dist = {}
    for b in (branch_summaries or []):
        if b.get("status") == "online":
            branch_code = b.get("branch_code")
            branch_name = b.get("short_name", branch_code)
            for lang_row in b.get("language_stats", []):
                name = lang_row.get("Language", "Other")
                count = int(lang_row.get("IssueCount") or 0)
                
                if name not in dist:
                    dist[name] = {"total": 0, "branches": {}}
                
                dist[name]["total"] += count
                dist[name]["branches"][branch_name] = dist[name]["branches"].get(branch_name, 0) + count
    return dist


def get_global_fiction_stats(branch_summaries: Optional[List[Dict]] = None) -> Dict[str, Any]:
    """Aggregate global fiction vs non-fiction distribution with branch breakdown."""
    total_fiction = 0
    total_nonfiction = 0
    branches = {}
    
    for b in (branch_summaries or []):
        if b.get("status") == "online":
            branch_name = b.get("short_name", b.get("branch_code"))
            f = b.get("fiction_stats", {}).get("fiction", 0)
            nf = b.get("fiction_stats", {}).get("nonfiction", 0)
            
            total_fiction += f
            total_nonfiction += nf
            branches[branch_name] = {"fiction": f, "nonfiction": nf}
            
    return {
        "fiction": total_fiction, 
        "nonfiction": total_nonfiction,
        "branch_breakdown": branches
    }


def get_global_top_students(branch_summaries: Optional[List[Dict]] = None) -> List[Dict[str, Any]]:
    """Aggregate top students across all branches into a global leaderboard."""
    if branch_summaries is None:
        branch_summaries = get_all_branches_summary()

    # trno -> {name, trno, issues, borrowernumber, branches}
    master_list = defaultdict(lambda: {"name": "", "trno": "", "issues": 0, "borrowernumber": None, "branches": []})

    for b in branch_summaries:
        branch_code = b.get("branch_code")
        branch_flag = b.get("flag", "")
        branch_color = b.get("color", "#888")
        branch_name = b.get("short_name", branch_code)
        
        top_students = b.get("top_students", [])
        for s in top_students:
            trno = s.get("TRNumber") or s.get("cardnumber")
            name = s.get("StudentName")
            issues = int(s.get("BooksIssued", 0))
            bid = s.get("borrowernumber")
            
            if not trno or not name:
                continue
            
            # Normalize key
            key = str(trno).strip().upper()
            
            if not master_list[key]["name"]:
                master_list[key]["name"] = name
                master_list[key]["trno"] = trno
                master_list[key]["borrowernumber"] = bid
                
            master_list[key]["issues"] += issues
            master_list[key]["branches"].append({
                "code": branch_code,
                "flag": branch_flag,
                "color": branch_color,
                "name": branch_name
            })

    # Convert and sort
    sorted_students = sorted(master_list.values(), key=lambda x: x["issues"], reverse=True)
    return sorted_students[:10]


def get_global_darajah_performance(branch_summaries: List[Dict]) -> List[Dict]:
    """Aggregate and rank Darajah (class) performance across all campuses."""
    darajah_map = defaultdict(lambda: {"name": "", "issues": 0, "students": 0, "campuses": set(), "branch_breakdown": {}})
    
    for s in branch_summaries:
        branch_code = s.get("branch_code")
        branch_name = s.get("short_name", branch_code)
        all_darajahs = s.get("all_darajahs", [])
        for d in all_darajahs:
            name = d.get("darajah")
            issues = int(d.get("issues", 0))
            students = int(d.get("total_students", 0))
            
            if not name or name == 'General':
                continue
                
            key = str(name).strip().upper()
            
            if not darajah_map[key]["name"]:
                darajah_map[key]["name"] = name
            
            darajah_map[key]["issues"] += issues
            darajah_map[key]["students"] += students
            darajah_map[key]["campuses"].add(branch_name)
            darajah_map[key]["branch_breakdown"][branch_name] = darajah_map[key]["branch_breakdown"].get(branch_name, 0) + issues
            
    # Convert and sort
    sorted_darajahs = []
    for k, v in darajah_map.items():
        if v["students"] == 0 and v["issues"] == 0:
            continue
        v["Darajah"] = v["name"]
        v["TotalIssues"] = v["issues"]
        v["campuses"] = list(v["campuses"])
        v["campus_count"] = len(v["campuses"])
        v["AvgIssuesPerStudent"] = round(v["issues"] / v["students"], 2) if v["students"] > 0 else 0
        v["Marhala"] = "Global"
        sorted_darajahs.append(v)
        
    return sorted(sorted_darajahs, key=lambda x: x["issues"], reverse=True)


def get_global_darajah_full_breakdown(branch_summaries: List[Dict]) -> List[Dict]:
    """Return a detailed list of darajahs from all branches, aggregated by name."""
    aggregated = {}
    for s in branch_summaries:
        branch_code = s.get("branch_code")
        branch_name = s.get("short_name", branch_code)
        branch_flag = s.get("flag", "")
        all_darajahs = s.get("all_darajahs", [])
        for d in all_darajahs:
            darajah_name = d.get("darajah")
            if not darajah_name or darajah_name == 'General':
                continue
            
            if darajah_name not in aggregated:
                aggregated[darajah_name] = {
                    "darajah": darajah_name,
                    "total_students": 0,
                    "issues": 0,
                    "active_borrowers": 0,
                    "branches": []
                }
                
            aggregated[darajah_name]["total_students"] += d.get("total_students", 0)
            aggregated[darajah_name]["issues"] += d.get("issues", 0)
            aggregated[darajah_name]["active_borrowers"] += d.get("active_borrowers", 0)
            
            # Add branch specific data for the breakdown modal
            aggregated[darajah_name]["branches"].append({
                "branch_code": branch_code,
                "branch_name": branch_name,
                "branch_flag": branch_flag,
                "total_students": d.get("total_students", 0),
                "issues": d.get("issues", 0),
                "active_borrowers": d.get("active_borrowers", 0),
                "rate": round(d.get("issues", 0) / d.get("total_students", 1), 2) if d.get("total_students", 0) > 0 else 0
            })
            
    for ag_d in aggregated.values():
        ag_d["rate"] = round(ag_d["issues"] / ag_d["total_students"], 2) if ag_d["total_students"] else 0
        ag_d["branch_count"] = len(ag_d["branches"])
        
    return sorted(list(aggregated.values()), key=lambda x: x["issues"], reverse=True)


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
        "today_checkouts":    sum(b.get("today_activity", {}).get("checkouts", 0) for b in online_branches),
        "today_checkins":     sum(b.get("today_activity", {}).get("checkins", 0) for b in online_branches),
        "total_fees":         sum(b.get("total_fees", 0) for b in online_branches),
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


def get_global_subject_cloud_from_summaries(branch_summaries: List[Dict]) -> List[Dict]:
    """Aggregate subject cloud metrics from summaries."""
    cloud_map = defaultdict(int)
    for s in branch_summaries:
        if s.get("status") == "online":
            for item in s.get("subject_cloud", []):
                subj = item.get("upper_subject")
                count = int(item.get("issue_count", 0))
                if subj:
                    cloud_map[subj] += count
            
    sorted_cloud = [{"Subject": k, "issue_count": v} for k, v in cloud_map.items()]
    return sorted(sorted_cloud, key=lambda x: x["issue_count"], reverse=True)[:30]


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

    for branch_code in target_codes:
        conn = get_branch_conn(branch_code)
        if isinstance(conn, _MockConnection):
            continue
        try:
            cur = conn.cursor(dictionary=True)
            if not start or not end:
                cur.close()
                continue

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
                r["branch_code"] = branch_code
                r["branch_name"] = cfg.get("short_name", branch_code)
                r["branch_flag"] = cfg.get("flag", "")
                r["branch_color"] = cfg.get("color", "#888")
            all_students.extend(rows)
        except Exception as e:
            logger.error(f"Error fetching top students ({sex}) for {branch_code}: {e}")
        finally:
            try:
                conn.close()
            except Exception:
                pass

    all_students.sort(key=lambda s: int(s.get("BooksIssued", 0)), reverse=True)
    return all_students[:limit]

# Add this function to services/branch_queries.py

def get_global_language_chart_data(branch_summaries: List[Dict]) -> List[Dict]:
    """
    Format language distribution data specifically for Chart.js doughnut chart.
    Returns a list of {label, value, color} objects with consistent ordering.
    """
    # Define color palette for languages (matches the image)
    language_colors = {
        "English": "#3b82f6",        # Blue
        "Arabic": "#10b981",          # Green
        "Lisan-ud-Dawat": "#f59e0b",  # Amber/Gold
        "Multilingual": "#8b5cf6",    # Purple
        "Urdu": "#ec4899",             # Pink
        "French": "#06b6d4",           # Cyan
        "Farsi": "#ef4444",            # Red
        "Other": "#94a3b8",            # Slate gray
    }
    
    # Get raw distribution
    raw_dist = get_global_language_distribution(branch_summaries)
    
    # Define desired order (matches your image)
    desired_order = ["English", "Arabic", "Lisan-ud-Dawat", "Multilingual", "Urdu", "French", "Farsi", "Other"]
    
    # Build result in desired order
    result = []
    for lang in desired_order:
        if lang in raw_dist:
            result.append({
                "label": lang,
                "value": raw_dist[lang]["total"],
                "color": language_colors.get(lang, "#94a3b8")
            })
    
    # Add any missing languages that weren't in the desired order
    for lang, data in raw_dist.items():
        if lang not in desired_order:
            result.append({
                "label": lang,
                "value": data["total"],
                "color": language_colors.get(lang, "#94a3b8")
            })
    
    return result

def get_global_top_books_by_lang(branch_summaries: List[Dict], lang_code: str, limit: int = 10) -> List[Dict]:
    """Aggregate top books across all branches for a specific language."""
    all_books = []
    for b in branch_summaries:
        if lang_code == 'ara':
            books = b.get("top_books_arabic", [])
        elif lang_code == 'eng':
            books = b.get("top_books_english", [])
        else:
            continue
            
        for book in books:
            # Add branch metadata
            book["branch_flag"] = b.get("flag", "")
            book["branch_name"] = b.get("short_name", b.get("branch_code"))
            all_books.append(book)
            
    # Sort and limit global top titles
    all_books.sort(key=lambda x: x.get("issues", 0), reverse=True)
    return all_books[:limit]