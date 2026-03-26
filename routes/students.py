# routes/students.py - FULLY CORRECTED VERSION

from flask import (
    Blueprint,
    render_template,
    session,
    redirect,
    url_for,
    send_file,
    current_app,
    request,
    jsonify,
    Response
)
from db_koha import get_koha_conn
from db_app import get_conn as get_app_conn
from datetime import date, datetime, timedelta
from io import BytesIO, StringIO
import pandas as pd
import os
import re
import csv
from urllib.parse import quote
from config import Config

# Import comprehensive export functions
from services.exports import (
    _ensure_font_registered,
    _shape_if_rtl,
    create_student_landscape_report,
    create_darajah_landscape_report,
    create_monthly_landscape_report,
    export_to_pdf_landscape,
    export_to_pdf_portrait,
    dataframe_to_excel_bytes
)

# darajah / class config (max / mustawā) from central Koha queries
from services import koha_queries as KQ
from services.koha_queries import darajah_max_books as _darajah_max_books_from_db

bp = Blueprint("students", __name__, url_prefix="/students")

# Accept any of these borrower attribute codes as "Darajah"
DARAJAH_CODES = ("STD", "CLASS", "DAR", "CLASS_STD")

# Accept any of these borrower attribute codes for TR number lookups
TR_ATTR_CODES = ("TRNO", "TRN", "TR_NUMBER", "TR")

# Cache for darajah_max_books() ranges
_DARAJAH_MAX_CACHE = None


# ---------------- TAQEEM HELPER FUNCTIONS ----------------

def _calculate_simple_taqeem(student: dict) -> dict:
    """Simple fallback Taqeem calculation when marks_service is not available."""
    try:
        ay_label = Config.CURRENT_ACADEMIC_YEAR()
        ay_only = ay_label.replace('H', '').strip()
        
        return {
            "academic_year": ay_only,
            "total": 0,
            "book_issue": {
                "total": 0,
                "physical": {"issues": 0, "marks": 0.0},
                "digital": {"issues": 0, "marks": 0.0}
            },
            "book_review": {"marks": 0, "submitted": 0},
            "program_attendance": 0,
            "last_updated": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "student_trno": student.get("TRNumber", ""),
            "status": "not_available",
            "message": "Taqeem data is being calculated. Please check back later."
        }
    except Exception as e:
        current_app.logger.error(f"Error in simple Taqeem calculation: {e}")
        return {
            "academic_year": Config.CURRENT_ACADEMIC_YEAR().replace('H', '').strip(),
            "total": 0,
            "book_issue": {
                "total": 0,
                "physical": {"issues": 0, "marks": 0.0},
                "digital": {"issues": 0, "marks": 0.0}
            },
            "book_review": {"marks": 0, "submitted": 0},
            "program_attendance": 0,
            "status": "error",
            "message": "Unable to calculate Taqeem marks."
        }


def _get_student_taqeem_from_db(student_identifier: str, academic_year: str) -> dict:
    """Get Taqeem marks from database for a student."""
    conn = None
    cur = None
    try:
        conn = get_app_conn()  # This will now create a fresh connection
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                total_marks, book_issue_total, book_review_marks, 
                program_attendance_marks, last_updated, student_trno,
                physical_books_issued, digital_books_issued,
                physical_books_marks, digital_books_marks
            FROM student_taqeem 
            WHERE (student_username = ? OR student_trno = ? OR student_username = 'TR' || ?) 
            AND academic_year = ?
        """, (student_identifier, student_identifier, student_identifier, academic_year))
        
        marks_row = cur.fetchone()
        
        # Also fetch actual review count if available
        review_count = 0
        cur.execute("""
            SELECT review_count FROM book_review_marks 
            WHERE (student_username = ? OR student_trno = ?) 
            AND academic_year = ?
        """, (student_identifier, student_identifier, academic_year))
        rev_row = cur.fetchone()
        if rev_row:
            review_count = rev_row[0] or 0
        
        if marks_row:
            return {
                "academic_year": academic_year,
                "total": marks_row[0] or 0,
                "book_issue": {
                    "total": marks_row[1] or 0,
                    "physical": {
                        "issues": marks_row[6] or 0,
                        "marks": marks_row[8] or 0
                    },
                    "digital": {
                        "issues": marks_row[7] or 0,
                        "marks": marks_row[9] or 0
                    }
                },
                "book_review": {
                    "marks": marks_row[2] or 0,
                    "submitted": review_count if review_count > 0 else (int((marks_row[2] or 0) / 10) if marks_row[2] else 0)
                },
                "program_attendance": marks_row[3] or 0,
                "last_updated": marks_row[4],
                "student_trno": marks_row[5]
            }
        return None
        
    except Exception as e:
        current_app.logger.error(f"Error getting student taqeem from DB: {e}")
        return None
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# ---------------- OPAC URL HELPER ----------------
def get_opac_base_url():
    return current_app.config.get("KOHA_OPAC_BASE_URL", "https://library-nairobi.jameasaifiyah.org")

def get_opac_book_url(biblionumber: int) -> str:
    opac_base = get_opac_base_url()
    return f"{opac_base.rstrip('/')}/cgi-bin/koha/opac-detail.pl?biblionumber={biblionumber}"

def get_opac_author_url(author_name: str) -> str:
    opac_base = get_opac_base_url()
    encoded_author = quote(author_name)
    return f"{opac_base.rstrip('/')}/cgi-bin/koha/opac-search.pl?q=au:{encoded_author}"


# ---------------- LOGIN DECORATOR ----------------
def require_login(f):
    from functools import wraps
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("auth_bp.login"))
        return f(*args, **kwargs)
    return wrapper


# ---------------- Teacher Mapping Helper ----------------
def _get_teachers_for_darajah(darajah_name: str) -> list[dict]:
    """Get teachers mapped to a specific darajah."""
    if not darajah_name:
        return []
    
    conn = None
    cur = None
    try:
        conn = get_app_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT tm.teacher_name, tm.role, u.email
            FROM teacher_darajah_mapping tm
            LEFT JOIN users u ON tm.teacher_username = u.username
            WHERE tm.darajah_name = ?
            ORDER BY 
                CASE tm.role 
                    WHEN 'masool' THEN 1 
                    WHEN 'class_teacher' THEN 2 
                    ELSE 3 
                END,
                tm.teacher_name
        """, (darajah_name,))
        
        teachers = []
        for row in cur.fetchall():
            role_display = 'Masool' if row[1] == 'masool' else \
                          'Darajah Teacher' if row[1] == 'class_teacher' else 'Assistant'
            teachers.append({
                'name': row[0],
                'role': role_display,
                'email': row[2]
            })
        
        return teachers
    except Exception as e:
        current_app.logger.error(f"Error fetching teachers for darajah {darajah_name}: {e}")
        return []
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# ---------------- Hijri helpers ----------------
def _hijri_date_label(d: date) -> str:
    return KQ.get_hijri_date_label(d)

def _to_hijri_str(val) -> str:
    if not val:
        return "-"
    if isinstance(val, datetime):
        d = val.date()
    elif isinstance(val, date):
        d = val
    elif isinstance(val, str):
        try:
            d = datetime.strptime(val.split(" ")[0], "%Y-%m-%d").date()
        except Exception:
            return val
    else:
        return str(val)
    return _hijri_date_label(d)

def _hijri_month_label_and_key(d: date):
    if not d:
        return "-", (0, 0)
    if isinstance(d, datetime):
        d = d.date()
    
    label = KQ.get_hijri_month_year_label(d)
    
    try:
        from hijri_converter import convert
        h = convert.Gregorian(d.year, d.month, d.day).to_hijri()
        return label, (h.year, h.month)
    except Exception:
        return label, (d.year, d.month)

def _ay_period_label():
    start, end = KQ.get_ay_bounds()
    return f"{_hijri_date_label(start)} to {_hijri_date_label(end)}"


# ---------------- Darajah → Max / Mustawā mapping ----------------
def _load_darajah_max_cache():
    global _DARAJAH_MAX_CACHE
    if _DARAJAH_MAX_CACHE is not None:
        return _DARAJAH_MAX_CACHE

    ranges = []
    try:
        rows = _darajah_max_books_from_db()
        for label, max_books in rows:
            digits = re.findall(r"(\d+)", str(label))
            if not digits:
                continue
            if len(digits) == 1:
                lo = hi = int(digits[0])
            else:
                lo, hi = int(digits[0]), int(digits[1])
            ranges.append((lo, hi, int(max_books)))
    except Exception:
        ranges = []

    _DARAJAH_MAX_CACHE = ranges
    return _DARAJAH_MAX_CACHE

def _max_books_for_darajah(darajah_label: str):
    if not darajah_label:
        return None

    m = re.search(r"(\d+)", str(darajah_label))
    if not m:
        return None
    d_num = int(m.group(1))

    ranges = _load_darajah_max_cache()
    for lo, hi, max_books in ranges:
        if lo <= d_num <= hi:
            return max_books

    if 1 <= d_num <= 4:
        return 4
    if 5 <= d_num <= 7:
        return 5
    if 8 <= d_num <= 11:
        return 6
    return None


# ---------------- HTML CLEANING UTILITY ----------------
def clean_html_for_pdf(html_text: str) -> str:
    if not html_text or not isinstance(html_text, str):
        return str(html_text) if html_text is not None else ""
    html_text = re.sub(r'<[^>]+>', '', html_text)
    html_text = re.sub(r'\s+', ' ', html_text).strip()
    return html_text


# ---------------- BOOK REVIEW HELPER ----------------
def _get_book_reviews_for_month(student_username: str, month_label: str):
    """Get book review marks for a student for a specific month."""
    conn = None
    cur = None
    try:
        conn = get_app_conn()
        cur = conn.cursor()
        
        month_year_match = re.search(r'(\d{4})', month_label)
        academic_year = month_year_match.group(1) if month_year_match else Config.CURRENT_ACADEMIC_YEAR().replace('H', '').strip()
        
        cur.execute("""
            SELECT 
                brm.student_username,
                brm.student_name,
                brm.darajah_name,
                brm.academic_year,
                brm.marks,
                brm.remarks,
                brm.source,
                brm.uploaded_by,
                brm.uploaded_at
            FROM book_review_marks brm
            WHERE brm.student_username = ? AND brm.academic_year = ?
            ORDER BY brm.uploaded_at DESC
        """, (student_username, academic_year))
        
        reviews = []
        columns = [desc[0] for desc in cur.description]
        for row in cur.fetchall():
            review_dict = dict(zip(columns, row))
            if review_dict.get('uploaded_at'):
                review_dict['uploaded_at'] = review_dict['uploaded_at'].strftime('%Y-%m-%d %H:%M')
            reviews.append(review_dict)
        
        return reviews
    except Exception as e:
        current_app.logger.error(f"Error fetching book reviews for {student_username}: {e}")
        return []
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# ---------------- DATA ACCESS - FIXED WITH DICTIONARY CURSOR ----------------
def get_student_info(identifier):
    """Fetch student details, borrowed books (AY), engagement metrics, fees, photo, and teacher mapping."""
    identifier = (identifier or "").strip()

    conn = get_koha_conn()
    cur = conn.cursor(dictionary=True)

    try:
        # --- Student core info ---
        darajah_codes_ph = ",".join(["%s"] * len(DARAJAH_CODES))
        tr_codes_ph = ",".join(["%s"] * len(TR_ATTR_CODES))

        sql = f"""
            SELECT
                b.borrowernumber,
                b.cardnumber,
                CONCAT_WS(' ', b.firstname, b.surname) AS FullName,
                b.userid AS `ITS ID`,
                b.email AS EduEmail,
                b.categorycode,
                COALESCE(c.description, b.categorycode) AS Marhala,
                COALESCE(std.attribute, b.branchcode) AS Darajah,
                tr.attribute AS TRNumber
            FROM borrowers b
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            LEFT JOIN borrower_attributes std
                   ON std.borrowernumber = b.borrowernumber
                  AND std.code IN ({darajah_codes_ph})
            LEFT JOIN borrower_attributes tr
                   ON tr.borrowernumber = b.borrowernumber
                  AND tr.code IN ({tr_codes_ph})
            WHERE
                  LOWER(b.cardnumber) = LOWER(%s)
               OR LOWER(b.userid)     = LOWER(%s)
               OR LOWER(COALESCE(tr.attribute,'')) = LOWER(%s)
               OR CAST(b.borrowernumber AS CHAR) = %s
            LIMIT 1
        """
        params = (*DARAJAH_CODES, *TR_ATTR_CODES, identifier, identifier, identifier, identifier)
        cur.execute(sql, params)
        student = cur.fetchone()
        
        if not student:
            return None

        borrowernumber = student["borrowernumber"]
        darajah_name = student.get("Darajah", "")
        student_username = student.get("ITS ID") or student.get("cardnumber")

        # AY bounds
        start_ay, end_ay = KQ.get_ay_bounds()

        # ===== Library engagement metrics =====
        # Currently issued books
        cur.execute(
            """
            SELECT
                bi.biblionumber,
                bi.title,
                it.ccode AS collection,
                ExtractValue(
                    bmd.metadata,
                    '//datafield[@tag="041"]/subfield[@code="a"]'
                ) AS language,
                iss.issuedate AS date_issued,
                iss.date_due,
                iss.returndate,
                (iss.returndate IS NOT NULL) AS returned
            FROM issues iss
            JOIN items it USING (itemnumber)
            JOIN biblio bi USING (biblionumber)
            LEFT JOIN biblio_metadata bmd USING (biblionumber)
            WHERE iss.borrowernumber = %s
            ORDER BY iss.issuedate DESC
            LIMIT 200
            """,
            (borrowernumber,),
        )
        borrowed_books = cur.fetchall()

        # Old issues history
        try:
            cur.execute(
                """
                SELECT
                    bi.biblionumber,
                    bi.title,
                    it.ccode AS collection,
                    ExtractValue(
                        bmd.metadata,
                        '//datafield[@tag="041"]/subfield[@code="a"]'
                    ) AS language,
                    oi.issuedate AS date_issued,
                    oi.returndate,
                    oi.returndate AS date_due,
                    1 AS returned
                FROM old_issues oi
                JOIN items it USING (itemnumber)
                JOIN biblio bi USING (biblionumber)
                LEFT JOIN biblio_metadata bmd USING (biblionumber)
                WHERE oi.borrowernumber = %s
                ORDER BY oi.issuedate DESC
                LIMIT 300
                """,
                (borrowernumber,),
            )
            borrowed_books += cur.fetchall()
        except Exception:
            pass

        # Compute overdue flags & counts
        today = date.today()
        overdue_count = 0
        active_count = 0
        for b in borrowed_books:
            is_returned = bool(b.get("returned"))
            if not is_returned:
                active_count += 1

            if not is_returned and b.get("date_due"):
                due_val = b["date_due"]
                if isinstance(due_val, datetime):
                    due_date = due_val.date()
                elif isinstance(due_val, date):
                    due_date = due_val
                else:
                    try:
                        due_date = datetime.strptime(str(due_val).split(" ")[0], "%Y-%m-%d").date()
                    except Exception:
                        due_date = None
                b["overdue"] = bool(due_date and today > due_date)
            else:
                b["overdue"] = False

            if b["overdue"]:
                overdue_count += 1

            b["_issued_hijri"] = _to_hijri_str(b.get("date_issued"))
            b["_due_hijri"] = _to_hijri_str(b.get("date_due"))
            
            biblionumber = b.get("biblionumber")
            if biblionumber:
                b["opac_url"] = get_opac_book_url(biblionumber)
            else:
                b["opac_url"] = "#"

        # AY issues (statistics)
        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM statistics
            WHERE borrowernumber=%s AND type='issue'
              AND DATE(`datetime`) BETWEEN %s AND %s
            """,
            (borrowernumber, start_ay, end_ay),
        )
        row = cur.fetchone()
        ay_issues = int(row["cnt"]) if row and row.get("cnt") is not None else 0

        # Last issue date
        cur.execute(
            """
            SELECT
              MAX(CASE WHEN s.type='issue' THEN DATE(s.`datetime`) END) AS last_issue
            FROM statistics s
            WHERE s.borrowernumber=%s
            """,
            (borrowernumber,),
        )
        row = cur.fetchone() or {}
        last_issue_date = _to_hijri_str(row.get("last_issue"))

        # Last return date
        cur.execute(
            """
            SELECT MAX(returndate) AS last_return
            FROM (
              SELECT returndate FROM issues WHERE borrowernumber=%s
              UNION ALL
              SELECT returndate FROM old_issues WHERE borrowernumber=%s
            ) t
            """,
            (borrowernumber, borrowernumber),
        )
        row = cur.fetchone() or {}
        last_return_date = _to_hijri_str(row.get("last_return"))

        # Reservations count
        reservations = 0
        try:
            cur.execute("SELECT COUNT(*) AS cnt FROM reserves WHERE borrowernumber=%s", (borrowernumber,))
            rr = cur.fetchone()
            reservations = int(rr["cnt"]) if rr and rr.get("cnt") is not None else 0
        except Exception:
            reservations = 0

        # Outstanding balance
        cur.execute(
            """
            SELECT COALESCE(SUM(amountoutstanding),0) AS outstanding
            FROM accountlines
            WHERE borrowernumber=%s
            """,
            (borrowernumber,),
        )
        row = cur.fetchone() or {}
        outstanding_balance = float(row.get("outstanding") or 0)

        # Fees paid totals
        cur.execute(
            """
            SELECT
              COALESCE(SUM(CASE WHEN credit_type_code='PAYMENT' AND (status IS NULL OR status<>'VOID') THEN -amount END),0) AS TotalFeesPaid,
              MAX(CASE WHEN credit_type_code='PAYMENT' AND (status IS NULL OR status<>'VOID') THEN date END) AS LastPaymentDate
            FROM accountlines
            WHERE borrowernumber=%s
            """,
            (borrowernumber,),
        )
        fees_row = cur.fetchone() or {}
        total_fees_paid = float(fees_row.get("TotalFeesPaid") or 0)
        last_payment_date = _to_hijri_str(fees_row.get("LastPaymentDate"))

        # Fees paid in AY
        cur.execute(
            """
            SELECT COALESCE(SUM(-amount),0) AS paid
            FROM accountlines
            WHERE borrowernumber=%s
              AND credit_type_code='PAYMENT'
              AND (status IS NULL OR status<>'VOID')
              AND DATE(`date`) BETWEEN %s AND %s
            """,
            (borrowernumber, start_ay, end_ay),
        )
        ay_row = cur.fetchone() or {}
        fees_paid_ay = float(ay_row.get("paid") or 0)

        # Top authors
        fav_authors = []
        try:
            cur.execute(
                """
                SELECT bi.author AS author, COUNT(*) AS cnt
                FROM (
                  SELECT itemnumber, issuedate FROM issues WHERE borrowernumber=%s
                  UNION ALL
                  SELECT itemnumber, issuedate FROM old_issues WHERE borrowernumber=%s
                ) u
                JOIN items it USING (itemnumber)
                JOIN biblio bi USING (biblionumber)
                WHERE u.issuedate >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
                GROUP BY bi.author
                ORDER BY cnt DESC
                LIMIT 50
                """,
                (borrowernumber, borrowernumber),
            )
            fav_rows = cur.fetchall() or []

            for r in fav_rows:
                a = (r.get("author") or "").strip()
                if not a:
                    continue
                if a.lower() in ("unknown", "unk", "n/a", "na", "-", "none"):
                    continue
                
                opac_url = get_opac_author_url(a)
                fav_authors.append({
                    "name": a,
                    "count": int(r['cnt']),
                    "opac_url": opac_url,
                    "display": f"{a} ({int(r['cnt'])})"
                })

            fav_authors = fav_authors[:10]
        except Exception:
            pass

        # Group by Hijri month-year
        months_map = {}
        for b in borrowed_books:
            di = b.get("date_issued")
            if isinstance(di, datetime):
                d = di.date()
            elif isinstance(di, date):
                d = di
            elif di:
                try:
                    d = datetime.strptime(str(di).split(" ")[0], "%Y-%m-%d").date()
                except Exception:
                    d = None

            if not d or not (start_ay <= d <= end_ay):
                continue

            label, key = _hijri_month_label_and_key(d)
            entry = months_map.get(key)
            if not entry:
                entry = {"label": label, "books": []}
                months_map[key] = entry
            entry["books"].append(b)

        grouped_items = []
        for key, entry in months_map.items():
            grouped_items.append((key, entry["label"], entry["books"]))
        grouped_items.sort(key=lambda x: x[0], reverse=True)
        grouped_sorted = [(label, books) for (_, label, books) in grouped_items]

        # Fees list
        cur.execute(
            """
            SELECT date, amount, description, note
            FROM accountlines
            WHERE borrowernumber = %s
              AND DATE(`date`) BETWEEN %s AND %s
            ORDER BY date DESC
            LIMIT 50
            """,
            (borrowernumber, start_ay, end_ay),
        )
        fees_list = cur.fetchall() or []
        for f in fees_list:
            f["date"] = _to_hijri_str(f.get("date"))

        # Per-month stats
        darajah_label = student.get("Darajah")
        max_books_allowed = _max_books_for_darajah(darajah_label)
        month_stats = {}
        for month_label, books in grouped_sorted:
            count = len(books)

            if max_books_allowed:
                status = "above" if count >= max_books_allowed else "below"
            else:
                status = "na"

            target = max_books_allowed

            if target:
                if count >= target:
                    reco_status = "meets"
                    reco_text = (f"Excellent – you met or exceeded the recommended reading level "
                                f"of {target} books this month.")
                elif count == 0:
                    reco_status = "none"
                    reco_text = (f"No books issued this month. Please plan to borrow and read at least "
                                f"{target} books next month.")
                else:
                    reco_status = "below"
                    reco_text = (f"Below the recommended level of {target} books. Try to borrow and read "
                                "a bit more next month.")
            else:
                reco_status = "na"
                reco_text = f"This month you issued {count} book(s)."

            month_stats[month_label] = {
                "count": count,
                "max_books": max_books_allowed,
                "status": status,
                "target": target,
                "reco_status": reco_status,
                "reco_text": reco_text,
                "review_count": 0,
                "review_marks": 0
            }

    finally:
        cur.close()
        conn.close()

    # ===== NOW GET DATA FROM APP DATABASE (separate connections) =====
    
    # Get campus branch from mapping
    campus_branch = "Global"
    app_conn = None
    app_cur = None
    try:
        app_conn = get_app_conn()
        app_cur = app_conn.cursor()
        app_cur.execute("""
            SELECT campus_branch FROM student_darajah_mapping 
            WHERE student_username = ? 
            ORDER BY enrollment_date DESC LIMIT 1
        """, (student_username,))
        row = app_cur.fetchone()
        if row:
            campus_branch = row[0]
    except Exception as e:
        current_app.logger.error(f"Error fetching campus_branch for student {student_username}: {e}")
    finally:
        if app_cur:
            app_cur.close()
        if app_conn:
            app_conn.close()
    student["campus_branch"] = campus_branch

    # Get teachers for this darajah
    teachers = []
    if darajah_name:
        teachers = _get_teachers_for_darajah(darajah_name)

    # Get book reviews for each month
    book_reviews_by_month = {}
    if student_username:
        for month_label, _ in grouped_sorted:
            reviews = _get_book_reviews_for_month(student_username, month_label)
            if reviews:
                book_reviews_by_month[month_label] = reviews
                # Update month stats with review data
                if month_label in month_stats:
                    month_stats[month_label]["review_count"] = len(reviews)
                    month_stats[month_label]["review_marks"] = sum(float(r.get('marks', 0)) for r in reviews)

    # Photo lookup
    photos_dir = os.path.join(current_app.root_path, "static", "photos")
    avatar_fallback = "images/avatar.png"

    photo_filename = None
    if student.get("cardnumber"):
        for ext in [".jpg", ".jpeg", ".png"]:
            candidate = os.path.join(photos_dir, f"{student['cardnumber']}{ext}")
            if os.path.exists(candidate):
                photo_filename = f"photos/{student['cardnumber']}{ext}"
                break
    if not photo_filename:
        photo_filename = avatar_fallback

    # Taqeem data
    student_identifier = student.get("ITS ID") or student.get("cardnumber") or student.get("TRNumber")
    student["Taqeem"] = None

    if student_identifier:
        try:
            current_ay = Config.CURRENT_ACADEMIC_YEAR().replace('H', '')
            
            try:
                from services.marks_service import update_student_taqeem
                update_student_taqeem(student_identifier, current_ay)
            except Exception as update_e:
                current_app.logger.error(f"Error during automatic Taqeem update: {update_e}")

            taqeem_from_db = _get_student_taqeem_from_db(student_identifier, current_ay)
            
            if taqeem_from_db:
                student["Taqeem"] = taqeem_from_db
                from services.marks_service import get_student_program_participation
                student["ProgramParticipation"] = get_student_program_participation(student_identifier, current_ay)
            else:
                student["Taqeem"] = _calculate_simple_taqeem(student)
                    
        except Exception as e:
            current_app.logger.error(f"Error loading Taqeem for {student_identifier}: {e}")
            student["Taqeem"] = _calculate_simple_taqeem(student)
    else:
        student["Taqeem"] = _calculate_simple_taqeem(student)

    # Build info dict
    student["Photo"] = photo_filename
    student["BorrowedBooks"] = borrowed_books
    student["BorrowedBooksGrouped"] = grouped_sorted
    student["MonthStats"] = month_stats
    student["Teachers"] = teachers
    student["PrimaryTeacher"] = teachers[0] if teachers else None
    student["FavoriteAuthors"] = fav_authors
    student["BookReviewsByMonth"] = book_reviews_by_month
    student["ProgramParticipation"] = student.get("ProgramParticipation", [])

    # Metrics bundle
    ay_period_label = _ay_period_label()
    student["Metrics"] = {
        "CurrentlyIssued": active_count,
        "OverdueNow": overdue_count,
        "AYIssues": ay_issues,
        "Reservations": reservations,
        "OutstandingBalance": outstanding_balance,
        "TotalFeesPaid": total_fees_paid,
        "FeesPaidAY": fees_paid_ay,
        "LastPaymentDate": last_payment_date,
        "LastIssueDate": last_issue_date,
        "LastReturnDate": last_return_date,
        "FavoriteAuthors": [fa["display"] for fa in fav_authors],
        "MaxBooksAllowed": max_books_allowed,
        "AYPeriodLabel": ay_period_label,
        "Darajah": darajah_name,
        "Marhala": student.get("Marhala"),
        "TeacherCount": len(teachers)
    }

    student["FeesList"] = fees_list

    return student


# ============================================================================
# ROUTES
# ============================================================================

@bp.route("/<identifier>")
@require_login
def student(identifier):
    info = get_student_info(identifier)
    if not info:
        return render_template(
            "student.html",
            found=False,
            message=f"No data found for identifier {identifier}.",
            hide_nav=True,
        )

    if not info.get("Taqeem"):
        info["Taqeem"] = _calculate_simple_taqeem(info)

    opac_base = get_opac_base_url()
    
    from services.recommendation_service import RecommendationService
    metrics = info.get("Metrics", {})
    marhala = metrics.get("Marhala") or "Darajah 1-4"
    
    recs = {
        "trending": RecommendationService.get_marhala_recommendations(marhala, limit=3),
        "personalized": RecommendationService.get_personalized_recommendations(identifier, limit=3)
    }

    return render_template(
        "student.html",
        found=True,
        info=info,
        hide_nav=True,
        opac_base_url=opac_base,
        recommendations=recs
    )


@bp.route("/lookup", methods=["GET", "POST"])
@require_login
def lookup_student():
    q = (request.values.get("q") or "").strip()
    if not q:
        return redirect(url_for("reports_bp.reports_page"))
    return redirect(url_for("students.student", identifier=q))


@bp.route("/<identifier>/json")
@require_login
def student_json(identifier):
    info = get_student_info(identifier)
    if not info:
        return jsonify({"success": False, "message": "Student not found"}), 404
    
    def clean_data(data):
        if isinstance(data, datetime):
            return data.isoformat()
        elif isinstance(data, date):
            return data.isoformat()
        elif isinstance(data, dict):
            return {k: clean_data(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [clean_data(item) for item in data]
        else:
            return data
    
    cleaned_info = clean_data(info)
    return jsonify({"success": True, "data": cleaned_info})


# ============================================================================
# PDF EXPORT ROUTES
# ============================================================================

@bp.route("/<identifier>/pdf")
@bp.route("/<identifier>/pdf/<orientation>")
@require_login
def export_student_pdf(identifier, orientation='portrait'):
    info = get_student_info(identifier)
    if not info:
        return "Student not found", 404

    summary_data = {
        'Full Name': info.get('FullName', ''),
        'ITS ID': info.get('ITS ID', ''),
        'TR Number': info.get('TRNumber', ''),
        'Darajah': info.get('Darajah', ''),
        'Marhala': info.get('Marhala', ''),
        'Email': info.get('EduEmail', '')
    }

    all_books = []
    for month, books in info.get("BorrowedBooksGrouped", []):
        for book in books:
            all_books.append({
                'Month': month,
                'Title': clean_html_for_pdf(book.get('title', '')),
                'Collection': book.get('collection', ''),
                'Language': book.get('language', ''),
                'Issued': book.get('_issued_hijri', ''),
                'Due': book.get('_due_hijri', ''),
                'Status': 'Overdue' if book.get('overdue') else 'Returned' if book.get('returned') else 'Active'
            })

    if all_books:
        df = pd.DataFrame(all_books)
    else:
        df = pd.DataFrame({'Message': ['No borrowed books in Academic Year']})

    title = f"Student Profile: {info.get('FullName', 'Student')}"
    subtitle = f"Academic Year: {info.get('Metrics', {}).get('AYPeriodLabel', '')}"
    
    try:
        if orientation.lower() == 'landscape':
            pdf_bytes = export_to_pdf_landscape(df=df, title=title, subtitle=subtitle, summary_stats=summary_data)
        else:
            pdf_bytes = export_to_pdf_portrait(df=df, title=title, subtitle=subtitle, summary_stats=summary_data)
        
        buffer = BytesIO(pdf_bytes)
        student_name_clean = re.sub(r'[^\w\s-]', '', info.get('FullName', '')).replace(' ', '_')
        filename = f"student_profile_{student_name_clean}_{orientation}.pdf"
        
        return send_file(buffer, as_attachment=True, download_name=filename, mimetype="application/pdf")
    
    except Exception as e:
        current_app.logger.error(f"Error generating PDF: {e}")
        return f"Error generating PDF: {str(e)}", 500


@bp.route("/<identifier>/report/landscape")
@require_login
def export_student_landscape_report(identifier):
    info = get_student_info(identifier)
    if not info:
        return "Student not found", 404

    try:
        pdf_bytes = create_student_landscape_report(
            student_info=info,
            borrowed_books=info.get("BorrowedBooks", []),
            monthly_stats=info.get("MonthStats", {}),
            include_taqeem=True
        )
        
        buffer = BytesIO(pdf_bytes)
        student_name_clean = re.sub(r'[^\w\s-]', '', info.get('FullName', '')).replace(' ', '_')
        filename = f"student_reading_report_{student_name_clean}_landscape.pdf"
        
        return send_file(buffer, as_attachment=True, download_name=filename, mimetype="application/pdf")
    
    except Exception as e:
        current_app.logger.error(f"Error generating landscape report: {e}")
        return f"Error generating report: {str(e)}", 500


@bp.route("/<identifier>/export/monthly-landscape/<month_label>")
@require_login
def export_monthly_landscape_report(identifier, month_label):
    info = get_student_info(identifier)
    if not info:
        return "Student not found", 404

    month_books = []
    for month, books in info.get("BorrowedBooksGrouped", []):
        if month == month_label:
            month_books = books
            break

    if not month_books:
        return "No books found for this month", 404

    monthly_data = []
    for book in month_books:
        monthly_data.append({
            'Title': clean_html_for_pdf(book.get('title', '')),
            'Collection': book.get('collection', ''),
            'Language': book.get('language', ''),
            'Issued': book.get('_issued_hijri', ''),
            'Due': book.get('_due_hijri', ''),
            'Status': 'Overdue' if book.get('overdue') else 'Returned' if book.get('returned') else 'Active'
        })

    month_stats = info.get("MonthStats", {}).get(month_label, {})
    summary_stats = {
        'Books Issued': month_stats.get('count', 0),
        'Target': month_stats.get('target', 0),
        'Reviews': month_stats.get('review_count', 0),
        'Review Marks': month_stats.get('review_marks', 0)
    }

    try:
        df = pd.DataFrame(monthly_data)
        
        pdf_bytes = create_monthly_landscape_report(
            month_label=month_label,
            data_df=df,
            summary_stats=summary_stats,
            report_type='reading'
        )
        
        buffer = BytesIO(pdf_bytes)
        student_name_clean = re.sub(r'[^\w\s-]', '', info.get('FullName', '')).replace(' ', '_')
        month_clean = re.sub(r'[^\w\s-]', '', month_label).replace(' ', '_')
        filename = f"monthly_report_{student_name_clean}_{month_clean}_landscape.pdf"
        
        return send_file(buffer, as_attachment=True, download_name=filename, mimetype="application/pdf")
    
    except Exception as e:
        current_app.logger.error(f"Error generating monthly landscape report: {e}")
        return f"Error generating report: {str(e)}", 500


@bp.route("/<identifier>/export/month/<month_label>/excel")
@require_login
def export_month_excel(identifier, month_label):
    info = get_student_info(identifier)
    if not info:
        return "Student not found", 404
    
    month_books = []
    for month, books in info.get("BorrowedBooksGrouped", []):
        if month == month_label:
            month_books = books
            break
    
    if not month_books:
        return "No books found for this month", 404
    
    monthly_data = []
    for book in month_books:
        monthly_data.append({
            'Title': clean_html_for_pdf(book.get('title', '')),
            'Collection': book.get('collection', ''),
            'Language': book.get('language', ''),
            'Issued': book.get('_issued_hijri', ''),
            'Due': book.get('_due_hijri', ''),
            'Status': 'Overdue' if book.get('overdue') else 'Returned' if book.get('returned') else 'Active',
            'OPAC_URL': book.get('opac_url', '')
        })
    
    df = pd.DataFrame(monthly_data)
    month_stats = info.get("MonthStats", {}).get(month_label, {})
    
    summary_data = {
        'Student Name': [info.get('FullName', '')],
        'TR Number': [info.get('TRNumber', '')],
        'Month': [month_label],
        'Total Books': [month_stats.get('count', 0)],
        'Target': [month_stats.get('target', 0)],
        'Status': [month_stats.get('reco_status', 'N/A')],
        'Book Reviews': [month_stats.get('review_count', 0)],
        'Review Marks': [month_stats.get('review_marks', 0)]
    }
    summary_df = pd.DataFrame(summary_data)
    
    try:
        additional_sheets = {'Summary': summary_df}
        excel_bytes = dataframe_to_excel_bytes(df, sheet_name='Borrowed Books', additional_sheets=additional_sheets)
        
        buffer = BytesIO(excel_bytes)
        student_name_clean = re.sub(r'[^\w\s-]', '', info.get('FullName', '')).replace(' ', '_')
        month_clean = re.sub(r'[^\w\s-]', '', month_label).replace(' ', '_')
        filename = f"monthly_report_{student_name_clean}_{month_clean}.xlsx"
        
        return send_file(buffer, as_attachment=True, download_name=filename, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    
    except Exception as e:
        current_app.logger.error(f"Error generating Excel: {e}")
        return f"Error generating Excel: {str(e)}", 500


@bp.route("/<identifier>/export/csv")
@require_login
def export_student_csv(identifier):
    info = get_student_info(identifier)
    if not info:
        return "Student not found", 404
    
    output = StringIO()
    writer = csv.writer(output)
    
    writer.writerow([
        'Student Name', 'ITS ID', 'TR Number', 'Darajah', 'Marhala',
        'Book Title', 'Collection', 'Language', 'Date Issued (Hijri)', 
        'Due Date (Hijri)', 'Returned', 'Overdue', 'Status'
    ])
    
    for b in info.get("BorrowedBooks", []):
        writer.writerow([
            info.get('FullName', ''),
            info.get('ITS ID', ''),
            info.get('TRNumber', ''),
            info.get('Darajah', ''),
            info.get('Marhala', ''),
            b.get('title', ''),
            b.get('collection', ''),
            b.get('language', ''),
            b.get('_issued_hijri', ''),
            b.get('_due_hijri', ''),
            'Yes' if b.get('returned') else 'No',
            'Yes' if b.get('overdue') else 'No',
            'Returned' if b.get('returned') else 'Overdue' if b.get('overdue') else 'Active'
        ])
    
    output.seek(0)
    student_name_clean = re.sub(r'[^\w\s-]', '', info.get('FullName', '')).replace(' ', '_')
    filename = f"borrowed_books_{student_name_clean}.csv"
    
    return Response(output, mimetype="text/csv", headers={"Content-Disposition": f"attachment;filename={filename}"})


@bp.route("/<identifier>/export/monthly-csv/<month_label>")
@require_login
def export_monthly_csv(identifier, month_label):
    info = get_student_info(identifier)
    if not info:
        return "Student not found", 404
    
    month_books = []
    for month, books in info.get("BorrowedBooksGrouped", []):
        if month == month_label:
            month_books = books
            break
    
    if not month_books:
        return "No books found for this month", 404
    
    output = StringIO()
    writer = csv.writer(output)
    month_stats = info.get("MonthStats", {}).get(month_label, {})
    
    writer.writerow([
        'Student Name', 'ITS ID', 'TR Number', 'Darajah', 'Marhala', 
        'Month', 'Monthly Target', 'Monthly Issued', 'Monthly Reviews', 'Review Marks',
        'Book Title', 'Collection', 'Language', 'Date Issued (Hijri)', 
        'Due Date (Hijri)', 'Returned', 'Overdue', 'Status'
    ])
    
    for b in month_books:
        writer.writerow([
            info.get('FullName', ''),
            info.get('ITS ID', ''),
            info.get('TRNumber', ''),
            info.get('Darajah', ''),
            info.get('Marhala', ''),
            month_label,
            month_stats.get('target', 0),
            month_stats.get('count', 0),
            month_stats.get('review_count', 0),
            month_stats.get('review_marks', 0),
            b.get('title', ''),
            b.get('collection', ''),
            b.get('language', ''),
            b.get('_issued_hijri', ''),
            b.get('_due_hijri', ''),
            'Yes' if b.get('returned') else 'No',
            'Yes' if b.get('overdue') else 'No',
            'Returned' if b.get('returned') else 'Overdue' if b.get('overdue') else 'Active'
        ])
    
    output.seek(0)
    student_name_clean = re.sub(r'[^\w\s-]', '', info.get('FullName', '')).replace(' ', '_')
    month_clean = re.sub(r'[^\w\s-]', '', month_label).replace(' ', '_')
    filename = f"borrowed_books_{student_name_clean}_{month_clean}.csv"
    
    return Response(output, mimetype="text/csv", headers={"Content-Disposition": f"attachment;filename={filename}"})


@bp.route("/<identifier>/export/excel")
@require_login
def export_student_excel(identifier):
    info = get_student_info(identifier)
    if not info:
        return "Student not found", 404

    try:
        main_data = []
        for month, books in info.get("BorrowedBooksGrouped", []):
            for book in books:
                main_data.append({
                    'Month': month,
                    'Title': clean_html_for_pdf(book.get('title', '')),
                    'Collection': book.get('collection', ''),
                    'Language': book.get('language', ''),
                    'Issued': book.get('_issued_hijri', ''),
                    'Due': book.get('_due_hijri', ''),
                    'Status': 'Overdue' if book.get('overdue') else 'Returned' if book.get('returned') else 'Active'
                })
        
        main_df = pd.DataFrame(main_data) if main_data else pd.DataFrame({'Message': ['No data']})
        
        metrics = info.get('Metrics', {})
        summary_data = {
            'Field': ['Full Name', 'ITS ID', 'TR Number', 'Darajah', 'Marhala', 
                     'AY Issues', 'Currently Issued', 'Overdue', 'Mustawā', 
                     'Fees Paid (AY)', 'Outstanding Balance'],
            'Value': [info.get('FullName', ''), info.get('ITS ID', ''), info.get('TRNumber', ''),
                     info.get('Darajah', ''), info.get('Marhala', ''),
                     metrics.get('AYIssues', 0), metrics.get('CurrentlyIssued', 0),
                     metrics.get('OverdueNow', 0), metrics.get('MaxBooksAllowed', 'N/A'),
                     f"{metrics.get('FeesPaidAY', 0):.2f}", f"{metrics.get('OutstandingBalance', 0):.2f}"]
        }
        summary_df = pd.DataFrame(summary_data)
        
        monthly_stats = []
        for month, stats in info.get('MonthStats', {}).items():
            monthly_stats.append({
                'Month': month,
                'Books Issued': stats.get('count', 0),
                'Target': stats.get('target', 0),
                'Status': stats.get('reco_status', ''),
                'Reviews': stats.get('review_count', 0),
                'Review Marks': stats.get('review_marks', 0)
            })
        monthly_df = pd.DataFrame(monthly_stats) if monthly_stats else pd.DataFrame({'Message': ['No monthly stats']})
        
        additional_sheets = {'Summary': summary_df, 'Monthly Stats': monthly_df}
        excel_bytes = dataframe_to_excel_bytes(main_df, sheet_name='Borrowed Books', additional_sheets=additional_sheets)
        
        buffer = BytesIO(excel_bytes)
        student_name_clean = re.sub(r'[^\w\s-]', '', info.get('FullName', '')).replace(' ', '_')
        filename = f"student_data_{student_name_clean}.xlsx"
        
        return send_file(buffer, as_attachment=True, download_name=filename, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    
    except Exception as e:
        current_app.logger.error(f"Error generating Excel: {e}")
        return f"Error generating Excel: {str(e)}", 500


# ============================================================================
# API ENDPOINTS
# ============================================================================

@bp.route("/api/search", methods=["GET"])
@require_login
def search_students():
    query = request.args.get('q', '').strip()
    if not query or len(query) < 2:
        return jsonify({"success": False, "message": "Query too short"}), 400
    
    conn = get_koha_conn()
    cur = conn.cursor(dictionary=True)
    
    try:
        darajah_codes_ph = ",".join(["%s"] * len(DARAJAH_CODES))
        tr_codes_ph = ",".join(["%s"] * len(TR_ATTR_CODES))
        
        sql = f"""
            SELECT 
                b.borrowernumber,
                b.cardnumber,
                CONCAT_WS(' ', b.firstname, b.surname) AS FullName,
                b.userid AS ITS_ID,
                b.email AS EduEmail,
                COALESCE(c.description, b.categorycode) AS Marhala,
                COALESCE(std.attribute, b.branchcode) AS Darajah,
                tr.attribute AS TRNumber
            FROM borrowers b
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber
                AND std.code IN ({darajah_codes_ph})
            LEFT JOIN borrower_attributes tr
                ON tr.borrowernumber = b.borrowernumber
                AND tr.code IN ({tr_codes_ph})
            WHERE 
                b.firstname LIKE %s OR
                b.surname LIKE %s OR
                CONCAT_WS(' ', b.firstname, b.surname) LIKE %s OR
                b.userid LIKE %s OR
                tr.attribute LIKE %s
            ORDER BY b.surname, b.firstname
            LIMIT 20
        """
        
        like_pattern = f"%{query}%"
        params = (*DARAJAH_CODES, *TR_ATTR_CODES, like_pattern, like_pattern, like_pattern, like_pattern, like_pattern)
        
        cur.execute(sql, params)
        results = cur.fetchall()
        
        for student in results:
            darajah = student.get('Darajah')
            if darajah:
                teachers = _get_teachers_for_darajah(darajah)
                student['Teachers'] = teachers
                student['PrimaryTeacher'] = teachers[0] if teachers else None
        
        return jsonify({"success": True, "results": results})
        
    except Exception as e:
        current_app.logger.error(f"Error searching students: {e}")
        return jsonify({"success": False, "message": "Search error"}), 500
    finally:
        cur.close()
        conn.close()


@bp.route("/api/darajah/<darajah_name>")
@require_login
def get_darajah_students(darajah_name):
    conn = get_koha_conn()
    cur = conn.cursor(dictionary=True)
    
    try:
        darajah_codes_ph = ",".join(["%s"] * len(DARAJAH_CODES))
        tr_codes_ph = ",".join(["%s"] * len(TR_ATTR_CODES))
        
        sql = f"""
            SELECT 
                b.borrowernumber,
                b.cardnumber,
                CONCAT_WS(' ', b.firstname, b.surname) AS FullName,
                b.userid AS ITS_ID,
                b.email AS EduEmail,
                COALESCE(c.description, b.categorycode) AS Marhala,
                COALESCE(std.attribute, b.branchcode) AS Darajah,
                tr.attribute AS TRNumber
            FROM borrowers b
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber
                AND std.code IN ({darajah_codes_ph})
            LEFT JOIN borrower_attributes tr
                ON tr.borrowernumber = b.borrowernumber
                AND tr.code IN ({tr_codes_ph})
            WHERE COALESCE(std.attribute, b.branchcode) = %s
            ORDER BY b.surname, b.firstname
        """
        
        params = (*DARAJAH_CODES, *TR_ATTR_CODES, darajah_name)
        cur.execute(sql, params)
        students = cur.fetchall()
        
        teachers = _get_teachers_for_darajah(darajah_name)
        
        return jsonify({
            "success": True,
            "darajah": darajah_name,
            "teacher_count": len(teachers),
            "student_count": len(students),
            "teachers": teachers,
            "students": students
        })
        
    except Exception as e:
        current_app.logger.error(f"Error fetching darajah students: {e}")
        return jsonify({"success": False, "message": str(e)}), 500
    finally:
        cur.close()
        conn.close()


# ==================== MARKS / TAQEEM API ENDPOINTS ====================

@bp.route("/api/marks/<student_username>")
@require_login
def get_student_marks(student_username):
    try:
        from services.marks_service import calculate_total_taqeem
        
        current_ay = Config.CURRENT_ACADEMIC_YEAR().replace('H', '')
        academic_year = request.args.get('year', current_ay)
        marks_data = calculate_total_taqeem(student_username, academic_year)
        
        if not marks_data:
            marks_data = _calculate_simple_taqeem({"TRNumber": student_username})
        
        return jsonify({
            "success": True,
            "student": student_username,
            "academic_year": academic_year,
            "marks": marks_data
        })
    except Exception as e:
        current_app.logger.error(f"Error fetching marks for {student_username}: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@bp.route("/api/marks/update/<student_username>", methods=["POST"])
@require_login
def update_student_marks_api(student_username):
    try:
        current_ay = Config.CURRENT_ACADEMIC_YEAR().replace('H', '')
        academic_year = request.json.get('year', current_ay) if request.json else current_ay
        
        info = get_student_info(student_username)
        if not info:
            return jsonify({"success": False, "message": "Student not found"}), 404
        
        taqeem_data = None
        try:
            from services.marks_service import calculate_total_taqeem
            taqeem_data = calculate_total_taqeem(
                student_username=student_username, 
                academic_year=academic_year
            )
        except Exception as e:
            current_app.logger.error(f"Error calculating Taqeem: {e}")
            taqeem_data = _calculate_simple_taqeem(info)
        
        return jsonify({
            "success": True,
            "message": f"Marks updated for {student_username}",
            "taqeem": taqeem_data
        })
            
    except Exception as e:
        current_app.logger.error(f"Error updating marks for {student_username}: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@bp.route("/api/marks/class/<darajah_name>")
@require_login
def get_class_marks(darajah_name):
    conn = None
    cur = None
    try:
        conn = get_app_conn()
        cur = conn.cursor()
        
        current_ay = Config.CURRENT_ACADEMIC_YEAR().replace('H', '')
        academic_year = request.args.get('year', current_ay)
        
        cur.execute("""
            SELECT 
                student_username,
                student_name,
                total_marks,
                book_issue_total,
                book_review_marks,
                program_attendance_marks,
                last_updated
            FROM student_taqeem
            WHERE darajah_name = ? AND academic_year = ?
            ORDER BY student_name
        """, (darajah_name, academic_year))
        
        marks_list = []
        for row in cur.fetchall():
            marks_list.append({
                "username": row[0],
                "name": row[1],
                "total": row[2],
                "book_issue": row[3],
                "book_review": row[4],
                "program_attendance": row[5],
                "last_updated": row[6]
            })
        
        return jsonify({
            "success": True,
            "darajah": darajah_name,
            "academic_year": academic_year,
            "student_count": len(marks_list),
            "marks": marks_list
        })
        
    except Exception as e:
        current_app.logger.error(f"Error fetching class marks for {darajah_name}: {e}")
        return jsonify({"success": False, "message": str(e)}), 500
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# ============================================================================
# BATCH EXPORT ROUTES
# ============================================================================

@bp.route("/batch/export/pdf", methods=["POST"])
@require_login
def batch_export_pdf():
    try:
        data = request.get_json()
        if not data or 'students' not in data:
            return jsonify({"success": False, "message": "No students provided"}), 400
        
        student_identifiers = data['students']
        
        if not student_identifiers or not isinstance(student_identifiers, list):
            return jsonify({"success": False, "message": "Invalid students list"}), 400
        
        if len(student_identifiers) > 50:
            return jsonify({"success": False, "message": "Batch size limited to 50 students"}), 400
        
        reports = []
        for identifier in student_identifiers[:20]:
            info = get_student_info(identifier)
            if info:
                reports.append({
                    'name': f"student_{info.get('ITS ID', identifier)}",
                    'type': 'student',
                    'params': {
                        'student_info': info,
                        'borrowed_books': info.get("BorrowedBooks", []),
                        'monthly_stats': info.get("MonthStats", {})
                    }
                })
        
        if not reports:
            return jsonify({"success": False, "message": "No valid students found"}), 404
        
        from services.exports import create_batch_reports
        results = create_batch_reports(reports)
        
        if results:
            first_key = list(results.keys())[0]
            buffer = BytesIO(results[first_key])
            filename = f"batch_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
            return send_file(buffer, as_attachment=True, download_name=filename, mimetype="application/pdf")
        else:
            return jsonify({"success": False, "message": "No reports generated"}), 500
            
    except Exception as e:
        current_app.logger.error(f"Error in batch export: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


# ============================================================================
# HELPER FUNCTIONS FOR EXPORTS
# ============================================================================

def _prepare_student_data_for_export(info):
    if not info:
        return None
    
    export_data = {
        'student_id': info.get('ITS ID', ''),
        'full_name': info.get('FullName', ''),
        'tr_number': info.get('TRNumber', ''),
        'darajah': info.get('Darajah', ''),
        'marhala': info.get('Marhala', ''),
        'email': info.get('EduEmail', ''),
        'photo': info.get('Photo', ''),
        'teachers': info.get('Teachers', []),
        'metrics': info.get('Metrics', {}),
        'taqeem': info.get('Taqeem'),
        'favorite_authors': info.get('FavoriteAuthors', []),
        'fees_list': info.get('FeesList', [])
    }
    
    borrowed_by_month = []
    for month, books in info.get("BorrowedBooksGrouped", []):
        month_data = {
            'month': month,
            'books': [],
            'stats': info.get('MonthStats', {}).get(month, {})
        }
        
        for book in books:
            month_data['books'].append({
                'title': clean_html_for_pdf(book.get('title', '')),
                'collection': book.get('collection', ''),
                'language': book.get('language', ''),
                'issued': book.get('_issued_hijri', ''),
                'due': book.get('_due_hijri', ''),
                'overdue': book.get('overdue', False),
                'returned': book.get('returned', False),
                'opac_url': book.get('opac_url', '#')
            })
        
        borrowed_by_month.append(month_data)
    
    export_data['borrowed_by_month'] = borrowed_by_month
    
    reviews_by_month = []
    for month, reviews in info.get("BookReviewsByMonth", {}).items():
        reviews_by_month.append({
            'month': month,
            'reviews': reviews
        })
    
    export_data['reviews_by_month'] = reviews_by_month
    
    return export_data