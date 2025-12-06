# routes/students.py
from flask import (
    Blueprint,
    render_template,
    session,
    redirect,
    url_for,
    send_file,
    current_app,
    request,
)
from db_koha import get_koha_conn
from datetime import date, datetime
from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Table,
    TableStyle,
    Spacer,
    Image,
)
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from reportlab.lib.enums import TA_RIGHT  # optional (for RTL alignment)
import os
import re

# ✅ reuse font + RTL helpers from exports (they register a Unicode TTF & shape Arabic)
from services.exports import _ensure_font_registered, _shape_if_rtl

# ✅ darajah / class config (max / mustawā) from central Koha queries
from services.koha_queries import darajah_max_books as _darajah_max_books_from_db

bp = Blueprint("students_bp", __name__)

# Accept any of these borrower attribute codes as "Class"
CLASS_CODES = ("STD", "CLASS", "DAR", "CLASS_STD")

# Accept any of these borrower attribute codes for Class Teacher
CLASS_TEACHER_CODES = (
    "CLASS_TEACHER",
    "TEACHER",
    "ADVISOR",
    "MENTOR",
    "HOMEROOM",
    "HR_TEACHER",
)

# Cache for darajah_max_books() ranges
_DARAJAH_MAX_CACHE = None


# ---------------- LOGIN DECORATOR ----------------
def require_login(f):
    from functools import wraps

    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            # align with other dashboards
            return redirect(url_for("auth_bp.login"))
        return f(*args, **kwargs)

    return wrapper


# ---------------- Hijri helpers ----------------
def _hijri_date_label(d: date) -> str:
    """Format a date as Hijri (fallback to Gregorian if hijri_converter not available)."""
    if not d:
        return "-"
    if isinstance(d, datetime):
        d = d.date()
    try:
        from hijri_converter import convert

        h = convert.Gregorian(d.year, d.month, d.day).to_hijri()
        hijri_months = [
            "Muḥarram al-Harām",
            "Safar al-Muzaffar",
            "Rabi al-Awwal",
            "Rabī al-Aakhar",
            "Jamādil Awwal",
            "Jamādā al-ʾŪkhrā",
            "Rajab al-Asab",
            "Shabān al-Karim",
            "Shehrullah al-Moazzam",
            "Shawwāl al-Mukarram",
            "Zilqādah al-Harām",
            "Zilhijjatil Harām",
        ]
        return f"{h.day} {hijri_months[h.month - 1]} {h.year} H"
    except Exception:
        try:
            return d.strftime("%d %B %Y")
        except Exception:
            return str(d)


def _to_hijri_str(val) -> str:
    """Convert a date/datetime/ISO string to a Hijri label string."""
    if not val:
        return "-"
    if isinstance(val, datetime):
        d = val.date()
    elif isinstance(val, date):
        d = val
    elif isinstance(val, str):
        # Try YYYY-MM-DD first
        try:
            d = datetime.strptime(val.split(" ")[0], "%Y-%m-%d").date()
        except Exception:
            return val
    else:
        return str(val)
    return _hijri_date_label(d)


def _hijri_month_label_and_key(d: date):
    """
    Return (label, sort_key) for a month in Hijri:
      label   -> e.g. "Jumada al-Ula 1447 H"
      sort_key-> (year, month) tuple for ordering
    Fallback to Gregorian month/year if Hijri conversion not available.
    """
    if not d:
        return "-", (0, 0)
    if isinstance(d, datetime):
        d = d.date()
    try:
        from hijri_converter import convert

        h = convert.Gregorian(d.year, d.month, d.day).to_hijri()
        hijri_months = [
            "Muḥarram al-Harām",
            "Safar al-Muzaffar",
            "Rabi al-Awwal",
            "Rabī al-Aakhar",
            "Jamādil Awwal",
            "Jamādā al-ʾŪkhrā",
            "Rajab al-Asab",
            "Shabān al-Karim",
            "Shehrullah al-Moazzam",
            "Shawwāl al-Mukarram",
            "Zilqādah al-Harām",
            "Zilhijjatil Harām",
        ]
        label = f"{hijri_months[h.month - 1]} {h.year} H"
        return label, (h.year, h.month)
    except Exception:
        return d.strftime("%B %Y"), (d.year, d.month)


# ---------------- Academic window ----------------
def _ay_bounds():
    """
    Academic window: Apr 1 of current AY → today (if between Apr–Dec),
    or previous AY (Apr 1 – Dec 31) if we are in Jan–Mar.
    (Aligned with teacher / HOD dashboards.)
    """
    today = date.today()
    if 4 <= today.month <= 12:
        start = date(today.year, 4, 1)
        end = today
    else:
        ay_year = today.year - 1
        start = date(ay_year, 4, 1)
        end = date(ay_year, 12, 31)
    return start, end


def _ay_period_label():
    """Human-friendly AY label in Hijri (fallback to Gregorian)."""
    start, end = _ay_bounds()
    return f"{_hijri_date_label(start)} – {_hijri_date_label(end)}"


# ---------------- Darajah → Max / Mustawā mapping ----------------
def _load_darajah_max_cache():
    """
    Load darajah ranges from services.koha_queries.darajah_max_books()
    and cache as a list of (low_darajah, high_darajah, max_books).
    """
    global _DARAJAH_MAX_CACHE
    if _DARAJAH_MAX_CACHE is not None:
        return _DARAJAH_MAX_CACHE

    ranges = []
    try:
        rows = _darajah_max_books_from_db()  # e.g. [('Darajah 1–4', 4), ('Darajah 5–7', 5), ...]
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


def _max_books_for_darajah(class_label: str):
    """
    Return max / mustawā books for a student based on their Darajah.

    Priority:
      1) Use config from darajah_max_books() in services/koha_queries.py
      2) Fallback heuristic if no config rows / parse failure:
           Darajah 1–4  -> 4
           Darajah 5–7  -> 5
           Darajah 8–11 -> 6
    """
    if not class_label:
        return None

    # Extract first number from something like "3AF", "5 B M", "Darajah 7", etc.
    m = re.search(r"(\d+)", str(class_label))
    if not m:
        return None
    d_num = int(m.group(1))

    # 1) Try central configuration
    ranges = _load_darajah_max_cache()
    for lo, hi, max_books in ranges:
        if lo <= d_num <= hi:
            return max_books

    # 2) Fallback heuristic
    if 1 <= d_num <= 4:
        return 4
    if 5 <= d_num <= 7:
        return 5
    if 8 <= d_num <= 11:
        return 6
    return None


# ---------------- DATA ACCESS ----------------
def get_student_info(identifier):
    """Fetch student details, borrowed books (AY), engagement metrics, fines, photo, and class teacher.
       Lookup supports Cardnumber, ITS (userid), TRNO (borrower_attributes), and Borrowernumber.
       Class and TRNO come from borrower_attributes (multiple code support).
       Department uses categories.description, falling back to categorycode.
    """
    identifier = (identifier or "").strip()

    conn = get_koha_conn()
    cur = conn.cursor(dictionary=True)

    # --- Student core info ---
    class_codes_ph = ",".join(["%s"] * len(CLASS_CODES))
    teacher_codes_ph = ",".join(["%s"] * len(CLASS_TEACHER_CODES))

    sql = f"""
        SELECT
            b.borrowernumber,
            b.cardnumber,
            CONCAT_WS(' ', b.firstname, b.surname) AS FullName,
            b.userid AS `ITS ID`,
            b.email AS EduEmail,
            b.categorycode,
            COALESCE(c.description, b.categorycode) AS Department,
            COALESCE(std.attribute, b.branchcode) AS Class,
            trno.attribute AS TRNumber,
            teacher.attribute AS ClassTeacher
        FROM borrowers b
        LEFT JOIN categories c ON c.categorycode = b.categorycode
        LEFT JOIN borrower_attributes std
               ON std.borrowernumber = b.borrowernumber
              AND std.code IN ({class_codes_ph})
        LEFT JOIN borrower_attributes trno
               ON trno.borrowernumber = b.borrowernumber
              AND trno.code = 'TRNO'
        LEFT JOIN borrower_attributes teacher
               ON teacher.borrowernumber = b.borrowernumber
              AND teacher.code IN ({teacher_codes_ph})
        WHERE
              LOWER(b.cardnumber) = LOWER(%s)                 -- Cardnumber
           OR LOWER(b.userid)     = LOWER(%s)                 -- ITS ID
           OR LOWER(COALESCE(trno.attribute,'')) = LOWER(%s)  -- TRNO from attributes
           OR CAST(b.borrowernumber AS CHAR) = %s             -- Borrowernumber
        LIMIT 1
    """
    params = (*CLASS_CODES, *CLASS_TEACHER_CODES, identifier, identifier, identifier, identifier)
    cur.execute(sql, params)
    student = cur.fetchone()
    if not student:
        conn.close()
        return None

    borrowernumber = student["borrowernumber"]

    # AY bounds + label (aligned with dashboards)
    start_ay, end_ay = _ay_bounds()
    ay_period_label = _ay_period_label()

    # ===== Library engagement metrics =====
    # Active loans (unreturned) + basic history from issues
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

    # Old issues (history)
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

    # Compute overdue flags & counts, and Hijri date strings
    today = date.today()
    overdue_count = 0
    active_count = 0
    for b in borrowed_books:
        is_returned = bool(b.get("returned"))
        if not is_returned:
            active_count += 1

        # overdue flag
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

        # Hijri display strings
        b["_issued_hijri"] = _to_hijri_str(b.get("date_issued"))
        b["_due_hijri"] = _to_hijri_str(b.get("date_due"))

    # Lifetime issues (issues + old_issues)
    lifetime_issues = len(borrowed_books)

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
    last_issue_date_raw = row.get("last_issue")
    last_issue_date = _to_hijri_str(last_issue_date_raw)

    # Last return date (issues/old_issues)
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
    last_return_date_raw = row.get("last_return")
    last_return_date = _to_hijri_str(last_return_date_raw)

    # Reservations (holds) count (if table exists)
    reservations = 0
    try:
        cur.execute("SELECT COUNT(*) AS cnt FROM reserves WHERE borrowernumber=%s", (borrowernumber,))
        rr = cur.fetchone()
        reservations = int(rr["cnt"]) if rr and rr.get("cnt") is not None else 0
    except Exception:
        reservations = 0

    # Outstanding balance now
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

    # Fines paid totals + last payment date (exclude VOID) – lifetime
    cur.execute(
        """
        SELECT
          COALESCE(SUM(CASE WHEN credit_type_code='PAYMENT' AND (status IS NULL OR status<>'VOID') THEN -amount END),0) AS TotalFinesPaid,
          MAX(CASE WHEN credit_type_code='PAYMENT' AND (status IS NULL OR status<>'VOID') THEN date END) AS LastPaymentDate
        FROM accountlines
        WHERE borrowernumber=%s
        """,
        (borrowernumber,),
    )
    fines_row = cur.fetchone() or {}
    total_fines_paid = float(fines_row.get("TotalFinesPaid") or 0)
    last_payment_date = _to_hijri_str(fines_row.get("LastPaymentDate"))

    # Fines paid in AY (current academic year window ONLY)
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
    fines_paid_ay = float(ay_row.get("paid") or 0)

    # Top authors (last 12 months) – REAL NAMES ONLY
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

        fav_authors = []
        for r in fav_rows:
            a = (r.get("author") or "").strip()
            # Skip blanks / placeholders / "Unknown"
            if not a:
                continue
            if a.lower() in ("unknown", "unk", "n/a", "na", "-", "none"):
                continue
            fav_authors.append(f"{a} ({int(r['cnt'])})")

        # Keep only top 10 *real* authors
        fav_authors = fav_authors[:10]
    except Exception:
        fav_authors = []

    # --- Group by Hijri month-year for display (AY ONLY) ---
    # use Hijri month labels but still filter by AY window on Gregorian dates
    months_map = {}
    for b in borrowed_books:
        di = b.get("date_issued")
        # filter to AY range for grouped display
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

    # turn into sorted list (latest Hijri month first)
    grouped_items = []
    for key, entry in months_map.items():
        grouped_items.append((key, entry["label"], entry["books"]))
    grouped_items.sort(key=lambda x: x[0], reverse=True)
    grouped_sorted = [(label, books) for (_, label, books) in grouped_items]

    # --- Per-month stats vs darajah-based Mustawā --- 
    class_label = student.get("Class")
    max_books_allowed = _max_books_for_darajah(class_label)
    month_stats = {}
    for month_label, books in grouped_sorted:
        count = len(books)

        # status relative to max allowed (for backwards compatibility)
        if max_books_allowed:
            status = "above" if count >= max_books_allowed else "below"
        else:
            status = "na"

        # Use darajah config as Mustawā al-Maṭlūb (recommended per month)
        target = max_books_allowed

        if target:
            if count >= target:
                reco_status = "meets"
                reco_text = (
                    "Excellent – you met or exceeded the recommended reading level "
                    f"of {target} books this month."
                )
            elif count == 0:
                reco_status = "none"
                reco_text = (
                    "No books issued this month. Please plan to borrow and read at least "
                    f"{target} books next month."
                )
            else:
                reco_status = "below"
                reco_text = (
                    f"Below the recommended level of {target} books. Try to borrow and read "
                    "a bit more next month."
                )
        else:
            # No configured target; still give a neutral message
            reco_status = "na"
            reco_text = f"This month you issued {count} book(s)."

        month_stats[month_label] = {
            "count": count,
            "max_books": max_books_allowed,
            "status": status,
            "target": target,
            "reco_status": reco_status,
            "reco_text": reco_text,
        }

    # --- Detailed fines list (AY only, current academic year) ---
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
    fines_list = cur.fetchall() or []
    for f in fines_list:
        f["date"] = _to_hijri_str(f.get("date"))

    cur.close()
    conn.close()

    # --- Photo lookup ---
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

    # --- Build info dict (names used by template) ---
    student["Photo"] = photo_filename
    student["BorrowedBooks"] = borrowed_books
    student["BorrowedBooksGrouped"] = grouped_sorted
    student["MonthStats"] = month_stats

    # legacy fields still used in template badges
    student["Total Issues"] = lifetime_issues
    student["Overdue Count"] = overdue_count
    student["Total Fines Paid"] = total_fines_paid

    # new metrics bundle
    student["Metrics"] = {
        "ActiveLoans": active_count,
        "OverdueNow": overdue_count,
        "LifetimeIssues": lifetime_issues,
        "AYIssues": ay_issues,
        "Reservations": reservations,
        "OutstandingBalance": outstanding_balance,
        "TotalFinesPaid": total_fines_paid,
        "FinesPaidAY": fines_paid_ay,
        "LastPaymentDate": last_payment_date,
        "LastIssueDate": last_issue_date,
        "LastReturnDate": last_return_date,
        "FavoriteAuthors": fav_authors,
        "MaxBooksAllowed": max_books_allowed,
        "AYPeriodLabel": ay_period_label,
    }

    # expose fines list if you show it in the template
    student["FinesList"] = fines_list

    return student


# ---------------- ROUTES ----------------
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

    opac_base = current_app.config.get("KOHA_OPAC_BASE_URL", "").rstrip("/")
    return render_template(
        "student.html",
        found=True,
        info=info,
        hide_nav=True,
        opac_base_url=opac_base,
    )


@bp.route("/lookup", methods=["GET", "POST"])
@require_login
def lookup_student():
    q = (request.values.get("q") or "").strip()
    if not q:
        return redirect(url_for("reports_bp.reports_page"))
    return redirect(url_for("students_bp.student", identifier=q))


@bp.route("/<identifier>/pdf")
@require_login
def export_student_pdf(identifier):
    """Export student profile to PDF with Unicode font + Arabic shaping (AY-focused)."""
    info = get_student_info(identifier)
    if not info:
        return "Student not found", 404

    # 1) Ensure a Unicode-capable font (e.g., NotoNaskhArabic-Regular.ttf) is embedded
    font_name = _ensure_font_registered()

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    styles = getSampleStyleSheet()

    # 2) Use our font everywhere (so Arabic glyphs render)
    for key in ("Title", "Normal", "Heading2", "Heading3"):
        styles[key].fontName = font_name
    # Optional: uncomment if you want right alignment for long Arabic paragraphs
    # styles["Normal"].alignment = TA_RIGHT

    elements = []

    # 3) Shaped title (Arabic joining + bidi)
    title_txt = _shape_if_rtl(f"Library Report – {info.get('FullName') or ''}")
    elements.append(Paragraph(title_txt, styles["Title"]))
    elements.append(Spacer(1, 0.3 * cm))

    # Photo
    photo_path = os.path.join(current_app.root_path, "static", info["Photo"])
    if not os.path.exists(photo_path):
        photo_path = os.path.join(current_app.root_path, "static", "images", "avatar.png")
    if os.path.exists(photo_path):
        elements.append(Image(photo_path, width=4 * cm, height=4 * cm))
        elements.append(Spacer(1, 0.3 * cm))

    met = info["Metrics"]

    # Helper: shape any cell text that might contain Arabic
    def S(x):
        return _shape_if_rtl(str(x) if x is not None else "-")

    # Identity / class info + key metrics (AY-focused)
    summary_data = [
        [S("👤 Full Name"), S(info.get("FullName") or "-")],
        [S("🆔 ITS ID"), S(info.get("ITS ID") or "-")],
        [S("📘 TR Number"), S(info.get("TRNumber") or "-")],
        [S("🏫 Class"), S(info.get("Class") or "-")],
        [S("👩‍🏫 Class Teacher"), S(info.get("ClassTeacher") or "-")],
        [S("🏢 Department"), S(info.get("Department") or "-")],
        [S("📅 Academic Year"), S(met.get("AYPeriodLabel") or "-")],
        [S("📚 AY Issues"), S(met["AYIssues"])],
        [S("📖 Active Loans"), S(met["ActiveLoans"])],
        [S("⏰ Overdue Now"), S(met["OverdueNow"])],
        [S("💰 Fines Paid (AY)"), S(f"{met['FinesPaidAY']:.2f}")],
        [S("💰 Total Fines Paid"), S(f"{met['TotalFinesPaid']:.2f}")],
        [S("💳 Outstanding Balance"), S(f"{met['OutstandingBalance']:.2f}")],
        [S("🕒 Last Issue (Hijri)"), S(met["LastIssueDate"] or "-")],
        [S("🕒 Last Return (Hijri)"), S(met["LastReturnDate"] or "-")],
    ]
    summary_table = Table(summary_data, colWidths=[5.5 * cm, 7.5 * cm])
    summary_table.setStyle(
        TableStyle(
            [
                ("FONTNAME", (0, 0), (-1, -1), font_name),
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ]
        )
    )
    elements.append(summary_table)
    elements.append(Spacer(1, 0.5 * cm))

    # Section header (shaped)
    elements.append(
        Paragraph(
            S("Borrowed Books (Academic Year, grouped by month)"), styles["Heading2"]
        )
    )

    # Borrowed books tables (AY-only, already filtered in info)
    if info["BorrowedBooksGrouped"]:
        for month, books in info["BorrowedBooksGrouped"]:
            elements.append(Paragraph(S(f"📅 {month}"), styles["Heading3"]))
            books_data = [
                [
                    S("Title"),
                    S("Collection"),
                    S("Language"),
                    S("Date Issued (Hijri)"),
                    S("Due Date (Hijri)"),
                    S("Returned"),
                    S("Overdue?"),
                ]
            ]
            for b in books:
                books_data.append(
                    [
                        S(b.get("title", "N/A")),
                        S(b.get("collection") or "-"),
                        S(b.get("language") or "-"),
                        S(b.get("_issued_hijri") or "-"),
                        S(b.get("_due_hijri") or "-"),
                        S("Yes" if b.get("returned") else "No"),
                        S("Overdue") if b.get("overdue") else S("On Time"),
                    ]
                )
            books_table = Table(
                books_data,
                repeatRows=1,
                colWidths=[5 * cm, 2.5 * cm, 2.5 * cm, 3 * cm, 3 * cm, 2 * cm, 2 * cm],
            )
            books_table.setStyle(
                TableStyle(
                    [
                        ("FONTNAME", (0, 0), (-1, -1), font_name),
                        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                    ]
                )
            )
            elements.append(books_table)
            elements.append(Spacer(1, 0.3 * cm))
    else:
        elements.append(
            Paragraph(
                S("No borrowed books found in this Academic Year."), styles["Normal"]
            )
        )

    # Favorite authors brief
    fav = met.get("FavoriteAuthors") or []
    if fav:
        elements.append(Spacer(1, 0.3 * cm))
        elements.append(
            Paragraph(
                S("Favorite Authors (last 12 months): ") + S(", ".join(fav)),
                styles["Normal"],
            )
        )

    # Fines & payments (AY)
    fines = info.get("FinesList") or []
    if fines:
        elements.append(Spacer(1, 0.5 * cm))
        elements.append(Paragraph(S("💰 Fines & Payments (Academic Year)"), styles["Heading2"]))
        fines_data = [[S("Date (Hijri)"), S("Amount"), S("Description"), S("Note")]]
        for f in fines:
            fines_data.append(
                [
                    S(f.get("date", "-")),
                    S(f.get("amount", "-")),
                    S(f.get("description", "-")),
                    S(f.get("note", "-")),
                ]
            )
        fines_table = Table(
            fines_data, repeatRows=1, colWidths=[3.5 * cm, 2.5 * cm, 5 * cm, 3 * cm]
        )
        fines_table.setStyle(
            TableStyle(
                [
                    ("FONTNAME", (0, 0), (-1, -1), font_name),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ]
            )
        )
        elements.append(fines_table)

    doc.build(elements)
    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name=f"student_report_{identifier}.pdf",
        mimetype="application/pdf",
    )
