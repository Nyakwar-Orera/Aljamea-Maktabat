# routes/students.py
from flask import Blueprint, render_template, session, redirect, url_for, send_file, current_app, request
from db_koha import get_koha_conn
from datetime import date, datetime
from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from reportlab.lib.enums import TA_RIGHT  # optional (for RTL alignment)
import os
from collections import defaultdict

# ✅ reuse font + RTL helpers from exports (they register a Unicode TTF & shape Arabic)
from services.exports import _ensure_font_registered, _shape_if_rtl

bp = Blueprint("students_bp", __name__)

# Accept any of these borrower attribute codes as "Class"
CLASS_CODES = ("STD", "CLASS", "DAR", "CLASS_STD")

# Accept any of these borrower attribute codes for Class Teacher
CLASS_TEACHER_CODES = ("CLASS_TEACHER", "TEACHER", "ADVISOR", "MENTOR", "HOMEROOM", "HR_TEACHER")

# ---------------- LOGIN DECORATOR ----------------
def require_login(f):
    from functools import wraps
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("admin_bp.index"))
        return f(*args, **kwargs)
    return wrapper

# ---------------- Academic window ----------------
def _ay_bounds():
    """Academic window: Apr 1 → today (capped at Dec 31 of current year). None/None before April."""
    today = date.today()
    year = today.year
    if today.month < 4:
        return None, None
    start = date(year, 4, 1)
    end = min(today, date(year, 12, 31))
    return start, end

# ---------------- DATA ACCESS ----------------
def get_student_info(identifier):
    """Fetch student details, borrowed books, engagement metrics, fines, photo, and class teacher.
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

    # ===== Library engagement metrics =====
    start_ay, end_ay = _ay_bounds()

    # Active loans (unreturned) + basic history from issues
    cur.execute(
        """
        SELECT bi.title,
               iss.issuedate AS date_issued,
               iss.date_due,
               iss.returndate,
               (iss.returndate IS NOT NULL) AS returned
        FROM issues iss
        JOIN items it USING (itemnumber)
        JOIN biblio bi USING (biblionumber)
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
            SELECT bi.title,
                   oi.issuedate AS date_issued,
                   oi.returndate,
                   oi.returndate AS date_due,
                   1 AS returned
            FROM old_issues oi
            JOIN items it USING (itemnumber)
            JOIN biblio bi USING (biblionumber)
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
            due_date = b["date_due"].date() if hasattr(b["date_due"], "date") else b["date_due"]
            b["overdue"] = (today > due_date)
        else:
            b["overdue"] = False
        if b["overdue"]:
            overdue_count += 1

    # Lifetime issues (issues + old_issues)
    lifetime_issues = len(borrowed_books)

    # AY issues (statistics)
    ay_issues = 0
    if start_ay:
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
    last_issue_date = row.get("last_issue")

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
    last_return_date = row.get("last_return")

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

    # Fines paid totals + last payment date (exclude VOID)
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
    last_payment_date = fines_row.get("LastPaymentDate")

    fines_paid_ay = 0.0
    if start_ay:
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

    # Top authors (last 12 months)
    try:
        cur.execute(
            """
            SELECT COALESCE(bi.author,'Unknown') AS author, COUNT(*) AS cnt
            FROM (
              SELECT itemnumber, issuedate FROM issues WHERE borrowernumber=%s
              UNION ALL
              SELECT itemnumber, issuedate FROM old_issues WHERE borrowernumber=%s
            ) u
            JOIN items it USING (itemnumber)
            JOIN biblio bi USING (biblionumber)
            WHERE u.issuedate >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
            GROUP BY author
            ORDER BY cnt DESC
            LIMIT 3
            """,
            (borrowernumber, borrowernumber),
        )
        fav_authors = [f"{r['author']} ({int(r['cnt'])})" for r in (cur.fetchall() or [])]
    except Exception:
        fav_authors = []

    # --- Group by month-year for display ---
    grouped = defaultdict(list)
    for b in borrowed_books:
        if b["date_issued"] and hasattr(b["date_issued"], "strftime"):
            month_label = b["date_issued"].strftime("%B %Y")
        else:
            month_label = "Unknown"
        grouped[month_label].append(b)

    grouped = sorted(
        grouped.items(),
        key=lambda x: datetime.strptime(x[0], "%B %Y") if x[0] != "Unknown" else datetime.min,
        reverse=True,
    )

    # --- Detailed fines list ---
    cur.execute(
        """
        SELECT date, amount, description, note
        FROM accountlines
        WHERE borrowernumber = %s
        ORDER BY date DESC
        LIMIT 50
        """,
        (borrowernumber,),
    )
    fines_list = cur.fetchall() or []

    cur.close(); conn.close()

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
    student["BorrowedBooksGrouped"] = grouped

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
    return render_template("student.html", found=True, info=info, hide_nav=True)

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
    """Export student profile to PDF with Unicode font + Arabic shaping."""
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

    # 3) Shape title (Arabic joining + bidi)
    title_txt = _shape_if_rtl(f"Student Report - {info.get('FullName') or ''}")
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

    summary_data = [
        [S("📚 Lifetime Issues"), S(met["LifetimeIssues"])],
        [S("🗓️ AY Issues"), S(met["AYIssues"])],
        [S("📖 Active Loans"), S(met["ActiveLoans"])],
        [S("⏰ Overdue Now"), S(met["OverdueNow"])],
        [S("💰 Total Fines Paid"), S(f"{met['TotalFinesPaid']:.2f}")],
        [S("💰 Fines Paid (AY)"), S(f"{met['FinesPaidAY']:.2f}")],
        [S("💳 Outstanding Balance"), S(f"{met['OutstandingBalance']:.2f}")],
        [S("🕒 Last Issue"), S(met["LastIssueDate"] or "-")],
        [S("🕒 Last Return"), S(met["LastReturnDate"] or "-")],
        [S("👤 Class Teacher"), S(info.get("ClassTeacher") or "-")],
        [S("🏷️ Class"), S(info.get("Class") or "-")],
        [S("🧾 TR Number"), S(info.get("TRNumber") or "-")],
        [S("🏢 Department"), S(info.get("Department") or "-")],
    ]
    summary_table = Table(summary_data, colWidths=[6*cm, 6*cm])
    summary_table.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, -1), font_name),  # ensure our font is used in the table
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
    ]))
    elements.append(summary_table)
    elements.append(Spacer(1, 0.5 * cm))

    # Section header (shaped)
    elements.append(Paragraph(S("Borrowed Books (Grouped by Month)"), styles["Heading2"]))

    # Borrowed books tables
    if info["BorrowedBooksGrouped"]:
        for month, books in info["BorrowedBooksGrouped"]:
            elements.append(Paragraph(S(f"📅 {month}"), styles["Heading3"]))
            books_data = [[S("Title"), S("Date Issued"), S("Due Date"), S("Returned"), S("Overdue?")]]
            for b in books:
                books_data.append([
                    S(b.get("title", "N/A")),
                    S(b.get("date_issued") or "N/A"),
                    S(b.get("date_due") or "N/A"),
                    S("Yes" if b.get("returned") else "No"),
                    S("Overdue") if b.get("overdue") else S("On Time"),
                ])
            books_table = Table(books_data, repeatRows=1, colWidths=[6*cm, 3*cm, 3*cm, 2*cm, 2*cm])
            books_table.setStyle(TableStyle([
                ("FONTNAME", (0, 0), (-1, -1), font_name),  # use our font here too
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ]))
            elements.append(books_table)
            elements.append(Spacer(1, 0.3 * cm))
    else:
        elements.append(Paragraph(S("No borrowed books found."), styles["Normal"]))

    # Favorite authors brief
    fav = met.get("FavoriteAuthors") or []
    if fav:
        elements.append(Spacer(1, 0.3 * cm))
        elements.append(Paragraph(S("Favorite Authors (last 12 months): ") + S(", ".join(fav)), styles["Normal"]))

    doc.build(elements)
    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name=f"student_report_{identifier}.pdf",
        mimetype="application/pdf",
    )
