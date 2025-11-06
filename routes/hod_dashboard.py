# routes/hod_dashboard.py
from flask import Blueprint, render_template, session, redirect, url_for, flash, current_app, request, send_file
from routes.reports import department_report, class_report
from routes.students import get_student_info
from services.koha_queries import top_titles
from db_koha import get_koha_conn
from datetime import date, timedelta
from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from services.exports import _ensure_font_registered, dataframe_to_pdf_bytes
import os

bp = Blueprint("hod_dashboard_bp", __name__)

# --------------------------------------------------
# AY RANGE  (April 1 → Today; if before April, use last AY)
# --------------------------------------------------
def _ay_bounds():
    today = date.today()
    year = today.year
    if today.month < 4:
        return date(year - 1, 4, 1), today
    return date(year, 4, 1), today


# --------------------------------------------------
# KPIs (Department)
# --------------------------------------------------
def get_department_kpis(department):
    """Department-level KPIs (borrowers, issues, fines, overdues, active loans)."""
    start, end = _ay_bounds()

    conn = get_koha_conn()
    cur = conn.cursor()

    # Make the filter forgiving: match on categorycode OR description (LIKE)
    like = f"%{department}%"

    # Borrowers
    cur.execute("""
        SELECT COUNT(DISTINCT b.borrowernumber)
        FROM borrowers b
        LEFT JOIN categories c ON b.categorycode = c.categorycode
        WHERE (b.categorycode LIKE %s OR c.description LIKE %s);
    """, (like, like))
    total_borrowers = int(cur.fetchone()[0] or 0)

    # Issues (AY)
    cur.execute("""
        SELECT COUNT(*) 
        FROM statistics s
        JOIN borrowers b ON s.borrowernumber = b.borrowernumber
        LEFT JOIN categories c ON b.categorycode = c.categorycode
        WHERE s.type='issue'
          AND (b.categorycode LIKE %s OR c.description LIKE %s)
          AND DATE(s.`datetime`) BETWEEN %s AND %s;
    """, (like, like, start, end))
    total_issues = int(cur.fetchone()[0] or 0)

    # Fines paid (AY) — payments stored as negatives; flip sign
    cur.execute("""
        SELECT COALESCE(SUM(
            CASE
              WHEN a.credit_type_code='PAYMENT'
                   AND (a.status IS NULL OR a.status <> 'VOID')
                   AND DATE(a.`date`) BETWEEN %s AND %s
              THEN -a.amount ELSE 0 END
        ),0)
        FROM accountlines a
        JOIN borrowers b ON a.borrowernumber = b.borrowernumber
        LEFT JOIN categories c ON b.categorycode = c.categorycode
        WHERE (b.categorycode LIKE %s OR c.description LIKE %s);
    """, (start, end, like, like))
    total_fines = float(cur.fetchone()[0] or 0.0)

    # Active loans (now)
    cur.execute("""
        SELECT COUNT(*)
        FROM issues i
        JOIN borrowers b ON i.borrowernumber = b.borrowernumber
        LEFT JOIN categories c ON b.categorycode = c.categorycode
        WHERE (b.categorycode LIKE %s OR c.description LIKE %s)
          AND i.returndate IS NULL;
    """, (like, like))
    active_loans = int(cur.fetchone()[0] or 0)

    # Overdues (now)
    cur.execute("""
        SELECT COUNT(*)
        FROM issues i
        JOIN borrowers b ON i.borrowernumber = b.borrowernumber
        LEFT JOIN categories c ON b.categorycode = c.categorycode
        WHERE (b.categorycode LIKE %s OR c.description LIKE %s)
          AND i.date_due < CURDATE()
          AND i.returndate IS NULL;
    """, (like, like))
    overdues = int(cur.fetchone()[0] or 0)

    conn.close()
    return total_borrowers, total_issues, total_fines, active_loans, overdues


# --------------------------------------------------
# DAILY TREND (current month)  — mirrors admin style, returns continuous days
# --------------------------------------------------
def get_monthly_trend(department):
    today = date.today()
    month_start = date(today.year, today.month, 1)
    month_end = today

    conn = get_koha_conn()
    cur = conn.cursor()
    like = f"%{department}%"

    cur.execute("""
        SELECT DATE(s.`datetime`) AS day, COUNT(*) AS cnt
        FROM statistics s
        JOIN borrowers b ON s.borrowernumber = b.borrowernumber
        LEFT JOIN categories c ON b.categorycode = c.categorycode
        WHERE s.type = 'issue'
          AND (b.categorycode LIKE %s OR c.description LIKE %s)
          AND DATE(s.`datetime`) BETWEEN %s AND %s
        GROUP BY day
        ORDER BY day;
    """, (like, like, month_start, month_end))
    rows = cur.fetchall()
    conn.close()

    # Build a continuous series
    by_day = {r[0]: int(r[1]) for r in rows}
    labels, values = [], []
    d = month_start
    while d <= month_end:
        labels.append(d.strftime("%d %b"))
        values.append(by_day.get(d, 0))
        d += timedelta(days=1)

    # Fallback: if still empty, show last 7 days with zeros (so Chart shows axes)
    if not labels:
        d = today - timedelta(days=6)
        while d <= today:
            labels.append(d.strftime("%d %b"))
            values.append(0)
            d += timedelta(days=1)

    return labels, values


# --------------------------------------------------
# CLASS BREAKDOWN (AY) — same philosophy as admin: group + return arrays
# --------------------------------------------------
def get_class_breakdown(department):
    start, end = _ay_bounds()
    like = f"%{department}%"

    conn = get_koha_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT COALESCE(std.attribute, 'Unknown') AS class_name, COUNT(*) AS cnt
        FROM statistics s
        JOIN borrowers b ON s.borrowernumber = b.borrowernumber
        LEFT JOIN borrower_attributes std
               ON std.borrowernumber = b.borrowernumber
              AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
        LEFT JOIN categories c ON b.categorycode = c.categorycode
        WHERE s.type='issue'
          AND (b.categorycode LIKE %s OR c.description LIKE %s)
          AND DATE(s.`datetime`) BETWEEN %s AND %s
        GROUP BY class_name
        ORDER BY cnt DESC;
    """, (like, like, start, end))
    rows = cur.fetchall()
    conn.close()

    labels = [r[0] for r in rows]
    values = [int(r[1]) for r in rows]

    # Fallback so the bar chart has axes if no data
    if not labels:
        labels, values = ["—"], [0]

    return labels, values


# --------------------------------------------------
# HOD DASHBOARD
# --------------------------------------------------
@bp.route("/")
def dashboard():
    if not session.get("logged_in") or session.get("role") != "hod":
        return redirect(url_for("auth_bp.login"))

    dept_name = session.get("department_name")
    username = session.get("username")
    if not dept_name:
        flash("⚠️ No department assigned to your account.", "warning")
        return redirect(url_for("auth_bp.login"))

    # KPIs & charts
    total_borrowers, total_issues, total_fines, active_loans, overdues = get_department_kpis(dept_name)
    trend_labels, trend_values = get_monthly_trend(dept_name)
    class_labels, class_values = get_class_breakdown(dept_name)

    # Leaderboards (reuse your service)
    top_ar = top_titles(limit=10, arabic=True)
    top_non_ar = top_titles(limit=10, non_arabic=True)

    # Department summary table (group by class)
    df = department_report(dept_name)
    summary_table = []
    if not df.empty:
        for col in ("ClassName", "Class", "STD", "class"):
            if col in df.columns:
                summary_table = (
                    df.groupby(col)[["Issues_AY", "FinesPaid_AY", "ActiveLoans", "Overdues"]]
                    .sum()
                    .reset_index()
                    .to_dict("records")
                )
                break

    # Quick class KPIs used by the right list
    class_kpis = {
        "Total Classes": len(class_labels) if class_labels else 0,
        "Top Class Issues": max(class_values) if class_values else 0,
        "Average Issues per Class": round(sum(class_values)/len(class_values), 1) if class_values else 0
    }

    # Handy server-side log for sanity checks
    print("📆 Trend sample:", trend_labels[:5], trend_values[:5])
    print("🏫 Class sample:", class_labels[:5], class_values[:5])

    return render_template(
        "hod_dashboard.html",
        dept_name=dept_name,
        username=username,
        total_borrowers=total_borrowers,
        total_issues=total_issues,
        total_fines=total_fines,
        active_loans=active_loans,
        overdues=overdues,
        trend_labels=trend_labels,
        trend_values=trend_values,
        class_labels=class_labels,
        class_values=class_values,
        top_arabic=top_ar,
        top_non_arabic=top_non_ar,
        summary_table=summary_table,
        class_kpis=class_kpis
    )


# --------------------------------------------------
# SEARCH
# --------------------------------------------------
@bp.route("/search")
def search():
    if not session.get("logged_in") or session.get("role") != "hod":
        return redirect(url_for("auth_bp.login"))

    query = (request.args.get("q") or "").strip()
    dept_name = session.get("department_name")

    if not query:
        return redirect(url_for("hod_dashboard_bp.dashboard"))

    df = department_report(dept_name)
    if df.empty:
        flash("⚠️ No records found for your department.", "warning")
        return redirect(url_for("hod_dashboard_bp.dashboard"))

    students, classes = [], []

    if "FullName" in df.columns:
        students = df[
            df["FullName"].str.contains(query, case=False, na=False)
            | df["TRNumber"].astype(str).str.contains(query, case=False, na=False)
        ].to_dict("records")

    if "Class" in df.columns:
        class_df = (
            df[df["Class"].astype(str).str.contains(query, case=False, na=False)]
            .groupby("Class")[["Issues_AY", "FinesPaid_AY", "ActiveLoans", "Overdues"]]
            .agg("sum")
            .reset_index()
            .assign(StudentCount=lambda x: x["Class"].map(df["Class"].value_counts()))
        )
        classes = class_df.to_dict("records")

    return render_template(
        "hod_search_results.html",
        dept_name=dept_name,
        query=query,
        students=students,
        classes=classes
    )


# --------------------------------------------------
# DOWNLOADS
# --------------------------------------------------
@bp.route("/download/department/pdf")
def download_department_pdf():
    if not session.get("logged_in") or session.get("role") != "hod":
        return redirect(url_for("auth_bp.login"))

    dept_name = session.get("department_name")
    df = department_report(dept_name)
    if df.empty:
        flash("⚠️ No data found for your department.", "warning")
        return redirect(url_for("hod_dashboard_bp.dashboard"))

    pdf_bytes = dataframe_to_pdf_bytes(f"Department Report - {dept_name}", df)
    return send_file(
        BytesIO(pdf_bytes),
        as_attachment=True,
        download_name=f"department_report_{dept_name}.pdf",
        mimetype="application/pdf",
    )


@bp.route("/download/class/<class_name>")
def download_class_pdf(class_name):
    if not session.get("logged_in") or session.get("role") != "hod":
        return redirect(url_for("auth_bp.login"))

    df = class_report(class_name)
    if df.empty:
        flash(f"⚠️ No data found for class {class_name}.", "warning")
        return redirect(url_for("hod_dashboard_bp.dashboard"))

    pdf_bytes = dataframe_to_pdf_bytes(f"Class Report - {class_name}", df)
    return send_file(
        BytesIO(pdf_bytes),
        as_attachment=True,
        download_name=f"class_report_{class_name}.pdf",
        mimetype="application/pdf",
    )


@bp.route("/download/student/<identifier>")
def download_student_pdf(identifier):
    """Download individual student report with photo."""
    if not session.get("logged_in") or session.get("role") != "hod":
        return redirect(url_for("auth_bp.login"))

    info = get_student_info(identifier)
    if not info:
        flash("⚠️ Student not found.", "warning")
        return redirect(url_for("hod_dashboard_bp.dashboard"))

    font_name = _ensure_font_registered()
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    styles = getSampleStyleSheet()
    for key in ("Title", "Normal", "Heading2", "Heading3"):
        styles[key].fontName = font_name

    elements = [Paragraph(f"Student Report - {info.get('FullName','')}", styles["Title"]), Spacer(1, 0.3 * cm)]

    photo_path = os.path.join(current_app.root_path, "static", info.get("Photo", "images/avatar.png"))
    if not os.path.exists(photo_path):
        photo_path = os.path.join(current_app.root_path, "static", "images", "avatar.png")

    if os.path.exists(photo_path):
        elements.append(Image(photo_path, width=4 * cm, height=4 * cm))
        elements.append(Spacer(1, 0.3 * cm))

    metrics = info.get("Metrics", {})
    data = [
        ["📚 Lifetime Issues", metrics.get("LifetimeIssues", 0)],
        ["🗓️ AY Issues", metrics.get("AYIssues", 0)],
        ["📖 Active Loans", metrics.get("ActiveLoans", 0)],
        ["⏰ Overdue Now", metrics.get("OverdueNow", 0)],
        ["💰 Fines Paid", f"{metrics.get('TotalFinesPaid', 0):.2f}"],
        ["🏷️ Class", info.get("Class") or "-"],
        ["🧾 TR Number", info.get("TRNumber") or "-"],
    ]
    t = Table(data, colWidths=[6 * cm, 6 * cm])
    t.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("BACKGROUND", (0, 0), (-1, 0), colors.whitesmoke),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    elements.append(t)

    doc.build(elements)
    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name=f"student_report_{identifier}.pdf",
        mimetype="application/pdf",
    )
