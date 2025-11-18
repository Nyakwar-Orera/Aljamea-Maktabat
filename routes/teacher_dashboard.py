# routes/teacher_dashboard.py
from flask import (
    Blueprint, render_template, session, redirect, url_for,
    flash, current_app, request, send_file
)
from datetime import date
from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Table, TableStyle,
    Spacer, Image, PageBreak
)
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from services.koha_queries import top_titles
from services.exports import _ensure_font_registered, _shape_if_rtl
from routes.reports import class_report
from routes.students import get_student_info
from db_koha import get_koha_conn
import pandas as pd
import os


bp = Blueprint("teacher_dashboard_bp", __name__)


# --------------------------------------------------
# TEACHER DASHBOARD (class summary + KPIs)
# --------------------------------------------------
@bp.route("/")
def dashboard():
    if not session.get("logged_in") or session.get("role") != "teacher":
        return redirect(url_for("auth_bp.login"))

    class_name = session.get("class_name")
    username = session.get("username")
    if not class_name:
        flash("⚠️ Your account is not linked to any class.", "warning")
        return render_template("teacher_dashboard.html")

    df = class_report(class_name)
    if df.empty:
        flash(f"No records found for {class_name}. Falling back to AJSN branch.", "info")
        df = class_report("AJSN")

    total_students = len(df)
    total_issues = int(df.get("Issues_AY", pd.Series(dtype=int)).sum())
    total_fines = float(df.get("FinesPaid_AY", pd.Series(dtype=float)).sum())
    total_overdues = int(df.get("Overdues", pd.Series(dtype=int)).sum())
    active_loans = int(df.get("ActiveLoans", pd.Series(dtype=int)).sum())

    top_students = (
        df.sort_values("Issues_AY", ascending=False)
          .head(10)
          .reset_index(drop=True)
          .to_dict("records")
    )

    # ---------- Daily borrowing trend ----------
    today = date.today()
    start = date(today.year, today.month, 1)
    end = today

    conn = get_koha_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT DATE(issuedate), COUNT(*) FROM issues
        WHERE issuedate BETWEEN %s AND %s
        GROUP BY DATE(issuedate)
        ORDER BY DATE(issuedate);
    """, (start, end))
    issue_rows = cur.fetchall()

    cur.execute("""
        SELECT DATE(returndate), COUNT(*) FROM issues
        WHERE returndate BETWEEN %s AND %s
        GROUP BY DATE(returndate)
        ORDER BY DATE(returndate);
    """, (start, end))
    return_rows = cur.fetchall()

    cur.close()
    conn.close()

    issue_df = pd.DataFrame(issue_rows, columns=["day", "issues"]) if issue_rows else pd.DataFrame(columns=["day", "issues"])
    return_df = pd.DataFrame(return_rows, columns=["day", "returns"]) if return_rows else pd.DataFrame(columns=["day", "returns"])

    trend_df = pd.DataFrame({"day": pd.date_range(start, end)})
    for df_ in (issue_df, return_df):
        if not df_.empty:
            df_["day"] = pd.to_datetime(df_["day"])
    trend_df = trend_df.merge(issue_df, on="day", how="left").merge(return_df, on="day", how="left")
    trend_df.fillna(0, inplace=True)
    trend_df["day_str"] = trend_df["day"].dt.strftime("%d %b")
    daily_trend = trend_df[["day_str", "issues", "returns"]].values.tolist()

    top_arabic = top_titles(limit=10, arabic=True)
    top_non_arabic = top_titles(limit=10, non_arabic=True)

    label = f"{username} ({class_name})"
    return render_template(
        "teacher_dashboard.html",
        class_name=label,
        total_students=total_students,
        total_issues=total_issues,
        total_fines=total_fines,
        total_overdues=total_overdues,
        active_loans=active_loans,
        top_students=top_students,
        top_arabic=top_arabic,
        top_non_arabic=top_non_arabic,
        daily_trend=daily_trend,
        current_month=today.strftime("%B %Y"),
        students=df.to_dict(orient="records"),
    )


# --------------------------------------------------
# SEARCH STUDENT
# --------------------------------------------------
@bp.route("/search_student")
def search_student():
    if not session.get("logged_in") or session.get("role") != "teacher":
        return redirect(url_for("auth_bp.login"))

    class_name = session.get("class_name")
    username = session.get("username")
    query = (request.args.get("q") or "").strip()
    if not query:
        return redirect(url_for("teacher_dashboard_bp.dashboard"))

    df = class_report(class_name)
    results = df[
        df["FullName"].str.contains(query, case=False, na=False)
        | df["TRNumber"].astype(str).str.contains(query, case=False, na=False)
    ]
    return render_template(
        "teacher_dashboard_search.html",
        class_name=f"{username} ({class_name})",
        query=query,
        results=results.to_dict(orient="records"),
    )


# --------------------------------------------------
# UNIFIED DOWNLOAD HANDLER (class or student)
# --------------------------------------------------
@bp.route("/download_report")
def download_report():
    """Handles both class and student PDF generation based on 'scope' param."""
    scope = request.args.get("scope", "")
    fmt = request.args.get("fmt", "pdf")

    if fmt != "pdf":
        flash("Only PDF downloads are supported.", "warning")
        return redirect(url_for("teacher_dashboard_bp.dashboard"))

    # student:27295 → individual report
    if scope.startswith("student:"):
        identifier = scope.split("student:")[-1]
        return redirect(url_for("teacher_dashboard_bp.download_student_pdf", identifier=identifier))

    # otherwise → full class report
    return download_class_pdf()


# --------------------------------------------------
# CLASS PDF REPORT
# --------------------------------------------------
@bp.route("/download_class/pdf")
def download_class_pdf():
    """Generate a visually polished full class report — professional layout with photos and branding."""
    if not session.get("logged_in") or session.get("role") != "teacher":
        return redirect(url_for("auth_bp.login"))

    class_name = session.get("class_name")
    df = class_report(class_name)
    if df.empty:
        flash("⚠️ No data found for your class.", "warning")
        return redirect(url_for("teacher_dashboard_bp.dashboard"))

    # Skip missing name/TR rows
    df = df[df["FullName"].notna() & df["TRNumber"].notna()]
    if df.empty:
        flash("⚠️ No valid student records found for your class.", "warning")
        return redirect(url_for("teacher_dashboard_bp.dashboard"))

    from datetime import datetime
    font_name = _ensure_font_registered()
    buffer = BytesIO()

    # --- Setup styles ---
    from reportlab.lib.enums import TA_CENTER
    styles = getSampleStyleSheet()
    for key in ("Title", "Normal", "Heading2", "Heading3"):
        styles[key].fontName = font_name
    styles.add(ParagraphStyle(name="CenterTitle", alignment=TA_CENTER, fontName=font_name, fontSize=16, leading=20))
    styles.add(ParagraphStyle(name="SectionHeader", fontName=font_name, fontSize=12, textColor=colors.HexColor("#004080")))
    styles.add(ParagraphStyle(name="Small", fontName=font_name, fontSize=9))
    styles.add(ParagraphStyle(name="Tiny", fontName=font_name, fontSize=8, textColor=colors.grey))
    S = lambda x: _shape_if_rtl(str(x) if x is not None else "-")

    # --- PDF layout setup ---
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=2 * cm,
        rightMargin=2 * cm,
        topMargin=2 * cm,
        bottomMargin=2 * cm,
        title=f"Class Report - {class_name}",
    )

    def footer(canvas, _doc):
        canvas.saveState()
        canvas.setFont(font_name, 8)
        page_str = f"Page {_doc.page}"
        date_str = datetime.now().strftime("%d %b %Y")
        canvas.drawRightString(A4[0] - 2 * cm, 1.5 * cm, page_str)
        canvas.drawString(2 * cm, 1.5 * cm, date_str)
        canvas.restoreState()

    elements = []

    # --- Cover Page ---
    import os
    logo_path = os.path.join(current_app.root_path, "static", "images", "logo.png")
    if os.path.exists(logo_path):
        img = Image(logo_path)
        img._restrictSize(5 * cm, 5 * cm)
        elements.append(img)
        elements.append(Spacer(1, 0.3 * cm))

    elements.append(Paragraph(S("Al-Jamea tus-Saifiyah • Maktabat"), styles["CenterTitle"]))
    elements.append(Spacer(1, 0.1 * cm))
    elements.append(Paragraph(S("📘 Monthly Class Library Report"), styles["CenterTitle"]))
    elements.append(Spacer(1, 0.3 * cm))
    elements.append(Paragraph(S(f"Class: {class_name}"), styles["CenterTitle"]))
    elements.append(Spacer(1, 0.1 * cm))
    elements.append(Paragraph(datetime.now().strftime("%d %B %Y"), styles["CenterTitle"]))
    elements.append(Spacer(1, 1 * cm))
    elements.append(
        Paragraph(
            S("This report provides a detailed breakdown of student borrowing activity, fines, and engagement metrics for the current academic period."),
            styles["Small"],
        )
    )
    elements.append(PageBreak())

    # --- Class Summary ---
    total_students = len(df)
    total_issues = int(df.get("Issues_AY", 0).sum())
    total_fines = float(df.get("FinesPaid_AY", 0).sum())
    total_overdues = int(df.get("Overdues", 0).sum())
    summary_table = Table(
        [
            ["Total Students", total_students],
            ["Total Issues (AY)", total_issues],
            ["Total Overdues", total_overdues],
            ["Total Fines Paid", f"{total_fines:.2f}"],
        ],
        colWidths=[7 * cm, 7 * cm],
    )
    summary_table.setStyle(
        TableStyle(
            [
                ("FONTNAME", (0, 0), (-1, -1), font_name),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.grey),
                ("BACKGROUND", (0, 0), (-1, -1), colors.whitesmoke),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#004080")),
                ("FONTSIZE", (0, 0), (-1, -1), 10),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    elements.append(Paragraph(S("📊 Class Summary"), styles["Heading2"]))
    elements.append(summary_table)
    elements.append(Spacer(1, 0.6 * cm))

    # --- Each Student Section ---
    for _, row in df.iterrows():
        trno = str(row.get("TRNumber", "")).strip()
        if not trno:
            continue
        info = get_student_info(trno)
        if not info or not info.get("FullName"):
            continue

        elements.append(Paragraph(S(f"👤 {info.get('FullName')} ({trno})"), styles["SectionHeader"]))
        elements.append(Spacer(1, 0.15 * cm))

        # --- Photo
        photo_rel = info.get("Photo") or "images/avatar.png"
        photo_path = os.path.join(current_app.root_path, "static", photo_rel)
        if not os.path.exists(photo_path):
            photo_path = os.path.join(current_app.root_path, "static", "images", "avatar.png")
        if os.path.exists(photo_path):
            photo_img = Image(photo_path)
            photo_img._restrictSize(3.2 * cm, 3.2 * cm)
        else:
            photo_img = None

        # --- Metrics table
        met = info.get("Metrics", {})
        summary_data = [
            ["Lifetime Issues", met.get("LifetimeIssues", 0)],
            ["AY Issues", met.get("AYIssues", 0)],
            ["Active Loans", met.get("ActiveLoans", 0)],
            ["Overdue Now", met.get("OverdueNow", 0)],
            ["Total Fines Paid", f"{met.get('TotalFinesPaid', 0):.2f}"],
            ["Outstanding Balance", f"{met.get('OutstandingBalance', 0):.2f}"],
            ["Last Issue", met.get("LastIssueDate") or "-"],
            ["Last Return", met.get("LastReturnDate") or "-"],
            ["Department", info.get("Department") or "-"],
            ["Class", info.get("Class") or "-"],
        ]
        t = Table(summary_data, colWidths=[6 * cm, 6 * cm])
        t.setStyle(
            TableStyle(
                [
                    ("FONTNAME", (0, 0), (-1, -1), font_name),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.lightgrey),
                    ("BACKGROUND", (0, 0), (-1, -1), colors.whitesmoke),
                    ("FONTSIZE", (0, 0), (-1, -1), 8),
                    ("TOPPADDING", (0, 0), (-1, -1), 4),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ]
            )
        )

        if photo_img:
            layout = Table([[photo_img, t]], colWidths=[3.5 * cm, 10.5 * cm])
            layout.setStyle(
                TableStyle(
                    [
                        ("VALIGN", (0, 0), (-1, -1), "TOP"),
                        ("LEFTPADDING", (0, 0), (-1, -1), 0),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                        ("TOPPADDING", (0, 0), (-1, -1), 0),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
                    ]
                )
            )
            elements.append(layout)
        else:
            elements.append(t)

        elements.append(Spacer(1, 0.25 * cm))

        # --- Borrowed Books
        borrowed = info.get("BorrowedBooks", [])
        elements.append(Paragraph(S("📖 Borrowed Books"), styles["Heading3"]))
        if borrowed:
            books_data = [["Title", "Issued", "Due", "Returned", "Status"]]
            for b in borrowed[:15]:
                books_data.append([
                    S(b.get("title", "N/A")),
                    S(b.get("date_issued") or "-"),
                    S(b.get("date_due") or "-"),
                    S("Yes" if b.get("returned") else "No"),
                    S("Overdue" if b.get("overdue") else "On Time"),
                ])
            book_table = Table(books_data, repeatRows=1, colWidths=[6*cm, 3*cm, 3*cm, 2*cm, 2*cm])
            book_table.setStyle(TableStyle([
                ("FONTNAME", (0, 0), (-1, -1), font_name),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#004080")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.lightgrey]),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
            ]))
            elements.append(book_table)
        else:
            elements.append(Paragraph(S("No borrowed books."), styles["Normal"]))

        elements.append(Spacer(1, 0.4 * cm))
        elements.append(Paragraph("<hr width='100%' color='#cccccc'/>", styles["Normal"]))
        elements.append(Spacer(1, 0.4 * cm))

    doc.build(elements, onFirstPage=footer, onLaterPages=footer)
    buffer.seek(0)

    return send_file(
        buffer,
        as_attachment=True,
        download_name=f"class_report_{class_name}.pdf",
        mimetype="application/pdf",
    )


# --------------------------------------------------
# INDIVIDUAL STUDENT PDF (DETAILED)
# --------------------------------------------------
@bp.route("/download/student/<identifier>")
def download_student_pdf(identifier):
    """Generate detailed PDF report for an individual student."""
    if not session.get("logged_in") or session.get("role") != "teacher":
        return redirect(url_for("auth_bp.login"))

    info = get_student_info(identifier)
    if not info:
        flash("⚠️ Student not found.", "warning")
        return redirect(url_for("teacher_dashboard_bp.dashboard"))

    font_name = _ensure_font_registered()
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    styles = getSampleStyleSheet()
    for key in ("Title", "Normal", "Heading2", "Heading3"):
        styles[key].fontName = font_name
    S = lambda x: _shape_if_rtl(str(x) if x is not None else "-")

    elements = []

    # Header
    elements.append(Paragraph(S(f"Student Report - {info.get('FullName','')}"), styles["Title"]))
    elements.append(Spacer(1, 0.3 * cm))

    # Photo (with fallback)
    photo_path = os.path.join(current_app.root_path, "static", info.get("Photo", "images/avatar.png"))
    if not os.path.exists(photo_path):
        photo_path = os.path.join(current_app.root_path, "static", "images", "avatar.png")
    if os.path.exists(photo_path):
        img = Image(photo_path)
        img._restrictSize(5 * cm, 5 * cm)
        elements.append(img)
        elements.append(Spacer(1, 0.3 * cm))

    # Summary table
    met = info["Metrics"]
    summary_data = [
        ["📚 Lifetime Issues", met.get("LifetimeIssues", 0)],
        ["🗓️ AY Issues", met.get("AYIssues", 0)],
        ["📖 Active Loans", met.get("ActiveLoans", 0)],
        ["⏰ Overdue Now", met.get("OverdueNow", 0)],
        ["💰 Total Fines Paid", f"{met.get('TotalFinesPaid', 0):.2f}"],
        ["💳 Outstanding Balance", f"{met.get('OutstandingBalance', 0):.2f}"],
        ["🕒 Last Issue", met.get("LastIssueDate") or "-"],
        ["🕒 Last Return", met.get("LastReturnDate") or "-"],
        ["👤 Class Teacher", info.get("ClassTeacher") or "-"],
        ["🏷️ Class", info.get("Class") or "-"],
        ["🧾 TR Number", info.get("TRNumber") or "-"],
        ["🏢 Department", info.get("Department") or "-"],
    ]
    t = Table(summary_data, colWidths=[6 * cm, 6 * cm])
    t.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, -1), font_name),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
    ]))
    elements.append(t)
    elements.append(Spacer(1, 0.4 * cm))

    # Borrowed Books Section
    elements.append(Paragraph(S("📖 Borrowed Books (Grouped by Month)"), styles["Heading2"]))
    if info.get("BorrowedBooksGrouped"):
        for month, books in info["BorrowedBooksGrouped"]:
            elements.append(Paragraph(S(f"📅 {month}"), styles["Heading3"]))
            book_data = [["Title", "Issued", "Due", "Returned", "Status"]]
            for b in books:
                book_data.append([
                    S(b.get("title", "N/A")),
                    S(b.get("date_issued") or "-"),
                    S(b.get("date_due") or "-"),
                    S("Yes" if b.get("returned") else "No"),
                    S("Overdue" if b.get("overdue") else "On Time"),
                ])
            book_table = Table(book_data, repeatRows=1, colWidths=[6*cm, 3*cm, 3*cm, 2*cm, 2*cm])
            book_table.setStyle(TableStyle([
                ("FONTNAME", (0, 0), (-1, -1), font_name),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
            ]))
            elements.append(book_table)
            elements.append(Spacer(1, 0.3 * cm))
    else:
        elements.append(Paragraph(S("No borrowed books found."), styles["Normal"]))

    # Favorite authors
    fav = met.get("FavoriteAuthors") or []
    if fav:
        elements.append(Spacer(1, 0.3 * cm))
        elements.append(Paragraph(S("Favorite Authors (last 12 months): ") + S(", ".join(fav)), styles["Normal"]))

    # Fines list (if available)
    fines = info.get("FinesList") or []
    if fines:
        elements.append(PageBreak())
        elements.append(Paragraph(S("💰 Recent Fines and Payments"), styles["Heading2"]))
        fine_table = [["Date", "Amount", "Description", "Note"]]
        for f in fines:
            fine_table.append([
                S(f.get("date", "-")),
                S(f.get("amount", "-")),
                S(f.get("description", "-")),
                S(f.get("note", "-")),
            ])
        ft = Table(fine_table, repeatRows=1, colWidths=[3*cm, 2*cm, 5*cm, 4*cm])
        ft.setStyle(TableStyle([
            ("FONTNAME", (0, 0), (-1, -1), font_name),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
        ]))
        elements.append(ft)

    doc.build(elements)
    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name=f"student_report_{identifier}.pdf",
        mimetype="application/pdf",
    )
