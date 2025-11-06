from flask import Blueprint, render_template, session, redirect, url_for, flash, current_app, request, send_file
from routes.reports import class_report
from services.koha_queries import top_titles
from db_koha import get_koha_conn
from datetime import date
from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from services.exports import _ensure_font_registered, _shape_if_rtl
from routes.students import get_student_info
import pandas as pd
import os

bp = Blueprint("teacher_dashboard_bp", __name__)

# --------------------------------------------------
# MAIN DASHBOARD  → daily issues/returns for current month
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

    # ---------- Class Data ----------
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

    # ---------- Current Month Daily Trend ----------
    today = date.today()
    start = date(today.year, today.month, 1)
    end = today

    conn = get_koha_conn()
    cur = conn.cursor()

    # Issues
    cur.execute(
        """
        SELECT DATE(issuedate) AS day, COUNT(*) AS cnt
        FROM issues
        WHERE issuedate BETWEEN %s AND %s
        GROUP BY day ORDER BY day;
        """,
        (start, end),
    )
    issue_rows = cur.fetchall()
    issue_df = pd.DataFrame(issue_rows, columns=["day", "issues"]) if issue_rows else pd.DataFrame(columns=["day","issues"])

    # Returns
    cur.execute(
        """
        SELECT DATE(returndate) AS day, COUNT(*) AS cnt
        FROM issues
        WHERE returndate BETWEEN %s AND %s
        GROUP BY day ORDER BY day;
        """,
        (start, end),
    )
    return_rows = cur.fetchall()
    return_df = pd.DataFrame(return_rows, columns=["day","returns"]) if return_rows else pd.DataFrame(columns=["day","returns"])

    cur.close(); conn.close()

    # ✅ Merge → fill missing days (type-safe)
    all_days = pd.date_range(start, end)
    trend_df = pd.DataFrame({"day": all_days})

    for df_ in (issue_df, return_df):
        if not df_.empty:
            df_["day"] = pd.to_datetime(df_["day"])

    trend_df = (
        trend_df.merge(issue_df, on="day", how="left")
                .merge(return_df, on="day", how="left")
    )
    trend_df.fillna(0, inplace=True)
    trend_df["day_str"] = trend_df["day"].dt.strftime("%d %b")  # e.g. "01 Nov"

    daily_trend = trend_df[["day_str","issues","returns"]].values.tolist()

    # ---------- Top Titles ----------
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
# DOWNLOAD CLASS PDF
# --------------------------------------------------
@bp.route("/download_class/pdf")
def download_class_pdf():
    if not session.get("logged_in") or session.get("role") != "teacher":
        return redirect(url_for("auth_bp.login"))

    class_name = session.get("class_name")
    username = session.get("username")
    df = class_report(class_name)
    if df.empty:
        flash("⚠️ No data found for your class.", "warning")
        return redirect(url_for("teacher_dashboard_bp.dashboard"))

    from services.exports import dataframe_to_pdf_bytes
    pdf_bytes = dataframe_to_pdf_bytes(f"Class Report - {username} ({class_name})", df)
    return send_file(
        BytesIO(pdf_bytes),
        as_attachment=True,
        download_name=f"class_report_{username}_{class_name}.pdf",
        mimetype="application/pdf",
    )

# --------------------------------------------------
# DOWNLOAD INDIVIDUAL STUDENT PDF (with photo)
# --------------------------------------------------
@bp.route("/download/student/<identifier>")
def download_student_pdf(identifier):
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

    elements = [Paragraph(S(f"Student Report - {info.get('FullName','')}"), styles["Title"]), Spacer(1,0.3*cm)]

    photo_path = os.path.join(current_app.root_path,"static",info.get("Photo","images/avatar.png"))
    if not os.path.exists(photo_path):
        photo_path=os.path.join(current_app.root_path,"static","images","avatar.png")
    if os.path.exists(photo_path):
        elements.append(Image(photo_path,width=4*cm,height=4*cm))
        elements.append(Spacer(1,0.3*cm))

    met = info.get("Metrics",{})
    summary_data=[
        ["📚 Lifetime Issues",met.get("LifetimeIssues",0)],
        ["🗓️ AY Issues",met.get("AYIssues",0)],
        ["📖 Active Loans",met.get("ActiveLoans",0)],
        ["⏰ Overdue Now",met.get("OverdueNow",0)],
        ["💰 Total Fines Paid",f"{met.get('TotalFinesPaid',0):.2f}"],
        ["💳 Outstanding",f"{met.get('OutstandingBalance',0):.2f}"],
        ["🕒 Last Issue",met.get("LastIssueDate") or "-"],
        ["🕒 Last Return",met.get("LastReturnDate") or "-"],
        ["👤 Class Teacher",info.get("ClassTeacher") or "-"],
        ["🏷️ Class",info.get("Class") or "-"],
        ["🧾 TR Number",info.get("TRNumber") or "-"],
    ]
    t=Table(summary_data,colWidths=[6*cm,6*cm])
    t.setStyle(TableStyle([("FONTNAME",(0,0),(-1,-1),font_name),("GRID",(0,0),(-1,-1),0.5,colors.grey)]))
    elements+=[t,Spacer(1,0.5*cm)]
    doc.build(elements)
    buffer.seek(0)
    return send_file(buffer,as_attachment=True,download_name=f"student_report_{identifier}.pdf",mimetype="application/pdf")
