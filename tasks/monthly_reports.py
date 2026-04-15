from __future__ import annotations

from flask import render_template, current_app
from flask_mail import Message

from io import BytesIO
from datetime import datetime
from typing import List, Tuple

from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    PageBreak,
    Image,
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import cm

from db_app import get_appdata_conn
from db_koha import get_koha_conn
from routes.students import get_student_info
from services.exports import (
    dataframe_to_pdf_bytes,
    _ensure_font_registered,
    _shape_if_rtl,
)

# Local copy of the attribute codes we treat as "darajah" and "TR number"
_DARAJAH_ATTR_CODES = ("STD", "CLASS", "DAR", "CLASS_STD")
_TR_ATTR_CODES = ("TRNO", "TRN", "TR_NUMBER", "TR")


# -------------------------------------------------------------------
# Koha lookups (no mapping uploads needed)
# -------------------------------------------------------------------
def koha_distinct_darajahs() -> List[str]:
    """
    Get distinct darajahs from Koha.
    Uses borrower_attributes for darajah (STD/CLASS/...) with a fallback to branchcode.
    """
    conn = get_koha_conn()
    cur = conn.cursor()

    placeholders = ",".join(["%s"] * len(_DARAJAH_ATTR_CODES))
    cur.execute(
        f"""
        SELECT DISTINCT COALESCE(std.attribute, b.branchcode) AS cls
        FROM borrowers b
        LEFT JOIN borrower_attributes std
               ON std.borrowernumber = b.borrowernumber
              AND std.code IN ({placeholders})
        WHERE COALESCE(std.attribute, b.branchcode) IS NOT NULL
        ORDER BY cls;
        """,
        _DARAJAH_ATTR_CODES,
    )
    darajahs = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return darajahs


# -------------------------------------------------------------------
# Local recipients (address book)
# -------------------------------------------------------------------
def _get_teacher_emails_for_darajah(darajah_name: str) -> List[str]:
    """
    From local appdata 'teacher_darajah_mapping' table.
    Returns list of teacher emails for a darajah.
    """
    conn = get_appdata_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT teacher_email 
        FROM teacher_darajah_mapping 
        WHERE darajah_name = ? AND teacher_email IS NOT NULL AND teacher_email <> ''
        """,
        (darajah_name,),
    )
    emails = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return emails


def _get_all_hod_emails() -> List[str]:
    """
    From local appdata 'department_heads' table (email column).
    """
    conn = get_appdata_conn()
    cur = conn.cursor()
    cur.execute("SELECT email FROM department_heads WHERE email IS NOT NULL AND email<>''")
    emails = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return emails


def _get_all_departments() -> List[Tuple[str, str, str]]:
    """
    Authoritative list of departments to email from local 'department_heads'.
    Returns list of tuples: (department_name, head_name, email)
    """
    conn = get_appdata_conn()
    cur = conn.cursor()
    cur.execute("SELECT department_name, head_name, email FROM department_heads")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
def _safe_image(path: str, max_w_cm: float, max_h_cm: float) -> Image | None:
    """Return a ReportLab Image if the path exists, otherwise None."""
    import os

    if not path or not os.path.exists(path):
        return None
    img = Image(path)
    img._restrictSize(max_w_cm * cm, max_h_cm * cm)
    return img


# -------------------------------------------------------------------
# PDF builders
# -------------------------------------------------------------------
def build_darajah_detailed_pdf(darajah_name: str) -> bytes | None:
    """
    Generate a beautiful, structured darajah report PDF with Taqeem integration:
    - Cover page (with logo, title, date)
    - Darajah summary block with Taqeem overview
    - Per-student sections with photo, metrics, borrowed books, and Taqeem details
    - Page numbers in footer
    Skips rows without FullName/TRNumber to avoid ReportLab layout errors.
    """
    # Lazy import to avoid circular imports
    from routes.reports import darajah_report

    df = darajah_report(darajah_name)
    if df.empty:
        return None

    # Ensure required columns exist and drop problematic rows
    if "FullName" not in df.columns or "TRNumber" not in df.columns:
        return None
    df = df[df["FullName"].notna() & df["TRNumber"].notna()]
    if df.empty:
        return None

    font_name = _ensure_font_registered()
    buffer = BytesIO()

    # ---------- Styles ----------
    styles = getSampleStyleSheet()
    for key in ("Title", "Normal", "Heading2", "Heading3"):
        styles[key].fontName = font_name
    styles.add(ParagraphStyle(name="CenterTitle", alignment=TA_CENTER, fontName=font_name, fontSize=16, leading=20))
    styles.add(ParagraphStyle(name="SectionHeader", fontName=font_name, fontSize=12, textColor=colors.HexColor("#004080")))
    styles.add(ParagraphStyle(name="Small", fontName=font_name, fontSize=9))
    styles.add(ParagraphStyle(name="Tiny", fontName=font_name, fontSize=8, textColor=colors.grey))
    S = lambda x: _shape_if_rtl(str(x) if x is not None else "-")

    # ---------- Doc & Footer ----------
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=2 * cm,
        rightMargin=2 * cm,
        topMargin=2 * cm,
        bottomMargin=2 * cm,
        title=f"Darajah Report - {darajah_name}",
    )

    def _footer(canvas, _doc):
        canvas.saveState()
        canvas.setFont(font_name, 8)
        page_str = f"Page {_doc.page}"
        date_str = datetime.now().strftime("%d %b %Y")
        # Right footer: page number
        canvas.drawRightString(A4[0] - 2 * cm, 1.5 * cm, page_str)
        # Left footer: date
        canvas.drawString(2 * cm, 1.5 * cm, date_str)
        canvas.restoreState()

    elements: list = []

    # ---------- Cover Page ----------
    try:
        import os

        logo = _safe_image(
            os.path.join(current_app.root_path, "static", "images", "logo.png"),
            max_w_cm=5,
            max_h_cm=5,
        )
    except Exception:
        logo = None

    if logo:
        elements.append(logo)
        elements.append(Spacer(1, 0.2 * cm))

    elements.append(Paragraph(S("Al-Jamea tus-Saifiyah • Maktabat"), styles["CenterTitle"]))
    elements.append(Spacer(1, 0.1 * cm))
    elements.append(Paragraph(S("📘 Monthly Darajah Library Report"), styles["CenterTitle"]))
    elements.append(Spacer(1, 0.3 * cm))
    elements.append(Paragraph(S(f"Darajah: {darajah_name}"), styles["CenterTitle"]))
    elements.append(Spacer(1, 0.15 * cm))
    elements.append(Paragraph(datetime.now().strftime("%d %B %Y"), styles["CenterTitle"]))
    elements.append(Spacer(1, 0.8 * cm))
    elements.append(
        Paragraph(
            S("This report provides a detailed breakdown of student borrowing activity, fees, engagement metrics, and Taqeem marks for the current academic period."),
            styles["Small"],
        )
    )
    elements.append(PageBreak())

    # ---------- Darajah Summary ----------
    total_students = int(len(df))
    total_issues = int(df.get("Issues_AY", 0).sum())
    total_fees = float(df.get("FeesPaid_AY", 0).sum())
    total_overdues = int(df.get("Overdues", 0).sum())
    
    # Calculate average Taqeem if available
    avg_taqeem = "N/A"
    taqeem_scores = []
    for _, row in df.iterrows():
        trno = str(row.get("TRNumber", "")).strip()
        if trno:
            info = get_student_info(trno)
            if info and info.get("Taqeem") and info["Taqeem"].get("total"):
                taqeem_scores.append(info["Taqeem"]["total"])
    
    if taqeem_scores:
        avg_taqeem = f"{sum(taqeem_scores) / len(taqeem_scores):.1f}/100"

    summary_table = Table(
        [
            ["Total Students", total_students],
            ["Total Issues (AY)", total_issues],
            ["Total Overdues", total_overdues],
            ["Total Fees Paid", f"{total_fees:.2f}"],
            ["Average Taqeem", avg_taqeem],
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
    elements.append(Paragraph(S("📊 Darajah Summary"), styles["Heading2"]))
    elements.append(summary_table)
    elements.append(Spacer(1, 0.6 * cm))

    # ---------- Per-student sections ----------
    for _, row in df.iterrows():
        trno = str(row.get("TRNumber", "")).strip()
        if not trno:
            continue

        info = get_student_info(trno)
        # Skip if no info or no valid name; prevents giant cells/layout errors
        if not info or not info.get("FullName"):
            continue

        # Get Taqeem data
        taqeem = info.get("Taqeem", {})
        taqeem_total = taqeem.get("total", 0) if taqeem else 0

        # Header with Taqeem score
        header_text = f"👤 {info.get('FullName')} ({trno})"
        if taqeem_total > 0:
            header_text += f" | Taqeem: {taqeem_total}/100"
        
        elements.append(Paragraph(S(header_text), styles["SectionHeader"]))
        elements.append(Spacer(1, 0.15 * cm))

        # Photo (real or avatar)
        try:
            import os

            photo_rel = info.get("Photo") or "images/avatar.png"
            photo_path = os.path.join(current_app.root_path, "static", photo_rel)
            if not os.path.exists(photo_path):
                photo_path = os.path.join(current_app.root_path, "static", "images", "avatar.png")
            photo_img = _safe_image(photo_path, max_w_cm=3.2, max_h_cm=3.2)
        except Exception:
            photo_img = None

        # Metrics (summary) table next to photo - INCLUDING TAQEEM
        met = info.get("Metrics", {}) or {}
        summary_data = [
            ["AY Issues", met.get("AYIssues", 0)],
            ["Total Fees Paid", f"{met.get('TotalFeesPaid', 0):.2f}"],
            ["Outstanding Balance", f"{met.get('OutstandingBalance', 0):.2f}"],
            ["Last Issue", met.get("LastIssueDate") or "-"],
            ["Darajah", info.get("Darajah") or "-"],
            ["Marhala", info.get("Marhala") or "-"],
            ["Taqeem Total", f"{taqeem_total}/100" if taqeem_total > 0 else "N/A"],
        ]
        
        # Add Taqeem breakdown if available
        if taqeem and taqeem.get("book_issue"):
            summary_data.extend([
                ["Book Issue Marks", f"{taqeem['book_issue'].get('total', 0)}/60"],
                ["Book Review Marks", f"{taqeem.get('book_review', {}).get('marks', 0)}/30"],
                ["Program Attendance", f"{taqeem.get('program_attendance', 0)}/10"],
            ])

        t = Table(summary_data, colWidths=[6 * cm, 6 * cm])
        t.setStyle(
            TableStyle(
                [
                    ("FONTNAME", (0, 0), (-1, -1), font_name),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.lightgrey),
                    ("BACKGROUND", (0, 0), (-1, -1), colors.whitesmoke),
                    ("BACKGROUND", (0, -3 if taqeem else -1), (-1, -1), colors.lavender),
                    ("FONTSIZE", (0, 0), (-1, -1), 8),
                    ("TOPPADDING", (0, 0), (-1, -1), 4),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ]
            )
        )

        if photo_img:
            # Compose photo + metrics in a 2-column layout
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

        # Borrowed Books
        borrowed = info.get("BorrowedBooks") or []
        elements.append(Paragraph(S("📖 Borrowed Books"), styles["Heading3"]))
        if borrowed:
            books_data = [["Title", "Issued", "Due", "Returned", "Status"]]
            # Limit for layout safety; adjust if you prefer
            for b in borrowed[:15]:
                books_data.append(
                    [
                        S(b.get("title", "N/A")),
                        S(b.get("_issued_hijri") or "-"),
                        S(b.get("_due_hijri") or "-"),
                        S("Yes" if b.get("returned") else "No"),
                        S("Overdue" if b.get("overdue") else "On Time"),
                    ]
                )
            book_table = Table(
                books_data,
                repeatRows=1,
                colWidths=[6 * cm, 3 * cm, 3 * cm, 2 * cm, 2 * cm],
            )
            book_table.setStyle(
                TableStyle(
                    [
                        ("FONTNAME", (0, 0), (-1, -1), font_name),
                        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#004080")),
                        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.lightgrey]),
                        ("FONTSIZE", (0, 0), (-1, -1), 8),
                    ]
                )
            )
            elements.append(book_table)
        else:
            elements.append(Paragraph(S("No borrowed books."), styles["Normal"]))

        elements.append(Spacer(1, 0.4 * cm))
        # Soft divider
        elements.append(Paragraph("<hr width='100%' color='#cccccc'/>", styles["Normal"]))
        elements.append(Spacer(1, 0.4 * cm))

    # Build
    doc.build(elements, onFirstPage=_footer, onLaterPages=_footer)
    buffer.seek(0)
    return buffer.getvalue()


def build_department_pdf_bytes(dept_name: str) -> bytes | None:
    """
    Build a department PDF from the department dataframe.
    Avoids calling Flask route functions that return Response objects.
    """
    from routes.reports import department_report  # lazy import
    df = department_report(dept_name if dept_name else None)
    if df.empty:
        return None
    return dataframe_to_pdf_bytes(f"Marhala Report - {dept_name}", df)


# -------------------------------------------------------------------
# Email sender
# -------------------------------------------------------------------
def _send_email_with_pdf(app, mail, subject, recipients, html_body, filename, pdf_bytes, cc=None):
    """
    Safe email sender with attachment.
    """
    msg = Message(
        subject=subject,
        sender=app.config.get("MAIL_USERNAME"),
        recipients=recipients,
        cc=(cc or []),
    )
    msg.html = html_body
    msg.attach(filename, "application/pdf", pdf_bytes)
    try:
        mail.send(msg)
        app.logger.info(f"✅ Email sent to {recipients} CC {cc or []} ({filename})")
        return True
    except Exception as e:
        app.logger.error(f"❌ Email send failed to {recipients}: {e}")
        return False


# -------------------------------------------------------------------
# Main report senders
# -------------------------------------------------------------------
def send_darajah_reports(app, mail, cc_hods: bool = True):
    """
    Generate & send per-darajah reports using Koha data.
    Teachers are fetched from local 'teacher_darajah_mapping' table.
    Optionally CC all HODs (default: True).
    """
    darajahs = koha_distinct_darajahs()
    hod_cc = _get_all_hod_emails() if cc_hods else []

    app.logger.info(f"🚀 send_darajah_reports() started - Found {len(darajahs)} darajahs")

    with app.app_context():
        for darajah_name in darajahs:
            app.logger.info(f"🧩 Processing darajah: {darajah_name}")
            teacher_emails = _get_teacher_emails_for_darajah(darajah_name)
            app.logger.info(f"   ↳ Teacher emails: {teacher_emails}")

            if not teacher_emails:
                app.logger.warning(f"⚠️ No teacher emails for darajah '{darajah_name}', skipping.")
                continue

            try:
                pdf_bytes = build_darajah_detailed_pdf(darajah_name)
                if not pdf_bytes:
                    app.logger.info(f"ℹ️ No data for darajah {darajah_name}, skipping email.")
                    continue
            except Exception as e:
                app.logger.error(f"❌ Failed to generate darajah PDF for {darajah_name}: {e}")
                continue

            subject = f"📘 Monthly Library Report – Darajah {darajah_name}"
            
            # Get teacher names for the email
            conn = get_appdata_conn()
            cur = conn.cursor()
            cur.execute(
                "SELECT DISTINCT teacher_name FROM teacher_darajah_mapping WHERE darajah_name = ?",
                (darajah_name,)
            )
            teacher_names = [row[0] for row in cur.fetchall()]
            conn.close()
            
            html_body = render_template(
                "emails/darajah_report_email.html",
                darajah_name=darajah_name,
                teacher_names=teacher_names,
            )
            filename = f"darajah_{darajah_name.replace(' ', '_')}.pdf"

            _send_email_with_pdf(
                app, mail, subject, teacher_emails, html_body, filename, pdf_bytes, cc=hod_cc
            )


def send_department_reports(app, mail):
    """
    Generate & send per-department reports (one email per HOD),
    using local department_heads as the recipient list.
    """
    depts = _get_all_departments()

    with app.app_context():
        for dept, head_name, head_email in depts:
            if not head_email:
                app.logger.warning(f"⚠️ No HOD email for marhala '{dept}', skipping.")
                continue

            try:
                pdf_bytes = build_department_pdf_bytes(dept)
                if not pdf_bytes:
                    app.logger.info(f"ℹ️ No data for marhala {dept}, skipping email.")
                    continue
            except Exception as e:
                app.logger.error(f"❌ Failed to generate marhala PDF for {dept}: {e}")
                continue

            subject = f"📊 Monthly Library Report – Marhala {dept}"
            html_body = render_template(
                "emails/marhala_report_email.html",
                marhala_name=dept,
                dept=dept,
                head_name=head_name,
            )
            filename = f"marhala_{dept.replace(' ', '_')}.pdf"

            _send_email_with_pdf(app, mail, subject, [head_email], html_body, filename, pdf_bytes)


def send_all_reports(app, mail):
    """
    Entry point used by scheduler and /run_email_reports_now.
    """
    app.logger.info("📤 Starting full monthly report dispatch (Koha-driven)...")
    send_darajah_reports(app, mail, cc_hods=True)  # CC all HODs by default
    send_department_reports(app, mail)
    app.logger.info("🎉 All monthly reports (darajah + marhala) processed successfully.")
