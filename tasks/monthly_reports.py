# tasks/monthly_reports.py
import io
from flask import render_template
from flask_mail import Message
from db_app import get_appdata_conn
from services.reporting import export_class_pdf, export_department_pdf


def send_class_reports(app, mail):
    """Send monthly class reports to class teachers."""
    conn = get_appdata_conn()
    cur = conn.cursor()
    # Use positional indexing to avoid row-factory assumptions
    cur.execute("SELECT DISTINCT class_name, teacher_name, teacher_email FROM mappings;")
    mappings = cur.fetchall()
    conn.close()

    with app.app_context():
        for m in mappings:
            class_name, teacher_name, teacher_email = m[0], m[1], m[2]

            if not teacher_email:
                app.logger.warning(f"⚠️ No email for class {class_name}, skipping.")
                continue

            # Generate PDF (returns bytes or None)
            pdf_bytes = export_class_pdf(class_name)
            if not pdf_bytes:
                app.logger.info(f"ℹ️ No data for class {class_name}, skipping email.")
                continue

            # Compose email
            subject = f"📚 Monthly Library Report - Class {class_name}"
            html_body = render_template(
                "emails/class_report_email.html",
                class_name=class_name,
                teacher_name=teacher_name,
            )

            msg = Message(
                subject,
                sender=app.config.get("MAIL_USERNAME"),
                recipients=[teacher_email],
            )
            msg.html = html_body
            # export_* returns raw bytes, not BytesIO
            msg.attach(f"class_{class_name}.pdf", "application/pdf", pdf_bytes)

            try:
                mail.send(msg)
                app.logger.info(f"✅ Sent class report for {class_name} to {teacher_email}")
            except Exception as e:
                app.logger.error(f"❌ Failed to send class report for {class_name}: {e}")


def send_department_reports(app, mail):
    """Send monthly department reports to department heads."""
    conn = get_appdata_conn()
    cur = conn.cursor()
    cur.execute("SELECT department_name, head_name, email FROM department_heads;")
    heads = cur.fetchall()
    conn.close()

    with app.app_context():
        for h in heads:
            dept, head_name, head_email = h[0], h[1], h[2]

            if not head_email:
                app.logger.warning(f"⚠️ No email for department {dept}, skipping.")
                continue

            # Generate PDF (returns bytes or None)
            pdf_bytes = export_department_pdf(dept)
            if not pdf_bytes:
                app.logger.info(f"ℹ️ No data for department {dept}, skipping email.")
                continue

            # Compose email
            subject = f"📊 Monthly Library Report - {dept} Department"
            html_body = render_template(
                "emails/department_report_email.html",
                dept=dept,
                head_name=head_name,
            )

            msg = Message(
                subject,
                sender=app.config.get("MAIL_USERNAME"),
                recipients=[head_email],
            )
            msg.html = html_body
            msg.attach(f"department_{dept}.pdf", "application/pdf", pdf_bytes)

            try:
                mail.send(msg)
                app.logger.info(f"✅ Sent department report for {dept} to {head_email}")
            except Exception as e:
                app.logger.error(f"❌ Failed to send department report for {dept}: {e}")


def send_all_reports(app, mail):
    """Main entry point – send both class and department reports in one run."""
    send_class_reports(app, mail)
    send_department_reports(app, mail)
    app.logger.info("🎉 All monthly reports (class + department) have been processed.")
