import os
from flask_mail import Mail, Message
from flask import current_app, render_template


def init_mail(app):
    """Initialize Flask-Mail with app config."""
    return Mail(app)


def _send_email(mail, to_email, subject, template_name, context,
                attachment_bytes=None, attachment_filename=None):
    """Internal helper to send emails with optional PDF attachment."""
    with current_app.app_context():
        # Render HTML body from template
        body = render_template(template_name, **context)

        msg = Message(
            subject=subject,
            sender=current_app.config["MAIL_USERNAME"],
            recipients=[to_email]
        )
        msg.body = "Please see the attached report."
        msg.html = body

        # Optional PDF attachment
        if attachment_bytes and attachment_filename:
            msg.attach(
                filename=attachment_filename,
                content_type="application/pdf",
                data=attachment_bytes
            )

        # Send email
        mail.send(msg)


# ------- Public Send Functions -------

def send_darajah_report_email(mail, to_email, darajah_name,
                              attachment_bytes, attachment_filename):
    """
    Send darajah report email with PDF attachment.
    Keeps backwards compatibility with the old class naming by also supplying class_name.
    """
    _send_email(
        mail,
        to_email,
        subject=f"📚 Monthly Library Report - Darajah {darajah_name}",
        template_name="emails/class_report_email.html",
        context={"darajah_name": darajah_name, "class_name": darajah_name},
        attachment_bytes=attachment_bytes,
        attachment_filename=attachment_filename,
    )


# Backwards-compatible alias (Class -> Darajah)
def send_class_report_email(mail, to_email, class_name,
                            attachment_bytes, attachment_filename):
    """Deprecated: use send_darajah_report_email."""
    return send_darajah_report_email(mail, to_email, class_name,
                                     attachment_bytes, attachment_filename)


def send_marhala_report_email(mail, to_email, marhala_name,
                              attachment_bytes, attachment_filename):
    """
    Send marhala (formerly department) report email with PDF attachment.
    Supplies both marhala_name and department_name for template compatibility.
    """
    _send_email(
        mail,
        to_email,
        subject=f"🏛️ Library Report - Marhala {marhala_name}",
        template_name="emails/department_report_email.html",
        context={"marhala_name": marhala_name, "department_name": marhala_name, "dept": marhala_name},
        attachment_bytes=attachment_bytes,
        attachment_filename=attachment_filename,
    )


# Backwards-compatible alias (Department -> Marhala)
def send_department_report_email(mail, to_email, department_name,
                                 attachment_bytes, attachment_filename):
    """Deprecated: use send_marhala_report_email."""
    return send_marhala_report_email(mail, to_email, department_name,
                                     attachment_bytes, attachment_filename)


def send_student_report_email(mail, to_email, student_name,
                              attachment_bytes, attachment_filename):
    """Send student report email with PDF attachment."""
    _send_email(
        mail, to_email,
        subject=f"🎓 Your Personal Library Report - {student_name}",
        template_name="emails/student_report_email.html",   # ✅ updated path
        context={"student_name": student_name},
        attachment_bytes=attachment_bytes,
        attachment_filename=attachment_filename
    )
