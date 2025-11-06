import os
from flask_mail import Mail, Message
from flask import current_app, render_template

def init_mail(app):
    """Initialize Flask-Mail with app config"""
    return Mail(app)

def _send_email(mail, to_email, subject, template_name, context, attachment_bytes=None, attachment_filename=None):
    """Internal helper to send emails with optional PDF attachment"""
    with current_app.app_context():
        body = render_template(template_name, **context)
        msg = Message(
            subject=subject,
            sender=current_app.config["MAIL_USERNAME"],
            recipients=[to_email]
        )
        msg.body = "Please see the attached report."
        msg.html = body

        if attachment_bytes and attachment_filename:
            msg.attach(
                filename=attachment_filename,
                content_type="application/pdf",
                data=attachment_bytes
            )

        mail.send(msg)

# ------- Public Send Functions -------

def send_class_report_email(mail, to_email, class_name, attachment_bytes, attachment_filename):
    _send_email(
        mail, to_email,
        subject=f"Monthly Library Report - Class {class_name}",
        template_name="email/class_report_email.html",
        context={"class_name": class_name},
        attachment_bytes=attachment_bytes,
        attachment_filename=attachment_filename
    )

def send_department_report_email(mail, to_email, department_name, attachment_bytes, attachment_filename):
    _send_email(
        mail, to_email,
        subject=f"Library Report - {department_name} Department",
        template_name="email/department_report_email.html",
        context={"department_name": department_name},
        attachment_bytes=attachment_bytes,
        attachment_filename=attachment_filename
    )

def send_student_report_email(mail, to_email, student_name, attachment_bytes, attachment_filename):
    _send_email(
        mail, to_email,
        subject=f"Your Personal Library Report - {student_name}",
        template_name="email/student_report_email.html",
        context={"student_name": student_name},
        attachment_bytes=attachment_bytes,
        attachment_filename=attachment_filename
    )
