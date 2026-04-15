# routes/password_reset.py — FIXED: removed deprecated before_app_first_request
from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from werkzeug.security import generate_password_hash
from flask_mail import Message
from db_app import get_conn

bp = Blueprint("password_reset_bp", __name__)
_mail = None


def get_serializer(app=None):
    app = app or current_app
    return URLSafeTimedSerializer(app.config["SECRET_KEY"], salt="password-reset")


def setup_mail(app):
    """Initialize mail — called from create_app() instead of deprecated hook."""
    global _mail
    from email_utils import init_mail
    _mail = init_mail(app)


@bp.route("/forgot", methods=["GET", "POST"])
def forgot_password():
    """Request a password reset link by email."""
    if request.method == "POST":
        email = (request.form.get("email") or "").strip()

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT username, email FROM users WHERE email = ?", (email,))
        row = cur.fetchone()

        if not row:
            # Don\'t reveal if email exists — always show success
            flash("📩 If that email exists, a reset link has been sent.", "info")
            return redirect(url_for("auth_bp.login"))

        username = row[0] if isinstance(row, tuple) else row["username"]
        user_email = row[1] if isinstance(row, tuple) else row["email"]

        s = get_serializer()
        token = s.dumps(user_email)
        reset_url = url_for("password_reset_bp.reset_password", token=token, _external=True)

        try:
            msg = Message(
                subject="🔐 Password Reset – Maktabat al-Jamea",
                recipients=[user_email],
                html=render_template(
                    "emails/password_reset_email.html",
                    username=username,
                    reset_url=reset_url,
                ),
            )
            _mail.send(msg)
            flash("📩 Password reset link sent to your email.", "info")
        except Exception as e:
            current_app.logger.error("Password reset email failed: %s", e, exc_info=True)
            flash("⚠️ Could not send reset email. Contact the administrator.", "warning")

        return redirect(url_for("auth_bp.login"))

    return render_template("forgot.html")


@bp.route("/reset/<token>", methods=["GET", "POST"])
def reset_password(token):
    """Reset password using the signed token."""
    s = get_serializer()
    try:
        email = s.loads(token, max_age=3600)  # 1 hour expiry
    except SignatureExpired:
        flash("⏰ Reset link has expired. Please request a new one.", "warning")
        return redirect(url_for("password_reset_bp.forgot_password"))
    except BadSignature:
        flash("❌ Invalid reset link.", "danger")
        return redirect(url_for("auth_bp.login"))

    if request.method == "POST":
        new_password = request.form.get("password", "")
        confirm_password = request.form.get("confirm_password", "")

        if len(new_password) < 8:
            flash("❌ Password must be at least 8 characters.", "danger")
            return render_template("reset.html", token=token)

        if new_password != confirm_password:
            flash("❌ Passwords do not match.", "danger")
            return render_template("reset.html", token=token)

        conn = get_conn()
        conn.execute(
            "UPDATE users SET password_hash = ? WHERE email = ?",
            (generate_password_hash(new_password), email),
        )
        conn.commit()
        flash("✅ Password updated successfully! Please log in.", "success")
        return redirect(url_for("auth_bp.login"))

    return render_template("reset.html", token=token)
