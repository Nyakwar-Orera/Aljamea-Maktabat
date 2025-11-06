# routes/password_reset.py
from flask import Blueprint, render_template, request, redirect, url_for, flash, session, current_app
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from werkzeug.security import generate_password_hash
from flask_mail import Message
from db_app import get_conn
from email_utils import init_mail

bp = Blueprint("password_reset_bp", __name__)
mail = None  # initialized later by app context


def get_serializer(app):
    return URLSafeTimedSerializer(app.config["SECRET_KEY"], salt="password-reset")


# ---------------- ROUTES ----------------
@bp.before_app_first_request
def setup_mail():
    """Initialize Flask-Mail within this blueprint."""
    global mail
    mail = init_mail(current_app)


@bp.route("/forgot", methods=["GET", "POST"])
def forgot_password():
    """Request password reset link."""
    if request.method == "POST":
        email = request.form.get("email")
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT username, email FROM users WHERE email = ?", (email,))
        user = cur.fetchone()
        conn.close()

        if not user:
            flash("❌ No user found with that email address.", "danger")
            return render_template("forgot.html")

        username, email = user
        s = get_serializer(current_app)
        token = s.dumps(email)

        reset_url = url_for("password_reset_bp.reset_password", token=token, _external=True)

        # Compose email
        msg = Message(
            subject="🔐 Password Reset – Maktabat al-Jamea",
            recipients=[email],
            html=render_template("email/password_reset_email.html", username=username, reset_url=reset_url),
        )
        mail.send(msg)
        flash("📩 Password reset link sent to your email.", "info")
        return redirect(url_for("auth_bp.login"))

    return render_template("forgot.html")


@bp.route("/reset/<token>", methods=["GET", "POST"])
def reset_password(token):
    """Validate token and allow user to reset password."""
    s = get_serializer(current_app)
    try:
        email = s.loads(token, max_age=3600)  # 1-hour expiry
    except SignatureExpired:
        flash("⏰ Reset link expired. Please try again.", "danger")
        return redirect(url_for("password_reset_bp.forgot_password"))
    except BadSignature:
        flash("❌ Invalid or corrupted reset link.", "danger")
        return redirect(url_for("password_reset_bp.forgot_password"))

    if request.method == "POST":
        password = request.form.get("password")
        confirm = request.form.get("confirm")

        if password != confirm:
            flash("⚠️ Passwords do not match.", "warning")
            return render_template("reset.html")

        hashed = generate_password_hash(password)
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("UPDATE users SET password_hash = ? WHERE email = ?", (hashed, email))
        conn.commit()
        conn.close()

        flash("✅ Password reset successful! You can now log in.", "success")
        return redirect(url_for("auth_bp.login"))

    return render_template("reset.html")
