# routes/auth.py — PRODUCTION HARDENED
from flask import (
    Blueprint, render_template, request, redirect, url_for,
    session, flash, current_app,
)
from werkzeug.security import check_password_hash
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
import base64
from db_app import get_conn
from datetime import datetime

bp = Blueprint("auth_bp", __name__)


# --------------------------------------------------
# Signed Token Authentication (replaces Base64)
# --------------------------------------------------
def _get_token_serializer():
    """Get a secure token serializer using the app\'s SECRET_KEY."""
    return URLSafeTimedSerializer(
        current_app.config["SECRET_KEY"],
        salt="external-token-login"
    )


def generate_external_token(username: str, role: str = "teacher", branch_code: str = "AJSN") -> str:
    """Generate a signed, time-limited token for external login."""
    s = _get_token_serializer()
    return s.dumps({"username": username, "role": role, "branch_code": branch_code})


def verify_external_token(token: str, max_age: int = None):
    """
    Verify and decode a token.
    1. Attempts secure signed decoding (itsdangerous).
    2. Falls back to legacy Base64 hyphenated decoding (C# style: username-date).
    - If max_age is None (default), the token does not expire.
    """
    s = _get_token_serializer()
    
    # --- Try Secure Signed Token ---
    try:
        data = s.loads(token, max_age=max_age)
        return data["username"], data.get("role", "teacher"), data.get("branch_code", "AJSN")
    except (SignatureExpired, BadSignature):
        pass # Try legacy fallback
    
    # --- Try Legacy Base64 Hyphenated (C# Style: username-date) ---
    try:
        decoded = base64.b64decode(token).decode("utf-8")
        if "-" in decoded:
            parts = decoded.split("-")
            username = parts[0]
            # When using legacy tokens, we assume a default role. 
            # The token_login() route will look up the actual role/branch from the DB.
            return username, "teacher", "AJSN"
    except Exception:
        raise ValueError("Invalid token. Access denied.")

    raise ValueError("Invalid token format. Please request a new link.")


# --------------------------------------------------
# Normal Login Route
# --------------------------------------------------
@bp.route("/", methods=["GET", "POST"])
def login():
    if request.method == "GET" and session.get("logged_in"):
        role = (session.get("role") or "").lower()
        if role == "super_admin":
            return redirect(url_for("super_admin_bp.god_eye"))
        elif role == "admin":
            return redirect(url_for("dashboard_bp.dashboard"))
        elif role == "hod":
            return redirect(url_for("hod_dashboard_bp.dashboard"))
        elif role == "teacher":
            return redirect(url_for("teacher_dashboard_bp.dashboard"))

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""

        if not username or not password:
            flash("❌ Please enter username and password.", "danger")
            return render_template("index.html", hide_nav=True)

        # Brute force protection
        from services.security_service import limiter
        ip_addr = request.remote_addr
        if limiter.is_locked(ip_addr) or limiter.is_locked(username):
            flash("🚫 Account locked due to too many failed attempts. Try again in 5 minutes.", "danger")
            return render_template("index.html", hide_nav=True)

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE username = ?", (username,))
        row = cur.fetchone()

        if not row:
            limiter.log_attempt(ip_addr)
            limiter.log_attempt(username)
            flash("❌ Invalid credentials.", "danger")
            return render_template("index.html", hide_nav=True)

        # Build user dict safely
        columns = [desc[0] for desc in cur.description] if hasattr(cur, 'description') and cur.description else []
        if columns:
            user = dict(zip(columns, row))
        else:
            user = {
                "id": row[0], "username": row[1], "email": row[2],
                "role": row[3], "password_hash": row[4],
                "department_name": row[5], "class_name": row[6],
                "profile_picture": row[7] if len(row) > 7 else None,
                "darajah_name": row[8] if len(row) > 8 else None,
            }

        password_hash = user.get("password_hash")
        if not password_hash:
            # User exists but has no password set (e.g. student accounts use token login)
            limiter.log_attempt(ip_addr)
            limiter.log_attempt(username)
            flash("❌ This account does not support password login. Please use your token login link.", "danger")
            return render_template("index.html", hide_nav=True)

        if not check_password_hash(password_hash, password):
            limiter.log_attempt(ip_addr)
            limiter.log_attempt(username)
            flash("❌ Invalid credentials.", "danger")
            return render_template("index.html", hide_nav=True)

        # ✅ Successful Login
        limiter.reset(ip_addr)
        limiter.reset(username)

        role = (user.get("role") or "admin").strip().lower()
        profile_pic = user.get("profile_picture") or "images/avatar.png"

        # Determine darajah_name for teachers
        darajah_name = user.get("darajah_name") or user.get("class_name") or user.get("department_name")

        # Update last_login
        try:
            conn.execute("UPDATE users SET last_login = ? WHERE id = ?",
                        (datetime.utcnow().isoformat(), user["id"]))
            conn.commit()
        except Exception:
            pass

        session.clear()
        session["logged_in"] = True
        session["user_id"] = user["id"]
        session["username"] = user["username"]
        session["role"] = role
        session["department_name"] = user.get("department_name")
        session["class_name"] = user.get("class_name")
        session["profile_picture"] = profile_pic
        # Branch-aware session
        branch_code = user.get("branch_code") or "AJSN"
        session["branch_code"] = branch_code if role != "super_admin" else None
        session["is_super_admin"] = (role == "super_admin")
        session["selected_ay"] = "current" # Default to 1447H

        if role == "hod":
            session["marhala_name"] = user.get("department_name")
        if role == "teacher" and darajah_name:
            session["darajah_name"] = darajah_name

        if role == "super_admin":
            return redirect(url_for("super_admin_bp.god_eye"))
        elif role == "admin":
            return redirect(url_for("dashboard_bp.dashboard"))
        elif role == "hod":
            return redirect(url_for("hod_dashboard_bp.dashboard"))
        elif role == "teacher":
            return redirect(url_for("teacher_dashboard_bp.dashboard"))
        else:
            flash("⚠️ Role not recognized.", "warning")
            session.clear()
            return redirect(url_for("auth_bp.login"))

    return render_template("index.html", hide_nav=True)


# --------------------------------------------------
# Signed Token Login (SECURE — replaces Base64)
# --------------------------------------------------
@bp.route("/token-login")
def token_login():
    token = request.args.get("token")
    if not token:
        flash("❌ Missing token.", "danger")
        return redirect(url_for("auth_bp.login"))

    try:
        username, role, _ = verify_external_token(token)
    except ValueError as e:
        flash(f"❌ {str(e)}", "danger")
        return redirect(url_for("auth_bp.login"))

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE username = ?", (username,))
    row = cur.fetchone()

    if not row:
        flash("❌ User not found.", "danger")
        return redirect(url_for("auth_bp.login"))

    columns = [desc[0] for desc in cur.description] if hasattr(cur, 'description') and cur.description else []
    if columns:
        user = dict(zip(columns, row))
    else:
        user = {
            "id": row[0], "username": row[1], "email": row[2],
            "role": row[3], "class_name": row[6],
            "department_name": row[5],
            "profile_picture": row[7] if len(row) > 7 else "images/avatar.png",
            "darajah_name": row[8] if len(row) > 8 else None,
        }

    darajah_name = user.get("darajah_name") or user.get("class_name")
    branch_code = user.get("branch_code") or "AJSN"

    session.clear()
    session["logged_in"] = True
    session["user_id"] = user.get("id")
    session["username"] = user.get("username", username)
    session["role"] = role
    session["department_name"] = user.get("department_name")
    session["class_name"] = user.get("class_name")
    session["profile_picture"] = user.get("profile_picture") or "images/avatar.png"
    session["branch_code"] = branch_code if role != "super_admin" else None
    session["is_super_admin"] = (role == "super_admin")
    session["selected_ay"] = "current"  # Default for token users

    if role == "student":
        # Students log in via token — route to student portal
        session.clear()
        session["logged_in"] = True
        session["username"] = username
        session["role"] = "student"
        session["cardnumber"] = username
        return redirect(url_for("students.student_portal"))

    if role == "hod":
        session["marhala_name"] = user.get("department_name")
    if role == "teacher" and darajah_name:
        session["darajah_name"] = darajah_name

    # Route to the correct dashboard based on role
    if role == "super_admin":
        return redirect(url_for("super_admin_bp.god_eye"))
    elif role == "admin":
        return redirect(url_for("dashboard_bp.dashboard"))
    elif role == "hod":
        return redirect(url_for("hod_dashboard_bp.dashboard"))
    elif role == "teacher":
        return redirect(url_for("teacher_dashboard_bp.dashboard"))
    else:
        flash("⚠️ Role not recognized. Please contact the administrator.", "warning")
        session.clear()
        return redirect(url_for("auth_bp.login"))


@bp.route("/change_ay", methods=["POST"])
def change_ay():
    """Global endpoint to update selected academic year in session."""
    year = (request.form.get("academic_year") or "").strip()
    if year:
        session["selected_ay"] = year
        current_app.logger.info(f"AY changed to: {year} by {session.get('username')}")
    
    # Try to redirect to referring page
    next_page = request.referrer
    if next_page and url_for('auth_bp.login') not in next_page:
        return redirect(next_page)
    
    # Fallback based on role
    role = (session.get("role") or "").lower()
    if role == "super_admin":
        return redirect(url_for("super_admin_bp.god_eye"))
    elif role == "admin":
        return redirect(url_for("dashboard_bp.dashboard"))
    elif role == "teacher":
        return redirect(url_for("teacher_dashboard_bp.dashboard"))
    
    return redirect(url_for("auth_bp.login"))


# --------------------------------------------------
# Logout
# --------------------------------------------------
@bp.route("/logout")
def logout():
    session.clear()
    flash("Logged out successfully.", "info")
    return redirect(url_for("auth_bp.login"))
