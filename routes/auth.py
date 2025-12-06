from flask import (
    Blueprint,
    render_template,
    request,
    redirect,
    url_for,
    session,
    flash,
)
from werkzeug.security import check_password_hash
from db_app import get_conn
import base64
from datetime import datetime, timedelta

bp = Blueprint("auth_bp", __name__)

# --------------------------------------------------
# Token-based authentication helpers
# --------------------------------------------------
def verify_external_token(encoded_token: str):
    """Base64 decode and verify token"""
    try:
        decoded = base64.b64decode(encoded_token).decode("utf-8")
        username, sdate_str = decoded.split('-')
        token_time = datetime.strptime(sdate_str, "%Y%m%d")
    except Exception:
        raise ValueError("Invalid token format or expired token")

    if datetime.utcnow() - token_time > timedelta(days=1):
        raise ValueError("Token has expired.")
    
    return username, "teacher", None  # return username, role (teacher), class_name (optional)


# --------------------------------------------------
# Normal Login Route (POST method)
# --------------------------------------------------
@bp.route("/", methods=["GET", "POST"])
def login():
    # If already logged in and they hit "/", send them to their dashboard
    if request.method == "GET" and session.get("logged_in"):
        role = (session.get("role") or "").lower()
        if role == "admin":
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

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE username = ?", (username,))
        row = cur.fetchone()
        conn.close()

        if not row:
            flash("❌ Invalid username", "danger")
            return render_template("index.html", hide_nav=True)

        # 0 id, 1 username, 2 email, 3 role, 4 password_hash,
        # 5 department_name, 6 class_name, 7 profile_picture, ...
        user = {
            "id": row[0],
            "username": row[1],
            "email": row[2],
            "role": row[3],
            "password_hash": row[4],
            "department_name": row[5],
            "class_name": row[6],
            "profile_picture": row[7] if len(row) > 7 else None,
        }

        if not check_password_hash(user["password_hash"], password):
            flash("❌ Incorrect password", "danger")
            return render_template("index.html", hide_nav=True)

        # Normalise role and profile picture
        raw_role = (user["role"] or "").strip()
        role = raw_role.lower() if raw_role else "admin"
        profile_pic = user["profile_picture"] or "images/avatar.png"

        session.clear()
        session["logged_in"] = True
        session["user_id"] = user["id"]
        session["username"] = user["username"]
        session["role"] = role
        session["department_name"] = user["department_name"]
        session["class_name"] = user["class_name"]
        session["profile_picture"] = profile_pic

        if role == "admin":
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
# Token-based Login Route
# --------------------------------------------------
@bp.route("/token-login")
def token_login():
    encoded_token = request.args.get("token")
    if not encoded_token:
        flash("❌ Missing token.", "danger")
        return redirect(url_for("auth_bp.login"))

    try:
        username, role, class_name = verify_external_token(encoded_token)
    except ValueError as e:
        flash(f"❌ {str(e)}", "danger")
        return redirect(url_for("auth_bp.login"))

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE username = ?", (username,))
    row = cur.fetchone()
    conn.close()

    if not row:
        flash("❌ User not found.", "danger")
        return redirect(url_for("auth_bp.login"))

    session.clear()
    session["logged_in"] = True
    session["user_id"] = row[0]
    session["username"] = row[1]
    session["role"] = role
    session["class_name"] = class_name or row[6]  # Default to class_name from DB if none provided
    session["profile_picture"] = row[7] if len(row) > 7 else "images/avatar.png"

    return redirect(url_for("teacher_dashboard_bp.dashboard"))


# --------------------------------------------------
# Logout Route
# --------------------------------------------------
@bp.route("/logout")
def logout():
    session.clear()
    flash("Logged out successfully.", "info")
    return redirect(url_for("auth_bp.login"))
