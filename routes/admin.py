from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, flash, current_app
from werkzeug.security import generate_password_hash
from config import Config
from db_app import get_conn
from tasks.scheduler import reload_scheduler

bp = Blueprint("admin_bp", __name__)

# ---------------- AUTH ----------------
@bp.route("/")
def index():
    """Landing page → login page or dashboard if already logged in."""
    if session.get("logged_in"):
        return redirect(url_for("dashboard_bp.dashboard"))
    return render_template("index.html", hide_nav=True)


@bp.route("/login", methods=["GET", "POST"])
def login():
    """Admin login using credentials from .env (Config)."""
    if request.method == "POST":
        user = request.form.get("username", "").strip()
        pw = request.form.get("password", "").strip()

        if user == Config.ADMIN_USER and pw == Config.ADMIN_PASS:
            session["logged_in"] = True
            session["user"] = user
            flash(f"✅ Welcome back {user}, logged in successfully.", "success")
            return redirect(url_for("dashboard_bp.dashboard"))
        else:
            flash("❌ Invalid credentials", "danger")

    return render_template("index.html", hide_nav=True)


@bp.route("/logout")
def logout():
    """Clear session and return to login page."""
    session.clear()
    flash("ℹ️ You have been signed out.", "info")
    return redirect(url_for("admin_bp.index"))


@bp.route("/settings")
def admin_settings():
    """Render Admin Settings page."""
    if not session.get("logged_in"):
        return redirect(url_for("admin_bp.index"))
    return render_template("settings.html")


# ---------------- USERS ----------------
@bp.route("/api/add_user", methods=["POST"])
def add_user():
    """Admin-only: Add new user (Admin, HOD, or Class Teacher) with password and email notification."""
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")

    try:
        username = request.form.get("username", "").strip()
        email = request.form.get("email", "").strip()
        role = request.form.get("role", "teacher").strip().lower()
        password = request.form.get("password", "").strip()
        department_name = request.form.get("department_name", "").strip() or None
        class_name = request.form.get("class_name", "").strip() or None

        # Validate required fields
        if not username or not email:
            return jsonify(success=False, error="Missing username or email.")
        if not password:
            return jsonify(success=False, error="Password is required.")
        if role not in ("admin", "hod", "teacher"):
            return jsonify(success=False, error="Invalid role selected.")
        if role == "hod" and not department_name:
            return jsonify(success=False, error="Department name is required for HODs.")
        if role == "teacher" and not class_name:
            return jsonify(success=False, error="Class name is required for Teachers.")

        # Hash password
        password_hash = generate_password_hash(password)

        conn = get_conn()
        cur = conn.cursor()

        # Insert into users table
        cur.execute("""
            INSERT INTO users (username, email, role, password_hash, department_name, class_name)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (username, email, role, password_hash, department_name, class_name))

        # Mirror in department_heads table for HODs
        if role == "hod" and department_name:
            cur.execute("""
                INSERT OR IGNORE INTO department_heads (department_name, head_name, email)
                VALUES (?, ?, ?)
            """, (department_name, username, email))

        conn.commit()
        conn.close()

        # ✅ Send email to user
        try:
            from flask_mail import Message
            mail = current_app.extensions.get("mail")
            if mail:
                msg = Message(
                    subject="Your Maktabat al-Jamea Account Details",
                    recipients=[email],
                    body=f"""
Dear {username},

Your Maktabat al-Jamea account has been created successfully.

Username: {username}
Role: {role.title()}
Password: {password}

You can log in here: {url_for('auth_bp.login', _external=True)}

Please keep your credentials secure.

Regards,
Maktabat al-Jamea Admin
"""
                )
                mail.send(msg)
                current_app.logger.info(f"Account email sent to {email}.")
        except Exception as e:
            current_app.logger.warning(f"⚠️ Could not send email to {email}: {e}")

        flash(f"✅ User '{username}' added successfully and email sent to {email}.", "success")
        return jsonify(success=True)

    except Exception as e:
        current_app.logger.error(f"Add user error: {e}")
        return jsonify(success=False, error=str(e))



@bp.route("/api/remove_user", methods=["POST"])
def remove_user():
    """Admin-only: remove user by username."""
    if not session.get("logged_in"):
        return jsonify(success=False)
    try:
        data = request.get_json()
        username = data.get("username")

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM users WHERE username=?", (username,))
        conn.commit()
        conn.close()
        flash(f"🗑️ User '{username}' removed successfully.", "info")
        return jsonify(success=True)
    except Exception as e:
        current_app.logger.error(f"Remove user error: {e}")
        return jsonify(success=False)


@bp.route("/api/list_users", methods=["GET"])
def list_users():
    """List all users with department/class info."""
    if not session.get("logged_in"):
        return jsonify([])
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT username, email, role, department_name, class_name
            FROM users
            ORDER BY role ASC, username ASC
        """)
        rows = cur.fetchall()
        conn.close()
        return jsonify([
            {
                "username": r[0],
                "email": r[1],
                "role": r[2],
                "department_name": r[3],
                "class_name": r[4],
            }
            for r in rows
        ])
    except Exception as e:
        current_app.logger.error(f"List users error: {e}")
        return jsonify([])


# ---------------- MAPPINGS ----------------
@bp.route("/api/upload_mapping", methods=["POST"])
def upload_mapping():
    if not session.get("logged_in"):
        return jsonify(success=False)
    try:
        file = request.files["mapping_file"]
        import pandas as pd

        df = pd.read_csv(file) if file.filename.endswith(".csv") else pd.read_excel(file)
        required = {"student_id", "student_name", "class_name", "teacher_name", "teacher_email"}
        if not required.issubset(df.columns):
            return jsonify(success=False, error="Missing required columns.")

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM mappings")

        for _, r in df.iterrows():
            cur.execute("""
                INSERT INTO mappings (student_id, student_name, class_name, teacher_name, teacher_email)
                VALUES (?, ?, ?, ?, ?)
            """, (
                str(r["student_id"]),
                str(r["student_name"]),
                str(r["class_name"]),
                str(r["teacher_name"]),
                str(r.get("teacher_email", "")),
            ))

        conn.commit()
        conn.close()
        return jsonify(success=True)
    except Exception as e:
        current_app.logger.error(f"Upload mapping error: {e}")
        return jsonify(success=False)


# ---------------- EMAIL SETTINGS ----------------
@bp.route("/api/save_email_settings", methods=["POST"])
def save_email_settings():
    if not session.get("logged_in"):
        return jsonify(success=False)
    try:
        sender_email = request.form.get("email_sender")
        frequency = request.form.get("email_frequency")
        day_of_week = request.form.get("day_of_week")
        day_of_month = request.form.get("day_of_month")
        send_hour = request.form.get("send_hour", 8)
        send_minute = request.form.get("send_minute", 0)

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM email_settings")
        cur.execute("""
            INSERT INTO email_settings
            (sender_email, frequency, day_of_week, day_of_month, send_hour, send_minute)
            VALUES (?,?,?,?,?,?)
        """, (sender_email, frequency, day_of_week, day_of_month, send_hour, send_minute))
        conn.commit()
        conn.close()

        mail = current_app.extensions.get("mail")
        if mail:
            reload_scheduler(current_app, mail)

        return jsonify(success=True)
    except Exception as e:
        current_app.logger.error(f"Save email settings error: {e}")
        return jsonify(success=False)


# ---------------- DB SETTINGS ----------------
@bp.route("/api/save_db_settings", methods=["POST"])
def save_db_settings():
    if not session.get("logged_in"):
        return jsonify(success=False)
    try:
        host = request.form.get("db_host")
        db_name = request.form.get("db_name")
        db_user = request.form.get("db_user")
        db_pass = request.form.get("db_pass")

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM db_settings")
        cur.execute("""
            INSERT INTO db_settings (host, db_name, db_user, db_pass)
            VALUES (?,?,?,?)
        """, (host, db_name, db_user, db_pass))
        conn.commit()
        conn.close()
        return jsonify(success=True)
    except Exception as e:
        current_app.logger.error(f"Save db settings error: {e}")
        return jsonify(success=False)


# ---------------- DEPARTMENT HEADS ----------------
@bp.route("/api/add_department_head", methods=["POST"])
def add_department_head():
    if not session.get("logged_in"):
        return jsonify(success=False)
    try:
        dept_name = request.form.get("department_name")
        head_name = request.form.get("head_name")
        email = request.form.get("email")

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO department_heads (department_name, head_name, email)
            VALUES (?,?,?)
        """, (dept_name, head_name, email))
        conn.commit()
        conn.close()
        return jsonify(success=True)
    except Exception as e:
        current_app.logger.error(f"Add department head error: {e}")
        return jsonify(success=False)


@bp.route("/api/remove_department_head", methods=["POST"])
def remove_department_head():
    if not session.get("logged_in"):
        return jsonify(success=False)
    try:
        data = request.get_json()
        dept_name = data.get("department_name")

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM department_heads WHERE department_name=?", (dept_name,))
        conn.commit()
        conn.close()
        return jsonify(success=True)
    except Exception as e:
        current_app.logger.error(f"Remove department head error: {e}")
        return jsonify(success=False)


@bp.route("/api/list_department_heads", methods=["GET"])
def list_department_heads():
    if not session.get("logged_in"):
        return jsonify([])
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT department_name, head_name, email FROM department_heads")
        rows = cur.fetchall()
        conn.close()
        return jsonify([
            {"department_name": r[0], "head_name": r[1], "email": r[2]}
            for r in rows
        ])
    except Exception as e:
        current_app.logger.error(f"List department heads error: {e}")
        return jsonify([])
