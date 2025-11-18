from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, flash, current_app
from werkzeug.security import generate_password_hash
from werkzeug.utils import secure_filename
from config import Config
from db_app import get_conn
from tasks.scheduler import reload_scheduler
from tasks.monthly_reports import send_all_reports
import os

bp = Blueprint("admin_bp", __name__)

# ---------------- AUTH ----------------
@bp.route("/")
def index():
    if session.get("logged_in"):
        return redirect(url_for("dashboard_bp.dashboard"))
    return render_template("index.html", hide_nav=True)

@bp.route("/login", methods=["GET", "POST"])
def login():
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
    session.clear()
    flash("ℹ️ You have been signed out.", "info")
    return redirect(url_for("admin_bp.index"))

@bp.route("/settings")
def admin_settings():
    if not session.get("logged_in"):
        return redirect(url_for("admin_bp.index"))
    return render_template("settings.html")

# ---------------- UTIL: Audit logger ----------------
def _audit(actor, action, details=""):
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("INSERT INTO audit_log (actor, action, details) VALUES (?,?,?)", (actor, action, details))
        conn.commit(); conn.close()
    except Exception as e:
        current_app.logger.warning(f"Audit write failed: {e}")

# ---------------- USERS ----------------
@bp.route("/api/add_user", methods=["POST"])
def add_user():
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")
    try:
        username = request.form.get("username", "").strip()
        email = request.form.get("email", "").strip()
        role = request.form.get("role", "teacher").strip().lower()
        password = request.form.get("password", "").strip()
        department_name = request.form.get("department_name", "").strip() or None
        class_name = request.form.get("class_name", "").strip() or None

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

        password_hash = generate_password_hash(password)
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""
            INSERT INTO users (username, email, role, password_hash, department_name, class_name)
            VALUES (?,?,?,?,?,?)
        """, (username, email, role, password_hash, department_name, class_name))

        if role == "hod" and department_name:
            cur.execute("""
                INSERT OR IGNORE INTO department_heads (department_name, head_name, email)
                VALUES (?,?,?)
            """, (department_name, username, email))

        conn.commit(); conn.close()

        # Send account email from template (if mail configured)
        try:
            from flask_mail import Message
            mail = current_app.extensions.get("mail")
            if mail:
                # Load template
                conn = get_conn(); cur = conn.cursor()
                cur.execute("SELECT subject, html FROM email_templates WHERE template_key='account_created'")
                row = cur.fetchone()
                cur.close(); conn.close()
                subject = row[0] if row else "Your Maktabat al-Jamea Account"
                html = (row[1] if row else
                        "<p>Dear {{ username }},</p><p>Your account has been created.<br>"
                        "Role: {{ role }}<br>Username: {{ username }}<br>Password: {{ password }}</p>"
                        "<p>Login: {{ login_url }}</p>")
                # Very simple token replacement
                html = (html.replace("{{ username }}", username)
                            .replace("{{ role }}", role.title())
                            .replace("{{ password }}", password)
                            .replace("{{ login_url }}", url_for('auth_bp.login', _external=True)))
                msg = Message(subject=subject, recipients=[email], html=html)
                mail.send(msg)
                current_app.logger.info(f"Account email sent to {email}.")
        except Exception as e:
            current_app.logger.warning(f"⚠️ Could not send email to {email}: {e}")

        _audit(session.get("user","admin"), "add_user", f"{username}/{role}")
        flash(f"✅ User '{username}' added successfully and email sent to {email}.", "success")
        return jsonify(success=True)
    except Exception as e:
        current_app.logger.error(f"Add user error: {e}")
        return jsonify(success=False, error=str(e))

@bp.route("/api/remove_user", methods=["POST"])
def remove_user():
    if not session.get("logged_in"):
        return jsonify(success=False)
    try:
        data = request.get_json()
        username = data.get("username")
        conn = get_conn(); cur = conn.cursor()
        cur.execute("DELETE FROM users WHERE username=?", (username,))
        conn.commit(); conn.close()
        _audit(session.get("user","admin"), "remove_user", username)
        flash(f"🗑️ User '{username}' removed successfully.", "info")
        return jsonify(success=True)
    except Exception as e:
        current_app.logger.error(f"Remove user error: {e}")
        return jsonify(success=False)

@bp.route("/api/list_users", methods=["GET"])
def list_users():
    if not session.get("logged_in"):
        return jsonify([])
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""
            SELECT username, email, role, department_name, class_name
            FROM users
            ORDER BY role ASC, username ASC
        """)
        rows = cur.fetchall(); conn.close()
        return jsonify([
            {"username": r[0], "email": r[1], "role": r[2], "department_name": r[3], "class_name": r[4]}
            for r in rows
        ])
    except Exception as e:
        current_app.logger.error(f"List users error: {e}")
        return jsonify([])

# ---------------- BRANDING & SITE SETTINGS ----------------
@bp.route("/api/get_site_settings", methods=["GET"])
def get_site_settings():
    if not session.get("logged_in"):
        return jsonify({})
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT org_name, site_name, theme_color, footer_text, logo_path FROM site_settings LIMIT 1")
    row = cur.fetchone(); conn.close()
    if not row:
        return jsonify({})
    return jsonify({
        "org_name": row[0], "site_name": row[1], "theme_color": row[2],
        "footer_text": row[3], "logo_path": row[4]
    })

@bp.route("/api/save_site_settings", methods=["POST"])
def save_site_settings():
    if not session.get("logged_in"):
        return jsonify(success=False)
    try:
        org_name = request.form.get("org_name","").strip()
        site_name = request.form.get("site_name","").strip()
        theme_color = request.form.get("theme_color","#004080").strip()
        footer_text = request.form.get("footer_text","").strip()
        conn = get_conn(); cur = conn.cursor()
        # Always keep a single row
        cur.execute("DELETE FROM site_settings")
        cur.execute("""
            INSERT INTO site_settings (org_name, site_name, theme_color, footer_text, logo_path)
            VALUES (?,?,?,?, 'images/logo.png')
        """, (org_name or "Al-Jamea tus-Saifiyah", site_name or "Maktabat al-Jamea", theme_color, footer_text))
        conn.commit(); conn.close()
        _audit(session.get("user","admin"), "save_site_settings", f"{site_name}/{theme_color}")
        return jsonify(success=True)
    except Exception as e:
        current_app.logger.error(f"Save site settings error: {e}")
        return jsonify(success=False)

@bp.route("/api/upload_logo", methods=["POST"])
def upload_logo():
    if not session.get("logged_in"):
        return jsonify(success=False)
    try:
        f = request.files.get("logo_file")
        if not f:
            return jsonify(success=False, error="No file provided")
        filename = secure_filename(f.filename)
        if not filename.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".webp")):
            return jsonify(success=False, error="Unsupported file type")
        static_dir = os.path.join(current_app.root_path, "static", "branding")
        os.makedirs(static_dir, exist_ok=True)
        target = os.path.join(static_dir, filename)
        f.save(target)

        rel_path = os.path.join("branding", filename).replace("\\", "/")
        conn = get_conn(); cur = conn.cursor()
        cur.execute("UPDATE site_settings SET logo_path=?", (rel_path,))
        conn.commit(); conn.close()
        _audit(session.get("user","admin"), "upload_logo", rel_path)
        return jsonify(success=True, logo_path=rel_path)
    except Exception as e:
        current_app.logger.error(f"Upload logo error: {e}")
        return jsonify(success=False)

# ---------------- EMAIL SETTINGS (schedule) ----------------
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

        conn = get_conn(); cur = conn.cursor()
        cur.execute("DELETE FROM email_settings")
        cur.execute("""
            INSERT INTO email_settings
            (sender_email, frequency, day_of_week, day_of_month, send_hour, send_minute)
            VALUES (?,?,?,?,?,?)
        """, (sender_email, frequency, day_of_week, day_of_month, send_hour, send_minute))
        conn.commit(); conn.close()

        mail = current_app.extensions.get("mail")
        if mail:
            reload_scheduler(current_app, mail)

        _audit(session.get("user","admin"), "save_email_settings", frequency or "")
        return jsonify(success=True)
    except Exception as e:
        current_app.logger.error(f"Save email settings error: {e}")
        return jsonify(success=False)

# ---------------- EMAIL TEMPLATES ----------------
@bp.route("/api/get_email_templates", methods=["GET"])
def get_email_templates():
    if not session.get("logged_in"):
        return jsonify([])
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT template_key, subject, html FROM email_templates ORDER BY template_key ASC")
    rows = cur.fetchall(); conn.close()
    return jsonify([{"template_key": r[0], "subject": r[1], "html": r[2]} for r in rows])

@bp.route("/api/save_email_template", methods=["POST"])
def save_email_template():
    if not session.get("logged_in"):
        return jsonify(success=False)
    try:
        key = request.form.get("template_key","").strip()
        subject = request.form.get("subject","").strip()
        html = request.form.get("html","").strip()
        if not key or not subject or not html:
            return jsonify(success=False, error="Missing template fields")
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""
            INSERT INTO email_templates (template_key, subject, html)
            VALUES (?,?,?)
            ON CONFLICT(template_key) DO UPDATE SET subject=excluded.subject, html=excluded.html
        """, (key, subject, html))
        conn.commit(); conn.close()
        _audit(session.get("user","admin"), "save_email_template", key)
        return jsonify(success=True)
    except Exception as e:
        current_app.logger.error(f"Save email template error: {e}")
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
        conn = get_conn(); cur = conn.cursor()
        cur.execute("DELETE FROM db_settings")
        cur.execute("INSERT INTO db_settings (host, db_name, db_user, db_pass) VALUES (?,?,?,?)",
                    (host, db_name, db_user, db_pass))
        conn.commit(); conn.close()
        _audit(session.get("user","admin"), "save_db_settings", host or "")
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
        conn = get_conn(); cur = conn.cursor()
        cur.execute("INSERT INTO department_heads (department_name, head_name, email) VALUES (?,?,?)",
                    (dept_name, head_name, email))
        conn.commit(); conn.close()
        _audit(session.get("user","admin"), "add_department_head", dept_name or "")
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
        conn = get_conn(); cur = conn.cursor()
        cur.execute("DELETE FROM department_heads WHERE department_name=?", (dept_name,))
        conn.commit(); conn.close()
        _audit(session.get("user","admin"), "remove_department_head", dept_name or "")
        return jsonify(success=True)
    except Exception as e:
        current_app.logger.error(f"Remove department head error: {e}")
        return jsonify(success=False)

@bp.route("/api/list_department_heads", methods=["GET"])
def list_department_heads():
    if not session.get("logged_in"):
        return jsonify([])
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT department_name, head_name, email FROM department_heads")
        rows = cur.fetchall(); conn.close()
        return jsonify([{"department_name": r[0], "head_name": r[1], "email": r[2]} for r in rows])
    except Exception as e:
        current_app.logger.error(f"List department heads error: {e}")
        return jsonify([])

# ---------------- DIAGNOSTICS & MANUAL ACTIONS ----------------
@bp.route("/api/test_email", methods=["POST"])
def test_email():
    if not session.get("logged_in"):
        return jsonify(success=False)
    try:
        to_addr = request.form.get("to")
        from flask_mail import Message
        mail = current_app.extensions.get("mail")
        if not mail:
            return jsonify(success=False, error="Mail not configured")
        msg = Message(subject="Maktabat: Test Email", recipients=[to_addr], html="<p>This is a test email.</p>")
        mail.send(msg)
        _audit(session.get("user","admin"), "test_email", to_addr or "")
        return jsonify(success=True)
    except Exception as e:
        current_app.logger.error(f"Test email error: {e}")
        return jsonify(success=False, error=str(e))

@bp.route("/api/list_audit", methods=["GET"])
def list_audit():
    if not session.get("logged_in"):
        return jsonify([])
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT ts, actor, action, details FROM audit_log ORDER BY ts DESC LIMIT 100")
    rows = cur.fetchall(); conn.close()
    return jsonify([{"ts": r[0], "actor": r[1], "action": r[2], "details": r[3]} for r in rows])

# ---------------- MANUAL EMAIL TRIGGER ----------------
@bp.route("/run_email_reports_now", methods=["POST"])
def run_email_reports_now():
    if not session.get("logged_in"):
        flash("Unauthorized access.", "danger")
        return redirect(url_for("admin_bp.index"))
    try:
        mail = current_app.extensions.get("mail")
        send_all_reports(current_app, mail)
        _audit(session.get("user","admin"), "run_email_reports_now")
        flash("✅ Email reports sent successfully. Check your inbox/logs.", "success")
    except Exception as e:
        current_app.logger.exception("Manual email trigger failed")
        flash(f"❌ Error sending reports: {e}", "danger")
    return redirect(url_for("admin_bp.admin_settings"))
