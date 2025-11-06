from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from werkzeug.security import check_password_hash
from db_app import get_conn

bp = Blueprint("auth_bp", __name__)

@bp.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE username = ?", (username,))
        row = cur.fetchone()
        conn.close()

        if not row:
            flash("❌ Invalid username", "danger")
            return render_template("index.html", hide_nav=True)

        # Map row to dict
        user = {
            "id": row[0],
            "username": row[1],
            "email": row[2],
            "role": row[3],
            "password_hash": row[4],
            "department_name": row[5],
            "class_name": row[6],
        }

        # Check password
        if not check_password_hash(user["password_hash"], password):
            flash("❌ Incorrect password", "danger")
            return render_template("index.html", hide_nav=True)

        # Save session
        session["logged_in"] = True
        session["username"] = user["username"]
        session["role"] = user["role"]
        session["department_name"] = user["department_name"]
        session["class_name"] = user["class_name"]

        # Redirect based on role
        if user["role"] == "admin":
            return redirect(url_for("dashboard_bp.dashboard"))
        elif user["role"] == "hod":
            return redirect(url_for("hod_dashboard_bp.dashboard"))
        elif user["role"] == "teacher":
            return redirect(url_for("teacher_dashboard_bp.dashboard"))
        else:
            flash("⚠️ Role not recognized.", "warning")
            return redirect(url_for("auth_bp.login"))

    return render_template("index.html", hide_nav=True)


@bp.route("/logout")
def logout():
    session.clear()
    flash("Logged out successfully.", "info")
    return redirect(url_for("auth_bp.login"))
