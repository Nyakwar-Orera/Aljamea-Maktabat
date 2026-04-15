# routes/profile.py
from flask import (
    Blueprint,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    session,
    current_app,
)
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename
from db_app import get_conn
import os

bp = Blueprint("profile_bp", __name__)


def _allowed_profile_file(filename: str) -> bool:
    """Check if file has an allowed image extension."""
    if not filename or "." not in filename:
        return False
    ext = filename.rsplit(".", 1)[1].lower()
    allowed = current_app.config.get(
        "ALLOWED_PROFILE_EXTENSIONS",
        {"png", "jpg", "jpeg", "gif"},
    )
    return ext in allowed


# View Profile
@bp.route("/profile", methods=["GET"])
def view_profile():
    """View user profile."""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    username = session.get("username")
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT username, email, role, department_name, class_name,
               profile_picture, notification_count, last_login, created_at
        FROM users WHERE username = ?
        """,
        (username,),
    )
    user = cur.fetchone()
    conn.close()

    if not user:
        flash("❌ User not found", "danger")
        return redirect(url_for("auth_bp.logout"))

    return render_template("profile.html", user=user)


# Edit Profile
@bp.route("/profile/edit", methods=["GET", "POST"])
def edit_profile():
    """Edit user profile (including profile picture)."""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    username = session.get("username")

    if request.method == "POST":
        new_email = (request.form.get("email") or "").strip()
        new_username = (request.form.get("username") or "").strip()
        new_department = (request.form.get("department_name") or "").strip()
        new_class = (request.form.get("class_name") or "").strip()

        if not new_email or not new_username:
            flash("❌ Username and email are required", "danger")
            return redirect(url_for("profile_bp.edit_profile"))

        conn = get_conn()
        cur = conn.cursor()

        # Get current picture so we keep it if user doesn't upload a new one
        cur.execute(
            "SELECT profile_picture FROM users WHERE username = ?",
            (username,),
        )
        row = cur.fetchone()
        current_picture = row[0] if row and row[0] else "images/avatar.png"

        # ---- Handle Profile Picture upload ----
        profile_picture = request.files.get("profile_picture")
        profile_path_rel = current_picture  # default: keep existing

        if profile_picture and profile_picture.filename:
            if not _allowed_profile_file(profile_picture.filename):
                flash(
                    "❌ Only image files (png, jpg, jpeg, gif) are allowed.",
                    "danger",
                )
                conn.close()
                return redirect(url_for("profile_bp.edit_profile"))

            upload_folder = current_app.config["PROFILE_UPLOAD_FOLDER"]
            os.makedirs(upload_folder, exist_ok=True)

            filename = secure_filename(profile_picture.filename)
            absolute_path = os.path.join(upload_folder, filename)
            profile_picture.save(absolute_path)

            # Store path relative to static folder, e.g. "images/profiles/filename.png"
            relative_path = os.path.relpath(
                absolute_path,
                current_app.static_folder,
            ).replace("\\", "/")

            profile_path_rel = relative_path

        # ---- Update DB ----
        cur.execute(
            """
            UPDATE users
               SET email = ?,
                   username = ?,
                   department_name = ?,
                   class_name = ?,
                   profile_picture = ?
             WHERE username = ?
            """,
            (
                new_email,
                new_username,
                new_department,
                new_class,
                profile_path_rel,
                username,
            ),
        )
        conn.commit()
        conn.close()

        # Update session username & picture
        session["username"] = new_username
        session["profile_picture"] = profile_path_rel or "images/avatar.png"

        flash("✅ Profile updated successfully", "success")
        return redirect(url_for("profile_bp.view_profile"))

    # GET: prefill the form
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT username, email, role, department_name, class_name, profile_picture
        FROM users WHERE username = ?
        """,
        (username,),
    )
    user = cur.fetchone()
    conn.close()

    return render_template("edit_profile.html", user=user)


# Change Password
@bp.route("/profile/change-password", methods=["GET", "POST"])
def change_password():
    """Change user password."""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    if request.method == "POST":
        old_password = request.form.get("old_password") or ""
        new_password = request.form.get("new_password") or ""
        confirm_password = request.form.get("confirm_password") or ""

        if new_password != confirm_password:
            flash("❌ New passwords do not match", "danger")
            return redirect(url_for("profile_bp.change_password"))

        username = session.get("username")
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT password_hash FROM users WHERE username = ?",
            (username,),
        )
        user = cur.fetchone()
        conn.close()

        if not user or not check_password_hash(user[0], old_password):
            flash("❌ Old password is incorrect", "danger")
            return redirect(url_for("profile_bp.change_password"))

        new_password_hash = generate_password_hash(new_password)

        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE users SET password_hash = ? WHERE username = ?",
            (new_password_hash, username),
        )
        conn.commit()
        conn.close()

        flash("✅ Password changed successfully", "success")
        return redirect(url_for("profile_bp.view_profile"))

    return render_template("change_password.html")
