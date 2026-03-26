# routes/library.py — PRODUCTION HARDENED
import os
import logging
from flask import (
    Blueprint, render_template, send_from_directory, abort,
    current_app, session, request, redirect, url_for, flash,
)
from werkzeug.utils import secure_filename
from db_app import get_conn

bp = Blueprint("library_bp", __name__)
logger = logging.getLogger(__name__)

BOOKS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "static", "uploads", "digital_books"
)
os.makedirs(BOOKS_DIR, exist_ok=True)

ALLOWED_EXTENSIONS = {"pdf"}
MAX_BOOK_SIZE = 100 * 1024 * 1024  # 100MB


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def _require_role(*roles):
    """Check if current user has one of the required roles."""
    role = (session.get("role") or "").lower()
    if role not in roles:
        abort(403)


def _audit_log(action: str, details: str = ""):
    """Log library actions for audit trail."""
    username = session.get("username", "unknown")
    logger.info(f"[AUDIT] {username} | {action} | {details}")
    try:
        conn = get_conn()
        conn.execute(
            "INSERT INTO audit_log (actor, action, details, created_at) VALUES (?, ?, ?, datetime(\'now\'))",
            (username, action, details)
        )
        conn.commit()
    except Exception:
        pass  # Audit table may not exist yet


@bp.route("/library/upload", methods=["POST"])
def upload_book():
    """Allow HODs/Admins to upload digital books with size validation."""
    _require_role("admin", "hod")

    if "book_pdf" not in request.files:
        flash("❌ No file selected", "danger")
        return redirect(request.referrer or url_for("library_bp.browse"))

    file = request.files["book_pdf"]
    if file.filename == "":
        flash("❌ No file selected", "danger")
        return redirect(request.referrer or url_for("library_bp.browse"))

    if not allowed_file(file.filename):
        flash("❌ Only PDF files are allowed", "danger")
        return redirect(request.referrer or url_for("library_bp.browse"))

    # Check file size
    file.seek(0, os.SEEK_END)
    size = file.tell()
    file.seek(0)
    if size > MAX_BOOK_SIZE:
        flash(f"❌ File too large. Maximum size is {MAX_BOOK_SIZE // (1024*1024)}MB", "danger")
        return redirect(request.referrer or url_for("library_bp.browse"))

    filename = secure_filename(file.filename)
    file.save(os.path.join(BOOKS_DIR, filename))
    _audit_log("BOOK_UPLOAD", f"Uploaded: {filename} ({size // 1024}KB)")
    flash(f"✅ Book \'{filename}\' uploaded successfully", "success")
    return redirect(request.referrer or url_for("library_bp.browse"))


@bp.route("/library/delete/<filename>", methods=["POST"])
def delete_book(filename):
    """Allow HODs/Admins to delete digital books."""
    _require_role("admin", "hod")

    safe_filename = os.path.basename(filename)
    path = os.path.join(BOOKS_DIR, safe_filename)
    if os.path.exists(path):
        os.remove(path)
        _audit_log("BOOK_DELETE", f"Deleted: {safe_filename}")
        flash(f"🗑️ Book \'{safe_filename}\' deleted", "info")
    else:
        flash("❌ File not found", "warning")

    return redirect(request.referrer or url_for("library_bp.browse"))


@bp.route("/library/reader/<filename>")
def reader(filename):
    """Securely serve a digital book within a custom reader UI."""
    if not session.get("logged_in"):
        return abort(401)

    safe_filename = os.path.basename(filename)
    if not os.path.exists(os.path.join(BOOKS_DIR, safe_filename)):
        return abort(404)

    return render_template("library/reader.html", filename=safe_filename)


@bp.route("/api/serve_book/<filename>")
def serve_book(filename):
    """Serve the PDF file only to logged-in users."""
    if not session.get("logged_in"):
        return abort(401)

    safe_filename = os.path.basename(filename)
    return send_from_directory(BOOKS_DIR, safe_filename)


@bp.route("/library/browse")
def browse():
    """List available digital books."""
    if not session.get("logged_in"):
        return abort(401)

    books = []
    if os.path.exists(BOOKS_DIR):
        books = sorted([f for f in os.listdir(BOOKS_DIR) if f.endswith(".pdf")])

    return render_template("library/browse.html", books=books)
