import os
from flask import Blueprint, render_template, send_from_directory, abort, current_app, session, request, jsonify, redirect, url_for, flash
from werkzeug.utils import secure_filename
from db_app import get_conn

bp = Blueprint("library_bp", __name__)

BOOKS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'static', 'uploads', 'digital_books')
os.makedirs(BOOKS_DIR, exist_ok=True)

ALLOWED_EXTENSIONS = {'pdf'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@bp.route("/library/upload", methods=["POST"])
def upload_book():
    """Allow HODs to upload digital books."""
    role = (session.get("role") or "").lower()
    if role not in ['admin', 'hod']:
        return abort(403)
        
    if 'book_pdf' not in request.files:
        flash("❌ No file selected", "danger")
        return redirect(request.referrer)
        
    file = request.files['book_pdf']
    if file.filename == '':
        flash("❌ No file selected", "danger")
        return redirect(request.referrer)
        
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        file.save(os.path.join(BOOKS_DIR, filename))
        flash(f"✅ Book '{filename}' uploaded successfully", "success")
    else:
        flash("❌ Only PDF files are allowed", "danger")
        
    return redirect(request.referrer)

@bp.route("/library/delete/<filename>", methods=["POST"])
def delete_book(filename):
    """Allow HODs to delete digital books."""
    role = (session.get("role") or "").lower()
    if role not in ['admin', 'hod']:
        return abort(403)
        
    safe_filename = os.path.basename(filename)
    path = os.path.join(BOOKS_DIR, safe_filename)
    if os.path.exists(path):
        os.remove(path)
        flash(f"🗑️ Book '{safe_filename}' deleted", "info")
    else:
        flash("❌ File not found", "warning")
        
    return redirect(request.referrer)

@bp.route("/library/reader/<filename>")
def reader(filename):
    """Securely serve a digital book within a custom reader UI."""
    if not session.get("logged_in"):
        return abort(401)
    
    # Ensure filename is safe and exists
    safe_filename = os.path.basename(filename)
    if not os.path.exists(os.path.join(BOOKS_DIR, safe_filename)):
        return abort(404)
        
    return render_template("library/reader.html", filename=safe_filename)

@bp.route("/api/serve_book/<filename>")
def serve_book(filename):
    """Serve the PDF file only to logged-in users, preventing direct link sharing."""
    if not session.get("logged_in"):
        return abort(401)
        
    safe_filename = os.path.basename(filename)
    return send_from_directory(BOOKS_DIR, safe_filename)

@bp.route("/library/browse")
def browse():
    """List available digital books based on the user's Darajah or Role."""
    if not session.get("logged_in"):
        return abort(401)
        
    # In a real app, this would query a 'digital_books' table with permissions.
    # For now, we list all files in the directory for simplicity.
    books = []
    if os.path.exists(BOOKS_DIR):
        books = [f for f in os.listdir(BOOKS_DIR) if f.endswith('.pdf')]
    
    return render_template("library/browse.html", books=books)
