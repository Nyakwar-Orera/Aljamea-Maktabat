from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, flash, current_app, send_file, Response
from werkzeug.security import generate_password_hash
from werkzeug.utils import secure_filename
from config import Config
from db_app import get_conn
from tasks.scheduler import reload_scheduler
from tasks.monthly_reports import send_all_reports
import os
import sqlite3
import csv
import io
from datetime import datetime
import pandas as pd
from io import BytesIO

bp = Blueprint("admin_bp", __name__, template_folder='templates/admin')

# ========== SHARED UTILITIES ==========
import functools
from typing import Optional, Dict, Any
from flask import jsonify

def require_admin(f):
    """Decorator: require logged_in session"""
    @functools.wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get("logged_in"):
            return jsonify(success=False, error="Unauthorized"), 401
        return f(*args, **kwargs)
    return decorated_function

def db_operation(func):
    """Context manager for DB operations with cleanup"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        conn = None
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            if conn:
                conn.rollback()
            current_app.logger.error(f"DB error in {func.__name__}: {e}")
            raise
        finally:
            if conn:
                conn.close()
    return wrapper

def audit_log(actor: str, action: str, details: str = ""):
    """Centralized audit logging"""
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO audit_log (actor, action, details) VALUES (?, ?, ?)",
                (actor, action, details)
            )
            conn.commit()
    except Exception as e:
        current_app.logger.warning(f"Audit log failed: {e}")


# ---------------- AUTH ----------------
@bp.route("/")
def index():
    if session.get("logged_in"):
        return redirect(url_for("dashboard_bp.dashboard"))
    return render_template("index.html", hide_nav=True)

@bp.route("/admin")
def admin_home():
    return redirect(url_for("admin_bp.admin_settings") if session.get("logged_in") else "admin_bp.index")


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
    return render_template("admin/settings.html", campus_branches=Config.CAMPUS_BRANCHES)


# ---------------- UTIL: Audit logger ----------------
def _audit(actor, action, details=""):
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO audit_log (actor, action, details) VALUES (?,?,?)",
            (actor, action, details),
        )
        conn.commit()
    except Exception as e:
        current_app.logger.warning(f"Audit write failed: {e}")
    finally:
        if conn:
            conn.close()


# ---------------- CSV UPLOAD FOR TEACHER-DARAJAH MAPPING ----------------
@bp.route("/api/upload_csv_users", methods=["POST"])
def upload_csv_users():
    """
    Upload CSV file with teacher-darajah mapping.
    Expected CSV columns: ITS (username), Name, email, darajah, role, class teacher
    ITS is used as username, Name as display name, darajah for darajah_name
    Role is determined: if role column is "class teacher" then role='teacher'
    """
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")

    if 'csv_file' not in request.files:
        return jsonify(success=False, error="No file uploaded")
    
    file = request.files['csv_file']
    if file.filename == '':
        return jsonify(success=False, error="No file selected")
    
    if not file.filename.lower().endswith('.csv'):
        return jsonify(success=False, error="File must be a CSV")
    
    conn = None
    try:
        # Read CSV content
        stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)
        csv_reader = csv.DictReader(stream)
        
        # Validate required columns (updated column names)
        required_columns = ['ITS', 'Name', 'email', 'darajah', 'role', 'class teacher']
        for col in required_columns:
            if col not in csv_reader.fieldnames:
                return jsonify(success=False, error=f"CSV missing required column: {col}")
        
        conn = get_conn()
        cur = conn.cursor()
        
        user_import_stats = {
            'total': 0,
            'added': 0,
            'updated': 0,
            'skipped': 0,
            'teacher_mappings': 0,
            'student_mappings': 0,
            'errors': []
        }
        
        # Process each row
        for row_num, row in enumerate(csv_reader, start=2):  # row 1 is header
            user_import_stats['total'] += 1
            
            try:
                # Extract and clean data
                its = str(row['ITS']).strip()
                name = str(row['Name']).strip()
                email = str(row['email']).strip().lower()
                darajah_name = str(row['darajah']).strip()
                role_col = str(row['role']).strip().lower()
                class_teacher = str(row['class teacher']).strip().upper()
                
                # Skip if ITS is empty
                if not its:
                    user_import_stats['skipped'] += 1
                    user_import_stats['errors'].append(f"Row {row_num}: ITS is empty")
                    continue
                
                # Determine role: if role column says "class teacher", set as teacher
                if "teacher" in role_col.lower() or class_teacher == "YES":
                    role = "teacher"
                elif role_col == "hod":
                    role = "hod"
                elif role_col == "admin":
                    role = "admin"
                else:
                    role = "student"
                
                # Generate a default password (first 4 chars of ITS + "123")
                default_password = its[:4] + "123" if len(its) >= 4 else its + "123"
                password_hash = generate_password_hash(default_password)
                
                # Check if user already exists
                cur.execute("SELECT username FROM users WHERE username = ?", (its,))
                existing = cur.fetchone()
                
                if existing:
                    # Update existing user
                    cur.execute("""
                        UPDATE users 
                        SET email = ?, role = ?, class_name = ?, 
                            darajah_name = ?, password_hash = ?, teacher_name = ?
                        WHERE username = ?
                    """, (email, role, darajah_name, darajah_name, password_hash, name, its))
                    user_import_stats['updated'] += 1
                else:
                    # Insert new user
                    cur.execute("""
                        INSERT INTO users (username, email, role, password_hash, class_name, darajah_name, teacher_name)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (its, email, role, password_hash, darajah_name, darajah_name, name))
                    user_import_stats['added'] += 1
                
                # ===== CREATE MAPPING RECORDS =====
                if role == "teacher":
                    # Check if teacher-darajah mapping already exists
                    cur.execute("""
                        SELECT id FROM teacher_darajah_mapping 
                        WHERE teacher_username = ? AND darajah_name = ?
                    """, (its, darajah_name))
                    
                    if not cur.fetchone():
                        # Insert teacher-darajah mapping
                        cur.execute("""
                            INSERT INTO teacher_darajah_mapping 
                            (teacher_username, teacher_name, darajah_name, teacher_email, role, academic_year, campus_branch)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (its, name, darajah_name, email, 'class_teacher', Config.CURRENT_ACADEMIC_YEAR(), Config.CAMPUS_BRANCH))
                        user_import_stats['teacher_mappings'] += 1
                
                elif role == "student":
                    # Check if student-darajah mapping already exists
                    cur.execute("""
                        SELECT id FROM student_darajah_mapping 
                        WHERE student_username = ? AND darajah_name = ?
                    """, (its, darajah_name))
                    
                    if not cur.fetchone():
                        # Insert student-darajah mapping
                        cur.execute("""
                            INSERT INTO student_darajah_mapping 
                            (student_username, student_name, darajah_name, academic_year)
                            VALUES (?, ?, ?, ?)
                        """, (its, name, darajah_name, Config.CURRENT_ACADEMIC_YEAR()))
                        user_import_stats['student_mappings'] += 1
                
            except Exception as e:
                user_import_stats['skipped'] += 1
                user_import_stats['errors'].append(f"Row {row_num}: {str(e)}")
                continue
        
        conn.commit()
        
        # Create summary message
        summary = f"Processed {user_import_stats['total']} records: {user_import_stats['added']} added, {user_import_stats['updated']} updated, {user_import_stats['teacher_mappings']} teacher mappings, {user_import_stats['student_mappings']} student mappings, {user_import_stats['skipped']} skipped"
        if user_import_stats['errors']:
            summary += f". {len(user_import_stats['errors'])} errors occurred."
        
        _audit(session.get("user", "admin"), "upload_csv_users", summary)
        
        return jsonify({
            'success': True,
            'message': summary,
            'stats': user_import_stats
        })
        
    except Exception as e:
        current_app.logger.error(f"CSV upload error: {e}")
        return jsonify(success=False, error=f"Error processing CSV: {str(e)}")
    finally:
        if conn:
            conn.close()


@bp.route("/programs")
def admin_programs():
    if not session.get("logged_in"):
        return redirect(url_for("admin_bp.index"))
    from config import Config
    conn = get_conn()
    cur = conn.cursor()
    # Get all distinct darajahs that have students or mapping
    cur.execute("""
        SELECT DISTINCT darajah_name FROM teacher_darajah_mapping
        WHERE darajah_name IS NOT NULL AND darajah_name != ''
        UNION
        SELECT DISTINCT darajah_name FROM users
        WHERE darajah_name IS NOT NULL AND darajah_name != ''
        ORDER BY darajah_name
    """)
    all_darajahs = [r[0] for r in cur.fetchall()]

    # Get teacher mappings for the UI
    cur.execute("SELECT darajah_name, teacher_name, role FROM teacher_darajah_mapping")
    teacher_mappings = [
        {"darajah": r[0], "name": r[1], "role": r[2]}
        for r in cur.fetchall()
    ]
    conn.close()

    return render_template("library_programs.html",
                           current_academic_year=Config.CURRENT_ACADEMIC_YEAR(),
                           campus_branches=Config.CAMPUS_BRANCHES,
                           all_darajahs=all_darajahs,
                           teacher_mappings=teacher_mappings)


# ---------------- LIBRARY PROGRAM MANAGEMENT ----------------
@bp.route("/api/list_programs", methods=["GET"])
def list_programs():
    if not session.get("logged_in"):
        return jsonify([])
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, title, date, marks, marks_category, darajahs, marhalas, 
                   frequency, venue, conductor, department_note, academic_year, created_at, campus_branch
            FROM library_programs 
            ORDER BY 
              CASE WHEN LOWER(title) LIKE '%books issued%' OR LOWER(title) LIKE '%book issue%' THEN 0 ELSE 1 END,
              date DESC, campus_branch ASC
            """
        )
        rows = cur.fetchall()
        return jsonify([
            {
                "id": r['id'],
                "title": r['title'],
                "date": r['date'],
                "marks": float(r['marks']),
                "marks_category": r['marks_category'],
                "darajahs": r['darajahs'],
                "marhalas": r['marhalas'],
                "frequency": r['frequency'],
                "venue": r['venue'],
                "conductor": r['conductor'],
                "department_note": r['department_note'],
                "academic_year": r['academic_year'],
                "created_at": r['created_at'],
                "campus_branch": r['campus_branch']
            }
            for r in rows
        ])
    except Exception as e:
        current_app.logger.error(f"List programs error: {e}")
        return jsonify([])
    finally:
        if conn:
            conn.close()


@bp.route("/api/add_program", methods=["POST"])
def add_program():
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")
    conn = None
    try:
        title = request.form.get("title", "").strip()
        date = request.form.get("date", "").strip()
        marks = float(request.form.get("marks", 0))
        marks_category = request.form.get("marks_category", "Manual").strip()
        darajahs = request.form.get("darajahs", "All").strip() # This could be JSON from front-end
        marhalas = request.form.get("marhalas", "All").strip()
        frequency = request.form.get("frequency", "monthly").strip()
        venue = request.form.get("venue", "").strip()
        conductor = request.form.get("conductor", "").strip()
        note = request.form.get("department_note", "").strip()
        academic_year = request.form.get("academic_year", Config.CURRENT_ACADEMIC_YEAR()).strip()
        campus_branch = request.form.get("campus_branch", "Global").strip()

        if not title or not date:
            return jsonify(success=False, error="Title and Date are required")

        conn = get_conn()
        cur = conn.cursor()
        
        # Check total marks constraint for this branch (Global + this branch)
        cur.execute("""
            SELECT COALESCE(SUM(marks), 0) FROM library_programs 
            WHERE academic_year = ? AND (campus_branch = 'Global' OR campus_branch = ?)
        """, (academic_year, campus_branch))
        total_allotted = cur.fetchone()[0] or 0
        
        if total_allotted + marks > 100.0:
            remaining = max(0, 100.0 - total_allotted)
            return jsonify(success=False, error=f"Total mark allotment cannot exceed 100%. Only {remaining:.1f}% remaining.")

        cur.execute("""
            INSERT INTO library_programs 
            (title, date, marks, marks_category, darajahs, marhalas, frequency, venue, conductor, department_note, academic_year, campus_branch)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (title, date, marks, marks_category, darajahs, marhalas, frequency, venue, conductor, note, academic_year, campus_branch))
        
        conn.commit()
        _audit(session.get("user", "admin"), "add_program", f"Program: {title} ({marks}%)")
        return jsonify(success=True, message=f"Program '{title}' added successfully.")
    except Exception as e:
        current_app.logger.error(f"Add program error: {e}")
        return jsonify(success=False, error=str(e))
    finally:
        if conn:
            conn.close()


@bp.route("/api/remove_program", methods=["POST"])
def remove_program():
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")
    conn = None
    try:
        data = request.get_json()
        program_id = data.get("id")
        
        conn = get_conn()
        cur = conn.cursor()
        
        # Get program details for audit
        cur.execute("SELECT title FROM library_programs WHERE id = ?", (program_id,))
        row = cur.fetchone()
        if not row:
            return jsonify(success=False, error="Program not found")
        
        title = row[0]
        
        # Delete program (cascades to attendance if configured, otherwise manual delete)
        cur.execute("DELETE FROM library_program_attendance WHERE program_id = ?", (program_id,))
        cur.execute("DELETE FROM library_programs WHERE id = ?", (program_id,))
        
        conn.commit()
        _audit(session.get("user", "admin"), "remove_program", f"Program: {title}")
        return jsonify(success=True, message=f"Program '{title}' removed successfully. Mark allotment has been released.")
    except Exception as e:
        current_app.logger.error(f"Remove program error: {e}")
        return jsonify(success=False, error=str(e))
    finally:
        if conn:
            conn.close()


@bp.route("/api/upload_program_marks", methods=["POST"])
def upload_program_marks():
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")
    
    program_id = request.form.get("program_id")
    academic_year = request.form.get("academic_year", Config.CURRENT_ACADEMIC_YEAR())
    
    if 'csv_file' not in request.files:
        return jsonify(success=False, error="No file uploaded")
    
    file = request.files['csv_file']
    if file.filename == '':
        return jsonify(success=False, error="No file selected")
    
    try:
        from services.marks_service import process_program_marks_upload
        result = process_program_marks_upload(file, program_id, academic_year, uploaded_by=session.get("user", "admin"))
        return jsonify(result)
    except Exception as e:
        current_app.logger.error(f"Program marks upload error: {e}")
        return jsonify(success=False, error=str(e))


@bp.route("/api/get_program", methods=["GET"])
def get_program():
    """Get details of a single program by ID."""
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")
    program_id = request.args.get("id")
    if not program_id:
        return jsonify(success=False, error="Program ID required")
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, title, date, marks, marks_category, darajahs, marhalas,
                   frequency, venue, conductor, department_note, academic_year, created_at, campus_branch
            FROM library_programs WHERE id = ?
        """, (program_id,))
        row = cur.fetchone()
        if not row:
            return jsonify(success=False, error="Program not found")

        # Count students with marks recorded
        cur.execute("""
            SELECT COUNT(*), COALESCE(SUM(marks), 0)
            FROM library_program_attendance
            WHERE program_id = ? OR program_name = (SELECT title FROM library_programs WHERE id = ?)
        """, (program_id, program_id))
        att = cur.fetchone()

        return jsonify({
            "id": row[0], "title": row[1], "date": row[2], "marks": row[3],
            "marks_category": row[4], "darajahs": row[5], "marhalas": row[6],
            "frequency": row[7], "venue": row[8], "conductor": row[9],
            "department_note": row[10], "academic_year": row[11], "created_at": row[12],
            "campus_branch": row[13],
            "students_recorded": att[0] if att else 0,
            "marks_distributed": att[1] if att else 0,
        })
    except Exception as e:
        current_app.logger.error(f"Get program error: {e}")
        return jsonify(success=False, error=str(e))
    finally:
        if conn:
            conn.close()


@bp.route("/api/edit_program_marks", methods=["POST"])
def edit_program_marks():
    """
    Edit marks allotted to a program.
    Rules:
      - Past programs (date < today): marks can only be increased, not decreased.
      - Future programs (date >= today): marks can be changed freely.
      - Total marks for the academic year must remain <= 100%.
    """
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")
    conn = None
    try:
        data = request.get_json()
        program_id = data.get("id")
        new_marks = float(data.get("marks", 0))

        if not program_id:
            return jsonify(success=False, error="Program ID required")
        if new_marks <= 0 or new_marks > 100:
            return jsonify(success=False, error="Marks must be between 0.01 and 100")

        conn = get_conn()
        cur = conn.cursor()

        # Get current program
        cur.execute("SELECT title, date, marks, academic_year, campus_branch FROM library_programs WHERE id = ?", (program_id,))
        row = cur.fetchone()
        if not row:
            return jsonify(success=False, error="Program not found")

        title, prog_date, old_marks, academic_year, campus_branch = row[0], row[1], row[2], row[3], row[4]

        # Determine if program is in the past
        from datetime import date as _date
        today = _date.today().isoformat()
        is_past = prog_date < today

        if is_past and new_marks < old_marks:
            return jsonify(
                success=False,
                error=f"Past programs cannot have marks reduced. Current: {old_marks}%. You may only increase marks for past programs."
            )

        # Check 100% cap: total for this AY minus old_marks + new_marks <= 100
        cur.execute("""
            SELECT COALESCE(SUM(marks), 0) FROM library_programs 
            WHERE academic_year = ? AND id != ? AND (campus_branch = 'Global' OR campus_branch = ?)
        """, (academic_year, program_id, campus_branch))
        other_total = cur.fetchone()[0]
        
        if other_total + new_marks > 100.0:
            remaining = max(0, 100.0 - other_total)
            return jsonify(
                success=False,
                error=f"Cannot allot {new_marks}%. Only {remaining:.1f}% remaining."
            )

        # Apply the update
        cur.execute("UPDATE library_programs SET marks = ? WHERE id = ?", (new_marks, program_id))
        conn.commit()
        _audit(session.get("user", "admin"), "edit_program_marks",
               f"Program '{title}': {old_marks}% → {new_marks}%")

        return jsonify(success=True, message=f"Marks for '{title}' updated to {new_marks}%.")
    except Exception as e:
        current_app.logger.error(f"Edit program marks error: {e}")
        return jsonify(success=False, error=str(e))
    finally:
        if conn:
            conn.close()


@bp.route("/api/edit_program", methods=["POST"])
def edit_program():
    """
    Full edit of a program (title, date, venue, conductor, notes, marhalas, frequency).
    Any program (past or future) can have its metadata edited.
    Marks changes still follow the same rules as edit_program_marks.
    """
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")
    conn = None
    try:
        data = request.get_json()
        program_id = data.get("id")
        if not program_id:
            return jsonify(success=False, error="Program ID required")

        conn = get_conn()
        cur = conn.cursor()

        # Fetch existing program
        cur.execute("SELECT title, date, marks, academic_year, campus_branch FROM library_programs WHERE id = ?", (program_id,))
        existing = cur.fetchone()
        if not existing:
            return jsonify(success=False, error="Program not found")

        old_title, old_date, old_marks, academic_year, old_branch = existing[0], existing[1], existing[2], existing[3], existing[4]

        # Extract fields (fallback to existing if not provided)
        new_title = (data.get("title") or "").strip() or old_title
        new_date = (data.get("date") or "").strip() or old_date
        new_venue = (data.get("venue") or "").strip()
        new_conductor = (data.get("conductor") or "").strip()
        new_darajahs = data.get("darajahs", "All")
        new_marhalas = data.get("marhalas", "All")
        new_note = (data.get("note") or "").strip()
        new_frequency = (data.get("frequency") or "once").strip()
        new_marks_category = (data.get("marks_category") or "Manual").strip()
        new_campus_branch = (data.get("campus_branch") or old_branch).strip()

        # Handle marks changes with the same protection rules
        marks_changed = False
        new_marks = data.get("marks")
        if new_marks is not None:
            new_marks = float(new_marks)
            from datetime import date as _date
            today = _date.today().isoformat()
            is_past = old_date < today

            if is_past and new_marks < old_marks:
                return jsonify(
                    success=False,
                    error=f"Past programs cannot have marks reduced. Current: {old_marks}%. You may only increase marks for past programs."
                )

            # Check 100% cap
            cur.execute("SELECT COALESCE(SUM(marks), 0) FROM library_programs WHERE academic_year = ? AND id != ?",
                        (academic_year, program_id))
            other_total = cur.fetchone()[0]
            if other_total + new_marks > 100.0:
                remaining = max(0, 100.0 - other_total)
                return jsonify(
                    success=False,
                    error=f"Cannot allot {new_marks}%. Only {remaining:.1f}% remaining."
                )
            marks_changed = True
        else:
            new_marks = old_marks

        # Update the program
        cur.execute("""
            UPDATE library_programs
            SET title = ?, date = ?, marks = ?, marks_category = ?,
                venue = ?, conductor = ?, department_note = ?,
                marhalas = ?, darajahs = ?, frequency = ?, campus_branch = ?
            WHERE id = ?
        """, (new_title, new_date, new_marks, new_marks_category,
              new_venue, new_conductor, new_note,
              new_marhalas if isinstance(new_marhalas, str) else str(new_marhalas),
              new_darajahs if isinstance(new_darajahs, str) else str(new_darajahs),
              new_frequency, new_campus_branch, program_id))
        conn.commit()

        changes = []
        if new_title != old_title: changes.append(f"title: '{old_title}'→'{new_title}'")
        if new_date != old_date: changes.append(f"date: {old_date}→{new_date}")
        if marks_changed: changes.append(f"marks: {old_marks}%→{new_marks}%")
        _audit(session.get("user", "admin"), "edit_program",
               f"Program #{program_id}: {', '.join(changes) if changes else 'metadata updated'}")

        return jsonify(success=True, message=f"Program '{new_title}' updated successfully.")
    except Exception as e:
        current_app.logger.error(f"Edit program error: {e}")
        return jsonify(success=False, error=str(e))
    finally:
        if conn:
            conn.close()


# ---------------- USERS ----------------
@bp.route("/api/add_user", methods=["POST"])
def add_user():
    """
    Add a new user (Admin / HOD / Teacher / Student) with:
      - Clean validation errors
      - Friendly duplicate message (username/email already registered)
      - Graceful handling of SQLite 'database is locked'
      - Professional JSON responses for front-end to show as toasts
    """
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")

    conn = None
    try:
        username = request.form.get("username", "").strip()
        email = request.form.get("email", "").strip().lower()
        role = request.form.get("role", "teacher").strip().lower()
        password = request.form.get("password", "").strip()
        department_name = request.form.get("department_name", "").strip() or None
        class_name = request.form.get("class_name", "").strip() or None
        teacher_name = request.form.get("teacher_name", "").strip() or None
        campus_branch = request.form.get("campus_branch", "Global").strip()

        # --- Basic validation ---
        if not username:
            return jsonify(success=False, error="Username is required.")
        if role not in ("admin", "hod", "teacher", "student"):
            return jsonify(success=False, error="Invalid role selected.")
        if role == "hod" and not department_name:
            return jsonify(
                success=False,
                error="Marhala name is required for Heads of Marhala.",
            )
        if role == "teacher" and not class_name:
            return jsonify(
                success=False,
                error="Darajah name is required for Darajah Teachers.",
            )

        # For teachers, generate password hash; for others, require password
        if role == "teacher":
            # Teachers use token-based login, so no password needed
            password_hash = None
        else:
            if not password:
                return jsonify(success=False, error="Password is required for non-teacher roles.")
            password_hash = generate_password_hash(password)

        conn = get_conn()
        cur = conn.cursor()

        # --- Check if user already exists (username or email) BEFORE inserting ---
        if email:
            cur.execute(
                "SELECT username, email FROM users WHERE username = ? OR LOWER(email) = ?",
                (username, email),
            )
        else:
            cur.execute(
                "SELECT username, email FROM users WHERE username = ?",
                (username,)
            )
        existing = cur.fetchone()
        if existing:
            return jsonify(
                success=False,
                error=(
                    "An account with this username or email already exists. "
                    "No changes were made."
                ),
            )

        # --- Insert user ---
        if role == "teacher":
            # Teacher with token-based login
            cur.execute(
                """
                INSERT INTO users (username, email, role, password_hash, department_name, 
                                   class_name, darajah_name, teacher_name, teacher_email, campus_branch)
                VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (username, email, role, password_hash, department_name, 
                 class_name, class_name, teacher_name, email, campus_branch),
            )
        else:
            # Regular user with password
            cur.execute(
                """
                INSERT INTO users (username, email, role, password_hash, department_name, 
                                   class_name, darajah_name, teacher_name, campus_branch)
                VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (username, email, role, password_hash, department_name, 
                 class_name, class_name, teacher_name, campus_branch),
            )

        # --- Create mapping records if applicable ---
        if role == "teacher" and class_name:
            # Add teacher-darajah mapping
            cur.execute("""
                INSERT INTO teacher_darajah_mapping 
                (teacher_username, teacher_name, darajah_name, teacher_email, role, academic_year)
                VALUES (?, ?, ?, ?, 'class_teacher', ?)
            """, (username, teacher_name or username, class_name, email, Config.CURRENT_ACADEMIC_YEAR()))
        
        elif role == "student" and class_name:
            # Add student-darajah mapping
            cur.execute("""
                INSERT INTO student_darajah_mapping 
                (student_username, student_name, darajah_name, academic_year)
                VALUES (?, ?, ?, ?)
            """, (username, username, class_name, Config.CURRENT_ACADEMIC_YEAR()))

        # --- Sync department_heads for HODs ---
        if role == "hod" and department_name:
            cur.execute(
                """
                INSERT OR IGNORE INTO department_heads (department_name, head_name, email)
                VALUES (?,?,?)
                """,
                (department_name, username, email),
            )

        conn.commit()

        # --- Audit & response ---
        _audit(session.get("user", "admin"), "add_user", f"{username}/{role}")
        return jsonify(
            success=True,
            message=f"User '{username}' registered successfully as {role.title()}.",
        )

    except sqlite3.IntegrityError as e:
        # Typically UNIQUE constraint violations etc.
        current_app.logger.warning(f"Add user integrity error: {e}")
        msg = "An account with this username or email already exists."
        return jsonify(success=False, error=msg)

    except sqlite3.OperationalError as e:
        # This is where "database is locked" usually comes
        current_app.logger.error(f"Add user operational error: {e}")
        if "locked" in str(e).lower():
            msg = (
                "The system database is currently busy. "
                "Please wait a few seconds and try again."
            )
        else:
            msg = "A database error occurred while creating the user."
        return jsonify(success=False, error=msg)

    except Exception as e:
        current_app.logger.error(f"Add user error: {e}")
        return jsonify(
            success=False,
            error=(
                "An unexpected error occurred while creating the user. "
                "Please try again."
            ),
        )

    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


@bp.route("/api/remove_user", methods=["POST"])
def remove_user():
    if not session.get("logged_in"):
        return jsonify(success=False)
    conn = None
    try:
        data = request.get_json()
        username = data.get("username")
        conn = get_conn()
        cur = conn.cursor()
        
        # First, delete mapping records
        cur.execute("DELETE FROM teacher_darajah_mapping WHERE teacher_username = ?", (username,))
        cur.execute("DELETE FROM student_darajah_mapping WHERE student_username = ?", (username,))
        cur.execute("DELETE FROM department_heads WHERE head_name = ?", (username,))
        
        # Then delete the user
        cur.execute("DELETE FROM users WHERE username=?", (username,))
        
        conn.commit()
        _audit(session.get("user", "admin"), "remove_user", username)
        flash(f"🗑️ User '{username}' removed successfully.", "info")
        return jsonify(success=True)
    except Exception as e:
        current_app.logger.error(f"Remove user error: {e}")
        return jsonify(success=False)
    finally:
        if conn:
            conn.close()


@bp.route("/api/update_user", methods=["POST"])
def update_user():
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")
    conn = None
    try:
        username = request.form.get("username", "").strip()
        email = request.form.get("email", "").strip()
        role = request.form.get("role", "").strip()
        password = request.form.get("password")
        teacher_name = request.form.get("teacher_name", "").strip()
        department_name = request.form.get("department_name", "").strip()
        class_name = request.form.get("class_name", "").strip()
        campus_branch = request.form.get("campus_branch", "Global").strip()

        if not username:
            return jsonify(success=False, error="Username is required")

        conn = get_conn()
        cur = conn.cursor()

        # Update user fields
        if password:
            from werkzeug.security import generate_password_hash
            hashed_pw = generate_password_hash(password)
            cur.execute("""
                UPDATE users 
                SET email = ?, role = ?, password_hash = ?, teacher_name = ?, 
                    department_name = ?, class_name = ?, darajah_name = ?, campus_branch = ?
                WHERE username = ?
            """, (email, role, hashed_pw, teacher_name, department_name, class_name, class_name, campus_branch, username))
        else:
            cur.execute("""
                UPDATE users 
                SET email = ?, role = ?, teacher_name = ?, 
                    department_name = ?, class_name = ?, darajah_name = ?, campus_branch = ?
                WHERE username = ?
            """, (email, role, teacher_name, department_name, class_name, class_name, campus_branch, username))

        conn.commit()
        _audit(session.get("user", "admin"), "update_user", f"{username} as {role}")
        return jsonify(success=True, message=f"User '{username}' updated successfully.")
    except Exception as e:
        current_app.logger.error(f"Update user error: {e}")
        return jsonify(success=False, error=str(e))
    finally:
        if conn:
            conn.close()


@bp.route("/api/list_users", methods=["GET"])
def list_users():
    if not session.get("logged_in"):
        return jsonify([])
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT username, email, role, department_name, class_name, darajah_name, 
                   teacher_name, teacher_email, profile_picture, created_at, campus_branch
            FROM users
            ORDER BY role ASC, username ASC
            """
        )
        rows = cur.fetchall()
        return jsonify(
            [
                {
                    "username": r[0],
                    "email": r[1],
                    "role": r[2],
                    "department_name": r[3],
                    "class_name": r[4],
                    "darajah_name": r[5],
                    "teacher_name": r[6],
                    "teacher_email": r[7],
                    "profile_picture": r[8],
                    "created_at": r[9],
                    "campus_branch": r[10]
                }
                for r in rows
            ]
        )
    except Exception as e:
        current_app.logger.error(f"List users error: {e}")
        return jsonify([])
    finally:
        if conn:
            conn.close()


# ---------------- TEACHER-DARAJAH MAPPING MANAGEMENT ----------------
@bp.route("/api/get_teacher_darajah_mapping", methods=["GET"])
def get_teacher_darajah_mapping():
    """Get all teacher-darajah mappings"""
    if not session.get("logged_in"):
        return jsonify([])
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT tm.id, tm.teacher_username, tm.teacher_name, 
                   tm.darajah_name, tm.role, tm.academic_year,
                   tm.teacher_email, tm.created_at
            FROM teacher_darajah_mapping tm
            ORDER BY tm.darajah_name, tm.teacher_name
        """)
        rows = cur.fetchall()
        return jsonify([
            {
                "id": r[0],
                "teacher_username": r[1],
                "teacher_name": r[2],
                "darajah_name": r[3],
                "role": r[4],
                "academic_year": r[5],
                "email": r[6],
                "created_at": r[7]
            }
            for r in rows
        ])
    except Exception as e:
        current_app.logger.error(f"Get teacher mapping error: {e}")
        return jsonify([])
    finally:
        if conn:
            conn.close()


@bp.route("/api/add_teacher_mapping", methods=["POST"])
def add_teacher_mapping():
    """Manually add a teacher-darajah mapping"""
    if not session.get("logged_in"):
        return jsonify(success=False)
    conn = None
    try:
        teacher_username = request.form.get("teacher_username", "").strip()
        teacher_name = request.form.get("teacher_name", "").strip()
        darajah_name = request.form.get("darajah_name", "").strip()
        teacher_email = request.form.get("teacher_email", "").strip().lower()
        role = request.form.get("role", "class_teacher").strip()
        
        if not teacher_username or not darajah_name:
            return jsonify(success=False, error="Teacher username and darajah name are required")
        
        conn = get_conn()
        cur = conn.cursor()
        
        # Check if teacher exists in users table
        cur.execute("SELECT username, email FROM users WHERE username = ? AND role = 'teacher'", (teacher_username,))
        teacher_user = cur.fetchone()
        
        if not teacher_user:
            # Create the teacher user if it doesn't exist
            cur.execute("""
                INSERT INTO users (username, email, role, teacher_name, teacher_email, darajah_name, class_name)
                VALUES (?, ?, 'teacher', ?, ?, ?, ?)
            """, (teacher_username, teacher_email or teacher_username + "@jamea.org", teacher_name, teacher_email, darajah_name, darajah_name))
        else:
            # Update teacher email if provided
            if teacher_email and teacher_user[1] != teacher_email:
                cur.execute("UPDATE users SET teacher_email = ?, email = ? WHERE username = ?", 
                           (teacher_email, teacher_email, teacher_username))
        
        # Check if mapping already exists
        cur.execute("""
            SELECT id FROM teacher_darajah_mapping 
            WHERE teacher_username = ? AND darajah_name = ?
        """, (teacher_username, darajah_name))
        
        if cur.fetchone():
            return jsonify(success=False, error="This teacher is already mapped to this darajah")
        
        # Add mapping
        cur.execute("""
            INSERT INTO teacher_darajah_mapping 
            (teacher_username, teacher_name, darajah_name, teacher_email, role, academic_year)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (teacher_username, teacher_name or teacher_username, darajah_name, teacher_email, role, Config.CURRENT_ACADEMIC_YEAR))
        
        conn.commit()
        
        _audit(session.get("user", "admin"), "add_teacher_mapping", 
               f"{teacher_username} -> {darajah_name}")
        
        return jsonify(success=True, message="Teacher-darajah mapping added successfully")
    
    except Exception as e:
        current_app.logger.error(f"Add teacher mapping error: {e}")
        return jsonify(success=False, error=str(e))
    finally:
        if conn:
            conn.close()


@bp.route("/api/remove_teacher_mapping", methods=["POST"])
def remove_teacher_mapping():
    """Remove a teacher-darajah mapping"""
    if not session.get("logged_in"):
        return jsonify(success=False)
    conn = None
    try:
        data = request.get_json()
        mapping_id = data.get("mapping_id")
        
        if not mapping_id:
            return jsonify(success=False, error="Mapping ID is required")
        
        conn = get_conn()
        cur = conn.cursor()
        
        # Get mapping details for audit log
        cur.execute("SELECT teacher_username, darajah_name FROM teacher_darajah_mapping WHERE id = ?", (mapping_id,))
        mapping = cur.fetchone()
        
        if not mapping:
            return jsonify(success=False, error="Mapping not found")
        
        # Delete the mapping
        cur.execute("DELETE FROM teacher_darajah_mapping WHERE id = ?", (mapping_id,))
        
        # Update user's darajah_name if this was their only mapping
        cur.execute("""
            SELECT COUNT(*) FROM teacher_darajah_mapping 
            WHERE teacher_username = ?
        """, (mapping[0],))
        
        count = cur.fetchone()[0]
        if count == 0:
            cur.execute("UPDATE users SET darajah_name = NULL WHERE username = ?", (mapping[0],))
        
        conn.commit()
        
        _audit(session.get("user", "admin"), "remove_teacher_mapping", 
               f"{mapping[0]} -> {mapping[1]}")
        
        return jsonify(success=True, message="Teacher-darajah mapping removed successfully")
    
    except Exception as e:
        current_app.logger.error(f"Remove teacher mapping error: {e}")
        return jsonify(success=False, error=str(e))
    finally:
        if conn:
            conn.close()


# ---------------- GET TEACHERS FOR A DARAJAH ----------------
@bp.route("/api/get_teachers_for_darajah/<darajah_name>", methods=["GET"])
def get_teachers_for_darajah(darajah_name):
    """Get all teachers mapped to a specific darajah"""
    if not session.get("logged_in"):
        return jsonify([])
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT tm.teacher_username, tm.teacher_name, tm.role, tm.teacher_email
            FROM teacher_darajah_mapping tm
            WHERE tm.darajah_name = ?
            ORDER BY tm.role DESC, tm.teacher_name
        """, (darajah_name,))
        rows = cur.fetchall()
        return jsonify([
            {
                "username": r[0],
                "name": r[1],
                "role": r[2],
                "email": r[3]
            }
            for r in rows
        ])
    except Exception as e:
        current_app.logger.error(f"Get teachers for darajah error: {e}")
        return jsonify([])
    finally:
        if conn:
            conn.close()


# ---------------- GET STUDENTS FOR A DARAJAH ----------------
@bp.route("/api/get_students_for_darajah/<darajah_name>", methods=["GET"])
def get_students_for_darajah(darajah_name):
    """Get all students in a specific darajah"""
    if not session.get("logged_in"):
        return jsonify([])
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT sm.student_username, sm.student_name, u.email, sm.academic_year, sm.enrollment_date
            FROM student_darajah_mapping sm
            LEFT JOIN users u ON sm.student_username = u.username
            WHERE sm.darajah_name = ?
            ORDER BY sm.student_name
        """, (darajah_name,))
        rows = cur.fetchall()
        return jsonify([
            {
                "username": r[0],
                "name": r[1],
                "email": r[2],
                "academic_year": r[3],
                "enrollment_date": r[4]
            }
            for r in rows
        ])
    except Exception as e:
        current_app.logger.error(f"Get students for darajah error: {e}")
        return jsonify([])
    finally:
        if conn:
            conn.close()


# ---------------- STUDENT MANAGEMENT ----------------
@bp.route("/api/add_student_mapping", methods=["POST"])
def add_student_mapping():
    """Manually add a student-darajah mapping"""
    if not session.get("logged_in"):
        return jsonify(success=False)
    conn = None
    try:
        student_username = request.form.get("student_username", "").strip()
        student_name = request.form.get("student_name", "").strip()
        darajah_name = request.form.get("darajah_name", "").strip()
        email = request.form.get("email", "").strip().lower()
        
        if not student_username or not darajah_name:
            return jsonify(success=False, error="Student username and darajah name are required")
        
        conn = get_conn()
        cur = conn.cursor()
        
        # Check if student exists in users table
        cur.execute("SELECT username FROM users WHERE username = ?", (student_username,))
        if not cur.fetchone():
            # Create the student user if it doesn't exist
            default_password = student_username[:4] + "123" if len(student_username) >= 4 else student_username + "123"
            password_hash = generate_password_hash(default_password)
            
            cur.execute("""
                INSERT INTO users (username, email, role, password_hash, class_name, darajah_name)
                VALUES (?, ?, 'student', ?, ?, ?)
            """, (student_username, email, password_hash, darajah_name, darajah_name))
        
        # Check if mapping already exists
        cur.execute("""
            SELECT id FROM student_darajah_mapping 
            WHERE student_username = ? AND darajah_name = ?
        """, (student_username, darajah_name))
        
        if cur.fetchone():
            return jsonify(success=False, error="This student is already mapped to this darajah")
        
        # Add mapping
        cur.execute("""
            INSERT INTO student_darajah_mapping 
            (student_username, student_name, darajah_name, academic_year)
            VALUES (?, ?, ?, ?)
        """, (student_username, student_name or student_username, darajah_name, Config.CURRENT_ACADEMIC_YEAR))
        
        # Update user's darajah_name
        cur.execute("UPDATE users SET darajah_name = ? WHERE username = ?", 
                   (darajah_name, student_username))
        
        conn.commit()
        
        _audit(session.get("user", "admin"), "add_student_mapping", 
               f"{student_username} -> {darajah_name}")
        
        return jsonify(success=True, message="Student-darajah mapping added successfully")
    
    except Exception as e:
        current_app.logger.error(f"Add student mapping error: {e}")
        return jsonify(success=False, error=str(e))
    finally:
        if conn:
            conn.close()


@bp.route("/api/remove_student_mapping", methods=["POST"])
def remove_student_mapping():
    """Remove a student-darajah mapping"""
    if not session.get("logged_in"):
        return jsonify(success=False)
    conn = None
    try:
        data = request.get_json()
        mapping_id = data.get("mapping_id")
        
        if not mapping_id:
            return jsonify(success=False, error="Mapping ID is required")
        
        conn = get_conn()
        cur = conn.cursor()
        
        # Get mapping details for audit log
        cur.execute("SELECT student_username, darajah_name FROM student_darajah_mapping WHERE id = ?", (mapping_id,))
        mapping = cur.fetchone()
        
        if not mapping:
            return jsonify(success=False, error="Mapping not found")
        
        # Delete the mapping
        cur.execute("DELETE FROM student_darajah_mapping WHERE id = ?", (mapping_id,))
        
        # Update user's darajah_name
        cur.execute("UPDATE users SET darajah_name = NULL WHERE username = ?", (mapping[0],))
        
        conn.commit()
        
        _audit(session.get("user", "admin"), "remove_student_mapping", 
               f"{mapping[0]} -> {mapping[1]}")
        
        return jsonify(success=True, message="Student-darajah mapping removed successfully")
    
    except Exception as e:
        current_app.logger.error(f"Remove student mapping error: {e}")
        return jsonify(success=False, error=str(e))
    finally:
        if conn:
            conn.close()


# ---------------- STUDENT PROGRESS MANAGEMENT ----------------
@bp.route("/api/get_student_progress", methods=["GET"])
def get_student_progress():
    """Get all student progress records"""
    if not session.get("logged_in"):
        return jsonify([])
    conn = None
    try:
        student_username = request.args.get("student_username", "")
        darajah_name = request.args.get("darajah_name", "")
        month_year = request.args.get("month_year", "")
        
        conn = get_conn()
        cur = conn.cursor()
        
        query = """
            SELECT sp.id, sp.student_username, sp.darajah_name, sp.month_year,
                   sp.books_borrowed, sp.books_returned, sp.overdue_books,
                   sp.attendance_days, sp.total_days, sp.remarks,
                   sp.reported_by, sp.reported_at,
                   u.teacher_name, u.teacher_email
            FROM student_progress sp
            LEFT JOIN users u ON sp.student_username = u.username
            WHERE 1=1
        """
        params = []
        
        if student_username:
            query += " AND sp.student_username = ?"
            params.append(student_username)
        if darajah_name:
            query += " AND sp.darajah_name = ?"
            params.append(darajah_name)
        if month_year:
            query += " AND sp.month_year = ?"
            params.append(month_year)
        
        query += " ORDER BY sp.month_year DESC, sp.student_username"
        
        cur.execute(query, params)
        rows = cur.fetchall()
        
        return jsonify([
            {
                "id": r[0],
                "student_username": r[1],
                "darajah_name": r[2],
                "month_year": r[3],
                "books_borrowed": r[4],
                "books_returned": r[5],
                "overdue_books": r[6],
                "attendance_days": r[7],
                "total_days": r[8],
                "attendance_percentage": round((r[7] / r[8] * 100) if r[8] > 0 else 0, 1),
                "remarks": r[9],
                "reported_by": r[10],
                "reported_at": r[11],
                "teacher_name": r[12],
                "teacher_email": r[13]
            }
            for r in rows
        ])
    except Exception as e:
        current_app.logger.error(f"Get student progress error: {e}")
        return jsonify([])
    finally:
        if conn:
            conn.close()


@bp.route("/api/save_student_progress", methods=["POST"])
def save_student_progress():
    """Save or update student progress record"""
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")
    
    conn = None
    try:
        progress_id = request.form.get("id")
        student_username = request.form.get("student_username", "").strip()
        darajah_name = request.form.get("darajah_name", "").strip()
        month_year = request.form.get("month_year", "").strip()
        books_borrowed = int(request.form.get("books_borrowed", 0))
        books_returned = int(request.form.get("books_returned", 0))
        overdue_books = int(request.form.get("overdue_books", 0))
        attendance_days = int(request.form.get("attendance_days", 0))
        total_days = int(request.form.get("total_days", 20))
        remarks = request.form.get("remarks", "").strip()
        
        if not student_username or not darajah_name or not month_year:
            return jsonify(success=False, error="Student username, darajah name, and month/year are required")
        
        conn = get_conn()
        cur = conn.cursor()
        
        if progress_id:
            # Update existing record
            cur.execute("""
                UPDATE student_progress 
                SET books_borrowed = ?, books_returned = ?, overdue_books = ?,
                    attendance_days = ?, total_days = ?, remarks = ?,
                    reported_by = ?, reported_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (books_borrowed, books_returned, overdue_books, 
                  attendance_days, total_days, remarks,
                  session.get("user", "admin"), progress_id))
        else:
            # Insert new record
            cur.execute("""
                INSERT INTO student_progress 
                (student_username, darajah_name, month_year,
                 books_borrowed, books_returned, overdue_books,
                 attendance_days, total_days, remarks, reported_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (student_username, darajah_name, month_year,
                  books_borrowed, books_returned, overdue_books,
                  attendance_days, total_days, remarks, 
                  session.get("user", "admin")))
        
        conn.commit()
        
        _audit(session.get("user", "admin"), "save_student_progress", 
               f"{student_username}/{month_year}")
        
        return jsonify(success=True, message="Student progress saved successfully")
    
    except Exception as e:
        current_app.logger.error(f"Save student progress error: {e}")
        return jsonify(success=False, error=str(e))
    finally:
        if conn:
            conn.close()


@bp.route("/api/delete_student_progress", methods=["POST"])
def delete_student_progress():
    """Delete a student progress record"""
    if not session.get("logged_in"):
        return jsonify(success=False)
    conn = None
    try:
        data = request.get_json()
        progress_id = data.get("id")
        
        if not progress_id:
            return jsonify(success=False, error="Progress ID is required")
        
        conn = get_conn()
        cur = conn.cursor()
        
        # Get record details for audit log
        cur.execute("SELECT student_username, month_year FROM student_progress WHERE id = ?", (progress_id,))
        record = cur.fetchone()
        
        if not record:
            return jsonify(success=False, error="Record not found")
        
        # Delete the record
        cur.execute("DELETE FROM student_progress WHERE id = ?", (progress_id,))
        conn.commit()
        
        _audit(session.get("user", "admin"), "delete_student_progress", 
               f"{record[0]}/{record[1]}")
        
        return jsonify(success=True, message="Student progress record deleted successfully")
    
    except Exception as e:
        current_app.logger.error(f"Delete student progress error: {e}")
        return jsonify(success=False, error=str(e))
    finally:
        if conn:
            conn.close()


# ---------------- BRANDING & SITE SETTINGS ----------------
@bp.route("/api/get_site_settings", methods=["GET"])
def get_site_settings():
    if not session.get("logged_in"):
        return jsonify({})
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT org_name, site_name, theme_color, footer_text, logo_path FROM site_settings LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            return jsonify({})
        return jsonify(
            {
                "org_name": row[0],
                "site_name": row[1],
                "theme_color": row[2],
                "footer_text": row[3],
                "logo_path": row[4],
            }
        )
    finally:
        if conn:
            conn.close()


@bp.route("/api/save_site_settings", methods=["POST"])
def save_site_settings():
    if not session.get("logged_in"):
        return jsonify(success=False)
    conn = None
    try:
        org_name = request.form.get("org_name", "").strip()
        site_name = request.form.get("site_name", "").strip()
        theme_color = request.form.get("theme_color", "#004080").strip()
        footer_text = request.form.get("footer_text", "").strip()
        conn = get_conn()
        cur = conn.cursor()
        # Always keep a single row
        cur.execute("DELETE FROM site_settings")
        cur.execute(
            """
            INSERT INTO site_settings (org_name, site_name, theme_color, footer_text, logo_path)
            VALUES (?,?,?,?, 'images/logo.png')
            """,
            (
                org_name or "Al-Jamea tus-Saifiyah",
                site_name or "Maktabat al-Jamea",
                theme_color,
                footer_text,
            ),
        )
        conn.commit()
        _audit(
            session.get("user", "admin"),
            "save_site_settings",
            f"{site_name}/{theme_color}",
        )
        return jsonify(success=True)
    except Exception as e:
        current_app.logger.error(f"Save site settings error: {e}")
        return jsonify(success=False)
    finally:
        if conn:
            conn.close()


@bp.route("/api/upload_logo", methods=["POST"])
def upload_logo():
    if not session.get("logged_in"):
        return jsonify(success=False)
    conn = None
    try:
        f = request.files.get("logo_file")
        if not f:
            return jsonify(success=False, error="No file provided")
        filename = secure_filename(f.filename)
        if not filename.lower().endswith(
            (".png", ".jpg", ".jpeg", ".gif", ".webp")
        ):
            return jsonify(success=False, error="Unsupported file type")
        static_dir = os.path.join(current_app.root_path, "static", "branding")
        os.makedirs(static_dir, exist_ok=True)
        target = os.path.join(static_dir, filename)
        f.save(target)

        rel_path = os.path.join("branding", filename).replace("\\", "/")
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("UPDATE site_settings SET logo_path=?", (rel_path,))
        conn.commit()
        _audit(session.get("user", "admin"), "upload_logo", rel_path)
        return jsonify(success=True, logo_path=rel_path)
    except Exception as e:
        current_app.logger.error(f"Upload logo error: {e}")
        return jsonify(success=False)
    finally:
        if conn:
            conn.close()


# ---------------- EMAIL SETTINGS (schedule) ----------------
@bp.route("/api/save_email_settings", methods=["POST"])
def save_email_settings():
    if not session.get("logged_in"):
        return jsonify(success=False)
    conn = None
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
        cur.execute(
            """
            INSERT INTO email_settings
            (sender_email, frequency, day_of_week, day_of_month, send_hour, send_minute)
            VALUES (?,?,?,?,?,?)
            """,
            (sender_email, frequency, day_of_week, day_of_month, send_hour, send_minute),
        )
        conn.commit()

        mail = current_app.extensions.get("mail")
        if mail:
            reload_scheduler(current_app, mail)

        _audit(
            session.get("user", "admin"),
            "save_email_settings",
            frequency or "",
        )
        return jsonify(success=True)
    except Exception as e:
        current_app.logger.error(f"Save email settings error: {e}")
        return jsonify(success=False)
    finally:
        if conn:
            conn.close()


# ---------------- EMAIL TEMPLATES ----------------
@bp.route("/api/get_email_templates", methods=["GET"])
def get_email_templates():
    if not session.get("logged_in"):
        return jsonify([])
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT template_key, subject, html FROM email_templates ORDER BY template_key ASC"
        )
        rows = cur.fetchall()
        return jsonify(
            [
                {"template_key": r[0], "subject": r[1], "html": r[2]}
                for r in rows
            ]
        )
    finally:
        if conn:
            conn.close()


@bp.route("/api/save_email_template", methods=["POST"])
def save_email_template():
    if not session.get("logged_in"):
        return jsonify(success=False)
    conn = None
    try:
        key = request.form.get("template_key", "").strip()
        subject = request.form.get("subject", "").strip()
        html = request.form.get("html", "").strip()
        if not key or not subject or not html:
            return jsonify(success=False, error="Missing template fields")
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO email_templates (template_key, subject, html)
            VALUES (?,?,?)
            ON CONFLICT(template_key) DO UPDATE SET subject=excluded.subject, html=excluded.html
            """,
            (key, subject, html),
        )
        conn.commit()
        _audit(session.get("user", "admin"), "save_email_template", key)
        return jsonify(success=True)
    except Exception as e:
        current_app.logger.error(f"Save email template error: {e}")
        return jsonify(success=False)
    finally:
        if conn:
            conn.close()


# ---------------- DB SETTINGS ----------------
@bp.route("/api/save_db_settings", methods=["POST"])
def save_db_settings():
    if not session.get("logged_in"):
        return jsonify(success=False)
    conn = None
    try:
        host = request.form.get("db_host")
        db_name = request.form.get("db_name")
        db_user = request.form.get("db_user")
        db_pass = request.form.get("db_pass")
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM db_settings")
        cur.execute(
            "INSERT INTO db_settings (host, db_name, db_user, db_pass) VALUES (?,?,?,?)",
            (host, db_name, db_user, db_pass),
        )
        conn.commit()
        _audit(session.get("user", "admin"), "save_db_settings", host or "")
        return jsonify(success=True)
    except Exception as e:
        current_app.logger.error(f"Save db settings error: {e}")
        return jsonify(success=False)
    finally:
        if conn:
            conn.close()


# ---------------- DEPARTMENT HEADS (MARHALA HEADS) ----------------
@bp.route("/api/add_department_head", methods=["POST"])
def add_department_head():
    if not session.get("logged_in"):
        return jsonify(success=False)
    conn = None
    try:
        dept_name = request.form.get("department_name")
        head_name = request.form.get("head_name")
        email = request.form.get("email")
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO department_heads (department_name, head_name, email) VALUES (?,?,?)",
            (dept_name, head_name, email),
        )
        conn.commit()
        _audit(
            session.get("user", "admin"),
            "add_department_head",
            dept_name or "",
        )
        return jsonify(success=True)
    except Exception as e:
        current_app.logger.error(f"Add department head error: {e}")
        return jsonify(success=False)
    finally:
        if conn:
            conn.close()


@bp.route("/api/remove_department_head", methods=["POST"])
def remove_department_head():
    if not session.get("logged_in"):
        return jsonify(success=False)
    conn = None
    try:
        data = request.get_json()
        dept_name = data.get("department_name")
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM department_heads WHERE department_name=?", (dept_name,)
        )
        conn.commit()
        _audit(
            session.get("user", "admin"),
            "remove_department_head",
            dept_name or "",
        )
        return jsonify(success=True)
    except Exception as e:
        current_app.logger.error(f"Remove department head error: {e}")
        return jsonify(success=False)
    finally:
        if conn:
            conn.close()


@bp.route("/api/list_department_heads", methods=["GET"])
def list_department_heads():
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT department_name, head_name, email FROM department_heads")
        rows = cur.fetchall()
        return jsonify(
            [
                {
                    "department_name": r[0],
                    "head_name": r[1],
                    "email": r[2],
                }
                for r in rows
            ]
        )
    except Exception as e:
        current_app.logger.error(f"List department heads error: {e}")
        return jsonify([])
    finally:
        if conn:
            conn.close()


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
        msg = Message(
            subject="Maktabat: Test Email",
            recipients=[to_addr],
            html="<p>This is a test email.</p>",
        )
        mail.send(msg)
        _audit(session.get("user", "admin"), "test_email", to_addr or "")
        return jsonify(success=True)
    except Exception as e:
        current_app.logger.error(f"Test email error: {e}")
        return jsonify(success=False, error=str(e))


@bp.route("/api/list_audit", methods=["GET"])
def list_audit():
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT ts, actor, action, details FROM audit_log ORDER BY ts DESC LIMIT 100"
        )
        rows = cur.fetchall()
        return jsonify(
            [
                {"ts": r[0], "actor": r[1], "action": r[2], "details": r[3]}
                for r in rows
            ]
        )
    finally:
        if conn:
            conn.close()


# ---------------- MANUAL EMAIL TRIGGER ----------------
@bp.route("/run_email_reports_now", methods=["POST"])
def run_email_reports_now():
    if not session.get("logged_in"):
        flash("Unauthorized access.", "danger")
        return redirect(url_for("admin_bp.index"))
    try:
        mail = current_app.extensions.get("mail")
        send_all_reports(current_app, mail)
        _audit(session.get("user", "admin"), "run_email_reports_now")
        flash(
            "✅ Email reports sent successfully. Check your inbox/logs.", "success"
        )
    except Exception as e:
        current_app.logger.exception("Manual email trigger failed")
        flash(f"❌ Error sending reports: {e}", "danger")
    return redirect(url_for("admin_bp.admin_settings"))


# ---------------- IMPORT TEACHERS FROM CSV (NEW) ----------------
@bp.route("/api/run_ai_nudge_now", methods=["POST"])
def run_ai_nudge_now():
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized"), 401
    
    from tasks.ai_nudge import send_ai_nudges
    from app import mail
    try:
        send_ai_nudges(current_app, mail)
        _audit(session.get("user", "admin"), "run_ai_nudge_now", "Manual AI Nudge trigger")
        return jsonify(success=True, message="AI Nudge job triggered successfully")
    except Exception as e:
        return jsonify(success=False, error=str(e))


@bp.route("/api/import_teachers_csv", methods=["POST"])
def import_teachers_csv():
    """Import teachers from Class_Teachers_1447.csv format"""
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")
    
    if 'csv_file' not in request.files:
        return jsonify(success=False, error="No file uploaded")
    
    file = request.files['csv_file']
    if file.filename == '':
        return jsonify(success=False, error="No file selected")
    
    if not file.filename.lower().endswith('.csv'):
        return jsonify(success=False, error="File must be a CSV")
    
    conn = None
    try:
        # Read CSV content
        stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)
        csv_reader = csv.DictReader(stream)
        
        # Validate required columns
        required_columns = ['ClassName', 'ITSID', 'Name', 'Email_Jamea']
        for col in required_columns:
            if col not in csv_reader.fieldnames:
                return jsonify(success=False, error=f"CSV missing required column: {col}")
        
        conn = get_conn()
        cur = conn.cursor()
        
        stats = {
            'total': 0,
            'added': 0,
            'updated': 0,
            'mappings_added': 0,
            'mappings_updated': 0,
            'errors': []
        }
        
        # Process each row
        for row_num, row in enumerate(csv_reader, start=2):
            stats['total'] += 1
            
            try:
                # Clean the data
                class_name = row.get('ClassName', '').strip()
                itsid = row.get('ITSID', '').strip()
                teacher_name = row.get('Name', '').strip()
                email = row.get('Email_Jamea', '').strip().lower()
                
                if not itsid or not class_name:
                    stats['errors'].append(f"Row {row_num}: ITSID or ClassName is empty")
                    continue
                
                # 1. Add/update user record
                cur.execute("SELECT COUNT(*) FROM users WHERE username = ?", (itsid,))
                if cur.fetchone()[0] == 0:
                    # Create new user with token-based login (no password needed)
                    cur.execute("""
                        INSERT INTO users (username, email, role, teacher_email, teacher_name, darajah_name, class_name)
                        VALUES (?, ?, 'teacher', ?, ?, ?, ?)
                    """, (itsid, email, email, teacher_name, class_name, class_name))
                    stats['added'] += 1
                else:
                    # Update existing user
                    cur.execute("""
                        UPDATE users 
                        SET email = ?, teacher_email = ?, teacher_name = ?, darajah_name = ?, class_name = ?, role = 'teacher'
                        WHERE username = ?
                    """, (email, email, teacher_name, class_name, class_name, itsid))
                    stats['updated'] += 1
                
                # 2. Add/update teacher-darajah mapping
                cur.execute("""
                    SELECT COUNT(*) FROM teacher_darajah_mapping 
                    WHERE teacher_username = ? AND darajah_name = ?
                """, (itsid, class_name))
                
                if cur.fetchone()[0] == 0:
                    cur.execute("""
                        INSERT INTO teacher_darajah_mapping 
                        (teacher_username, teacher_name, darajah_name, teacher_email, academic_year)
                        VALUES (?, ?, ?, ?, ?)
                    """, (itsid, teacher_name, class_name, email, Config.CURRENT_ACADEMIC_YEAR))
                    stats['mappings_added'] += 1
                else:
                    cur.execute("""
                        UPDATE teacher_darajah_mapping 
                        SET teacher_name = ?, teacher_email = ?, academic_year = ?
                        WHERE teacher_username = ? AND darajah_name = ?
                    """, (teacher_name, email, Config.CURRENT_ACADEMIC_YEAR, itsid, class_name))
                    stats['mappings_updated'] += 1
                
            except Exception as e:
                stats['errors'].append(f"Row {row_num}: {str(e)}")
                continue
        
        conn.commit()
        
        # Create summary message
        summary = f"Processed {stats['total']} teacher records: {stats['added']} new users, {stats['updated']} updated users, {stats['mappings_added']} new mappings, {stats['mappings_updated']} updated mappings."
        if stats['errors']:
            summary += f" {len(stats['errors'])} errors occurred."
        
        _audit(session.get("user", "admin"), "import_teachers_csv", summary)
        
        return jsonify({
            'success': True,
            'message': summary,
            'stats': stats
        })
        
    except Exception as e:
        current_app.logger.error(f"Import teachers CSV error: {e}")
        return jsonify(success=False, error=f"Error processing CSV: {str(e)}")
    finally:
        if conn:
            conn.close()


# ---------------- MARKS / TAQEEM ADMINISTRATION ----------------

# MARKS ALLOTMENT ENDPOINTS (NEW)
from services.marks_allotment import (
    get_all_allotments, save_category_allotment, delete_category_allotment,
    override_student_books_issued, get_student_books_override, clear_student_books_override
)

@bp.route("/api/get_marks_allotments", methods=["GET"])
def get_marks_allotments():
    """Get all mark allotments (categories + programs)."""
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")
    
    try:
        academic_year = request.args.get('academic_year', None)
        allotments = get_all_allotments(academic_year)
        return jsonify({
            'success': True,
            'data': allotments
        })
    except Exception as e:
        current_app.logger.error(f"Get allotments error: {e}")
        return jsonify(success=False, error=str(e))

@bp.route("/api/save_category_allotment", methods=["POST"])
def api_save_category_allotment():
    """Admin saves category allotment (Books Physical, etc.)."""
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")
    
    try:
        name = request.form.get('name').strip()
        max_marks = float(request.form.get('max_marks', 0))
        description = request.form.get('description', '').strip()
        priority = int(request.form.get('priority', 0))
        academic_year = request.form.get('academic_year', None)
        
        success = save_category_allotment(name, max_marks, description, academic_year, priority)
        if success:
            _audit(session.get("user", "admin"), "save_category_allotment", f"{name}:{max_marks}")
            return jsonify(success=True, message=f"Saved {name}: {max_marks}pts")
        else:
            return jsonify(success=False, error="Failed to save allotment")
    except Exception as e:
        current_app.logger.error(f"Save allotment error: {e}")
        return jsonify(success=False, error=str(e))

@bp.route("/api/delete_category_allotment", methods=["POST"])
def api_delete_category_allotment():
    """Admin deletes category allotment."""
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")
    
    try:
        name = request.form.get('name').strip()
        academic_year = request.form.get('academic_year', None)
        
        success = delete_category_allotment(name, academic_year)
        if success:
            _audit(session.get("user", "admin"), "delete_category_allotment", name)
            return jsonify(success=True, message=f"Deleted {name}")
        else:
            return jsonify(success=False, error="Allotment not found")
    except Exception as e:
        current_app.logger.error(f"Delete allotment error: {e}")
        return jsonify(success=False, error=str(e))

@bp.route("/api/override_student_books", methods=["POST"])
def api_override_student_books():
    """Admin manually overrides Koha book counts for a student."""
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")
    
    try:
        student_username = request.form.get('student_username').strip()
        physical_issued = int(request.form.get('physical_issued', 0))
        digital_issued = int(request.form.get('digital_issued', 0))
        academic_year = request.form.get('academic_year', None)
        
        success = override_student_books_issued(student_username, physical_issued, digital_issued, academic_year)
        if success:
            from services.marks_service import update_student_taqeem
            update_student_taqeem(student_username, academic_year)
            _audit(session.get("user", "admin"), "override_books", f"{student_username}:P{physical_issued}D{digital_issued}")
            return jsonify(success=True, message="Book counts overridden and Taqeem updated")
        else:
            return jsonify(success=False, error="Failed to save override")
    except Exception as e:
        current_app.logger.error(f"Override books error: {e}")
        return jsonify(success=False, error=str(e))

@bp.route("/api/get_student_books_override", methods=["GET"])
def api_get_student_books_override():
    """Get manual book override for a student."""
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")
    
    try:
        student_username = request.args.get('student_username')
        academic_year = request.args.get('academic_year', None)
        override = get_student_books_override(student_username, academic_year)
        return jsonify(success=True, data=override)
    except Exception as e:
        current_app.logger.error(f"Get override error: {e}")
        return jsonify(success=False, error=str(e))

@bp.route("/api/clear_student_books_override", methods=["POST"])
def api_clear_student_books_override():
    """Clear manual override, revert to Koha auto."""
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")
    
    try:
        student_username = request.form.get('student_username').strip()
        academic_year = request.form.get('academic_year', None)
        
        success = clear_student_books_override(student_username, academic_year)
        if success:
            from services.marks_service import update_student_taqeem
            update_student_taqeem(student_username, academic_year)
            _audit(session.get("user", "admin"), "clear_books_override", student_username)
            return jsonify(success=True, message="Override cleared, reverted to Koha data")
        else:
            return jsonify(success=False, error="No override found")
    except Exception as e:
        current_app.logger.error(f"Clear override error: {e}")
        return jsonify(success=False, error=str(e))

# ORIGINAL BOOK REVIEW UPLOAD (kept for legacy)
@bp.route("/api/upload_book_reviews", methods=["POST"])
def upload_book_reviews():
    """
    Upload CSV or Excel file for book review marks.
    Supports CSV: Trno, Marks, Name, ClassName, Remarks
    Supports Excel: Trno, Marks, Total, Percent, Name, ClassName
    """
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")

    if 'csv_file' not in request.files:
        return jsonify(success=False, error="No file uploaded")
    
    file = request.files['csv_file']
    if file.filename == '':
        return jsonify(success=False, error="No file selected")
    
    try:
        academic_year = request.form.get('academic_year', Config.CURRENT_ACADEMIC_YEAR).strip()
        file_type = request.form.get('file_type', '').strip()
        
        # Determine file type from extension if not specified
        if not file_type:
            if file.filename.lower().endswith(('.xlsx', '.xls')):
                file_type = 'excel'
            elif file.filename.lower().endswith('.csv'):
                file_type = 'csv'
            else:
                return jsonify(success=False, error="Unsupported file type. Please upload CSV or Excel file.")
        
        # Import the service function
        from services.marks_service import process_book_review_upload
        
        # Process the uploaded file
        result = process_book_review_upload(file, file_type, academic_year)
        
        if result.get('success'):
            msg = f"Successfully processed {result.get('processed', 0)} book review records."
            if result.get('errors', 0) > 0:
                msg += f" {result.get('errors', 0)} errors occurred."
            
            _audit(session.get("user", "admin"), "upload_book_reviews", msg)
            return jsonify({
                'success': True, 
                'message': msg,
                'stats': result
            })
        else:
            return jsonify({
                'success': False, 
                'error': result.get('error', 'Unknown error')
            })
        
    except Exception as e:
        current_app.logger.error(f"Error uploading book reviews: {e}")
        return jsonify(success=False, error=str(e))



@bp.route("/api/export_book_reviews", methods=["GET"])
def export_book_reviews():
    """
    Export book review marks to CSV or Excel format.
    """
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")
    
    try:
        export_format = request.args.get('format', 'csv').lower()
        academic_year = request.args.get('academic_year', Config.CURRENT_ACADEMIC_YEAR)
        
        # Import the service function
        from services.marks_service import export_book_review_marks
        
        content, filename = export_book_review_marks(academic_year, export_format)
        
        if export_format == 'excel':
            # For Excel file
            return send_file(
                BytesIO(content),
                as_attachment=True,
                download_name=filename,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        else:
            # For CSV file
            response = Response(
                content,
                mimetype='text/csv'
            )
            response.headers.set('Content-Disposition', 'attachment', filename=filename)
            return response
            
    except Exception as e:
        current_app.logger.error(f"Error exporting book reviews: {e}")
        return jsonify(success=False, error=str(e))


@bp.route("/api/get_book_review_stats", methods=["GET"])
def get_book_review_stats():
    """
    Get statistics about book review marks.
    """
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")
    
    try:
        academic_year = request.args.get('academic_year', Config.CURRENT_ACADEMIC_YEAR)
        
        # Import the service function
        from services.marks_service import get_book_review_stats
        
        stats = get_book_review_stats(academic_year)
        
        return jsonify({
            'success': True,
            'stats': stats
        })
        
    except Exception as e:
        current_app.logger.error(f"Error getting book review stats: {e}")
        return jsonify(success=False, error=str(e))


@bp.route("/api/get_book_review_marks", methods=["GET"])
def get_book_review_marks():
    """
    Get book review marks for a specific student or all students.
    """
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")
    
    conn = None
    try:
        student_username = request.args.get('student_username', '')
        trno = request.args.get('trno', '')
        academic_year = request.args.get('academic_year', Config.CURRENT_ACADEMIC_YEAR)
        
        conn = get_conn()
        cur = conn.cursor()
        
        query = """
            SELECT 
                brm.student_username,
                brm.student_trno as Trno,
                brm.student_name as Name,
                brm.darajah_name as ClassName,
                brm.academic_year as AcademicYear,
                brm.marks as Marks,
                brm.review_count as ReviewCount,
                brm.remarks as Remarks,
                brm.source as Source,
                brm.uploaded_at as LastUpdated,
                u.email as Email
            FROM book_review_marks brm
            LEFT JOIN users u ON brm.student_username = u.username
            WHERE brm.academic_year = ?
        """
        
        params = [academic_year]
        
        if student_username:
            query += " AND brm.student_username = ?"
            params.append(student_username)
        elif trno:
            query += " AND brm.student_trno = ?"
            params.append(trno)
        
        query += " ORDER BY brm.darajah_name, brm.student_name"
        
        cur.execute(query, params)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        
        return jsonify({
            'success': True,
            'columns': columns,
            'data': [dict(r) for r in rows]
        })
        
    except Exception as e:
        current_app.logger.error(f"Error getting book review marks: {e}")
        return jsonify(success=False, error=str(e))
    finally:
        if conn:
            conn.close()


@bp.route("/api/delete_book_review_marks", methods=["POST"])
def delete_book_review_marks():
    """
    Delete book review marks for a specific student.
    """
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")
    
    conn = None
    try:
        data = request.get_json()
        student_username = data.get('student_username', '').strip()
        academic_year = data.get('academic_year', Config.CURRENT_ACADEMIC_YEAR).strip()
        
        if not student_username:
            return jsonify(success=False, error="Student username is required")
        
        conn = get_conn()
        cur = conn.cursor()
        
        # Get student details for audit log
        cur.execute("SELECT student_name FROM book_review_marks WHERE student_username = ? AND academic_year = ?", 
                   (student_username, academic_year))
        student = cur.fetchone()
        
        if not student:
            return jsonify(success=False, error="No book review marks found for this student")
        
        # Delete the marks
        cur.execute("DELETE FROM book_review_marks WHERE student_username = ? AND academic_year = ?", 
                   (student_username, academic_year))
        
        # Update student taqeem
        from services.marks_service import update_student_taqeem
        update_student_taqeem(student_username, academic_year)
        
        conn.commit()
        
        _audit(session.get("user", "admin"), "delete_book_review_marks", 
               f"{student_username} ({academic_year})")
        
        return jsonify({
            'success': True,
            'message': f"Book review marks deleted for {student_username}"
        })
        
    except Exception as e:
        current_app.logger.error(f"Error deleting book review marks: {e}")
        return jsonify(success=False, error=str(e))
    finally:
        if conn:
            conn.close()


@bp.route("/api/get_taqeem_overview", methods=["GET"])
def get_taqeem_overview():
    """
    Get aggregated Taqeem marks for all students for the professional overview table.
    """
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")
    
    conn = None
    try:
        academic_year = request.args.get('academic_year', Config.CURRENT_ACADEMIC_YEAR)
        
        conn = get_conn()
        cur = conn.cursor()
        
        # Pull from student_taqeem which should be updated whenever marks change
        cur.execute("""
            SELECT 
                student_username as username,
                student_name as name,
                darajah_name as class_name,
                ROUND(physical_books_marks + digital_books_marks, 2) as total_pd_marks,
                ROUND(book_review_marks, 2) as total_review_marks,
                ROUND(program_attendance_marks, 2) as total_program_marks,
                ROUND(total_marks, 2) as grand_total
            FROM student_taqeem
            WHERE academic_year = ?
            ORDER BY darajah_name, student_name
        """, (academic_year,))
        
        rows = cur.fetchall()
        data = [dict(zip([d[0] for d in cur.description], row)) for row in rows]
        
        return jsonify(success=True, data=data)
        
    except Exception as e:
        current_app.logger.error(f"Error getting taqeem overview: {e}")
        return jsonify(success=False, error=str(e))
    finally:
        if conn:
            conn.close()


@bp.route("/api/recalc_all_taqeem", methods=["POST"])
def recalc_all_taqeem():
    """Trigger a full recalculation of all student Taqeem marks."""
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")
    
    try:
        from services.marks_service import update_all_student_taqeem
        academic_year = request.json.get('academic_year', Config.CURRENT_ACADEMIC_YEAR)
        count = update_all_student_taqeem(academic_year)
        
        _audit(session.get("user", "admin"), "recalc_all_taqeem", f"AY: {academic_year}, Count: {count}")
        return jsonify(success=True, message=f"Successfully recalculated marks for {count} students.")
    except Exception as e:
        current_app.logger.error(f"Recalc all taqeem error: {e}")
        return jsonify(success=False, error=str(e))


@bp.route("/api/get_student_taqeem", methods=["GET"])
def get_student_taqeem():
    """
    Get Taqeem marks for a specific student or all students.
    """
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")
    
    conn = None
    try:
        student_username = request.args.get('student_username', '')
        trno = request.args.get('trno', '')
        darajah_name = request.args.get('darajah_name', '')
        academic_year = request.args.get('academic_year', Config.CURRENT_ACADEMIC_YEAR)
        
        conn = get_conn()
        cur = conn.cursor()
        
        query = """
            SELECT 
                st.student_username,
                st.student_trno as Trno,
                st.student_name as Name,
                st.darajah_name as ClassName,
                st.academic_year as AcademicYear,
                st.physical_books_issued,
                st.digital_books_issued,
                st.physical_books_marks,
                st.digital_books_marks,
                st.book_issue_total,
                st.book_review_marks,
                st.program_attendance_marks,
                st.total_marks,
                st.last_updated
            FROM student_taqeem st
            WHERE st.academic_year = ?
        """
        
        params = [academic_year]
        
        if student_username:
            query += " AND st.student_username = ?"
            params.append(student_username)
        elif trno:
            query += " AND st.student_trno = ?"
            params.append(trno)
        elif darajah_name:
            query += " AND st.darajah_name = ?"
            params.append(darajah_name)
        
        query += " ORDER BY st.darajah_name, st.student_name"
        
        cur.execute(query, params)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        
        return jsonify({
            'success': True,
            'columns': columns,
            'data': [dict(r) for r in rows]
        })
        
    except Exception as e:
        current_app.logger.error(f"Error getting student taqeem: {e}")
        return jsonify(success=False, error=str(e))
    finally:
        if conn:
            conn.close()


@bp.route("/api/update_all_taqeem", methods=["POST"])
def update_all_taqeem():
    """
    Update Taqeem for all students.
    """
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")
    
    try:
        academic_year = request.form.get('academic_year', Config.CURRENT_ACADEMIC_YEAR).strip()
        
        # Import the service function
        from services.marks_service import update_all_student_taqeem
        
        success_count = update_all_student_taqeem(academic_year)
        
        _audit(session.get("user", "admin"), "update_all_taqeem", 
               f"Updated {success_count} students")
        
        return jsonify({
            'success': True,
            'message': f"Updated Taqeem for {success_count} students"
        })
        
    except Exception as e:
        current_app.logger.error(f"Error updating all taqeem: {e}")
        return jsonify(success=False, error=str(e))


@bp.route("/api/upload_program_attendance", methods=["POST"])
def upload_program_attendance():
    """
    Upload CSV for library program attendance.
    Expected columns: ITSID, Name, ClassName, AcademicYear, ProgramName, Date, Marks
    """
    if not session.get("logged_in"):
        return jsonify(success=False, error="Unauthorized")

    if 'csv_file' not in request.files:
        return jsonify(success=False, error="No file uploaded")
    
    file = request.files['csv_file']
    conn = None
    try:
        stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)
        csv_reader = csv.DictReader(stream)
        
        conn = get_conn()
        cur = conn.cursor()
        
        count = 0
        from services.marks_service import update_student_taqeem

        for row in csv_reader:
            clean_row = {k.lower().replace(" ", ""): v for k, v in row.items()}
            itsid = clean_row.get('itsid', '').strip()
            if not itsid: continue
            
            marks_val = clean_row.get('marks', '0')
            try:
                marks = float(marks_val)
            except ValueError:
                marks = 0.0

            ay = clean_row.get('academicyear', Config.CURRENT_ACADEMIC_YEAR).strip()
            
            cur.execute("""
                INSERT INTO library_program_attendance 
                (student_username, student_name, darajah_name, academic_year, program_name, attendance_date, marks, recorded_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                itsid, 
                clean_row.get('name', ''), 
                clean_row.get('classname', ''), 
                ay, 
                clean_row.get('programname', 'Library Program'),
                clean_row.get('date', datetime.now().strftime('%Y-%m-%d')),
                marks,
                session.get("user", "admin")
            ))
            
            # Trigger taqeem update
            update_student_taqeem(itsid, ay)
            
            count += 1
            
        conn.commit()
        return jsonify(success=True, message=f"Recorded {count} attendance entries.")
    except Exception as e:
        current_app.logger.error(f"Error uploading attendance: {e}")
        return jsonify(success=False, error=str(e))
