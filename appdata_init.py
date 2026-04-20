import sqlite3
import os
from config import Config
from werkzeug.security import generate_password_hash

def init_appdata():
    """Initialize the local AppData SQLite database with all required tables."""
    db_path = Config.APP_SQLITE_PATH
    if not db_path:
        db_path = os.path.join(os.path.dirname(__file__), 'appdata.db')
    
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    cur = conn.cursor()
    
    # ── Users table ──────────────────────────────────────────────────────────
    cur.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT,
            role TEXT NOT NULL DEFAULT 'teacher',
            password_hash TEXT,
            department_name TEXT,
            class_name TEXT,
            profile_picture TEXT,
            darajah_name TEXT,
            teacher_name TEXT,
            teacher_email TEXT,
            campus_branch TEXT DEFAULT 'Global',
            branch_code TEXT DEFAULT 'AJSN',
            last_login TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Migrate users table — add new columns if they don't exist yet
    _safe_migrations = [
        "ALTER TABLE users ADD COLUMN teacher_name TEXT",
        "ALTER TABLE users ADD COLUMN teacher_email TEXT",
        "ALTER TABLE users ADD COLUMN campus_branch TEXT DEFAULT 'Global'",
        "ALTER TABLE users ADD COLUMN branch_code TEXT DEFAULT 'AJSN'",
        "ALTER TABLE users ADD COLUMN profile_picture TEXT",
        "ALTER TABLE users ADD COLUMN last_login TEXT",
    ]
    for sql in _safe_migrations:
        try:
            cur.execute(sql)
        except Exception:
            pass  # column already exists

    # ── System config ─────────────────────────────────────────────────────────
    cur.execute('''
        CREATE TABLE IF NOT EXISTS system_config (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')

    # ── Teacher ↔ Darajah mapping ─────────────────────────────────────────────
    cur.execute('''
        CREATE TABLE IF NOT EXISTS teacher_darajah_mapping (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            teacher_username TEXT NOT NULL,
            teacher_name TEXT,
            darajah_name TEXT NOT NULL,
            teacher_email TEXT,
            role TEXT DEFAULT 'class_teacher',
            academic_year TEXT,
            campus_branch TEXT DEFAULT 'Global',
            branch_code TEXT DEFAULT 'AJSN',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # ── Student ↔ Darajah mapping ─────────────────────────────────────────────
    cur.execute('''
        CREATE TABLE IF NOT EXISTS student_darajah_mapping (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_username TEXT NOT NULL,
            student_name TEXT,
            darajah_name TEXT NOT NULL,
            academic_year TEXT,
            campus_branch TEXT DEFAULT 'Global',
            branch_code TEXT DEFAULT 'AJSN',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # ── Library Programs ──────────────────────────────────────────────────────
    cur.execute('''
        CREATE TABLE IF NOT EXISTS library_programs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            date TEXT NOT NULL,
            marks REAL DEFAULT 0,
            marks_category TEXT DEFAULT 'Manual',
            darajahs TEXT DEFAULT 'All',
            marhalas TEXT DEFAULT 'All',
            frequency TEXT DEFAULT 'once',
            venue TEXT,
            conductor TEXT,
            department_note TEXT,
            academic_year TEXT,
            campus_branch TEXT DEFAULT 'Global',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # ── Library Program Attendance ────────────────────────────────────────────
    cur.execute('''
        CREATE TABLE IF NOT EXISTS library_program_attendance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            program_id INTEGER,
            program_name TEXT,
            student_username TEXT,
            student_name TEXT,
            darajah_name TEXT,
            marks REAL DEFAULT 0,
            attended INTEGER DEFAULT 1,
            academic_year TEXT,
            campus_branch TEXT DEFAULT 'Global',
            branch_code TEXT DEFAULT 'AJSN',
            uploaded_by TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (program_id) REFERENCES library_programs(id)
        )
    ''')

    # ── Email Settings ────────────────────────────────────────────────────────
    cur.execute('''
        CREATE TABLE IF NOT EXISTS email_settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            server TEXT,
            port INTEGER DEFAULT 587,
            use_tls INTEGER DEFAULT 1,
            username TEXT,
            password TEXT,
            default_sender TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # ── Audit Log ─────────────────────────────────────────────────────────────
    cur.execute('''
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor TEXT,
            action TEXT,
            details TEXT,
            ip_address TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # ── Book Review Marks ─────────────────────────────────────────────────────
    cur.execute('''
        CREATE TABLE IF NOT EXISTS book_review_marks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_username TEXT NOT NULL,
            student_trno TEXT,
            student_name TEXT,
            darajah_name TEXT,
            academic_year TEXT,
            marks REAL DEFAULT 0,
            review_count INTEGER DEFAULT 0,
            remarks TEXT,
            source TEXT,
            uploaded_by TEXT,
            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            book_review_name TEXT,
            hijri_month TEXT,
            percent REAL,
            grade TEXT,
            campus_branch TEXT DEFAULT 'Global',
            branch_code TEXT DEFAULT 'AJSN'
        )
    ''')

    # Migrate tables — add new columns if they don't exist yet
    _migrations = [
        "ALTER TABLE audit_log ADD COLUMN actor TEXT",
        "ALTER TABLE audit_log ADD COLUMN details TEXT",
        "ALTER TABLE book_review_marks ADD COLUMN book_review_name TEXT",
        "ALTER TABLE book_review_marks ADD COLUMN hijri_month TEXT",
        "ALTER TABLE book_review_marks ADD COLUMN percent REAL",
        "ALTER TABLE book_review_marks ADD COLUMN grade TEXT",
        "ALTER TABLE book_review_marks ADD COLUMN campus_branch TEXT DEFAULT 'Global'",
        "ALTER TABLE book_review_marks ADD COLUMN branch_code TEXT DEFAULT 'AJSN'",
    ]
    for sql in _migrations:
        try:
            cur.execute(sql)
        except Exception:
            pass

    # ── Seed admin user if needed ─────────────────────────────────────────────
    admin_user = os.getenv("ADMIN_USER", "admin")
    admin_pass = os.getenv("ADMIN_PASS", "admin123")
    
    cur.execute("SELECT * FROM users WHERE username = ?", (admin_user,))
    if not cur.fetchone():
        cur.execute('''
            INSERT INTO users (username, role, password_hash, branch_code, campus_branch)
            VALUES (?, ?, ?, ?, ?)
        ''', (admin_user, 'admin', generate_password_hash(admin_pass), 'AJSN', 'Global'))
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    init_appdata()
