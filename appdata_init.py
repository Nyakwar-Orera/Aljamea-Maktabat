# appdata_init.py — FIXED VERSION

from db_app import get_conn, close_conn
from werkzeug.security import generate_password_hash
import csv
import os


def init_appdata():
    conn = get_conn()
    cur = conn.cursor()

    # ---- Users (for dashboard access control) ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,  -- ITSID from CSV
        email TEXT,  -- Email_Jamea from CSV
        role TEXT CHECK(role IN ('admin','hod','teacher','student')) NOT NULL DEFAULT 'admin',
        password_hash TEXT,
        department_name TEXT,
        class_name TEXT,  -- ClassName from CSV
        profile_picture TEXT DEFAULT 'images/avatar.png',
        notification_count INTEGER DEFAULT 0,
        last_login DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        teacher_email TEXT DEFAULT NULL,  -- Store teacher's email separately
        teacher_name TEXT DEFAULT NULL,   -- Store teacher's full name
        darajah_name TEXT DEFAULT NULL,   -- ClassName from CSV (for quick access)
        trno TEXT DEFAULT NULL,            -- Transaction number/student ID
        campus_branch TEXT DEFAULT 'Global' -- Campus branch
    );
    """)

    # Migration: add missing columns
    cur.execute("PRAGMA table_info(users);")
    existing_cols = {row[1] for row in cur.fetchall()}
    
    for col_name, col_type in [
        ("profile_picture", "TEXT DEFAULT 'images/avatar.png'"),
        ("notification_count", "INTEGER DEFAULT 0"),
        ("last_login", "DATETIME"),
        ("created_at", "DATETIME"),
        ("teacher_email", "TEXT DEFAULT NULL"),
        ("teacher_name", "TEXT DEFAULT NULL"),
        ("darajah_name", "TEXT DEFAULT NULL"),
        ("trno", "TEXT DEFAULT NULL"),
        ("campus_branch", "TEXT DEFAULT 'Global'")
    ]:
        if col_name not in existing_cols:
            cur.execute(f"ALTER TABLE users ADD COLUMN {col_name} {col_type};")

    # Ensure email is unique via index
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email_unique
        ON users(email)
        WHERE email IS NOT NULL;
    """)

    # Fix old avatar paths
    cur.execute("""
        UPDATE users
           SET profile_picture = 'images/avatar.png'
         WHERE profile_picture IS NULL
            OR profile_picture = ''
            OR profile_picture = 'images/default-avatar.png';
    """)

    # Ensure at least one admin user exists
    cur.execute("SELECT COUNT(*) FROM users WHERE role='admin';")
    if cur.fetchone()[0] == 0:
        cur.execute("""
            INSERT INTO users (username, email, role, password_hash)
            VALUES (?, ?, ?, ?)
        """, (
            "admin",
            "admin@maktabat.local",
            "admin",
            generate_password_hash("adminpass")
        ))

    # ---- Teacher-Darajah Mapping ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS teacher_darajah_mapping (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        teacher_username TEXT NOT NULL,
        teacher_name TEXT NOT NULL,
        darajah_name TEXT NOT NULL,
        teacher_email TEXT,
        role TEXT CHECK(role IN ('masool', 'class_teacher', 'assistant')) DEFAULT 'class_teacher',
        academic_year TEXT DEFAULT '1447',
        campus_branch TEXT DEFAULT 'Global',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (teacher_username) REFERENCES users(username) ON DELETE CASCADE
    );
    """)
    
    # Function to import teachers from CSV
    def import_teachers_from_csv(csv_filepath="Class_Teachers_1447.csv"):
        if not os.path.exists(csv_filepath):
            return
        
        teachers_imported = 0
        with open(csv_filepath, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row in reader:
                class_name = row.get('ClassName', '').strip()
                itsid = row.get('ITSID', '').strip()
                teacher_name = row.get('Name', '').strip()
                email = row.get('Email_Jamea', '').strip()
                
                if not itsid or not class_name:
                    continue
                
                cur.execute("SELECT COUNT(*) FROM users WHERE username = ?", (itsid,))
                if cur.fetchone()[0] == 0:
                    cur.execute("""
                        INSERT INTO users (username, email, role, teacher_email, teacher_name, darajah_name, class_name)
                        VALUES (?, ?, 'teacher', ?, ?, ?, ?)
                    """, (itsid, email, email, teacher_name, class_name, class_name))
                else:
                    cur.execute("""
                        UPDATE users 
                        SET email = ?, teacher_email = ?, teacher_name = ?, darajah_name = ?, class_name = ?, role = 'teacher'
                        WHERE username = ?
                    """, (email, email, teacher_name, class_name, class_name, itsid))
                
                cur.execute("""
                    SELECT COUNT(*) FROM teacher_darajah_mapping 
                    WHERE teacher_username = ? AND darajah_name = ?
                """, (itsid, class_name))
                
                if cur.fetchone()[0] == 0:
                    cur.execute("""
                        INSERT INTO teacher_darajah_mapping 
                        (teacher_username, teacher_name, darajah_name, teacher_email, academic_year, campus_branch)
                        VALUES (?, ?, ?, ?, '1447', 'Global')
                    """, (itsid, teacher_name, class_name, email))
                else:
                    cur.execute("""
                        UPDATE teacher_darajah_mapping 
                        SET teacher_name = ?, teacher_email = ?, academic_year = '1447'
                        WHERE teacher_username = ? AND darajah_name = ?
                    """, (teacher_name, email, itsid, class_name))
                
                teachers_imported += 1
    
    import_teachers_from_csv()
    
    # ---- Student-Darajah Mapping ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS student_darajah_mapping (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student_username TEXT NOT NULL,
        student_name TEXT,
        darajah_name TEXT NOT NULL,
        academic_year TEXT DEFAULT '1447',
        enrollment_date DATE DEFAULT CURRENT_DATE,
        campus_branch TEXT DEFAULT 'Global',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (student_username) REFERENCES users(username) ON DELETE CASCADE
    );
    """)
    
    # ---- Student Attendance/Progress ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS student_progress (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student_username TEXT NOT NULL,
        darajah_name TEXT NOT NULL,
        month_year TEXT NOT NULL,
        books_borrowed INTEGER DEFAULT 0,
        books_returned INTEGER DEFAULT 0,
        overdue_books INTEGER DEFAULT 0,
        attendance_days INTEGER DEFAULT 0,
        total_days INTEGER DEFAULT 20,
        remarks TEXT,
        reported_by TEXT,
        reported_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (student_username) REFERENCES users(username) ON DELETE CASCADE
    );
    """)

    # ---- Academic Year Tracking ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS academic_years (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        year_label TEXT UNIQUE NOT NULL,
        start_date DATE,
        end_date DATE,
        is_active BOOLEAN DEFAULT 1,
        total_months INTEGER DEFAULT 9,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """)
    
    cur.execute("SELECT COUNT(*) FROM academic_years WHERE year_label='1447';")
    if cur.fetchone()[0] == 0:
        cur.execute("""
            INSERT INTO academic_years (year_label, total_months, is_active)
            VALUES ('1447', 9, 1)
        """)
    
    # ---- Student Level (Mustawā) Mapping ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS student_levels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        darajah_name TEXT NOT NULL,
        mustawa_level INTEGER NOT NULL,
        physical_books_weight REAL DEFAULT 40.0,
        digital_books_weight REAL DEFAULT 20.0,
        academic_year TEXT DEFAULT '1447',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(darajah_name, academic_year)
    );
    """)
    
    for class_num in range(1, 12):
        class_name = f"Class {class_num}"
        if class_num <= 7:
            physical_weight = 40.0
            digital_weight = 20.0
        else:
            physical_weight = 20.0
            digital_weight = 40.0
        
        cur.execute("""
            INSERT OR IGNORE INTO student_levels 
            (darajah_name, mustawa_level, physical_books_weight, digital_books_weight)
            VALUES (?, ?, ?, ?)
        """, (class_name, class_num, physical_weight, digital_weight))
    
    # ---- Book Review Marks ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS book_review_marks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student_username TEXT NOT NULL,
        student_trno TEXT,
        student_name TEXT,
        darajah_name TEXT NOT NULL,
        academic_year TEXT DEFAULT '1447',
        marks REAL DEFAULT 0.0,
        review_count INTEGER DEFAULT 0,
        source TEXT DEFAULT 'manual',
        remarks TEXT,
        uploaded_by TEXT,
        uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (student_username) REFERENCES users(username) ON DELETE CASCADE,
        UNIQUE(student_username, academic_year)
    );
    """)
    
    # ---- Library Program Attendance ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS library_program_attendance (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student_username TEXT NOT NULL,
        student_name TEXT,
        darajah_name TEXT NOT NULL,
        academic_year TEXT DEFAULT '1447',
        program_name TEXT,
        program_id INTEGER,
        attendance_date DATE,
        attended BOOLEAN DEFAULT 1,
        marks REAL DEFAULT 0.0,
        uploaded_by TEXT,
        recorded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (student_username) REFERENCES users(username) ON DELETE CASCADE
    );
    """)
    
    # ---- Aggregated Student Marks (Taqeem Summary) ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS student_taqeem (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student_username TEXT NOT NULL,
        student_trno TEXT,
        student_name TEXT,
        darajah_name TEXT NOT NULL,
        academic_year TEXT DEFAULT '1447',
        physical_books_issued INTEGER DEFAULT 0,
        digital_books_issued INTEGER DEFAULT 0,
        physical_books_marks REAL DEFAULT 0.0,
        digital_books_marks REAL DEFAULT 0.0,
        book_issue_total REAL DEFAULT 0.0,
        book_review_marks REAL DEFAULT 0.0,
        program_attendance_marks REAL DEFAULT 0.0,
        total_marks REAL DEFAULT 0.0,
        last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (student_username) REFERENCES users(username) ON DELETE CASCADE,
        UNIQUE(student_username, academic_year)
    );
    """)

    # ---- Department Heads ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS department_heads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        department_name TEXT NOT NULL,
        head_name TEXT,
        email TEXT NOT NULL
    );
    """)

    # ---- Email settings ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS email_settings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sender_email TEXT NOT NULL,
        frequency TEXT CHECK(frequency IN ('daily','weekly','monthly')) NOT NULL DEFAULT 'monthly',
        day_of_week TEXT DEFAULT 'mon',
        day_of_month INTEGER DEFAULT 1,
        send_hour INTEGER DEFAULT 8,
        send_minute INTEGER DEFAULT 0
    );
    """)

    # ---- Koha connection overrides ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS db_settings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        host TEXT,
        db_name TEXT,
        db_user TEXT,
        db_pass TEXT
    );
    """)

    # ---- Site branding ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS site_settings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        org_name TEXT DEFAULT 'Al-Jamea tus-Saifiyah',
        site_name TEXT DEFAULT 'Maktabat al-Jamea',
        theme_color TEXT DEFAULT '#004080',
        footer_text TEXT DEFAULT '© Maktabat al-Jamea',
        logo_path TEXT DEFAULT 'images/logo.png'
    );
    """)
    cur.execute("SELECT COUNT(*) FROM site_settings;")
    if cur.fetchone()[0] == 0:
        cur.execute(
            "INSERT INTO site_settings (org_name) VALUES ('Al-Jamea tus-Saifiyah')"
        )

    # ---- Email templates ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS email_templates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        template_key TEXT UNIQUE NOT NULL,
        subject TEXT NOT NULL,
        html TEXT NOT NULL
    );
    """)
    defaults = [
        (
            "class_report",
            "Monthly Library Report – Darajah {{ class_name }}",
            "<p>Dear Teacher,</p><p>Please find attached the monthly report for darajah "
            "<b>{{ class_name }}</b>.</p><p>Regards,<br>Maktabat al-Jamea</p>"
        ),
        (
            "department_report",
            "Monthly Library Report – {{ dept }} Marhala",
            "<p>Dear {{ head_name }},</p><p>Attached is the monthly library report for "
            "<b>{{ dept }}</b> marhala.</p><p>Regards,<br>Maktabat al-Jamea</p>"
        ),
        (
            "account_created",
            "Your Maktabat al-Jamea Account",
            "<p>Dear {{ username }},</p><p>Your account has been created.<br>"
            "Role: {{ role }}<br>Username: {{ username }}<br>Password: {{ password }}</p>"
            "<p>Login: {{ login_url }}</p>"
        ),
        (
            "student_report",
            "Monthly Library Report for {{ student_name }}",
            "<p>Dear {{ parent_name }},</p><p>Please find attached the monthly library report for "
            "<b>{{ student_name }}</b> of Darajah <b>{{ darajah_name }}</b>.</p>"
            "<p><strong>Class Teacher:</strong> {{ teacher_name }}</p>"
            "<p>Regards,<br>Maktabat al-Jamea</p>"
        ),
        (
            "teacher_token_login",
            "Your Teacher Login Link - Maktabat al-Jamea",
            "<p>Dear {{ teacher_name }},</p><p>Here is your login link for the Maktabat al-Jamea portal:</p>"
            "<p><strong>Login Link:</strong> <a href='{{ login_url }}'>Click here to login</a></p>"
            "<p><strong>Note:</strong> This link is valid for 24 hours.</p>"
            "<p>Regards,<br>Maktabat al-Jamea</p>"
        ),
    ]
    for k, s, h in defaults:
        cur.execute(
            "INSERT OR IGNORE INTO email_templates (template_key, subject, html) "
            "VALUES (?,?,?)",
            (k, s, h),
        )

    # ---- Audit Log ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts DATETIME DEFAULT CURRENT_TIMESTAMP,
        actor TEXT,
        action TEXT,
        details TEXT
    );
    """)

    # ---- Lab Sessions ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS lab_sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student_name TEXT,
        trno TEXT,
        computer_id TEXT,
        start_time DATETIME,
        end_time DATETIME,
        status TEXT,
        session_date DATE DEFAULT CURRENT_DATE
    );
    """)

    # ---- Marks Allotments ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS marks_allotments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type TEXT NOT NULL DEFAULT 'category',
        name TEXT NOT NULL,
        description TEXT,
        max_marks REAL NOT NULL,
        priority INTEGER DEFAULT 0,
        academic_year TEXT DEFAULT '1447',
        campus_branch TEXT DEFAULT 'Global',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(name, academic_year, campus_branch)
    );
    """)

    cur.execute("SELECT COUNT(*) FROM marks_allotments WHERE type = 'category'")
    if cur.fetchone()[0] == 0:
        defaults = [
            ('Books Issued', 40.0, 'Books issued (automatic from Koha) - 40% weight', 1),
            ('Book Review/Quality', 30.0, 'Book review submissions and quality assessment - 30% weight', 2),
            ('Library Programs', 30.0, 'Attendance at library programs and workshops - 30% weight', 3),
        ]
        for name, marks, desc, prio in defaults:
            cur.execute("""
                INSERT INTO marks_allotments (type, name, max_marks, description, priority)
                VALUES ('category', ?, ?, ?, ?)
            """, (name, marks, desc, prio))

    # ---- Student Books Override ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS student_books_override (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student_username TEXT NOT NULL,
        academic_year TEXT NOT NULL,
        physical_books_issued INTEGER DEFAULT 0,
        digital_books_issued INTEGER DEFAULT 0,
        overridden_by TEXT DEFAULT 'admin',
        overridden_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(student_username, academic_year)
    );
    """)

    # ---- Library Programs ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS library_programs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        date TEXT NOT NULL,
        marks REAL NOT NULL DEFAULT 0.0,
        marks_category TEXT DEFAULT 'Manual',
        darajahs TEXT DEFAULT 'All',
        marhalas TEXT DEFAULT 'All',
        frequency TEXT DEFAULT 'once',
        venue TEXT,
        conductor TEXT,
        department_note TEXT,
        academic_year TEXT DEFAULT '1447',
        campus_branch TEXT DEFAULT 'Global',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # ---- Library Program Marks ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS library_program_marks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        program_id INTEGER NOT NULL,
        student_username TEXT NOT NULL,
        student_name TEXT,
        darajah_name TEXT,
        academic_year TEXT DEFAULT '1447',
        marks REAL DEFAULT 0.0,
        uploaded_by TEXT DEFAULT 'admin',
        uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (program_id) REFERENCES library_programs(id) ON DELETE CASCADE,
        UNIQUE(program_id, student_username)
    );
    """)

    # ---- Create Indexes ----
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_class_name ON users(class_name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_darajah_name ON users(darajah_name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_teacher_email ON users(teacher_email);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_trno ON users(trno);")
    
    cur.execute("CREATE INDEX IF NOT EXISTS idx_book_review_student ON book_review_marks(student_username, academic_year);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_book_review_darajah ON book_review_marks(darajah_name, academic_year);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_book_review_trno ON book_review_marks(student_trno, academic_year);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_program_attendance_student ON library_program_attendance(student_username, academic_year);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_program_attendance_darajah ON library_program_attendance(darajah_name, academic_year);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_program_attendance_date ON library_program_attendance(attendance_date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_student_taqeem ON student_taqeem(student_username, academic_year);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_taqeem_darajah ON student_taqeem(darajah_name, academic_year);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_taqeem_trno ON student_taqeem(student_trno, academic_year);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_student_levels ON student_levels(darajah_name, academic_year);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_academic_years_active ON academic_years(is_active);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_teacher_darajah ON teacher_darajah_mapping(teacher_username, darajah_name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_teacher_by_darajah ON teacher_darajah_mapping(darajah_name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_student_darajah ON student_darajah_mapping(student_username, darajah_name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_student_by_darajah ON student_darajah_mapping(darajah_name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_student_progress ON student_progress(student_username, month_year);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_progress_by_darajah ON student_progress(darajah_name, month_year);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_department_heads_name ON department_heads(department_name);")

    conn.commit()
    
    # Close the connection properly
    close_conn(conn)
    
    print("✅ Database initialization completed successfully!")


if __name__ == "__main__":
    init_appdata()