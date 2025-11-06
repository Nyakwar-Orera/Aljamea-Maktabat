from db_app import get_conn
from werkzeug.security import generate_password_hash

def init_appdata():
    conn = get_conn()
    cur = conn.cursor()

    # ---- Users (for dashboard access control) ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        email TEXT,
        role TEXT CHECK(role IN ('admin','hod','teacher','student')) NOT NULL DEFAULT 'admin',
        password_hash TEXT,
        department_name TEXT,   -- for HODs
        class_name TEXT         -- for Class Teachers
    );
    """)

    # ✅ Ensure at least one admin user exists
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
        print("👤 Default admin user created → username: admin | password: adminpass")

    # ---- Mappings (student -> class -> teacher email) ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS mappings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student_id TEXT NOT NULL,
        student_name TEXT NOT NULL,
        class_name TEXT NOT NULL,
        teacher_name TEXT NOT NULL,
        teacher_email TEXT
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

    # ---- Koha connection overrides (optional) ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS db_settings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        host TEXT,
        db_name TEXT,
        db_user TEXT,
        db_pass TEXT
    );
    """)

    # ✅ Create useful indexes for performance
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);")

    conn.commit()
    conn.close()
    print("✅ appdata.db initialized with updated schema.")

if __name__ == "__main__":
    init_appdata()
