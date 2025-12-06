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
        department_name TEXT,
        class_name TEXT,
        profile_picture TEXT DEFAULT 'images/avatar.png',
        notification_count INTEGER DEFAULT 0,
        last_login DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # 🔄 MIGRATION: add missing columns if the table is from an older version
    cur.execute("PRAGMA table_info(users);")
    existing_cols = {row[1] for row in cur.fetchall()}
    added_cols = []

    if "profile_picture" not in existing_cols:
        cur.execute(
            "ALTER TABLE users "
            "ADD COLUMN profile_picture TEXT DEFAULT 'images/avatar.png';"
        )
        added_cols.append("profile_picture")

    if "notification_count" not in existing_cols:
        cur.execute(
            "ALTER TABLE users "
            "ADD COLUMN notification_count INTEGER DEFAULT 0;"
        )
        added_cols.append("notification_count")

    if "last_login" not in existing_cols:
        cur.execute(
            "ALTER TABLE users "
            "ADD COLUMN last_login DATETIME;"
        )
        added_cols.append("last_login")

    if "created_at" not in existing_cols:
        # Can't use DEFAULT CURRENT_TIMESTAMP in ALTER TABLE
        cur.execute(
            "ALTER TABLE users "
            "ADD COLUMN created_at DATETIME;"
        )
        cur.execute(
            "UPDATE users SET created_at = CURRENT_TIMESTAMP "
            "WHERE created_at IS NULL;"
        )
        added_cols.append("created_at")

    if added_cols:
        print("🔄 Migrated users table, added columns:", ", ".join(added_cols))

    # ✅ Ensure email is unique via index (NULLs allowed)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email_unique
        ON users(email)
        WHERE email IS NOT NULL;
    """)

    # 🔄 FIX OLD AVATAR PATHS
    # If any user still has the old default 'images/default-avatar.png'
    # or empty value, update to the new 'images/avatar.png'
    cur.execute("""
        UPDATE users
           SET profile_picture = 'images/avatar.png'
         WHERE profile_picture IS NULL
            OR profile_picture = ''
            OR profile_picture = 'images/default-avatar.png';
    """)
    print("🎨 Ensured users.profile_picture points to images/avatar.png where needed")

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

    # ---- Department Heads ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS department_heads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        department_name TEXT NOT NULL,
        head_name TEXT,
        email TEXT NOT NULL
    );
    """)

    # ---- Email settings (scheduling) ----
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

    # ---- Site branding / appearance ----
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

    # ---- Email templates (html) ----
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
            "Monthly Library Report – Class {{ class_name }}",
            "<p>Dear Teacher,</p>"
            "<p>Please find attached the monthly report for class "
            "<b>{{ class_name }}</b>.</p>"
            "<p>Regards,<br>Maktabat al-Jamea</p>"
        ),
        (
            "department_report",
            "Monthly Library Report – {{ dept }} Department",
            "<p>Dear {{ head_name }},</p>"
            "<p>Attached is the monthly library report for "
            "<b>{{ dept }}</b>.</p>"
            "<p>Regards,<br>Maktabat al-Jamea</p>"
        ),
        (
            "account_created",
            "Your Maktabat al-Jamea Account",
            "<p>Dear {{ username }},</p>"
            "<p>Your account has been created.<br>"
            "Role: {{ role }}<br>"
            "Username: {{ username }}<br>"
            "Password: {{ password }}</p>"
            "<p>Login: {{ login_url }}</p>"
        ),
    ]
    for k, s, h in defaults:
        cur.execute(
            "INSERT OR IGNORE INTO email_templates (template_key, subject, html) "
            "VALUES (?,?,?)",
            (k, s, h),
        )

    # ---- Audit log ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts DATETIME DEFAULT CURRENT_TIMESTAMP,
        actor TEXT,
        action TEXT,
        details TEXT
    );
    """)

    # ✅ Indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts);")
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_department_heads_name
        ON department_heads(department_name);
    """)

    conn.commit()
    conn.close()
    print("✅ appdata.db initialized with updated schema.")


if __name__ == "__main__":
    init_appdata()
