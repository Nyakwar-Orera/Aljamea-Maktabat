import sqlite3
import os
from werkzeug.security import generate_password_hash

# Path to your local SQLite DB
DB_PATH = os.path.join(os.path.dirname(__file__), "appdata.db")

# 🧑‍🏫 List of teachers to register (username, email, class_name, password)
teachers = [
    ("50590", "patrickogonyo76@gmail.com", "1 B M", "1234"),
    ("teacher10bm", "teacher10bm@jamea.edu", "10 B M", "12345"),
    ("teacher9af", "teacher9af@jamea.edu", "9 A F", "12345"),
    ("teacher8am", "teacher8am@jamea.edu", "8 A M", "12345"),
]

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

for username, email, class_name, password in teachers:
    password_hash = generate_password_hash(password)
    cur.execute("""
        INSERT OR REPLACE INTO users (username, email, role, password_hash, class_name)
        VALUES (?, ?, 'teacher', ?, ?)
    """, (username, email, password_hash, class_name))
    print(f"✅ Registered teacher: {username} → {class_name}")

conn.commit()
conn.close()
print("🎉 Done seeding teachers!")
