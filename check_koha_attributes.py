# check_koha_attributes.py
from db_koha import get_koha_conn

def show_departments_and_classes():
    conn = get_koha_conn()
    cur = conn.cursor()

    print("\n===== 🏛 Departments (Koha categories) =====")
    cur.execute("""
        SELECT DISTINCT
            COALESCE(c.description, b.categorycode) AS department,
            b.categorycode
        FROM borrowers b
        LEFT JOIN categories c ON b.categorycode = c.categorycode
        ORDER BY department;
    """)
    for row in cur.fetchall():
        print(f"Department: {row[0]} | Code: {row[1]}")

    print("\n===== 🎓 Classes (Borrower attributes STD) =====")
    cur.execute("""
        SELECT DISTINCT attribute
        FROM borrower_attributes
        WHERE code = 'STD'
        ORDER BY attribute;
    """)
    for row in cur.fetchall():
        print(f"Class STD: {row[0]}")

    cur.close()
    conn.close()
    print("\n✅ Done. Use these exact values for department_name (HODs) and class_name (Teachers).")


if __name__ == "__main__":
    show_departments_and_classes()
