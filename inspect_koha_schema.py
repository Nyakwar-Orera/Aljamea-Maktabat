"""
Quick Koha DB inspector for the 'statistics' table or any other table.

Usage:
    python inspect_koha_schema.py statistics    # To inspect the columns in 'statistics'
"""

from db_koha import get_koha_conn


def show_tables():
    """Print all tables in the current Koha database."""
    conn = get_koha_conn()
    cur = conn.cursor()
    cur.execute("SHOW TABLES;")
    print("=== Tables in Koha DB ===")
    for (tbl,) in cur.fetchall():
        print(f" - {tbl}")
    cur.close()
    conn.close()


def show_columns(table_name: str, schema: str = None):
    """
    Print column info for a given table.
    """
    conn = get_koha_conn()
    cur = conn.cursor()

    # Find current schema if not provided
    if schema is None:
        cur.execute("SELECT DATABASE();")
        row = cur.fetchone()
        schema = row[0]

    sql = """
        SELECT
            COLUMN_NAME,
            COLUMN_TYPE,
            IS_NULLABLE,
            COLUMN_KEY,
            COLUMN_DEFAULT,
            EXTRA
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME   = %s
        ORDER BY ORDINAL_POSITION;
    """
    cur.execute(sql, (schema, table_name))
    cols = cur.fetchall()

    print(f"\n=== Columns for {schema}.{table_name} ===")
    if not cols:
        print("No such table or no columns found.")
    else:
        for (
            name,
            col_type,
            is_nullable,
            col_key,
            default,
            extra,
        ) in cols:
            print(
                f"{name:30} {col_type:25} "
                f"NULLABLE={is_nullable:3} "
                f"KEY={col_key or '-':3} "
                f"DEFAULT={repr(default):10} "
                f"EXTRA={extra or '-'}"
            )

    cur.close()
    conn.close()


if __name__ == "__main__":
    import sys

    # Default to 'statistics' table if nothing passed
    table = sys.argv[1] if len(sys.argv) > 1 else "statistics"

    # Show tables in Koha DB
    show_tables()

    # Show columns for the provided table
    show_columns(table)
