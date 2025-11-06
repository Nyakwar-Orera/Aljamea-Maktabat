# routes/reports.py
from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, send_file
from db_koha import get_koha_conn
import pandas as pd
import io
from datetime import date

from services.exports import dataframe_to_pdf_bytes, dataframe_to_excel_bytes
from routes.students import get_student_info

bp = Blueprint("reports_bp", __name__)

# Borrower attribute codes we accept as "class"
CLASS_CODES = ("STD", "CLASS", "DAR", "CLASS_STD")

# Borrower attribute codes we accept for TR number lookups
TR_ATTR_CODES = ("TRNO", "TRN", "TR_NUMBER", "TR")  # include your local variants as needed


# ---------------- AY WINDOW ----------------
def _ay_bounds():
    today = date.today()
    year = today.year
    if today.month < 4:
        return None, None
    start = date(year, 4, 1)
    end = min(today, date(year, 12, 31))
    return start, end


# ---------------- INDIVIDUAL LOOKUP ----------------
def _resolve_borrower_by_identifier(identifier: str) -> int | None:
    if not identifier:
        return None

    conn = get_koha_conn()
    cur = conn.cursor()

    # 1) If numeric, try direct borrowernumber
    try:
        bn = int(identifier)
        cur.execute("SELECT borrowernumber FROM borrowers WHERE borrowernumber=%s", (bn,))
        row = cur.fetchone()
        if row:
            cur.close(); conn.close()
            return bn
    except ValueError:
        pass

    # 2) Cardnumber
    cur.execute("SELECT borrowernumber FROM borrowers WHERE cardnumber=%s", (identifier,))
    row = cur.fetchone()
    if row:
        bn = int(row[0])
        cur.close(); conn.close()
        return bn

    # 3) ITS (userid)
    cur = conn.cursor()
    cur.execute("SELECT borrowernumber FROM borrowers WHERE userid=%s", (identifier,))
    row = cur.fetchone()
    if row:
        bn = int(row[0])
        cur.close(); conn.close()
        return bn

    # 4) TR number via borrower_attributes
    placeholders = ",".join(["%s"] * len(TR_ATTR_CODES))
    sql = f"""
        SELECT ba.borrowernumber
        FROM borrower_attributes ba
        WHERE ba.code IN ({placeholders}) AND ba.attribute=%s
        LIMIT 1
    """
    cur.execute(sql, (*TR_ATTR_CODES, identifier))
    row = cur.fetchone()
    cur.close(); conn.close()
    if row:
        return int(row[0])

    return None


# ---------------- UTILITIES ----------------
def _class_rows_for_value(class_std: str) -> list[dict]:
    """
    Class-wise rows:
    - Removed: Category, Class
    - Added: Titles_AY (list of titles issued in this AY)
    - Keeps: Issues_AY (total issues in AY)
    - Replaced cardnumber with TRNumber (COALESCE(TR, cardnumber))
    """
    start, end = _ay_bounds()
    conn = get_koha_conn()
    cur = conn.cursor(dictionary=True)

    # Optional join for listing issued titles in AY
    titles_join = ""
    titles_params: list = []
    if start:
        titles_join = """
            LEFT JOIN (
                SELECT s.borrowernumber,
                       GROUP_CONCAT(biblio.title SEPARATOR ' | ') AS titles_ay
                FROM statistics s
                JOIN items  USING (itemnumber)
                JOIN biblio USING (biblionumber)
                WHERE s.type='issue' AND DATE(s.`datetime`) BETWEEN %s AND %s
                GROUP BY s.borrowernumber
            ) t ON t.borrowernumber = b.borrowernumber
        """
        titles_params = [start, end]

    sql = f"""
        SELECT
          b.borrowernumber,
          COALESCE(tr.attribute, b.cardnumber)               AS TRNumber,
          CONCAT(b.surname, ' ', b.firstname)                AS FullName,
          b.email                                            AS EduEmail,
          UPPER(COALESCE(b.sex,''))                          AS Sex,
          b.dateenrolled                                     AS Enrolled,
          b.dateexpiry                                       AS Expiry,
          COALESCE(a.active_loans, 0)                        AS ActiveLoans,
          COALESCE(a.overdues, 0)                            AS Overdues,
          COALESCE(ay.total_issues_ay, 0)                    AS Issues_AY,
          {("COALESCE(t.titles_ay,'')" if start else "''")}  AS Titles_AY,
          COALESCE(fay.fines_paid_ay, 0)                     AS FinesPaid_AY,
          COALESCE(ob.outstanding, 0)                        AS OutstandingBalance,
          li.last_issue                                      AS LastIssue
        FROM borrowers b
        LEFT JOIN borrower_attributes std
               ON std.borrowernumber = b.borrowernumber
              AND std.code IN ({",".join(["%s"]*len(CLASS_CODES))})
        LEFT JOIN borrower_attributes tr
               ON tr.borrowernumber = b.borrowernumber
              AND tr.code IN ({",".join(["%s"]*len(TR_ATTR_CODES))})
        LEFT JOIN (
            SELECT borrowernumber,
                   COUNT(*) AS active_loans,
                   SUM(CASE WHEN returndate IS NULL AND date_due < NOW() THEN 1 ELSE 0 END) AS overdues
            FROM issues
            WHERE returndate IS NULL
            GROUP BY borrowernumber
        ) a ON a.borrowernumber = b.borrowernumber
        LEFT JOIN (
            SELECT borrowernumber,
                   COUNT(*) AS total_issues_ay
            FROM statistics
            WHERE type='issue' {("AND DATE(`datetime`) BETWEEN %s AND %s" if start else "")}
            GROUP BY borrowernumber
        ) ay ON ay.borrowernumber = b.borrowernumber
        LEFT JOIN (
            SELECT borrowernumber,
                   MAX(DATE(`datetime`)) AS last_issue
            FROM statistics
            WHERE type='issue'
            GROUP BY borrowernumber
        ) li ON li.borrowernumber = b.borrowernumber
        LEFT JOIN (
            SELECT borrowernumber,
                   SUM(CASE
                         WHEN credit_type_code='PAYMENT'
                              AND (status IS NULL OR status <> 'VOID')
                              {("AND DATE(`date`) BETWEEN %s AND %s" if start else "")}
                         THEN -amount ELSE 0 END) AS fines_paid_ay
            FROM accountlines
            GROUP BY borrowernumber
        ) fay ON fay.borrowernumber = b.borrowernumber
        LEFT JOIN (
            SELECT borrowernumber,
                   SUM(COALESCE(amountoutstanding,0)) AS outstanding
            FROM accountlines
            GROUP BY borrowernumber
        ) ob ON ob.borrowernumber = b.borrowernumber
        {titles_join}
        WHERE (std.attribute = %s OR b.branchcode = %s)
        ORDER BY FullName ASC;
    """

    params = list(CLASS_CODES) + list(TR_ATTR_CODES)
    if start:
        params += [start, end]    # ay subquery
        params += [start, end]    # fines_ay subquery
    params += titles_params       # titles_ay subquery (if any)
    params += [class_std, class_std]

    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


def class_report(class_std: str | None):
    if class_std:
        rows = _class_rows_for_value(class_std)
    else:
        sql_list = f"""
            SELECT DISTINCT COALESCE(std.attribute, b.branchcode) AS cls
            FROM borrowers b
            LEFT JOIN borrower_attributes std
              ON std.borrowernumber = b.borrowernumber
             AND std.code IN ({",".join(["%s"]*len(CLASS_CODES))})
            WHERE COALESCE(std.attribute, b.branchcode) IS NOT NULL
            ORDER BY cls;
        """
        conn = get_koha_conn(); cur = conn.cursor()
        cur.execute(sql_list, CLASS_CODES)
        classes = [r[0] for r in cur.fetchall()]
        cur.close(); conn.close()

        rows = []
        for cls in classes:
            rows += _class_rows_for_value(cls)

    # Removed: Category, Class
    # Added: Titles_AY
    # Replaced 'cardnumber' with 'TRNumber'
    cols = ["borrowernumber","TRNumber","FullName","EduEmail","Sex",
            "Enrolled","Expiry","ActiveLoans","Overdues","Issues_AY",
            "Titles_AY","FinesPaid_AY","OutstandingBalance","LastIssue"]
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)


def _dept_rows_for_value(dept: str) -> list[dict]:
    """
    Department-wise rows:
    - Removed: Category
    - Keeps: Class (useful to see class within dept)
    - Keeps: Issues_AY
    - Replaced cardnumber with TRNumber (COALESCE(TR, cardnumber))
    """
    start, end = _ay_bounds()
    conn = get_koha_conn()
    cur = conn.cursor(dictionary=True)

    sql = f"""
        SELECT
          b.borrowernumber,
          COALESCE(tr.attribute, b.cardnumber)               AS TRNumber,
          CONCAT(b.surname, ' ', b.firstname)                AS FullName,
          b.email                                            AS EduEmail,
          UPPER(COALESCE(b.sex,''))                          AS Sex,
          b.dateenrolled                                     AS Enrolled,
          b.dateexpiry                                       AS Expiry,
          COALESCE(std.attribute, b.branchcode)              AS Class,
          COALESCE(a.active_loans, 0)                        AS ActiveLoans,
          COALESCE(a.overdues, 0)                            AS Overdues,
          COALESCE(ay.total_issues_ay, 0)                    AS Issues_AY,
          COALESCE(fay.fines_paid_ay, 0)                     AS FinesPaid_AY,
          COALESCE(ob.outstanding, 0)                        AS OutstandingBalance,
          li.last_issue                                      AS LastIssue
        FROM borrowers b
        LEFT JOIN borrower_attributes std
               ON std.borrowernumber = b.borrowernumber
              AND std.code IN ({",".join(["%s"]*len(CLASS_CODES))})
        LEFT JOIN borrower_attributes tr
               ON tr.borrowernumber = b.borrowernumber
              AND tr.code IN ({",".join(["%s"]*len(TR_ATTR_CODES))})
        LEFT JOIN (
            SELECT borrowernumber,
                   COUNT(*) AS active_loans,
                   SUM(CASE WHEN returndate IS NULL AND date_due < NOW() THEN 1 ELSE 0 END) AS overdues
            FROM issues
            WHERE returndate IS NULL
            GROUP BY borrowernumber
        ) a ON a.borrowernumber = b.borrowernumber
        LEFT JOIN (
            SELECT borrowernumber,
                   COUNT(*) AS total_issues_ay
            FROM statistics
            WHERE type='issue' {("AND DATE(`datetime`) BETWEEN %s AND %s" if start else "")}
            GROUP BY borrowernumber
        ) ay ON ay.borrowernumber = b.borrowernumber
        LEFT JOIN (
            SELECT borrowernumber,
                   MAX(DATE(`datetime`)) AS last_issue
            FROM statistics
            WHERE type='issue'
            GROUP BY borrowernumber
        ) li ON li.borrowernumber = b.borrowernumber
        LEFT JOIN (
            SELECT borrowernumber,
                   SUM(CASE
                         WHEN credit_type_code='PAYMENT'
                              AND (status IS NULL OR status <> 'VOID')
                              {("AND DATE(`date`) BETWEEN %s AND %s" if start else "")}
                         THEN -amount ELSE 0 END) AS fines_paid_ay
            FROM accountlines
            GROUP BY borrowernumber
        ) fay ON fay.borrowernumber = b.borrowernumber
        LEFT JOIN (
            SELECT borrowernumber,
                   SUM(COALESCE(amountoutstanding,0)) AS outstanding
            FROM accountlines
            GROUP BY borrowernumber
        ) ob ON ob.borrowernumber = b.borrowernumber
        LEFT JOIN categories c ON c.categorycode = b.categorycode
        WHERE (COALESCE(c.description, b.categorycode) = %s OR b.categorycode = %s)
        ORDER BY FullName ASC;
    """

    params = list(CLASS_CODES) + list(TR_ATTR_CODES)
    if start:
        params += [start, end]
        params += [start, end]
    params += [dept, dept]

    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


def department_report(dept_code: str | None):
    if dept_code:
        rows = _dept_rows_for_value(dept_code)
    else:
        sql_list = """
            SELECT DISTINCT COALESCE(c.description, b.categorycode) AS dept
            FROM borrowers b
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            WHERE COALESCE(c.description, b.categorycode) IS NOT NULL
            ORDER BY dept;
        """
        conn = get_koha_conn(); cur = conn.cursor()
        cur.execute(sql_list)
        depts = [r[0] for r in cur.fetchall()]
        cur.close(); conn.close()

        rows = []
        for d in depts:
            rows += _dept_rows_for_value(d)

    # Removed: Category; TRNumber shown instead of cardnumber
    cols = ["borrowernumber","TRNumber","FullName","EduEmail","Sex",
            "Enrolled","Expiry","Class","ActiveLoans","Overdues","Issues_AY",
            "FinesPaid_AY","OutstandingBalance","LastIssue"]
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)


def top_books_df(arabic_only=False, limit=25):
    """
    Top titles for the CURRENT AY window (Apr 1 -> today, max Dec 31),
    derived from `statistics` (type='issue').
    - arabic_only=True  => filter titles containing Arabic characters
    - arabic_only=False => all languages
    """
    start, end = _ay_bounds()
    if not start:
        return pd.DataFrame(columns=["Title", "Count", "LastIssued"])

    conn = get_koha_conn()
    cur = conn.cursor()

    lang_filter = "AND biblio.title REGEXP '[ء-ي]'" if arabic_only else ""
    cur.execute(f"""
        SELECT biblio.title, COUNT(*) AS cnt, MAX(DATE(s.`datetime`)) AS last_issued
        FROM statistics s
        JOIN items  USING (itemnumber)
        JOIN biblio USING (biblionumber)
        WHERE s.type='issue'
          AND DATE(s.`datetime`) BETWEEN %s AND %s
          {lang_filter}
        GROUP BY biblio.biblionumber
        ORDER BY cnt DESC
        LIMIT %s;
    """, (start, end, limit))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return pd.DataFrame(columns=["Title", "Count", "LastIssued"])
    return pd.DataFrame(rows, columns=["Title", "Count", "LastIssued"])


# ---------------- ROUTES ----------------
@bp.route("/")
def reports_page():
    if not session.get("logged_in"):
        return redirect(url_for("admin_bp.index"))

    conn = get_koha_conn(); cur = conn.cursor()

    sql_classes = f"""
        SELECT DISTINCT COALESCE(std.attribute, b.branchcode) AS cls
        FROM borrowers b
        LEFT JOIN borrower_attributes std
          ON std.borrowernumber = b.borrowernumber
         AND std.code IN ({",".join(["%s"]*len(CLASS_CODES))})
        WHERE COALESCE(std.attribute, b.branchcode) IS NOT NULL
        ORDER BY cls;
    """
    cur.execute(sql_classes, CLASS_CODES)
    classes = [r[0] for r in cur.fetchall()]

    cur.execute("""
        SELECT DISTINCT COALESCE(c.description, b.categorycode) AS dept
        FROM borrowers b
        LEFT JOIN categories c ON c.categorycode = b.categorycode
        WHERE COALESCE(c.description, b.categorycode) IS NOT NULL
        ORDER BY dept;
    """)
    departments = [r[0] for r in cur.fetchall()]
    cur.close(); conn.close()

    return render_template("reports.html", classes=classes, departments=departments)


@bp.route("/api/generate_report", methods=["POST"])
def generate_report():
    if not session.get("logged_in"):
        return jsonify(success=False)

    report_type = request.form.get("report_type")

    if report_type == "class_wise":
        class_val = request.form.get("class_value")
        df = class_report(class_val if class_val else None)
        html = df.to_html(classes="table table-sm table-striped", index=False,
                          float_format=lambda x: f"{x:,.2f}")
        return jsonify(success=True, html=html)

    elif report_type == "department_wise":
        dept_val = request.form.get("department_value")
        df = department_report(dept_val if dept_val else None)
        html = df.to_html(classes="table table-sm table-striped", index=False,
                          float_format=lambda x: f"{x:,.2f}")
        return jsonify(success=True, html=html)

    elif report_type == "individual":
        identifier = (request.form.get("identifier") or "").strip()
        try:
            borrowernumber = _resolve_borrower_by_identifier(identifier)
            if not borrowernumber:
                return jsonify(success=False, html="<p>No student found.</p>")
            info = get_student_info(str(borrowernumber))
            if not info:
                return jsonify(success=False, html="<p>No student found.</p>")
            rendered_html = render_template("student.html", found=True, info=info, hide_nav=True)
            return jsonify(success=True, html=rendered_html)
        except Exception:
            return jsonify(success=False, html="<p>Unexpected error while looking up the student.</p>")

    elif report_type == "top_books":
        df = top_books_df(arabic_only=False)
        return jsonify(success=True, html=df.to_html(classes="table table-sm table-striped", index=False))

    elif report_type == "top_arabic":
        df = top_books_df(arabic_only=True)
        return jsonify(success=True, html=df.to_html(classes="table table-sm table-striped", index=False))

    return jsonify(success=False, html="<p>Unknown report type.</p>")


# ---------------- EXPORT ROUTES (PDF) ----------------
@bp.route("/export/class/<class_val>/pdf")
def export_class_pdf(class_val):
    df = class_report(class_val if class_val != "All" else None)
    pdf_bytes = dataframe_to_pdf_bytes(f"Class Report - {class_val}", df)
    return send_file(io.BytesIO(pdf_bytes), as_attachment=True,
                     download_name=f"class_report_{class_val}.pdf",
                     mimetype="application/pdf")


@bp.route("/export/department/<dept_val>/pdf")
def export_department_pdf(dept_val):
    df = department_report(dept_val if dept_val != "All" else None)
    pdf_bytes = dataframe_to_pdf_bytes(f"Department Report - {dept_val}", df)
    return send_file(io.BytesIO(pdf_bytes), as_attachment=True,
                     download_name=f"department_report_{dept_val}.pdf",
                     mimetype="application/pdf")


@bp.route("/export/top_books/pdf")
def export_top_books_pdf():
    df = top_books_df(arabic_only=False)
    pdf_bytes = dataframe_to_pdf_bytes("Top 25 Books (All)", df)
    return send_file(io.BytesIO(pdf_bytes), as_attachment=True,
                     download_name="top_books.pdf",
                     mimetype="application/pdf")


@bp.route("/export/top_arabic/pdf")
def export_top_arabic_pdf():
    df = top_books_df(arabic_only=True)
    pdf_bytes = dataframe_to_pdf_bytes("Top 25 Arabic Books", df)
    return send_file(io.BytesIO(pdf_bytes), as_attachment=True,
                     download_name="top_arabic_books.pdf",
                     mimetype="application/pdf")


# ---------------- EXPORT ROUTES (EXCEL) ----------------
@bp.route("/export/class/<class_val>/excel")
def export_class_excel(class_val):
    df = class_report(class_val if class_val != "All" else None)
    xls_bytes = dataframe_to_excel_bytes(df, sheet_name=f"Class_{class_val}")
    return send_file(io.BytesIO(xls_bytes), as_attachment=True,
                     download_name=f"class_report_{class_val}.xlsx",
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@bp.route("/export/department/<dept_val>/excel")
def export_department_excel(dept_val):
    df = department_report(dept_val if dept_val != "All" else None)
    xls_bytes = dataframe_to_excel_bytes(df, sheet_name=f"Dept_{dept_val}")
    return send_file(io.BytesIO(xls_bytes), as_attachment=True,
                     download_name=f"department_report_{dept_val}.xlsx",
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@bp.route("/export/top_books/excel")
def export_top_books_excel():
    df = top_books_df(arabic_only=False)
    xls_bytes = dataframe_to_excel_bytes(df, sheet_name="TopBooks_All")
    return send_file(io.BytesIO(xls_bytes), as_attachment=True,
                     download_name="top_books.xlsx",
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@bp.route("/export/top_arabic/excel")
def export_top_arabic_excel():
    df = top_books_df(arabic_only=True)
    xls_bytes = dataframe_to_excel_bytes(df, sheet_name="TopBooks_Arabic")
    return send_file(io.BytesIO(xls_bytes), as_attachment=True,
                     download_name="top_arabic_books.xlsx",
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
