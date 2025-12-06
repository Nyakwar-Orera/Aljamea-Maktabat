# routes/reports.py
from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, send_file
from db_koha import get_koha_conn
import pandas as pd
import io
import re
from datetime import date
from flask import current_app

from services.exports import dataframe_to_pdf_bytes, dataframe_to_excel_bytes
from routes.students import get_student_info

bp = Blueprint("reports_bp", __name__)

# Borrower attribute codes we accept as "class"
CLASS_CODES = ("STD", "CLASS", "DAR", "CLASS_STD")

# Borrower attribute codes we accept for TR number lookups
TR_ATTR_CODES = ("TRNO", "TRN", "TR_NUMBER", "TR")  # include your local variants as needed


# ---------------- ROLE HELPERS ----------------
def _current_role() -> str:
    """
    Normalize role from session:
    - 'admin'
    - 'hod'
    - 'teacher'
    - or '' if missing
    """
    return (session.get("role") or "").strip().lower()


def _hod_department() -> str | None:
    """
    Return the HOD's department label as stored in session["department_name"].
    This should match COALESCE(categories.description, categorycode) in Koha.
    """
    dep = session.get("department_name")
    if dep:
        return str(dep)
    return None


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
    """
    Resolve a patron by:
      1) borrowernumber
      2) cardnumber
      3) userid (ITS)
      4) TR number via borrower_attributes

    Only returns ACTIVE patrons:
      - dateexpiry IS NULL OR >= today
    """
    if not identifier:
        return None

    conn = get_koha_conn()
    cur = conn.cursor()

    # common active filter
    active_filter = " AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())"

    # 1) If numeric, try direct borrowernumber
    try:
        bn = int(identifier)
        cur.execute(
            f"""
            SELECT b.borrowernumber
            FROM borrowers b
            WHERE b.borrowernumber=%s
              {active_filter}
            """,
            (bn,),
        )
        row = cur.fetchone()
        if row:
            cur.close()
            conn.close()
            return int(row[0])
    except ValueError:
        pass

    # 2) Cardnumber
    cur.execute(
        f"""
        SELECT b.borrowernumber
        FROM borrowers b
        WHERE b.cardnumber=%s
          {active_filter}
        """,
        (identifier,),
    )
    row = cur.fetchone()
    if row:
        bn = int(row[0])
        cur.close()
        conn.close()
        return bn

    # 3) ITS (userid)
    cur.execute(
        f"""
        SELECT b.borrowernumber
        FROM borrowers b
        WHERE b.userid=%s
          {active_filter}
        """,
        (identifier,),
    )
    row = cur.fetchone()
    if row:
        bn = int(row[0])
        cur.close()
        conn.close()
        return bn

    # 4) TR number via borrower_attributes (joined to borrowers for active filter)
    placeholders = ",".join(["%s"] * len(TR_ATTR_CODES))
    sql = f"""
        SELECT b.borrowernumber
        FROM borrower_attributes ba
        JOIN borrowers b ON b.borrowernumber = ba.borrowernumber
        WHERE ba.code IN ({placeholders})
          AND ba.attribute=%s
          {active_filter}
        LIMIT 1
    """
    cur.execute(sql, (*TR_ATTR_CODES, identifier))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row:
        return int(row[0])

    return None


# ---------------- UTILITIES ----------------
def _class_rows_for_value(class_std: str, dept_filter: str | None = None) -> list[dict]:
    """
    Class-wise rows (per Darajah) with AY metrics:

    - Only ACTIVE patrons (dateexpiry IS NULL or >= today)
    - Optional dept_filter => restrict to that department
      (matching COALESCE(categories.description, categorycode))
    - Replaces cardnumber with TRNumber (COALESCE(TR, cardnumber))
    - Adds:
        * Issues_AY
        * FinesPaid_AY
        * OutstandingBalance
        * Collections
        * Language
    """
    start, end = _ay_bounds()
    conn = get_koha_conn()
    cur = conn.cursor(dictionary=True)

    # Fixed: Collections and language subquery
    collections_language_join = """
        LEFT JOIN (
            SELECT s.borrowernumber,
                   GROUP_CONCAT(DISTINCT it.ccode ORDER BY it.ccode SEPARATOR ', ') AS collections,
                   ExtractValue(
                       bmd.metadata,
                       '//datafield[@tag="041"]/subfield[@code="a"]'
                   ) AS language
            FROM statistics s
            JOIN items it ON it.itemnumber = s.itemnumber
            JOIN biblio bib ON it.biblionumber = bib.biblionumber
            LEFT JOIN biblio_metadata bmd ON bib.biblionumber = bmd.biblionumber
            WHERE s.type = 'issue' AND DATE(s.`datetime`) BETWEEN %s AND %s
            GROUP BY s.borrowernumber
        ) cl ON cl.borrowernumber = b.borrowernumber
    """
    collections_language_params = [start, end]

    dept_clause = ""
    if dept_filter:
        dept_clause = "AND COALESCE(c.description, b.categorycode) = %s"

    # Build the query based on whether we have AY dates
    ay_where = "AND DATE(`datetime`) BETWEEN %s AND %s" if start else ""
    fay_where = "AND DATE(`date`) BETWEEN %s AND %s" if start else ""
    
    sql = f"""
        SELECT
          b.borrowernumber,
          COALESCE(tr.attribute, b.cardnumber)               AS TRNumber,
          CONCAT(
            COALESCE(b.surname, ''),
            CASE WHEN b.surname IS NOT NULL AND b.firstname IS NOT NULL THEN ' ' ELSE '' END,
            COALESCE(b.firstname, '')
          )                                                   AS FullName,
          b.email                                            AS EduEmail,
          UPPER(COALESCE(b.sex,''))                          AS Sex,
          b.dateenrolled                                     AS Enrolled,
          b.dateexpiry                                       AS Expiry,
          COALESCE(a.active_loans, 0)                        AS ActiveLoans,
          COALESCE(a.overdues, 0)                            AS Overdues,
          COALESCE(ay.total_issues_ay, 0)                    AS Issues_AY,
          COALESCE(fay.fines_paid_ay, 0)                     AS FinesPaid_AY,
          COALESCE(ob.outstanding, 0)                        AS OutstandingBalance,
          cl.collections                                     AS Collections,
          cl.language                                        AS Language
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
            WHERE type='issue' {ay_where}
            GROUP BY borrowernumber
        ) ay ON ay.borrowernumber = b.borrowernumber
        LEFT JOIN (
            SELECT borrowernumber,
                   SUM(CASE
                         WHEN credit_type_code='PAYMENT'
                              AND (status IS NULL OR status <> 'VOID')
                              {fay_where}
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
        {collections_language_join}
        WHERE (std.attribute = %s OR b.branchcode = %s)
          AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
          {dept_clause}
        ORDER BY FullName ASC;
    """

    # Build parameters
    params = list(CLASS_CODES) + list(TR_ATTR_CODES)
    if start:
        params += [start, end]    # ay subquery
    if start:
        params += [start, end]    # fines_ay subquery
    params += collections_language_params       # collections and language subquery
    params += [class_std, class_std]
    if dept_filter:
        params.append(dept_filter)

    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Calculate total number of students in class after fetching data
    total_students = len(rows)

    # Add total students to each row
    for row in rows:
        row["TotalStudents"] = total_students

    return rows

def class_report(class_std: str | None, dept_filter: str | None = None):
    """
    Class-wise (Darajah) report.

    - If class_std is provided: one Darajah.
    - If None: all Darajahs (optionally limited to dept_filter).
    
    Returns: (DataFrame, total_students)
    """
    if class_std:
        # If a specific class is provided, fetch class-specific rows
        rows = _class_rows_for_value(class_std, dept_filter)
        total_students = len(rows) if rows else 0
    else:
        # Discover all classes first (optionally limited to dept_filter)
        conn = get_koha_conn()
        cur = conn.cursor()

        dept_clause = ""
        params: list = list(CLASS_CODES)
        if dept_filter:
            dept_clause = "AND COALESCE(c.description, b.categorycode) = %s"
            params.append(dept_filter)

        sql_list = f"""
            SELECT DISTINCT COALESCE(std.attribute, b.branchcode) AS cls
            FROM borrowers b
            LEFT JOIN borrower_attributes std
              ON std.borrowernumber = b.borrowernumber
             AND std.code IN ({",".join(["%s"]*len(CLASS_CODES))})
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            WHERE COALESCE(std.attribute, b.branchcode) IS NOT NULL
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              {dept_clause}
            ORDER BY cls;
        """
        cur.execute(sql_list, params)
        classes = [r[0] for r in cur.fetchall()]
        cur.close()
        conn.close()

        # Fetch rows for each class and aggregate them
        rows = []
        for cls in classes:
            rows += _class_rows_for_value(cls, dept_filter)
        
        total_students = len(rows)

    # Define the columns for the returned DataFrame
    cols = [
        "borrowernumber",
        "TRNumber",
        "FullName",
        "EduEmail",
        "Sex",
        "Enrolled",
        "Expiry",
        "ActiveLoans",
        "Overdues",
        "Issues_AY",
        "FinesPaid_AY",
        "OutstandingBalance",
        "Collections",
        "Language",
        "TotalStudents"
    ]

    # Return the rows as a DataFrame and total students
    df = pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)
    return df, total_students


def _dept_rows_for_value(dept: str, dept_filter: str | None = None) -> list[dict]:
    """
    Department-wise rows:
    - Only ACTIVE patrons (dateexpiry IS NULL or >= today)
    - Keeps: Class (STD / branchcode)
    - Keeps: Issues_AY
    - Adds: Collections, Language
    - Reorders columns for a more professional layout
    """
    start, end = _ay_bounds()
    conn = get_koha_conn()
    cur = conn.cursor(dictionary=True)

    # Fixed: Collections and language subquery
    collections_language_join = """
        LEFT JOIN (
            SELECT s.borrowernumber,
                   GROUP_CONCAT(DISTINCT it.ccode ORDER BY it.ccode SEPARATOR ', ') AS collections,
                   ExtractValue(
                       bmd.metadata,
                       '//datafield[@tag="041"]/subfield[@code="a"]'
                   ) AS language
            FROM statistics s
            JOIN items it ON it.itemnumber = s.itemnumber
            JOIN biblio bib ON it.biblionumber = bib.biblionumber
            LEFT JOIN biblio_metadata bmd ON bib.biblionumber = bmd.biblionumber
            WHERE s.type = 'issue' AND DATE(s.`datetime`) BETWEEN %s AND %s
            GROUP BY s.borrowernumber
        ) cl ON cl.borrowernumber = b.borrowernumber
    """
    collections_language_params = [start, end]

    # Build the query based on whether we have AY dates
    ay_where = "AND DATE(`datetime`) BETWEEN %s AND %s" if start else ""
    fay_where = "AND DATE(`date`) BETWEEN %s AND %s" if start else ""
    
    sql = f"""
        SELECT
          b.borrowernumber,
          COALESCE(tr.attribute, b.cardnumber)               AS TRNumber,
          CONCAT(
            COALESCE(b.surname, ''),
            CASE WHEN b.surname IS NOT NULL AND b.firstname IS NOT NULL THEN ' ' ELSE '' END,
            COALESCE(b.firstname, '')
          )                                                   AS FullName,
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
          cl.collections                                     AS Collections,
          cl.language                                        AS Language
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
            WHERE type='issue' {ay_where}
            GROUP BY borrowernumber
        ) ay ON ay.borrowernumber = b.borrowernumber
        LEFT JOIN (
            SELECT borrowernumber,
                   SUM(CASE
                         WHEN credit_type_code='PAYMENT'
                              AND (status IS NULL OR status <> 'VOID')
                              {fay_where}
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
        {collections_language_join}
        WHERE (COALESCE(c.description, b.categorycode) = %s OR b.categorycode = %s)
          AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
        ORDER BY FullName ASC;
    """

    # Build parameters
    params = list(CLASS_CODES) + list(TR_ATTR_CODES)
    if start:
        params += [start, end]    # ay subquery
    if start:
        params += [start, end]    # fines_ay subquery
    params += collections_language_params       # collections and language subquery
    params += [dept, dept]

    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Add total number of students in department to each row
    total_students = len(rows)
    for row in rows:
        row["TotalStudents"] = total_students

    return rows


def department_report(dept_code: str | None):
    """
    Department-wise report (Admin only; HOD is blocked before calling this).
    
    Returns: (DataFrame, total_students)
    """
    if dept_code:
        rows = _dept_rows_for_value(dept_code)
        total_students = len(rows) if rows else 0
    else:
        sql_list = """
            SELECT DISTINCT COALESCE(c.description, b.categorycode) AS dept
            FROM borrowers b
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            WHERE COALESCE(c.description, b.categorycode) IS NOT NULL
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
            ORDER BY dept;
        """
        conn = get_koha_conn()
        cur = conn.cursor()
        cur.execute(sql_list)
        depts = [r[0] for r in cur.fetchall()]
        cur.close()
        conn.close()

        rows = []
        for d in depts:
            rows += _dept_rows_for_value(d)
        
        total_students = len(rows)

    # Adjust columns to make the report more professional and focused
    cols = [
        "TRNumber",
        "FullName",
        "EduEmail",
        "Class",
        "Sex",
        "Enrolled",
        "Expiry",
        "ActiveLoans",
        "Overdues",
        "Issues_AY",
        "FinesPaid_AY",
        "OutstandingBalance",
        "Collections",
        "Language",
        "TotalStudents"
    ]
    
    df = pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)
    return df, total_students

# ---------------- HTML CLEANING UTILITY ----------------
def clean_html_for_pdf(html_text: str) -> str:
    """
    Clean HTML tags that ReportLab's Paragraph parser doesn't support.
    Removes span tags, style attributes, target="_blank", etc.
    """
    if not html_text or not isinstance(html_text, str):
        return str(html_text) if html_text is not None else ""
    
    # Remove span tags completely (they cause issues with inline styles)
    html_text = re.sub(r'<span[^>]*>', '', html_text)
    html_text = re.sub(r'</span>', '', html_text)
    
    # Remove style attributes from any tag
    html_text = re.sub(r' style="[^"]*"', '', html_text)
    
    # Remove target="_blank" from links
    html_text = re.sub(r' target="_blank"', '', html_text)
    
    # Remove class attributes
    html_text = re.sub(r' class="[^"]*"', '', html_text)
    
    # Remove data-* attributes
    html_text = re.sub(r' data-[^=]*="[^"]*"', '', html_text)
    
    # Remove div tags but keep content
    html_text = re.sub(r'<div[^>]*>', '', html_text)
    html_text = re.sub(r'</div>', '', html_text)
    
    # Convert <br/> to <br />
    html_text = html_text.replace('<br/>', '<br />')
    
    # Remove multiple spaces
    html_text = re.sub(r'\s+', ' ', html_text).strip()
    
    return html_text

def clean_dataframe_for_pdf(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean HTML from all string columns in a DataFrame for PDF export.
    """
    if df.empty:
        return df
    
    df_clean = df.copy()
    
    for col in df_clean.columns:
        if df_clean[col].dtype == 'object':
            df_clean[col] = df_clean[col].apply(
                lambda x: clean_html_for_pdf(str(x)) if pd.notna(x) else ""
            )
    
    return df_clean

def top_books_df(
    arabic_only: bool = False,
    english_only: bool = False,
    limit: int = 25,
    dept_filter: str | None = None,
    for_pdf: bool = False  # Add this parameter to differentiate between web and PDF
):
    """
    Top titles for the CURRENT AY window (Apr 1 -> today, max Dec 31),
    derived from issues + old_issues (all_iss), joined via MARC.

    Professional extras:
      - Language from MARC 041$a
      - Collections from items.ccode (GROUP_CONCAT)
      - Only ACTIVE patrons (dateexpiry IS NULL or >= today)

    Filters:
      - arabic_only  => 041$a LIKE 'ar%%'
      - english_only => 041$a LIKE 'eng%%'
      - dept_filter  => restrict borrowers to COALESCE(categories.description, categorycode)
      - for_pdf      => if True, don't add HTML links, just plain text
    """
    start, end = _ay_bounds()
    if not start:
        return pd.DataFrame(columns=["Title", "Language", "Collections", "Count", "LastIssued"])

    conn = get_koha_conn()
    cur = conn.cursor()

    lang_clause = ""
    lang_param = None
    if arabic_only:
        lang_clause = """
          AND ExtractValue(
                bmd.metadata,
                '//datafield[@tag="041"]/subfield[@code="a"]'
              ) LIKE %s
        """
        lang_param = "ar%"
    elif english_only:
        lang_clause = """
          AND ExtractValue(
                bmd.metadata,
                '//datafield[@tag="041"]/subfield[@code="a"]'
              ) LIKE %s
        """
        lang_param = "eng%"

    dept_clause = ""
    if dept_filter:
        dept_clause = "AND COALESCE(c.description, b.categorycode) = %s"

    sql = f"""
        SELECT
            bib.title AS Title,
            ExtractValue(
                bmd.metadata,
                '//datafield[@tag="041"]/subfield[@code="a"]'
            ) AS Language,
            GROUP_CONCAT(DISTINCT it.ccode ORDER BY it.ccode SEPARATOR ', ') AS Collections,
            COUNT(*) AS cnt,
            MAX(DATE(all_iss.issuedate)) AS last_issued,
            bib.biblionumber AS BiblioNumber
        FROM (
            SELECT borrowernumber, itemnumber, issuedate
            FROM issues
            UNION ALL
            SELECT borrowernumber, itemnumber, issuedate
            FROM old_issues
        ) all_iss
        JOIN borrowers b
             ON b.borrowernumber = all_iss.borrowernumber
        LEFT JOIN categories c
             ON c.categorycode = b.categorycode
        JOIN items it
             ON all_iss.itemnumber = it.itemnumber
        JOIN biblio bib
             ON it.biblionumber = bib.biblionumber
        LEFT JOIN biblio_metadata bmd
             ON bib.biblionumber = bmd.biblionumber
        WHERE DATE(all_iss.issuedate) BETWEEN %s AND %s
          AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
          {dept_clause}
          {lang_clause}
        GROUP BY bib.biblionumber, bib.title, Language
        ORDER BY cnt DESC
        LIMIT %s;
    """

    params: list = [start, end]
    if dept_filter:
        params.append(dept_filter)
    if lang_clause:
        params.append(lang_param)
    params.append(int(limit))

    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        return pd.DataFrame(columns=["Title", "Language", "Collections", "Count", "LastIssued"])

    processed_rows = []
    for row in rows:
        # Tuple indices: 0=Title, 1=Language, 2=Collections, 3=cnt, 4=last_issued, 5=BiblioNumber
        title = row[0]
        language = row[1]
        collections = row[2]
        count = row[3]
        last_issued = row[4]
        bib_number = row[5]  # This is the BiblioNumber
        
        # If exporting for PDF, use plain text without HTML
        if for_pdf:
            processed_rows.append({
                "Title": title,
                "Language": language,
                "Collections": collections,
                "Count": count,
                "LastIssued": last_issued,
                "BiblioNumber": bib_number  # Keep for reference but not shown
            })
        else:
            # For web display, create clickable link
            title_with_link = f'<a href="https://library-opac.ajsn.co.ke/catalog/{bib_number}" target="_blank">{title}</a>'
            
            # Add custom styling for Arabic titles
            if language and language.lower().startswith('ar'):
                title_with_link = f'<span style="font-family: Al Kanz, sans-serif; text-align: center;">{title_with_link}</span>'
            else:
                title_with_link = f'<span style="text-align: center;">{title_with_link}</span>'
            
            processed_rows.append({
                "Title": title_with_link,
                "Language": language,
                "Collections": collections,
                "Count": count,
                "LastIssued": last_issued
            })
    
    columns = ["Title", "Language", "Collections", "Count", "LastIssued", "BiblioNumber"] if for_pdf else ["Title", "Language", "Collections", "Count", "LastIssued"]
    df = pd.DataFrame(processed_rows, columns=columns)
    return df

# ---------------- ROUTES ----------------
@bp.route("/")
def reports_page():
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = _current_role()
    is_admin = role == "admin"
    is_hod = role == "hod"
    hod_dept = _hod_department()

    conn = get_koha_conn()
    cur = conn.cursor()

    # Classes list
    if is_hod and hod_dept:
        # Only classes within this HOD's department
        sql_classes = f"""
            SELECT DISTINCT COALESCE(std.attribute, b.branchcode) AS cls
            FROM borrowers b
            LEFT JOIN borrower_attributes std
              ON std.borrowernumber = b.borrowernumber
             AND std.code IN ({",".join(["%s"]*len(CLASS_CODES))})
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            WHERE COALESCE(std.attribute, b.branchcode) IS NOT NULL
              AND COALESCE(c.description, b.categorycode) = %s
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
            ORDER BY cls;
        """
        params = list(CLASS_CODES) + [hod_dept]
        cur.execute(sql_classes, params)
    else:
        # All classes for Admin (or other roles)
        sql_classes = f"""
            SELECT DISTINCT COALESCE(std.attribute, b.branchcode) AS cls
            FROM borrowers b
            LEFT JOIN borrower_attributes std
              ON std.borrowernumber = b.borrowernumber
             AND std.code IN ({",".join(["%s"]*len(CLASS_CODES))})
            WHERE COALESCE(std.attribute, b.branchcode) IS NOT NULL
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
            ORDER BY cls;
        """
        cur.execute(sql_classes, CLASS_CODES)
    classes = [r[0] for r in cur.fetchall()]

    # Departments list – only needed for Admin (HOD never sees department-wise option)
    if is_admin:
        cur.execute(
            """
            SELECT DISTINCT COALESCE(c.description, b.categorycode) AS dept
            FROM borrowers b
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            WHERE COALESCE(c.description, b.categorycode) IS NOT NULL
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
            ORDER BY dept;
            """
        )
        departments = [r[0] for r in cur.fetchall()]
    else:
        departments = []

    cur.close()
    conn.close()

    return render_template(
        "reports.html",
        classes=classes,
        departments=departments,
        is_hod=is_hod,
        is_admin=is_admin,
        hod_dept=hod_dept,
    )

@bp.route("/api/generate_report", methods=["POST"])
def generate_report():
    if not session.get("logged_in"):
        return jsonify(success=False)

    role = _current_role()
    is_admin = role == "admin"
    is_hod = role == "hod"
    hod_dept = _hod_department()

    report_type = request.form.get("report_type")

    # -------- CLASS-WISE (Darajah) --------
    if report_type == "class_wise":
        class_val = request.form.get("class_value")

        if is_hod and hod_dept:
            # HOD: restrict to their department (class list already filtered in UI as well)
            df, total_students = class_report(class_val if class_val else None, dept_filter=hod_dept)
        else:
            df, total_students = class_report(class_val if class_val else None, dept_filter=None)

        html = df.to_html(
            classes="table table-sm table-striped",
            index=False,
            float_format=lambda x: f"{x:,.2f}",
            escape=False  # ADD THIS LINE - allows HTML rendering
        )
        
        # Add total students to response
        response_data = {
            "success": not df.empty,
            "html": html,
            "total_students": total_students,
            "class_value": class_val or "All"
        }
        return jsonify(response_data)

    # -------- DEPARTMENT-WISE --------
    elif report_type == "department_wise":
        # HOD must NOT see this at all
        if is_hod:
            return jsonify(success=False, html="<p>You are not allowed to view department-wise reports.</p>")

        dept_val = request.form.get("department_value")
        df, total_students = department_report(dept_val if dept_val else None)
        html = df.to_html(
            classes="table table-sm table-striped",
            index=False,
            float_format=lambda x: f"{x:,.2f}",
            escape=False  # ADD THIS LINE
        )
        
        response_data = {
            "success": not df.empty,
            "html": html,
            "total_students": total_students,
            "dept_value": dept_val or "All"
        }
        return jsonify(response_data)

    # -------- INDIVIDUAL STUDENT --------
    elif report_type == "individual":
        identifier = (request.form.get("identifier") or "").strip()
        try:
            borrowernumber = _resolve_borrower_by_identifier(identifier)
            if not borrowernumber:
                return jsonify(success=False, html="<p>No active student found.</p>")

            # If HOD, ensure this student is in their department
            if is_hod and hod_dept:
                conn = get_koha_conn()
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT COALESCE(c.description, b.categorycode) AS dept
                    FROM borrowers b
                    LEFT JOIN categories c ON c.categorycode = b.categorycode
                    WHERE b.borrowernumber = %s;
                    """,
                    (borrowernumber,),
                )
                row = cur.fetchone()
                cur.close()
                conn.close()
                if not row or (row[0] != hod_dept):
                    return jsonify(success=False, html="<p>Student not in your department.</p>")

            info = get_student_info(str(borrowernumber))
            if not info:
                return jsonify(success=False, html="<p>No student found.</p>")

            # ✅ Get OPAC base URL from Flask config
            opac_base = current_app.config.get("KOHA_OPAC_BASE_URL", "https://library-opac.ajsn.co.ke")
            
            # ✅ Render with OPAC URL
            rendered_html = render_template(
                "student.html", 
                found=True, 
                info=info, 
                hide_nav=True,
                opac_base_url=opac_base  # This is the key - pass the URL to template
            )
            return jsonify(success=True, html=rendered_html)
        except Exception as e:
            current_app.logger.error(f"Error in individual report: {e}")
            return jsonify(success=False, html="<p>Unexpected error while looking up the student.</p>")

    # -------- TOP 25 ENGLISH (MARC 041 eng%) --------
    elif report_type == "top_books":
        if is_hod and hod_dept:
            df = top_books_df(arabic_only=False, english_only=True, dept_filter=hod_dept, for_pdf=False)
        else:
            df = top_books_df(arabic_only=False, english_only=True, dept_filter=None, for_pdf=False)

        html = df.to_html(
            classes="table table-sm table-striped",
            index=False,
            float_format=lambda x: f"{x:,.2f}",
            escape=False  # ADD THIS LINE
        )
        return jsonify(success=not df.empty, html=html)

    # -------- TOP 25 ARABIC (MARC 041 ar%) --------
    elif report_type == "top_arabic":
        if is_hod and hod_dept:
            df = top_books_df(arabic_only=True, english_only=False, dept_filter=hod_dept, for_pdf=False)
        else:
            df = top_books_df(arabic_only=True, english_only=False, dept_filter=None, for_pdf=False)

        html = df.to_html(
            classes="table table-sm table-striped",
            index=False,
            float_format=lambda x: f"{x:,.2f}",
            escape=False  # ADD THIS LINE
        )
        return jsonify(success=not df.empty, html=html)

    return jsonify(success=False, html="<p>Unknown report type.</p>")

# ---------------- EXPORT ROUTES (PDF) ----------------
@bp.route("/export/class/<class_val>/pdf")
def export_class_pdf(class_val):
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = _current_role()
    is_hod = role == "hod"
    hod_dept = _hod_department() if is_hod else None

    df, _ = class_report(class_val if class_val != "All" else None, dept_filter=hod_dept)
    
    # Clean HTML from DataFrame before PDF generation
    df_clean = clean_dataframe_for_pdf(df)
    
    pdf_bytes = dataframe_to_pdf_bytes(f"Class Report - {class_val}", df_clean)
    return send_file(
        io.BytesIO(pdf_bytes),
        as_attachment=True,
        download_name=f"class_report_{class_val}.pdf",
        mimetype="application/pdf",
    )


@bp.route("/export/department/<dept_val>/pdf")
def export_department_pdf(dept_val):
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    # HOD not allowed department-wise
    if _current_role() == "hod":
        return redirect(url_for("reports_bp.reports_page"))

    df, _ = department_report(dept_val if dept_val != "All" else None)
    
    # Clean HTML from DataFrame before PDF generation
    df_clean = clean_dataframe_for_pdf(df)
    
    pdf_bytes = dataframe_to_pdf_bytes(f"Department Report - {dept_val}", df_clean)
    return send_file(
        io.BytesIO(pdf_bytes),
        as_attachment=True,
        download_name=f"department_report_{dept_val}.pdf",
        mimetype="application/pdf",
    )


@bp.route("/export/top_books/pdf")
def export_top_books_pdf():
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = _current_role()
    is_hod = role == "hod"
    hod_dept = _hod_department() if is_hod else None

    # Get data for PDF (plain text, no HTML)
    df = top_books_df(arabic_only=False, english_only=True, dept_filter=hod_dept, for_pdf=True)
    
    # Clean any remaining HTML just in case
    df_clean = clean_dataframe_for_pdf(df)
    
    # Remove BiblioNumber column if it exists (internal use only)
    if "BiblioNumber" in df_clean.columns:
        df_clean = df_clean.drop(columns=["BiblioNumber"])
    
    # Rename columns for better display
    df_clean = df_clean.rename(columns={
        "Title": "Book Title",
        "Count": "Times Issued",
        "LastIssued": "Last Issued"
    })
    
    pdf_bytes = dataframe_to_pdf_bytes("Top 25 English Books (AY)", df_clean)
    return send_file(
        io.BytesIO(pdf_bytes),
        as_attachment=True,
        download_name="top_english_books.pdf",
        mimetype="application/pdf",
    )


@bp.route("/export/top_arabic/pdf")
def export_top_arabic_pdf():
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = _current_role()
    is_hod = role == "hod"
    hod_dept = _hod_department() if is_hod else None

    # Get data for PDF (plain text, no HTML)
    df = top_books_df(arabic_only=True, english_only=False, dept_filter=hod_dept, for_pdf=True)
    
    # Clean any remaining HTML just in case
    df_clean = clean_dataframe_for_pdf(df)
    
    # Remove BiblioNumber column if it exists (internal use only)
    if "BiblioNumber" in df_clean.columns:
        df_clean = df_clean.drop(columns=["BiblioNumber"])
    
    # Rename columns for better display
    df_clean = df_clean.rename(columns={
        "Title": "Book Title",
        "Count": "Times Issued",
        "LastIssued": "Last Issued"
    })
    
    pdf_bytes = dataframe_to_pdf_bytes("Top 25 Arabic Books (AY)", df_clean)
    return send_file(
        io.BytesIO(pdf_bytes),
        as_attachment=True,
        download_name="top_arabic_books.pdf",
        mimetype="application/pdf",
    )


# ---------------- EXPORT ROUTES (EXCEL) ----------------
@bp.route("/export/class/<class_val>/excel")
def export_class_excel(class_val):
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = _current_role()
    is_hod = role == "hod"
    hod_dept = _hod_department() if is_hod else None

    df, _ = class_report(class_val if class_val != "All" else None, dept_filter=hod_dept)
    
    # Clean HTML for Excel export too (Excel can handle HTML but cleaner is better)
    df_clean = clean_dataframe_for_pdf(df)
    
    xls_bytes = dataframe_to_excel_bytes(df_clean, sheet_name=f"Class_{class_val}")
    return send_file(
        io.BytesIO(xls_bytes),
        as_attachment=True,
        download_name=f"class_report_{class_val}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@bp.route("/export/department/<dept_val>/excel")
def export_department_excel(dept_val):
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    # HOD not allowed department-wise
    if _current_role() == "hod":
        return redirect(url_for("reports_bp.reports_page"))

    df, _ = department_report(dept_val if dept_val != "All" else None)
    
    # Clean HTML for Excel export
    df_clean = clean_dataframe_for_pdf(df)
    
    xls_bytes = dataframe_to_excel_bytes(df_clean, sheet_name=f"Dept_{dept_val}")
    return send_file(
        io.BytesIO(xls_bytes),
        as_attachment=True,
        download_name=f"department_report_{dept_val}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@bp.route("/export/top_books/excel")
def export_top_books_excel():
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = _current_role()
    is_hod = role == "hod"
    hod_dept = _hod_department() if is_hod else None

    # Get plain text data for Excel
    df = top_books_df(arabic_only=False, english_only=True, dept_filter=hod_dept, for_pdf=True)
    
    # Clean HTML for Excel export
    df_clean = clean_dataframe_for_pdf(df)
    
    # Remove BiblioNumber column if it exists
    if "BiblioNumber" in df_clean.columns:
        df_clean = df_clean.drop(columns=["BiblioNumber"])
    
    # Rename columns for better display
    df_clean = df_clean.rename(columns={
        "Title": "Book Title",
        "Count": "Times Issued",
        "LastIssued": "Last Issued"
    })
    
    xls_bytes = dataframe_to_excel_bytes(df_clean, sheet_name="TopBooks_English")
    return send_file(
        io.BytesIO(xls_bytes),
        as_attachment=True,
        download_name="top_english_books.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@bp.route("/export/top_arabic/excel")
def export_top_arabic_excel():
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = _current_role()
    is_hod = role == "hod"
    hod_dept = _hod_department() if is_hod else None

    # Get plain text data for Excel
    df = top_books_df(arabic_only=True, english_only=False, dept_filter=hod_dept, for_pdf=True)
    
    # Clean HTML for Excel export
    df_clean = clean_dataframe_for_pdf(df)
    
    # Remove BiblioNumber column if it exists
    if "BiblioNumber" in df_clean.columns:
        df_clean = df_clean.drop(columns=["BiblioNumber"])
    
    # Rename columns for better display
    df_clean = df_clean.rename(columns={
        "Title": "Book Title",
        "Count": "Times Issued",
        "LastIssued": "Last Issued"
    })
    
    xls_bytes = dataframe_to_excel_bytes(df_clean, sheet_name="TopBooks_Arabic")
    return send_file(
        io.BytesIO(xls_bytes),
        as_attachment=True,
        download_name="top_arabic_books.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )