# routes/reports.py - FULLY UPDATED WITH FIXED CURSOR DICTIONARY

from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, send_file, current_app
from db_koha import get_koha_conn
from services import koha_queries as KQ
from db_app import get_conn as get_app_conn
import pandas as pd
import io
import re
import csv
from datetime import date
import urllib.parse

from services.exports import dataframe_to_pdf_bytes, dataframe_to_excel_bytes
from routes.students import get_student_info
from typing import Any, List, Dict, Optional, Union

bp = Blueprint("reports_bp", __name__)

# Borrower attribute codes we accept as "darajah"
DARAJAH_CODES = ("STD", "CLASS", "DAR", "CLASS_STD")

# Borrower attribute codes we accept for TR number lookups
TR_ATTR_CODES = ("TRNO", "TRN", "TR_NUMBER", "TR")

# ---------------- OPAC URL HELPER ----------------
def get_opac_base_url():
    """Get OPAC base URL from Flask config with fallback."""
    return current_app.config.get("KOHA_OPAC_BASE_URL", "https://library-nairobi.jameasaifiyah.org")

def get_opac_book_url(biblionumber: int) -> str:
    """Generate OPAC book URL from biblionumber."""
    opac_base = get_opac_base_url()
    return f"{opac_base.rstrip('/')}/cgi-bin/koha/opac-detail.pl?biblionumber={biblionumber}"

# ---------------- HELPER FUNCTIONS FOR SQL ----------------
def _darajah_codes_sql() -> str:
    """Return SQL-safe string for DARAJAH_CODES"""
    return ", ".join([f"'{code}'" for code in DARAJAH_CODES])

def _tr_codes_sql() -> str:
    """Return SQL-safe string for TR_ATTR_CODES"""
    return ", ".join([f"'{code}'" for code in TR_ATTR_CODES])


# ---------------- ROLE HELPERS ----------------
def _current_role() -> str:
    """Normalize role from session."""
    return (session.get("role") or "").strip().lower()


def _hod_marhala() -> str | None:
    """Return the HOD's marhala label."""
    dep = session.get("department_name")
    if dep:
        return str(dep)
    return None


def _teacher_darajah() -> str | None:
    """Return the teacher's darajah (class) from session."""
    darajah = session.get("darajah_name") or session.get("class_name")
    if darajah:
        return str(darajah)
    return None


# ---------------- TEACHER MAPPING HELPER ----------------
def _get_teachers_for_darajah(darajah_name: str) -> list[dict]:
    """Get teachers mapped to a specific darajah from the app database."""
    conn = None
    cur = None
    try:
        conn = get_app_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT tm.teacher_name, tm.role, u.email
            FROM teacher_darajah_mapping tm
            LEFT JOIN users u ON tm.teacher_username = u.username
            WHERE tm.darajah_name = ?
            ORDER BY 
                CASE tm.role 
                    WHEN 'masool' THEN 1 
                    WHEN 'class_teacher' THEN 2 
                    ELSE 3 
                END,
                tm.teacher_name
        """, (darajah_name,))
        
        teachers = []
        for row in cur.fetchall():
            role_display = 'Masool' if row[1] == 'masool' else \
                          'Class Teacher' if row[1] == 'class_teacher' else 'Assistant'
            teachers.append({
                'name': row[0],
                'role': role_display,
                'email': row[2]
            })
        return teachers
    except Exception as e:
        current_app.logger.error(f"Error fetching teachers for darajah {darajah_name}: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()
    return []


# ---------------- INDIVIDUAL LOOKUP ----------------
def _resolve_borrower_by_identifier(identifier: str) -> int | None:
    """Resolve a patron by various identifiers."""
    if not identifier:
        return None

    conn = get_koha_conn()
    cur = conn.cursor()
    active_filter = " AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())"

    try:
        # 1) borrowernumber
        try:
            bn = int(identifier)
            cur.execute(f"SELECT b.borrowernumber FROM borrowers b WHERE b.borrowernumber=%s {active_filter}", (bn,))
            row = cur.fetchone()
            if row:
                return int(row[0])
        except ValueError:
            pass

        # 2) Cardnumber
        cur.execute(f"SELECT b.borrowernumber FROM borrowers b WHERE b.cardnumber=%s {active_filter}", (identifier,))
        row = cur.fetchone()
        if row:
            return int(row[0])

        # 3) ITS (userid)
        cur.execute(f"SELECT b.borrowernumber FROM borrowers b WHERE b.userid=%s {active_filter}", (identifier,))
        row = cur.fetchone()
        if row:
            return int(row[0])

        # 4) TR number via borrower_attributes
        sql = f"""
            SELECT b.borrowernumber
            FROM borrower_attributes ba
            JOIN borrowers b ON b.borrowernumber = ba.borrowernumber
            WHERE ba.code IN ({_tr_codes_sql()})
              AND ba.attribute=%s
              {active_filter}
            LIMIT 1
        """
        cur.execute(sql, (identifier,))
        row = cur.fetchone()
        if row:
            return int(row[0])
        return None
    finally:
        cur.close()
        conn.close()


# ---------------- DARAJAH ROWS FUNCTION - FIXED WITH DICTIONARY CURSOR ----------------
def _darajah_rows_for_value(darajah_std: str, marhala_filter: str | None = None) -> list[dict]:
    """Darajah-wise rows with AY metrics."""
    start, end = KQ.get_ay_bounds()
    conn = get_koha_conn()
    cur = conn.cursor(dictionary=True)

    try:
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
        marhala_clause = ""
        if marhala_filter:
            marhala_clause = "AND COALESCE(c.description, b.categorycode) = %s"

        ay_where = "AND DATE(`datetime`) BETWEEN %s AND %s" if start else ""
        fay_where = "AND DATE(`date`) BETWEEN %s AND %s" if start else ""
        collections_language_select = "cl.collections AS Collections, cl.language AS Language" if start else "NULL AS Collections, NULL AS Language"
        
        sql = f"""
            SELECT
              b.borrowernumber,
              b.cardnumber,
              COALESCE(tr.attribute, b.cardnumber)               AS TRNumber,
              b.surname,
              b.firstname,
              CONCAT(
                COALESCE(b.surname, ''),
                CASE WHEN b.surname IS NOT NULL AND b.firstname IS NOT NULL THEN ' ' ELSE '' END,
                COALESCE(b.firstname, '')
              )                                                   AS FullName,
              b.email                                            AS EduEmail,
              UPPER(COALESCE(b.sex,''))                          AS Sex,
              b.dateenrolled                                     AS Enrolled,
              b.dateexpiry                                       AS Expiry,
              COALESCE(a.currently_issued, 0)                        AS CurrentlyIssued,
              COALESCE(a.overdues, 0)                            AS Overdues,
              COALESCE(ay.total_issues_ay, 0)                    AS Issues_AY,
              COALESCE(fay.fees_paid_ay, 0)                     AS FeesPaid_AY,
              COALESCE(ob.outstanding, 0)                        AS OutstandingBalance,
              {collections_language_select}
            FROM borrowers b
            LEFT JOIN borrower_attributes std
                   ON std.borrowernumber = b.borrowernumber
                  AND std.code IN ({_darajah_codes_sql()})
            LEFT JOIN borrower_attributes tr
                   ON tr.borrowernumber = b.borrowernumber
                  AND tr.code IN ({_tr_codes_sql()})
            LEFT JOIN (
                SELECT borrowernumber,
                       COUNT(*) AS currently_issued,
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
                             THEN -amount ELSE 0 END) AS fees_paid_ay
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
            {collections_language_join if start else ""}
            WHERE (std.attribute = %s OR b.branchcode = %s)
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              {marhala_clause}
            ORDER BY Issues_AY DESC, FullName ASC;
        """

        # Parameters must match the SQL subquery order:
        # 1. ay subquery uses [start, end]
        # 2. fay subquery uses [start, end]
        # 3. collections_language_join uses [start, end]
        # 4. WHERE clause: darajah_std x2, optional marhala_filter
        params: List[Any] = []
        if start:
            params.extend([start, end])  # ay
        if start:
            params.extend([start, end])  # fay
        if start:
            params.extend([start, end])  # collections
        params.extend([darajah_std, darajah_std])  # WHERE
        if marhala_filter:
            params.append(marhala_filter)

        cur.execute(sql, params)
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    # Add darajah to each row
    for row in rows:
        row["Darajah"] = darajah_std

    return rows


def darajah_report(darajah_std: str | None, marhala_filter: str | None = None):
    """Darajah-wise report. Returns: (DataFrame, total_students)"""
    if darajah_std:
        rows = _darajah_rows_for_value(darajah_std, marhala_filter)
        total_students = len(rows) if rows else 0
    else:
        conn = get_koha_conn()
        cur = conn.cursor(dictionary=True)
        try:
            marhala_clause = ""
            params = []
            if marhala_filter:
                marhala_clause = "AND COALESCE(c.description, b.categorycode) = %s"
                params.append(marhala_filter)

            sql_list = f"""
                SELECT DISTINCT COALESCE(std.attribute, b.branchcode) AS cls
                FROM borrowers b
                LEFT JOIN borrower_attributes std
                  ON std.borrowernumber = b.borrowernumber
                 AND std.code IN ({_darajah_codes_sql()})
                LEFT JOIN categories c ON c.categorycode = b.categorycode
                WHERE COALESCE(std.attribute, b.branchcode) IS NOT NULL
                  AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                  {marhala_clause}
                ORDER BY cls;
            """
            cur.execute(sql_list, params)
            darajahs = [r['cls'] for r in cur.fetchall()]
        finally:
            cur.close()
            conn.close()

        rows = []
        for darajah in darajahs:
            rows += _darajah_rows_for_value(darajah, marhala_filter)
        total_students = len(rows)

    # Process rows for display with links
    processed_rows = []
    for row in rows:
        borrowernumber = row.get("borrowernumber")
        cardnumber = row.get("cardnumber")
        full_name = row.get("FullName", "")
        
        if not full_name or full_name.strip() == "" or full_name.lower() == "none":
            full_name = f"Student #{cardnumber}" if cardnumber else "Unknown Student"
        
        if borrowernumber:
            student_link = f'<a href="/students/{borrowernumber}" target="_blank">{full_name}</a>'
        elif cardnumber:
            student_link = f'<a href="/students/search?q={urllib.parse.quote(cardnumber)}" target="_blank">{full_name}</a>'
        else:
            student_link = full_name
        
        processed_rows.append({
            "TRNumber": row.get("TRNumber", ""),
            "FullName": student_link,
            "Sex": row.get("Sex", ""),
            "CurrentlyIssued": row.get("CurrentlyIssued", 0),
            "Overdues": row.get("Overdues", 0),
            "Issues_AY": row.get("Issues_AY", 0),
            "FeesPaid_AY": "{:.2f}".format(row.get("FeesPaid_AY", 0.0)),
            "Collections": row.get("Collections", ""),
            "Language": row.get("Language", ""),
            "Darajah": row.get("Darajah", "")
        })
    df = pd.DataFrame(processed_rows) if processed_rows else pd.DataFrame()
    if not df.empty and "Issues_AY" in df.columns:
        df["Issues_AY"] = pd.to_numeric(df["Issues_AY"], errors="coerce").fillna(0)
        df = df.sort_values(by="Issues_AY", ascending=False)
    
    return df, total_students


# ---------------- MARHALA ROWS FUNCTION - FIXED WITH DICTIONARY CURSOR ----------------
def _marhala_rows_for_value(marhala: str) -> list[dict]:
    """Marhala-wise rows with AY metrics."""
    start, end = KQ.get_ay_bounds()
    conn = get_koha_conn()
    cur = conn.cursor(dictionary=True)

    try:
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
        ay_where = "AND DATE(`datetime`) BETWEEN %s AND %s" if start else ""
        fay_where = "AND DATE(`date`) BETWEEN %s AND %s" if start else ""
        collections_language_select = "cl.collections AS Collections, cl.language AS Language" if start else "NULL AS Collections, NULL AS Language"
        
        sql = f"""
            SELECT
              b.borrowernumber,
              b.cardnumber,
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
              COALESCE(std.attribute, b.branchcode)              AS Darajah,
              COALESCE(a.currently_issued, 0)                        AS CurrentlyIssued,
              COALESCE(a.overdues, 0)                            AS Overdues,
              COALESCE(ay.total_issues_ay, 0)                    AS Issues_AY,
              COALESCE(fay.fees_paid_ay, 0)                     AS FeesPaid_AY,
              COALESCE(ob.outstanding, 0)                        AS OutstandingBalance,
              {collections_language_select}
            FROM borrowers b
            LEFT JOIN borrower_attributes std
                   ON std.borrowernumber = b.borrowernumber
                  AND std.code IN ({_darajah_codes_sql()})
            LEFT JOIN borrower_attributes tr
                   ON tr.borrowernumber = b.borrowernumber
                  AND tr.code IN ({_tr_codes_sql()})
            LEFT JOIN (
                SELECT borrowernumber,
                       COUNT(*) AS currently_issued,
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
                             THEN -amount ELSE 0 END) AS fees_paid_ay
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
            {collections_language_join if start else ""}
            WHERE (COALESCE(c.description, b.categorycode) = %s OR b.categorycode = %s)
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
            ORDER BY Issues_AY DESC, FullName ASC;
        """

        # Parameters must match the SQL subquery order:
        # 1. ay subquery uses [start, end]
        # 2. fay subquery uses [start, end]
        # 3. collections_language_join uses [start, end]
        # 4. WHERE clause: marhala x2
        params = []
        if start:
            params.extend([start, end])  # ay
        if start:
            params.extend([start, end])  # fay
        if start:
            params.extend([start, end])  # collections
        params.extend([marhala, marhala])  # WHERE clause

        cur.execute(sql, params)
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    return rows


def marhala_report(marhala_code: str | None):
    """Marhala-wise report. Returns: (DataFrame, total_students)"""
    if marhala_code:
        rows = _marhala_rows_for_value(marhala_code)
        total_students = len(rows) if rows else 0
    else:
        conn = get_koha_conn()
        cur = conn.cursor(dictionary=True)
        try:
            sql_list = """
                SELECT DISTINCT COALESCE(c.description, b.categorycode) AS marhala
                FROM borrowers b
                LEFT JOIN categories c ON c.categorycode = b.categorycode
                WHERE COALESCE(c.description, b.categorycode) IS NOT NULL
                  AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                ORDER BY marhala;
            """
            cur.execute(sql_list)
            marhalas = [r['marhala'] for r in cur.fetchall()]
        finally:
            cur.close()
            conn.close()

        rows = []
        for m in marhalas:
            rows += _marhala_rows_for_value(m)
        total_students = len(rows)

    # Process rows for display with links
    processed_rows = []
    for row in rows:
        borrowernumber = row.get("borrowernumber")
        cardnumber = row.get("cardnumber")
        full_name = row.get("FullName", "")
        
        if not full_name or full_name.strip() == "" or full_name.lower() == "none":
            full_name = f"Student #{cardnumber}" if cardnumber else "Unknown Student"
        
        if borrowernumber:
            student_link = f'<a href="/students/{borrowernumber}" target="_blank">{full_name}</a>'
        elif cardnumber:
            student_link = f'<a href="/students/search?q={urllib.parse.quote(cardnumber)}" target="_blank">{full_name}</a>'
        else:
            student_link = full_name
        
        processed_rows.append({
            "TRNumber": row.get("TRNumber", ""),
            "FullName": student_link,
            "Darajah": row.get("Darajah", ""),
            "Sex": row.get("Sex", ""),
            "CurrentlyIssued": row.get("CurrentlyIssued", 0),
            "Overdues": row.get("Overdues", 0),
            "Issues_AY": row.get("Issues_AY", 0),
            "FeesPaid_AY": "{:.2f}".format(row.get("FeesPaid_AY", 0.0)),
            "Collections": row.get("Collections", ""),
            "Language": row.get("Language", "")
        })
    
    df = pd.DataFrame(processed_rows) if processed_rows else pd.DataFrame()
    if not df.empty and "Issues_AY" in df.columns:
        df["Issues_AY"] = pd.to_numeric(df["Issues_AY"], errors="coerce").fillna(0)
        df = df.sort_values(by="Issues_AY", ascending=False)
        
    return df, total_students

# ---------------- HTML CLEANING UTILITY ----------------
def clean_html_for_pdf(html_text: str) -> str:
    """
    Clean HTML tags that ReportLab's Paragraph parser doesn't support.
    Removes span tags, style attributes, target="_blank", etc.
    """
    if not html_text or not isinstance(html_text, str):
        return str(html_text) if html_text is not None else ""
    
    # Remove all HTML tags including links, keep only text
    html_text = re.sub(r'<a[^>]*>(.*?)</a>', r'\1', html_text)
    
    # Remove span tags completely
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


# ---------------- TOP BOOKS FUNCTION WITH FIXED OPAC URL ----------------
def top_books_df(
    arabic_only: bool = False,
    english_only: bool = False,
    limit: int = 25,
    marhala_filter: str | None = None,
    darajah_filter: str | None = None,
    for_pdf: bool = False
):
    """
    Top titles for the CURRENT AY window, derived from issues + old_issues.
    """
    start, end = KQ.get_ay_bounds()
    if not start:
        return pd.DataFrame(columns=["Title", "Language", "Collections", "Count", "LastIssued"])

    conn = get_koha_conn()
    cur = conn.cursor(dictionary=True)

    try:
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

        marhala_clause = ""
        if marhala_filter:
            marhala_clause = "AND COALESCE(c.description, b.categorycode) = %s"

        darajah_clause = ""
        if darajah_filter:
            darajah_clause = "AND COALESCE(std.attribute, b.branchcode) = %s"

        sql = f"""
            SELECT
                bib.title AS Title,
                ExtractValue(
                    bmd.metadata,
                    '//datafield[@tag="041"]/subfield[@code="a"]'
                ) AS Language,
                GROUP_CONCAT(DISTINCT it.ccode ORDER BY it.ccode SEPARATOR ', ') AS Collections,
                COUNT(*) AS cnt,
                MAX(DATE(s.datetime)) AS last_issued,
                bib.biblionumber AS BiblioNumber
            FROM statistics s
            JOIN borrowers b
                 ON b.borrowernumber = s.borrowernumber
            LEFT JOIN borrower_attributes std
                 ON std.borrowernumber = b.borrowernumber
                AND std.code IN ({_darajah_codes_sql()})
            LEFT JOIN categories c
                 ON c.categorycode = b.categorycode
            JOIN items it
                 ON s.itemnumber = it.itemnumber
            JOIN biblio bib
                 ON it.biblionumber = bib.biblionumber
            LEFT JOIN biblio_metadata bmd
                 ON bib.biblionumber = bmd.biblionumber
            WHERE s.type = 'issue'
              AND DATE(s.datetime) BETWEEN %s AND %s
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              {marhala_clause}
              {darajah_clause}
              {lang_clause}
            GROUP BY bib.biblionumber, bib.title, Language
            ORDER BY cnt DESC
            LIMIT %s;
        """

        params = [start, end]
        if marhala_filter:
            params.append(marhala_filter)
        if darajah_filter:
            params.append(darajah_filter)
        if lang_clause:
            params.append(lang_param)
        params.append(int(limit))

        cur.execute(sql, params)
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    if not rows:
        return pd.DataFrame(columns=["Title", "Language", "Collections", "Count", "LastIssued"])

    processed_rows = []
    for row in rows:
        title = row.get("Title", "")
        language = row.get("Language", "")
        collections = row.get("Collections", "")
        count = row.get("cnt", 0)
        last_issued = row.get("last_issued")
        bib_number = row.get("BiblioNumber")
        
        if for_pdf:
            processed_rows.append({
                "Title": title,
                "Language": language,
                "Collections": collections,
                "Count": count,
                "LastIssued": last_issued.strftime('%Y-%m-%d') if last_issued else "",
                "BiblioNumber": bib_number
            })
        else:
            if bib_number:
                opac_url = get_opac_book_url(bib_number)
                title_with_link = f'<a href="{opac_url}" target="_blank" class="book-link">{title}</a>'
            else:
                title_with_link = title
            
            if language and language.lower().startswith('ar'):
                title_with_link = f'<span style="font-family: Al Kanz, sans-serif; text-align: center;">{title_with_link}</span>'
            else:
                title_with_link = f'<span style="text-align: center;">{title_with_link}</span>'
            
            processed_rows.append({
                "Title": title_with_link,
                "Language": language or "",
                "Collections": collections or "",
                "Count": count,
                "LastIssued": last_issued.strftime('%Y-%m-%d') if last_issued else ""
            })
    
    columns = ["Title", "Language", "Collections", "Count", "LastIssued", "BiblioNumber"] if for_pdf else ["Title", "Language", "Collections", "Count", "LastIssued"]
    df = pd.DataFrame(processed_rows, columns=columns)
    return df


# ---------------- TOP AUTHORS FUNCTION ----------------
def top_authors_df(
    limit: int = 25,
    marhala_filter: str | None = None,
    darajah_filter: str | None = None,
    for_pdf: bool = False
):
    """
    Top authors by number of books issued.
    """
    start, end = KQ.get_ay_bounds()
    if not start:
        return pd.DataFrame(columns=["Author", "Books Issued", "Top Titles"])

    conn = get_koha_conn()
    cur = conn.cursor(dictionary=True)

    try:
        marhala_clause = ""
        if marhala_filter:
            marhala_clause = "AND COALESCE(c.description, b.categorycode) = %s"

        darajah_clause = ""
        if darajah_filter:
            darajah_clause = "AND COALESCE(std.attribute, b.branchcode) = %s"

        sql = f"""
            SELECT
                ExtractValue(
                    bmd.metadata,
                    '//datafield[@tag="100"]/subfield[@code="a"]'
                ) AS Author,
                COUNT(DISTINCT bib.biblionumber) AS books_issued,
                GROUP_CONCAT(DISTINCT bib.title ORDER BY bib.title SEPARATOR '; ') AS top_titles,
                COUNT(*) AS total_issues
            FROM statistics s
            JOIN borrowers b
                 ON b.borrowernumber = s.borrowernumber
            LEFT JOIN borrower_attributes std
                 ON std.borrowernumber = b.borrowernumber
                AND std.code IN ({_darajah_codes_sql()})
            LEFT JOIN categories c
                 ON c.categorycode = b.categorycode
            JOIN items it
                 ON s.itemnumber = it.itemnumber
            JOIN biblio bib
                 ON it.biblionumber = bib.biblionumber
            LEFT JOIN biblio_metadata bmd
                 ON bib.biblionumber = bmd.biblionumber
            WHERE s.type = 'issue'
              AND DATE(s.datetime) BETWEEN %s AND %s
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND ExtractValue(
                    bmd.metadata,
                    '//datafield[@tag="100"]/subfield[@code="a"]'
                  ) IS NOT NULL
              AND ExtractValue(
                    bmd.metadata,
                    '//datafield[@tag="100"]/subfield[@code="a"]'
                  ) != ''
              {marhala_clause}
              {darajah_clause}
            GROUP BY Author
            ORDER BY books_issued DESC, total_issues DESC
            LIMIT %s;
        """

        params = [start, end]
        if marhala_filter:
            params.append(marhala_filter)
        if darajah_filter:
            params.append(darajah_filter)
        params.append(int(limit))

        cur.execute(sql, params)
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    if not rows:
        return pd.DataFrame(columns=["Author", "Books Issued", "Top Titles"])

    processed_rows = []
    for row in rows:
        author = row.get("Author") or "Unknown Author"
        books_issued = row.get("books_issued", 0)
        top_titles = row.get("top_titles") or ""
        
        if top_titles:
            titles_list = top_titles.split('; ')
            if len(titles_list) > 3:
                top_titles_display = '; '.join(titles_list[:3]) + f'... (+{len(titles_list)-3} more)'
            else:
                top_titles_display = '; '.join(titles_list)
        else:
            top_titles_display = ""
        
        processed_rows.append({
            "Author": author,
            "Books Issued": books_issued,
            "Top Titles": top_titles_display
        })
    
    return pd.DataFrame(processed_rows)

# ---------------- DATA PROCESSING FOR DISPLAY ----------------
def _process_display_df(df: pd.DataFrame, report_type: str) -> pd.DataFrame:
    """
    Process DataFrame for display by renaming columns and formatting.
    """
    if df.empty:
        return df
    
    df_display = df.copy()
    
    # Rename columns for better display
    column_rename_map = {
        "CurrentlyIssued": "Currently Issued",
        "Issues_AcademicYear": "Issues (Academic Year)",
        "FeesPaid_AcademicYear": "Fees Paid (Academic Year)",
        "TRNumber": "TR Number",
        "FullName": "Full Name",
        "TeacherName": "Teacher Name",
        "TeacherRole": "Teacher Role"
    }
    
    df_display = df_display.rename(columns=column_rename_map)
    
    # Format numeric columns (for display, not for linked columns)
    for col in df_display.columns:
        if col not in ["Full Name", "Title", "Author"]:  # Skip linked/text columns
            if df_display[col].dtype in ['float64', 'float32', 'int64', 'int32']:
                df_display[col] = df_display[col].apply(
                    lambda x: f"{x:,.2f}" if isinstance(x, (float, int)) and '.' in str(x) else f"{x:,}"
                )
    
    return df_display


def taqeem_report_df(darajah_name: str, academic_year: str | None = None):
    """
    Generate a Taqeem (Marks) report for a specific darajah.
    """
    if academic_year is None:
        from config import Config
        academic_year = Config.CURRENT_ACADEMIC_YEAR().replace('H', '').strip()
    
    from services.marks_service import calculate_total_taqeem
    
    # 1. Get all students in this darajah from our app database
    conn = get_app_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT student_username, student_name 
        FROM student_darajah_mapping 
        WHERE darajah_name = ? AND academic_year = ?
        ORDER BY student_name
    """, (darajah_name, academic_year))
    students = cur.fetchall()
    conn.close()
    
    if not students:
        return pd.DataFrame()
    
    report_data = []
    for itsid, name in students:
        marks = calculate_total_taqeem(itsid, academic_year)
        
        report_data.append({
            "ITSID": itsid,
            "Name": name,
            "Book Issues (60)": marks['book_issue']['total'],
            "Physical Issues": marks['book_issue']['physical_count'],
            "Digital Issues": marks['book_issue']['digital_count'],
            "Reviews (30)": marks['book_review']['marks'],
            "Programs (10)": marks['program_attendance'],
            "Total (100)": marks['total']
        })
    
    return pd.DataFrame(report_data)


# ---------------- ROUTES ----------------
@bp.route("/")
def reports_page():
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = _current_role()
    is_admin = role == "admin"
    is_hod = role == "hod"
    is_teacher = role == "teacher"
    hod_marhala = _hod_marhala()
    teacher_darajah = _teacher_darajah()

    conn = get_koha_conn()
    cur = conn.cursor()

    # Darajahs list
    if is_teacher and teacher_darajah:
        darajahs = [teacher_darajah]
    elif is_hod and hod_marhala:
        # Only darajahs within this HOD's marhala
        sql_darajahs = f"""
            SELECT DISTINCT COALESCE(std.attribute, b.branchcode) AS cls
            FROM borrowers b
            LEFT JOIN borrower_attributes std
              ON std.borrowernumber = b.borrowernumber
             AND std.code IN ({_darajah_codes_sql()})
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            WHERE COALESCE(std.attribute, b.branchcode) IS NOT NULL
              AND COALESCE(c.description, b.categorycode) = %s
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
            ORDER BY cls;
        """
        cur.execute(sql_darajahs, (hod_marhala,))
        darajahs = [r[0] for r in cur.fetchall()]
    else:
        # All darajahs for Admin (or other roles)
        sql_darajahs = f"""
            SELECT DISTINCT COALESCE(std.attribute, b.branchcode) AS cls
            FROM borrowers b
            LEFT JOIN borrower_attributes std
              ON std.borrowernumber = b.borrowernumber
             AND std.code IN ({_darajah_codes_sql()})
            WHERE COALESCE(std.attribute, b.branchcode) IS NOT NULL
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
            ORDER BY cls;
        """
        cur.execute(sql_darajahs)
        darajahs = [r[0] for r in cur.fetchall()]

    # Marhalas list – only needed for Admin (HOD never sees marhala-wise option)
    if is_admin:
        cur.execute(
            """
            SELECT DISTINCT COALESCE(c.description, b.categorycode) AS marhala
            FROM borrowers b
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            WHERE COALESCE(c.description, b.categorycode) IS NOT NULL
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
            ORDER BY marhala;
            """
        )
        marhalas = [r[0] for r in cur.fetchall()]
    else:
        marhalas = []

    cur.close()
    conn.close()

    return render_template(
        "reports.html",
        darajahs=darajahs,
        marhalas=marhalas,
        is_hod=is_hod,
        is_admin=is_admin,
        is_teacher=is_teacher,
        hod_marhala=hod_marhala,
    )


@bp.route("/api/generate_report", methods=["POST"])
def generate_report():
    if not session.get("logged_in"):
        return jsonify(success=False)

    role = _current_role()
    is_admin = role == "admin"
    is_hod = role == "hod"
    is_teacher = role == "teacher"
    hod_marhala = _hod_marhala()
    teacher_darajah = _teacher_darajah()

    report_type = request.form.get("report_type")

    # -------- DARAJAH-WISE --------
    if report_type == "darajah_wise":
        darajah_val = request.form.get("darajah_value")

        # Teacher: force to their own darajah only
        if is_teacher:
            if not teacher_darajah:
                return jsonify(success=False, html="<p>No darajah mapped to your account.</p>")
            if darajah_val and darajah_val != teacher_darajah:
                return jsonify(success=False, html="<p>You can only view your own darajah.</p>")
            darajah_val = teacher_darajah

        if is_hod and hod_marhala:
            # HOD: restrict to their marhala
            df, total_students = darajah_report(darajah_val if darajah_val else None, marhala_filter=hod_marhala)
        else:
            df, total_students = darajah_report(darajah_val if darajah_val else None, marhala_filter=None)

        html = df.to_html(
            classes="table table-sm table-striped",
            index=False,
            escape=False
        )
        
        response_data = {
            "success": not df.empty,
            "html": html,
            "total_students": total_students,
            "darajah_value": darajah_val or "All"
        }
        return jsonify(response_data)

    # -------- MARHALA-WISE --------
    elif report_type == "marhala_wise":
        # HOD/Teachers must NOT see this at all
        if is_hod or is_teacher:
            return jsonify(success=False, html="<p>You are not allowed to view marhala-wise reports.</p>")

        marhala_val = request.form.get("marhala_value")
        df, total_students = marhala_report(marhala_val if marhala_val else None)
        
        html = df.to_html(
            classes="table table-sm table-striped",
            index=False,
            escape=False
        )
        
        response_data = {
            "success": not df.empty,
            "html": html,
            "total_students": total_students,
            "marhala_value": marhala_val or "All"
        }
        return jsonify(response_data)

    # -------- INDIVIDUAL STUDENT --------
    elif report_type == "individual":
        identifier = (request.form.get("identifier") or "").strip()
        try:
            borrowernumber = _resolve_borrower_by_identifier(identifier)
            if not borrowernumber:
                return jsonify(success=False, html="<p>No active student found.</p>")

            # If HOD, ensure this student is in their marhala
            if is_hod and hod_marhala:
                conn = get_koha_conn()
                cur = conn.cursor()
                
                sql = f"""
                    SELECT COALESCE(c.description, b.categorycode) AS marhala
                          ,COALESCE(std.attribute, b.branchcode) AS darajah
                    FROM borrowers b
                    LEFT JOIN borrower_attributes std
                           ON std.borrowernumber = b.borrowernumber
                          AND std.code IN ({_darajah_codes_sql()})
                    LEFT JOIN categories c ON c.categorycode = b.categorycode
                    WHERE b.borrowernumber = %s;
                """
                
                cur.execute(sql, (borrowernumber,))
                row = cur.fetchone()
                cur.close()
                conn.close()
                
                if not row or (row[0] != hod_marhala):
                    return jsonify(success=False, html="<p>Student not in your marhala.</p>")

            # If Teacher, ensure student is in their darajah
            if is_teacher and teacher_darajah:
                conn = get_koha_conn()
                cur = conn.cursor()
                
                sql = f"""
                    SELECT COALESCE(std.attribute, b.branchcode) AS darajah
                    FROM borrowers b
                    LEFT JOIN borrower_attributes std
                           ON std.borrowernumber = b.borrowernumber
                          AND std.code IN ({_darajah_codes_sql()})
                    WHERE b.borrowernumber = %s;
                """
                
                cur.execute(sql, (borrowernumber,))
                row = cur.fetchone()
                cur.close()
                conn.close()
                
                if not row or row[0] != teacher_darajah:
                    return jsonify(success=False, html="<p>Student not in your darajah.</p>")

            info = get_student_info(str(borrowernumber))
            if not info:
                return jsonify(success=False, html="<p>No student found.</p>")

            # Get darajah information for teacher mapping
            darajah = info.get('class', '')
            teachers = _get_teachers_for_darajah(darajah) if darajah else []
            
            # Get OPAC base URL for template
            opac_base = get_opac_base_url()
            
            # Render with OPAC URL and teacher information
            rendered_html = render_template(
                "student.html", 
                found=True, 
                info=info, 
                hide_nav=True,
                opac_base_url=opac_base,
                teachers=teachers
            )
            return jsonify(success=True, html=rendered_html)
        except Exception as e:
            current_app.logger.error(f"Error in individual report: {e}")
            return jsonify(success=False, html="<p>Unexpected error while looking up the student.</p>")

    # -------- TOP 25 ENGLISH (MARC 041 eng%) --------
    elif report_type == "top_books":
        if is_teacher:
            if not teacher_darajah:
                return jsonify(success=False, html="<p>No darajah mapped to your account.</p>")
            df = top_books_df(arabic_only=False, english_only=True, marhala_filter=None, darajah_filter=teacher_darajah, for_pdf=False)
        elif is_hod and hod_marhala:
            df = top_books_df(arabic_only=False, english_only=True, marhala_filter=hod_marhala, darajah_filter=None, for_pdf=False)
        else:
            df = top_books_df(arabic_only=False, english_only=True, marhala_filter=None, darajah_filter=None, for_pdf=False)

        html = df.to_html(
            classes="table table-sm table-striped",
            index=False,
            escape=False
        )
        return jsonify(success=not df.empty, html=html)

    # -------- TOP 25 ARABIC (MARC 041 ar%) --------
    elif report_type == "top_arabic":
        if is_teacher:
            if not teacher_darajah:
                return jsonify(success=False, html="<p>No darajah mapped to your account.</p>")
            df = top_books_df(arabic_only=True, english_only=False, marhala_filter=None, darajah_filter=teacher_darajah, for_pdf=False)
        elif is_hod and hod_marhala:
            df = top_books_df(arabic_only=True, english_only=False, marhala_filter=hod_marhala, darajah_filter=None, for_pdf=False)
        else:
            df = top_books_df(arabic_only=True, english_only=False, marhala_filter=None, darajah_filter=None, for_pdf=False)

        html = df.to_html(
            classes="table table-sm table-striped",
            index=False,
            escape=False
        )
        return jsonify(success=not df.empty, html=html)

    # -------- TOP 25 AUTHORS --------
    elif report_type == "top_authors":
        if is_teacher:
            if not teacher_darajah:
                return jsonify(success=False, html="<p>No darajah mapped to your account.</p>")
            df = top_authors_df(marhala_filter=None, darajah_filter=teacher_darajah, for_pdf=False)
        elif is_hod and hod_marhala:
            df = top_authors_df(marhala_filter=hod_marhala, darajah_filter=None, for_pdf=False)
        else:
            df = top_authors_df(marhala_filter=None, darajah_filter=None, for_pdf=False)

        html = df.to_html(
            classes="table table-sm table-striped",
            index=False,
            escape=False
        )
        return jsonify(success=not df.empty, html=html)

    return jsonify(success=False, html="<p>Unknown report type.</p>")



# ---------------- TREND & YEAR API ----------------
@bp.route("/api/trend_data")
def api_trend_data():
    """Return monthly trend data for the given period as JSON."""
    if not session.get("logged_in"):
        return jsonify(success=False)

    role = _current_role()
    is_hod = role == "hod"
    is_teacher = role == "teacher"
    hod_marhala = _hod_marhala() if is_hod else None
    teacher_darajah = _teacher_darajah() if is_teacher else None

    hijri_year = request.args.get("year", type=int)

    try:
        if hijri_year:
            start_str, end_str = KQ.get_ay_bounds_for_hijri_year(hijri_year)
            start = start_str
            end = end_str
            if not start:
                return jsonify(success=False, labels=[], values=[])
            effective_end = min(end, date.today())
        else:
            start, effective_end_raw = KQ.get_ay_bounds()
            if not start:
                return jsonify(success=False, labels=[], values=[])
            effective_end = min(effective_end_raw, date.today())

        marhala = hod_marhala
        darajah = teacher_darajah if is_teacher else None

        labels, values = KQ.get_monthly_trend_for_period(start, effective_end, marhala, darajah)
        return jsonify(success=True, labels=labels, values=values)
    except Exception as e:
        current_app.logger.error(f"Error in api_trend_data: {e}")
        return jsonify(success=False, labels=[], values=[])


@bp.route("/api/available_years")
def api_available_years():
    """Return list of available Hijri academic years with data."""
    if not session.get("logged_in"):
        return jsonify(success=False)
    role = _current_role()
    if role not in ("admin", "hod"):
        return jsonify(success=False, years=[])
    try:
        years = KQ.get_available_academic_years()
        return jsonify(success=True, years=years)
    except Exception as e:
        current_app.logger.error(f"Error in api_available_years: {e}")
        return jsonify(success=False, years=[])


@bp.route("/api/report_for_year", methods=["POST"])
def report_for_year():
    """Generate a darajah or marhala report for a specific Hijri academic year."""
    if not session.get("logged_in"):
        return jsonify(success=False)
    role = _current_role()
    if role != "admin":
        return jsonify(success=False, html="<p>Admin access required.</p>")

    hijri_year = request.form.get("hijri_year", type=int)
    report_type = request.form.get("report_type", "darajah_wise")
    darajah_val = request.form.get("darajah_value")
    marhala_val = request.form.get("marhala_value")

    if not hijri_year:
        return jsonify(success=False, html="<p>No year specified.</p>")

    try:
        start, end = KQ.get_ay_bounds_for_hijri_year(hijri_year)
        if not start:
            return jsonify(success=False, html="<p>Could not compute year bounds.</p>")

        # Temporarily patch get_ay_bounds to return the requested year
        original_bounds = KQ.get_ay_bounds
        KQ.get_ay_bounds = lambda: (start, min(end, date.today()))
        try:
            if report_type == "darajah_wise":
                df, total_students = darajah_report(darajah_val or None)
            else:
                df, total_students = marhala_report(marhala_val or None)
        finally:
            KQ.get_ay_bounds = original_bounds

        html = df.to_html(
            classes="table table-sm table-striped",
            index=False, escape=False
        )
        return jsonify(success=not df.empty, html=html, total_students=total_students)
    except Exception as e:
        current_app.logger.error(f"Error in report_for_year: {e}")
        import traceback; traceback.print_exc()
        return jsonify(success=False, html=f"<p>Error generating report: {e}</p>")


# ---------------- EXPORT ROUTES (PDF) ----------------
@bp.route("/export/darajah/<darajah_val>/pdf")
def export_darajah_pdf(darajah_val):
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = _current_role()
    is_hod = role == "hod"
    is_teacher = role == "teacher"
    hod_marhala = _hod_marhala() if is_hod else None
    teacher_darajah = _teacher_darajah() if is_teacher else None

    if is_teacher:
        if not teacher_darajah or (darajah_val != teacher_darajah and darajah_val != "All"):
            return redirect(url_for("reports_bp.reports_page"))
        darajah_val = teacher_darajah

    df, _ = darajah_report(darajah_val if darajah_val != "All" else None, marhala_filter=hod_marhala)
    
    # Clean HTML from DataFrame before PDF generation
    df_clean = clean_dataframe_for_pdf(df)
    
    # Rename columns for better display in PDF
    df_clean = df_clean.rename(columns={
        "CurrentlyIssued": "Currently Issued",
        "Issues_AY": "Issues (Academic Year)",
        "FeesPaid_AY": "Fees Paid (Academic Year)",
        "TRNumber": "TR Number",
        "FullName": "Full Name"
    })
    
    # Remove teacher columns
    cols_to_drop = ["TeacherName", "TeacherRole", "Teacher Email", "Teacher Name", "Teacher Role"]
    df_clean = df_clean.drop(columns=[c for c in cols_to_drop if c in df_clean.columns], errors='ignore')
    
    # Explicitly set portrait orientation
    pdf_bytes = dataframe_to_pdf_bytes(
        f"Darajah Report - {darajah_val}", 
        df_clean,
        orientation='portrait'
    )
    
    return send_file(
        io.BytesIO(pdf_bytes),
        as_attachment=True,
        download_name=f"darajah_report_{darajah_val}.pdf",
        mimetype="application/pdf",
    )


@bp.route("/export/taqeem/<darajah_val>/pdf")
def export_taqeem_pdf(darajah_val):
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    df = taqeem_report_df(darajah_val)
    if df.empty:
        return redirect(url_for("reports_bp.reports_page"))

    pdf_bytes = dataframe_to_pdf_bytes(
        f"Taqeem Marks Report - {darajah_val}", 
        df,
        orientation='landscape'
    )
    
    return send_file(
        io.BytesIO(pdf_bytes),
        as_attachment=True,
        download_name=f"taqeem_report_{darajah_val}.pdf",
        mimetype="application/pdf",
    )



@bp.route("/export/marhala/<marhala_val>/pdf")
def export_marhala_pdf(marhala_val):
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    # HOD not allowed marhala-wise
    if _current_role() == "hod":
        return redirect(url_for("reports_bp.reports_page"))

    df, _ = marhala_report(marhala_val if marhala_val != "All" else None)
    
    # Clean HTML from DataFrame before PDF generation
    df_clean = clean_dataframe_for_pdf(df)
    
    # Rename columns for better display in PDF
    df_clean = df_clean.rename(columns={
        "CurrentlyIssued": "Currently Issued",
        "Issues_AY": "Issues (Academic Year)",
        "FeesPaid_AY": "Fees Paid (Academic Year)",
        "TRNumber": "TR Number",
        "FullName": "Full Name"
    })
    
    # Remove teacher columns
    cols_to_drop = ["TeacherName", "TeacherRole", "Teacher Email", "Teacher Name", "Teacher Role"]
    df_clean = df_clean.drop(columns=[c for c in cols_to_drop if c in df_clean.columns], errors='ignore')
    
    # Explicitly set portrait orientation
    pdf_bytes = dataframe_to_pdf_bytes(
        f"Marhala Report - {marhala_val}", 
        df_clean,
        orientation='portrait'
    )
    
    return send_file(
        io.BytesIO(pdf_bytes),
        as_attachment=True,
        download_name=f"marhala_report_{marhala_val}.pdf",
        mimetype="application/pdf",
    )

@bp.route("/export/top_books/pdf")
def export_top_books_pdf():
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = _current_role()
    is_hod = role == "hod"
    is_teacher = role == "teacher"
    hod_marhala = _hod_marhala() if is_hod else None
    teacher_darajah = _teacher_darajah() if is_teacher else None

    if is_teacher and not teacher_darajah:
        return redirect(url_for("reports_bp.reports_page"))

    # Get data for PDF (plain text, no HTML)
    df = top_books_df(
        arabic_only=False,
        english_only=True,
        marhala_filter=hod_marhala if not is_teacher else None,
        darajah_filter=teacher_darajah if is_teacher else None,
        for_pdf=True,
    )
    
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
    
    pdf_bytes = dataframe_to_pdf_bytes(
        "Top 25 English Books (Academic Year)", 
        df_clean,
        orientation='portrait'
    )
    
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
    is_teacher = role == "teacher"
    hod_marhala = _hod_marhala() if is_hod else None
    teacher_darajah = _teacher_darajah() if is_teacher else None

    if is_teacher and not teacher_darajah:
        return redirect(url_for("reports_bp.reports_page"))

    # Get data for PDF (plain text, no HTML)
    df = top_books_df(
        arabic_only=True,
        english_only=False,
        marhala_filter=hod_marhala if not is_teacher else None,
        darajah_filter=teacher_darajah if is_teacher else None,
        for_pdf=True,
    )
    
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
    
    pdf_bytes = dataframe_to_pdf_bytes(
        "Top 25 Arabic Books (Academic Year)", 
        df_clean,
        orientation='portrait'
    )
    
    return send_file(
        io.BytesIO(pdf_bytes),
        as_attachment=True,
        download_name="top_arabic_books.pdf",
        mimetype="application/pdf",
    )


@bp.route("/export/top_authors/pdf")
def export_top_authors_pdf():
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = _current_role()
    is_hod = role == "hod"
    is_teacher = role == "teacher"
    hod_marhala = _hod_marhala() if is_hod else None
    teacher_darajah = _teacher_darajah() if is_teacher else None

    if is_teacher and not teacher_darajah:
        return redirect(url_for("reports_bp.reports_page"))

    # Get data for PDF (plain text, no HTML)
    df = top_authors_df(
        marhala_filter=hod_marhala if not is_teacher else None,
        darajah_filter=teacher_darajah if is_teacher else None,
        for_pdf=True,
    )
    
    # Clean any remaining HTML just in case
    df_clean = clean_dataframe_for_pdf(df)
    
    pdf_bytes = dataframe_to_pdf_bytes(
        "Top 25 Authors (Academic Year)", 
        df_clean,
        orientation='portrait'
    )
    
    return send_file(
        io.BytesIO(pdf_bytes),
        as_attachment=True,
        download_name="top_authors.pdf",
        mimetype="application/pdf",
    )


# ---------------- EXPORT ROUTES (EXCEL) ----------------
@bp.route("/export/darajah/<darajah_val>/excel")
def export_darajah_excel(darajah_val):
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = _current_role()
    is_hod = role == "hod"
    is_teacher = role == "teacher"
    hod_marhala = _hod_marhala() if is_hod else None
    teacher_darajah = _teacher_darajah() if is_teacher else None

    if is_teacher:
        if not teacher_darajah or (darajah_val != teacher_darajah and darajah_val != "All"):
            return redirect(url_for("reports_bp.reports_page"))
        darajah_val = teacher_darajah

    df, _ = darajah_report(darajah_val if darajah_val != "All" else None, marhala_filter=hod_marhala)
    
    # Clean HTML for Excel export
    df_clean = clean_dataframe_for_pdf(df)
    
    # Rename columns for better display
    column_rename_map = {
        "CurrentlyIssued": "Currently Issued",
        "Issues_AcademicYear": "Issues (Academic Year)",
        "FeesPaid_AcademicYear": "Fees Paid (Academic Year)",
        "TRNumber": "TR Number",
        "FullName": "Full Name",
        "TeacherName": "Teacher Name",
        "TeacherRole": "Teacher Role"
    }
    df_clean = df_clean.rename(columns=column_rename_map)
    
    xls_bytes = dataframe_to_excel_bytes(df_clean, sheet_name=f"Darajah_{darajah_val}")
    return send_file(
        io.BytesIO(xls_bytes),
        as_attachment=True,
        download_name=f"darajah_report_{darajah_val}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@bp.route("/export/taqeem/<darajah_val>/excel")
def export_taqeem_excel(darajah_val):
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    df = taqeem_report_df(darajah_val)
    if df.empty:
        return redirect(url_for("reports_bp.reports_page"))

    xls_bytes = dataframe_to_excel_bytes(df, sheet_name=f"Taqeem_{darajah_val}")
    return send_file(
        io.BytesIO(xls_bytes),
        as_attachment=True,
        download_name=f"taqeem_report_{darajah_val}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )



@bp.route("/export/marhala/<marhala_val>/excel")
def export_marhala_excel(marhala_val):
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    # HOD not allowed marhala-wise
    if _current_role() == "hod":
        return redirect(url_for("reports_bp.reports_page"))

    df, _ = marhala_report(marhala_val if marhala_val != "All" else None)
    
    # Clean HTML for Excel export
    df_clean = clean_dataframe_for_pdf(df)
    
    # Rename columns for better display
    column_rename_map = {
        "CurrentlyIssued": "Currently Issued",
        "Issues_AcademicYear": "Issues (Academic Year)",
        "FeesPaid_AcademicYear": "Fees Paid (Academic Year)",
        "TRNumber": "TR Number",
        "FullName": "Full Name",
        "TeacherName": "Teacher Name",
        "TeacherRole": "Teacher Role"
    }
    df_clean = df_clean.rename(columns=column_rename_map)
    
    xls_bytes = dataframe_to_excel_bytes(df_clean, sheet_name=f"Marhala_{marhala_val}")
    return send_file(
        io.BytesIO(xls_bytes),
        as_attachment=True,
        download_name=f"marhala_report_{marhala_val}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@bp.route("/export/top_books/excel")
def export_top_books_excel():
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = _current_role()
    is_hod = role == "hod"
    is_teacher = role == "teacher"
    hod_marhala = _hod_marhala() if is_hod else None
    teacher_darajah = _teacher_darajah() if is_teacher else None

    if is_teacher and not teacher_darajah:
        return redirect(url_for("reports_bp.reports_page"))

    # Get plain text data for Excel
    df = top_books_df(
        arabic_only=False,
        english_only=True,
        marhala_filter=hod_marhala if not is_teacher else None,
        darajah_filter=teacher_darajah if is_teacher else None,
        for_pdf=True,
    )
    
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
    is_teacher = role == "teacher"
    hod_marhala = _hod_marhala() if is_hod else None
    teacher_darajah = _teacher_darajah() if is_teacher else None

    if is_teacher and not teacher_darajah:
        return redirect(url_for("reports_bp.reports_page"))

    # Get plain text data for Excel
    df = top_books_df(
        arabic_only=True,
        english_only=False,
        marhala_filter=hod_marhala if not is_teacher else None,
        darajah_filter=teacher_darajah if is_teacher else None,
        for_pdf=True,
    )
    
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


@bp.route("/export/top_authors/excel")
def export_top_authors_excel():
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = _current_role()
    is_hod = role == "hod"
    is_teacher = role == "teacher"
    hod_marhala = _hod_marhala() if is_hod else None
    teacher_darajah = _teacher_darajah() if is_teacher else None

    if is_teacher and not teacher_darajah:
        return redirect(url_for("reports_bp.reports_page"))

    # Get plain text data for Excel
    df = top_authors_df(
        marhala_filter=hod_marhala if not is_teacher else None,
        darajah_filter=teacher_darajah if is_teacher else None,
        for_pdf=True,
    )
    
    # Clean HTML for Excel export
    df_clean = clean_dataframe_for_pdf(df)
    
    xls_bytes = dataframe_to_excel_bytes(df_clean, sheet_name="TopAuthors")
    return send_file(
        io.BytesIO(xls_bytes),
        as_attachment=True,
        download_name="top_authors.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


# ---------------- EXPORT ROUTES (CSV) ----------------
@bp.route("/export/darajah/<darajah_val>/csv")
def export_darajah_csv(darajah_val):
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = _current_role()
    is_hod = role == "hod"
    is_teacher = role == "teacher"
    hod_marhala = _hod_marhala() if is_hod else None
    teacher_darajah = _teacher_darajah() if is_teacher else None

    if is_teacher:
        if not teacher_darajah or (darajah_val != teacher_darajah and darajah_val != "All"):
            return redirect(url_for("reports_bp.reports_page"))
        darajah_val = teacher_darajah

    df, _ = darajah_report(darajah_val if darajah_val != "All" else None, marhala_filter=hod_marhala)
    
    # Clean HTML for CSV
    df_clean = clean_dataframe_for_pdf(df)
    
    # Create CSV in memory
    output = io.StringIO()
    df_clean.to_csv(output, index=False, encoding='utf-8')
    output.seek(0)
    
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8')),
        as_attachment=True,
        download_name=f"darajah_report_{darajah_val}.csv",
        mimetype="text/csv",
    )


@bp.route("/export/taqeem/<darajah_val>/csv")
def export_taqeem_csv(darajah_val):
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    df = taqeem_report_df(darajah_val)
    if df.empty:
        return redirect(url_for("reports_bp.reports_page"))

    output = io.StringIO()
    df.to_csv(output, index=False, encoding='utf-8')
    output.seek(0)
    
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8')),
        as_attachment=True,
        download_name=f"taqeem_report_{darajah_val}.csv",
        mimetype="text/csv",
    )



@bp.route("/export/marhala/<marhala_val>/csv")
def export_marhala_csv(marhala_val):
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    # HOD not allowed marhala-wise
    if _current_role() == "hod":
        return redirect(url_for("reports_bp.reports_page"))

    df, _ = marhala_report(marhala_val if marhala_val != "All" else None)
    
    # Clean HTML for CSV
    df_clean = clean_dataframe_for_pdf(df)
    
    # Create CSV in memory
    output = io.StringIO()
    df_clean.to_csv(output, index=False, encoding='utf-8')
    output.seek(0)
    
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8')),
        as_attachment=True,
        download_name=f"marhala_report_{marhala_val}.csv",
        mimetype="text/csv",
    )


@bp.route("/export/top_books/csv")
def export_top_books_csv():
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = _current_role()
    is_hod = role == "hod"
    is_teacher = role == "teacher"
    hod_marhala = _hod_marhala() if is_hod else None
    teacher_darajah = _teacher_darajah() if is_teacher else None

    if is_teacher and not teacher_darajah:
        return redirect(url_for("reports_bp.reports_page"))

    # Get plain text data for CSV
    df = top_books_df(
        arabic_only=False,
        english_only=True,
        marhala_filter=hod_marhala if not is_teacher else None,
        darajah_filter=teacher_darajah if is_teacher else None,
        for_pdf=True,
    )
    
    # Remove BiblioNumber column if it exists
    if "BiblioNumber" in df.columns:
        df = df.drop(columns=["BiblioNumber"])
    
    # Rename columns for better display
    df = df.rename(columns={
        "Title": "Book Title",
        "Count": "Times Issued",
        "LastIssued": "Last Issued"
    })
    
    # Create CSV in memory
    output = io.StringIO()
    df.to_csv(output, index=False, encoding='utf-8')
    output.seek(0)
    
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8')),
        as_attachment=True,
        download_name="top_english_books.csv",
        mimetype="text/csv",
    )


@bp.route("/export/top_arabic/csv")
def export_top_arabic_csv():
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = _current_role()
    is_hod = role == "hod"
    is_teacher = role == "teacher"
    hod_marhala = _hod_marhala() if is_hod else None
    teacher_darajah = _teacher_darajah() if is_teacher else None

    if is_teacher and not teacher_darajah:
        return redirect(url_for("reports_bp.reports_page"))

    # Get plain text data for CSV
    df = top_books_df(
        arabic_only=True,
        english_only=False,
        marhala_filter=hod_marhala if not is_teacher else None,
        darajah_filter=teacher_darajah if is_teacher else None,
        for_pdf=True,
    )
    
    # Remove BiblioNumber column if it exists
    if "BiblioNumber" in df.columns:
        df = df.drop(columns=["BiblioNumber"])
    
    # Rename columns for better display
    df = df.rename(columns={
        "Title": "Book Title",
        "Count": "Times Issued",
        "LastIssued": "Last Issued"
    })
    
    # Create CSV in memory
    output = io.StringIO()
    df.to_csv(output, index=False, encoding='utf-8')
    output.seek(0)
    
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8')),
        as_attachment=True,
        download_name="top_arabic_books.csv",
        mimetype="text/csv",
    )


@bp.route("/export/top_authors/csv")
def export_top_authors_csv():
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = _current_role()
    is_hod = role == "hod"
    is_teacher = role == "teacher"
    hod_marhala = _hod_marhala() if is_hod else None
    teacher_darajah = _teacher_darajah() if is_teacher else None

    if is_teacher and not teacher_darajah:
        return redirect(url_for("reports_bp.reports_page"))

    # Get plain text data for CSV
    df = top_authors_df(
        marhala_filter=hod_marhala if not is_teacher else None,
        darajah_filter=teacher_darajah if is_teacher else None,
        for_pdf=True,
    )
    
    # Create CSV in memory
    output = io.StringIO()
    df.to_csv(output, index=False, encoding='utf-8')
    output.seek(0)
    
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8')),
        as_attachment=True,
        download_name="top_authors.csv",
        mimetype="text/csv",
    )

# ---------------- LANDSCAPE PDF EXPORT ROUTES ----------------
@bp.route("/export/darajah/<darajah_val>/pdf-landscape")
def export_darajah_pdf_landscape(darajah_val):
    """Export darajah report as PDF (landscape orientation)."""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = _current_role()
    is_hod = role == "hod"
    is_teacher = role == "teacher"
    hod_marhala = _hod_marhala() if is_hod else None
    teacher_darajah = _teacher_darajah() if is_teacher else None

    if is_teacher:
        if not teacher_darajah or (darajah_val != teacher_darajah and darajah_val != "All"):
            return redirect(url_for("reports_bp.reports_page"))
        darajah_val = teacher_darajah

    df, _ = darajah_report(darajah_val if darajah_val != "All" else None, marhala_filter=hod_marhala)
    
    # Clean HTML from DataFrame before PDF generation
    df_clean = clean_dataframe_for_pdf(df)
    
    # Rename columns for better display in PDF
    df_clean = df_clean.rename(columns={
        "CurrentlyIssued": "Currently Issued",
        "Issues_AY": "Issues (Academic Year)",
        "FeesPaid_AY": "Fees Paid (Academic Year)",
        "TRNumber": "TR Number",
        "FullName": "Full Name"
    })
    
    # Remove teacher columns
    cols_to_drop = ["TeacherName", "TeacherRole", "Teacher Email", "Teacher Name", "Teacher Role"]
    df_clean = df_clean.drop(columns=[c for c in cols_to_drop if c in df_clean.columns], errors='ignore')
    
    # Use landscape orientation
    pdf_bytes = dataframe_to_pdf_bytes(
        f"Darajah Report - {darajah_val} (Landscape)", 
        df_clean,
        orientation='landscape'
    )
    
    return send_file(
        io.BytesIO(pdf_bytes),
        as_attachment=True,
        download_name=f"darajah_report_{darajah_val}_landscape.pdf",
        mimetype="application/pdf",
    )


@bp.route("/export/marhala/<marhala_val>/pdf-landscape")
def export_marhala_pdf_landscape(marhala_val):
    """Export marhala report as PDF (landscape orientation)."""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    # HOD not allowed marhala-wise
    if _current_role() == "hod":
        return redirect(url_for("reports_bp.reports_page"))

    df, _ = marhala_report(marhala_val if marhala_val != "All" else None)
    
    # Clean HTML from DataFrame before PDF generation
    df_clean = clean_dataframe_for_pdf(df)
    
    # Rename columns for better display in PDF
    df_clean = df_clean.rename(columns={
        "CurrentlyIssued": "Currently Issued",
        "Issues_AY": "Issues (Academic Year)",
        "FeesPaid_AY": "Fees Paid (Academic Year)",
        "TRNumber": "TR Number",
        "FullName": "Full Name"
    })
    
    # Remove teacher columns
    cols_to_drop = ["TeacherName", "TeacherRole", "Teacher Email", "Teacher Name", "Teacher Role"]
    df_clean = df_clean.drop(columns=[c for c in cols_to_drop if c in df_clean.columns], errors='ignore')
    
    # Use landscape orientation
    pdf_bytes = dataframe_to_pdf_bytes(
        f"Marhala Report - {marhala_val} (Landscape)", 
        df_clean,
        orientation='landscape'
    )
    
    return send_file(
        io.BytesIO(pdf_bytes),
        as_attachment=True,
        download_name=f"marhala_report_{marhala_val}_landscape.pdf",
        mimetype="application/pdf",
    )