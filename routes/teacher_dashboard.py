# routes/teacher_dashboard.py - COMPLETE UPDATED VERSION WITH FIXED STUDENT LISTING

from flask import (
    Blueprint, render_template, session, redirect, url_for,
    flash, current_app, request, send_file, jsonify
)
from datetime import date, datetime
from io import BytesIO
import urllib.parse
import re
import traceback
import html

from reportlab.lib.pagesizes import A4, landscape, portrait
from reportlab.lib.units import cm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Table, TableStyle,
    Spacer, Image, PageBreak
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors

from services.exports import (
    _ensure_font_registered,
    _shape_if_rtl,
)
from routes.reports import darajah_report
from routes.students import get_student_info
from db_koha import get_koha_conn
from services import koha_queries as KQ

import pandas as pd
import os


bp = Blueprint("teacher_dashboard_bp", __name__)

OPAC_BASE = "https://library-nairobi.jameasaifiyah.org/"


# --------------------------------------------------
# HTML SAFETY HELPERS
# --------------------------------------------------
def _escape_html(text):
    """Escape HTML special characters in text."""
    if text is None:
        return ""
    return html.escape(str(text))

def _clean_student_name(name):
    """Clean and escape student name, removing any HTML tags."""
    if not name:
        return "Name not available"
    
    name_str = str(name)
    name_str = re.sub(r'<[^>]+>', '', name_str)
    name_str = html.unescape(name_str)
    name_str = html.escape(name_str)
    name_str = name_str.strip()
    
    if not name_str or name_str.lower() in ['none', 'null', 'nan', '']:
        return "Name not available"
    
    return name_str

def _format_student_display(tr_number, full_name):
    """Format student display with TR number and cleaned name."""
    clean_name = _clean_student_name(full_name)
    clean_tr = _escape_html(str(tr_number).strip()) if tr_number else ""
    
    if clean_tr and clean_name != "Name not available":
        return f"TR {clean_tr} - {clean_name}"
    elif clean_tr:
        return f"TR {clean_tr}"
    elif clean_name != "Name not available":
        return clean_name
    else:
        return "Student"

def _clean_title(title):
    """Clean and escape book titles."""
    if not title:
        return ""
    
    title_str = str(title)
    title_str = re.sub(r'<[^>]+>', '', title_str)
    title_str = html.unescape(title_str)
    title_str = html.escape(title_str)
    return title_str.strip()


# --------------------------------------------------
# HIJRI DATE HELPERS
# --------------------------------------------------
def _hijri_date_label(d: date) -> str:
    """Get full Hijri date label"""
    try:
        from hijri_converter import convert
        h = convert.Gregorian(d.year, d.month, d.day).to_hijri()
        hijri_months = [
            "Muḥarram al-Harām", "Safar al-Muzaffar", "Rabi al-Awwal", "Rabī al-Aakhar",
            "Jamādil Awwal", "Jamādā al-ʾŪkhrā", "Rajab al-Asab", "Shabān al-Karim",
            "Shehrullah al-Moazzam", "Shawwāl al-Mukarram", "Zilqādah al-Harām", "Zilhijjatil Harām",
        ]
        return f"{h.day} {hijri_months[h.month - 1]} {h.year} H"
    except Exception as e:
        current_app.logger.warning(f"Hijri conversion error: {e}")
        return d.strftime("%d %B %Y")


def _hijri_date_label_short(d: date) -> str:
    """
    Short Hijri date for table columns – compact so it fits in narrow cells.
    Example: 05-01-47 H (for 5 Muharram 1447 H)
    """
    try:
        from hijri_converter import convert
        h = convert.Gregorian(d.year, d.month, d.day).to_hijri()
        return f"{h.day:02d}-{h.month:02d}-{str(h.year)[-2:]} H"
    except Exception as e:
        current_app.logger.warning(f"Hijri conversion error: {e}")
        return d.strftime("%d-%m-%y")


def _hijri_month_year_label(d: date) -> str:
    """Get Hijri month-year label"""
    return KQ.get_hijri_month_year_label(d)


def _hijri_from_any(value) -> str:
    """
    Convert a value (date/datetime/ISO string) to a short Hijri date label.
    """
    if not value:
        return "-"
    try:
        if isinstance(value, datetime):
            g = value.date()
        elif isinstance(value, date):
            g = value
        else:
            s = str(value).split(" ")[0]
            g = datetime.strptime(s, "%Y-%m-%d").date()
        return KQ.get_hijri_date_label(g)
    except Exception as e:
        current_app.logger.warning(f"Hijri conversion error for {value}: {e}")
        return str(value)


def get_academic_year_period():
    """Get formatted Academic Year period in Hijri."""
    start, end = KQ.get_ay_bounds()
    
    if not start or not end:
        return "Academic Year not started yet"
    
    try:
        from hijri_converter import convert
        h_start = convert.Gregorian(start.year, start.month, start.day).to_hijri()
        month_start = [
            "Muḥarram al-Harām", "Safar al-Muzaffar", "Rabi al-Awwal", "Rabī al-Akhar",
            "Jamādil Awwal", "Jamādā al-ʾŪkhrā", "Rajab al-Asab", "Shabān al-Karim",
            "Shehrullah al-Moazzam", "Shawwāl al-Mukarram", "Zilqādah al-Harām", "Zilhijjatil Harām",
        ][h_start.month - 1]
        
        h_end = convert.Gregorian(end.year, end.month, end.day).to_hijri()
        month_end = [
            "Muḥarram al-Harām", "Safar al-Muzaffar", "Rabi al-Awwal", "Rabī al-Akhar",
            "Jamādil Awwal", "Jamādā al-ʾŪkhrā", "Rajab al-Asab", "Shabān al-Karim",
            "Shehrullah al-Moazzam", "Shawwāl al-Mukarram", "Zilqādah al-Harām", "Zilhijjatil Harām",
        ][h_end.month - 1]
        
        return f"{h_start.day} {month_start} {h_start.year} H to {h_end.day} {month_end} {h_end.year} H"
    except Exception:
        return f"{start.strftime('%d %b %Y')} to {end.strftime('%d %b %Y')}"


# --------------------------------------------------
# TEACHER ACCESS HELPERS
# --------------------------------------------------
def _teacher_allowed_darajah(username: str, darajah_name: str) -> bool:
    """
    Validate that the teacher is mapped to the given darajah.
    """
    if not username or not darajah_name:
        current_app.logger.warning(f"Invalid parameters: username={username}, darajah={darajah_name}")
        return False
    
    try:
        from db_app import get_conn as get_app_conn
        conn = get_app_conn()
        cur = conn.cursor()
        
        darajah_clean = darajah_name.strip()
        
        cur.execute(
            """
            SELECT COUNT(*)
            FROM teacher_darajah_mapping
            WHERE teacher_username = ?
              AND darajah_name = ?
            """,
            (username, darajah_clean),
        )
        ok = cur.fetchone()[0] > 0
        
        cur.close()
        conn.close()
        
        current_app.logger.info(f"Teacher {username} allowed for {darajah_clean}: {ok}")
        return ok
    except Exception as e:
        current_app.logger.error(f"Error checking teacher access: {e}")
        return False


def get_teacher_darajah(username: str):
    """
    Get the darajah that a teacher is mapped to.
    Returns the first darajah found for the teacher.
    """
    if not username:
        return None
    
    try:
        from db_app import get_conn as get_app_conn
        conn = get_app_conn()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT darajah_name 
            FROM teacher_darajah_mapping 
            WHERE teacher_username = ?
            LIMIT 1
        """, (username,))
        
        result = cur.fetchone()
        cur.close()
        conn.close()
        
        if result:
            darajah = result[0]
            current_app.logger.info(f"Teacher {username} mapped to darajah: {darajah}")
            return darajah
        return None
    except Exception as e:
        current_app.logger.error(f"Error getting teacher darajah: {e}")
        return None


# --------------------------------------------------
# DARAJAH TREND (AY, using statistics table)
# --------------------------------------------------
def _darajah_ay_trend(darajah_name: str):
    """Get monthly trend data for a darajah"""
    try:
        labels, values = KQ.get_ay_trend_data(darajah_name=darajah_name)
        
        start, end = KQ.get_ay_bounds()
        last_month = min(date.today(), end).replace(day=1)
        period_label = f"{_hijri_month_year_label(start)} – {_hijri_month_year_label(last_month)}"
        
        return labels, values, period_label
    except Exception as e:
        current_app.logger.error(f"Error getting darajah trend: {e}")
        return [], [], ""


# --------------------------------------------------
# FIXED: GET AY STUDENT STATISTICS - ONLY STUDENTS WITH BOOKS
# --------------------------------------------------
def _get_ay_student_stats(darajah_name: str):
    """
    Get per-student AY statistics - ONLY STUDENTS WITH BOOKS ISSUED.
    This uses INNER JOIN with statistics to ensure only students with issues are returned.
    """
    start, end = KQ.get_ay_bounds()
    if not start:
        return []
    
    try:
        conn = get_koha_conn()
        cur = conn.cursor()
        
        # FIXED: INNER JOIN with statistics to only get students with issues
        cur.execute(
            """
            SELECT 
                trno.attribute AS TRNumber,
                CONCAT(COALESCE(b.surname, ''), ' ', COALESCE(b.firstname, '')) AS FullName,
                COUNT(*) AS Issues_AY,
                COALESCE(fee_totals.FeesPaid_AY, 0.0) AS FeesPaid_AY,
                COALESCE(overdue_counts.Overdues, 0) AS Overdues,
                GROUP_CONCAT(DISTINCT it.ccode ORDER BY it.ccode SEPARATOR ', ') AS CollectionsUsed
            FROM statistics s
            INNER JOIN borrowers b ON b.borrowernumber = s.borrowernumber
            INNER JOIN items it ON s.itemnumber = it.itemnumber
            LEFT JOIN borrower_attributes std
                 ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
            LEFT JOIN borrower_attributes trno
                 ON trno.borrowernumber = b.borrowernumber
                AND trno.code = 'TRNO'
            LEFT JOIN (
                SELECT a.borrowernumber, SUM(-a.amount) AS FeesPaid_AY
                FROM accountlines a
                WHERE a.credit_type_code = 'PAYMENT'
                  AND (a.status IS NULL OR a.status <> 'VOID')
                  AND DATE(a.date) BETWEEN %s AND %s
                GROUP BY a.borrowernumber
            ) fee_totals ON fee_totals.borrowernumber = b.borrowernumber
            LEFT JOIN (
                SELECT i.borrowernumber, COUNT(*) AS Overdues
                FROM issues i
                WHERE i.returndate IS NULL
                  AND i.date_due < CURDATE()
                GROUP BY i.borrowernumber
            ) overdue_counts ON overdue_counts.borrowernumber = b.borrowernumber
            WHERE s.type = 'issue'
              AND DATE(s.datetime) BETWEEN %s AND %s
              AND (std.attribute = %s OR b.branchcode = %s)
              AND trno.attribute IS NOT NULL
              AND trno.attribute != ''
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            GROUP BY trno.attribute, b.borrowernumber, b.surname, b.firstname
            ORDER BY Issues_AY DESC
            """,
            (start, end, start, end, darajah_name, darajah_name)
        )
        
        rows = cur.fetchall()
        current_app.logger.info(f"Found {len(rows)} students with books issued for darajah {darajah_name}")
        
        # Process the results
        processed_list = []
        for student in rows:
            tr_no = student.get("TRNumber", "")
            if not tr_no:
                continue
                
            full_name = student.get("FullName", "").strip()
            if not full_name or full_name.lower() in ['none', 'null']:
                # Try to get student info for better name
                try:
                    student_info = get_student_info(tr_no)
                    if student_info and student_info.get("FullName"):
                        full_name = student_info.get("FullName")
                    else:
                        full_name = f"Student (TR: {tr_no})"
                except:
                    full_name = f"Student (TR: {tr_no})"
            else:
                full_name = _clean_student_name(full_name)
            
            issues_ay = int(student.get("Issues_AY", 0))
            if issues_ay > 0:
                processed_list.append({
                    "TRNumber": tr_no,
                    "FullName": full_name,
                    "Issues_AY": issues_ay,
                    "Overdues": int(student.get("Overdues", 0)),
                    "FeesPaid_AY": float(student.get("FeesPaid_AY", 0.0)),
                    "CollectionsUsed": student.get("CollectionsUsed", ""),
                    "display_name": _format_student_display(tr_no, full_name)
                })
        
        cur.close()
        conn.close()
        
        return processed_list
        
    except Exception as e:
        current_app.logger.error(f"Error getting AY student stats for {darajah_name}: {str(e)}")
        current_app.logger.error(traceback.format_exc())
        return []


# --------------------------------------------------
# NEW: GET ALL STUDENTS IN DARAJAH (including zero issues)
# --------------------------------------------------
def _get_all_students_in_darajah(darajah_name: str):
    """
    Get ALL students in a darajah, including those with zero books issued.
    Used for accurate student count.
    """
    start, end = KQ.get_ay_bounds()
    
    try:
        conn = get_koha_conn()
        cur = conn.cursor()
        
        cur.execute(
            """
            SELECT 
                trno.attribute AS TRNumber,
                CONCAT(COALESCE(b.surname, ''), ' ', COALESCE(b.firstname, '')) AS FullName,
                COALESCE(issue_counts.Issues_AY, 0) AS Issues_AY,
                COALESCE(overdue_counts.Overdues, 0) AS Overdues,
                COALESCE(fee_totals.FeesPaid_AY, 0.0) AS FeesPaid_AY,
                b.cardnumber,
                b.email,
                b.dateexpiry
            FROM borrowers b
            LEFT JOIN borrower_attributes std
                 ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
            LEFT JOIN borrower_attributes trno
                 ON trno.borrowernumber = b.borrowernumber
                AND trno.code = 'TRNO'
            LEFT JOIN (
                SELECT s.borrowernumber, COUNT(*) AS Issues_AY
                FROM statistics s
                WHERE s.type = 'issue'
                  AND DATE(s.datetime) BETWEEN %s AND %s
                GROUP BY s.borrowernumber
            ) issue_counts ON issue_counts.borrowernumber = b.borrowernumber
            LEFT JOIN (
                SELECT a.borrowernumber, SUM(-a.amount) AS FeesPaid_AY
                FROM accountlines a
                WHERE a.credit_type_code = 'PAYMENT'
                  AND (a.status IS NULL OR a.status <> 'VOID')
                  AND DATE(a.date) BETWEEN %s AND %s
                GROUP BY a.borrowernumber
            ) fee_totals ON fee_totals.borrowernumber = b.borrowernumber
            LEFT JOIN (
                SELECT i.borrowernumber, COUNT(*) AS Overdues
                FROM issues i
                WHERE i.returndate IS NULL
                  AND i.date_due < CURDATE()
                GROUP BY i.borrowernumber
            ) overdue_counts ON overdue_counts.borrowernumber = b.borrowernumber
            WHERE (std.attribute = %s OR b.branchcode = %s)
              AND trno.attribute IS NOT NULL
              AND trno.attribute != ''
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            ORDER BY Issues_AY DESC, FullName ASC
            """,
            (start, end, start, end, darajah_name, darajah_name)
        )
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        students_list = []
        for row in rows:
            tr_no = row.get("TRNumber", "")
            if not tr_no:
                continue
                
            full_name = row.get("FullName", "").strip()
            if not full_name or full_name.lower() in ['none', 'null']:
                full_name = f"Student (TR: {tr_no})"
            else:
                full_name = _clean_student_name(full_name)
            
            students_list.append({
                "TRNumber": tr_no,
                "FullName": full_name,
                "Issues_AY": int(row.get("Issues_AY", 0)),
                "Overdues": int(row.get("Overdues", 0)),
                "FeesPaid_AY": float(row.get("FeesPaid_AY", 0.0)),
                "CardNumber": row.get("cardnumber", ""),
                "Email": row.get("email", ""),
                "DateExpiry": row.get("dateexpiry", ""),
                "display_name": _format_student_display(tr_no, full_name)
            })
        
        return students_list
        
    except Exception as e:
        current_app.logger.error(f"Error getting all students for darajah {darajah_name}: {str(e)}")
        return []


# --------------------------------------------------
# FIXED: CURRENT MONTH SUMMARY
# --------------------------------------------------
def _darajah_current_month_summary(darajah_name: str):
    """
    Get summary of books issued to students in the current month.
    Returns a tuple of (summary_dict, collections_summary)
    """
    try:
        today = date.today()
        month_start = date(today.year, today.month, 1)
        month_end = today

        conn = get_koha_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT
                trno.attribute AS TRNo,
                TRIM(CONCAT(
                    COALESCE(b.surname, ''),
                    CASE WHEN b.surname IS NOT NULL AND b.firstname IS NOT NULL 
                         THEN ' ' ELSE '' END,
                    COALESCE(b.firstname, '')
                )) AS FullName,
                COUNT(DISTINCT s.itemnumber) AS BooksMonth,
                GROUP_CONCAT(DISTINCT bib.title ORDER BY bib.title SEPARATOR ' • ') AS TitlesMonth,
                GROUP_CONCAT(DISTINCT it.ccode ORDER BY it.ccode SEPARATOR ', ') AS CollectionsMonth,
                GROUP_CONCAT(DISTINCT bib.biblionumber) AS BiblioIDs
            FROM statistics s
            INNER JOIN borrowers b ON b.borrowernumber = s.borrowernumber
            LEFT JOIN borrower_attributes std ON 
                std.borrowernumber = b.borrowernumber
                AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
            LEFT JOIN borrower_attributes trno ON 
                trno.borrowernumber = b.borrowernumber
                AND trno.code = 'TRNO'
            INNER JOIN items it ON s.itemnumber = it.itemnumber
            INNER JOIN biblio bib ON it.biblionumber = bib.biblionumber
            WHERE s.type = 'issue'
              AND DATE(s.datetime) BETWEEN %s AND %s
              AND (std.attribute = %s OR b.branchcode = %s)
              AND trno.attribute IS NOT NULL
              AND trno.attribute != ''
            GROUP BY trno.attribute, b.borrowernumber, b.surname, b.firstname
            HAVING BooksMonth > 0
            ORDER BY BooksMonth DESC
        """, (month_start, month_end, darajah_name, darajah_name))
        
        rows = cur.fetchall()
        
        summary_by_trno = {}
        collections_all = set()

        for r in rows:
            tr = str(r.get("TRNo") or "").strip()
            if not tr:
                continue
                
            books_month = int(r.get("BooksMonth") or 0)
            titles_month = r.get("TitlesMonth") or ""
            collections_month = r.get("CollectionsMonth") or ""
            full_name = r.get("FullName") or ""
            biblio_ids = r.get("BiblioIDs") or ""

            # Clean the name
            full_name = _clean_student_name(full_name)
            if not full_name or full_name == "Name not available":
                full_name = f"Student (TR: {tr})"
            else:
                full_name = full_name.strip()

            # Clean titles
            cleaned_titles = []
            if titles_month:
                title_list = titles_month.split(' • ')
                for title in title_list:
                    cleaned_titles.append(_clean_title(title))
                titles_month = ' • '.join(cleaned_titles)

            summary_by_trno[tr] = {
                "full_name": full_name,
                "books_month": books_month,
                "titles_month": titles_month,
                "collections_month": collections_month,
                "biblio_ids": biblio_ids,
                "display_name": _format_student_display(tr, full_name)
            }

            # Track all unique collections
            if collections_month:
                for cc in collections_month.split(","):
                    c = cc.strip()
                    if c:
                        collections_all.add(c)

        collections_summary = ", ".join(sorted(collections_all)) if collections_all else ""
        
        cur.close()
        conn.close()
        
        return summary_by_trno, collections_summary
        
    except Exception as e:
        current_app.logger.error(f"Error in _darajah_current_month_summary: {str(e)}")
        current_app.logger.error(traceback.format_exc())
        return {}, ""


# --------------------------------------------------
# CURRENT OVERDUES PER STUDENT
# --------------------------------------------------
def _darajah_current_overdues(darajah_name: str):
    """Get current overdue books for a darajah"""
    try:
        today = date.today()

        conn = get_koha_conn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
              trno.attribute AS TRNo,
              COUNT(*) AS OverdueNow
            FROM issues iss
            JOIN borrowers b
                 ON b.borrowernumber = iss.borrowernumber
            LEFT JOIN borrower_attributes std
                 ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
            LEFT JOIN borrower_attributes trno
                 ON trno.borrowernumber = b.borrowernumber
                AND trno.code = 'TRNO'
            WHERE (std.attribute = %s OR b.branchcode = %s)
              AND trno.attribute IS NOT NULL
              AND trno.attribute != ''
              AND iss.returndate IS NULL
              AND iss.date_due < %s
            GROUP BY trno.attribute, b.borrowernumber;
            """,
            (darajah_name, darajah_name, today),
        )

        rows = cur.fetchall()
        cur.close()
        conn.close()

        overdue_by_trno = {}
        for r in rows:
            tr = str(r.get("TRNo") or "").strip()
            if not tr:
                continue
            overdue_by_trno[tr] = int(r.get("OverdueNow") or 0)

        return overdue_by_trno
    except Exception as e:
        current_app.logger.error(f"Error getting current overdues: {e}")
        return {}


# --------------------------------------------------
# GET ACCURATE DARAJAH AY STATISTICS
# --------------------------------------------------
def _get_darajah_ay_stats(darajah_name: str):
    """Get accurate AY statistics for a darajah."""
    start, end = KQ.get_ay_bounds()
    if not start:
        return {
            "books_issued": 0,
            "overdues": 0,
            "fees_paid": 0.0,
            "active_students": 0,
            "total_students": 0
        }

    try:
        conn = get_koha_conn()
        cur = conn.cursor()
        
        # Get AY books issued
        cur.execute(
            """
            SELECT COUNT(*) as books_issued
            FROM statistics s
            JOIN borrowers b
                 ON b.borrowernumber = s.borrowernumber
            LEFT JOIN borrower_attributes std
                 ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
            LEFT JOIN borrower_attributes trno
                 ON trno.borrowernumber = b.borrowernumber
                AND trno.code = 'TRNO'
            WHERE s.type = 'issue'
              AND DATE(s.datetime) BETWEEN %s AND %s
              AND (std.attribute = %s OR b.branchcode = %s)
              AND trno.attribute IS NOT NULL
              AND trno.attribute != ''
            """,
            (start, end, darajah_name, darajah_name)
        )
        books_row = cur.fetchone()
        books_issued = books_row["books_issued"] if books_row else 0
        
        # Get AY fees paid
        cur.execute(
            """
            SELECT COALESCE(SUM(
                CASE
                  WHEN a.credit_type_code='PAYMENT'
                       AND (a.status IS NULL OR a.status <> 'VOID')
                       AND DATE(a.date) BETWEEN %s AND %s
                  THEN -a.amount ELSE 0 END
            ),0) as fees_paid
            FROM accountlines a
            JOIN borrowers b ON a.borrowernumber = b.borrowernumber
            LEFT JOIN borrower_attributes std
                 ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
            LEFT JOIN borrower_attributes trno
                 ON trno.borrowernumber = b.borrowernumber
                AND trno.code = 'TRNO'
            WHERE (std.attribute = %s OR b.branchcode = %s)
              AND trno.attribute IS NOT NULL
              AND trno.attribute != ''
            """,
            (start, end, darajah_name, darajah_name)
        )
        fees_row = cur.fetchone()
        fees_paid = float(fees_row["fees_paid"] if fees_row else 0)
        
        # Get currently overdue
        cur.execute(
            """
            SELECT COUNT(*) as overdues
            FROM issues i
            JOIN borrowers b
                 ON b.borrowernumber = i.borrowernumber
            LEFT JOIN borrower_attributes std
                 ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
            LEFT JOIN borrower_attributes trno
                 ON trno.borrowernumber = b.borrowernumber
                AND trno.code = 'TRNO'
            WHERE (std.attribute = %s OR b.branchcode = %s)
              AND trno.attribute IS NOT NULL
              AND trno.attribute != ''
              AND i.returndate IS NULL
              AND i.date_due < CURDATE()
            """,
            (darajah_name, darajah_name)
        )
        overdues_row = cur.fetchone()
        overdues = overdues_row["overdues"] if overdues_row else 0
        
        # Get active students with TR numbers (students who issued at least 1 book)
        cur.execute(
            """
            SELECT COUNT(DISTINCT trno.attribute) as active_students
            FROM statistics s
            JOIN borrowers b ON b.borrowernumber = s.borrowernumber
            LEFT JOIN borrower_attributes std
                 ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
            LEFT JOIN borrower_attributes trno
                 ON trno.borrowernumber = b.borrowernumber
                AND trno.code = 'TRNO'
            WHERE s.type = 'issue'
              AND DATE(s.datetime) BETWEEN %s AND %s
              AND (std.attribute = %s OR b.branchcode = %s)
              AND trno.attribute IS NOT NULL
              AND trno.attribute != ''
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            """,
            (start, end, darajah_name, darajah_name)
        )
        students_row = cur.fetchone()
        active_students = students_row["active_students"] if students_row else 0
        
        # Get total students in darajah
        cur.execute(
            """
            SELECT COUNT(DISTINCT trno.attribute) as total_students
            FROM borrowers b
            LEFT JOIN borrower_attributes std
                 ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
            LEFT JOIN borrower_attributes trno
                 ON trno.borrowernumber = b.borrowernumber
                AND trno.code = 'TRNO'
            WHERE (std.attribute = %s OR b.branchcode = %s)
              AND trno.attribute IS NOT NULL
              AND trno.attribute != ''
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            """,
            (darajah_name, darajah_name)
        )
        total_row = cur.fetchone()
        total_students = total_row["total_students"] if total_row else 0
        
        cur.close()
        conn.close()
        
        return {
            "books_issued": books_issued,
            "overdues": overdues,
            "fees_paid": fees_paid,
            "active_students": active_students,
            "total_students": total_students
        }
        
    except Exception as e:
        current_app.logger.error(f"Error getting darajah AY stats: {e}")
        return {
            "books_issued": 0,
            "overdues": 0,
            "fees_paid": 0.0,
            "active_students": 0,
            "total_students": 0
        }


# --------------------------------------------------
# TOP TITLES FOR THIS DARAJAH
# --------------------------------------------------
def _darajah_top_titles_by_lang(darajah_name: str, lang_pattern: str, limit: int = 10):
    """Get top titles by language for a darajah"""
    try:
        start, end = KQ.get_ay_bounds()
        conn = get_koha_conn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                bib.biblionumber AS Biblio_ID,
                bib.title AS Title,
                ExtractValue(
                    bmd.metadata,
                    '//datafield[@tag="041"]/subfield[@code="a"]'
                ) AS Language,
                GROUP_CONCAT(DISTINCT it.ccode SEPARATOR ', ') AS Collections,
                COUNT(*) AS Times_Issued,
                MAX(all_iss.issuedate) AS LastIssued
            FROM (
                SELECT borrowernumber, itemnumber, issuedate
                FROM issues
                UNION ALL
                SELECT borrowernumber, itemnumber, issuedate
                FROM old_issues
            ) all_iss
            JOIN borrowers b
                 ON b.borrowernumber = all_iss.borrowernumber
            LEFT JOIN borrower_attributes std
                 ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
            JOIN items it
                 ON all_iss.itemnumber = it.itemnumber
            JOIN biblio bib
                 ON it.biblionumber = bib.biblionumber
            JOIN biblio_metadata bmd
                 ON bib.biblionumber = bmd.biblionumber
            WHERE DATE(all_iss.issuedate) BETWEEN %s AND %s
              AND (std.attribute = %s OR b.branchcode = %s)
              AND ExtractValue(
                    bmd.metadata,
                    '//datafield[@tag="041"]/subfield[@code="a"]'
                  ) LIKE %s
            GROUP BY bib.biblionumber, bib.title, Language
            ORDER BY Times_Issued DESC
            LIMIT %s;
            """,
            (start, end, darajah_name, darajah_name, lang_pattern, int(limit)),
        )

        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        for row in rows:
            if 'Title' in row:
                row['Title'] = _clean_title(row['Title'])
        return rows or []
    except Exception as e:
        current_app.logger.error(f"Error getting top titles: {e}")
        return []


# --------------------------------------------------
# GET TOP STUDENTS FOR DARAJAH
# --------------------------------------------------
def _get_top_students_for_darajah(darajah_name: str, limit: int = 10):
    """Get top students in a darajah based on AY issues"""
    start, end = KQ.get_ay_bounds()
    if not start:
        return []
    
    try:
        conn = get_koha_conn()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                trno.attribute AS TRNumber,
                CASE 
                    WHEN (b.surname IS NULL OR b.surname = '' OR b.surname = 'None') 
                         AND (b.firstname IS NULL OR b.firstname = '' OR b.firstname = 'None')
                    THEN CONCAT('Student #', b.cardnumber)
                    WHEN b.surname IS NULL OR b.surname = '' OR b.surname = 'None'
                    THEN b.firstname
                    WHEN b.firstname IS NULL OR b.firstname = '' OR b.firstname = 'None'
                    THEN b.surname
                    ELSE CONCAT(b.surname, ' ', b.firstname)
                END AS FullName,
                b.cardnumber,
                COALESCE(std.attribute, b.branchcode) AS Class,
                c.description AS Department,
                COUNT(*) AS Issues_AY,
                GROUP_CONCAT(DISTINCT it.ccode ORDER BY it.ccode SEPARATOR ', ') AS CollectionsUsed
            FROM statistics s
            JOIN borrowers b ON b.borrowernumber = s.borrowernumber
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
            LEFT JOIN borrower_attributes trno
                ON trno.borrowernumber = b.borrowernumber
                AND trno.code = 'TRNO'
            JOIN items it ON s.itemnumber = it.itemnumber
            WHERE s.type = 'issue'
              AND DATE(s.datetime) BETWEEN %s AND %s
              AND (std.attribute = %s OR b.branchcode = %s)
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
              AND trno.attribute IS NOT NULL
              AND trno.attribute != ''
            GROUP BY b.borrowernumber, b.surname, b.firstname, b.cardnumber, 
                     std.attribute, b.branchcode, c.description, trno.attribute
            ORDER BY Issues_AY DESC
            LIMIT %s;
        """, (start, end, darajah_name, darajah_name, limit))
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        for row in rows:
            if row["FullName"] is None or str(row["FullName"]).strip() == "" or str(row["FullName"]).lower() == "none":
                row["FullName"] = f"Student #{row['TRNumber']}"
            
            row["FullName"] = _clean_student_name(row["FullName"])
            row["display_name"] = _format_student_display(row["TRNumber"], row["FullName"])
        
        return rows
        
    except Exception as e:
        current_app.logger.error(f"Error getting top students for darajah {darajah_name}: {str(e)}")
        current_app.logger.error(traceback.format_exc())
        return []


# --------------------------------------------------
# PARSE DARAJAH NAME FOR DISPLAY
# --------------------------------------------------
def _parse_darajah_name(darajah_name: str):
    """
    Parse darajah names like '5 B M', '5 B F', '7A', '7AF', etc.
    Returns a dict with parsed components.
    """
    if not darajah_name:
        return {
            "original": "", 
            "display": "", 
            "year": "", 
            "section": "", 
            "gender": "",
            "gender_code": "",
            "is_arabic": False
        }
    
    original = darajah_name.strip()
    
    normalized = original.upper().replace(" ", "")
    
    year_match = re.match(r'^(\d+)', normalized)
    year = year_match.group(1) if year_match else ""
    
    remaining = normalized[len(year):] if year else normalized
    
    section = ""
    gender_code = ""
    gender = ""
    
    if remaining:
        if len(remaining) >= 2:
            if remaining[0].isalpha() and remaining[1] in ['M', 'F']:
                section = remaining[0]
                gender_code = remaining[1]
            elif remaining[0] in ['M', 'F']:
                gender_code = remaining[0]
            elif remaining[0].isalpha():
                section = remaining[0]
                if len(remaining) > 1 and remaining[1] in ['M', 'F']:
                    gender_code = remaining[1]
        elif len(remaining) == 1:
            if remaining[0] in ['M', 'F']:
                gender_code = remaining[0]
            elif remaining[0].isalpha():
                section = remaining[0]
    
    if " " in original:
        parts = original.upper().split()
        if len(parts) >= 3:
            if parts[1].isalpha() and len(parts[1]) == 1:
                section = parts[1]
            if parts[-1] in ['M', 'F']:
                gender_code = parts[-1]
        elif len(parts) == 2:
            if parts[1] in ['M', 'F']:
                gender_code = parts[1]
    
    if gender_code == 'M':
        gender = "Boys"
    elif gender_code == 'F':
        gender = "Girls"
    else:
        if "M" in original.upper() or "BOYS" in original.upper():
            gender = "Boys"
            gender_code = "M"
        elif "F" in original.upper() or "GIRLS" in original.upper():
            gender = "Girls"
            gender_code = "F"
        else:
            gender = "Mixed"
            gender_code = ""
    
    display_parts = []
    if year:
        display_parts.append(f"Darajah {year}")
        if section:
            display_parts.append(section)
        if gender_code:
            display_parts.append(gender_code)
    
    display = " ".join(display_parts) if display_parts else original
    
    is_arabic = "عربي" in original or "ARABIC" in original.upper()
    
    return {
        "original": original,
        "display": display,
        "year": year,
        "section": section,
        "gender": gender,
        "gender_code": gender_code,
        "is_arabic": is_arabic
    }


# --------------------------------------------------
# DARAJAH MANAGEMENT FUNCTIONS
# --------------------------------------------------
def get_darajahs_from_teacher_mapping():
    """Get darajahs from teacher_darajah_mapping table"""
    try:
        from db_app import get_conn as get_app_conn
        
        conn = get_app_conn()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT DISTINCT darajah_name 
            FROM teacher_darajah_mapping 
            WHERE darajah_name IS NOT NULL 
            AND darajah_name != ''
            ORDER BY 
                CASE 
                    WHEN CAST(substr(darajah_name, 1, instr(darajah_name || ' ', ' ') - 1) AS INTEGER) IS NOT NULL
                    THEN CAST(substr(darajah_name, 1, instr(darajah_name || ' ', ' ') - 1) AS INTEGER)
                    ELSE 999 
                END,
                darajah_name
        """)
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        darajahs = []
        for row in rows:
            darajah_name = row[0]
            if darajah_name:
                parsed = _parse_darajah_name(darajah_name)
                darajahs.append({
                    "name": darajah_name,
                    "display": parsed["display"],
                    "year": parsed["year"],
                    "section": parsed["section"],
                    "gender": parsed["gender"],
                    "gender_code": parsed["gender_code"],
                    "is_arabic": parsed["is_arabic"],
                    "active_students": 0
                })
        
        return darajahs
    except Exception as e:
        current_app.logger.error(f"Error getting darajahs from teacher mapping: {e}")
        return []


def should_include_darajah(darajah_name, parsed_info):
    """Apply reality-based filtering for darajahs"""
    if not parsed_info["year"] or not parsed_info["year"].isdigit():
        return False
        
    try:
        darajah_num = int(parsed_info["year"])
    except ValueError:
        return False
    
    if darajah_num < 1 or darajah_num > 11:
        return False
    
    if darajah_num >= 9:
        if parsed_info["gender_code"] != "M":
            return False
    
    return True


def is_recognized_darajah(darajah_name):
    """Check if a darajah matches known patterns"""
    if not darajah_name:
        return False
    
    recognized_patterns = [
        r'^\d+ [A-C] [MF]$',
        r'^\d+[A-C][MF]$',
        r'^\d+ DARS F$',
        r'^\d+ DARS$',
    ]
    
    for pattern in recognized_patterns:
        if re.match(pattern, darajah_name, re.IGNORECASE):
            return True
    
    return False


def get_all_darajahs():
    """Get all distinct darajahs using multiple data sources"""
    teacher_mapping_darajahs = get_darajahs_from_teacher_mapping()
    if teacher_mapping_darajahs:
        return teacher_mapping_darajahs
    
    try:
        conn = get_koha_conn()
        cur = conn.cursor()
        
        cur.execute("""
            WITH recent_activity AS (
                SELECT DISTINCT 
                    COALESCE(std.attribute, b.branchcode) AS darajah_name,
                    COUNT(DISTINCT s.borrowernumber) as active_count
                FROM statistics s
                JOIN borrowers b ON s.borrowernumber = b.borrowernumber
                LEFT JOIN borrower_attributes std
                    ON std.borrowernumber = b.borrowernumber
                    AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
                WHERE s.type = 'issue'
                    AND s.datetime >= DATE_SUB(NOW(), INTERVAL 180 DAY)
                    AND COALESCE(std.attribute, b.branchcode) IS NOT NULL
                    AND COALESCE(std.attribute, b.branchcode) != ''
                GROUP BY COALESCE(std.attribute, b.branchcode)
            ),
            active_students AS (
                SELECT 
                    COALESCE(std.attribute, b.branchcode) AS darajah_name,
                    COUNT(DISTINCT b.borrowernumber) as student_count
                FROM borrowers b
                LEFT JOIN borrower_attributes std
                    ON std.borrowernumber = b.borrowernumber
                    AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
                LEFT JOIN borrower_attributes trno
                    ON trno.borrowernumber = b.borrowernumber
                    AND trno.code = 'TRNO'
                WHERE COALESCE(std.attribute, b.branchcode) IS NOT NULL
                    AND COALESCE(std.attribute, b.branchcode) != ''
                    AND trno.attribute IS NOT NULL
                    AND trno.attribute != ''
                    AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                    AND (b.debarred IS NULL OR b.debarred = 0)
                    AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
                GROUP BY COALESCE(std.attribute, b.branchcode)
            )
            SELECT 
                COALESCE(ra.darajah_name, asd.darajah_name) AS darajah_name,
                COALESCE(ra.active_count, 0) as recent_activity,
                COALESCE(asd.student_count, 0) as active_students
            FROM recent_activity ra
            FULL JOIN active_students asd ON ra.darajah_name = asd.darajah_name
            ORDER BY 
                CASE 
                    WHEN COALESCE(ra.darajah_name, asd.darajah_name) REGEXP '^[0-9]+' 
                    THEN CAST(SUBSTRING_INDEX(COALESCE(ra.darajah_name, asd.darajah_name), ' ', 1) AS UNSIGNED)
                    ELSE 999 
                END
        """)
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        darajahs = []
        for row in rows:
            darajah_name = row[0]
            if not darajah_name:
                continue
                
            parsed = _parse_darajah_name(darajah_name)
            
            if not should_include_darajah(darajah_name, parsed):
                continue
                
            darajahs.append({
                "name": darajah_name,
                "display": parsed["display"],
                "year": parsed["year"],
                "section": parsed["section"],
                "gender": parsed["gender"],
                "gender_code": parsed["gender_code"],
                "is_arabic": parsed["is_arabic"],
                "active_students": row[2] if len(row) > 2 else 0,
                "recent_activity": row[1] if len(row) > 1 else 0
            })
        
        darajahs.sort(key=lambda x: (
            int(x["year"]) if x["year"] and x["year"].isdigit() else 999,
            x["section"] or "",
            {"M": 0, "F": 1}.get(x["gender_code"], 2),
            x["name"]
        ))
        
        return darajahs
    except Exception as e:
        current_app.logger.error(f"Error getting all darajahs: {e}")
        return []


# --------------------------------------------------
# DASHBOARD ROUTE - MAIN
# --------------------------------------------------
@bp.route("/")
def dashboard():
    """Main teacher dashboard - FIXED to show students with books"""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role not in ("teacher", "admin"):
        flash("You must be a darajah teacher or admin to view this dashboard.", "danger")
        return redirect(url_for("auth_bp.login"))

    username = session.get("username")
    current_app.logger.info(f"Dashboard accessed by {username} with role {role}")

    # Determine darajah
    if role == "teacher":
        username = session.get("username")
        current_app.logger.info(f"Teacher {username} accessing dashboard")
        
        token_darajah = (
            request.args.get("token")
            or request.args.get("darajah_name")
            or request.args.get("darajah")
        )
        token_darajah = token_darajah.strip() if token_darajah else None

        if token_darajah:
            current_app.logger.info(f"Token provided: {token_darajah}")
            if _teacher_allowed_darajah(username, token_darajah):
                session["darajah_name"] = token_darajah
                current_app.logger.info(f"Teacher {username} authorized for darajah {token_darajah}")
            else:
                flash("⚠️ You are not authorized for this darajah.", "warning")
                current_app.logger.warning(f"Teacher {username} NOT authorized for darajah {token_darajah}")

        darajah_name = session.get("darajah_name")
        
        if not darajah_name:
            darajah_name = get_teacher_darajah(username)
            if darajah_name:
                session["darajah_name"] = darajah_name
                current_app.logger.info(f"Teacher {username} mapped to darajah {darajah_name} from database")
                
    else:
        darajah_name = (
            request.args.get("darajah")
            or request.args.get("darajah_name")
            or session.get("darajah_name")
        )
        current_app.logger.info(f"Admin accessing dashboard for darajah: {darajah_name}")

    if darajah_name:
        darajah_name = darajah_name.replace('+', ' ')
        darajah_name = darajah_name.replace('%20', ' ')
        darajah_name = darajah_name.strip()
        current_app.logger.info(f"Processed darajah_name: '{darajah_name}'")

    def _render_empty_dashboard(extra_message=None):
        if extra_message:
            flash(extra_message, "warning")

        all_darajahs = []
        if role == "admin":
            all_darajahs = get_all_darajahs()

        parsed_darajah = _parse_darajah_name(darajah_name or "")
        
        return render_template(
            "teacher_dashboard.html",
            darajah_name=darajah_name or "",
            parsed_darajah=parsed_darajah,
            darajah_masool_name=username or "",
            darajah_report_title="Darajah Library Report",
            current_month_label="",
            total_students=0,
            active_borrowers=0,
            books_issued_darajah=0,
            total_overdues_current=0,
            total_overdues_ay=0,
            total_fees=0.0,
            trend_labels=[],
            trend_values=[],
            trend_period_label="",
            ay_period_label="",
            top_students=[],
            top_arabic=[],
            top_english=[],
            students=[],  # This will be empty
            month_students=[],
            collections_summary="",
            all_darajahs=all_darajahs,
            is_admin=role == "admin",
        )

    if not darajah_name:
        current_app.logger.warning(f"No darajah_name provided for user {username}")
        if role == "admin":
            all_darajahs = get_all_darajahs()
            return render_template(
                "teacher_dashboard_select.html",
                all_darajahs=all_darajahs,
                username=username,
                is_admin=True
            )
        else:
            flash("⚠️ Your account is not linked to any darajah. Please contact the administrator.", "warning")
            return _render_empty_dashboard("⚠️ Your account is not linked to any darajah.")

    parsed_darajah = _parse_darajah_name(darajah_name)
    current_app.logger.info(f"Parsed darajah: {parsed_darajah}")
    
    try:
        # Get accurate AY statistics
        ay_stats = _get_darajah_ay_stats(darajah_name)
        current_app.logger.info(f"AY stats for {darajah_name}: {ay_stats}")
        
        # Get ALL students in darajah for accurate count
        all_students_list = _get_all_students_in_darajah(darajah_name)
        total_students = len(all_students_list)
        
        # Get ONLY students with books issued
        students_with_books = _get_ay_student_stats(darajah_name)
        active_borrowers = len(students_with_books)
        
        current_app.logger.info(f"Total students in darajah: {total_students}")
        current_app.logger.info(f"Students with books issued: {active_borrowers}")
        
        # KPIs
        books_issued_darajah = ay_stats["books_issued"]
        total_overdues_ay = ay_stats["overdues"]
        total_fees = ay_stats["fees_paid"]
        
        # Get current overdues
        current_overdues_dict = _darajah_current_overdues(darajah_name)
        total_overdues_current = sum(current_overdues_dict.values()) if isinstance(current_overdues_dict, dict) else 0
        
        # Get trend data
        trend_labels, trend_values, trend_period_label = _darajah_ay_trend(darajah_name)
        ay_period_label = trend_period_label
        
        # Get top 5 students
        top_students = _get_top_students_for_darajah(darajah_name, limit=5)
        
        # Get top titles
        top_arabic = _darajah_top_titles_by_lang(darajah_name, "%Arabic%", limit=10)
        top_english = _darajah_top_titles_by_lang(darajah_name, "%English%", limit=10)
        
        # Get current month summary
        month_summary, collections_summary = _darajah_current_month_summary(darajah_name)
        
        # Prepare month students list
        month_students_list = []
        for tr_no, month_data in month_summary.items():
            current_overdue = current_overdues_dict.get(tr_no, 0) if isinstance(current_overdues_dict, dict) else 0
            
            month_students_list.append({
                "TRNumber": tr_no,
                "FullName": month_data["full_name"],
                "display_name": month_data["display_name"],
                "Books_CurrentMonth": month_data["books_month"],
                "Titles_CurrentMonth": month_data["titles_month"],
                "Collections_CurrentMonth": month_data["collections_month"],
                "Overdues_Current": current_overdue
            })
        
        month_students_list.sort(key=lambda x: x.get("Books_CurrentMonth", 0), reverse=True)
        
        darajah_masool_name = _escape_html(username) if username else ""
        today = date.today()
        current_month_label = _hijri_month_year_label(today)
        
        darajah_report_title = f"{parsed_darajah['display']} – Library Report"

        all_darajahs = []
        if role == "admin":
            all_darajahs = get_all_darajahs()
        
        # Prepare students list with display names
        for student in students_with_books:
            if "display_name" not in student:
                student["display_name"] = _format_student_display(
                    student.get("TRNumber"), 
                    student.get("FullName")
                )
        
        # Prepare month students list with cleaned titles
        for student in month_students_list:
            if student.get("Titles_CurrentMonth"):
                titles = student["Titles_CurrentMonth"].split(' • ')
                cleaned_titles = [_clean_title(t) for t in titles if t.strip()]
                student["Titles_CurrentMonth"] = ' • '.join(cleaned_titles)

        current_app.logger.info(f"Rendering teacher dashboard for darajah: {darajah_name}")
        return render_template(
            "teacher_dashboard.html",
            darajah_name=darajah_name,
            parsed_darajah=parsed_darajah,
            darajah_masool_name=darajah_masool_name,
            darajah_report_title=darajah_report_title,
            current_month_label=current_month_label,
            total_students=total_students,
            active_borrowers=active_borrowers,
            books_issued_darajah=books_issued_darajah,
            total_overdues_current=total_overdues_current,
            total_overdues_ay=total_overdues_ay,
            total_fees=total_fees,
            trend_labels=trend_labels,
            trend_values=trend_values,
            trend_period_label=trend_period_label,
            ay_period_label=ay_period_label,
            top_students=top_students,
            top_arabic=top_arabic,
            top_english=top_english,
            students=students_with_books,  # This is the key - only students with books
            month_students=month_students_list,
            collections_summary=collections_summary,
            all_darajahs=all_darajahs,
            is_admin=role == "admin",
            today=date.today().strftime("%Y-%m-%d"),
            OPAC_BASE=OPAC_BASE,
        )
        
    except Exception as e:
        current_app.logger.error(f"Error processing dashboard data: {str(e)}", exc_info=True)
        flash(f"Error processing data: {str(e)}", "danger")
        return _render_empty_dashboard(f"Error processing data for {darajah_name}")


# --------------------------------------------------
# OTHER ROUTES (unchanged from original)
# --------------------------------------------------
@bp.route("/darajah-explorer")
def darajah_explorer():
    """Darajah explorer page for admin"""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role != "admin":
        flash("Admin access required for darajah explorer.", "danger")
        return redirect(url_for("dashboard_bp.dashboard"))
    
    try:
        from hijri_converter import convert
        
        today = date.today()
        h = convert.Gregorian(today.year, today.month, today.day).to_hijri()
        hijri_months = [
            "Muḥarram al-Harām", "Safar al-Muzaffar", "Rabi al-Awwal", "Rabī al-Akhar",
            "Jamādil Awwal", "Jamādā al-ʾŪkhrā", "Rajab al-Asab", "Shabān al-Karim",
            "Shehrullah al-Moazzam", "Shawwāl al-Mukarram", "Zilqādah al-Harām", "Zilhijjatil Harām",
        ]
        hijri_today = f"{h.day} {hijri_months[h.month - 1]} {h.year} H"
    except Exception:
        hijri_today = today.strftime("%d %B %Y")
    
    all_darajahs = [d for d in get_all_darajahs() if d.get("gender") != "Mixed"]
    
    darajahs_with_stats = []
    for darajah_info in all_darajahs:
        darajah_name = darajah_info["name"]
        
        try:
            ay_stats = _get_darajah_ay_stats(darajah_name)
            
            icon = "users"
            gender = darajah_info.get("gender", "")
            if "Boys" in gender:
                icon = "male"
            elif "Girls" in gender:
                icon = "female"
            
            darajah_number = darajah_info.get("year", "")
            if not darajah_number:
                match = re.match(r'^(\d+)', darajah_name)
                darajah_number = match.group(1) if match else "Other"
            
            parsed = _parse_darajah_name(darajah_name)
            
            darajahs_with_stats.append({
                "darajah_name": darajah_name,
                "darajah_number": darajah_number,
                "display": parsed["display"],
                "section": parsed["section"],
                "gender": parsed["gender"],
                "gender_code": parsed["gender_code"],
                "icon": icon,
                "active_students": ay_stats["active_students"],
                "total_students": ay_stats["total_students"],
                "books_issued": ay_stats["books_issued"],
                "fees_paid": ay_stats["fees_paid"],
                "overdues": ay_stats["overdues"]
            })
        except Exception as e:
            current_app.logger.warning(f"Error getting stats for {darajah_name}: {e}")
            parsed = _parse_darajah_name(darajah_name)
            darajahs_with_stats.append({
                "darajah_name": darajah_name,
                "darajah_number": parsed.get("year", "Other"),
                "display": parsed["display"],
                "section": parsed["section"],
                "gender": parsed["gender"],
                "gender_code": parsed["gender_code"],
                "icon": "users",
                "active_students": 0,
                "total_students": 0,
                "books_issued": 0,
                "fees_paid": 0.0,
                "overdues": 0
            })
    
    darajahs_by_number = {}
    for darajah_info in darajahs_with_stats:
        darajah_num = darajah_info["darajah_number"]
        if darajah_num not in darajahs_by_number:
            darajahs_by_number[darajah_num] = []
        darajahs_by_number[darajah_num].append(darajah_info)
    
    sorted_numbers = sorted(
        [num for num in darajahs_by_number.keys() if num and num.isdigit()], 
        key=lambda x: int(x)
    )
    if "Other" in darajahs_by_number:
        sorted_numbers.append("Other")
    
    for darajah_num in darajahs_by_number:
        darajahs_by_number[darajah_num].sort(key=lambda x: (
            x["section"] or "", 
            x["gender"] or "",
            x["darajah_name"] or ""
        ))
    
    return render_template(
        "darajah_explorer.html",
        hijri_today=hijri_today,
        total_darajahs=len(darajahs_with_stats),
        darajahs_by_year=darajahs_by_number,
        sorted_years=sorted_numbers
    )


@bp.route("/quick-select")
def quick_select():
    """Quick darajah selection endpoint for admin"""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role != "admin":
        flash("Admin access required.", "danger")
        return redirect(url_for("dashboard_bp.dashboard"))

    darajah_name = request.args.get("darajah")
    if not darajah_name:
        flash("No darajah selected.", "warning")
        return redirect(url_for("teacher_dashboard_bp.dashboard"))

    return redirect(url_for("teacher_dashboard_bp.dashboard", darajah_name=darajah_name))


@bp.route("/api/darajahs")
def api_darajahs():
    """API endpoint to get all darajahs"""
    if not session.get("logged_in"):
        return jsonify({"error": "Not authenticated"}), 401

    role = (session.get("role") or "").lower()
    if role != "admin":
        return jsonify({"error": "Admin access required"}), 403

    try:
        darajahs = get_all_darajahs()
        return jsonify({
            "success": True,
            "darajahs": darajahs,
            "count": len(darajahs)
        })
    except Exception as e:
        current_app.logger.error(f"API error getting darajahs: {e}")
        return jsonify({"error": str(e)}), 500


@bp.route("/search_student")
def search_student():
    """Search student within this darajah"""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role not in ("teacher", "admin"):
        return redirect(url_for("auth_bp.login"))

    if role == "teacher":
        darajah_name = session.get("darajah_name")
        darajah_label = darajah_name
    else:
        darajah_name = (
            request.args.get("darajah")
            or request.args.get("darajah_name")
            or session.get("darajah_name")
        )
        darajah_label = darajah_name or f"{session.get('username')} (Admin view)"

    if not darajah_name:
        flash("⚠️ No darajah selected.", "warning")
        return redirect(url_for("teacher_dashboard_bp.dashboard"))

    username = session.get("username")
    query = (request.args.get("q") or "").strip()
    if not query:
        return redirect(url_for("teacher_dashboard_bp.dashboard"))

    try:
        students_list = _get_all_students_in_darajah(darajah_name)
        
        results = []
        for student in students_list:
            tr_no = student.get("TRNumber", "")
            full_name = student.get("FullName", "")
            
            if query.lower() in str(tr_no).lower() or query.lower() in full_name.lower():
                results.append(student)
        
        parsed_darajah = _parse_darajah_name(darajah_name)
        
        for student in results:
            if "display_name" not in student:
                student["display_name"] = _format_student_display(
                    student.get("TRNumber"), 
                    student.get("FullName")
                )
        
        return render_template(
            "teacher_dashboard_search.html",
            darajah_name=darajah_label or f"{username} ({darajah_name})",
            parsed_darajah=parsed_darajah,
            query=query,
            results=results,
            OPAC_BASE=OPAC_BASE,
        )
    except Exception as e:
        current_app.logger.error(f"Error in search_student: {e}")
        flash(f"Error searching students: {e}", "danger")
        return redirect(url_for("teacher_dashboard_bp.dashboard"))


@bp.route("/download_report")
def download_report():
    """Unified download handler"""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    scope = request.args.get("scope", "")
    fmt = request.args.get("fmt", "pdf")

    if fmt != "pdf":
        flash("Only PDF downloads are supported.", "warning")
        return redirect(url_for("teacher_dashboard_bp.dashboard"))

    if scope.startswith("student:"):
        identifier = scope.split("student:")[-1]
        return redirect(
            url_for("teacher_dashboard_bp.download_student_pdf", identifier=identifier)
        )

    return download_darajah_pdf()


@bp.route("/download_darajah/pdf")
def download_darajah_pdf():
    """Download darajah PDF report"""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role not in ("teacher", "admin"):
        return redirect(url_for("auth_bp.login"))

    if role == "teacher":
        darajah_name = session.get("darajah_name")
        darajah_masool_name = session.get("username") or ""
    else:
        darajah_name = (
            request.args.get("darajah")
            or request.args.get("darajah_name")
            or session.get("darajah_name")
        )
        darajah_masool_name = session.get("username") or ""

    if not darajah_name:
        flash("⚠️ No darajah selected.", "warning")
        return redirect(url_for("teacher_dashboard_bp.dashboard"))

    students_list = _get_ay_student_stats(darajah_name)
    
    if not students_list:
        flash("⚠️ No data found for your darajah.", "warning")
        return redirect(url_for("teacher_dashboard_bp.dashboard"))
    
    df = pd.DataFrame(students_list)
    total_students = len(students_list)

    trend_labels, trend_values, trend_period_label = _darajah_ay_trend(darajah_name)
    total_issues_trend = int(sum(trend_values))
    avg_month_issues = (total_issues_trend / len(trend_values)) if trend_values else 0
    
    if trend_values:
        max_month_issues = max(trend_values)
        max_idx = trend_values.index(max_month_issues)
        max_month_label = trend_labels[max_idx]
    else:
        max_month_issues = 0
        max_month_label = "-"

    font_name = _ensure_font_registered()
    buffer = BytesIO()

    parsed_darajah = _parse_darajah_name(darajah_name)
    display_name = parsed_darajah["display"] or darajah_name
    
    pagesize = landscape(A4)
    doc = SimpleDocTemplate(
        buffer,
        pagesize=pagesize,
        leftMargin=1.5 * cm,
        rightMargin=1.5 * cm,
        topMargin=1.5 * cm,
        bottomMargin=1.5 * cm,
    )

    def on_page(canvas, doc):
        canvas.saveState()
        canvas.setFont(font_name, 8)
        page_num = canvas.getPageNumber()
        canvas.drawCentredString(
            doc.width / 2 + doc.leftMargin,
            doc.bottomMargin / 2,
            f"Page {page_num}"
        )
        canvas.restoreState()

    styles = getSampleStyleSheet()
    for key in ("Title", "Normal", "Heading2", "Heading3"):
        styles[key].fontName = font_name

    styles.add(
        ParagraphStyle(
            name="CenterTitle",
            alignment=1,
            fontName=font_name,
            fontSize=14,
            leading=18,
        )
    )

    S = lambda x: _shape_if_rtl(str(x) if x is not None else "-")

    elements = []

    # PAGE 1: DARAJAH SUMMARY
    logo_path = os.path.join(current_app.root_path, "static", "images", "logo.png")
    if os.path.exists(logo_path):
        img = Image(logo_path)
        img._restrictSize(4 * cm, 4 * cm)
        elements.append(img)
        elements.append(Spacer(1, 0.2 * cm))

    elements.append(
        Paragraph(S("Al-Jamea-tus-Saifiyah • Maktabat"), styles["CenterTitle"])
    )
    elements.append(Spacer(1, 0.1 * cm))
    elements.append(
        Paragraph(
            S(f"{display_name} – Library Report (Academic Year)"),
            styles["CenterTitle"],
        )
    )
    elements.append(Spacer(1, 0.1 * cm))

    today = date.today()
    hijri_date_str = _hijri_date_label(today)
    elements.append(Paragraph(hijri_date_str, styles["CenterTitle"]))
    elements.append(Spacer(1, 0.3 * cm))

    info_table_data = [
        ["Darajah", S(display_name)],
        ["Original Code", S(darajah_name)],
        ["Darajah Masool", S(darajah_masool_name)],
        ["Academic Year Period", S(trend_period_label)],
        ["Total Issues (AY)", str(total_issues_trend)],
        [
            "Maximum Issues in a Single Month",
            f"{max_month_issues} ({max_month_label})",
        ],
        ["Total Students with Books", str(total_students)],
    ]
    info_table = Table(info_table_data, colWidths=[5 * cm, 15 * cm])
    info_table.setStyle(
        TableStyle(
            [
                ("FONTNAME", (0, 0), (-1, -1), font_name),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
                ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#f3e3cf")),
            ]
        )
    )
    elements.append(info_table)
    elements.append(Spacer(1, 0.4 * cm))

    collections_by_student = {}
    for student in students_list:
        trno = student.get("TRNumber", "")
        if trno:
            collections_by_student[trno] = student.get("CollectionsUsed", "")

    students_list.sort(key=lambda x: x.get("Issues_AY", 0), reverse=True)

    data = [
        [
            "TR No",
            "Full Name",
            "Books Issued",
            "Overdues",
            "Fees Paid",
            "Collections",
        ]
    ]

    for student in students_list:
        trno = student.get("TRNumber", "")
        collections = collections_by_student.get(trno, "")
        
        data.append(
            [
                S(trno or "-"),
                S(student.get("FullName") or "-"),
                S(str(student.get("Issues_AY") or "0")),
                S(str(student.get("Overdues") or "0")),
                S(f"{float(student.get('FeesPaid_AY') or 0.0):.2f}"),
                S(collections[:30] + "..." if len(collections) > 30 else collections),
            ]
        )

    col_widths = [2.5 * cm, 5.5 * cm, 2.5 * cm, 2 * cm, 3 * cm, 4.5 * cm]

    table = Table(data, colWidths=col_widths, repeatRows=1)
    table.setStyle(
        TableStyle(
            [
                ("FONTNAME", (0, 0), (-1, -1), font_name),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
                ("ALIGNMENT", (2, 0), (4, -1), "CENTER"),
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#edd7b5")),
            ]
        )
    )
    elements.append(table)

    elements.append(PageBreak())

    # PAGE 2: TOP TITLES
    top_arabic = _darajah_top_titles_by_lang(darajah_name, "%Arabic%", limit=15)
    top_english = _darajah_top_titles_by_lang(darajah_name, "%English%", limit=15)

    elements.append(
        Paragraph(S("Top Arabic Titles (Academic Year)"), styles["Heading2"])
    )
    elements.append(Spacer(1, 0.2 * cm))

    if top_arabic:
        arabic_data = [["Title", "Times Issued", "Collections"]]
        for t in top_arabic:
            arabic_data.append(
                [
                    S(t.get("Title") or "-"),
                    S(str(t.get("Times_Issued") or "0")),
                    S(t.get("Collections") or "-"),
                ]
            )
        arabic_table = Table(
            arabic_data, colWidths=[10 * cm, 3 * cm, 7 * cm], repeatRows=1
        )
        arabic_table.setStyle(
            TableStyle(
                [
                    ("FONTNAME", (0, 0), (-1, -1), font_name),
                    ("FONTSIZE", (0, 0), (-1, -1), 9),
                    ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
                    ("ALIGNMENT", (1, 0), (1, -1), "CENTER"),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f3e3cf")),
                ]
            )
        )
        elements.append(arabic_table)
    else:
        elements.append(Paragraph(S("No Arabic titles issued in AY."), styles["Normal"]))

    elements.append(Spacer(1, 0.5 * cm))

    elements.append(
        Paragraph(S("Top English Titles (Academic Year)"), styles["Heading2"])
    )
    elements.append(Spacer(1, 0.2 * cm))

    if top_english:
        english_data = [["Title", "Times Issued", "Collections"]]
        for t in top_english:
            english_data.append(
                [
                    S(t.get("Title") or "-"),
                    S(str(t.get("Times_Issued") or "0")),
                    S(t.get("Collections") or "-"),
                ]
            )
        english_table = Table(
            english_data, colWidths=[10 * cm, 3 * cm, 7 * cm], repeatRows=1
        )
        english_table.setStyle(
            TableStyle(
                [
                    ("FONTNAME", (0, 0), (-1, -1), font_name),
                    ("FONTSIZE", (0, 0), (-1, -1), 9),
                    ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
                    ("ALIGNMENT", (1, 0), (1, -1), "CENTER"),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f3e3cf")),
                ]
            )
        )
        elements.append(english_table)
    else:
        elements.append(Paragraph(S("No English titles issued in AY."), styles["Normal"]))

    doc.build(elements, onFirstPage=on_page, onLaterPages=on_page)
    buffer.seek(0)

    filename = f"Darajah_Report_{darajah_name.replace(' ', '_')}_{today.strftime('%Y%m%d')}.pdf"
    return send_file(
        buffer,
        as_attachment=True,
        download_name=filename,
        mimetype="application/pdf",
    )


@bp.route("/download_student/pdf/<identifier>")
def download_student_pdf(identifier):
    """Download individual student PDF report"""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    student_info = get_student_info(identifier)
    if not student_info:
        flash("Student not found.", "danger")
        return redirect(url_for("teacher_dashboard_bp.dashboard"))

    role = (session.get("role") or "").lower()
    if role == "teacher":
        darajah_name = session.get("darajah_name")
        if not darajah_name:
            flash("No darajah selected.", "warning")
            return redirect(url_for("teacher_dashboard_bp.dashboard"))

        student_darajah = student_info.get("Class") or student_info.get("Darajah")
        if student_darajah != darajah_name:
            flash("Student does not belong to your darajah.", "danger")
            return redirect(url_for("teacher_dashboard_bp.dashboard"))

    font_name = _ensure_font_registered()
    buffer = BytesIO()

    pagesize = portrait(A4)
    doc = SimpleDocTemplate(
        buffer,
        pagesize=pagesize,
        leftMargin=2 * cm,
        rightMargin=2 * cm,
        topMargin=2 * cm,
        bottomMargin=2 * cm,
    )

    def on_page(canvas, doc):
        canvas.saveState()
        canvas.setFont(font_name, 8)
        page_num = canvas.getPageNumber()
        canvas.drawCentredString(
            doc.width / 2 + doc.leftMargin,
            doc.bottomMargin / 2,
            f"Page {page_num}"
        )
        canvas.restoreState()

    styles = getSampleStyleSheet()
    for key in ("Title", "Normal", "Heading2", "Heading3"):
        styles[key].fontName = font_name

    styles.add(
        ParagraphStyle(
            name="CenterTitle",
            alignment=1,
            fontName=font_name,
            fontSize=14,
            leading=18,
        )
    )

    S = lambda x: _shape_if_rtl(str(x) if x is not None else "-")

    elements = []

    logo_path = os.path.join(current_app.root_path, "static", "images", "logo.png")
    if os.path.exists(logo_path):
        img = Image(logo_path)
        img._restrictSize(3.5 * cm, 3.5 * cm)
        elements.append(img)
        elements.append(Spacer(1, 0.2 * cm))

    elements.append(
        Paragraph(S("Al-Jamea-tus-Saifiyah • Maktabat"), styles["CenterTitle"])
    )
    elements.append(Spacer(1, 0.1 * cm))
    elements.append(
        Paragraph(S("Individual Student Library Report"), styles["CenterTitle"])
    )
    elements.append(Spacer(1, 0.1 * cm))

    today = date.today()
    hijri_date_str = _hijri_date_label(today)
    elements.append(Paragraph(hijri_date_str, styles["CenterTitle"]))
    elements.append(Spacer(1, 0.4 * cm))

    info_table_data = [
        ["TR No", S(student_info.get("TRNo") or identifier)],
        ["Full Name", S(student_info.get("FullName") or "-")],
        ["Darajah/Class", S(student_info.get("Class") or student_info.get("Darajah") or "-")],
        ["Date of Birth", S(student_info.get("DateOfBirth") or "-")],
        ["Phone", S(student_info.get("Phone") or "-")],
        ["Email", S(student_info.get("Email") or "-")],
        ["Address", S(student_info.get("Address") or "-")],
        ["City", S(student_info.get("City") or "-")],
        ["Library Card No", S(student_info.get("CardNumber") or "-")],
    ]

    info_table = Table(info_table_data, colWidths=[5 * cm, 12 * cm])
    info_table.setStyle(
        TableStyle(
            [
                ("FONTNAME", (0, 0), (-1, -1), font_name),
                ("FONTSIZE", (0, 0), (-1, -1), 10),
                ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
                ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#f3e3cf")),
            ]
        )
    )
    elements.append(info_table)
    elements.append(Spacer(1, 0.6 * cm))

    borrowed_books = student_info.get("BorrowedBooks", [])
    if borrowed_books:
        elements.append(
            Paragraph(S("Currently Issued Books"), styles["Heading2"])
        )
        elements.append(Spacer(1, 0.2 * cm))

        book_data = [["Title", "Author", "Barcode", "Issued", "Due", "Collection"]]
        for book in borrowed_books:
            due_date = book.get("due_date") or book.get("date_due") or "-"
            book_data.append(
                [
                    S(book.get("title") or "-"),
                    S(book.get("author") or "-"),
                    S(book.get("barcode") or "-"),
                    S(_hijri_from_any(book.get("issuedate") or "-")),
                    S(_hijri_from_any(due_date)),
                    S(book.get("collection") or "-"),
                ]
            )

        book_table = Table(
            book_data,
            colWidths=[5 * cm, 3.5 * cm, 2.5 * cm, 2 * cm, 2 * cm, 2.5 * cm],
            repeatRows=1,
        )
        book_table.setStyle(
            TableStyle(
                [
                    ("FONTNAME", (0, 0), (-1, -1), font_name),
                    ("FONTSIZE", (0, 0), (-1, -1), 9),
                    ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
                    ("ALIGNMENT", (2, 0), (5, -1), "CENTER"),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#edd7b5")),
                ]
            )
        )
        elements.append(book_table)
    else:
        elements.append(
            Paragraph(S("No books currently issued."), styles["Normal"])
        )

    elements.append(Spacer(1, 0.6 * cm))

    issue_history = student_info.get("IssueHistory", [])
    if issue_history:
        elements.append(
            Paragraph(S("Recent Issue History (Last 20)"), styles["Heading2"])
        )
        elements.append(Spacer(1, 0.2 * cm))

        history_data = [["Title", "Issued", "Returned", "Collection"]]
        for issue in issue_history[:20]:
            history_data.append(
                [
                    S(issue.get("title") or "-"),
                    S(_hijri_from_any(issue.get("issuedate") or "-")),
                    S(_hijri_from_any(issue.get("returndate") or "-")),
                    S(issue.get("collection") or "-"),
                ]
            )

        history_table = Table(
            history_data,
            colWidths=[7 * cm, 3 * cm, 3 * cm, 4 * cm],
            repeatRows=1,
        )
        history_table.setStyle(
            TableStyle(
                [
                    ("FONTNAME", (0, 0), (-1, -1), font_name),
                    ("FONTSIZE", (0, 0), (-1, -1), 9),
                    ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
                    ("ALIGNMENT", (1, 0), (3, -1), "CENTER"),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#edd7b5")),
                ]
            )
        )
        elements.append(history_table)
    else:
        elements.append(
            Paragraph(S("No issue history available."), styles["Normal"])
        )

    doc.build(elements, onFirstPage=on_page, onLaterPages=on_page)
    buffer.seek(0)

    filename = f"Student_Report_{identifier}_{today.strftime('%Y%m%d')}.pdf"
    return send_file(
        buffer,
        as_attachment=True,
        download_name=filename,
        mimetype="application/pdf",
    )
