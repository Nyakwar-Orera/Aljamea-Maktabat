# routes/hod_dashboard.py - COMPLETE UPDATED VERSION WITH HIJRI SERVICE FUNCTIONS

from flask import (
    Blueprint,
    render_template,
    session,
    redirect,
    url_for,
    flash,
    current_app,
    request,
    send_file,
    jsonify
)
from routes.reports import darajah_report, marhala_report
from routes.students import get_student_info
from db_koha import get_koha_conn
from datetime import date, timedelta, datetime
from io import BytesIO
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.units import cm
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Table,
    TableStyle,
    Spacer,
    Image,
    PageBreak
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from services.exports import (
    _ensure_font_registered, 
    _shape_if_rtl, 
    dataframe_to_pdf_bytes
)
import os
import pandas as pd
import re
import traceback
import html
from collections import defaultdict

# Import the updated koha_queries as KQ - NO INDIVIDUAL FUNCTION IMPORTS
from services import koha_queries as KQ

bp = Blueprint("hod_dashboard_bp", __name__)

# --------------------------------------------------
# PROFESSIONAL CONSTANTS & CONFIGURATION
# --------------------------------------------------
MARHALA_TYPES = {
    "ACADEMIC": {
        "color": "#4CAF50",
        "icon": "graduation-cap",
        "description": "Educational departments for students"
    },
    "NON_ACADEMIC": {
        "color": "#2196F3",
        "icon": "users",
        "description": "Staff and support departments"
    },
    "LIBRARY": {
        "color": "#FF9800",
        "icon": "book",
        "description": "Library-related departments"
    },
    "ADMIN": {
        "color": "#9C27B0",
        "icon": "building",
        "description": "Administrative departments"
    }
}

PERFORMANCE_LEVELS = {
    "EXCELLENT": {"min": 80, "color": "#4CAF50", "label": "Excellent"},
    "GOOD": {"min": 60, "color": "#8BC34A", "label": "Good"},
    "AVERAGE": {"min": 40, "color": "#FFC107", "label": "Average"},
    "NEEDS_IMPROVEMENT": {"min": 20, "color": "#FF9800", "label": "Needs Improvement"},
    "POOR": {"min": 0, "color": "#F44336", "label": "Poor"}
}

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

def _generate_opac_url(biblio_id):
    """Generate OPAC URL for a book based on biblionumber."""
    try:
        opac_base = current_app.config.get("KOHA_OPAC_BASE_URL", "https://library-nairobi.jameasaifiyah.org")
        return f"{opac_base}/cgi-bin/koha/opac-detail.pl?biblionumber={biblio_id}"
    except:
        return f"https://library-nairobi.jameasaifiyah.org/cgi-bin/koha/opac-detail.pl?biblionumber={biblio_id}"

def _generate_student_url(identifier):
    """Generate URL for student details page."""
    return url_for('students.student', identifier=identifier)

# --------------------------------------------------
# PROFESSIONAL HELPER FUNCTIONS
# --------------------------------------------------
def _calculate_performance_score(issues_per_patron, engagement_rate, overdue_rate):
    """
    Calculate a comprehensive performance score (0-100).
    Higher is better.
    """
    # Normalize metrics
    issues_score = min(issues_per_patron * 10, 40)  # Max 40 points
    engagement_score = min(engagement_rate * 2, 40)  # Max 40 points
    overdue_penalty = min(overdue_rate * 0.5, 20)  # Max 20 point penalty
    
    score = issues_score + engagement_score - overdue_penalty
    return max(0, min(100, score))

def _get_performance_level(score):
    """Determine performance level based on score."""
    for level, config in PERFORMANCE_LEVELS.items():
        if score >= config["min"]:
            return level
    return "POOR"

def _format_currency(amount):
    """Format currency with KSH symbol."""
    try:
        amount = float(amount)
        return f"KSh {amount:,.2f}"
    except:
        return f"KSh {amount}"

def _get_marhala_type_icon(marhala_name, marhala_code=None):
    """Get appropriate icon for marhala type."""
    # First check by code
    if marhala_code:
        if marhala_code in ['S-CO', 'S-CGB', 'S-CGA', 'S-CT', 'S-DARS']:
            return "graduation-cap", "Academic"
        elif marhala_code in ['T', 'T-KG']:
            return "chalkboard-teacher", "Teaching Staff"
        elif marhala_code == 'L':
            return "book-reader", "Library Staff"
        elif marhala_code in ['S', 'HO', 'M-KG', 'PT']:
            return "user-tie", "Support Staff"
    
    # Fallback to name-based detection
    marhala_lower = marhala_name.lower()
    if any(word in marhala_lower for word in ['student', 'std', 'darajah', 'grade', 'form', 'year', 'class', 'collegiate', 'culture', 'dars']):
        return "graduation-cap", "Academic"
    elif any(word in marhala_lower for word in ['teacher', 'asateza', 'teaching', 'faculty']):
        return "chalkboard-teacher", "Teaching Staff"
    elif any(word in marhala_lower for word in ['library', 'maktabat']):
        return "book-reader", "Library Staff"
    elif any(word in marhala_lower for word in ['staff', 'employee', 'support']):
        return "user-tie", "Support Staff"
    elif any(word in marhala_lower for word in ['admin', 'administration']):
        return "building", "Administration"
    else:
        return "building", "Other"

def _get_time_period_label():
    """Get current time period label for reports using service function."""
    return KQ.get_hijri_date_label(date.today())

def _classify_darajah_info(darajah_name):
    """
    Classify darajah information from name.
    Returns dict with: gender, year, section, icon
    """
    if not darajah_name:
        return {
            "gender": "Mixed",
            "year": "",
            "section": "",
            "icon": "users"
        }
    
    name_lower = darajah_name.lower()
    
    # Determine gender
    if "boys" in name_lower or "بنين" in darajah_name:
        gender = "Boys"
        icon = "male"
    elif "girls" in name_lower or "بنات" in darajah_name:
        gender = "Girls"
        icon = "female"
    else:
        gender = "Mixed"
        icon = "users"
    
    # Extract year (look for numbers)
    year_match = re.search(r'(\d+)', darajah_name)
    year = year_match.group(1) if year_match else ""
    
    # Extract section (look for letters after numbers or section keywords)
    section = ""
    section_patterns = [
        r'Section\s+([A-Z])',
        r'Sec\s+([A-Z])',
        r'\s+([A-Z])$',  # Letter at end
        r'(\d+)\s*([A-Z])',  # Letter after number
    ]
    
    for pattern in section_patterns:
        section_match = re.search(pattern, darajah_name, re.IGNORECASE)
        if section_match:
            section = section_match.group(section_match.lastindex)
            break
    
    return {
        "gender": gender,
        "year": year,
        "section": section,
        "icon": icon
    }

# --------------------------------------------------
# HIJRI HELPERS - UPDATED TO USE SERVICE FUNCTIONS
# --------------------------------------------------
def _hijri_date_label(d: date) -> str:
    """Convert Gregorian date to Hijri date label using service function."""
    return KQ.get_hijri_date_label(d)

def _hijri_month_year_label(d: date) -> str:
    """Get Hijri month and year label for charts using service function."""
    return KQ.get_hijri_month_year_label(d)

# --------------------------------------------------
# ACADEMIC YEAR HELPER
# --------------------------------------------------
def get_academic_year_period(hijri_year=None):
    """Get formatted Academic Year period in Hijri using service function."""
    start, end = KQ.get_ay_bounds(hijri_year)
    
    if not start or not end:
        return "Academic Year not started yet"
    
    return f"{KQ.get_hijri_date_label(start)} to {KQ.get_hijri_date_label(end)}"

# ─────────────────────────────────────────────────────────────
# FIXED: GET ALL MARHALAS FOR HOD - CORRECTED DATABASE SCHEMA
# ─────────────────────────────────────────────────────────────
def get_all_marhalas_for_hod(hijri_year=None):
    """Get all distinct marhalas from Koha categories for HOD selection - FIXED VERSION."""
    conn = get_koha_conn()
    cur = conn.cursor(dictionary=True)  # Add dictionary=True
    
    try:
        # First, get all categories
        cur.execute("""
            SELECT 
                c.categorycode,
                c.description,
                c.enrolmentperiod,
                COUNT(DISTINCT b.borrowernumber) as member_count
            FROM categories c
            LEFT JOIN borrowers b ON c.categorycode = b.categorycode
            WHERE c.description IS NOT NULL 
                AND c.description != ''
            GROUP BY c.categorycode, c.description, c.enrolmentperiod
            ORDER BY c.description
        """)
        
        rows = cur.fetchall()
        marhalas = []
        
        # Get specified AY bounds for stats
        start, end = KQ.get_ay_bounds(hijri_year)
        
        # Get academic and non-academic codes
        academic_codes = KQ.get_academic_marhalas()
        non_academic_codes = KQ.get_non_academic_marhalas()
        
        for row in rows:
            categorycode = row["categorycode"]
            description = row["description"]
            enrolmentperiod = row["enrolmentperiod"]
            member_count = row["member_count"] or 0
            
            # Get detailed stats for this category
            cur.execute("""
                SELECT 
                    COUNT(DISTINCT b.borrowernumber) as active_borrowers,
                    COUNT(DISTINCT CASE 
                        WHEN trno.attribute IS NOT NULL AND trno.attribute != '' 
                        THEN trno.attribute END) as borrowers_with_tr
                FROM borrowers b
                LEFT JOIN borrower_attributes trno
                    ON trno.borrowernumber = b.borrowernumber
                    AND trno.code = 'TRNO'
                WHERE b.categorycode = %s
                    AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                    AND (b.debarred IS NULL OR b.debarred = 0)
                    AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            """, (categorycode,))
            
            stats_row = cur.fetchone()
            active_borrowers = stats_row["active_borrowers"] if stats_row else 0
            borrowers_with_tr = stats_row["borrowers_with_tr"] if stats_row else 0
            
            # Get AY issues if AY is active
            ay_issues = 0
            if start:
                cur.execute("""
                    SELECT COUNT(*) as ay_issues
                    FROM statistics s
                    JOIN borrowers b ON s.borrowernumber = b.borrowernumber
                    WHERE s.type = 'issue'
                        AND DATE(s.`datetime`) BETWEEN %s AND %s
                        AND b.categorycode = %s
                """, (start, end, categorycode))
                issues_row = cur.fetchone()
                ay_issues = issues_row["ay_issues"] if issues_row else 0
            
            # Get currently issued books
            cur.execute("""
                SELECT COUNT(*) as currently_issued
                FROM issues i
                JOIN borrowers b ON i.borrowernumber = b.borrowernumber
                WHERE b.categorycode = %s
                    AND i.returndate IS NULL
            """, (categorycode,))
            issued_row = cur.fetchone()
            currently_issued = issued_row["currently_issued"] if issued_row else 0
            
            # Get overdue books
            cur.execute("""
                SELECT COUNT(*) as overdues
                FROM issues i
                JOIN borrowers b ON i.borrowernumber = b.borrowernumber
                WHERE b.categorycode = %s
                    AND i.date_due < CURDATE()
                    AND i.returndate IS NULL
            """, (categorycode,))
            overdue_row = cur.fetchone()
            overdues = overdue_row["overdues"] if overdue_row else 0
            
            # Get total fees paid in AY
            ay_fees = 0.0
            if start:
                cur.execute("""
                    SELECT COALESCE(SUM(
                        CASE
                          WHEN a.credit_type_code='PAYMENT'
                               AND (a.status IS NULL OR a.status <> 'VOID')
                               AND DATE(a.`date`) BETWEEN %s AND %s
                          THEN -a.amount ELSE 0 END
                    ),0) as ay_fees
                    FROM accountlines a
                    JOIN borrowers b ON a.borrowernumber = b.borrowernumber
                    WHERE b.categorycode = %s
                """, (start, end, categorycode))
                fees_row = cur.fetchone()
                ay_fees = float(fees_row["ay_fees"] if fees_row else 0)
            
            # Get display name based on marhala type
            if categorycode in non_academic_codes:
                display_name = get_non_academic_marhala_display_name(categorycode)
            else:
                display_name = description.strip()
            
            # FIXED: Determine marhala type based on category code or description
            # Academic marhalas (including S-CO, S-DARS)
            if categorycode in ['S-CO', 'S-CGB', 'S-CGA', 'S-CT', 'S-DARS']:
                marhala_type = "Academic"
                icon = "graduation-cap"
                color = MARHALA_TYPES["ACADEMIC"]["color"]
            
            # Teacher categories (T, T-KG)
            elif categorycode in ['T', 'T-KG']:
                marhala_type = "Teaching Staff"
                icon = "chalkboard-teacher"
                color = MARHALA_TYPES["NON_ACADEMIC"]["color"]
            
            # Library
            elif categorycode == 'L':
                marhala_type = "Library Staff"
                icon = "book-reader"
                color = MARHALA_TYPES["LIBRARY"]["color"]
            
            # Other non-academic
            elif categorycode in ['S', 'HO', 'M-KG', 'PT']:
                marhala_type = "Support Staff"
                icon = "user-tie"
                color = MARHALA_TYPES["NON_ACADEMIC"]["color"]
            
            # Check by description if not matched by code
            else:
                desc_lower = description.lower()
                if any(word in desc_lower for word in ['student', 'std', 'darajah', 'grade', 'form', 'year', 'class', 'collegiate', 'culture', 'dars']):
                    marhala_type = "Academic"
                    icon = "graduation-cap"
                    color = MARHALA_TYPES["ACADEMIC"]["color"]
                elif any(word in desc_lower for word in ['faculty', 'staff', 'teacher', 'asateza', 'employee', 'teaching']):
                    marhala_type = "Teaching Staff"
                    icon = "chalkboard-teacher"
                    color = MARHALA_TYPES["NON_ACADEMIC"]["color"]
                elif 'library' in desc_lower:
                    marhala_type = "Library Staff"
                    icon = "book-reader"
                    color = MARHALA_TYPES["LIBRARY"]["color"]
                elif any(word in desc_lower for word in ['admin', 'administration', 'management']):
                    marhala_type = "Administration"
                    icon = "building"
                    color = MARHALA_TYPES["ADMIN"]["color"]
                else:
                    marhala_type = "Other"
                    icon = "building"
                    color = "#757575"
            
            marhalas.append({
                "code": categorycode,
                "name": description.strip(),
                "display": display_name,
                "type": marhala_type,
                "icon": icon,
                "color": color,
                "enrolment_period": enrolmentperiod,
                "total_members": member_count,
                "active_borrowers": active_borrowers,
                "borrowers_with_tr": borrowers_with_tr,
                "ay_issues": ay_issues,
                "currently_issued": currently_issued,
                "overdues": overdues,
                "ay_fees": ay_fees,
                "has_data": ay_issues > 0 or active_borrowers > 0 or currently_issued > 0
            })
        
        # Sort marhalas: Academic first, then by type
        type_order = {
            "Academic": 0,
            "Teaching Staff": 1,
            "Library Staff": 2,
            "Support Staff": 3,
            "Administration": 4,
            "Other": 5
        }
        
        marhalas.sort(key=lambda x: (
            type_order.get(x["type"], 99), 
            -x["ay_issues"]
        ))
        
        return marhalas
        
    except Exception as e:
        current_app.logger.error(f"Error getting all marhalas: {e}")
        traceback.print_exc()
        return []
    
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

# ─────────────────────────────────────────────────────────────
# FIXED: GET ACCURATE MARHALA STATS
# ─────────────────────────────────────────────────────────────
def _get_accurate_marhala_stats(marhala_code, hijri_year=None):
    """
    Get accurate marhala statistics using consistent logic.
    Returns: (total_borrowers, active_borrowers, ay_issues, ay_fees, currently_issued, overdues)
    """
    try:
        start, end = KQ.get_ay_bounds(hijri_year)
        conn = get_koha_conn()
        cur = conn.cursor(dictionary=True)
        
        # Get accurate borrower counts
        cur.execute(
            """
            SELECT 
                COUNT(DISTINCT b.borrowernumber) as total_borrowers,
                COUNT(DISTINCT CASE 
                    WHEN (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                        AND (b.debarred IS NULL OR b.debarred = 0)
                        AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
                        AND trno.attribute IS NOT NULL AND trno.attribute != ''
                    THEN trno.attribute END) as borrowers_with_tr
            FROM borrowers b
            LEFT JOIN borrower_attributes trno
                 ON trno.borrowernumber = b.borrowernumber
                AND trno.code = 'TRNO'
            WHERE b.categorycode = %s
            """,
            (marhala_code,)
        )
        borrower_row = cur.fetchone()
        total_borrowers = borrower_row["total_borrowers"] if borrower_row else 0
        active_borrowers = borrower_row["borrowers_with_tr"] if borrower_row else 0
        
        # AY Issues count (Students who issued at least one book)
        ay_issues = 0
        if start:
            cur.execute(
                """
                SELECT COUNT(DISTINCT b.borrowernumber) as ay_issues
                FROM statistics s
                JOIN borrowers b ON s.borrowernumber = b.borrowernumber
                WHERE s.type = 'issue'
                  AND DATE(s.`datetime`) BETWEEN %s AND %s
                  AND b.categorycode = %s
                  AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                  AND (b.debarred IS NULL OR b.debarred = 0)
                  AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
                """,
                (start, end, marhala_code)
            )
            issues_row = cur.fetchone()
            ay_issues = issues_row["ay_issues"] if issues_row else 0
        
        # AY Fees paid
        ay_fees = 0.0
        if start:
            cur.execute(
                """
                SELECT COALESCE(SUM(
                    CASE
                      WHEN a.credit_type_code='PAYMENT'
                           AND (a.status IS NULL OR a.status <> 'VOID')
                           AND DATE(a.`date`) BETWEEN %s AND %s
                      THEN -a.amount ELSE 0 END
                ),0) as ay_fees
                FROM accountlines a
                JOIN borrowers b ON a.borrowernumber = b.borrowernumber
                WHERE b.categorycode = %s
                  AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                  AND (b.debarred IS NULL OR b.debarred = 0)
                  AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
                """,
                (start, end, marhala_code)
            )
            fees_row = cur.fetchone()
            ay_fees = float(fees_row["ay_fees"] if fees_row else 0)
        
        # Currently issued
        cur.execute(
            """
            SELECT COUNT(*) as currently_issued
            FROM issues i
            JOIN borrowers b ON i.borrowernumber = b.borrowernumber
            WHERE b.categorycode = %s
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
              AND i.returndate IS NULL
            """,
            (marhala_code,)
        )
        issued_row = cur.fetchone()
        currently_issued = issued_row["currently_issued"] if issued_row else 0
        
        # Overdues
        cur.execute(
            """
            SELECT COUNT(*) as overdues
            FROM issues i
            JOIN borrowers b ON i.borrowernumber = b.borrowernumber
            WHERE b.categorycode = %s
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
              AND i.date_due < CURDATE()
              AND i.returndate IS NULL
            """,
            (marhala_code,)
        )
        overdues_row = cur.fetchone()
        overdues = overdues_row["overdues"] if overdues_row else 0
        
        return total_borrowers, active_borrowers, ay_issues, ay_fees, currently_issued, overdues
        
    except Exception as e:
        current_app.logger.error(f"Error getting marhala stats: {e}")
        traceback.print_exc()
        return 0, 0, 0, 0.0, 0, 0
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

def _get_marhala_recent_activity(marhala_code, limit=10):
    """Get recent borrowing items for a specific marhala."""
    try:
        conn = get_koha_conn()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT 
                CONCAT(COALESCE(b.surname, ''), ' ', COALESCE(b.firstname, '')) as StudentName,
                trno.attribute as ITS,
                COALESCE(std.attribute, b.branchcode) as Darajah,
                bib.title as BookTitle,
                DATE_FORMAT(s.datetime, '%d-%b-%Y %H:%i') as ActionDate,
                CASE WHEN s.type = 'issue' THEN 'Issue' ELSE 'Return' END as ActionType
            FROM statistics s
            JOIN borrowers b ON s.borrowernumber = b.borrowernumber
            LEFT JOIN borrower_attributes trno ON trno.borrowernumber = b.borrowernumber AND trno.code = 'TRNO'
            LEFT JOIN borrower_attributes std ON std.borrowernumber = b.borrowernumber AND std.code IN ('Class','STD','CLASS','DAR')
            JOIN items it ON s.itemnumber = it.itemnumber
            JOIN biblio bib ON it.biblionumber = bib.biblionumber
            WHERE b.categorycode = %s
            ORDER BY s.datetime DESC
            LIMIT %s
        """, (marhala_code, limit))
        rows = cur.fetchall()
        
        # Cleanup names
        for row in rows:
            row["StudentName"] = _clean_student_name(row["StudentName"])
            if not row["ITS"]:
                row["ITS"] = "N/A"
            if not row["Darajah"]:
                row["Darajah"] = "N/A"
            if len(row["BookTitle"]) > 40:
                row["BookTitle"] = row["BookTitle"][:37] + "..."
                
        return rows
    except Exception as e:
        current_app.logger.error(f"Error getting marhala activity: {e}")
        return []
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

def _get_marhala_overdue_books(marhala_code, limit=20):
    """Get current overdue books for a specific marhala."""
    try:
        conn = get_koha_conn()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT 
                CONCAT(COALESCE(b.surname, ''), ' ', COALESCE(b.firstname, '')) as StudentName,
                COALESCE(std.attribute, b.branchcode) as Darajah,
                bib.title as BookTitle,
                DATEDIFF(CURDATE(), i.date_due) as DaysOverdue,
                COALESCE((SELECT SUM(amount) FROM accountlines WHERE borrowernumber = b.borrowernumber), 0) as CurrentFee
            FROM issues i
            JOIN borrowers b ON i.borrowernumber = b.borrowernumber
            LEFT JOIN borrower_attributes std ON std.borrowernumber = b.borrowernumber AND std.code IN ('Class','STD','CLASS','DAR')
            JOIN items it ON i.itemnumber = it.itemnumber
            JOIN biblio bib ON it.biblionumber = bib.biblionumber
            WHERE b.categorycode = %s AND i.date_due < CURDATE() AND i.returndate IS NULL
            ORDER BY DaysOverdue DESC
            LIMIT %s
        """, (marhala_code, limit))
        rows = cur.fetchall()
        
        # Cleanup names
        for row in rows:
            row["StudentName"] = _clean_student_name(row["StudentName"])
            if not row["Darajah"]:
                row["Darajah"] = "N/A"
                
        return rows
    except Exception as e:
        current_app.logger.error(f"Error getting marhala overdues: {e}")
        return []
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

# --------------------------------------------------
# FIXED: GET MARHALA TOP TITLES
# --------------------------------------------------
def get_marhala_top_titles(marhala_code: str, lang_code: str, limit: int = 10, hijri_year=None):
    """Get marhala-scoped top titles in Academic Year by language with OPAC links."""
    start, end = KQ.get_ay_bounds(hijri_year)
    
    if not start:
        return []
    
    try:
        conn = get_koha_conn()
        cur = conn.cursor(dictionary=True)
        lang_like = f"{lang_code}%"
        
        # Try to get category description
        cur.execute("SELECT description FROM categories WHERE categorycode = %s", (marhala_code,))
        category_row = cur.fetchone()
        category_desc = category_row["description"] if category_row else marhala_code
        
        # First, let's see if there are any issues for this marhala
        cur.execute("""
            SELECT COUNT(*) as total_issues
            FROM statistics s
            JOIN borrowers b ON s.borrowernumber = b.borrowernumber
            WHERE s.type = 'issue'
                AND DATE(s.`datetime`) BETWEEN %s AND %s
                AND b.categorycode = %s
        """, (start, end, marhala_code))
        
        total_issues_row = cur.fetchone()
        total_issues = total_issues_row["total_issues"] if total_issues_row else 0
        
        if total_issues == 0:
            # No issues for this marhala
            return []
        
        # Now get top titles
        cur.execute(
            """
            SELECT
                bib.biblionumber AS Biblio_ID,
                bib.title        AS Title,
                GROUP_CONCAT(DISTINCT it.ccode ORDER BY it.ccode SEPARATOR ', ') AS Collections,
                COUNT(*) AS Times_Issued
            FROM statistics s
            JOIN borrowers b ON s.borrowernumber = b.borrowernumber
            JOIN items it ON s.itemnumber = it.itemnumber
            JOIN biblio bib ON it.biblionumber = bib.biblionumber
            JOIN biblio_metadata bmd ON bib.biblionumber = bmd.biblionumber
            WHERE s.type = 'issue'
                AND DATE(s.`datetime`) BETWEEN %s AND %s
                AND ExtractValue(
                      bmd.metadata,
                      '//datafield[@tag="041"]/subfield[@code="a"]'
                    ) LIKE %s
                AND b.categorycode = %s
            GROUP BY bib.biblionumber, bib.title
            ORDER BY Times_Issued DESC
            LIMIT %s;
            """,
            (start, end, lang_like, marhala_code, int(limit)),
        )
        
        rows = cur.fetchall()
        language_label = "Arabic" if lang_code.startswith("ar") else "English"
        
        for r in rows:
            r["Language"] = language_label
            r["Title"] = _clean_title(r.get("Title", ""))
            r["OPAC_URL"] = _generate_opac_url(r.get("Biblio_ID", ""))
        
        return rows
        
    except Exception as e:
        current_app.logger.error(f"Error getting marhala top titles for {marhala_code} ({lang_code}): {e}")
        return []
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

# --------------------------------------------------
# FIXED: GET MARHALA AY TREND
# --------------------------------------------------
def get_marhala_ay_trend(marhala_code: str, hijri_year=None):
    """Get monthly issue trend for a marhala in the specific AY."""
    start, end = KQ.get_ay_bounds(hijri_year)
    
    if not start:
        return ["—"], [0], ""
    
    try:
        labels, values = KQ.get_ay_trend_data(marhala_code=marhala_code, hijri_year=hijri_year)
        
        last_month = min(date.today(), end).replace(day=1)
        period_label = f"{KQ.get_hijri_month_year_label(start)} – {KQ.get_hijri_month_year_label(last_month)}"
        
        return labels, values, period_label
        
    except Exception as e:
        current_app.logger.error(f"Error getting marhala trend for {marhala_code}: {e}")
        return [], [], ""

# --------------------------------------------------
# FIXED: GET DARAJAH BREAKDOWN
# --------------------------------------------------
def get_darajah_breakdown(marhala_code: str):
    """Get darajah breakdown within a marhala."""
    start, end = KQ.get_ay_bounds()
    
    if not start:
        return ["—"], [0]
    
    try:
        conn = get_koha_conn()
        cur = conn.cursor(dictionary=True)
        
        sql = """
            SELECT 
                COALESCE(std.attribute, b.branchcode, 'Unknown') AS darajah_name, 
                COUNT(*) AS cnt
            FROM statistics s
            JOIN borrowers b ON s.borrowernumber = b.borrowernumber
            LEFT JOIN borrower_attributes std
                   ON std.borrowernumber = b.borrowernumber
                  AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
            WHERE s.type='issue'
              AND b.categorycode = %s
              AND DATE(s.`datetime`) BETWEEN %s AND %s
            GROUP BY darajah_name
            ORDER BY cnt DESC
            LIMIT 15
        """
        
        cur.execute(sql, (marhala_code, start, end))
        rows = cur.fetchall()
        
        # Filter out None or empty darajah names
        filtered_rows = [(r["darajah_name"], r["cnt"]) for r in rows if r["darajah_name"] and r["darajah_name"] != 'Unknown' and r["darajah_name"] != '']
        
        labels = [r[0] for r in filtered_rows] or ["—"]
        values = [int(r[1]) for r in filtered_rows] or [0]
        return labels, values
        
    except Exception as e:
        current_app.logger.error(f"Error getting darajah breakdown for {marhala_code}: {e}")
        return ["—"], [0]
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

# --------------------------------------------------
# FIXED: GET TOP STUDENTS IN MARHALA
# --------------------------------------------------
def get_top_students_in_marhala(marhala_code: str, limit: int = 10):
    """Get top students in a marhala based on Academic Year issues."""
    start, end = KQ.get_ay_bounds()
    if not start or not marhala_code:
        return []

    try:
        conn = get_koha_conn()
        cur = conn.cursor(dictionary=True)

        cur.execute("""
            SELECT 
                b.borrowernumber,
                trno.attribute AS TRNumber,
                CASE 
                    WHEN (b.surname IS NOT NULL AND b.surname != '' AND b.surname != 'None')
                         AND (b.firstname IS NOT NULL AND b.firstname != '' AND b.firstname != 'None')
                    THEN CONCAT(b.surname, ' ', b.firstname)
                    WHEN (b.surname IS NOT NULL AND b.surname != '' AND b.surname != 'None')
                    THEN b.surname
                    WHEN (b.firstname IS NOT NULL AND b.firstname != '' AND b.firstname != 'None')
                    THEN b.firstname
                    ELSE CONCAT('Student #', b.cardnumber)
                END AS FullName,
                COALESCE(std.attribute, b.branchcode) AS Darajah,
                COUNT(*) AS Issues_AY,
                GROUP_CONCAT(
                    DISTINCT it.ccode 
                    ORDER BY it.ccode 
                    SEPARATOR ', '
                ) AS Collections_AY
            FROM statistics s
            JOIN borrowers b ON b.borrowernumber = s.borrowernumber
            LEFT JOIN borrower_attributes std
                 ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
            LEFT JOIN borrower_attributes trno
                 ON trno.borrowernumber = b.borrowernumber
                AND trno.code = 'TRNO'
            JOIN items it ON s.itemnumber = it.itemnumber
            WHERE s.type = 'issue'
              AND DATE(s.`datetime`) BETWEEN %s AND %s
              AND b.categorycode = %s
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            GROUP BY 
                b.borrowernumber,
                b.surname,
                b.firstname,
                b.cardnumber,
                trno.attribute,
                std.attribute,
                b.branchcode
            ORDER BY Issues_AY DESC
            LIMIT %s;
        """, (start, end, marhala_code, limit))

        rows = cur.fetchall()

        # Final cleanup & normalization with HTML safety
        for row in rows:
            # Clean and format student name
            name = row.get("FullName")
            if not name or str(name).strip() == "" or str(name).lower() == "none":
                row["FullName"] = f"Student #{row.get('TRNumber') or 'Unknown'}"
            else:
                row["FullName"] = _clean_student_name(name)
            
            # Create DisplayName for HOD View parity (TR - Name)
            tr_val = str(row.get("TRNumber") or "").strip()
            if tr_val and row["FullName"] != "Student":
                row["DisplayName"] = f"TR {tr_val} - {row['FullName']}"
            else:
                row["DisplayName"] = row["FullName"]
            
            # Clean TR number
            if row.get("TRNumber"):
                row["TRNumber"] = str(row["TRNumber"]).strip()
            
            # Generate student URL
            tr_number = row.get("TRNumber")
            if tr_number:
                row["StudentURL"] = _generate_student_url(tr_number)
            else:
                row["StudentURL"] = "#"
            
            # Create display name with link
            display_name = _format_student_display(tr_number, row["FullName"])
            row["DisplayName"] = display_name
            
            # Normalize darajah (Asateza)
            if row.get("Darajah") and "AJSN" in str(row["Darajah"]).upper():
                row["Darajah"] = "Asateza"

        return rows

    except Exception as e:
        current_app.logger.error(f"Error getting top students in marhala {marhala_code}: {e}")
        traceback.print_exc()
        return []

    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

# --------------------------------------------------
# FIXED: GET NON-ACADEMIC MARHALA DISPLAY NAME
# --------------------------------------------------
def get_non_academic_marhala_display_name(category_code):
    """Get display name for non-academic marhala based on category code."""
    display_names = {
        'T': 'Teaching Staff',
        'T-KG': 'Asateza Kiram',
        'S': 'Support Staff',
        'L': 'Library Staff',
        'HO': 'Sighat ul Jamea',
        'M-KG': 'Mukhayyam Khidmat Guzar',
        'PT': 'Patron'
    }
    return display_names.get(category_code, category_code)

# --------------------------------------------------
# FIXED: GET NON-ACADEMIC MARHALA CODE FROM DISPLAY
# --------------------------------------------------
def get_non_academic_marhala_code_from_display(display_name):
    """Get category code from display name for non-academic marhalas."""
    code_map = {
        'Asateza': 'T',
        'Asateza Kiram': 'T-KG',
        'Staff': 'S',
        'Library Staff': 'L',
        'Sighat ul Jamea': 'HO',
        'Mukhayyam Khidmat Guzar': 'M-KG',
        'Patron': 'PT'
    }
    return code_map.get(display_name, display_name)

# --------------------------------------------------
# FIXED: IS ACADEMIC MARHALA
# --------------------------------------------------
def is_academic_marhala(category_code):
    """Check if a category code is academic."""
    academic_codes = KQ.get_academic_marhalas()
    return category_code in academic_codes

# --------------------------------------------------
# FIXED: IS NON-ACADEMIC MARHALA
# --------------------------------------------------
def is_non_academic_marhala(category_code):
    """Check if a category code is non-academic."""
    non_academic_codes = KQ.get_non_academic_marhalas()
    return category_code in non_academic_codes

# --------------------------------------------------
# FIXED: GET MARHALA DISPLAY NAME
# --------------------------------------------------
def get_marhala_display_name(category_code, category_description):
    """Get appropriate display name for marhala based on type."""
    if is_academic_marhala(category_code):
        return category_description.strip()
    elif is_non_academic_marhala(category_code):
        return get_non_academic_marhala_display_name(category_code)
    else:
        return category_description.strip()

# --------------------------------------------------
# PROFESSIONAL DASHBOARD ROUTE - UPDATED
# --------------------------------------------------
@bp.route("/")
def dashboard():
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role not in ("hod", "admin"):
        return redirect(url_for("auth_bp.login"))

    if role == "hod":
        marhala_name = session.get("marhala_name") or session.get("department_name")
    else:
        marhala_name = request.args.get("marhala") or session.get("marhala_name") or session.get("department_name")

    username = session.get("username")

    # Get Academic Year from session
    selected_ay = session.get("selected_ay", "current")
    hijri_year = None
    if selected_ay != "current":
        try:
            hijri_year = int(selected_ay)
        except (ValueError, TypeError):
            selected_ay = "current"

    def _render_empty_dashboard(extra_message=None):
        if extra_message:
            flash(extra_message, "warning")

        all_marhalas = []
        if role == "admin":
            all_marhalas = get_all_marhalas_for_hod(hijri_year=hijri_year)

        available_years = KQ.get_available_academic_years()
        today = date.today()
        hijri_today = _hijri_date_label(today)
        ay_period = get_academic_year_period(hijri_year=hijri_year)

        return render_template(
            "hod_dashboard.html",
            marhala_name=marhala_name or "",
            marhala_display_name="",
            username=username or "",
            total_borrowers=0,
            active_borrowers=0,
            total_issues_ay=0,
            total_fees_ay=0.0,
            currently_issued=0,
            overdues_now=0,
            trend_labels=[],
            trend_values=[],
            trend_period_label="",
            ay_total_issues_trend=0,
            avg_issues_month=0.0,
            peak_month_label="—",
            darajah_labels=[],
            darajah_values=[],
            top_darajah_name="—",
            top_darajah_issues=0,
            top_arabic=[],
            top_english=[],
            top_students=[],
            summary_table=[],
            total_darajahs=0,
            engagement_index=0.0,
            overdue_rate=0.0,
            today_hijri=hijri_today,
            today_greg=today.strftime("%d %B %Y"),
            ay_period_label=ay_period,
            all_marhalas=all_marhalas,
            is_admin=role == "admin",
            selected_ay=selected_ay,
            available_years=available_years,
            darajahs_in_marhala=[],
            darajah_group_labels=[],
            darajah_group_values=[],
            academic_departments=[],
            other_departments=[],
            currently_issued_data={"marhalas": [], "total_currently_issued": 0},
            # Professional enhancements
            performance_score=0,
            performance_level="N/A",
            performance_color="#9E9E9E",
            key_insights=[],
            marhala_type="Unknown",
            marhala_icon="building",
            marhala_color="#9E9E9E",
            stats_available=False,
            time_period=_get_time_period_label(),
            format_currency=_format_currency
        )
    
    if not marhala_name:
        if role == "admin":
            return redirect(url_for("hod_dashboard_bp.marhala_explorer"))
        else:
            return _render_empty_dashboard("⚠️ Your account is not linked to any marhala.")

    # Get marhala information
    all_marhalas_list = get_all_marhalas_for_hod(hijri_year=hijri_year)
    
    # Find the selected marhala
    selected_marhala = None
    for marhala in all_marhalas_list:
        if marhala["code"] == marhala_name or marhala["name"] == marhala_name:
            selected_marhala = marhala
            break
    
    if not selected_marhala:
        # Try to find by partial match
        for marhala in all_marhalas_list:
            if marhala_name.lower() in marhala["name"].lower() or marhala["name"].lower() in marhala_name.lower():
                selected_marhala = marhala
                flash(f"⚠️ Marhala mapped to: {marhala['name']}", "info")
                marhala_name = marhala["code"]
                break
    
    if not selected_marhala:
        flash(f"⚠️ Marhala '{marhala_name}' not found in Koha.", "warning")
        return _render_empty_dashboard()

    marhala_code = selected_marhala["code"]
    marhala_display_name = selected_marhala["display"]  # Use display name
    marhala_type = selected_marhala["type"]
    marhala_icon = selected_marhala["icon"]
    marhala_color = selected_marhala["color"]

    # Get detailed stats
    total_borrowers, active_borrowers, total_issues_ay, total_fees_ay, currently_issued, overdues_now = _get_accurate_marhala_stats(marhala_code, hijri_year=hijri_year)

    # Check if we have any data
    stats_available = total_issues_ay > 0 or currently_issued > 0 or active_borrowers > 0

    if not stats_available:
        flash(f"⚠️ No borrowing activity found for {marhala_display_name} in the current Academic Year.", "info")

    # Get trend data
    trend_labels, trend_values, trend_period_label = get_marhala_ay_trend(marhala_code, hijri_year=hijri_year)

    # Calculate trend statistics
    if trend_values:
        ay_total_issues_trend = int(sum(trend_values))
        avg_issues_month = round(ay_total_issues_trend / len(trend_values), 1) if trend_values else 0
        max_val = max(trend_values) if trend_values else 0
        max_idx = trend_values.index(max_val) if trend_values else 0
        peak_month_label = trend_labels[max_idx] if trend_labels else "—"
    else:
        ay_total_issues_trend = 0
        avg_issues_month = 0.0
        peak_month_label = "—"

    # Get darajah breakdown
    darajah_labels, darajah_values = get_darajah_breakdown(marhala_code)
    if darajah_values and darajah_labels:
        top_darajah_name = darajah_labels[0]
        top_darajah_issues = darajah_values[0]
    else:
        top_darajah_name = "—"
        top_darajah_issues = 0

    # Get top titles with OPAC links (only if we have data)
    top_arabic = []
    top_english = []
    if stats_available:
        top_arabic = get_marhala_top_titles(marhala_code, lang_code="ar", limit=5, hijri_year=hijri_year)
        top_english = get_marhala_top_titles(marhala_code, lang_code="eng", limit=5, hijri_year=hijri_year)

    # Get darajahs in marhala using koha_queries function
    darajahs_in_marhala = []
    darajah_stats = []  # For detailed table
    try:
        darajahs_data = KQ.get_darajah_summary_by_marhala(marhala_display_name)
        for darajah in darajahs_data:
            darajah_name = darajah.get("Darajah", "")
            if darajah_name and darajah_name != "Unknown":
                # Classify darajah info for cards
                darajah_info = _classify_darajah_info(darajah_name)
                
                darajah_entry = {
                    "darajah_name": darajah_name,
                    "name": darajah_name,
                    "display": darajah_name,
                    "books_issued": darajah.get("BooksIssued", 0),
                    "active_students": darajah.get("ActiveStudents", 0),
                    "issues_per_student": darajah.get("IssuesPerStudent", 0),
                    "collections": darajah.get("Collections", ""),
                    "marhala": darajah.get("Marhala", ""),
                    # Card display fields
                    "gender": darajah_info["gender"],
                    "year": darajah_info["year"],
                    "section": darajah_info["section"],
                    "icon": darajah_info["icon"]
                }
                darajahs_in_marhala.append(darajah_entry)
                
                # Also add to stats for table
                darajah_stats.append({
                    "Darajah": darajah_name,
                    "TotalMembers": darajah.get("ActiveStudents", 0),
                    "TotalBooksIssued": darajah.get("BooksIssued", 0),
                    "ActiveStudents": darajah.get("ActiveStudents", 0)
                })
    except Exception as e:
        current_app.logger.error(f"Error getting darajahs: {e}")

    # Group darajahs by year for card display
    darajahs_by_year = defaultdict(list)
    for darajah in darajahs_in_marhala:
        year = darajah.get("year", "")
        if year:
            darajahs_by_year[year].append(darajah)
        else:
            # Put darajahs without year in "Other" category
            darajahs_by_year["Other"].append(darajah)
    
    # Sort years
    sorted_years = sorted([y for y in darajahs_by_year.keys() if y != "Other"], key=lambda x: int(x) if x.isdigit() else 0)
    if "Other" in darajahs_by_year:
        sorted_years.append("Other")

    # Group darajahs by year for chart (existing logic)
    darajah_group_labels, darajah_group_values = [], []
    if darajahs_in_marhala:
        year_counts = defaultdict(int)
        for darajah in darajahs_in_marhala:
            # Extract year from darajah name
            match = re.search(r'\d+', darajah["name"])
            if match:
                year = match.group()
                key = f"Darajah {year}"
                year_counts[key] += darajah.get("active_students", 0)
        
        darajah_group_labels = list(year_counts.keys())
        darajah_group_values = [year_counts[k] for k in darajah_group_labels]

    # Get top students in marhala
    top_students = get_top_students_in_marhala(marhala_code, limit=5) if stats_available else []

    # Build summary table - Top 10 Darajah Performance
    summary_table = []
    
    # Filter out Asateza and sort by books issued (performance)
    filtered_darajahs = [
        d for d in darajahs_in_marhala 
        if d.get("name", "").upper() not in ["ASATEZA", "ASATEZA KIRAM"] 
        and "AJSN" not in d.get("name", "").upper()
    ]
    
    # Sort by books issued (descending) and take top 10
    sorted_darajahs = sorted(
        filtered_darajahs, 
        key=lambda x: x.get("books_issued", 0), 
        reverse=True
    )[:10]
    
    for darajah in sorted_darajahs:
        # Get darajah stats (you can enhance this with more detailed queries)
        darajah_metrics = {
            "currently_issued": 0,
            "overdues": 0,
            "fees_paid": 0.0
        }
        
        summary_table.append({
            "ClassName": darajah["name"],
            "StudentCount": darajah.get("active_students", 0),
            "Issues_AY": darajah.get("books_issued", 0),
            "IssuesPerStudent": darajah.get("issues_per_student", 0),
            "CurrentlyIssued": darajah_metrics["currently_issued"],
            "Overdues": darajah_metrics["overdues"],
            "FeesPaid_AY": darajah_metrics["fees_paid"]
        })

    # Get department performance data
    academic_departments = []
    other_departments = []
    
    if marhala_type == "Academic":
        all_academic = KQ.get_academic_departments_performance()
        academic_departments = [dept for dept in all_academic if dept.get("Marhala") == marhala_display_name]
    else:
        all_other = KQ.get_non_academic_departments_performance()
        other_departments = [dept for dept in all_other if dept.get("Marhala") == marhala_display_name]

    # Get currently issued books
    currently_issued_data = KQ.get_department_currently_issued(marhala_display_name)
    if not currently_issued_data or not currently_issued_data.get("marhalas"):
        currently_issued_data = KQ.get_department_currently_issued(marhala_code)

    # Calculate derived metrics
    total_darajahs = len(darajahs_in_marhala)
    engagement_index = round(total_issues_ay / active_borrowers, 1) if active_borrowers else 0.0
    overdue_rate = round((overdues_now / currently_issued) * 100, 1) if currently_issued else 0.0

    # Calculate performance score
    performance_score = _calculate_performance_score(
        engagement_index,
        (active_borrowers / total_borrowers * 100) if total_borrowers else 0,
        overdue_rate
    )
    performance_level = _get_performance_level(performance_score)
    performance_color = PERFORMANCE_LEVELS.get(performance_level, {}).get("color", "#9E9E9E")

    # Get key insights
    key_insights = []
    if stats_available:
        key_insights.append(f"{marhala_display_name} has {active_borrowers} active borrowers with TR numbers.")
        if total_issues_ay > 0:
            key_insights.append(f"Total books issued in AY: {total_issues_ay:,} ({engagement_index:.1f} per active borrower).")
        if currently_issued > 0:
            key_insights.append(f"Currently issued books: {currently_issued:,} ({overdue_rate:.1f}% overdue).")
        if darajahs_in_marhala:
            key_insights.append(f"Contains {total_darajahs} darajahs/groups.")
    
    if not key_insights:
        key_insights.append("No recent activity data available for this marhala.")

    # Date information
    today = date.today()
    today_hijri = _hijri_date_label(today)
    today_greg = today.strftime("%d %B %Y")
    ay_period_label = get_academic_year_period()

    # Get all marhalas for admin selector
    all_marhalas = []
    if role == "admin":
        all_marhalas = get_all_marhalas_for_hod()

    # Get detailed activities and overdues for HOD parity
    recent_activities = _get_marhala_recent_activity(marhala_code, limit=10)
    overdue_books_list = _get_marhala_overdue_books(marhala_code, limit=20)

    return render_template(
        "hod_dashboard.html",
        marhala_name=marhala_code,
        marhala_display_name=marhala_display_name,
        username=username,
        total_borrowers=total_borrowers,
        active_borrowers=active_borrowers,
        total_issues_ay=total_issues_ay,
        total_fees_ay=total_fees_ay,
        currently_issued=currently_issued,
        overdues_now=overdues_now,
        trend_labels=trend_labels,
        trend_values=trend_values,
        trend_period_label=trend_period_label,
        ay_total_issues_trend=ay_total_issues_trend,
        avg_issues_month=avg_issues_month,
        peak_month_label=peak_month_label,
        darajah_labels=darajah_labels,
        darajah_values=darajah_values,
        top_darajah_name=top_darajah_name,
        top_darajah_issues=top_darajah_issues,
        top_arabic=top_arabic,
        top_english=top_english,
        top_students=top_students,
        summary_table=summary_table,
        darajah_stats=darajah_stats, # Pass darajah_stats for the table
        total_darajahs=total_darajahs,
        engagement_index=engagement_index,
        overdue_rate=overdue_rate,
        today_hijri=today_hijri,
        today_greg=today_greg,
        ay_period_label=ay_period_label,
        all_marhalas=all_marhalas,
        is_admin=role == "admin",
        darajahs_in_marhala=darajahs_in_marhala,
        darajah_group_labels=darajah_group_labels,
        darajah_group_values=darajah_group_values,
        academic_departments=academic_departments,
        other_departments=other_departments,
        currently_issued_data=currently_issued_data,
        # Professional enhancements
        performance_score=performance_score,
        performance_level=performance_level,
        performance_color=performance_color,
        key_insights=key_insights,
        marhala_type=marhala_type,
        marhala_icon=marhala_icon,
        marhala_color=marhala_color,
        stats_available=stats_available,
        time_period=_get_time_period_label(),
        format_currency=_format_currency,
        # New variables for darajah cards
        darajahs_by_year=dict(darajahs_by_year),
        sorted_years=sorted_years,
        # Placeholders for template
        recent_activities=recent_activities,
        overdue_books_list=overdue_books_list
    )

# --------------------------------------------------
# ADMIN DASHBOARD SELECT TEMPLATE
# --------------------------------------------------
@bp.route("/admin-select")
def admin_select():
    """Admin dashboard selection page."""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role != "admin":
        flash("Admin access required.", "danger")
        return redirect(url_for("dashboard_bp.dashboard"))

    all_marhalas = get_all_marhalas_for_hod()
    today = date.today()
    hijri_today = _hijri_date_label(today)
    ay_period = get_academic_year_period()
    
    # Calculate summary stats
    total_currently_issued = 0
    total_fees = 0.0
    total_issues = 0
    total_marhalas = len(all_marhalas)
    marhalas_with_data = [m for m in all_marhalas if m.get("has_data", False)]
    
    for marhala in all_marhalas:
        total_currently_issued += (marhala.get("currently_issued") or 0)
        total_fees += (marhala.get("ay_fees") or 0.0)
        total_issues += (marhala.get("ay_issues") or 0)
    
    return redirect(url_for("hod_dashboard_bp.marhala_explorer"))

# --------------------------------------------------
# MARHALA EXPLORER (Professional Version)
# --------------------------------------------------
@bp.route("/marhala-explorer")
def marhala_explorer():
    """Professional marhala explorer page for admin to browse all marhalas."""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role != "admin":
        flash("Admin access required for marhala explorer.", "danger")
        return redirect(url_for("dashboard_bp.dashboard"))
    
    # Get Academic Year from session
    selected_ay = session.get("selected_ay", "current")
    hijri_year = None
    if selected_ay != "current":
        try:
            hijri_year = int(selected_ay)
        except (ValueError, TypeError):
            selected_ay = "current"

    hijri_today = KQ.get_hijri_date_label(date.today())
    
    # Get current academic year bounds
    start, end = KQ.get_ay_bounds(hijri_year)
    current_ay_year = start.year if start else date.today().year
    
    # Get all marhalas with stats for specified year
    all_marhalas = get_all_marhalas_for_hod(hijri_year=hijri_year)
    
    # FIXED: Separate by proper marhala types
    academic_marhalas = [m for m in all_marhalas if m["type"] == "Academic"]
    
    # FIXED: Include all non-academic types properly
    non_academic_marhalas = [
        m for m in all_marhalas if m["type"] in [
            "Teaching Staff", "Library Staff", "Support Staff", 
            "Administration", "Other"
        ]
    ]
    
    # Calculate totals
    total_marhalas = len(all_marhalas)
    marhalas_with_data = len([m for m in all_marhalas if m.get("has_data", False)])
    total_issues = sum(m.get("ay_issues", 0) for m in all_marhalas)
    total_members = sum(m.get("total_members", 0) for m in all_marhalas)
    total_fees = sum(m.get("ay_fees", 0) for m in all_marhalas)
    
    # Get marhala distribution for chart
    marhala_labels = []
    marhala_values = []
    marhala_colors = []
    
    # Group by type for chart
    type_totals = defaultdict(int)
    for marhala in all_marhalas:
        marhala_type = marhala["type"]
        if marhala_type in ["Teaching Staff", "Library Staff", "Support Staff"]:
            # Group these under "Non-Academic" for chart
            type_totals["Non-Academic"] += marhala.get("ay_issues", 0)
        else:
            type_totals[marhala_type] += marhala.get("ay_issues", 0)
    
    for marhala_type, total in type_totals.items():
        if total > 0:  # Only show types with data
            marhala_labels.append(marhala_type)
            marhala_values.append(total)
            # Get color for this type
            if marhala_type == "Academic":
                marhala_colors.append(MARHALA_TYPES["ACADEMIC"]["color"])
            elif marhala_type == "Non-Academic":
                marhala_colors.append(MARHALA_TYPES["NON_ACADEMIC"]["color"])
            elif marhala_type in ["Teaching Staff", "Library Staff", "Support Staff"]:
                marhala_colors.append(MARHALA_TYPES["NON_ACADEMIC"]["color"])
            elif marhala_type == "Administration":
                marhala_colors.append(MARHALA_TYPES["ADMIN"]["color"])
            else:
                marhala_colors.append("#757575")
    
    return render_template(
        "marhala_explorer.html",
        hijri_today=hijri_today,
        total_marhalas=total_marhalas,
        marhalas_with_data=marhalas_with_data,
        total_issues=total_issues,
        total_members=total_members,
        total_fees=total_fees,
        academic_marhalas=academic_marhalas,
        non_academic_marhalas=non_academic_marhalas,
        current_ay_year=current_ay_year,
        marhala_labels=marhala_labels,
        marhala_values=marhala_values,
        marhala_colors=marhala_colors,
        format_currency=_format_currency,
        time_period=_get_time_period_label(),
        all_marhalas=all_marhalas  # Add this line to pass all marhalas to template
    )

# --------------------------------------------------
# OTHER ROUTES (keep as they are with minor updates)
# --------------------------------------------------
@bp.route("/quick-select")
def quick_select():
    """Quick marhala selection endpoint for admin."""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role != "admin":
        flash("Admin access required.", "danger")
        return redirect(url_for("dashboard_bp.dashboard"))

    marhala_code = request.args.get("marhala")
    if not marhala_code:
        flash("No marhala selected.", "warning")
        return redirect(url_for("hod_dashboard_bp.dashboard"))

    return redirect(url_for("hod_dashboard_bp.dashboard", marhala=marhala_code))

@bp.route("/debug/marhala/<marhala_code>")
def debug_marhala(marhala_code):
    """Debug endpoint to check marhala information."""
    if not session.get("logged_in"):
        return "Not logged in", 401
    
    role = (session.get("role") or "").lower()
    if role not in ("hod", "admin"):
        return "Access denied", 403
    
    output = []
    output.append(f"<h1>Debug Marhala: {marhala_code}</h1>")
    output.append(f"<p><a href='/hod'>Back to HOD Dashboard</a></p>")
    
    try:
        conn = get_koha_conn()
        cur = conn.cursor(dictionary=True)
        
        output.append("<h2>1. Koha Categories</h2>")
        cur.execute("SELECT categorycode, description FROM categories WHERE categorycode = %s", (marhala_code,))
        categories = cur.fetchall()
        
        if categories:
            output.append("<table border='1'><tr><th>Code</th><th>Description</th></tr>")
            for row in categories:
                output.append(f"<tr><td>{row['categorycode']}</td><td>{row['description']}</td></tr>")
            output.append("</table>")
        else:
            output.append("<p>Category not found.</p>")
            # Try to find by name
            cur.execute("SELECT categorycode, description FROM categories WHERE description LIKE %s", (f"%{marhala_code}%",))
            similar = cur.fetchall()
            if similar:
                output.append("<h3>Similar categories found:</h3>")
                output.append("<table border='1'><tr><th>Code</th><th>Description</th></tr>")
                for row in similar:
                    output.append(f"<tr><td>{row['categorycode']}</td><td>{row['description']}</td></tr>")
                output.append("</table>")
        
        # Check borrowers
        output.append("<h2>2. Active Borrowers in Category</h2>")
        cur.execute("""
            SELECT COUNT(*) as count 
            FROM borrowers 
            WHERE categorycode = %s 
              AND (dateexpiry IS NULL OR dateexpiry >= CURDATE())
              AND (debarred IS NULL OR debarred = 0)
              AND (gonenoaddress IS NULL OR gonenoaddress = 0)
        """, (marhala_code,))
        
        count_row = cur.fetchone()
        borrower_count = count_row["count"] if count_row else 0
        output.append(f"<p>Active borrowers: {borrower_count}</p>")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        output.append(f"<p style='color: red;'>Error: {str(e)}</p>")
        output.append(f"<pre>{traceback.format_exc()}</pre>")
    
    return "<br>".join(output)

# --------------------------------------------------
# API ENDPOINTS
# --------------------------------------------------
@bp.route("/api/marhala/<marhala_code>")
def api_marhala_details(marhala_code):
    """API endpoint to get detailed marhala information."""
    if not session.get("logged_in"):
        return jsonify({"error": "Not authenticated"}), 401

    try:
        # Get marhala details
        all_marhalas = get_all_marhalas_for_hod()
        selected_marhala = None
        
        for marhala in all_marhalas:
            if marhala["code"] == marhala_code or marhala["name"] == marhala_code:
                selected_marhala = marhala
                break
        
        if not selected_marhala:
            return jsonify({"error": "Marhala not found"}), 404
        
        # Get detailed stats
        total_borrowers, active_borrowers, ay_issues, ay_fees, currently_issued, overdues = _get_accurate_marhala_stats(marhala_code)
        
        # Get darajahs
        darajahs_data = KQ.get_darajah_summary_by_marhala(selected_marhala["name"])
        darajahs = []
        
        for darajah in darajahs_data:
            darajahs.append({
                "name": darajah.get("Darajah", ""),
                "books_issued": darajah.get("BooksIssued", 0),
                "active_students": darajah.get("ActiveStudents", 0),
                "issues_per_student": darajah.get("IssuesPerStudent", 0)
            })
        
        return jsonify({
            "success": True,
            "marhala_info": selected_marhala,
            "stats": {
                "total_borrowers": total_borrowers,
                "active_borrowers": active_borrowers,
                "ay_issues": ay_issues,
                "ay_fees": ay_fees,
                "currently_issued": currently_issued,
                "overdues": overdues
            },
            "darajahs": darajahs,
            "darajahs_count": len(darajahs)
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@bp.route("/api/marhalas")
def api_marhalas():
    """API endpoint to get all marhalas for dropdowns."""
    if not session.get("logged_in"):
        return jsonify({"error": "Not authenticated"}), 401

    role = (session.get("role") or "").lower()
    if role != "admin":
        return jsonify({"error": "Admin access required"}), 403

    try:
        marhalas = get_all_marhalas_for_hod()
        return jsonify({
            "success": True,
            "marhalas": marhalas,
            "count": len(marhalas)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --------------------------------------------------
# SEARCH FUNCTIONALITY
# --------------------------------------------------
@bp.route("/search")
def search():
    """Search within marhala."""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role not in ("hod", "admin"):
        return redirect(url_for("auth_bp.login"))

    query = (request.args.get("q") or "").strip()

    if role == "hod":
        marhala_name = session.get("marhala_name") or session.get("department_name")
    else:
        marhala_name = request.args.get("marhala") or session.get("marhala_name") or session.get("department_name")

    if not query:
        return redirect(url_for("hod_dashboard_bp.dashboard"))

    # Get marhala report data
    report_data = marhala_report(marhala_name)
    
    # Handle the tuple return
    if isinstance(report_data, tuple):
        df, total_students = report_data
    else:
        df = report_data
    
    if not isinstance(df, pd.DataFrame) or df.empty:
        flash("⚠️ No records found for your marhala.", "warning")
        return redirect(url_for("hod_dashboard_bp.dashboard"))

    students, darajahs = [], []

    # Student matches by name or TR
    if "FullName" in df.columns and "TRNumber" in df.columns:
        mask_students = (
            df["FullName"].str.contains(query, case=False, na=False)
            | df["TRNumber"].astype(str).str.contains(query, case=False, na=False)
        )
        students = df[mask_students].to_dict("records")
        
        # Clean student names and add URLs
        for student in students:
            student["FullName"] = _clean_student_name(student.get("FullName", ""))
            tr_number = student.get("TRNumber", "")
            if tr_number:
                student["StudentURL"] = _generate_student_url(tr_number)
            else:
                student["StudentURL"] = "#"

    # Darajah matches
    if "Darajah" in df.columns:
        darajah_df = (
            df[df["Darajah"].astype(str).str.contains(query, case=False, na=False)]
            .groupby("Darajah")[["Issues_AY", "FeesPaid_AY", "CurrentlyIssued", "Overdues"]]
            .agg("sum")
            .reset_index()
        )
        darajah_df["StudentCount"] = darajah_df["Darajah"].map(df["Darajah"].value_counts())
        darajah_df = darajah_df.rename(columns={"Darajah": "DarajahName"})
        darajahs = darajah_df.to_dict("records")

    return render_template(
        "hod_search_results.html",
        marhala_name=marhala_name,
        query=query,
        students=students,
        darajahs=darajahs,
    )

# --------------------------------------------------
# EXPORT ROUTES
# --------------------------------------------------
@bp.route("/download/marhala/pdf")
def download_marhala_pdf():
    """Download marhala PDF report."""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role not in ("hod", "admin"):
        return redirect(url_for("auth_bp.login"))

    if role == "hod":
        marhala_name = session.get("marhala_name") or session.get("department_name")
    else:
        marhala_name = request.args.get("marhala") or session.get("marhala_name") or session.get("department_name")

    # Get marhala report data
    report_data = marhala_report(marhala_name)
    
    if isinstance(report_data, tuple):
        df, total_students = report_data
    else:
        df = report_data
    
    if not isinstance(df, pd.DataFrame) or df.empty:
        flash("⚠️ No data found for your marhala.", "warning")
        return redirect(url_for("hod_dashboard_bp.dashboard"))

    # Get marhala display name
    all_marhalas = get_all_marhalas_for_hod()
    marhala_display_name = marhala_name
    for marhala in all_marhalas:
        if marhala["code"] == marhala_name:
            marhala_display_name = marhala["name"]
            break

    pdf_bytes = dataframe_to_pdf_bytes(f"Marhala Report - {marhala_display_name}", df)
    return send_file(
        BytesIO(pdf_bytes),
        as_attachment=True,
        download_name=f"marhala_report_{marhala_name}.pdf",
        mimetype="application/pdf",
    )

@bp.route("/download/darajah/<darajah_name>")
def download_darajah_pdf(darajah_name):
    """Generate darajah PDF."""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role not in ("hod", "admin"):
        return redirect(url_for("auth_bp.login"))

    # Get darajah report data
    marhala_filter = None
    if role == "hod":
        marhala_filter = session.get("marhala_name") or session.get("department_name")
    
    report_data = darajah_report(darajah_name, marhala_filter=marhala_filter)
    
    if isinstance(report_data, tuple):
        df, total_students = report_data
    else:
        df = report_data
    
    if not isinstance(df, pd.DataFrame) or df.empty:
        flash(f"⚠️ No data found for darajah {darajah_name}.", "warning")
        return redirect(url_for("hod_dashboard_bp.dashboard"))

    # Drop incomplete rows
    df = df.dropna(subset=["FullName", "TRNumber"], how="all")
    df = df[~df["FullName"].isin(["", None, "NaN", "nan"])]
    df = df[~df["TRNumber"].isin(["", None, "NaN", "nan"])]

    if df.empty:
        flash(f"⚠️ All rows in darajah {darajah_name} were empty and skipped.", "warning")
        return redirect(url_for("hod_dashboard_bp.dashboard"))

    df = df.fillna("")
    if "Collections" in df.columns:
        df["Collections"] = df["Collections"].astype(str).apply(
            lambda x: x[:250] + "…" if len(x) > 250 else x
        )

    pdf_bytes = dataframe_to_pdf_bytes(f"Darajah Report - {darajah_name}", df)
    return send_file(
        BytesIO(pdf_bytes),
        as_attachment=True,
        download_name=f"darajah_report_{darajah_name}.pdf",
        mimetype="application/pdf",
    )

@bp.route("/student/<identifier>")
@bp.route("/student/<identifier>/")
def student_details(identifier):
    """Display detailed student information."""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role not in ("hod", "admin"):
        flash("You must be a Head of Department to view this page.", "danger")
        return redirect(url_for("auth_bp.login"))

    try:
        from routes.students import student as student_view
        return student_view(identifier)
        
    except Exception as e:
        current_app.logger.error(f"Error in student_details: {str(e)}", exc_info=True)
        flash(f'Error loading student details: {str(e)}', 'danger')
        return redirect(url_for('hod_dashboard_bp.dashboard'))