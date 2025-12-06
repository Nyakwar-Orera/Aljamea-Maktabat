# routes/hod_dashboard.py

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
from routes.reports import department_report, class_report
from routes.students import get_student_info
from routes.teacher_dashboard import _parse_class_name, get_all_classes
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
    make_doc_with_header_footer,
    dataframe_to_pdf_bytes
)
import os
import pandas as pd
import re
import traceback

bp = Blueprint("hod_dashboard_bp", __name__)

# --------------------------------------------------
# HIJRI HELPERS (same style as teacher_dashboard)
# --------------------------------------------------
def _hijri_date_label(d: date) -> str:
    try:
        from hijri_converter import convert

        h = convert.Gregorian(d.year, d.month, d.day).to_hijri()
        hijri_months = [
            "Muḥarram al-Harām",
            "Safar al-Muzaffar",
            "Rabi al-Awwal",
            "Rabī al-Aakhar",
            "Jamādil Awwal",
            "Jamādā al-ʾŪkhrā",
            "Rajab al-Asab",
            "Shabān al-Karim",
            "Shehrullah al-Moazzam",
            "Shawwāl al-Mukarram",
            "Zilqādah al-Harām",
            "Zilhijjatil Harām",
        ]
        return f"{h.day} {hijri_months[h.month - 1]} {h.year} H"
    except Exception:
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
    except Exception:
        # Fallback compact Gregorian
        return d.strftime("%d-%m-%y")


def _hijri_month_year_label(d: date) -> str:
    try:
        from hijri_converter import convert

        h = convert.Gregorian(d.year, d.month, d.day).to_hijri()
        hijri_months = [
            "Muḥarram al-Harām",
            "Safar al-Muzaffar",
            "Rabi al-Awwal",
            "Rabī al-Aakhar",
            "Jamādil Awwal",
            "Jamādā al-ʾŪkhrā",
            "Rajab al-Asab",
            "Shabān al-Karim",
            "Shehrullah al-Moazzam",
            "Shawwāl al-Mukarram",
            "Zilqādah al-Harām",
            "Zilhijjatil Harām",
        ]
        return f"{hijri_months[h.month - 1]} {h.year} H"
    except Exception:
        return d.strftime("%B %Y")


def _hijri_from_any(value) -> str:
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
        return _hijri_date_label(g)
    except Exception:
        return str(value)


# --------------------------------------------------
# AY RANGE (aligned with teacher / main dashboard)
# --------------------------------------------------
def _ay_bounds():
    """
    Academic Year window (same logic as teacher dashboard):
      - If today in Apr–Dec: 1 Apr (this year) → today
      - If today in Jan–Mar: 1 Apr (previous year) → 31 Dec (previous year)
    """
    today = date.today()

    if 4 <= today.month <= 12:
        start = date(today.year, 4, 1)
        end = today
    else:
        ay_year = today.year - 1
        start = date(ay_year, 4, 1)
        end = date(ay_year, 12, 31)

    return start, end


# Common WHERE fragment to only count "active" patrons
ACTIVE_PATRON_FILTER = """
  AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
  AND (b.debarred IS NULL OR b.debarred = 0 OR b.debarred = '0')
  AND COALESCE(b.gonenoaddress,0) = 0
  AND COALESCE(b.lost,0) = 0
"""


# --------------------------------------------------
# DEPARTMENT MAPPING HELPER
# --------------------------------------------------
def map_department_to_koha_code(dept_display_name: str):
    """
    Map human-readable department names to Koha category codes.
    This helps when the display name doesn't match the Koha category code.
    """
    if not dept_display_name:
        return dept_display_name
    
    # Common mappings - adjust these based on your actual Koha categories
    department_mappings = {
        # Example mappings
        "Collegiate I (5-7)": "COL1",
        "Collegiate II (8-11)": "COL2", 
        "Staff": "STAFF",
        "Administration": "ADMIN",
        "Collegiate 1": "COL1",
        "Collegiate 2": "COL2",
        "Collegiate 3": "COL3",
        # Add more mappings as needed
    }
    
    # Check exact match first
    if dept_display_name in department_mappings:
        mapped = department_mappings[dept_display_name]
        return mapped
    
    # Check partial matches
    dept_lower = dept_display_name.lower()
    for display_name, koha_code in department_mappings.items():
        if display_name.lower() in dept_lower or dept_lower in display_name.lower():
            return koha_code
    
    # If no mapping found, try to extract a code
    if "collegiate" in dept_lower:
        if "i" in dept_lower or "1" in dept_lower or "5-7" in dept_lower:
            return "COL1"
        elif "ii" in dept_lower or "2" in dept_lower or "8-10" in dept_lower:
            return "COL2"
        elif "iii" in dept_lower or "3" in dept_lower or "11-12" in dept_lower:
            return "COL3"
    
    # Return the original as a fallback
    return dept_display_name


# --------------------------------------------------
# GET ALL DEPARTMENTS FOR ADMIN - FIXED VERSION
# --------------------------------------------------
def get_all_departments():
    """
    Get all distinct departments from Koha categories for admin selection.
    FIXED: Removed ambiguous column reference for 'categorycode'
    """
    conn = get_koha_conn()
    cur = conn.cursor()
    
    try:
        # FIXED: Specify table aliases for ambiguous columns
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
        departments = []
        for row in rows:
            categorycode, description, enrolmentperiod, member_count = row
            # Determine department type
            desc_lower = description.lower()
            if any(word in desc_lower for word in ['student', 'class', 'darajah', 'daraja', 'form', 'grade', 'study', 'collegiate']):
                dept_type = "Academic"
            elif any(word in desc_lower for word in ['staff', 'faculty', 'teacher', 'employee', 'administration', 'admin']):
                dept_type = "Staff"
            elif 'library' in desc_lower:
                dept_type = "Library"
            else:
                dept_type = "Other"
            
            departments.append({
                "code": categorycode,
                "name": description.strip(),
                "display": description.strip(),
                "enrolment_period": enrolmentperiod,
                "fines_cap": 0.0,
                "total_members": member_count or 0,
                "type": dept_type
            })
        
    except Exception as e:
        traceback.print_exc()
        
        # Fallback: try a simpler query
        try:
            cur.execute("""
                SELECT 
                    categorycode,
                    description,
                    enrolmentperiod
                FROM categories
                WHERE description IS NOT NULL 
                  AND description != ''
                ORDER BY description
            """)
            
            rows = cur.fetchall()
            departments = []
            for row in rows:
                categorycode, description, enrolmentperiod = row
                departments.append({
                    "code": categorycode,
                    "name": description.strip(),
                    "display": description.strip(),
                    "enrolment_period": enrolmentperiod,
                    "fines_cap": 0.0,
                    "total_members": 0,
                    "type": "Other"
                })   
        except Exception as e2:
            departments = []
    
    finally:
        cur.close()
        conn.close()
    
    return departments


# --------------------------------------------------
# HELPER: GET ACCURATE DEPARTMENT STATS
# --------------------------------------------------
def _get_accurate_department_stats(dept_name: str):
    """
    Get accurate department statistics using consistent logic.
    Returns: (total_borrowers, active_borrowers, ay_issues, ay_fines, active_loans, overdues)
    """
    try:
        start, end = _ay_bounds()
        conn = get_koha_conn()
        cur = conn.cursor(dictionary=True)
        
        # Get accurate borrower counts (aligned with teacher dashboard)
        cur.execute(
            """
            SELECT COUNT(DISTINCT b.borrowernumber) as total_borrowers
            FROM borrowers b
            WHERE b.categorycode = %s
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0 OR b.debarred = '0')
              AND COALESCE(b.gonenoaddress,0) = 0
              AND COALESCE(b.lost,0) = 0
            """,
            (dept_name,)
        )
        total_row = cur.fetchone()
        total_borrowers = total_row["total_borrowers"] if total_row else 0
        
        # Count borrowers with TR numbers (for students)
        cur.execute(
            """
            SELECT COUNT(DISTINCT trno.attribute) as active_borrowers
            FROM borrowers b
            LEFT JOIN borrower_attributes trno
                 ON trno.borrowernumber = b.borrowernumber
                AND trno.code = 'TRNO'
            WHERE b.categorycode = %s
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0 OR b.debarred = '0')
              AND COALESCE(b.gonenoaddress,0) = 0
              AND COALESCE(b.lost,0) = 0
              AND trno.attribute IS NOT NULL
              AND trno.attribute != ''
            """,
            (dept_name,)
        )
        active_row = cur.fetchone()
        active_borrowers = active_row["active_borrowers"] if active_row else 0
        
        # AY Issues count
        cur.execute(
            """
            SELECT COUNT(*) as ay_issues
            FROM statistics s
            JOIN borrowers b ON s.borrowernumber = b.borrowernumber
            WHERE s.type = 'issue'
              AND DATE(s.`datetime`) BETWEEN %s AND %s
              AND b.categorycode = %s
            """,
            (start, end, dept_name)
        )
        issues_row = cur.fetchone()
        ay_issues = issues_row["ay_issues"] if issues_row else 0
        
        # AY Fines paid
        cur.execute(
            """
            SELECT COALESCE(SUM(
                CASE
                  WHEN a.credit_type_code='PAYMENT'
                       AND (a.status IS NULL OR a.status <> 'VOID')
                       AND DATE(a.`date`) BETWEEN %s AND %s
                  THEN -a.amount ELSE 0 END
            ),0) as ay_fines
            FROM accountlines a
            JOIN borrowers b ON a.borrowernumber = b.borrowernumber
            WHERE b.categorycode = %s
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0 OR b.debarred = '0')
              AND COALESCE(b.gonenoaddress,0) = 0
              AND COALESCE(b.lost,0) = 0
            """,
            (start, end, dept_name)
        )
        fines_row = cur.fetchone()
        ay_fines = float(fines_row["ay_fines"] if fines_row else 0)
        
        # Active loans
        cur.execute(
            """
            SELECT COUNT(*) as active_loans
            FROM issues i
            JOIN borrowers b ON i.borrowernumber = b.borrowernumber
            WHERE b.categorycode = %s
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0 OR b.debarred = '0')
              AND COALESCE(b.gonenoaddress,0) = 0
              AND COALESCE(b.lost,0) = 0
              AND i.returndate IS NULL
            """,
            (dept_name,)
        )
        loans_row = cur.fetchone()
        active_loans = loans_row["active_loans"] if loans_row else 0
        
        # Overdues
        cur.execute(
            """
            SELECT COUNT(*) as overdues
            FROM issues i
            JOIN borrowers b ON i.borrowernumber = b.borrowernumber
            WHERE b.categorycode = %s
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0 OR b.debarred = '0')
              AND COALESCE(b.gonenoaddress,0) = 0
              AND COALESCE(b.lost,0) = 0
              AND i.date_due < CURDATE()
              AND i.returndate IS NULL
            """,
            (dept_name,)
        )
        overdues_row = cur.fetchone()
        overdues = overdues_row["overdues"] if overdues_row else 0
        
        cur.close()
        conn.close()
        
        return total_borrowers, active_borrowers, ay_issues, ay_fines, active_loans, overdues
        
    except Exception as e:
        traceback.print_exc()
        return 0, 0, 0, 0.0, 0, 0


# --------------------------------------------------
# DEPARTMENT EXPLORER (for admin)
# --------------------------------------------------
@bp.route("/department-explorer")
def department_explorer():
    """Department explorer page for admin to browse all departments"""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role != "admin":
        flash("Admin access required for department explorer.", "danger")
        return redirect(url_for("dashboard_bp.dashboard"))
    
    try:
        from hijri_converter import convert
        
        today = date.today()
        h = convert.Gregorian(today.year, today.month, today.day).to_hijri()
        hijri_months = [
            "Muḥarram al-Harām", "Safar al-Muzaffar", "Rabi al-Awwal", "Rabī al-Aakhar",
            "Jamādil Awwal", "Jamādā al-ʾŪkhrā", "Rajab al-Asab", "Shabān al-Karim",
            "Shehrullah al-Moazzam", "Shawwāl al-Mukarram", "Zilqādah al-Harām", "Zilhijjatil Harām",
        ]
        hijri_today = f"{h.day} {hijri_months[h.month - 1]} {h.year} H"
    except Exception:
        hijri_today = today.strftime("%d %B %Y")
    
    # Get all departments
    all_departments = get_all_departments()
    
    # Get stats for each department
    departments_with_stats = []
    for dept in all_departments:
        dept_code = dept["code"]
        dept_name = dept["name"]
        
        # Get accurate stats
        total_borrowers, active_borrowers, ay_issues, ay_fines, active_loans, overdues = _get_accurate_department_stats(dept_code)
        
        # Determine icon based on department type
        icon = "building"  # default
        if dept["type"] == "Academic":
            icon = "graduation-cap"
        elif dept["type"] == "Staff":
            icon = "user-tie"
        elif dept["type"] == "Library":
            icon = "book"
        
        departments_with_stats.append({
            "dept_code": dept_code,
            "dept_name": dept_name,
            "display": dept_name,
            "type": dept["type"],
            "icon": icon,
            "total_borrowers": total_borrowers,
            "active_borrowers": active_borrowers,
            "ay_issues": ay_issues,
            "ay_fines": ay_fines,
            "active_loans": active_loans,
            "overdues": overdues,
            "enrolmentperiod": dept.get("enrolment_period"),
            "overduefinescap": dept.get("fines_cap")
        })
    
    # Sort departments by name
    departments_with_stats.sort(key=lambda x: x["dept_name"])
    
    # Group by type for better organization
    departments_by_type = {}
    for dept in departments_with_stats:
        dept_type = dept["type"]
        if dept_type not in departments_by_type:
            departments_by_type[dept_type] = []
        departments_by_type[dept_type].append(dept)
    
    # Sort types
    sorted_types = sorted(departments_by_type.keys())
    
    # Calculate engagement stats
    engagement_stats = {
        "total_borrowers": sum(dept["total_borrowers"] for dept in departments_with_stats),
        "academic_departments": len([d for d in departments_with_stats if d["type"] == "Academic"]),
        "staff_departments": len([d for d in departments_with_stats if d["type"] == "Staff"]),
    }
    
    return render_template(
        "department_explorer.html",
        hijri_today=hijri_today,
        total_departments=len(departments_with_stats),
        departments_by_type=departments_by_type,
        sorted_types=sorted_types,
        engagement_stats=engagement_stats
    )


# --------------------------------------------------
# KPIs (Department) - Updated for consistency
# --------------------------------------------------
def get_department_kpis(department: str):
    """
    Department-level KPIs scoped to:
      - Active borrowers in this department (lifetime)
      - Issues in current AY (statistics table)
      - Fines PAID in current AY
      - Active loans (now)
      - Overdues (now)
    """
    # Use the unified function for consistency
    total_borrowers, active_borrowers, ay_issues, ay_fines, active_loans, overdues = _get_accurate_department_stats(department)
    
    return total_borrowers, ay_issues, ay_fines, active_loans, overdues


# --------------------------------------------------
# DEPARTMENT TREND (AY, monthly, Hijri labels)
# --------------------------------------------------
def get_department_ay_trend(department: str):
    """
    Monthly borrowing trend for the department over the current AY,
    grouped by Gregorian month but labelled in Hijri (like teacher dashboard).
    """
    start, end = _ay_bounds()
    conn = get_koha_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT DATE_FORMAT(s.`datetime`, '%Y-%m') AS ym,
               COUNT(*) AS cnt
        FROM statistics s
        JOIN borrowers b ON s.borrowernumber = b.borrowernumber
        WHERE s.type='issue'
          AND b.categorycode = %s
          AND DATE(s.`datetime`) BETWEEN %s AND %s
        GROUP BY ym
        ORDER BY ym;
        """,
        (department, start, end),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    by_month = {ym: int(cnt) for ym, cnt in (rows or [])}

    labels = []
    values = []

    # Walk AY months from April → last active month
    m = date(start.year, 4, 1)
    last_month = end.replace(day=1)

    while m <= last_month:
        ym = m.strftime("%Y-%m")
        labels.append(_hijri_month_year_label(m))
        values.append(by_month.get(ym, 0))
        if m.month == 12:
            break
        m = date(m.year, m.month + 1, 1)

    period_label = f"{_hijri_month_year_label(start)} – {_hijri_month_year_label(last_month)}"
    return labels, values, period_label


# --------------------------------------------------
# CLASS BREAKDOWN (Issues by class in this department, AY) - FIXED
# --------------------------------------------------
def get_class_breakdown(department: str):
    start, end = _ay_bounds()

    conn = get_koha_conn()
    cur = conn.cursor()
    
    sql = """
        SELECT COALESCE(std.attribute, b.branchcode, 'Unknown') AS class_name, 
               COUNT(*) AS cnt
        FROM statistics s
        JOIN borrowers b ON s.borrowernumber = b.borrowernumber
        LEFT JOIN borrower_attributes std
               ON std.borrowernumber = b.borrowernumber
              AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
        WHERE s.type='issue'
          AND b.categorycode = %s
          AND DATE(s.`datetime`) BETWEEN %s AND %s
        GROUP BY class_name
        ORDER BY cnt DESC
        LIMIT 15
    """
    
    cur.execute(sql, (department, start, end))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    # Filter out None or empty class names
    filtered_rows = [(r[0], r[1]) for r in rows if r[0] and r[0] != 'Unknown' and r[0] != '']
    
    labels = [r[0] for r in filtered_rows] or ["—"]
    values = [int(r[1]) for r in filtered_rows] or [0]
    return labels, values


# --------------------------------------------------
# TOP TITLES (Arabic / English) by DEPARTMENT (AY)
# --------------------------------------------------
def get_department_top_titles(department: str, lang_code: str, limit: int = 10):
    """
    Department-scoped Top titles in AY by MARC 041$a language code.
    lang_code: 'ar' for Arabic, 'eng' for English.
    Returns list of dicts with:
      BiblioNumber, Title, Collections, Times_Issued, Language.
    """
    start, end = _ay_bounds()

    conn = get_koha_conn()
    cur = conn.cursor(dictionary=True)
    lang_like = f"{lang_code}%"

    cur.execute(
        """
        SELECT
            bib.biblionumber AS Biblio_ID,
            bib.title        AS Title,
            GROUP_CONCAT(DISTINCT it.ccode ORDER BY it.ccode SEPARATOR ', ') AS Collections,
            COUNT(*) AS Times_Issued
        FROM (
            SELECT borrowernumber, itemnumber, issuedate
            FROM issues
            UNION ALL
            SELECT borrowernumber, itemnumber, issuedate
            FROM old_issues
        ) all_iss
        JOIN borrowers b ON b.borrowernumber = all_iss.borrowernumber
        JOIN items it ON all_iss.itemnumber = it.itemnumber
        JOIN biblio bib ON it.biblionumber = bib.biblionumber
        JOIN biblio_metadata bmd ON bib.biblionumber = bmd.biblionumber
        WHERE DATE(all_iss.issuedate) BETWEEN %s AND %s
          AND ExtractValue(
                bmd.metadata,
                '//datafield[@tag="041"]/subfield[@code="a"]'
              ) LIKE %s
          AND b.categorycode = %s
        GROUP BY bib.biblionumber, bib.title
        ORDER BY Times_Issued DESC
        LIMIT %s;
        """,
        (start, end, lang_like, department, int(limit)),
    )
    rows = cur.fetchall()
    conn.close()

    language_label = "Arabic" if lang_code.startswith("ar") else "English"
    for r in rows:
        r["Language"] = language_label
    return rows


# --------------------------------------------------
# GET CLASSES IN DEPARTMENT
# --------------------------------------------------
def get_classes_in_department(department: str):
    """
    Get all classes belonging to a specific department.
    """
    conn = get_koha_conn()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT DISTINCT 
            COALESCE(std.attribute, b.branchcode) AS class_name
        FROM borrowers b
        LEFT JOIN borrower_attributes std
            ON std.borrowernumber = b.borrowernumber
            AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
        WHERE b.categorycode = %s
          AND COALESCE(std.attribute, b.branchcode) IS NOT NULL
          AND COALESCE(std.attribute, b.branchcode) != ''
        ORDER BY COALESCE(std.attribute, b.branchcode)
    """, (department,))
    
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    classes = []
    for row in rows:
        class_name = row[0]
        if class_name:
            parsed = _parse_class_name(class_name)
            classes.append({
                "name": class_name,
                "display": parsed["display"],
                "year": parsed["year"],
                "section": parsed["section"],
                "gender": parsed["gender"],
                "is_arabic": parsed["is_arabic"]
            })
    
    return classes


# --------------------------------------------------
# HOD DASHBOARD (HOD + Admin view) - COMPLETELY FIXED
# --------------------------------------------------
@bp.route("/")
def dashboard():
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role not in ("hod", "admin"):
        return redirect(url_for("auth_bp.login"))

    if role == "hod":
        dept_name = session.get("department_name")
    else:  # admin can optionally pass ?dept=XYZ
        dept_name = request.args.get("dept") or session.get("department_name")

    username = session.get("username")

    # Helper for empty dashboard
    def _render_empty_dashboard(extra_message=None):
        if extra_message:
            flash(extra_message, "warning")

        # Get all departments for admin dropdown
        all_departments = []
        if role == "admin":
            all_departments = get_all_departments()

        return render_template(
            "hod_dashboard.html",
            dept_name=dept_name or "",
            username=username or "",
            total_borrowers=0,
            total_issues_ay=0,
            total_fines_ay=0.0,
            active_loans=0,
            overdues_now=0,
            trend_labels=[],
            trend_values=[],
            trend_period_label="",
            ay_total_issues_trend=0,
            avg_issues_month=0.0,
            peak_month_label="—",
            class_labels=[],
            class_values=[],
            top_class_name="—",
            top_class_issues=0,
            top_arabic=[],
            top_english=[],
            summary_table=[],
            total_classes=0,
            engagement_index=0.0,
            overdue_rate=0.0,
            today_hijri="",
            today_greg="",
            ay_period_label="",
            all_departments=all_departments,
            is_admin=role == "admin",
            classes_in_dept=[],
            dept_display_name=dept_name or ""
        )

    # No department linked / selected
    if not dept_name:
        if role == "admin":
            # Get all departments for dropdown
            all_departments = get_all_departments()
            
            # Show a selection page instead of empty dashboard
            return render_template(
                "hod_dashboard_select.html",
                all_departments=all_departments,
                username=username,
                is_admin=True
            )
        else:
            return _render_empty_dashboard("⚠️ Your account is not linked to any department.")

    # Map department name to Koha code if needed
    original_dept_name = dept_name
    dept_koha_code = map_department_to_koha_code(dept_name)
    
    if dept_koha_code != original_dept_name:
        dept_name = dept_koha_code

    # Get all departments to find display name and check if department exists
    all_depts = get_all_departments()
    
    # Find department display name and check if it exists
    dept_display_name = original_dept_name
    dept_exists = False
    
    # Check if we got any departments back
    if all_depts:
        for dept in all_depts:
            if dept["code"] == dept_name:
                dept_display_name = dept["name"]
                dept_exists = True
                break
    else:
        print(f"DEBUG: No departments returned from get_all_departments()")
    
    if not dept_exists:
        print(f"DEBUG: Department '{dept_name}' not found in Koha categories")
        
        # Try to check if the department exists directly in Koha
        conn = get_koha_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT categorycode, description 
            FROM categories 
            WHERE categorycode = %s
        """, (dept_name,))
        
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        if row:
            # Found it directly
            dept_display_name = row[1] if row[1] else dept_name
            dept_exists = True
        else:
            # Try to find similar departments
            similar_depts = []
            if all_depts:
                for dept in all_depts:
                    if original_dept_name.lower() in dept["name"].lower() or dept["name"].lower() in original_dept_name.lower():
                        similar_depts.append(dept)
            
            if similar_depts:
                # Use the first similar department
                dept_name = similar_depts[0]["code"]
                dept_display_name = similar_depts[0]["name"]
                flash(f"⚠️ Department mapped to: {dept_display_name}", "info")
            else:
                flash(f"⚠️ Department '{original_dept_name}' not found in Koha. Showing empty dashboard.", "warning")
                return _render_empty_dashboard()

    # Core KPIs using accurate stats
    (
        total_borrowers,
        total_issues_ay,
        total_fines_ay,
        active_loans,
        overdues_now,
    ) = get_department_kpis(dept_name)

    # AY Trend (monthly, Hijri labels)
    trend_labels, trend_values, trend_period_label = get_department_ay_trend(dept_name)

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

    # Class breakdown
    class_labels, class_values = get_class_breakdown(dept_name)
    if class_values and class_labels:
        top_class_name = class_labels[0]
        top_class_issues = class_values[0]
    else:
        top_class_name = "—"
        top_class_issues = 0

    # Top titles (department)
    top_arabic = get_department_top_titles(dept_name, lang_code="ar", limit=10)
    top_english = get_department_top_titles(dept_name, lang_code="eng", limit=10)

    # FIXED: Handle department_report returning a tuple (df, total_students)
    summary_table = []
    try:
        # Get department report data
        report_data = department_report(dept_name)
        
        # Check what department_report returns - it returns (df, total_students)
        if isinstance(report_data, tuple) and len(report_data) >= 1:
            # It returns (df, total_students)
            df = report_data[0]  # Get the DataFrame
            total_students = report_data[1] if len(report_data) > 1 else 0
        else:
            # It returns just df (shouldn't happen with current reports.py)
            df = report_data
            total_students = 0
        
        # Now process the DataFrame
        if isinstance(df, pd.DataFrame) and not df.empty and "Class" in df.columns:
            # Clean data first
            df = df.dropna(subset=["Class"])
            df = df[df["Class"] != ""]
            
            # Group by class
            grouped = (
                df.groupby("Class")[["Issues_AY", "FinesPaid_AY", "ActiveLoans", "Overdues"]]
                .sum()
                .reset_index()
            )
            
            # Get student counts
            student_counts = df["Class"].value_counts()
            grouped["StudentCount"] = grouped["Class"].map(student_counts)
            grouped = grouped.rename(columns={"Class": "ClassName"})
            
            # Convert to list of dictionaries
            summary_table = grouped.to_dict("records")
        else:
            summary_table = []
            
    except Exception as e:
        print(f"Error processing department report for {dept_name}: {str(e)}")
        traceback.print_exc()
        summary_table = []
        # Don't flash error to avoid disrupting user experience

    # Get classes in this department
    classes_in_dept = get_classes_in_department(dept_name)

    # Extra HOD insights
    total_classes = len([c for c in class_labels if c and c != "—"])
    engagement_index = round(total_issues_ay / total_borrowers, 1) if total_borrowers else 0.0
    overdue_rate = round((overdues_now / active_loans) * 100, 1) if active_loans else 0.0

    today = date.today()
    today_hijri = _hijri_date_label(today)
    today_greg = today.strftime("%d %B %Y")
    ay_period_label = trend_period_label  # reuse for display

    # Get all departments for admin dropdown
    all_departments = []
    if role == "admin":
        all_departments = get_all_departments()

    return render_template(
        "hod_dashboard.html",
        dept_name=dept_name,
        dept_display_name=dept_display_name,
        username=username,
        total_borrowers=total_borrowers,
        total_issues_ay=total_issues_ay,
        total_fines_ay=total_fines_ay,
        active_loans=active_loans,
        overdues_now=overdues_now,
        trend_labels=trend_labels,
        trend_values=trend_values,
        trend_period_label=trend_period_label,
        ay_total_issues_trend=ay_total_issues_trend,
        avg_issues_month=avg_issues_month,
        peak_month_label=peak_month_label,
        class_labels=class_labels,
        class_values=class_values,
        top_class_name=top_class_name,
        top_class_issues=top_class_issues,
        top_arabic=top_arabic,
        top_english=top_english,
        summary_table=summary_table,
        total_classes=total_classes,
        engagement_index=engagement_index,
        overdue_rate=overdue_rate,
        today_hijri=today_hijri,
        today_greg=today_greg,
        ay_period_label=ay_period_label,
        all_departments=all_departments,
        is_admin=role == "admin",
        classes_in_dept=classes_in_dept,
    )


# --------------------------------------------------
# QUICK DEPARTMENT SELECTION FOR ADMIN
# --------------------------------------------------
@bp.route("/quick-select")
def quick_select():
    """Quick department selection endpoint for admin"""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role != "admin":
        flash("Admin access required.", "danger")
        return redirect(url_for("dashboard_bp.dashboard"))

    dept_code = request.args.get("dept")
    if not dept_code:
        flash("No department selected.", "warning")
        return redirect(url_for("hod_dashboard_bp.dashboard"))

    # Redirect to dashboard with selected department
    return redirect(url_for("hod_dashboard_bp.dashboard", dept=dept_code))


# --------------------------------------------------
# DEBUG ENDPOINT
# --------------------------------------------------
@bp.route("/debug/department/<dept_code>")
def debug_department(dept_code):
    """Debug endpoint to check department information"""
    if not session.get("logged_in"):
        return "Not logged in", 401
    
    role = (session.get("role") or "").lower()
    if role not in ("hod", "admin"):
        return "Access denied", 403
    
    output = []
    output.append(f"<h1>Debug Department: {dept_code}</h1>")
    output.append(f"<p><a href='/hod'>Back to HOD Dashboard</a></p>")
    
    # 1. Check Koha categories
    conn = get_koha_conn()
    cur = conn.cursor()
    
    output.append("<h2>1. Koha Categories</h2>")
    cur.execute("SELECT categorycode, description FROM categories WHERE categorycode LIKE %s OR description LIKE %s", 
                (f"%{dept_code}%", f"%{dept_code}%"))
    categories = cur.fetchall()
    
    if categories:
        output.append("<table border='1'><tr><th>Code</th><th>Description</th></tr>")
        for code, desc in categories:
            output.append(f"<tr><td>{code}</td><td>{desc}</td></tr>")
        output.append("</table>")
    else:
        output.append("<p>No matching categories found.</p>")
    
    # 2. Check borrowers in this category
    output.append("<h2>2. Active Borrowers in Category</h2>")
    cur.execute("""
        SELECT COUNT(*) as count 
        FROM borrowers 
        WHERE categorycode = %s 
          AND (dateexpiry IS NULL OR dateexpiry >= CURDATE())
          AND (debarred IS NULL OR debarred = 0 OR debarred = '0')
          AND COALESCE(gonenoaddress,0) = 0
          AND COALESCE(lost,0) = 0
    """, (dept_code,))
    
    count_row = cur.fetchone()
    borrower_count = count_row[0] if count_row else 0
    output.append(f"<p>Active borrowers: {borrower_count}</p>")
    
    # 3. List some borrowers
    if borrower_count > 0:
        output.append("<h3>Sample Borrowers</h3>")
        cur.execute("""
            SELECT borrowernumber, cardnumber, surname, firstname, categorycode
            FROM borrowers 
            WHERE categorycode = %s 
            LIMIT 10
        """, (dept_code,))
        
        borrowers = cur.fetchall()
        output.append("<table border='1'><tr><th>ID</th><th>Card</th><th>Name</th><th>Category</th></tr>")
        for b in borrowers:
            output.append(f"<tr><td>{b[0]}</td><td>{b[1]}</td><td>{b[2]}, {b[3]}</td><td>{b[4]}</td></tr>")
        output.append("</table>")
    
    cur.close()
    conn.close()
    
    # 4. Test department_report function
    output.append("<h2>3. Test department_report() Function</h2>")
    try:
        from routes.reports import department_report
        result = department_report(dept_code)
        
        if isinstance(result, tuple):
            df, total_students = result
            output.append(f"<p>Total students: {total_students}</p>")
            output.append(f"<p>DataFrame type: {type(df)}</p>")
            if hasattr(df, 'shape'):
                output.append(f"<p>DataFrame shape: {df.shape}</p>")
                if df.shape[0] > 0:
                    output.append("<h4>First few rows:</h4>")
                    output.append(df.head().to_html())
        else:
            output.append(f"<p>Result type: {type(result)}</p>")
            if isinstance(result, pd.DataFrame):
                output.append(f"<p>DataFrame shape: {result.shape}</p>")
    
    except Exception as e:
        output.append(f"<p style='color: red;'>Error: {str(e)}</p>")
        output.append(f"<pre>{traceback.format_exc()}</pre>")
    
    # 5. Test get_department_kpis
    output.append("<h2>4. Test get_department_kpis()</h2>")
    try:
        kpis = get_department_kpis(dept_code)
        output.append(f"<p>KPIs: {kpis}</p>")
    except Exception as e:
        output.append(f"<p style='color: red;'>Error: {str(e)}</p>")
    
    return "<br>".join(output)


# --------------------------------------------------
# DEPARTMENT DETAILS API
# --------------------------------------------------
@bp.route("/api/department/<dept_code>")
def api_department_details(dept_code):
    """API endpoint to get detailed department information"""
    if not session.get("logged_in"):
        return jsonify({"error": "Not authenticated"}), 401

    try:
        # Get department details
        conn = get_koha_conn()
        cur = conn.cursor(dictionary=True)
        
        # Get department info
        cur.execute("""
            SELECT 
                c.categorycode AS dept_code,
                c.description AS dept_name,
                c.enrolmentperiod,
                NULL AS overduefinescap
            FROM categories c
            WHERE c.categorycode = %s
            LIMIT 1
        """, (dept_code,))
        
        dept_info = cur.fetchone()
        if not dept_info:
            return jsonify({"error": "Department not found"}), 404
        
        # Get department stats
        total_borrowers, active_borrowers, ay_issues, ay_fines, active_loans, overdues = _get_accurate_department_stats(dept_code)
        
        # Get classes in department
        classes = get_classes_in_department(dept_code)
        
        cur.close()
        conn.close()
        
        return jsonify({
            "success": True,
            "dept_info": dept_info,
            "stats": {
                "total_borrowers": total_borrowers,
                "active_borrowers": active_borrowers,
                "ay_issues": ay_issues,
                "ay_fines": ay_fines,
                "active_loans": active_loans,
                "overdues": overdues
            },
            "classes": classes,
            "classes_count": len(classes)
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# --------------------------------------------------
# GET DEPARTMENTS API (for dropdowns)
# --------------------------------------------------
@bp.route("/api/departments")
def api_departments():
    """API endpoint to get all departments for dropdowns"""
    if not session.get("logged_in"):
        return jsonify({"error": "Not authenticated"}), 401

    role = (session.get("role") or "").lower()
    if role != "admin":
        return jsonify({"error": "Admin access required"}), 403

    try:
        departments = get_all_departments()
        return jsonify({
            "success": True,
            "departments": departments,
            "count": len(departments)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# --------------------------------------------------
# SEARCH
# --------------------------------------------------
@bp.route("/search")
def search():
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role not in ("hod", "admin"):
        return redirect(url_for("auth_bp.login"))

    query = (request.args.get("q") or "").strip()

    if role == "hod":
        dept_name = session.get("department_name")
    else:
        dept_name = request.args.get("dept") or session.get("department_name")

    if not query:
        return redirect(url_for("hod_dashboard_bp.dashboard"))

    # Get department report data
    report_data = department_report(dept_name)
    
    # Handle the tuple return
    if isinstance(report_data, tuple):
        df, total_students = report_data
    else:
        df = report_data
    
    if not isinstance(df, pd.DataFrame) or df.empty:
        flash("⚠️ No records found for your department.", "warning")
        return redirect(url_for("hod_dashboard_bp.dashboard"))

    students, classes = [], []

    # Student matches by name or TR
    if "FullName" in df.columns and "TRNumber" in df.columns:
        mask_students = (
            df["FullName"].str.contains(query, case=False, na=False)
            | df["TRNumber"].astype(str).str.contains(query, case=False, na=False)
        )
        students = df[mask_students].to_dict("records")

    # Class matches
    if "Class" in df.columns:
        class_df = (
            df[df["Class"].astype(str).str.contains(query, case=False, na=False)]
            .groupby("Class")[["Issues_AY", "FinesPaid_AY", "ActiveLoans", "Overdues"]]
            .agg("sum")
            .reset_index()
        )
        class_df["StudentCount"] = class_df["Class"].map(df["Class"].value_counts())
        class_df = class_df.rename(columns={"Class": "ClassName"})
        classes = class_df.to_dict("records")

    return render_template(
        "hod_search_results.html",
        dept_name=dept_name,
        query=query,
        students=students,
        classes=classes,
    )


# --------------------------------------------------
# DOWNLOADS
# --------------------------------------------------
@bp.route("/download/department/pdf")
def download_department_pdf():
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role not in ("hod", "admin"):
        return redirect(url_for("auth_bp.login"))

    if role == "hod":
        dept_name = session.get("department_name")
    else:
        dept_name = request.args.get("dept") or session.get("department_name")

    # Get department report data
    report_data = department_report(dept_name)
    
    # Handle the tuple return
    if isinstance(report_data, tuple):
        df, total_students = report_data
    else:
        df = report_data
    
    if not isinstance(df, pd.DataFrame) or df.empty:
        flash("⚠️ No data found for your department.", "warning")
        return redirect(url_for("hod_dashboard_bp.dashboard"))

    # Get department display name
    all_depts = get_all_departments()
    dept_display_name = dept_name
    for dept in all_depts:
        if dept["code"] == dept_name:
            dept_display_name = dept["name"]
            break

    pdf_bytes = dataframe_to_pdf_bytes(f"Department Report - {dept_display_name}", df)
    return send_file(
        BytesIO(pdf_bytes),
        as_attachment=True,
        download_name=f"department_report_{dept_name}.pdf",
        mimetype="application/pdf",
    )


@bp.route("/download/class/<class_name>")
def download_class_pdf(class_name):
    """Generate class PDF but skip blank or incomplete student rows."""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role not in ("hod", "admin"):
        return redirect(url_for("auth_bp.login"))

    # Get class report data
    report_data = class_report(class_name, dept_filter=session.get("department_name") if role == "hod" else None)
    
    # Handle the tuple return
    if isinstance(report_data, tuple):
        df, total_students = report_data
    else:
        df = report_data
    
    if not isinstance(df, pd.DataFrame) or df.empty:
        flash(f"⚠️ No data found for class {class_name}.", "warning")
        return redirect(url_for("hod_dashboard_bp.dashboard"))

    # Drop incomplete or blank rows
    df = df.dropna(subset=["FullName", "TRNumber"], how="all")
    df = df[~df["FullName"].isin(["", None, "NaN", "nan"])]
    df = df[~df["TRNumber"].isin(["", None, "NaN", "nan"])]

    if df.empty:
        flash(f"⚠️ All rows in class {class_name} were empty and skipped.", "warning")
        return redirect(url_for("hod_dashboard_bp.dashboard"))

    df = df.fillna("")
    if "Titles_AY" in df.columns:
        df["Titles_AY"] = df["Titles_AY"].astype(str).apply(
            lambda x: x[:250] + "…" if len(x) > 250 else x
        )

    pdf_bytes = dataframe_to_pdf_bytes(f"Class Report - {class_name}", df)
    return send_file(
        BytesIO(pdf_bytes),
        as_attachment=True,
        download_name=f"class_report_{class_name}.pdf",
        mimetype="application/pdf",
    )


@bp.route("/download/student/<identifier>")
def download_student_pdf(identifier):
    """Download individual student report with photo."""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role not in ("hod", "admin"):
        return redirect(url_for("auth_bp.login"))

    info = get_student_info(identifier)
    if not info:
        flash("⚠️ Student not found.", "warning")
        return redirect(url_for("hod_dashboard_bp.dashboard"))

    font_name = _ensure_font_registered()
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    styles = getSampleStyleSheet()
    for key in ("Title", "Normal", "Heading2", "Heading3"):
        styles[key].fontName = font_name

    elements = [
        Paragraph(f"Student Report - {info.get('FullName','')}", styles["Title"]),
        Spacer(1, 0.3 * cm),
    ]

    photo_path = os.path.join(
        current_app.root_path, "static", info.get("Photo", "images/avatar.png")
    )
    if not os.path.exists(photo_path):
        photo_path = os.path.join(
            current_app.root_path, "static", "images", "avatar.png"
        )
    if os.path.exists(photo_path):
        elements.append(Image(photo_path, width=4 * cm, height=4 * cm))
        elements.append(Spacer(1, 0.3 * cm))

    metrics = info.get("Metrics", {})
    data = [
        ["📚 Lifetime Issues", metrics.get("LifetimeIssues", 0)],
        ["🗓️ AY Issues", metrics.get("AYIssues", 0)],
        ["📖 Active Loans", metrics.get("ActiveLoans", 0)],
        ["⏰ Overdue Now", metrics.get("OverdueNow", 0)],
        ["💰 Fines Paid", f"{metrics.get('TotalFinesPaid', 0):.2f}"],
        ["🏷️ Class", info.get("Class") or "-"],
        ["🧾 TR Number", info.get("TRNumber") or "-"],
    ]
    t = Table(data, colWidths=[6 * cm, 6 * cm])
    t.setStyle(
        TableStyle(
            [
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("BACKGROUND", (0, 0), (-1, 0), colors.whitesmoke),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )
    )
    elements.append(t)

    doc.build(elements)
    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name=f"student_report_{identifier}.pdf",
        mimetype="application/pdf",
    )


# --------------------------------------------------
# STUDENT DETAILS
# --------------------------------------------------
@bp.route("/student/<identifier>")
def student_details(identifier):
    """View detailed student information"""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role not in ("hod", "admin"):
        return redirect(url_for("auth_bp.login"))

    if role == "hod":
        dept_name = session.get("department_name")
    else:
        dept_name = request.args.get("dept") or session.get("department_name")

    # Get student info
    info = get_student_info(identifier)
    if not info:
        flash("⚠️ Student not found.", "warning")
        return redirect(url_for("hod_dashboard_bp.dashboard"))

    # Get department display name
    all_depts = get_all_departments()
    dept_display_name = dept_name
    for dept in all_depts:
        if dept["code"] == dept_name:
            dept_display_name = dept["name"]
            break

    return render_template(
        "hod_student_details.html",
        dept_name=dept_name,
        dept_display_name=dept_display_name,
        student=info,
        is_admin=role == "admin"
    )