# routes/teacher_dashboard.py

from flask import (
    Blueprint, render_template, session, redirect, url_for,
    flash, current_app, request, send_file, jsonify
)
from datetime import date, datetime
from io import BytesIO
import urllib.parse
import re

from reportlab.lib.pagesizes import A4, landscape
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
    make_doc_with_header_footer,
)
from routes.reports import class_report
from routes.students import get_student_info
from db_koha import get_koha_conn

import pandas as pd
import os


bp = Blueprint("teacher_dashboard_bp", __name__)

OPAC_BASE = "https://library-opac.ajsn.co.ke"


# --------------------------------------------------
# HIJRI DATE HELPERS
# --------------------------------------------------
def _hijri_date_label(d: date) -> str:
    try:
        from hijri_converter import convert
        h = convert.Gregorian(d.year, d.month, d.day).to_hijri()
        hijri_months = [
            "Muḥarram al-Harām", "Safar al-Muzaffar", "Rabi al-Awwal", "Rabī al-Aakhar",
            "Jamādil Awwal", "Jamādā al-ʾŪkhrā", "Rajab al-Asab", "Shabān al-Karim",
            "Shehrullah al-Moazzam", "Shawwāl al-Mukarram", "Zilqādah al-Harām", "Zilhijjatil Harām",
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
            "Muḥarram al-Harām", "Safar al-Muzaffar", "Rabi al-Awwal", "Rabī al-Aakhar",
            "Jamādil Awwal", "Jamādā al-ʾŪkhrā", "Rajab al-Asab", "Shabān al-Karim",
            "Shehrullah al-Moazzam", "Shawwāl al-Mukarram", "Zilqādah al-Harām", "Zilhijjatil Harām",
        ]
        return f"{hijri_months[h.month - 1]} {h.year} H"
    except Exception:
        return d.strftime("%B %Y")


def _hijri_from_any(value) -> str:
    """
    Convert a value (date/datetime/ISO string) to a short Hijri date label.
    Uses a compact format so it fits in PDF table columns.
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
        return _hijri_date_label_short(g)
    except Exception:
        return str(value)


# --------------------------------------------------
# AY RANGE (Academic Year: 1 April → 31 December)
# --------------------------------------------------
def _ay_bounds():
    today = date.today()

    if 4 <= today.month <= 12:
        start = date(today.year, 4, 1)
        end = today
    else:
        ay_year = today.year - 1
        start = date(ay_year, 4, 1)
        end = date(ay_year, 12, 31)

    return start, end


# --------------------------------------------------
# HELPER: FILTER BorrowedBooksGrouped TO AY ONLY
# --------------------------------------------------
def _filter_borrowed_ay(borrowed_grouped):
    """
    borrowed_grouped: list of (month_label, [book_dicts])
    Returns same structure but only books whose date_issued is within AY.
    """
    if not borrowed_grouped:
        return []

    start, end = _ay_bounds()
    filtered = []

    for month_label, books in borrowed_grouped:
        if not books:
            continue
        kept_books = []
        for b in books:
            issued_raw = b.get("date_issued")
            issued_date = None

            if isinstance(issued_raw, datetime):
                issued_date = issued_raw.date()
            elif isinstance(issued_raw, date):
                issued_date = issued_raw
            elif issued_raw:
                try:
                    issued_str = str(issued_raw).split(" ")[0]
                    issued_date = datetime.strptime(issued_str, "%Y-%m-%d").date()
                except Exception:
                    issued_date = None

            if issued_date and start <= issued_date <= end:
                kept_books.append(b)

        if kept_books:
            filtered.append((month_label, kept_books))

    return filtered


# --------------------------------------------------
# CLASS TREND (AY, using statistics table)
# --------------------------------------------------
def _class_ay_trend(class_name: str):
    start, end = _ay_bounds()
    if not start:
        return [], [], ""

    conn = get_koha_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT DATE_FORMAT(s.`datetime`, '%Y-%m') AS ym,
               COUNT(*) AS cnt
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
          AND DATE(s.`datetime`) BETWEEN %s AND %s
          AND (std.attribute = %s OR b.branchcode = %s)
          AND trno.attribute IS NOT NULL
        GROUP BY ym
        ORDER BY ym;
        """,
        (start, end, class_name, class_name),
    )

    rows = cur.fetchall()
    cur.close()
    conn.close()

    by_month = {ym: int(cnt) for ym, cnt in (rows or [])}

    labels = []
    values = []

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
# CURRENT MONTH SUMMARY PER STUDENT (this Darajah)
# --------------------------------------------------
def _class_current_month_summary(class_name: str):
    today = date.today()
    month_start = date(today.year, today.month, 1)
    month_end = today

    conn = get_koha_conn()
    cur = conn.cursor(dictionary=True)

    cur.execute(
        """
        SELECT
          trno.attribute AS TRNo,
          CONCAT(
            COALESCE(b.surname, ''),
            CASE WHEN b.surname IS NOT NULL AND b.firstname IS NOT NULL THEN ' ' ELSE '' END,
            COALESCE(b.firstname, '')
          ) AS FullName,
          COUNT(*) AS BooksMonth,
          GROUP_CONCAT(DISTINCT bib.title ORDER BY bib.title SEPARATOR ' • ')
              AS TitlesMonth,
          GROUP_CONCAT(DISTINCT it.ccode ORDER BY it.ccode SEPARATOR ', ')
              AS CollectionsMonth
        FROM statistics s
        JOIN borrowers b
             ON b.borrowernumber = s.borrowernumber
        LEFT JOIN borrower_attributes std
             ON std.borrowernumber = b.borrowernumber
            AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
        LEFT JOIN borrower_attributes trno
             ON trno.borrowernumber = b.borrowernumber
            AND trno.code = 'TRNO'
        JOIN items it
             ON s.itemnumber = it.itemnumber
        JOIN biblio bib
             ON it.biblionumber = bib.biblionumber
        WHERE s.type = 'issue'
          AND DATE(s.`datetime`) BETWEEN %s AND %s
          AND (std.attribute = %s OR b.branchcode = %s)
          AND trno.attribute IS NOT NULL
        GROUP BY trno.attribute, b.borrowernumber
        ORDER BY BooksMonth DESC;
        """,
        (month_start, month_end, class_name, class_name),
    )

    rows = cur.fetchall()
    cur.close()
    conn.close()

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

        # Handle empty names properly
        if not full_name or full_name.strip() == "" or str(full_name).lower() == "none":
            full_name = "Name not available"
        else:
            # Clean up any whitespace
            full_name = full_name.strip()

        summary_by_trno[tr] = {
            "full_name": full_name,
            "books_month": books_month,
            "titles_month": titles_month,
            "collections_month": collections_month,
        }

        if collections_month:
            for cc in collections_month.split(","):
                c = cc.strip()
                if c:
                    collections_all.add(c)

    collections_summary = ", ".join(sorted(collections_all)) if collections_all else ""
    return summary_by_trno, collections_summary


# --------------------------------------------------
# CURRENT OVERDUES PER STUDENT (active loans overdue)
# --------------------------------------------------
def _class_current_overdues(class_name: str):
    today = date.today()

    conn = get_koha_conn()
    cur = conn.cursor(dictionary=True)

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
          AND iss.returndate IS NULL
          AND iss.date_due < %s
        GROUP BY trno.attribute, b.borrowernumber;
        """,
        (class_name, class_name, today),
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


# --------------------------------------------------
# TOP TITLES FOR THIS CLASS (AY, Arabic / English)
# --------------------------------------------------
def _class_top_titles_by_lang(class_name: str, lang_pattern: str, limit: int = 10):
    start, end = _ay_bounds()
    conn = get_koha_conn()
    cur = conn.cursor(dictionary=True)

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
            COUNT(*) AS Times_Issued
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
        (start, end, class_name, class_name, lang_pattern, int(limit)),
    )

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows or []


# --------------------------------------------------
# HELPER: PARSE CLASS NAME FOR DISPLAY - UPDATED
# --------------------------------------------------
def _parse_class_name(class_name: str):
    """
    Parse class names like '5 B M', '5 B F', '7A', '7AF', etc.
    Returns a dict with parsed components.
    Format: "Class 1 B F" instead of "Year 1 • Section B • Girls"
    """
    if not class_name:
        return {
            "original": "", 
            "display": "", 
            "year": "", 
            "section": "", 
            "gender": "",
            "gender_code": "",
            "is_arabic": False
        }
    
    original = class_name.strip()
    
    # Normalize the class name for parsing
    normalized = original.upper().replace(" ", "")
    
    # Extract year (numeric part at beginning)
    year_match = re.match(r'^(\d+)', normalized)
    year = year_match.group(1) if year_match else ""
    
    # Remove year from the string for further parsing
    remaining = normalized[len(year):] if year else normalized
    
    # Initialize variables
    section = ""
    gender_code = ""
    gender = ""
    
    # Parse section and gender code
    if remaining:
        # Look for patterns like "BM", "BF", "AM", "AF" or single letters
        if len(remaining) >= 2:
            # Check if it's a combination like "BM" or "BF"
            if remaining[0].isalpha() and remaining[1] in ['M', 'F']:
                section = remaining[0]
                gender_code = remaining[1]
            elif remaining[0] in ['M', 'F']:
                # Just gender code, no section
                gender_code = remaining[0]
            elif remaining[0].isalpha():
                # Just section, no gender code
                section = remaining[0]
                if len(remaining) > 1 and remaining[1] in ['M', 'F']:
                    gender_code = remaining[1]
        elif len(remaining) == 1:
            if remaining[0] in ['M', 'F']:
                gender_code = remaining[0]
            elif remaining[0].isalpha():
                section = remaining[0]
    
    # Also check the original (with spaces) for better parsing
    if " " in original:
        parts = original.upper().split()
        if len(parts) >= 3:
            # Format: "5 B M" or "1 A F"
            if parts[1].isalpha() and len(parts[1]) == 1:
                section = parts[1]
            if parts[-1] in ['M', 'F']:
                gender_code = parts[-1]
        elif len(parts) == 2:
            # Format: "5 M" or "5 F"
            if parts[1] in ['M', 'F']:
                gender_code = parts[1]
    
    # Determine gender from code
    if gender_code == 'M':
        gender = "Boys"
    elif gender_code == 'F':
        gender = "Girls"
    else:
        # Try to infer from original string
        if "M" in original.upper() or "BOYS" in original.upper():
            gender = "Boys"
            gender_code = "M"
        elif "F" in original.upper() or "GIRLS" in original.upper():
            gender = "Girls"
            gender_code = "F"
        else:
            gender = "Mixed"
            gender_code = ""
    
    # Build display string in "Class X Y Z" format
    display_parts = []
    if year:
        display_parts.append(f"Class {year}")
        if section:
            display_parts.append(section)
        if gender_code:
            display_parts.append(gender_code)
    
    display = " ".join(display_parts) if display_parts else original
    
    # Check if it's an Arabic class
    is_arabic = "عربي" in original or "ARABIC" in original.upper()
    
    return {
        "original": original,
        "display": display,  # Will be "Class 5 B M" or "Class 1 A F"
        "year": year,
        "section": section,
        "gender": gender,
        "gender_code": gender_code,
        "is_arabic": is_arabic
    }


# --------------------------------------------------
# CLASS EXPLORER (for admin to browse all classes)
# --------------------------------------------------
@bp.route("/class-explorer")
def class_explorer():
    """Class explorer page for admin to browse all classes"""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role != "admin":
        flash("Admin access required for class explorer.", "danger")
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
    
    # Get all classes
    all_classes = get_all_classes()
    
    # Get stats for each class
    classes_with_stats = []
    for class_info in all_classes:
        class_name = class_info["name"]
        
        # Get accurate stats using the SAME logic as teacher dashboard
        try:
            # Use the class_report function for accurate student count
            df, accurate_total_students = class_report(class_name)
            
            # Count active students with TR numbers (same as dashboard)
            if not df.empty:
                # Filter out invalid TRNumbers
                df = df.dropna(subset=["TRNumber"])
                df["TRNumber"] = df["TRNumber"].astype(str).str.strip()
                df = df[df["TRNumber"] != ""]  # Remove empty TRNumbers
                active_students = len(df)
            else:
                active_students = 0
            
            # Get AY issues count (same as dashboard)
            start, end = _ay_bounds()
            conn = get_koha_conn()
            cur = conn.cursor(dictionary=True)
            
            cur.execute(
                """
                SELECT COUNT(*) as ay_issues
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
                  AND DATE(s.`datetime`) BETWEEN %s AND %s
                  AND (std.attribute = %s OR b.branchcode = %s)
                  AND trno.attribute IS NOT NULL
                """,
                (start, end, class_name, class_name)
            )
            issues_row = cur.fetchone()
            ay_issues = issues_row["ay_issues"] if issues_row else 0
            
            cur.close()
            conn.close()
            
        except Exception as e:
            # Fallback if class_report fails
            accurate_total_students = 0
            active_students = 0
            ay_issues = 0
        
        # Determine icon based on gender
        icon = "users"  # default
        gender = class_info.get("gender", "")
        if "Boys" in gender:
            icon = "male"
        elif "Girls" in gender:
            icon = "female"
        
        # Extract class number for grouping (e.g., "5" from "5 B M")
        class_number = class_info.get("year", "")
        if not class_number:
            # Try to extract from class name directly
            match = re.match(r'^(\d+)', class_name)
            class_number = match.group(1) if match else "Other"
        
        # Use the parsed display name
        parsed = _parse_class_name(class_name)
        
        classes_with_stats.append({
            "class_name": class_name,  # Original like "5 B M"
            "class_number": class_number,  # Just the number "5"
            "display": parsed["display"],  # Use parsed display format
            "section": parsed["section"],
            "gender": parsed["gender"],
            "gender_code": parsed["gender_code"],
            "icon": icon,
            "total_students": accurate_total_students,  # Use accurate count
            "active_students": active_students,  # This should match dashboard
            "ay_issues": ay_issues
        })
    
    # Group classes by class number instead of "Year X"
    classes_by_number = {}
    for class_info in classes_with_stats:
        class_num = class_info["class_number"]
        if class_num not in classes_by_number:
            classes_by_number[class_num] = []
        classes_by_number[class_num].append(class_info)
    
    # Sort class numbers numerically
    sorted_numbers = sorted(
        [num for num in classes_by_number.keys() if num and num.isdigit()], 
        key=lambda x: int(x)
    )
    # Add "Other" if exists
    if "Other" in classes_by_number:
        sorted_numbers.append("Other")
    
    # Sort classes within each number
    for class_num in classes_by_number:
        classes_by_number[class_num].sort(key=lambda x: (
            x["section"] or "", 
            x["gender"] or "",
            x["class_name"] or ""
        ))
    
    return render_template(
        "class_explorer.html",
        hijri_today=hijri_today,
        total_classes=len(classes_with_stats),
        classes_by_year=classes_by_number,  # Still using same variable name for template compatibility
        sorted_years=sorted_numbers  # Still using same variable name for template compatibility
    )

# --------------------------------------------------
# GET ALL AVAILABLE CLASSES FOR ADMIN
# --------------------------------------------------
def get_all_classes():
    """
    Get all distinct classes from Koha for admin selection.
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
        WHERE COALESCE(std.attribute, b.branchcode) IS NOT NULL
            AND COALESCE(std.attribute, b.branchcode) != ''
        ORDER BY 
            CAST(REGEXP_SUBSTR(COALESCE(std.attribute, b.branchcode), '^[0-9]+') AS UNSIGNED),
            COALESCE(std.attribute, b.branchcode)
    """)
    
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
                "display": parsed["display"],  # Use the new display format
                "year": parsed["year"],
                "section": parsed["section"],
                "gender": parsed["gender"],
                "gender_code": parsed["gender_code"],
                "is_arabic": parsed["is_arabic"]
            })
    
    return classes


# --------------------------------------------------
# TEACHER DASHBOARD (Darajah Masool / Admin view)
# --------------------------------------------------
@bp.route("/")
def dashboard():
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role not in ("teacher", "admin"):
        flash("You must be a class teacher or admin to view this dashboard.", "danger")
        return redirect(url_for("auth_bp.login"))

    username = session.get("username")

    # Determine class
    if role == "teacher":
        class_name = session.get("class_name")
    else:  # admin can impersonate any class via querystring
        class_name = (
            request.args.get("class")
            or request.args.get("class_name")
            or session.get("class_name")
        )

    # Helper to always render the template with safe defaults
    def _render_empty_dashboard(extra_message=None):
        if extra_message:
            flash(extra_message, "warning")

        # Get all classes for admin dropdown
        all_classes = []
        if role == "admin":
            all_classes = get_all_classes()

        parsed_class = _parse_class_name(class_name or "")
        
        return render_template(
            "teacher_dashboard.html",
            class_name=class_name or "",
            parsed_class=parsed_class,
            darajah_masool_name=username or "",
            class_report_title="Class Library Report",
            current_month_label="",
            total_students=0,
            books_issued_class=0,
            total_overdues_current=0,
            total_overdues_ay=0,
            total_fines=0.0,
            trend_labels=[],
            trend_values=[],
            trend_period_label="",
            ay_period_label="",
            top_students=[],
            top_arabic=[],
            top_english=[],
            students=[],
            month_students=[],
            collections_summary="",
            all_classes=all_classes,
            is_admin=role == "admin",
        )

    # No class linked / selected
    if not class_name:
        if role == "admin":
            # Get all classes for dropdown
            all_classes = get_all_classes()
            
            # Show a selection page instead of empty dashboard
            return render_template(
                "teacher_dashboard_select.html",
                all_classes=all_classes,
                username=username,
                is_admin=True
            )
        else:
            return _render_empty_dashboard("⚠️ Your account is not linked to any class.")

    # Parse class name for better display
    parsed_class = _parse_class_name(class_name)
    
    # Main class report - using updated class_report function
    try:
        df, total_students = class_report(class_name)
    except Exception as e:
        flash(f"Error loading class report: {str(e)}", "danger")
        return _render_empty_dashboard(f"Error loading data for {class_name}")

    if df.empty:
        return _render_empty_dashboard(f"No records found for {class_name}.")

    # Ensure FullName column is properly formatted and handle None values
    if "FullName" not in df.columns:
        if {"surname", "firstname"}.issubset(df.columns):
            df["FullName"] = (
                df["surname"].fillna("") + " " + df["firstname"].fillna("")
            ).str.strip()
        else:
            df["FullName"] = df.get("FullName", "").fillna("")
    
    # Handle empty names properly
    def clean_name(name):
        if pd.isna(name) or name is None or str(name).strip() == "" or str(name).lower() == "none":
            return "Name not available"
        return str(name).strip()
    
    df["FullName"] = df["FullName"].apply(clean_name)

    if "Issues_AY" not in df.columns:
        df["Issues_AY"] = 0

    # Remove rows with invalid TRNumber
    df = df.dropna(subset=["TRNumber"])
    df["TRNumber"] = df["TRNumber"].astype(str).str.strip()
    
    # Filter out empty TRNumbers
    df = df[df["TRNumber"] != ""]
    
    df["Issues_AY"] = (
        pd.to_numeric(df["Issues_AY"], errors="coerce").fillna(0).astype(int)
    )

    # Current month (statistics)
    month_summary, collections_summary = _class_current_month_summary(class_name)
    overdues_current = _class_current_overdues(class_name)

    month_rows = []
    for tr, ms in month_summary.items():
        month_rows.append(
            {
                "TRNumber": tr,
                "FullName": ms["full_name"],
                "Books_CurrentMonth": ms["books_month"],
                "Titles_CurrentMonth": ms["titles_month"],
                "Collections_CurrentMonth": ms["collections_month"],
                "Overdues_Current": overdues_current.get(tr, 0),
            }
        )

    month_df = pd.DataFrame(month_rows)
    if not month_df.empty:
        month_df = month_df.sort_values(
            ["Books_CurrentMonth", "FullName"], ascending=[False, True]
        )

    # KPIs
    df_sorted = df.sort_values("Issues_AY", ascending=False)

    trend_labels, trend_values, trend_period_label = _class_ay_trend(class_name)
    books_issued_class = int(sum(trend_values))

    total_overdues_ay = int(
        pd.to_numeric(df_sorted.get("Overdues", 0), errors="coerce").fillna(0).sum()
    )

    if month_rows:
        total_overdues_current = int(
            sum(r["Overdues_Current"] for r in month_rows)
        )
    else:
        total_overdues_current = 0

    total_fines = float(
        pd.to_numeric(df_sorted.get("FinesPaid_AY", 0.0), errors="coerce")
        .fillna(0.0)
        .sum()
    )

    top_students = df_sorted.head(5).to_dict("records")
    ay_period_label = trend_period_label

    top_arabic = _class_top_titles_by_lang(class_name, "%Arabic%", limit=10)
    top_english = _class_top_titles_by_lang(class_name, "%English%", limit=10)

    darajah_masool_name = username
    today = date.today()
    current_month_label = _hijri_month_year_label(today)
    
    # Use the new display format for the report title
    class_report_title = f"{parsed_class['display']} – Library Report"

    # Get all classes for admin dropdown
    all_classes = []
    if role == "admin":
        all_classes = get_all_classes()

    return render_template(
        "teacher_dashboard.html",
        class_name=class_name,
        parsed_class=parsed_class,
        darajah_masool_name=darajah_masool_name,
        class_report_title=class_report_title,
        current_month_label=current_month_label,
        total_students=total_students,
        books_issued_class=books_issued_class,
        total_overdues_current=total_overdues_current,
        total_overdues_ay=total_overdues_ay,
        total_fines=total_fines,
        trend_labels=trend_labels,
        trend_values=trend_values,
        trend_period_label=trend_period_label,
        ay_period_label=ay_period_label,
        top_students=top_students,
        top_arabic=top_arabic,
        top_english=top_english,
        students=df_sorted.to_dict(orient="records"),
        month_students=month_df.to_dict(orient="records") if not month_df.empty else [],
        collections_summary=collections_summary,
        all_classes=all_classes,
        is_admin=role == "admin",
        today=date.today().strftime("%Y-%m-%d"),
    )


# --------------------------------------------------
# QUICK CLASS SELECTION FOR ADMIN
# --------------------------------------------------
@bp.route("/quick-select")
def quick_select():
    """Quick class selection endpoint for admin"""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role != "admin":
        flash("Admin access required.", "danger")
        return redirect(url_for("dashboard_bp.dashboard"))

    class_name = request.args.get("class")
    if not class_name:
        flash("No class selected.", "warning")
        return redirect(url_for("teacher_dashboard_bp.dashboard"))

    # Redirect to dashboard with selected class
    return redirect(url_for("teacher_dashboard_bp.dashboard", class_name=class_name))


# --------------------------------------------------
# GET CLASSES API (for dropdowns)
# --------------------------------------------------
@bp.route("/api/classes")
def api_classes():
    """API endpoint to get all classes for dropdowns"""
    if not session.get("logged_in"):
        return jsonify({"error": "Not authenticated"}), 401

    role = (session.get("role") or "").lower()
    if role != "admin":
        return jsonify({"error": "Admin access required"}), 403

    try:
        classes = get_all_classes()
        return jsonify({
            "success": True,
            "classes": classes,
            "count": len(classes)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# --------------------------------------------------
# SEARCH STUDENT (within this class)
# --------------------------------------------------
@bp.route("/search_student")
def search_student():
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role not in ("teacher", "admin"):
        return redirect(url_for("auth_bp.login"))

    if role == "teacher":
        class_name = session.get("class_name")
        class_label = class_name
    else:
        class_name = (
            request.args.get("class")
            or request.args.get("class_name")
            or session.get("class_name")
        )
        class_label = class_name or f"{session.get('username')} (Admin view)"

    if not class_name:
        flash("⚠️ No class selected.", "warning")
        return redirect(url_for("teacher_dashboard_bp.dashboard"))

    username = session.get("username")
    query = (request.args.get("q") or "").strip()
    if not query:
        return redirect(url_for("teacher_dashboard_bp.dashboard"))

    df, total_students = class_report(class_name)
    if df.empty:
        flash("⚠️ No records found for your class.", "warning")
        return redirect(url_for("teacher_dashboard_bp.dashboard"))

    # Ensure FullName column is properly formatted and handle None values
    if "FullName" not in df.columns:
        if {"surname", "firstname"}.issubset(df.columns):
            df["FullName"] = (
                df["surname"].fillna("") + " " + df["firstname"].fillna("")
            ).str.strip()
        else:
            df["FullName"] = df.get("FullName", "").fillna("")
    
    # Handle empty names properly
    def clean_name(name):
        if pd.isna(name) or name is None or str(name).strip() == "" or str(name).lower() == "none":
            return "Name not available"
        return str(name).strip()
    
    df["FullName"] = df["FullName"].apply(clean_name)

    df["TRNumber"] = df["TRNumber"].astype(str).str.strip()

    results = df[
        df["FullName"].str.contains(query, case=False, na=False)
        | df["TRNumber"].str.contains(query, case=False, na=False)
    ]

    if "Issues_AY" in results.columns:
        results = results.sort_values("Issues_AY", ascending=False)

    parsed_class = _parse_class_name(class_name)
    
    return render_template(
        "teacher_dashboard_search.html",
        class_name=class_label or f"{username} ({class_name})",
        parsed_class=parsed_class,
        query=query,
        results=results.to_dict(orient="records"),
    )


# --------------------------------------------------
# UNIFIED DOWNLOAD HANDLER (class or student)
# --------------------------------------------------
@bp.route("/download_report")
def download_report():
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

    return download_class_pdf()


# --------------------------------------------------
# CLASS + STUDENTS PDF REPORT (combined)
# --------------------------------------------------
@bp.route("/download_class/pdf")
def download_class_pdf():
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role not in ("teacher", "admin"):
        return redirect(url_for("auth_bp.login"))

    if role == "teacher":
        class_name = session.get("class_name")
        darajah_masool_name = session.get("username") or ""
    else:
        class_name = (
            request.args.get("class")
            or request.args.get("class_name")
            or session.get("class_name")
        )
        darajah_masool_name = session.get("username") or ""

    if not class_name:
        flash("⚠️ No class selected.", "warning")
        return redirect(url_for("teacher_dashboard_bp.dashboard"))

    df, total_students = class_report(class_name)
    if df.empty:
        flash("⚠️ No data found for your class.", "warning")
        return redirect(url_for("teacher_dashboard_bp.dashboard"))

    for col in ("TRNumber", "FullName", "Issues_AY"):
        if col not in df.columns:
            df[col] = ""

    # Clean up the data
    df = df.dropna(subset=["TRNumber"])
    df["TRNumber"] = df["TRNumber"].astype(str).str.strip()
    df = df[df["TRNumber"] != ""]  # Remove empty TRNumbers
    
    df["Issues_AY"] = (
        pd.to_numeric(df["Issues_AY"], errors="coerce").fillna(0).astype(int)
    )
    
    # Handle empty names properly
    def clean_name(name):
        if pd.isna(name) or name is None or str(name).strip() == "" or str(name).lower() == "none":
            return "Name not available"
        return str(name).strip()
    
    df["FullName"] = df["FullName"].apply(clean_name)

    if "Titles_AY" not in df.columns:
        df["Titles_AY"] = ""
    if "Note" not in df.columns:
        df["Note"] = ""
    if "Overdues" not in df.columns:
        df["Overdues"] = 0
    if "FinesPaid_AY" not in df.columns:
        df["FinesPaid_AY"] = 0.0

    df_sorted = df.sort_values("Issues_AY", ascending=False)

    trend_labels, trend_values, trend_period_label = _class_ay_trend(class_name)
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

    # Parse class name for display
    parsed_class = _parse_class_name(class_name)
    display_name = parsed_class["display"] or class_name
    
    # ✅ Landscape PDF with header title + page numbers
    doc_title = f"Class Library Report – {display_name}"
    doc, on_page = make_doc_with_header_footer(
        buffer,
        title=doc_title,
        landscape_mode=True,
        left_margin=1.5 * cm,
        right_margin=1.5 * cm,
        top_margin=1.5 * cm,
        bottom_margin=1.5 * cm,
    )

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

    # PAGE 1: CLASS SUMMARY
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
        ["Class", S(display_name)],
        ["Original Code", S(class_name)],
        ["Darajah Masool", S(darajah_masool_name)],
        ["Academic Year Period", S(trend_period_label)],
        ["Total Issues (AY, from statistics)", str(total_issues_trend)],
        [
            "Maximum Issues in a Single Month",
            f"{max_month_issues} ({max_month_label})",
        ],
        ["Total Students with TRNo", str(total_students)],
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

    data = [
        [
            "TR No",
            "Full Name",
            "Books Issued",
            "Overdues",
            "Fines Paid",
            "Book Titles",
            "Note",
        ]
    ]

    for _, row in df_sorted.iterrows():
        data.append(
            [
                S(row.get("TRNumber", "")),
                S(row.get("FullName", "")),
                str(row.get("Issues_AY", 0)),
                str(row.get("Overdues", 0)),
                f"{float(row.get('FinesPaid_AY', 0.0)):.2f}",
                S(row.get("Titles_AY", "")),
                S(row.get("Note", "")),
            ]
        )

    table = Table(
        data,
        repeatRows=1,
        colWidths=[2.0 * cm, 5.0 * cm, 3.0 * cm, 3.0 * cm, 3.0 * cm, 9.0 * cm, 3.0 * cm],
    )
    table.setStyle(
        TableStyle(
            [
                ("FONTNAME", (0, 0), (-1, -1), font_name),
                ("FONTSIZE", (0, 0), (-1, -1), 7),
                ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f3e3cf")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#4a2f13")),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )
    )
    elements.append(table)
    elements.append(Spacer(1, 0.5 * cm))

    if trend_labels:
        elements.append(
            Paragraph(
                S("Monthly Issues (Statistics – red below average, green above average)"),
                styles["Heading3"],
            )
        )
        month_data = [["Month", "Issues"]]
        for label, val in zip(trend_labels, trend_values):
            month_data.append([label, str(val)])

        month_table = Table(month_data, colWidths=[10 * cm, 10 * cm])
        month_style = [
            ("FONTNAME", (0, 0), (-1, -1), font_name),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ]
        for i, val in enumerate(trend_values, start=1):
            if avg_month_issues and val < avg_month_issues:
                month_style.append(
                    ("BACKGROUND", (1, i), (1, i), colors.HexColor("#f8d7da"))
                )
            else:
                month_style.append(
                    ("BACKGROUND", (1, i), (1, i), colors.HexColor("#d4edda"))
                )

        month_table.setStyle(TableStyle(month_style))
        elements.append(month_table)

    # STUDENT SECTIONS (AY ONLY)
    for _, row in df_sorted.iterrows():
        elements.append(PageBreak())
        trno = row.get("TRNumber", "")
        info = get_student_info(trno)
        if not info:
            elements.append(
                Paragraph(
                    S(f"Library report not available for TR {trno}"),
                    styles["Normal"],
                )
            )
            continue

        elements.append(
            Paragraph(
                S("Library Report – Academic Year"),
                styles["Title"],
            )
        )
        elements.append(Spacer(1, 0.2 * cm))

        its = info.get("ITS ID") or info.get("ITSNumber") or "-"
        full_name = info.get("FullName") or "-"
        
        # Handle None/empty names in student info
        if not full_name or str(full_name).lower() == "none" or str(full_name).strip() == "":
            full_name = "Name not available"
        
        id_table_data = [
            ["ITS ID", S(str(its))],
            ["TR Number", S(info.get("TRNumber") or str(trno))],
            ["Full Name", S(full_name)],
        ]
        id_table = Table(id_table_data, colWidths=[4 * cm, 12 * cm])
        id_table.setStyle(
            TableStyle(
                [
                    ("FONTNAME", (0, 0), (-1, -1), font_name),
                    ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
                    ("FONTSIZE", (0, 0), (-1, -1), 9),
                    ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#f3e3cf")),
                ]
            )
        )

        photo_rel = info.get("Photo", "images/avatar.png")
        photo_path = os.path.join(current_app.root_path, "static", photo_rel)
        if not os.path.exists(photo_path):
            photo_path = os.path.join(current_app.root_path, "static", "images", "avatar.png")

        if os.path.exists(photo_path):
            img = Image(photo_path)
            img._restrictSize(3.5 * cm, 4.5 * cm)
            header_table = Table(
                [[id_table, img]],
                colWidths=[15.5 * cm, 4.0 * cm],
            )
            header_table.setStyle(
                TableStyle(
                    [
                        ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ]
                )
            )
            elements.append(header_table)
        else:
            elements.append(id_table)

        # AY Issues Table
        ay_data = info.get("AY", [])
        ay_books = [d for d in ay_data if isinstance(d, dict)]
        ay_total_books = len(ay_books)

        elements.append(Spacer(1, 0.3 * cm))
        elements.append(
            Paragraph(
                S(f"Books issued this Academic Year ({ay_total_books})"),
                styles["Heading2"],
            )
        )

        if ay_books:
            ay_table_data = [["Date", "Title", "Call No.", "Collection"]]
            for b in ay_books:
                ay_table_data.append(
                    [
                        _hijri_from_any(b.get("date_issued")),
                        S(b.get("title", "")),
                        S(b.get("call_no", "")),
                        S(b.get("ccode", "")),
                    ]
                )

            ay_table = Table(
                ay_table_data,
                repeatRows=1,
                colWidths=[2.5 * cm, 10.0 * cm, 3.5 * cm, 4.0 * cm],
            )
            ay_table.setStyle(
                TableStyle(
                    [
                        ("FONTNAME", (0, 0), (-1, -1), font_name),
                        ("FONTSIZE", (0, 0), (-1, -1), 7),
                        ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
                        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                        ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
                        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ]
                )
            )
            elements.append(ay_table)
        else:
            elements.append(Paragraph(S("No books issued this Academic Year."), styles["Normal"]))

        # Borrowed Books Grouped (Only AY months)
        borrowed_grouped = info.get("BorrowedBooksGrouped", [])
        if borrowed_grouped:
            filtered_grouped = _filter_borrowed_ay(borrowed_grouped)
            if filtered_grouped:
                elements.append(Spacer(1, 0.3 * cm))
                elements.append(
                    Paragraph(S("Issues per Month (Academic Year)"), styles["Heading2"])
                )

                for month_label, books in filtered_grouped:
                    if not books:
                        continue

                    elements.append(Paragraph(S(month_label), styles["Heading3"]))
                    month_table_data = [["Date", "Title", "Call No.", "Collection"]]
                    for b in books:
                        month_table_data.append(
                            [
                                _hijri_from_any(b.get("date_issued")),
                                S(b.get("title", "")),
                                S(b.get("call_no", "")),
                                S(b.get("ccode", "")),
                            ]
                        )

                    month_table = Table(
                        month_table_data,
                        repeatRows=1,
                        colWidths=[2.5 * cm, 10.0 * cm, 3.5 * cm, 4.0 * cm],
                    )
                    month_table.setStyle(
                        TableStyle(
                            [
                                ("FONTNAME", (0, 0), (-1, -1), font_name),
                                ("FONTSIZE", (0, 0), (-1, -1), 7),
                                ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
                                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                                ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
                                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                            ]
                        )
                    )
                    elements.append(month_table)
                    elements.append(Spacer(1, 0.2 * cm))

    doc.build(elements, onFirstPage=on_page, onLaterPages=on_page)
    buffer.seek(0)
    filename = f"class_{class_name.replace(' ', '_')}_report_{today.strftime('%Y%m%d')}.pdf"
    return send_file(
        buffer,
        as_attachment=True,
        download_name=filename,
        mimetype="application/pdf",
    )


# --------------------------------------------------
# SINGLE STUDENT PDF REPORT
# --------------------------------------------------
@bp.route("/download_student/pdf/<identifier>")
def download_student_pdf(identifier):
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role not in ("teacher", "admin"):
        return redirect(url_for("auth_bp.login"))

    if role == "teacher":
        class_name = session.get("class_name")
        darajah_masool_name = session.get("username") or ""
    else:
        class_name = request.args.get("class") or session.get("class_name")
        darajah_masool_name = session.get("username") or ""

    info = get_student_info(identifier)
    if not info:
        flash(f"⚠️ No data found for student with identifier '{identifier}'.", "warning")
        return redirect(url_for("teacher_dashboard_bp.dashboard"))

    # Handle None/empty names in student info
    full_name = info.get("FullName") or ""
    if not full_name or str(full_name).lower() == "none" or str(full_name).strip() == "":
        full_name = "Name not available"

    font_name = _ensure_font_registered()
    buffer = BytesIO()

    doc_title = f"Library Report – {full_name}"
    doc, on_page = make_doc_with_header_footer(
        buffer,
        title=doc_title,
        landscape_mode=True,
        left_margin=1.5 * cm,
        right_margin=1.5 * cm,
        top_margin=1.5 * cm,
        bottom_margin=1.5 * cm,
    )

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

    # Header with logo
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
        Paragraph(S("Individual Student Library Report"), styles["CenterTitle"])
    )
    elements.append(Spacer(1, 0.1 * cm))

    today = date.today()
    hijri_date_str = _hijri_date_label(today)
    elements.append(Paragraph(hijri_date_str, styles["CenterTitle"]))
    elements.append(Spacer(1, 0.3 * cm))

    # Student Info Table
    its = info.get("ITS ID") or info.get("ITSNumber") or "-"
    trno = info.get("TRNumber") or identifier

    info_table_data = [
        ["ITS ID", S(str(its))],
        ["TR Number", S(trno)],
        ["Full Name", S(full_name)],
        ["Class", S(class_name or info.get("Class", "-"))],
        ["Darajah Masool", S(darajah_masool_name)],
    ]
    info_table = Table(info_table_data, colWidths=[4 * cm, 12 * cm])
    info_table.setStyle(
        TableStyle(
            [
                ("FONTNAME", (0, 0), (-1, -1), font_name),
                ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#f3e3cf")),
            ]
        )
    )

    # Student Photo
    photo_rel = info.get("Photo", "images/avatar.png")
    photo_path = os.path.join(current_app.root_path, "static", photo_rel)
    if not os.path.exists(photo_path):
        photo_path = os.path.join(current_app.root_path, "static", "images", "avatar.png")

    if os.path.exists(photo_path):
        img = Image(photo_path)
        img._restrictSize(3.5 * cm, 4.5 * cm)
        header_table = Table(
            [[info_table, img]],
            colWidths=[15.5 * cm, 4.0 * cm],
        )
        header_table.setStyle(
            TableStyle(
                [
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ]
            )
        )
        elements.append(header_table)
    else:
        elements.append(info_table)

    # AY Issues Table
    ay_data = info.get("AY", [])
    ay_books = [d for d in ay_data if isinstance(d, dict)]
    ay_total_books = len(ay_books)

    elements.append(Spacer(1, 0.3 * cm))
    elements.append(
        Paragraph(
            S(f"Books issued this Academic Year ({ay_total_books})"),
            styles["Heading2"],
        )
    )

    if ay_books:
        ay_table_data = [["Date", "Title", "Call No.", "Collection"]]
        for b in ay_books:
            ay_table_data.append(
                [
                    _hijri_from_any(b.get("date_issued")),
                    S(b.get("title", "")),
                    S(b.get("call_no", "")),
                    S(b.get("ccode", "")),
                ]
            )

        ay_table = Table(
            ay_table_data,
            repeatRows=1,
            colWidths=[2.5 * cm, 10.0 * cm, 3.5 * cm, 4.0 * cm],
        )
        ay_table.setStyle(
            TableStyle(
                [
                    ("FONTNAME", (0, 0), (-1, -1), font_name),
                    ("FONTSIZE", (0, 0), (-1, -1), 7),
                    ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ]
            )
        )
        elements.append(ay_table)
    else:
        elements.append(Paragraph(S("No books issued this Academic Year."), styles["Normal"]))

    # Borrowed Books Grouped (Only AY months)
    borrowed_grouped = info.get("BorrowedBooksGrouped", [])
    if borrowed_grouped:
        filtered_grouped = _filter_borrowed_ay(borrowed_grouped)
        if filtered_grouped:
            elements.append(Spacer(1, 0.3 * cm))
            elements.append(
                Paragraph(S("Issues per Month (Academic Year)"), styles["Heading2"])
            )

            for month_label, books in filtered_grouped:
                if not books:
                    continue

                elements.append(Paragraph(S(month_label), styles["Heading3"]))
                month_table_data = [["Date", "Title", "Call No.", "Collection"]]
                for b in books:
                    month_table_data.append(
                        [
                            _hijri_from_any(b.get("date_issued")),
                            S(b.get("title", "")),
                            S(b.get("call_no", "")),
                            S(b.get("ccode", "")),
                        ]
                    )

                month_table = Table(
                    month_table_data,
                    repeatRows=1,
                    colWidths=[2.5 * cm, 10.0 * cm, 3.5 * cm, 4.0 * cm],
                )
                month_table.setStyle(
                    TableStyle(
                        [
                            ("FONTNAME", (0, 0), (-1, -1), font_name),
                            ("FONTSIZE", (0, 0), (-1, -1), 7),
                            ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
                            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                            ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
                            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                        ]
                    )
                )
                elements.append(month_table)
                elements.append(Spacer(1, 0.2 * cm))

    doc.build(elements, onFirstPage=on_page, onLaterPages=on_page)
    buffer.seek(0)
    filename = f"student_{trno}_{full_name.replace(' ', '_')}_{today.strftime('%Y%m%d')}.pdf"
    return send_file(
        buffer,
        as_attachment=True,
        download_name=filename,
        mimetype="application/pdf",
    )