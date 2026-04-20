# routes/dashboard.py - FULLY UPDATED with fixes for cursor issues and URL building
from datetime import date, datetime
from flask import Blueprint, render_template, session, redirect, url_for, flash, request, jsonify, current_app
from db_koha import get_koha_conn
from services import koha_queries as KQ
import re
import math
import time
from collections import defaultdict

# Optional Hijri conversion
try:
    from hijri_converter import convert as hijri_convert
except ImportError:
    hijri_convert = None

bp = Blueprint("dashboard_bp", __name__)

# OPAC Base URL
def get_opac_base():
    from config import Config
    from flask import session
    bc = session.get("branch_code", "AJSN")
    cfg = Config.CAMPUS_REGISTRY.get(bc, {})
    return cfg.get("opac_url", "https://library-nairobi.jameasaifiyah.org")

# ---------------- AY WINDOW ----------------
def get_academic_year_period(hijri_year=None):
    """Get formatted Academic Year period in Hijri."""
    start, end = KQ.get_ay_bounds(hijri_year)
    
    if not start or not end:
        return "Academic Year not started yet"
    
    try:
        from hijri_converter import convert
        
        # Convert start and end dates to Hijri
        h_start = convert.Gregorian(start.year, start.month, start.day).to_hijri()
        h_end = convert.Gregorian(end.year, end.month, end.day).to_hijri()
        
        hijri_months = [
            "Muḥarram al-Harām", "Safar al-Muzaffar", "Rabi al-Awwal", "Rabī al-Akhar",
            "Jamādil Awwal", "Jamādā al-ʾŪkhrā", "Rajab al-Asab", "Shabān al-Karim",
            "Shehrullah al-Moazzam", "Shawwāl al-Mukarram", "Zilqādah al-Harām", "Zilhijjatil Harām",
        ]
        
        start_month = hijri_months[h_start.month - 1]
        end_month = hijri_months[h_end.month - 1]
        
        return f"{h_start.day} {start_month} {h_start.year} H to {h_end.day} {end_month} {h_end.year} H"
        
    except Exception:
        # Fallback to KQ's Hijri conversion
        return f"{KQ.get_hijri_date_label(start)} to {KQ.get_hijri_date_label(end)}"

def get_current_ay_year():
    """Get current Academic Year for display using Hijri year."""
    try:
        from hijri_converter import convert
        today = date.today()
        h_today = convert.Gregorian(today.year, today.month, today.day).to_hijri()
        
        # Get the Hijri year
        hijri_year = h_today.year
        
        # Check if we're before Shawwal (academic year start)
        # If current month is before Shawwal (month 10), we're in previous academic year
        if h_today.month < 10:
            return hijri_year - 1
        else:
            return hijri_year
    except Exception:
        # Fallback to Gregorian year calculation
        today = date.today()
        if 4 <= today.month <= 12:
            return today.year
        else:
            return today.year - 1

def get_hijri_today() -> str:
    """Get today's date in Hijri format (Full religious name)."""
    today = date.today()
    return KQ.get_hijri_date_label(today)

def get_hijri_date_label(d: date) -> str:
    """Convert Gregorian date to Hijri date label."""
    return KQ.get_hijri_date_label(d)

def _hijri_month_year_label(d: date) -> str:
    """Get Hijri month and year label for charts."""
    return KQ.get_hijri_month_year_label(d)

# ---------------- UTILITIES ----------------
def get_kpis(selected_marhala=None, hijri_year=None):
    """Get key performance indicators for a specific marhala and academic year."""
    s = KQ.get_summary(selected_marhala, hijri_year=hijri_year)
    
    total_patrons = int(s.get("total_patrons", 0))
    active_patrons = int(s.get("active_patrons", 0))
    student_patrons = int(s.get("student_patrons", 0))
    non_student_patrons = int(s.get("non_student_patrons", 0))
    total_issues = int(s.get("total_issues", 0))
    total_fees = float(s.get("fees_paid", 0.0))
    overdue = int(s.get("overdue", 0))
    total_titles = int(s.get("total_titles", 0))
    total_titles_issued = int(s.get("total_titles_issued", 0))
    currently_issued = int(s.get("currently_issued", 0))
    
    # Calculate currently issued fees
    fees_data = calculate_current_fees(selected_marhala, hijri_year=hijri_year)
    currently_issued_fees = fees_data["total"]
    currency = fees_data["currency"]
    
    return {
        "total_patrons": total_patrons,
        "active_patrons": active_patrons,
        "student_patrons": student_patrons,
        "non_student_patrons": non_student_patrons,
        "total_issues": total_issues,
        "total_fees": total_fees,
        "overdue": overdue,
        "total_titles": total_titles,
        "total_titles_issued": total_titles_issued,
        "currently_issued": currently_issued,
        "currently_issued_fees": currently_issued_fees,
        "currency": currency,
        "active_patrons_ay": int(s.get("active_patrons_ay", 0))
    }

def calculate_current_fees(selected_marhala=None, hijri_year=None):
    """Calculate total fees for currently issued books for a specific year."""
    from config import Config
    from flask import session
    bc = session.get("branch_code", "AJSN")
    cfg = Config.CAMPUS_REGISTRY.get(bc, {"currency": "KES", "fine_rate": 10})
    
    currency = cfg.get("currency", "KES")
    fine_rate = cfg.get("fine_rate", 10)
    
    start, end = KQ.get_ay_bounds(hijri_year)
    conn = get_koha_conn()
    try:
        cur = conn.cursor(dictionary=True)
        query = f"""
            SELECT COALESCE(SUM(
                CASE 
                    WHEN i.date_due < CURDATE() 
                    THEN DATEDIFF(CURDATE(), i.date_due) * {fine_rate}
                    ELSE 0 
                END
            ), 0) AS total_fees
            FROM issues i
            JOIN borrowers b ON i.borrowernumber = b.borrowernumber
            LEFT JOIN categories c ON b.categorycode = c.categorycode
            WHERE i.returndate IS NULL
              AND i.date_due < CURDATE()
              AND DATE(i.issuedate) BETWEEN %s AND %s
              AND b.categorycode LIKE 'S%%'
        """
        params = [start, end]
        if selected_marhala:
            query += " AND (c.description = %s OR b.categorycode = %s)"
            params.extend([selected_marhala, selected_marhala])
            
        cur.execute(query, params)
        result = cur.fetchone()
        total_fees = float(result.get("total_fees", 0) if result else 0.0)
        cur.close()

        # 21-DAY GRACE PERIOD: If AY started < 21 days ago, ignore fees
        if start and (date.today() - start).days < 21:
            total_fees = 0.0
            
        return {"total": total_fees, "currency": currency}
    except Exception as e:
        current_app.logger.error(f"Error calculating fees: {e}")
        return {"total": 0.0, "currency": currency}
    finally:
        conn.close()

def get_today_activity():
    """Get today's activity stats."""
    return KQ.today_activity()

def get_trends(selected_marhala=None, hijri_year=None):
    """Get borrowing trends for a specific academic year."""
    try:
        labels, values = KQ.get_ay_trend_data(marhala_code=selected_marhala, hijri_year=hijri_year)
        return labels, values
    except Exception as e:
        current_app.logger.error(f"Error getting trends: {e}")
        return [], []

def get_darajah_distribution(hijri_year=None):
    """Get gender distribution by Darajah with updated ranges."""
    start, end = KQ.get_ay_bounds(hijri_year)
    if not start:
        return [], [], []

    if hasattr(KQ, "get_gender_darajah_distribution"):
        return KQ.get_gender_darajah_distribution(hijri_year=hijri_year)
    
    return [], [], []

# ---------------- MARHALA FUNCTIONS ----------------
def get_all_marhalas():
    """Get all Marhala names for filter dropdown."""
    try:
        raw_marhalas = KQ.get_all_marhalas()
        # Use a set to ensure unique formatted names (removes "repetition")
        formatted = sorted(list(set(format_marhala_display_name(m) for m in raw_marhalas)))
        return formatted
    except Exception as e:
        current_app.logger.error(f"Error getting all marhalas: {e}")
        return [
            "Collegiate I (5-7)",
            "Culture Générale (Std 3-4)", 
            "Culture Générale (Std 1-2)",
            "Collegiate II & Higher Studies (Std 8-11)",
            "Dars Burhani",
            "Asateza Kiram",
            "Sighat ul Jamea",
            "Mukhayyam Khidmat Guzar",
            "Staff",
            "Teacher"
        ]

def get_academic_marhalas_list():
    """Get only academic marhalas."""
    academic_marhalas = KQ.get_academic_marhalas()
    if not academic_marhalas:
        return []
    
    conn = get_koha_conn()
    try:
        cur = conn.cursor(dictionary=True)  # Use dictionary cursor
        placeholders = ', '.join(['%s'] * len(academic_marhalas))
        cur.execute(
            f"SELECT DISTINCT description FROM categories WHERE categorycode IN ({placeholders}) ORDER BY description", 
            tuple(academic_marhalas)
        )
        rows = cur.fetchall()
        cur.close()
        return [row['description'] for row in rows if row.get('description')]
    except Exception as e:
        current_app.logger.error(f"Error getting academic marhalas: {e}")
        return []
    finally:
        conn.close()

def get_non_academic_marhalas_list():
    """Get only non-academic marhalas."""
    non_academic_marhalas = KQ.get_non_academic_marhalas()
    if not non_academic_marhalas:
        return []
    
    conn = get_koha_conn()
    try:
        cur = conn.cursor(dictionary=True)  # Use dictionary cursor
        placeholders = ', '.join(['%s'] * len(non_academic_marhalas))
        cur.execute(
            f"SELECT DISTINCT description FROM categories WHERE categorycode IN ({placeholders}) ORDER BY description", 
            tuple(non_academic_marhalas)
        )
        rows = cur.fetchall()
        cur.close()
        return [row['description'] for row in rows if row.get('description')]
    except Exception as e:
        current_app.logger.error(f"Error getting non-academic marhalas: {e}")
        return []
    finally:
        conn.close()

def get_marhala_distribution(hijri_year=None):
    """Get Marhala distribution."""
    try:
        return KQ.get_marhala_distribution_with_dars_burhani(hijri_year=hijri_year)
    except Exception as e:
        current_app.logger.error(f"Error getting marhala distribution: {e}")
        try:
            raw_rows = KQ.get_marhala_summary()
            aggregated = {}
            for row in raw_rows:
                name = format_marhala_display_name(row.get("Marhala", "Unknown"))
                val = int(row.get("BooksIssued", 0) or 0)
                aggregated[name] = aggregated.get(name, 0) + val
            
            labels = sorted(aggregated.keys())
            values = [aggregated[l] for l in labels]
            return labels, values
        except:
            return [], []

def get_darajah_summary_by_marhala(marhala_name=None, hijri_year=None):
    """Get Darajah summary filtered by Marhala."""
    try:
        rows = KQ.get_darajah_summary_by_marhala(marhala_name, hijri_year=hijri_year)
        for row in rows:
            row["Marhala"] = format_marhala_display_name(row.get("Marhala", ""))
        return rows
    except Exception as e:
        current_app.logger.error(f"Error getting darajah summary: {e}")
        return []

def get_top_darajah_summary_with_asateza_last(hijri_year=None):
    """Get top Darajah summary."""
    if hasattr(KQ, "get_top_darajah_summary_with_asateza_last"):
        try:
            rows = KQ.get_top_darajah_summary_with_asateza_last(hijri_year=hijri_year)
            for row in rows:
                row["Marhala"] = format_marhala_display_name(row.get("Marhala", ""))
            return rows
        except Exception:
            return []
    return []

def format_marhala_display_name(marhala_name):
    """Format Marhala name for consistent display."""
    if not marhala_name:
        return "Unknown"
    
    marhala_name = str(marhala_name).strip()
    
    display_map = {
        "Teacher": "Teaching Staff",
        "Library": "Library Staff",
        "Asateza Kiram": "Asateza Kiram",
        "Sighat ul jamea": "Sighat ul Jamea",
        "Mukhayyam Khidmat Guzar": "Mukhayyam Khidmat Guzar",
        "Collegiate I (5-7)": "Collegiate I (5-7)",
        "Culture Générale (Std 3-4)": "Culture Générale (Std 3-4)",
        "Culture Générale (Std 1-2)": "Culture Générale (Std 1-2)",
        "Collegiate II & Higher Studies (Std 8-11)": "Collegiate II & Higher Studies (Std 8-11)",
        "Collegiate II and Higher Studies": "Collegiate II & Higher Studies (Std 8-11)",
        "Dars Burhani": "Dars Burhani",
        "Staff": "Library Staff"
    }
    
    return display_map.get(marhala_name, marhala_name)

# ---------------- MARHALA PERFORMANCE FUNCTIONS ----------------
def get_academic_marhalas_performance(hijri_year=None):
    """Get academic marhalas performance."""
    try:
        start, end = KQ.get_ay_bounds(hijri_year)
        conn = get_koha_conn()
        cur = conn.cursor(dictionary=True)  # Use dictionary cursor

        academic_codes = ['S-CO', 'S-CGB', 'S-CGA', 'S-CT', 'S-DARS', 'S-DB']
        if not academic_codes:
            return []

        placeholders = ', '.join(['%s'] * len(academic_codes))
        query = f"""
            SELECT
                c.categorycode,
                c.description AS Marhala,
                COUNT(DISTINCT b.borrowernumber) AS TotalPatrons,
                COUNT(DISTINCT
                    CASE
                        WHEN trno.attribute IS NOT NULL AND trno.attribute != ''
                        THEN b.borrowernumber
                    END
                ) AS ActivePatrons,
                COUNT(s.borrowernumber) AS Issues,
                ROUND(
                    COUNT(s.borrowernumber) / NULLIF(
                        COUNT(DISTINCT
                            CASE
                                WHEN trno.attribute IS NOT NULL AND trno.attribute != ''
                                THEN b.borrowernumber
                            END
                        ), 0
                    ), 2
                ) AS IssuesPerActivePatron
            FROM categories c
            LEFT JOIN borrowers b ON c.categorycode = b.categorycode
                AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                AND (b.debarred IS NULL OR b.debarred = 0)
                AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            LEFT JOIN borrower_attributes trno ON b.borrowernumber = trno.borrowernumber
                AND trno.code = 'TRNO'
            LEFT JOIN statistics s ON b.borrowernumber = s.borrowernumber
                AND s.type = 'issue'
                AND DATE(s.datetime) BETWEEN %s AND %s
            WHERE c.categorycode IN ({placeholders})
            GROUP BY c.categorycode, c.description
            ORDER BY Issues DESC, Marhala ASC
        """

        params = [start, end] + academic_codes
        cur.execute(query, params)
        rows = cur.fetchall()
        cur.close()

        # Aggregate by display name to avoid "repetition" if multiple codes map to the same name
        aggregated = {}
        for row in rows:
            marhala_name = row.get("Marhala", "")
            if not marhala_name:
                continue
                
            display_name = format_marhala_display_name(marhala_name)
            active_patrons = int(row.get("ActivePatrons", 0) or 0)
            issues = int(row.get("Issues", 0) or 0)

            if display_name not in aggregated:
                aggregated[display_name] = {
                    "MARHALA": display_name,
                    "ISSUES": 0,
                    "PATRONS": 0,
                    "TYPE": "Academic"
                }
            
            aggregated[display_name]["ISSUES"] += issues
            aggregated[display_name]["PATRONS"] += active_patrons

        formatted_rows = []
        for name, data in aggregated.items():
            issues = data["ISSUES"]
            patrons = data["PATRONS"]
            data["ISSUES/PATRON"] = round(issues / patrons, 2) if patrons > 0 else 0.0
            formatted_rows.append(data)

        expected_marhalas = [
            "Collegiate I (5-7)",
            "Culture Générale (Std 3-4)",
            "Culture Générale (Std 1-2)",
            "Collegiate II & Higher Studies (Std 8-11)",
            "Dars Burhani"
        ]

        seen = {r["MARHALA"] for r in formatted_rows}
        for expected in expected_marhalas:
            if expected not in seen:
                formatted_rows.append({
                    "MARHALA": expected,
                    "ISSUES": 0,
                    "PATRONS": 0,
                    "ISSUES/PATRON": 0.0,
                    "TYPE": "Academic"
                })

        formatted_rows.sort(key=lambda x: x["ISSUES"], reverse=True)
        return formatted_rows

    except Exception as e:
        current_app.logger.error(f"Error getting academic marhalas performance: {e}")
        import traceback
        traceback.print_exc()
        return []
    finally:
        if 'conn' in locals():
            conn.close()

def get_non_academic_marhalas_performance(hijri_year=None):
    """Get non-academic marhalas performance."""
    try:
        start, end = KQ.get_ay_bounds(hijri_year)
        conn = get_koha_conn()
        cur = conn.cursor(dictionary=True)  # Use dictionary cursor

        non_academic_codes = ['T-KG', 'L', 'T', 'S', 'HO', 'M-KG']
        if not non_academic_codes:
            return []

        placeholders = ', '.join(['%s'] * len(non_academic_codes))
        query = f"""
            SELECT
                c.categorycode,
                c.description AS Marhala,
                COUNT(DISTINCT b.borrowernumber) AS TotalPatrons,
                COUNT(DISTINCT
                    CASE
                        WHEN trno.attribute IS NOT NULL AND trno.attribute != ''
                        THEN b.borrowernumber
                    END
                ) AS ActivePatrons,
                COUNT(s.borrowernumber) AS Issues,
                ROUND(
                    COUNT(s.borrowernumber) / NULLIF(
                        COUNT(DISTINCT
                            CASE
                                WHEN trno.attribute IS NOT NULL AND trno.attribute != ''
                                THEN b.borrowernumber
                            END
                        ), 0
                    ), 2
                ) AS IssuesPerActivePatron
            FROM categories c
            LEFT JOIN borrowers b ON c.categorycode = b.categorycode
                AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                AND (b.debarred IS NULL OR b.debarred = 0)
                AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            LEFT JOIN borrower_attributes trno ON b.borrowernumber = trno.borrowernumber
                AND trno.code = 'TRNO'
            LEFT JOIN statistics s ON b.borrowernumber = s.borrowernumber
                AND s.type = 'issue'
                AND DATE(s.datetime) BETWEEN %s AND %s
            WHERE c.categorycode IN ({placeholders})
            GROUP BY c.categorycode, c.description
            ORDER BY Issues DESC, Marhala ASC
        """

        params = [start, end] + non_academic_codes
        cur.execute(query, params)
        rows = cur.fetchall()
        cur.close()

        aggregated = {}
        for row in rows:
            marhala_name = row.get("Marhala", "")
            if not marhala_name:
                continue

            display_name = format_marhala_display_name(marhala_name)
            active_patrons = int(row.get("ActivePatrons", 0) or 0)
            issues = int(row.get("Issues", 0) or 0)

            if display_name not in aggregated:
                aggregated[display_name] = {
                    "MARHALA": display_name,
                    "ISSUES": 0,
                    "PATRONS": 0,
                    "TYPE": "Non-Academic"
                }
            
            aggregated[display_name]["ISSUES"] += issues
            aggregated[display_name]["PATRONS"] += active_patrons

        formatted_rows = []
        for name, data in aggregated.items():
            issues = data["ISSUES"]
            patrons = data["PATRONS"]
            # Ensure no division by zero and handle repetition already aggregated
            data["ISSUES/PATRON"] = round(issues / patrons, 2) if patrons > 0 else 0.0
            formatted_rows.append(data)

        expected_marhalas = [
            "Asateza Kiram",
            "Library Staff",
            "Teaching Staff",
            "Sighat ul Jamea",
            "Mukhayyam Khidmat Guzar"
        ]

        seen = {r["MARHALA"] for r in formatted_rows}
        for expected in expected_marhalas:
            if expected not in seen:
                formatted_rows.append({
                    "MARHALA": expected,
                    "ISSUES": 0,
                    "PATRONS": 0,
                    "ISSUES/PATRON": 0.0,
                    "TYPE": "Non-Academic"
                })

        formatted_rows.sort(key=lambda x: x["ISSUES"], reverse=True)
        return formatted_rows

    except Exception as e:
        current_app.logger.error(f"Error getting non-academic marhalas performance: {e}")
        import traceback
        traceback.print_exc()
        return []
    finally:
        if 'conn' in locals():
            conn.close()

# ---------------- DARAJAH FUNCTIONS ----------------
def get_all_darajahs_detailed(hijri_year=None):
    """Get all darajahs with detailed breakdown."""
    start, end = KQ.get_ay_bounds(hijri_year)
    if not start:
        return []

    conn = get_koha_conn()
    try:
        cur = conn.cursor(dictionary=True)  # Use dictionary cursor
        
        current_app.logger.info(f"Academic Year Dates: {start} to {end}")
        
        cur.execute("""
            SELECT 
                COALESCE(std.attribute, b.branchcode) AS Darajah,
                b.branchcode AS BranchCode,
                MAX(COALESCE(c.description, b.categorycode)) AS Marhala,
                COUNT(s.datetime) AS TotalIssues,
                COUNT(DISTINCT b.borrowernumber) AS TotalStudents,
                COUNT(DISTINCT 
                    CASE 
                        WHEN (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                             AND (b.debarred IS NULL OR b.debarred = 0)
                             AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
                             AND (trno.attribute IS NOT NULL AND trno.attribute != '')
                        THEN b.borrowernumber 
                    END
                ) AS ActiveStudents,
                COUNT(DISTINCT 
                    CASE WHEN s.borrowernumber IS NOT NULL 
                    THEN s.borrowernumber END
                ) AS ParticipatingStudents,
                COUNT(DISTINCT it.biblionumber) AS UniqueTitles,
                ROUND(COUNT(s.datetime) / NULLIF(COUNT(DISTINCT 
                    CASE 
                        WHEN (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                             AND (b.debarred IS NULL OR b.debarred = 0)
                             AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
                             AND (trno.attribute IS NOT NULL AND trno.attribute != '')
                        THEN b.borrowernumber 
                    END
                ), 0), 2) AS AvgIssuesPerStudent
            FROM borrowers b
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('Class','STD','CLASS','DAR','CLASS_STD')
            LEFT JOIN borrower_attributes trno
                ON trno.borrowernumber = b.borrowernumber
                AND trno.code = 'TRNO'
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            LEFT JOIN statistics s 
                ON s.borrowernumber = b.borrowernumber
                AND s.type = 'issue'
                AND DATE(s.datetime) BETWEEN %s AND %s
            LEFT JOIN items it ON s.itemnumber = it.itemnumber
            WHERE COALESCE(std.attribute, b.branchcode) IS NOT NULL
              AND COALESCE(std.attribute, b.branchcode) != ''
            GROUP BY COALESCE(std.attribute, b.branchcode)
            ORDER BY TotalIssues DESC;
        """, (start, end))
        
        rows = cur.fetchall()
        
        for row in rows:
            row["Marhala"] = format_marhala_display_name(row.get("Marhala", ""))
            
            # Engagement Calc
            total_st = row.get("TotalStudents", 0)
            active_st = row.get("ActiveStudents", 0)
            participating_st = row.get("ParticipatingStudents", 0)
            
            row["MembershipPercent"] = round((active_st / total_st * 100), 1) if total_st > 0 else 0
            row["BorrowerPercent"] = round((participating_st / active_st * 100), 1) if active_st > 0 else 0
            
            avg_issues = row.get("AvgIssuesPerStudent") or 0
            if avg_issues >= 8:
                row["Efficiency"] = "Elite"
                row["EfficiencyColor"] = "success"
            elif avg_issues >= 5:
                row["Efficiency"] = "Excellent"
                row["EfficiencyColor"] = "primary"
            elif avg_issues >= 3:
                row["Efficiency"] = "Good"
                row["EfficiencyColor"] = "info"
            elif avg_issues >= 1:
                row["Efficiency"] = "Average"
                row["EfficiencyColor"] = "warning"
            else:
                row["Efficiency"] = "Low"
                row["EfficiencyColor"] = "danger"
        
        cur.close()
        return rows
        
    except Exception as e:
        current_app.logger.error(f"Error getting all darajahs detailed: {e}")
        import traceback
        traceback.print_exc()
        return []
    finally:
        conn.close()

def get_currently_issued_by_marhala(marhala_name=None, hijri_year=None):
    """Get currently issued books by marhala."""
    try:
        result = KQ.get_department_currently_issued(marhala_name, hijri_year=hijri_year)
        for marhala in result.get("marhalas", []):
            marhala["Marhala"] = format_marhala_display_name(marhala.get("Marhala", ""))
        return result
    except Exception as e:
        current_app.logger.error(f"Error getting currently issued by marhala: {e}")
        try:
            start, end = KQ.get_ay_bounds()
            conn = get_koha_conn()
            cur = conn.cursor(dictionary=True)  # Use dictionary cursor
            
            query = """
                SELECT 
                    COALESCE(c.description, 'Unknown') as Marhala,
                    COUNT(*) as CurrentlyIssued,
                    SUM(CASE 
                        WHEN i.date_due < CURDATE() 
                        THEN 1 ELSE 0 
                    END) as Overdue
                FROM issues i
                JOIN borrowers b ON i.borrowernumber = b.borrowernumber
                LEFT JOIN categories c ON b.categorycode = c.categorycode
                WHERE i.returndate IS NULL
                  AND DATE(i.issuedate) BETWEEN %s AND %s
                  AND b.categorycode LIKE 'S%%'
            """
            
            params = [start, end]
            if marhala_name:
                query += " AND (c.description = %s OR b.categorycode = %s)"
                params.extend([marhala_name, marhala_name])
            
            query += " GROUP BY Marhala ORDER BY CurrentlyIssued DESC"
            
            cur.execute(query, params)
            rows = cur.fetchall()
            
            total_issued = sum(row.get('CurrentlyIssued', 0) for row in rows) if rows else 0
            
            processed_rows = []
            for row in rows:
                processed_rows.append({
                    "Marhala": format_marhala_display_name(row.get("Marhala", "Unknown")),
                    "CurrentlyIssued": row.get("CurrentlyIssued", 0),
                    "Overdue": row.get("Overdue", 0)
                })
            
            cur.close()
            return {
                "marhalas": processed_rows,
                "total_currently_issued": total_issued
            }
        except Exception as e2:
            current_app.logger.error(f"Fallback error in currently issued: {e2}")
            import traceback
            traceback.print_exc()
            return {"marhalas": [], "total_currently_issued": 0}
        finally:
            if 'conn' in locals():
                conn.close()

def get_marhala_summary(selected_marhala=None):
    """Get marhala summary."""
    try:
        rows = KQ.get_marhala_summary(selected_marhala)
        for row in rows:
            row["Marhala"] = format_marhala_display_name(row.get("Marhala", ""))
        return rows
    except Exception as e:
        current_app.logger.error(f"Error getting marhala summary: {e}")
        return []

# ---------------- KEY INSIGHTS ----------------
def get_key_insights(hijri_year=None):
    """Get key insights from the data."""
    try:
        return KQ.get_key_insights(hijri_year=hijri_year)
    except Exception as e:
        current_app.logger.error(f"Error getting key insights: {e}")
        return ["Data loading... Insights will appear shortly."]

# ---------------- LANGUAGE TOP 25 ----------------
def get_language_top25(selected_marhala=None):
    """Get top 25 titles by language, optionally filtered."""
    if hasattr(KQ, "get_language_top25"):
        try:
            return KQ.get_language_top25(selected_marhala)
        except Exception:
            pass

    return {
        "arabic": {"titles": [], "counts": [], "records": []},
        "english": {"titles": [], "counts": [], "records": []}
    }

# ---------------- TOP PERFORMANCE FUNCTIONS ----------------
def get_top_darajah_performance(hijri_year=None):
    """Get top 10 Darajah performance (excluding Asateza)."""
    try:
        all_darajahs = get_all_darajahs_detailed(hijri_year=hijri_year)
        
        filtered = []
        for d in all_darajahs:
            name = str(d.get("Darajah", "")).upper()
            marhala = str(d.get("Marhala", "")).upper()
            
            if "ASATEZA" in name or "ASATEZA" in marhala or "AJSN" in name:
                continue
            
            d["Marhala"] = format_marhala_display_name(d.get("Marhala", ""))
            filtered.append(d)
            
        for d in filtered:
            if d.get("TotalIssues") is None:
                d["TotalIssues"] = 0
                
        sorted_darajahs = sorted(filtered, key=lambda x: x["TotalIssues"], reverse=True)
        
        results = []
        for d in sorted_darajahs[:10]:
            results.append({
                "Darajah": d["Darajah"],
                "BooksIssued": d["TotalIssues"],
                "ActiveStudents": d["ActiveStudents"],
                "IssuesPerStudent": d["AvgIssuesPerStudent"],
                "Marhala": d["Marhala"]
            })
            
        return results
        
    except Exception as e:
        current_app.logger.error(f"Error getting top darajah performance: {e}")
        return []

def get_top_students(limit=10, selected_marhala=None, hijri_year=None, sex=None):
    """Get top 10 individual students with links to student details."""
    start, end = KQ.get_ay_bounds(hijri_year)
    if not start:
        return []

    try:
        rows = KQ.get_top_students(limit, selected_marhala, hijri_year=hijri_year, sex=sex)
        
        results = []
        for row in rows:
            student_id = row.get("borrowernumber")
            marhala = row.get("Department") or row.get("Marhala", "Unknown")
            
            # FIXED: Use 'identifier' parameter instead of 'borrowernumber' and 'marhala'
            student_link = url_for('hod_dashboard_bp.student_details', 
                                   identifier=student_id)
            
            results.append({
                "StudentName": row.get("StudentName", "Unknown"),
                "Class": row.get("Class", "—"),
                "BooksIssued": row.get("BooksIssued", 0),
                "Marhala": format_marhala_display_name(marhala),
                "StudentLink": student_link,
                "borrowernumber": row.get("borrowernumber"),
                "cardnumber": row.get("cardnumber", ""),
            })
            
        return results
    except Exception as e:
        current_app.logger.error(f"Error getting top students: {e}")
        import traceback
        traceback.print_exc()
        return []
    finally:
        pass

# ---------------- MARHALA COUNTS ----------------
def get_marhala_counts():
    """Get counts of different marhala types."""
    try:
        academic_marhalas = get_academic_marhalas_list()
        non_academic_marhalas = get_non_academic_marhalas_list()
        
        return {
            "total": len(academic_marhalas) + len(non_academic_marhalas),
            "academic": len(academic_marhalas),
            "non_academic": len(non_academic_marhalas)
        }
    except Exception as e:
        current_app.logger.error(f"Error getting marhala counts: {e}")
        return {"total": 0, "academic": 0, "non_academic": 0}

# ---------------- DARAJAH STUDENTS BREAKDOWN ----------------
def get_darajah_students_breakdown(darajah_name, page=1, per_page=20):
    """Get detailed breakdown of students in a specific darajah with pagination."""
    start, end = KQ.get_ay_bounds()
    if not start:
        return [], 0, 0
    
    conn = get_koha_conn()
    try:
        cur = conn.cursor(dictionary=True)  # Use dictionary cursor
        
        cur.execute("""
            SELECT COUNT(DISTINCT b.borrowernumber) as total_students
            FROM borrowers b
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('Class','STD','CLASS','DAR','CLASS_STD')
            WHERE (std.attribute = %s OR b.branchcode = %s)
        """, (darajah_name, darajah_name))
        
        count_result = cur.fetchone()
        total_students = count_result.get('total_students', 0) if count_result else 0

        cur.execute("""
            SELECT 
                b.borrowernumber,
                b.cardnumber,
                CASE 
                    WHEN (b.surname IS NULL OR b.surname = '' OR b.surname = 'None') 
                         AND (b.firstname IS NULL OR b.firstname = '' OR b.firstname = 'None')
                    THEN CONCAT('Student #', b.cardnumber)
                    WHEN b.surname IS NULL OR b.surname = '' OR b.surname = 'None'
                    THEN b.firstname
                    WHEN b.firstname IS NULL OR b.firstname = '' OR b.firstname = 'None'
                    THEN b.surname
                    ELSE CONCAT(b.surname, ' ', b.firstname)
                END AS StudentName,
                b.email,
                trno.attribute AS TRNumber,
                c.description AS Marhala,
                (
                    SELECT COUNT(*) 
                    FROM statistics s2 
                    WHERE s2.borrowernumber = b.borrowernumber 
                    AND s2.type = 'issue'
                    AND DATE(s2.`datetime`) BETWEEN %s AND %s
                ) AS BooksIssuedAY,
                (
                    SELECT GROUP_CONCAT(DISTINCT it2.ccode ORDER BY it2.ccode SEPARATOR ', ')
                    FROM statistics s2 
                    JOIN items it2 ON s2.itemnumber = it2.itemnumber
                    WHERE s2.borrowernumber = b.borrowernumber 
                    AND s2.type = 'issue'
                    AND DATE(s2.`datetime`) BETWEEN %s AND %s
                ) AS CollectionsUsed,
                (
                    SELECT COUNT(*)
                    FROM issues i
                    WHERE i.borrowernumber = b.borrowernumber
                    AND i.returndate IS NULL
                    AND i.date_due < CURDATE()
                    AND DATE(i.issuedate) BETWEEN %s AND %s
                ) AS OverdueCount,
                (
                    SELECT COALESCE(SUM(
                        CASE 
                            WHEN i2.date_due < CURDATE() 
                            THEN DATEDIFF(CURDATE(), i2.date_due) * 10
                            ELSE 0 
                        END
                    ), 0)
                    FROM issues i2
                    WHERE i2.borrowernumber = b.borrowernumber
                    AND i2.returndate IS NULL
                    AND i2.date_due < CURDATE()
                    AND DATE(i2.issuedate) BETWEEN %s AND %s
                ) AS CurrentFeesKES,
                b.dateexpiry
            FROM borrowers b
            LEFT JOIN categories c ON c.categorycode = b.categorycode
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('Class','STD','CLASS','DAR','CLASS_STD')
            LEFT JOIN borrower_attributes trno
                ON trno.borrowernumber = b.borrowernumber
                AND trno.code = 'TRNO'
            WHERE (std.attribute = %s OR b.branchcode = %s)
            ORDER BY BooksIssuedAY DESC, StudentName ASC
            LIMIT %s OFFSET %s;
        """, (start, end, start, end, start, end, start, end, darajah_name, darajah_name, per_page, (page-1)*per_page))

        students = cur.fetchall()

        for student in students:
            student["Marhala"] = format_marhala_display_name(student.get("Marhala", ""))
            
            current_fees = float(student.get("CurrentFeesKES", 0) or 0)
            student["CurrentFeesDisplay"] = f"KSh {current_fees:,.2f}"
            
            identifier = student.get("cardnumber") or str(student.get("borrowernumber"))
            student["StudentLink"] = f"/students/{identifier}"
        
        total_pages = (total_students // per_page) + (1 if total_students % per_page else 0)
        
        cur.close()
        return students, total_students, total_pages
    except Exception as e:
        current_app.logger.error(f"Error getting darajah students breakdown: {e}")
        import traceback
        traceback.print_exc()
        return [], 0, 0
    finally:
        conn.close()

# ---------------- MAIN DASHBOARD ROUTE ----------------
@bp.route("/", methods=["GET", "POST"])
def dashboard():
    start_total = time.time()
    current_app.logger.info("⚡ Starting dashboard load...")

    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role == "hod":
        return redirect(url_for("hod_dashboard_bp.dashboard"))
    elif role == "teacher":
        return redirect(url_for("teacher_dashboard_bp.dashboard"))
    elif role not in ["admin", "super_admin"]:
        flash("You must be logged in as an admin to view the Admin Analytics.", "danger")
        return redirect(url_for("auth_bp.login"))

    if role == "super_admin" and not session.get("branch_code"):
        # For super admin, if no branch is currently "active" in session, default to AJSN
        session["branch_code"] = "AJSN"

    # --- Handle Filters ---
    if request.method == "POST":
        selected_marhala = (request.form.get("marhalaFilter") or "").strip()
        session["selected_marhala"] = selected_marhala
    else:
        selected_marhala = (session.get("selected_marhala") or "").strip()

    if not selected_marhala:
        selected_marhala = None

    # --- Handle Academic Year Toggle ---
    selected_ay = session.get("selected_ay", "current")
    hijri_year = None
    if selected_ay != "current":
        try:
            hijri_year = int(selected_ay)
        except:
            selected_ay = "current"
    
    ay_period = get_academic_year_period(hijri_year)
    available_years = KQ.get_available_academic_years()
    
    # Dynamic label for AY
    if hijri_year:
        ay_label = f"144{hijri_year % 10}-144{hijri_year % 10 + 1}H" if hijri_year >= 1440 else f"{hijri_year}H"
    else:
        # Fallback to current year label (e.g. 1447-1448H)
        curr_yr = get_current_ay_year()
        ay_label = f"{curr_yr}-{curr_yr+1}H"

    t0 = time.time()
    kpi_data = get_kpis(selected_marhala, hijri_year=hijri_year)
    current_app.logger.info(f"⚡ get_kpis took: {time.time() - t0:.4f}s")

    t0 = time.time()
    today_checkouts, today_checkins = get_today_activity()
    current_app.logger.info(f"⚡ get_today_activity took: {time.time() - t0:.4f}s")

    t0 = time.time()
    trend_labels, trend_values = get_trends(selected_marhala, hijri_year=hijri_year)
    current_app.logger.info(f"⚡ get_trends took: {time.time() - t0:.4f}s")

    t0 = time.time()
    darajah_labels, darajah_male_values, darajah_female_values = get_darajah_distribution(hijri_year=hijri_year)
    current_app.logger.info(f"⚡ get_darajah_distribution took: {time.time() - t0:.4f}s")

    t0 = time.time()
    marhala_labels, marhala_values = get_marhala_distribution(hijri_year=hijri_year)
    current_app.logger.info(f"⚡ get_marhala_distribution took: {time.time() - t0:.4f}s")
    
    t0 = time.time()
    lang_labels, lang_values = KQ.get_issues_by_language(selected_marhala, hijri_year=hijri_year)
    current_app.logger.info(f"⚡ get_issues_by_language took: {time.time() - t0:.4f}s")

    t0 = time.time()
    lang_top = get_language_top25(selected_marhala)
    current_app.logger.info(f"⚡ get_language_top25 took: {time.time() - t0:.4f}s")
    
    t0 = time.time()
    subject_cloud = KQ.get_subject_cloud(selected_marhala, hijri_year=hijri_year, limit=40)
    if not subject_cloud:
        subject_cloud = []
    current_app.logger.info(f"⚡ get_subject_cloud took: {time.time() - t0:.4f}s")
    
    t0 = time.time()
    marhala_counts = get_marhala_counts()
    current_app.logger.info(f"⚡ get_marhala_counts took: {time.time() - t0:.4f}s")
    
    t0 = time.time()
    if selected_marhala:
        darajah_summary_rows = get_darajah_summary_by_marhala(selected_marhala, hijri_year=hijri_year)
        marhala_summary_rows = get_marhala_summary(selected_marhala)
    else:
        darajah_summary_rows = get_top_darajah_summary_with_asateza_last(hijri_year=hijri_year)
        marhala_summary_rows = get_marhala_summary(None)
    current_app.logger.info(f"⚡ summary_rows took: {time.time() - t0:.4f}s")
    
    all_marhalas_raw = get_all_marhalas()
    all_marhalas = [format_marhala_display_name(m) for m in all_marhalas_raw]
    
    academic_marhalas_list = [format_marhala_display_name(m) for m in get_academic_marhalas_list()]
    non_academic_marhalas_list = [format_marhala_display_name(m) for m in get_non_academic_marhalas_list()]
    
    academic_marhalas = get_academic_marhalas_performance(hijri_year=hijri_year)
    non_academic_marhalas = get_non_academic_marhalas_performance(hijri_year=hijri_year)
    
    currently_issued_data = get_currently_issued_by_marhala(selected_marhala, hijri_year=hijri_year)
    
    insights = get_key_insights(hijri_year=hijri_year)
    
    top_darajah_performance = get_top_darajah_performance(hijri_year=hijri_year)
    
    top_students_male = get_top_students(10, selected_marhala, hijri_year=hijri_year, sex='M')
    top_students_female = get_top_students(10, selected_marhala, hijri_year=hijri_year, sex='F')
    
    all_darajahs_detailed = get_all_darajahs_detailed(hijri_year=hijri_year)

    hijri_today = get_hijri_today()
    
    opac_base = get_opac_base()

    kpi_cards = [
        {
            "key": "total_patrons",
            "label": "All Students",
            "value": f"{kpi_data.get('total_patrons', 0):,}",
            "icon": "fa-users",
            "color": "vibrant-blue",
            "subtext": f"Includes {kpi_data['active_patrons']:,} active patrons"
        },
        {
            "key": "total_issues",
            "label": f"Books Issued {ay_label}",
            "value": f"{kpi_data['total_issues']:,}",
            "icon": "fa-book-open",
            "color": "vibrant-green",
            "subtext": f"{kpi_data['total_titles_issued']:,} distinct titles"
        },
        {
            "key": "currently_issued",
            "label": "Currently Issued",
            "value": f"{kpi_data['currently_issued']:,}",
            "icon": "fa-book",
            "color": "vibrant-purple",
            "subtext": f"{kpi_data['overdue']:,} overdue ({kpi_data['currency']} {kpi_data['currently_issued_fees']:,.2f})"
        },
        {
            "key": "today_activity",
            "label": "Today's Activity",
            "value": f"{today_checkouts} / {today_checkins}",
            "icon": "fa-calendar-day",
            "color": "vibrant-orange",
            "subtext": f"Checkouts / Checkins"
        },
        {
            "key": "garamat",
            "label": "Garamat Collected",
            "value": f"{kpi_data['currency']} {kpi_data['total_fees']:,.0f}",
            "icon": "fa-hand-holding-usd",
            "color": "vibrant-red",
            "subtext": f"Total for {ay_label}"
        },
    ]

    filter_indicator = ""
    if selected_marhala:
        filter_indicator = f"Filtered by Marhala: {format_marhala_display_name(selected_marhala)}"

    current_app.logger.info(f"⚡ Total dashboard load took: {time.time() - start_total:.4f}s")

    return render_template(
        "dashboard.html",
        hijri_today=hijri_today,
        ay_period=ay_period,
        kpi_cards=kpi_cards,
        kpi_data=kpi_data,
        trend_labels=trend_labels,
        trend_values=trend_values,
        darajah_labels=darajah_labels,
        darajah_male_values=darajah_male_values,
        darajah_female_values=darajah_female_values,
        marhala_labels=marhala_labels,
        marhala_values=marhala_values,
        arabic_top_records=lang_top["arabic"]["records"],
        english_top_records=lang_top["english"]["records"],
        darajah_summary_rows=darajah_summary_rows,
        marhala_summary_rows=marhala_summary_rows,
        academic_marhalas=academic_marhalas,
        non_academic_marhalas=non_academic_marhalas,
        currently_issued_data=currently_issued_data,
        insights=insights,
        all_marhalas=all_marhalas,
        academic_marhalas_list=academic_marhalas_list,
        non_academic_marhalas_list=non_academic_marhalas_list,
        top_students_male=top_students_male,
        top_students_female=top_students_female,
        selected_marhala=format_marhala_display_name(selected_marhala) if selected_marhala else None,
        filter_indicator=filter_indicator,
        top_darajah_performance=top_darajah_performance,
        all_darajahs_detailed=all_darajahs_detailed,
        selected_ay=selected_ay,
        available_years=available_years,
        opac_base=opac_base,
        current_fees_kes=kpi_data['currently_issued_fees'],
        overdue_count=kpi_data['overdue'],
        marhala_counts=marhala_counts,
        lang_labels=lang_labels,
        lang_values=lang_values,
        subject_cloud=subject_cloud
    )


@bp.route("/set-academic-year", methods=["POST"])
def set_academic_year():
    """Set the academic year filter."""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))
    
    selected_ay = request.form.get("selected_ay", "current")
    session["selected_ay"] = selected_ay
    
    return redirect(url_for("dashboard_bp.dashboard"))


@bp.route("/api/patron-bifurcation")
def api_patron_bifurcation():
    """API endpoint for 'All Students' card popup."""
    selected_marhala = session.get("selected_marhala")
    data = KQ.get_patron_bifurcation(selected_marhala)
    return jsonify(data)

@bp.route("/api/issues-bifurcation")
def api_issues_bifurcation():
    """API endpoint for 'Books Issued' card popup."""
    selected_marhala = session.get("selected_marhala")
    hijri_year = session.get("selected_ay")
    if hijri_year == "current": hijri_year = None
    data = KQ.get_issues_bifurcation(selected_marhala, hijri_year=hijri_year)
    return jsonify(data)

@bp.route("/api/fines-bifurcation")
def api_fines_bifurcation():
    """API endpoint for 'Garamat' card popup."""
    selected_marhala = session.get("selected_marhala")
    hijri_year = session.get("selected_ay")
    if hijri_year == "current": hijri_year = None
    data = KQ.get_fines_bifurcation(selected_marhala, hijri_year=hijri_year)
    return jsonify(data)

# ---------------- API ENDPOINTS ----------------
@bp.route("/api/darajah-search", methods=["GET"])
def api_darajah_search():
    """API endpoint for searching darajahs."""
    if not session.get("logged_in"):
        return jsonify({"error": "Unauthorized"}), 401
    
    search_term = request.args.get("search", "")
    marhala_filter = request.args.get("marhala", "all")
    
    all_darajahs = get_all_darajahs_detailed()
    
    filtered = []
    for darajah in all_darajahs:
        matches = True
        
        if search_term:
            search_lower = search_term.lower()
            darajah_name = str(darajah.get("Darajah", "")).lower()
            marhala_name = str(darajah.get("Marhala", "")).lower()
            
            if search_lower not in darajah_name and search_lower not in marhala_name:
                matches = False
        
        if marhala_filter and marhala_filter != "all":
            if darajah.get("Marhala") != marhala_filter:
                matches = False
        
        if matches:
            filtered.append(darajah)
    
    return jsonify({
        "success": True,
        "count": len(filtered),
        "darajahs": filtered
    })

@bp.route("/api/currently-issued-details")
def api_currently_issued_details():
    """API endpoint for listing students with currently issued books."""
    marhala_name = request.args.get("marhala")
    start, end = KQ.get_ay_bounds()
    conn = get_koha_conn()
    try:
        cur = conn.cursor(dictionary=True)
        query = """
            SELECT 
                b.userid, b.cardnumber, b.surname, b.firstname,
                c.description AS marhala,
                tr.attribute AS trno,
                std.attribute AS darajah,
                COUNT(*) as book_count,
                GROUP_CONCAT(bib.title SEPARATOR '; ') as books
            FROM issues i
            JOIN borrowers b ON i.borrowernumber = b.borrowernumber
            LEFT JOIN categories c ON b.categorycode = c.categorycode
            JOIN items it ON i.itemnumber = it.itemnumber
            JOIN biblio bib ON it.biblionumber = bib.biblionumber
            LEFT JOIN borrower_attributes tr 
                ON tr.borrowernumber = b.borrowernumber AND tr.code IN ('TRNO', 'TRN', 'TR_NUMBER', 'TR')
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber AND std.code IN ('STD', 'CLASS', 'DAR', 'CLASS_STD')
            WHERE i.returndate IS NULL
              AND DATE(i.issuedate) BETWEEN %s AND %s
              AND b.categorycode LIKE 'S%%'
        """
        params = [start, end]
        if marhala_name and marhala_name != "None":
            query += " AND (c.description = %s OR b.categorycode = %s)"
            params.extend([marhala_name, marhala_name])
            
        query += " GROUP BY b.borrowernumber ORDER BY book_count DESC"
        
        cur.execute(query, params)
        rows = cur.fetchall()
        return jsonify({"success": True, "data": rows})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
    finally:
        conn.close()

@bp.route("/api/overdue-details")
def api_overdue_details():
    """API endpoint for listing students with overdue books."""
    marhala_name = request.args.get("marhala")
    start, end = KQ.get_ay_bounds()
    conn = get_koha_conn()
    try:
        cur = conn.cursor(dictionary=True)
        query = """
            SELECT 
                b.userid, b.cardnumber, b.surname, b.firstname,
                c.description AS marhala,
                tr.attribute AS trno,
                std.attribute AS darajah,
                COUNT(*) as overdue_count,
                SUM(DATEDIFF(CURDATE(), i.date_due) * 10) as estimated_fees,
                GROUP_CONCAT(CONCAT(bib.title, ' (Due: ', i.date_due, ')') SEPARATOR '; ') as books
            FROM issues i
            JOIN borrowers b ON i.borrowernumber = b.borrowernumber
            LEFT JOIN categories c ON b.categorycode = c.categorycode
            JOIN items it ON i.itemnumber = it.itemnumber
            JOIN biblio bib ON it.biblionumber = bib.biblionumber
            LEFT JOIN borrower_attributes tr 
                ON tr.borrowernumber = b.borrowernumber AND tr.code IN ('TRNO', 'TRN', 'TR_NUMBER', 'TR')
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber AND std.code IN ('STD', 'CLASS', 'DAR', 'CLASS_STD')
            WHERE i.returndate IS NULL
              AND i.date_due < CURDATE()
              AND DATE(i.issuedate) BETWEEN %s AND %s
              AND b.categorycode LIKE 'S%%'
        """
        params = [start, end]
        if marhala_name and marhala_name != "None":
            query += " AND (c.description = %s OR b.categorycode = %s)"
            params.extend([marhala_name, marhala_name])
            
        query += " GROUP BY b.borrowernumber ORDER BY overdue_count DESC"
        
        cur.execute(query, params)
        rows = cur.fetchall()
        return jsonify({"success": True, "data": rows})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
    finally:
        conn.close()

@bp.route("/api/today-activity-details")
def api_today_activity_details():
    """API endpoint for listing today's borrowing/returning activity."""
    conn = get_koha_conn()
    try:
        cur = conn.cursor(dictionary=True)
        query = """
            SELECT 
                b.userid, b.cardnumber, b.surname, b.firstname,
                c.description AS marhala,
                tr.attribute AS trno,
                std.attribute AS darajah,
                s.type,
                bib.title,
                DATE_FORMAT(s.datetime, '%H:%i') as time
            FROM statistics s
            JOIN borrowers b ON s.borrowernumber = b.borrowernumber
            LEFT JOIN categories c ON b.categorycode = c.categorycode
            JOIN items it ON s.itemnumber = it.itemnumber
            JOIN biblio bib ON it.biblionumber = bib.biblionumber
            LEFT JOIN borrower_attributes tr 
                ON tr.borrowernumber = b.borrowernumber AND tr.code IN ('TRNO', 'TRN', 'TR_NUMBER', 'TR')
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber AND std.code IN ('STD', 'CLASS', 'DAR', 'CLASS_STD')
            WHERE DATE(s.datetime) = CURDATE()
            ORDER BY s.datetime DESC
        """
        cur.execute(query)
        rows = cur.fetchall()
        return jsonify({"success": True, "data": rows})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
    finally:
        conn.close()

@bp.route("/darajah-explorer")
def darajah_explorer():
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role != "admin":
        flash("You must be logged in as an admin to access the Darajah Explorer.", "danger")
        return redirect(url_for("auth_bp.login"))

    all_darajahs = [d for d in get_all_darajahs_detailed() if d.get("gender") != "Mixed"]
    all_marhalas = get_all_marhalas()
    darajah_numbers = [str(i) for i in range(1, 12)]
    hijri_today = get_hijri_today()
    opac_base = get_opac_base()
    
    import re
    darajahs_with_stats = []
    for d in all_darajahs:
        darajah_name = d.get("Darajah", "")
        # Parse darajah number/year
        match = re.search(r'(\d+)', darajah_name)
        darajah_number = match.group(1) if match else "Other"
        
        gender = d.get("gender", "")
        icon = "users"
        if "Boys" in gender: icon = "male"
        elif "Girls" in gender: icon = "female"
        
        # Match template keys
        d["darajah_name"] = darajah_name
        d["darajah_number"] = darajah_number
        d["icon"] = icon
        d["books_issued"] = d.get("TotalIssues", 0)
        # Ensure template-friendly keys are present
        d["TotalStudents"] = d.get("TotalStudents", 0)
        d["ActiveStudents"] = d.get("ActiveStudents", 0)
        
        darajahs_with_stats.append(d)

    darajahs_by_year = {}
    for d in darajahs_with_stats:
        num = d["darajah_number"]
        if num not in darajahs_by_year:
            darajahs_by_year[num] = []
        darajahs_by_year[num].append(d)
        
    sorted_years = sorted(
        [num for num in darajahs_by_year.keys() if num.isdigit()],
        key=lambda x: int(x)
    )
    if "Other" in darajahs_by_year:
        sorted_years.append("Other")

    return render_template(
        "darajah_explorer.html",
        darajahs=all_darajahs,
        total_darajahs=len(all_darajahs),
        all_marhalas=all_marhalas,
        darajah_numbers=darajah_numbers,
        hijri_today=hijri_today,
        OPAC_BASE=opac_base,
        darajahs_by_year=darajahs_by_year,
        sorted_years=sorted_years
    )

@bp.route("/darajah/<darajah_name>")
def darajah_detail(darajah_name):
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role != "admin":
        flash("You must be logged in as an admin to access Darajah details.", "danger")
        return redirect(url_for("dashboard_bp.dashboard"))

    darajah_name = darajah_name.replace('_', ' ')
    
    page = request.args.get('page', 1, type=int)
    per_page = 20
    
    all_darajahs = get_all_darajahs_detailed()
    darajah_info = None
    
    for darajah in all_darajahs:
        if darajah.get("Darajah") == darajah_name:
            darajah_info = darajah
            break
    
    if not darajah_info:
        flash(f"Darajah '{darajah_name}' not found.", "danger")
        return redirect(url_for("dashboard_bp.darajah_explorer"))
    
    students, total_students, total_pages = get_darajah_students_breakdown(darajah_name, page, per_page)
    
    start, end = KQ.get_ay_bounds()
    conn = get_koha_conn()
    try:
        cur = conn.cursor(dictionary=True)  # Use dictionary cursor
        
        cur.execute("""
            SELECT 
                bib.biblionumber,
                bib.title AS BookTitle,
                COUNT(*) AS TimesIssued,
                GROUP_CONCAT(DISTINCT it.ccode ORDER BY it.ccode SEPARATOR ', ') AS CollectionsUsed
            FROM statistics s
            JOIN borrowers b ON b.borrowernumber = s.borrowernumber
            LEFT JOIN borrower_attributes std
                ON std.borrowernumber = b.borrowernumber
                AND std.code IN ('Class','STD','CLASS','DAR','CLASS_STD')
            JOIN items it ON s.itemnumber = it.itemnumber
            JOIN biblio bib ON it.biblionumber = bib.biblionumber
            WHERE s.type = 'issue'
              AND DATE(s.`datetime`) BETWEEN %s AND %s
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
              AND (std.attribute = %s OR b.branchcode = %s)
            GROUP BY bib.biblionumber, bib.title
            ORDER BY TimesIssued DESC
            LIMIT 10;
        """, (start, end, darajah_name, darajah_name))
        
        top_books = cur.fetchall()
        cur.close()
    except Exception as e:
        current_app.logger.error(f"Error getting top books: {e}")
        top_books = []
    finally:
        conn.close()
    
    hijri_today = get_hijri_today()
    ay_period = get_academic_year_period()
    current_ay_year = get_current_ay_year()
    opac_base = get_opac_base()
    
    return render_template(
        "darajah_detail.html",
        darajah_info=darajah_info,
        darajah_name=darajah_name,
        students=students,
        total_students=total_students,
        page=page,
        total_pages=total_pages,
        per_page=per_page,
        top_books=top_books,
        hijri_today=hijri_today,
        ay_period=ay_period,
        current_ay_year=current_ay_year,
        OPAC_BASE=opac_base,
    )

@bp.route("/api/darajah-students/<darajah_name>")
def api_darajah_students(darajah_name):
    """API endpoint for getting students in a darajah with pagination."""
    if not session.get("logged_in"):
        return jsonify({"error": "Unauthorized"}), 401
    
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    
    darajah_name = darajah_name.replace('_', ' ')
    
    students, total_students, total_pages = get_darajah_students_breakdown(darajah_name, page, per_page)
    
    return jsonify({
        "success": True,
        "students": students,
        "total_students": total_students,
        "total_pages": total_pages,
        "current_page": page,
        "per_page": per_page
    })

@bp.route("/students/<identifier>")
def student_detail(identifier):
    """Student details page - links from top students table."""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role != "admin":
        flash("You must be logged in as an admin to access student details.", "danger")
        return redirect(url_for("dashboard_bp.dashboard"))
    
    student = KQ.find_student_by_identifier(identifier)
    
    if not student:
        flash(f"Student with identifier '{identifier}' not found.", "danger")
        return redirect(url_for("dashboard_bp.dashboard"))
    
    borrowed_books = KQ.borrowed_books_for(student["borrowernumber"])
    
    student["category"] = format_marhala_display_name(student.get("category", ""))
    
    hijri_today = get_hijri_today()
    opac_base = get_opac_base()
    
    return render_template(
        "student.html",
        student=student,
        borrowed_books=borrowed_books,
        hijri_today=hijri_today,
        OPAC_BASE=opac_base
    )

@bp.route("/calendar")
def calendar_page():
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))
    
    current_ay = get_current_ay_year()
    today_hijri = get_hijri_today()
    
    return render_template(
        "calendar.html",
        current_ay=current_ay,
        today_hijri=today_hijri
    )