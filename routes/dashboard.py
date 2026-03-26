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
    return current_app.config.get("KOHA_OPAC_BASE_URL", "https://library-nairobi.jameasaifiyah.org")

# ---------------- AY WINDOW ----------------
def get_academic_year_period():
    """Get formatted Academic Year period in Hijri."""
    start, end = KQ.get_ay_bounds()
    
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
def get_kpis(selected_marhala=None):
    """Get key performance indicators, optionally filtered by marhala."""
    s = KQ.get_summary(selected_marhala)
    
    active_patrons = int(s.get("active_patrons", 0))
    student_patrons = int(s.get("student_patrons", 0))
    non_student_patrons = int(s.get("non_student_patrons", 0))
    total_issues = int(s.get("total_issues", 0))
    total_fees = float(s.get("fees_paid", 0.0))
    overdue = int(s.get("overdue", 0))
    total_titles = int(s.get("total_titles", 0))
    total_titles_issued = int(s.get("total_titles_issued", 0))
    currently_issued = int(s.get("currently_issued", 0))
    
    # Calculate currently issued fees in Kenya Shillings
    currently_issued_fees = calculate_current_fees_in_kes(selected_marhala)
    
    return {
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
        "active_patrons_ay": int(s.get("active_patrons_ay", 0))
    }

def calculate_current_fees_in_kes(selected_marhala=None):
    """Calculate total fees for currently issued books in Kenya Shillings, optionally filtered."""
    start, end = KQ.get_ay_bounds()
    conn = get_koha_conn()
    try:
        cur = conn.cursor(dictionary=True)  # Use dictionary cursor
        query = """
            SELECT COALESCE(SUM(
                CASE 
                    WHEN i.date_due < CURDATE() 
                    THEN DATEDIFF(CURDATE(), i.date_due) * 10
                    ELSE 0 
                END
            ), 0) AS total_fees_kes
            FROM issues i
            JOIN borrowers b ON i.borrowernumber = b.borrowernumber
            LEFT JOIN categories c ON b.categorycode = c.categorycode
            WHERE i.returndate IS NULL
              AND i.date_due < CURDATE()
              AND DATE(i.issuedate) BETWEEN %s AND %s
        """
        params = [start, end]
        if selected_marhala:
            query += " AND (c.description = %s OR b.categorycode = %s)"
            params.extend([selected_marhala, selected_marhala])
            
        cur.execute(query, params)
        result = cur.fetchone()
        total_fees_kes = float(result.get("total_fees_kes", 0) if result else 0.0)
        cur.close()
        return total_fees_kes
    except Exception as e:
        current_app.logger.error(f"Error calculating fees: {e}")
        return 0.0
    finally:
        conn.close()

def get_today_activity():
    """Get today's activity stats."""
    return KQ.today_activity()

def get_trends(selected_marhala=None):
    """Get borrowing trends for the current Academic Year, optionally filtered."""
    try:
        labels, values = KQ.get_ay_trend_data(marhala_code=selected_marhala)
        return labels, values
    except Exception as e:
        current_app.logger.error(f"Error getting trends: {e}")
        return [], []

def get_darajah_distribution():
    """Get gender distribution by Darajah with updated ranges."""
    start, end = KQ.get_ay_bounds()
    if not start:
        return [], [], []

    if hasattr(KQ, "get_gender_darajah_distribution"):
        return KQ.get_gender_darajah_distribution()
    
    return [], [], []

# ---------------- MARHALA FUNCTIONS ----------------
def get_all_marhalas():
    """Get all Marhala names for filter dropdown."""
    try:
        return KQ.get_all_marhalas()
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

def get_marhala_distribution():
    """Get Marhala distribution."""
    try:
        return KQ.get_marhala_distribution_with_dars_burhani()
    except Exception as e:
        current_app.logger.error(f"Error getting marhala distribution: {e}")
        try:
            rows = KQ.get_marhala_summary()
            labels = []
            values = []
            for row in rows:
                marhala_name = format_marhala_display_name(row.get("Marhala", "Unknown"))
                labels.append(marhala_name)
                values.append(row.get("BooksIssued", 0))
            return labels, values
        except:
            return [], []

def get_darajah_summary_by_marhala(marhala_name=None):
    """Get Darajah summary filtered by Marhala."""
    try:
        rows = KQ.get_darajah_summary_by_marhala(marhala_name)
        for row in rows:
            row["Marhala"] = format_marhala_display_name(row.get("Marhala", ""))
        return rows
    except Exception as e:
        current_app.logger.error(f"Error getting darajah summary: {e}")
        return []

def get_top_darajah_summary_with_asateza_last():
    """Get top Darajah summary."""
    if hasattr(KQ, "get_top_darajah_summary_with_asateza_last"):
        try:
            rows = KQ.get_top_darajah_summary_with_asateza_last()
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
def get_academic_marhalas_performance():
    """Get academic marhalas performance."""
    try:
        start, end = KQ.get_ay_bounds()
        conn = get_koha_conn()
        cur = conn.cursor(dictionary=True)  # Use dictionary cursor

        academic_codes = ['S-CO', 'S-CGB', 'S-CGA', 'S-CT', 'S-DARS']
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

        formatted_rows = []
        for row in rows:
            marhala_name = row.get("Marhala", "")
            if not marhala_name:
                continue

            active_patrons = int(row.get("ActivePatrons", 0) or 0)
            issues = int(row.get("Issues", 0) or 0)
            issues_per_patron = round(issues / active_patrons, 2) if active_patrons > 0 else 0.0

            formatted_rows.append({
                "MARHALA": format_marhala_display_name(marhala_name),
                "ISSUES": issues,
                "PATRONS": active_patrons,
                "ISSUES/PATRON": issues_per_patron,
                "TYPE": "Academic"
            })

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

def get_non_academic_marhalas_performance():
    """Get non-academic marhalas performance."""
    try:
        start, end = KQ.get_ay_bounds()
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
def get_all_darajahs_detailed():
    """Get all darajahs with detailed breakdown."""
    start, end = KQ.get_ay_bounds()
    if not start:
        return []

    conn = get_koha_conn()
    try:
        cur = conn.cursor(dictionary=True)  # Use dictionary cursor
        
        current_app.logger.info(f"Academic Year Dates: {start} to {end}")
        
        cur.execute("""
            SELECT 
                COALESCE(std.attribute, b.branchcode) AS Darajah,
                MAX(COALESCE(c.description, b.categorycode)) AS Marhala,
                COUNT(s.datetime) AS TotalIssues,
                COUNT(DISTINCT b.borrowernumber) AS TotalStudents,
                COUNT(DISTINCT 
                    CASE WHEN trno.attribute IS NOT NULL AND trno.attribute != ''
                    THEN b.borrowernumber END
                ) AS ActiveStudents,
                COUNT(DISTINCT 
                    CASE WHEN s.borrowernumber IS NOT NULL 
                    THEN s.borrowernumber END
                ) AS ParticipatingStudents,
                COUNT(DISTINCT it.biblionumber) AS UniqueTitles,
                ROUND(COUNT(s.datetime) / NULLIF(COUNT(DISTINCT 
                    CASE WHEN trno.attribute IS NOT NULL AND trno.attribute != ''
                    THEN b.borrowernumber END
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
                AND DATE(s.`datetime`) BETWEEN %s AND %s
            LEFT JOIN items it ON s.itemnumber = it.itemnumber
            WHERE (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
              AND COALESCE(std.attribute, b.branchcode) IS NOT NULL
              AND COALESCE(std.attribute, b.branchcode) != ''
            GROUP BY COALESCE(std.attribute, b.branchcode)
            ORDER BY TotalIssues DESC;
        """, (start, end))
        
        rows = cur.fetchall()
        
        for row in rows:
            row["Marhala"] = format_marhala_display_name(row.get("Marhala", ""))
            avg_issues = row.get("AvgIssuesPerStudent") or 0
            if avg_issues >= 5:
                row["Efficiency"] = "Excellent"
                row["EfficiencyColor"] = "success"
            elif avg_issues >= 3:
                row["Efficiency"] = "Good"
                row["EfficiencyColor"] = "primary"
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

def get_currently_issued_by_marhala(marhala_name=None):
    """Get currently issued books by marhala."""
    try:
        result = KQ.get_department_currently_issued(marhala_name)
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
def get_key_insights():
    """Get key insights from the data."""
    try:
        return KQ.get_key_insights()
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
def get_top_darajah_performance():
    """Get top 10 Darajah performance (excluding Asateza)."""
    try:
        all_darajahs = get_all_darajahs_detailed()
        
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

def get_top_students(limit=10, selected_marhala=None):
    """Get top 10 individual students with links to student details."""
    start, end = KQ.get_ay_bounds()
    if not start:
        return []

    try:
        rows = KQ.get_top_students(limit, selected_marhala)
        
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
                "StudentLink": student_link
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
            LEFT JOIN borrower_attributes trno
                ON trno.borrowernumber = b.borrowernumber
                AND trno.code = 'TRNO'
            WHERE (std.attribute = %s OR b.branchcode = %s)
              AND trno.attribute IS NOT NULL
              AND trno.attribute != ''
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
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
              AND trno.attribute IS NOT NULL
              AND trno.attribute != ''
              AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
            ORDER BY StudentName ASC
            LIMIT %s OFFSET %s;
        """, (start, end, start, end, darajah_name, darajah_name, per_page, (page-1)*per_page))

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
    elif role != "admin":
        flash("You must be logged in as an admin to view the Admin Analytics.", "danger")
        return redirect(url_for("auth_bp.login"))

    if request.method == "POST":
        selected_marhala = (request.form.get("marhalaFilter") or "").strip()
        session["selected_marhala"] = selected_marhala
    else:
        selected_marhala = (session.get("selected_marhala") or "").strip()

    if not selected_marhala:
        selected_marhala = None

    ay_period = get_academic_year_period()

    t0 = time.time()
    kpi_data = get_kpis(selected_marhala)
    current_app.logger.info(f"⚡ get_kpis took: {time.time() - t0:.4f}s")

    t0 = time.time()
    today_checkouts, today_checkins = get_today_activity()
    current_app.logger.info(f"⚡ get_today_activity took: {time.time() - t0:.4f}s")

    t0 = time.time()
    trend_labels, trend_values = get_trends(selected_marhala)
    current_app.logger.info(f"⚡ get_trends took: {time.time() - t0:.4f}s")

    t0 = time.time()
    darajah_labels, darajah_male_values, darajah_female_values = get_darajah_distribution()
    current_app.logger.info(f"⚡ get_darajah_distribution took: {time.time() - t0:.4f}s")

    t0 = time.time()
    marhala_labels, marhala_values = get_marhala_distribution()
    current_app.logger.info(f"⚡ get_marhala_distribution took: {time.time() - t0:.4f}s")

    t0 = time.time()
    lang_top = get_language_top25(selected_marhala)
    current_app.logger.info(f"⚡ get_language_top25 took: {time.time() - t0:.4f}s")
    
    t0 = time.time()
    marhala_counts = get_marhala_counts()
    current_app.logger.info(f"⚡ get_marhala_counts took: {time.time() - t0:.4f}s")
    
    t0 = time.time()
    if selected_marhala:
        darajah_summary_rows = get_darajah_summary_by_marhala(selected_marhala)
        marhala_summary_rows = get_marhala_summary(selected_marhala)
    else:
        darajah_summary_rows = get_top_darajah_summary_with_asateza_last()
        marhala_summary_rows = get_marhala_summary(None)
    current_app.logger.info(f"⚡ summary_rows took: {time.time() - t0:.4f}s")
    
    all_marhalas_raw = get_all_marhalas()
    all_marhalas = [format_marhala_display_name(m) for m in all_marhalas_raw]
    
    academic_marhalas_list = [format_marhala_display_name(m) for m in get_academic_marhalas_list()]
    non_academic_marhalas_list = [format_marhala_display_name(m) for m in get_non_academic_marhalas_list()]
    
    academic_marhalas = get_academic_marhalas_performance()
    non_academic_marhalas = get_non_academic_marhalas_performance()
    
    currently_issued_data = get_currently_issued_by_marhala(selected_marhala)
    
    insights = get_key_insights()
    
    top_darajah_performance = get_top_darajah_performance()
    
    top_students = get_top_students(10, selected_marhala)
    
    all_darajahs_detailed = get_all_darajahs_detailed()

    hijri_today = get_hijri_today()
    
    opac_base = get_opac_base()

    kpi_cards = [
        {
            "key": "total_patrons",
            "label": "Total Active Patrons",
            "value": f"{kpi_data['active_patrons']:,}",
            "icon": "fa-users",
            "color": "primary",
            "subtext": f"{kpi_data['student_patrons']:,} students, {kpi_data['non_student_patrons']:,} non-students"
        },
        {
            "key": "total_issues",
            "label": "Books Issued (AY)",
            "value": f"{kpi_data['total_issues']:,}",
            "icon": "fa-book-open",
            "color": "success",
            "subtext": f"{kpi_data['total_titles_issued']:,} distinct titles"
        },
        {
            "key": "currently_issued",
            "label": "Currently Issued",
            "value": f"{kpi_data['currently_issued']:,}",
            "icon": "fa-book",
            "color": "info",
            "subtext": f"{kpi_data['overdue']:,} overdue (KSh {kpi_data['currently_issued_fees']:,.2f})"
        },
        {
            "key": "today_activity",
            "label": "Today's Activity",
            "value": f"{today_checkouts} / {today_checkins}",
            "icon": "fa-calendar-day",
            "color": "warning",
            "subtext": f"Checkouts / Checkins"
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
        selected_marhala=format_marhala_display_name(selected_marhala) if selected_marhala else None,
        filter_indicator=filter_indicator,
        top_darajah_performance=top_darajah_performance,
        top_students=top_students,
        all_darajahs_detailed=all_darajahs_detailed,
        OPAC_BASE=opac_base,
        current_fees_kes=kpi_data['currently_issued_fees'],
        overdue_count=kpi_data['overdue'],
        marhala_counts=marhala_counts
    )

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
    
    return render_template(
        "darajah_explorer.html",
        darajahs=all_darajahs,
        total_darajahs=len(all_darajahs),
        all_marhalas=all_marhalas,
        darajah_numbers=darajah_numbers,
        hijri_today=hijri_today,
        OPAC_BASE=opac_base,
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