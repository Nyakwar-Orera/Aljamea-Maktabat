"""
Marks Allotment Management Service
Handles admin configuration of maximum marks for Taqeem categories and programs.
"""

from db_app import get_conn
from config import Config
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def get_current_academic_year():
    """Return current academic year standardized (e.g., '1472')."""
    ay = Config.CURRENT_ACADEMIC_YEAR()
    return ay.replace('H', '').strip() if ay else ""

def get_all_allotments(academic_year=None):
    """
    Get all mark allotments for categories and programs.
    Returns list of dicts with category/program details and max_marks.
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    conn = get_conn()
    try:
        cur = conn.cursor()
        
        # Get category allotments (Books Physical/Digital, Reviews, Programs)
        cur.execute("""
            SELECT 
                'category' as type,
                name,
                '' as description,
                max_marks,
                academic_year,
                campus_branch,
                created_at,
                priority
            FROM marks_allotments 
            WHERE type = 'category' AND academic_year = ?
            ORDER BY campus_branch, priority, name
        """, (academic_year,))
        
        category_rows = cur.fetchall()
        
        # Get program allotments (specific library programs)
        cur.execute("""
            SELECT 
                'program' as type,
                title as name,
                department_note as description,
                marks as max_marks,
                academic_year,
                created_at
            FROM library_programs 
            WHERE marks_category != 'Books Issued' AND marks_category != 'Book Review'
            AND academic_year = ?
            ORDER BY date DESC
        """, (academic_year,))
        
        program_rows = cur.fetchall()
        
        # Combine results
        allotments = []
        for row in category_rows + program_rows:
            allotments.append({
                'type': row[0],
                'name': row[1],
                'description': row[2],
                'max_marks': row[3],
                'academic_year': row[4],
                'campus_branch': row[5] if row[0] == 'category' else 'Global',
                'created_at': row[6] if row[0] == 'category' else row[5],
                'id': row[7] if row[0] == 'category' else (row[6] if len(row) > 6 else None)
            })
        
        # Calculate totals
        category_total = sum(r[3] for r in category_rows)
        program_total = sum(r[3] for r in program_rows)
        
        return {
            'allotments': allotments,
            'category_total': category_total,
            'program_total': program_total,
            'grand_total': category_total + program_total
        }
    finally:
        conn.close()

def save_category_allotment(name, max_marks, description='', academic_year=None, campus_branch='Global', priority=0):
    """
    Save or update a category allotment (Books Physical, Reviews, etc.).
    Enforces case-insensitive uniqueness within an academic year and branch.
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    # Sanitize: trim and normalize common variations
    name = name.strip()
    # If the user enters something that sounds like "Books Issued", normalize it
    if name.lower().replace(' ', '') in ['bookissue', 'booksissue', 'booksissued', 'bookissued']:
        name = 'Books Issued'
    elif name.lower().replace(' ', '') in ['bookreview', 'bookreviews', 'reviews']:
        name = 'Book Reviews'
    
    conn = get_conn()
    try:
        cur = conn.cursor()
        
        # Check for case-insensitive match first to avoid duplicate names in DIFFERENT cases
        cur.execute("""
            SELECT name FROM marks_allotments 
            WHERE lower(name) = lower(?) AND academic_year = ? AND type = 'category' AND campus_branch = ?
        """, (name, academic_year, campus_branch))
        existing = cur.fetchone()
        
        if existing:
            # Update existing exact or case-matching entry
            cur.execute("""
                UPDATE marks_allotments 
                SET max_marks = ?, priority = ?, created_at = ?
                WHERE lower(name) = lower(?) AND academic_year = ? AND campus_branch = ?
            """, (max_marks, priority, datetime.now(), name, academic_year, campus_branch))
        else:
            # Insert new
            cur.execute("""
                INSERT INTO marks_allotments 
                (type, name, max_marks, academic_year, campus_branch, priority, created_at)
                VALUES ('category', ?, ?, ?, ?, ?, ?)
            """, (name, max_marks, academic_year, campus_branch, priority, datetime.now()))
        
        conn.commit()
        logger.info(f"Saved category allotment: {name} = {max_marks}pts for {academic_year}")
        return True
    except Exception as e:
        logger.error(f"Error saving category allotment: {e}")
        return False
    finally:
        conn.close()

def delete_category_allotment(name, academic_year=None):
    """
    Delete a category allotment.
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM marks_allotments 
            WHERE type = 'category' AND name = ? AND academic_year = ?
        """, (name, academic_year))
        
        deleted = cur.rowcount > 0
        conn.commit()
        
        if deleted:
            logger.info(f"Deleted category allotment: {name} for {academic_year}")
        
        return deleted
    except Exception as e:
        logger.error(f"Error deleting category allotment: {e}")
        return False
    finally:
        conn.close()

def get_category_allotment_max_marks(category_name, academic_year=None, campus_branch='Global'):
    """
    Get max marks for a specific category with branch-based fallback (Branch -> Global).
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    conn = get_conn()
    try:
        cur = conn.cursor()
        
        # 1. Try branch-specific allotment
        cur.execute("""
            SELECT max_marks FROM marks_allotments 
            WHERE type = 'category' AND name = ? AND academic_year = ? AND campus_branch = ?
        """, (category_name, academic_year, campus_branch))
        
        result = cur.fetchone()
        if result:
            return result[0]
            
        # 2. Fallback to Global if branch not found
        if campus_branch != 'Global':
            cur.execute("""
                SELECT max_marks FROM marks_allotments 
                WHERE type = 'category' AND name = ? AND academic_year = ? AND campus_branch = 'Global'
            """, (category_name, academic_year))
            fallback = cur.fetchone()
            if fallback:
                return fallback[0]
                
        return None
    finally:
        conn.close()

def override_student_books_issued(student_username, physical_issued, digital_issued, academic_year=None):
    """
    Admin override for manual book issued counts (bypasses Koha auto-calc).
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    conn = get_conn()
    try:
        cur = conn.cursor()
        
        # Save override to new table
        cur.execute("""
            INSERT OR REPLACE INTO student_books_override 
            (student_username, academic_year, physical_books_issued, digital_books_issued, 
             overridden_by, overridden_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (student_username, academic_year, physical_issued, digital_issued, 
              'admin', datetime.now()))
        
        conn.commit()
        logger.info(f"Book override saved for {student_username}: P:{physical_issued} D:{digital_issued}")
        return True
    except Exception as e:
        logger.error(f"Error saving book override: {e}")
        return False
    finally:
        conn.close()

def get_student_books_override(student_username, academic_year=None):
    """
    Get manual book override for student (if exists).
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT physical_books_issued, digital_books_issued 
            FROM student_books_override 
            WHERE student_username = ? AND academic_year = ?
        """, (student_username, academic_year))
        
        result = cur.fetchone()
        if result:
            return {
                'physical_issued': result[0],
                'digital_issued': result[1],
                'overridden': True
            }
        return {'overridden': False}
    finally:
        conn.close()

def clear_student_books_override(student_username, academic_year=None):
    """
    Remove manual override, revert to auto-calc from Koha.
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM student_books_override 
            WHERE student_username = ? AND academic_year = ?
        """, (student_username, academic_year))
        
        deleted = cur.rowcount > 0
        conn.commit()
        return deleted
    except Exception as e:
        logger.error(f"Error clearing override: {e}")
        return False
    finally:
        conn.close()

