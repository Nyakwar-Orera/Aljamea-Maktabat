"""
Service module for calculating and managing student Taqeem (marks).

This module handles:
1. Book issue marks calculation (60 marks: physical + digital)
2. Book review marks tracking (30 marks) - including CSV and Excel upload
3. Library program attendance marks (10 marks)
4. Total Taqeem calculation (100 marks)
"""

from db_app import get_conn
from db_koha import get_koha_conn
import logging
import csv
import io
import os
import sqlite3
from datetime import datetime
import pandas as pd
from io import BytesIO
from config import Config
from services.koha_queries import find_student_by_identifier, get_ay_bounds

logger = logging.getLogger(__name__)


def get_current_academic_year():
    """Return the current academic year from config, standardized (e.g., '1472')."""
    return Config.CLEAN_ACADEMIC_YEAR()


def process_book_review_upload(file, file_type='csv', academic_year=None, review_name=None, max_marks=30.0, overwrite=False):
    """
    Process uploaded file containing book review marks (CSV or Excel).
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    if not file:
        return {'success': False, 'error': 'No file uploaded'}
    
    try:
        if file_type.lower() == 'csv':
            return process_csv_file(file, academic_year, review_name, max_marks, overwrite)
        elif file_type.lower() in ['xlsx', 'xls', 'excel']:
            return process_excel_file(file, academic_year, review_name, max_marks, overwrite)
        else:
            return {'success': False, 'error': f'Unsupported file type: {file_type}'}
    except Exception as e:
        logger.error(f"Error processing {file_type} file: {e}")
        return {'success': False, 'error': f'Error processing file: {str(e)}'}


def process_csv_file(csv_file, academic_year=None, review_name=None, max_marks=30.0, overwrite=False):
    """Process CSV file containing book review marks."""
    if academic_year is None:
        academic_year = get_current_academic_year()
    try:
        if hasattr(csv_file, 'read'):
            content = csv_file.read()
            if isinstance(content, bytes):
                content = content.decode('utf-8')
        else:
            content = csv_file
        
        csv_reader = csv.DictReader(io.StringIO(content))
        fieldnames = csv_reader.fieldnames
        if not fieldnames:
            return {'success': False, 'error': 'CSV file has no headers'}
        
        # Identity column check
        has_trno = any(col.lower() in ['trno', 'student_trno', 'studentid', 'its'] for col in fieldnames)
        if not has_trno:
            return {'success': False, 'error': "CSV missing identity column (Trno/ITS)"}
        
        return process_book_review_rows(csv_reader, academic_year, source='csv', 
                                       review_name=review_name, max_marks=max_marks, overwrite=overwrite)

        
    except Exception as e:
        logger.error(f"Error processing CSV file: {e}")
        return {'success': False, 'error': f'Error processing CSV file: {str(e)}'}


def process_excel_file(excel_file, academic_year=None, review_name=None, max_marks=30.0, overwrite=False):
    """Process Excel file containing book review marks."""
    if academic_year is None:
        academic_year = get_current_academic_year()
    try:
        if hasattr(excel_file, 'read'):
            excel_data = pd.read_excel(BytesIO(excel_file.read()), sheet_name=None, header=None)
        else:
            excel_data = pd.read_excel(excel_file, sheet_name=None, header=None)
        
        processed_results = {
            'success': True,
            'total_processed': 0,
            'total_errors': 0,
            'sheet_results': [],
            'error_messages': []
        }
        
        for sheet_name, df in excel_data.items():
            logger.info(f"Processing sheet: {sheet_name}")
            df = df.where(pd.notnull(df), None)
            rows = df.to_dict('records')
            
            sheet_result = process_excel_sheet(rows, sheet_name, academic_year, review_name, max_marks, overwrite)
            
            if not sheet_result:
                logger.warning(f"Process sheet '{sheet_name}' returned None")
                continue

            processed_results['total_processed'] += sheet_result.get('processed', 0)
            processed_results['total_errors'] += sheet_result.get('errors', 0)
            processed_results['sheet_results'].append({
                'sheet_name': sheet_name,
                'processed': sheet_result.get('processed', 0),
                'errors': sheet_result.get('errors', 0),
                'total_rows': sheet_result.get('total_rows', 0)
            })
            
            if sheet_result.get('error_messages'):
                processed_results['error_messages'].extend([
                    f"Sheet '{sheet_name}': {msg}" 
                    for msg in sheet_result.get('error_messages', [])
                ])
        
        return processed_results
        
    except Exception as e:
        logger.error(f"Error processing Excel file: {e}")
        return {'success': False, 'error': f'Error processing Excel file: {str(e)}'}


def process_excel_sheet(rows, sheet_name, academic_year=None, review_name=None, max_marks=30.0, overwrite=False):
    """Process rows from an Excel sheet."""
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        success_count = 0
        error_count = 0
        errors = []
        processed_students = []
        
        header_row_idx = -1
        col_map = {}
        
        for i, row in enumerate(rows[:10]):
            row_values = [str(v).strip().lower().replace(' ', '').replace('_', '').replace('.', '') for k, v in row.items() if v is not None]
            
            if any(x in ['trno', 'studentid', 'id', 'studenttrno'] for x in row_values):
                header_row_idx = i
                for k, v in row.items():
                    if v:
                        normalized_key = str(v).strip().lower().replace(' ', '').replace('_', '').replace('.', '')
                        col_map[k] = normalized_key
                break
                
        if header_row_idx == -1:
            return {
                'success': False,
                'errors': 1,
                'total_rows': 0,
                'error_messages': ["Could not find header row containing 'Trno'"]
            }
            
        logger.info(f"Found header at row {header_row_idx}: {col_map}")

        for row_num, row in enumerate(rows[header_row_idx+1:], start=header_row_idx+2):
            try:
                normalized_row = {}
                for col_idx, col_name in col_map.items():
                    if col_idx in row:
                        normalized_row[col_name] = row[col_idx]
                
                trno = extract_column_value(normalized_row, [
                    'trno', 'student_trno', 'studentid', 'id', 'studenttrno', 'its', 'itsid', 'ejamaatid', 'regno'
                ])
                full_name = extract_column_value(normalized_row, [
                    'studentname', 'fullname', 'name', 'full_name', 'engname', 'displayname'
                ])
                marks_str = extract_column_value(normalized_row, [
                    'markstotal', 'marks', 'mark', 'score', 'obtained', 'marksobtained', 'taqeem'
                ])
                total_str = extract_column_value(normalized_row, ['total', 'totalscore', 'sum', 'max', 'outof'])
                percent_str = extract_column_value(normalized_row, ['percent', 'percentage', '％'])
                class_name = extract_column_value(normalized_row, ['darajaheg1am', 'darajah', 'classname', 'class', 'division', 'div'])
                remarks = extract_column_value(normalized_row, ['remarks', 'note', 'comment', 'review', 'feedback', 'bookofyourchoice', 'book_of_your_choice'])
                
                # New Multi-Review Fields
                ay_val = extract_column_value(normalized_row, ['academicyear', 'ay', 'year'])

                # Review session selection
                assigned_review = review_name
                row_review = extract_column_value(normalized_row, ['bookreviewname', 'reviewname', 'assignment', 'title', 'reviewtitle'])
                if row_review:
                    assigned_review = row_review
                if not assigned_review:
                    assigned_review = "Initial Upload"

                
                if not class_name and sheet_name:
                    class_name = sheet_name

                if not trno:
                    continue
                
                if isinstance(trno, (int, float)):
                    trno = str(int(trno)).strip()
                else:
                    trno = str(trno).strip()
                
                if not trno or trno.lower() in ['none', 'nan', 'null', '']:
                    continue
                
                marks = 0
                
                if marks_str is not None and marks_str != '' and marks_str != 'N/A':
                    try:
                        if isinstance(marks_str, (int, float)):
                            marks = float(marks_str)
                        else:
                            marks = float(str(marks_str).strip())
                    except (ValueError, TypeError):
                        pass
                
                if marks == 0 and total_str is not None and total_str != '' and total_str != 'N/A':
                    try:
                        if isinstance(total_str, (int, float)):
                            total_value = float(total_str)
                        else:
                            total_value = float(str(total_str).strip())
                        
                        if total_value <= 40:
                            marks = (total_value / 40) * 30
                    except (ValueError, TypeError):
                        pass
                
                if marks == 0 and percent_str is not None and percent_str != '' and percent_str != 'N/A':
                    try:
                        if isinstance(percent_str, (int, float)):
                            percent_value = float(percent_str)
                        else:
                            percent_str_clean = str(percent_str).strip().replace('%', '').replace('％', '')
                            percent_value = float(percent_str_clean)
                        
                        marks = (percent_value / 100) * 30
                        if not percent_val:
                            percent_val = percent_value
                    except (ValueError, TypeError):
                        pass

                # Scaling calculation
                if max_marks != 30.0:
                    try:
                        scale_factor = 30.0 / float(max_marks)
                        marks = marks * scale_factor
                    except: pass

                if marks == 0:
                    # Final fallback: if marks is zero but we have a grade, assign a default mark
                    if grade:
                        g = str(grade).upper().strip()
                        if g == 'A': marks = 25
                        elif g == 'B': marks = 20
                        elif g == 'C': marks = 15
                        elif g == 'D': marks = 10
                
                if marks == 0:
                    errors.append(f"Row {row_num}: No valid marks found for Trno {trno}")
                    error_count += 1
                    continue
                
                marks = min(max(0, marks), 30)
                
                # Normalize Percent for storage
                if not percent_val:
                    percent_val = (marks / 30) * 100
                
                # Review session selection
                assigned_review = review_name
                if row_review:
                    assigned_review = row_review
                if not assigned_review:
                    assigned_review = "Initial Upload"
                
                # Ensure AY
                if not ay_val:
                    ay_val = academic_year
                else:
                    ay_val = str(ay_val).replace('H', '').strip()
                
                username_from_trno = f"TR{trno}"
                
                cur.execute("""
                    SELECT username, teacher_name, class_name
                    FROM users 
                    WHERE username = ? OR username = ? OR trno = ?
                """, (trno, username_from_trno, trno))
                
                student_result = cur.fetchone()
                
                if student_result:
                    username = student_result[0]
                    if not full_name and student_result[1]:
                        full_name = student_result[1]
                    if not class_name and student_result[2]:
                        class_name = student_result[2]
                else:
                    username = trno
                    logger.warning(f"Student with Trno {trno} not found in users table")
                
                # Branch-aware mapping
                # We assume the user who is uploading has a branch context in session, 
                # but for individual rows we try to inherit from the user record if it exists.
                branch_code = 'AJSN'
                campus_branch = 'Global'
                
                cur.execute("""
                    SELECT branch_code, campus_branch
                    FROM users 
                    WHERE username = ? OR username = ? OR trno = ?
                """, (trno, username_from_trno, trno))
                
                user_branch_res = cur.fetchone()
                if user_branch_res:
                    branch_code = user_branch_res[0] or 'AJSN'
                    campus_branch = user_branch_res[1] or 'Global'

                if overwrite:
                    cur.execute("""
                        DELETE FROM book_review_marks 
                        WHERE student_username = ? AND academic_year = ? AND book_review_name = ?
                    """, (username, str(ay_val).replace('H', '').strip(), assigned_review))

                cur.execute("""
                    INSERT INTO book_review_marks (
                        student_username, student_trno, student_name, darajah_name, 
                        academic_year, marks, review_count, remarks, source, uploaded_by, 
                        uploaded_at, book_review_name, hijri_month, percent, grade
                    ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    username,
                    trno,
                    full_name if full_name else None,
                    class_name,
                    ay_val,
                    marks,
                    remarks,
                    f"Excel ({sheet_name})" if sheet_name else "Excel",
                    'admin',  # uploaded_by
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    assigned_review,
                    month,
                    percent_val,
                    grade
                ))

                
                success_count += 1
                processed_students.append(username)
                
            except Exception as e:
                errors.append(f"Row {row_num}: Error processing Trno {row.get('Trno', 'Unknown')}: {str(e)}")
                error_count += 1
                continue
        
        conn.commit()
        
        for student in processed_students:
            try:
                update_student_taqeem(student, academic_year)
            except Exception as e:
                logger.error(f"Error updating Taqeem for student {student}: {e}")
        
        return {
            'success': True,
            'processed': success_count,
            'errors': error_count,
            'total_rows': success_count + error_count,
            'error_messages': errors
        }
    finally:
        if conn:
            conn.close()


def extract_column_value(row_dict, possible_keys):
    """Extract value from row dictionary using possible key names."""
    for key in possible_keys:
        if key in row_dict:
            value = row_dict[key]
            if value is not None and value != '':
                return value
    return None


def process_book_review_rows(rows, academic_year=None, source='csv', review_name=None, max_marks=30.0, overwrite=False):
    """
    Process book review marks from rows.
    Scales marks from max_marks down to 30.0 (the system standard).
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    if not review_name:
        review_name = "Initial Upload"
    
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        success_count = 0
        error_count = 0
        errors = []
        processed_students = []
        
        for row_num, row in enumerate(rows, start=2):
            try:
                trno = extract_column_value(row, [
                    'Trno', 'trno', 'student_trno', 'studentid', 'id', 'studenttrno'
                ])
                marks_str = extract_column_value(row, [
                    'marks_total', 'Marks', 'marks', 'mark', 'score', 'obtained', 'taqeem'
                ])
                student_name = extract_column_value(row, [
                    'student_name', 'FullName', 'Name', 'fullname', 'name'
                ])
                darajah_name = extract_column_value(row, [
                    'darajah_(eg: "1 A M")', 'ClassName', 'classname', 'class', 'darajah'
                ])
                remarks = extract_column_value(row, [
                    'Remarks', 'remarks', 'note', 'comment', 'review', 'bookofyourchoice'
                ])
                # Multi-Review Fields
                review_name = extract_column_value(row, ['book_review_name', 'review_name', 'bookreviewname', 'reviewname', 'assignment'])
                grade = extract_column_value(row, ['grade', 'result', 'rating'])
                month = extract_column_value(row, ['hijri_month', 'hijrimonth', 'month', 'period'])
                percent_val = extract_column_value(row, ['percent', 'percentage', '％'])
                
                row_academic_year = extract_column_value(row, ['academic_year', 'AcademicYear', 'academicyear', 'ay', 'year']) or academic_year


                
                if not trno:
                    errors.append(f"Row {row_num}: Missing Trno")
                    error_count += 1
                    continue
                
                if isinstance(trno, (int, float)):
                    trno = str(int(trno)).strip()
                else:
                    trno = str(trno).strip()

                if not marks_str:
                    errors.append(f"Row {row_num}: Missing Marks for Trno {trno}")
                    error_count += 1
                    continue
                
                try:
                    marks = float(marks_str)
                except (ValueError, TypeError):
                    marks = 0

                # Scaling calculation
                if max_marks != 30.0:
                    try:
                        # Scale obtained marks to the 30-mark standard
                        scale_factor = 30.0 / float(max_marks)
                        marks = marks * scale_factor
                    except: pass

                # Fallback calculation if marks is zero
                if marks == 0 and percent_val:
                    try:
                        p = float(str(percent_val).replace('%', ''))
                        marks = (p / 100) * 30
                    except: pass
                
                if marks == 0 and grade:
                    g = str(grade).upper().strip()
                    if g == 'A': marks = 25
                    elif g == 'B': marks = 20
                    elif g == 'C': marks = 15
                    elif g == 'D': marks = 10
                
                if marks == 0:
                    errors.append(f"Row {row_num}: Invalid or missing marks for Trno {trno}")
                    error_count += 1
                    continue
                
                marks = min(max(0, marks), 30)
                if not percent_val:
                    percent_val = (marks / 30) * 100
                
                # Use provided review name or fall back to row specific one if exists
                assigned_review = review_name
                row_review = extract_column_value(row, ['book_review_name', 'review_name', 'bookreviewname', 'reviewname', 'assignment'])
                if row_review:
                    assigned_review = row_review

                
                username_from_trno = f"TR{trno}"
                
                cur.execute("""
                    SELECT username, teacher_name 
                    FROM users 
                    WHERE username = ? OR username = ? OR trno = ?
                """, (trno, username_from_trno, trno))
                
                student_result = cur.fetchone()
                
                if student_result:
                    username = student_result[0]
                    if not student_name and student_result[1]:
                        student_name = student_result[1]
                else:
                    username = trno
                    logger.warning(f"Student with Trno {trno} not found in users table, using Trno as username")
                
                # Branch-aware mapping
                branch_code = 'AJSN'
                campus_branch = 'Global'
                
                cur.execute("""
                    SELECT branch_code, campus_branch
                    FROM users 
                    WHERE username = ? OR username = ? OR trno = ?
                """, (trno, username_from_trno, trno))
                
                user_branch_res = cur.fetchone()
                if user_branch_res:
                    branch_code = user_branch_res[0] or 'AJSN'
                    campus_branch = user_branch_res[1] or 'Global'

                if overwrite:
                    # Clear existing record for this student + session combination
                    cur.execute("""
                        DELETE FROM book_review_marks 
                        WHERE student_username = ? AND academic_year = ? AND book_review_name = ?
                    """, (username, str(row_academic_year).replace('H', '').strip(), assigned_review))

                cur.execute("""
                    INSERT INTO book_review_marks (
                        student_username, student_trno, student_name, darajah_name, 
                        academic_year, marks, review_count, remarks, source, uploaded_by, 
                        uploaded_at, book_review_name, hijri_month, percent, grade
                    ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    username,
                    trno,
                    student_name if student_name else None,
                    darajah_name,
                    str(row_academic_year).replace('H', '').strip(),
                    marks,
                    remarks,
                    source,
                    'admin',
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    assigned_review,
                    month,
                    percent_val,
                    grade
                ))

                
                success_count += 1
                processed_students.append(username)
                
            except Exception as e:
                errors.append(f"Row {row_num}: Error processing Trno {row.get('Trno', 'Unknown')}: {str(e)}")
                error_count += 1
                continue
        
        conn.commit()
        
        for student in processed_students:
            try:
                update_student_taqeem(student, academic_year)
            except Exception as e:
                logger.error(f"Error updating Taqeem for student {student}: {e}")
        
        return {
            'success': True,
            'processed': success_count,
            'errors': error_count,
            'error_messages': errors,
            'total_rows': success_count + error_count
        }
    finally:
        if conn:
            conn.close()


def get_student_level_weights(darajah_name, academic_year=None):
    """Get the physical and digital book weights for a student's class level."""
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT physical_books_weight, digital_books_weight
            FROM student_levels
            WHERE darajah_name = ? AND academic_year = ?
        """, (darajah_name, academic_year))
        
        result = cur.fetchone()
        
        if result:
            return result[0], result[1]
        else:
            logger.warning(f"No level weights found for {darajah_name}, using defaults")
            return 40.0, 20.0
    finally:
        if conn:
            conn.close()


def calculate_book_issue_marks(student_username, academic_year=None):
    """
    Calculate marks for book issues (physical + digital).
    Returns 40 marks max for physical books only.
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    # Get student info from centralized Koha query
    patron = find_student_by_identifier(student_username)
    if not patron:
        return {
            'physical_count': 0,
            'digital_count': 0,
            'physical_marks': 0.0,
            'digital_marks': 0.0,
            'total': 0.0,
            'max_marks': 60.0,
            'branch_code': 'AJSN'
        }
    
    darajah_name = patron.get('darajah')
    borrower_number = patron.get('borrowernumber')
    branch_code = patron.get('branchcode', 'AJSN')
    
    # Get Academic Year bounds
    start_ay, end_ay = get_ay_bounds()
    
    physical_count = 0
    digital_count = 0
    
    if borrower_number and start_ay and end_ay:
        try:
            from db_koha import get_branch_conn
            conn_koha = get_branch_conn(branch_code)
            try:
                cur_koha = conn_koha.cursor(dictionary=True)
                # Get physical books issued (itype != 'DIGITAL' or NULL)
                cur_koha.execute("""
                    SELECT COUNT(DISTINCT it.itemnumber) as count
                    FROM statistics s
                    JOIN items it ON s.itemnumber = it.itemnumber
                    WHERE s.borrowernumber = %s
                    AND s.type = 'issue'
                    AND DATE(s.datetime) BETWEEN %s AND %s
                    AND (it.itype != 'DIGITAL' OR it.itype IS NULL)
                """, (borrower_number, start_ay, end_ay))
                
                res = cur_koha.fetchone()
                physical_count = res['count'] if res else 0
                
                # Get digital books issued (itype = 'DIGITAL')
                cur_koha.execute("""
                    SELECT COUNT(DISTINCT it.itemnumber) as count
                    FROM statistics s
                    JOIN items it ON s.itemnumber = it.itemnumber
                    WHERE s.borrowernumber = %s
                    AND s.type = 'issue'
                    AND DATE(s.datetime) BETWEEN %s AND %s
                    AND it.itype = 'DIGITAL'
                """, (borrower_number, start_ay, end_ay))
                
                res = cur_koha.fetchone()
                digital_count = res['count'] if res else 0
                
            finally:
                conn_koha.close()
                
        except Exception as e:
            logger.error(f"Error querying Koha for book issues: {e}")
    
    # Calculate physical book marks (max 40)
    physical_target = 40
    physical_marks = min((physical_count / physical_target) * 40, 40) if physical_target > 0 else 0
    
    # Calculate digital book marks (max 20)
    digital_target = 20
    digital_marks = min((digital_count / digital_target) * 20, 20) if digital_target > 0 else 0
    
    total_marks = physical_marks + digital_marks
    
    return {
        'physical_count': physical_count,
        'digital_count': digital_count,
        'physical_marks': round(physical_marks, 2),
        'digital_marks': round(digital_marks, 2),
        'total': round(total_marks, 2),
        'max_marks': 60,
        'branch_code': branch_code
    }


def get_book_review_marks(student_username, academic_year=None):
    """Get book review marks for a student (out of 30)."""
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT marks, review_count, remarks, student_trno, source 
            FROM book_review_marks
            WHERE student_username = ? AND academic_year = ?
        """, (student_username, academic_year))
        
        result = cur.fetchone()
        
        if result:
            return {
                'marks': result[0], 
                'review_count': result[1],
                'remarks': result[2],
                'trno': result[3],
                'source': result[4]
            }
        else:
            return {
                'marks': 0.0, 
                'review_count': 0, 
                'remarks': None, 
                'trno': None,
                'source': None
            }
    finally:
        if conn:
            conn.close()


def get_book_review_marks_by_trno(trno, academic_year=None):
    """Get book review marks for a student by Trno (out of 30)."""
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT marks, review_count, remarks, student_username, student_name, source
            FROM book_review_marks
            WHERE student_trno = ? AND academic_year = ?
        """, (trno, academic_year))
        
        result = cur.fetchone()
        
        if result:
            return {
                'marks': result[0], 
                'review_count': result[1],
                'remarks': result[2],
                'username': result[3],
                'name': result[4],
                'source': result[5]
            }
        else:
            return {
                'marks': 0.0, 
                'review_count': 0, 
                'remarks': None, 
                'username': None, 
                'name': None,
                'source': None
            }
    finally:
        if conn:
            conn.close()


def get_program_attendance_marks(student_username, academic_year=None):
    """Calculate library program attendance marks."""
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT SUM(marks) FROM library_program_attendance
            WHERE student_username = ? AND academic_year = ? AND attended = 1
        """, (student_username, academic_year))
        
        result = cur.fetchone()
        
        total_marks = result[0] if result and result[0] else 0.0
        return round(total_marks, 2)
    finally:
        if conn:
            conn.close()


def get_student_program_participation(student_username, academic_year=None):
    """Get a detailed list of programs a student attended."""
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                p.title, 
                p.date, 
                pa.marks,
                p.marks_category,
                p.venue
            FROM library_program_attendance pa
            JOIN library_programs p ON pa.program_id = p.id
            WHERE pa.student_username = ? AND pa.academic_year = ? AND pa.attended = 1
            ORDER BY p.date DESC
        """, (student_username, academic_year))
        
        rows = cur.fetchall()
        result = []
        for row in rows:
            result.append({
                'title': row[0],
                'date': row[1],
                'marks': row[2],
                'category': row[3],
                'venue': row[4]
            })
        return result
    except Exception as e:
        logger.error(f"Error fetching program participation for {student_username}: {e}")
        return []
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def calculate_total_taqeem(student_username, academic_year=None):
    """Calculate and return the total Taqeem (marks) for a student."""
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        total_result = {
            'book_issue': {'total': 0.0, 'physical_count': 0, 'digital_count': 0, 'max_marks': 60.0, 'physical_marks': 0.0, 'digital_marks': 0.0},
            'book_review': {'marks': 0.0, 'max_marks': 30.0},
            'program_attendance': 0.0,
            'total': 0.0,
            'details': []
        }
        
        # Get book issue marks
        book_issue_data = calculate_book_issue_marks(student_username, academic_year)
        total_result['book_issue'] = book_issue_data
        
        # Get book review marks
        book_review_data = get_book_review_marks(student_username, academic_year)
        total_result['book_review']['marks'] = book_review_data.get('marks', 0.0)
        
        # Get program attendance marks
        total_result['program_attendance'] = get_program_attendance_marks(student_username, academic_year)
        
        # Calculate total
        running_total = book_issue_data['total'] + book_review_data.get('marks', 0.0) + total_result['program_attendance']
        total_result['total'] = round(min(running_total, 100.0), 2)
        
        return total_result
    finally:
        if conn:
            conn.close()


def process_program_marks_upload(file, program_id, academic_year=None, uploaded_by='admin'):
    """Process manual marks upload for a specific program."""
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    if not file:
        return {'success': False, 'error': 'No file uploaded'}
    
    try:
        if file.filename.endswith('.csv'):
            df = pd.read_csv(io.StringIO(file.stream.read().decode("UTF8")))
        else:
            df = pd.read_excel(BytesIO(file.stream.read()))
            
        df.columns = [str(c).strip().lower() for c in df.columns]
        
        trno_col = next((c for c in df.columns if any(x in c for x in ['its', 'trno', 'student', 'id'])), None)
        marks_col = next((c for c in df.columns if any(x in c for x in ['marks', 'score', 'obtained'])), None)
        
        if not trno_col or not marks_col:
            return {'success': False, 'error': f"CSV/Excel must have 'ITS' and 'Marks' columns. Found: {list(df.columns)}"}
        
        conn = None
        try:
            conn = get_conn()
            cur = conn.cursor()
            
            cur.execute("SELECT marks, title FROM library_programs WHERE id = ?", (program_id,))
            prog = cur.fetchone()
            if not prog:
                return {'success': False, 'error': 'Program not found'}
            
            max_marks, program_title = prog
            
            stats = {'total': 0, 'success': 0, 'errors': 0}
            processed_students = []
            
            for _, row in df.iterrows():
                stats['total'] += 1
                try:
                    trno = str(row[trno_col]).strip()
                    marks = float(row[marks_col])
                    
                    if marks > max_marks:
                        marks = max_marks
                    
                    patron = find_student_by_identifier(trno)
                    
                    if patron:
                        username = patron.get('userid') or patron.get('cardnumber') or trno
                        name = f"{patron.get('firstname', '')} {patron.get('surname', '')}".strip()
                        darajah = patron.get('darajah')
                    else:
                        username = trno
                        name = f"Ghost Student ({trno})"
                        darajah = "Unknown (Manual Upload)"
                    
                    # Branch aware
                    branch_code = patron.get('branchcode', 'AJSN')
                    branch_info = Config.CAMPUS_REGISTRY.get(branch_code, {})
                    campus_name = branch_info.get("short_name", "Global")

                    cur.execute("""
                        INSERT OR REPLACE INTO library_program_attendance 
                        (student_username, student_name, darajah_name, academic_year, program_name, 
                         program_id, marks, attended, uploaded_by, campus_branch, branch_code)
                        VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
                    """, (username, name, darajah, academic_year, program_title, program_id, marks, uploaded_by, campus_name, branch_code))
                    
                    stats['success'] += 1
                    processed_students.append(username)
                except Exception as e:
                    stats['errors'] += 1
                    logger.error(f"Row upload error: {e}")
                    
            conn.commit()
            
            for u in set(processed_students):
                update_student_taqeem(u, academic_year)
                
            return {
                'success': True, 
                'message': f"Successfully uploaded marks for {stats['success']} students to '{program_title}'.",
                'stats': stats
            }
        finally:
            if conn:
                conn.close()
    except Exception as e:
        logger.error(f"Process program marks upload error: {e}")
        return {'success': False, 'error': str(e)}


def update_student_taqeem(student_username, academic_year=None):
    """Calculate and update the student_taqeem table with current marks."""
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    conn_app = None
    try:
        conn_app = get_conn()
        cur_app = conn_app.cursor()
        
        # First, ensure the student exists in the users table
        # Get student info - use a FRESH connection each time
        patron = None
        username = None
        trno = None
        student_name = None
        darajah_name = None
        
        try:
            from db_koha import get_koha_conn
            koha_conn = get_koha_conn()
            koha_cur = koha_conn.cursor(dictionary=True)
            koha_cur.execute("""
                SELECT 
                    b.borrowernumber,
                    b.cardnumber,
                    b.userid,
                    b.surname,
                    b.firstname,
                    b.email,
                    b.branchcode as koha_branch,
                    COALESCE(std.attribute, b.branchcode) AS darajah,
                    tr.attribute AS trno
                FROM borrowers b
                LEFT JOIN borrower_attributes std
                    ON std.borrowernumber = b.borrowernumber
                    AND std.code IN ('STD', 'CLASS', 'DAR', 'CLASS_STD')
                LEFT JOIN borrower_attributes tr
                    ON tr.borrowernumber = b.borrowernumber
                    AND tr.code IN ('TRNO', 'TRN', 'TR_NUMBER', 'TR')
                WHERE b.cardnumber = %s 
                   OR b.userid = %s 
                   OR tr.attribute = %s
                LIMIT 1
            """, (student_username, student_username, student_username))
            
            patron = koha_cur.fetchone()
            koha_cur.close()
            koha_conn.close()
            
        except Exception as e:
            logger.error(f"Error fetching student from Koha: {e}")
            patron = None

        if patron:
            username = patron.get('userid') or patron.get('cardnumber') or student_username
            trno = patron.get('trno') or student_username
            student_name = f"{patron.get('firstname', '')} {patron.get('surname', '')}".strip()
            darajah_name = patron.get('darajah')
            student_email = patron.get('email') or f"{username}@placeholder.local"
            koha_branch = patron.get('koha_branch') or _session.get("branch_code", "AJSN")
            
            # Ensure the student exists in users table
            cur_app.execute("""
                SELECT COUNT(*) FROM users WHERE username = ? OR trno = ?
            """, (username, trno))
            
            if cur_app.fetchone()[0] == 0:
                placeholder_campus = Config.CAMPUS_REGISTRY.get(koha_branch, {}).get("short_name", "Global")
                
                cur_app.execute("""
                    INSERT INTO users (username, email, role, teacher_name, darajah_name, trno, class_name, campus_branch, branch_code)
                    VALUES (?, ?, 'student', ?, ?, ?, ?, ?, ?)
                """, (username, student_email, student_name, darajah_name, trno, darajah_name, placeholder_campus, koha_branch))
                logger.info(f"Created placeholder user record for {username} (TR: {trno})")
        else:
            # Check local users as fallback
            cur_app.execute("""
                SELECT u.username, u.darajah_name, u.trno, u.teacher_name
                FROM users u
                WHERE u.username = ? OR u.trno = ?
            """, (student_username, student_username))
            user_row = cur_app.fetchone()
            
            if user_row:
                username, darajah_name, trno, student_name = user_row
            else:
                # Create a new user record for this student
                username = student_username
                trno = student_username
                student_name = f"Student {student_username}"
                darajah_name = "Unknown"
                
                # Insert the student into users table
                from flask import session as _session
                new_user_branch = _session.get("branch_code", "AJSN")
                new_user_campus = Config.CAMPUS_REGISTRY.get(new_user_branch, {}).get("short_name", "Global")

                cur_app.execute("""
                    INSERT INTO users (username, email, role, teacher_name, darajah_name, trno, class_name, campus_branch, branch_code)
                    VALUES (?, ?, 'student', ?, ?, ?, ?, ?, ?)
                """, (username, f"{username}@placeholder.local", student_name, darajah_name, trno, darajah_name, new_user_campus, new_user_branch))
                logger.info(f"Created new user record for {username} (TR: {trno})")
        
        # If we still don't have a username, something went wrong
        if not username:
            logger.error(f"Could not find or create user for {student_username}")
            return False
        
        # Calculate all marks
        taqeem = calculate_total_taqeem(student_username, academic_year)
        
        # Extract book issue data safely
        book_issue = taqeem.get('book_issue', {})
        
        # Branch and Campus for Taqeem
        branch_code = book_issue.get('branch_code', 'AJSN')
        branch_info = Config.CAMPUS_REGISTRY.get(branch_code, {})
        campus_name = branch_info.get("short_name", "Global")

        # Update or insert into student_taqeem
        cur_app.execute("""
            INSERT OR REPLACE INTO student_taqeem (
                student_username, student_trno, student_name, darajah_name, academic_year,
                physical_books_issued, digital_books_issued,
                physical_books_marks, digital_books_marks, book_issue_total,
                book_review_marks, program_attendance_marks, total_marks,
                campus_branch, branch_code, last_updated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            username, 
            trno, 
            student_name, 
            darajah_name, 
            academic_year,
            book_issue.get('physical_count', 0),
            book_issue.get('digital_count', 0),
            book_issue.get('physical_marks', 0.0),
            book_issue.get('digital_marks', 0.0),
            book_issue.get('total', 0.0),
            taqeem.get('book_review', {}).get('marks', 0.0),
            taqeem.get('program_attendance', 0.0),
            taqeem.get('total', 0.0),
            campus_name,
            branch_code
        ))
        
        conn_app.commit()
        return True
    except Exception as e:
        logger.error(f"Error updating Taqeem for {student_username}: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if conn_app:
            conn_app.close()


def update_all_student_taqeem(academic_year=None):
    """Update Taqeem for all students."""
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    unique_usernames = set()
    
    # 1. Fetch from local app DB
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT student_username FROM (
                SELECT username as student_username FROM users WHERE role = 'student'
                UNION
                SELECT student_username FROM library_program_attendance
                UNION
                SELECT student_username FROM book_review_marks
                UNION
                SELECT student_username FROM student_darajah_mapping
            )
        """)
        for row in cur.fetchall():
            if row[0]:
                unique_usernames.add(row[0])
    except Exception as e:
        logger.error(f"Error fetching students from local DB: {e}")
    finally:
        if conn:
            conn.close()

    # 2. Add all students from Koha DB
    try:
        from db_koha import get_koha_conn
        koha_conn = get_koha_conn()
        if koha_conn:
            koha_cur = koha_conn.cursor(dictionary=True)
            koha_cur.execute("""
                SELECT b.userid, b.cardnumber, tr.attribute as trno
                FROM borrowers b
                LEFT JOIN borrower_attributes tr 
                    ON tr.borrowernumber = b.borrowernumber AND tr.code IN ('TRNO', 'TRN', 'TR_NUMBER', 'TR')
                LEFT JOIN borrower_attributes std
                    ON std.borrowernumber = b.borrowernumber AND std.code IN ('STD', 'CLASS', 'DAR', 'CLASS_STD')
                WHERE 
                    (tr.attribute IS NOT NULL OR std.attribute IS NOT NULL) OR 
                    categorycode IN ('STD','STUDENT','PT','J1','J2','J3','J4','J5','J6','J7','J8','S1','S2','S3','S4')
            """)
            for patron in koha_cur.fetchall():
                uname = patron.get('userid') or patron.get('cardnumber') or patron.get('trno')
                if uname:
                    unique_usernames.add(uname)
            koha_cur.close()
            koha_conn.close()
    except Exception as e:
        logger.error(f"Error fetching students from Koha DB: {e}")
        
    success_count = 0
    total = len(unique_usernames)
    logger.info(f"Recalculating Taqeem for {total} total students...")
    
    for username in unique_usernames:
        if username and update_student_taqeem(username, academic_year):
            success_count += 1
            
    logger.info(f"Updated Taqeem for {success_count} students")
    return success_count


def get_book_review_stats(academic_year=None):
    """Get statistics about book review marks."""
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                COUNT(*) as total_students,
                AVG(marks) as average_marks,
                MIN(marks) as min_marks,
                MAX(marks) as max_marks,
                SUM(CASE WHEN marks >= 15 THEN 1 ELSE 0 END) as passed_count,
                SUM(CASE WHEN marks < 15 THEN 1 ELSE 0 END) as failed_count
            FROM book_review_marks
            WHERE academic_year = ?
        """, (academic_year,))
        
        stats = cur.fetchone()
        
        if stats:
            return {
                'total_students': stats[0],
                'average_marks': round(stats[1] or 0, 2),
                'min_marks': stats[2] or 0,
                'max_marks': stats[3] or 0,
                'passed_count': stats[4] or 0,
                'failed_count': stats[5] or 0
            }
        else:
            return {
                'total_students': 0,
                'average_marks': 0,
                'min_marks': 0,
                'max_marks': 0,
                'passed_count': 0,
                'failed_count': 0
            }
    finally:
        if conn:
            conn.close()


def export_book_review_marks(academic_year=None, file_format='csv'):
    """Export book review marks to CSV or Excel format."""
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                student_trno as Trno,
                student_name as FullName,
                darajah_name as ClassName,
                academic_year as AcademicYear,
                marks as Marks,
                review_count as ReviewCount,
                remarks as Remarks,
                source as Source,
                uploaded_by as UploadedBy,
                uploaded_at as LastUpdated
            FROM book_review_marks
            WHERE academic_year = ?
            ORDER BY darajah_name, student_name
        """, (academic_year,))
        
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    finally:
        if conn:
            conn.close()
    
    if file_format.lower() == 'excel':
        df = pd.DataFrame(rows, columns=columns)
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Book Review Marks', index=False)
        output.seek(0)
        return output, 'book_review_marks.xlsx'
    else:
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(columns)
        writer.writerows(rows)
        csv_content = output.getvalue()
        output.close()
        return csv_content, 'book_review_marks.csv'


def save_taqeem_to_db(student_username, student_trno, academic_year, book_issue_marks, 
                     book_review_marks, program_attendance_marks, total_marks):
    """Save calculated Taqeem to the database."""
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT darajah_name, teacher_name FROM users WHERE username = ?
        """, (student_username,))
        
        student_info = cur.fetchone()
        if not student_info:
            return False
            
        darajah_name, student_name = student_info
        if not student_name:
            student_name = student_username
        
        cur.execute("""
            INSERT OR REPLACE INTO student_taqeem (
                student_username, student_trno, student_name, darajah_name, academic_year,
                physical_books_issued, digital_books_issued,
                physical_books_marks, digital_books_marks, book_issue_total,
                book_review_marks, program_attendance_marks, total_marks,
                last_updated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            student_username, student_trno, student_name, darajah_name, academic_year,
            book_issue_marks.get('physical_count', 0),
            book_issue_marks.get('digital_count', 0),
            book_issue_marks.get('physical_marks', 0),
            book_issue_marks.get('digital_marks', 0),
            book_issue_marks.get('total', 0),
            book_review_marks,
            program_attendance_marks,
            total_marks
        ))
        
        conn.commit()
        return True
        
    except Exception as e:
        logger.error(f"Error saving Taqeem to DB for {student_username}: {e}")
        return False
    finally:
        if conn:
            conn.close()


def get_review_count(student_username, academic_year=None):
    """Get the number of reviews submitted by a student."""
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT review_count FROM book_review_marks
            WHERE student_username = ? AND academic_year = ?
        """, (student_username, academic_year))
        
        result = cur.fetchone()
        
        return result[0] if result and result[0] else 0
    finally:
        if conn:
            conn.close()


def calculate_book_review_marks(student_username, academic_year=None):
    """Calculate book review marks (out of 30)."""
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    book_review = get_book_review_marks(student_username, academic_year)
    return book_review['marks']


def calculate_program_attendance_marks(student_username, academic_year=None):
    """Calculate program attendance marks (out of 10)."""
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    return get_program_attendance_marks(student_username, academic_year)
def get_book_review_template(darajah_name=None, academic_year=None):
    """
    Generate an Excel template for book review marks.
    Prefills ALL enrolled students in the darajah (from Koha), not just
    those who already have a taqeem record, so no student is missed.
    Headers: Trno, Student Name, Darajah, Book Review Name, Grade, Marks, Hijri Month, Remarks, Academic Year
    """
    import pandas as pd
    import io
    
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    headers = ['Trno', 'Student Name', 'Darajah', 'Book Review Name', 'Grade', 'Marks', 'Hijri Month', 'Remarks', 'Academic Year']
    data = []
    
    if darajah_name and darajah_name != 'All':
        # ── PRIMARY SOURCE: Koha borrowers (catches ALL enrolled students) ──────
        koha_students = []
        try:
            from db_koha import get_koha_conn
            koha_conn = get_koha_conn()
            koha_cur = koha_conn.cursor(dictionary=True)
            koha_cur.execute("""
                SELECT DISTINCT
                    trno.attribute AS trno,
                    TRIM(CONCAT(
                        COALESCE(b.surname,  ''),
                        CASE WHEN b.surname IS NOT NULL AND b.firstname IS NOT NULL
                             THEN ' ' ELSE '' END,
                        COALESCE(b.firstname, '')
                    )) AS full_name
                FROM borrowers b
                JOIN borrower_attributes cls
                    ON cls.borrowernumber = b.borrowernumber
                   AND cls.code IN ('Class', 'DAR', 'CLASS', 'CLASS_STD', 'STD')
                   AND cls.attribute = %s
                JOIN borrower_attributes trno
                    ON trno.borrowernumber = b.borrowernumber
                   AND trno.code = 'TRNO'
                WHERE (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
                ORDER BY full_name
            """, (darajah_name,))
            koha_students = koha_cur.fetchall()
            koha_cur.close()
            koha_conn.close()
            logger.info(f"Template: fetched {len(koha_students)} students from Koha for darajah '{darajah_name}'")
        except Exception as e:
            logger.warning(f"Koha unavailable for template generation; falling back to student_taqeem. Error: {e}")

        if koha_students:
            for st in koha_students:
                trno = str(st.get('trno') or '').strip()
                if not trno:
                    continue
                full_name = (st.get('full_name') or f'Student ({trno})').strip()
                data.append({
                    'Trno': trno,
                    'Student Name': full_name,
                    'Darajah': darajah_name,
                    'Book Review Name': '',
                    'Grade': '',
                    'Marks': '',
                    'Hijri Month': '',
                    'Remarks': '',
                    'Academic Year': academic_year
                })
        else:
            # ── FALLBACK: local student_taqeem table ────────────────────────────
            conn = None
            try:
                conn = get_conn()
                cur = conn.cursor()
                cur.execute("""
                    SELECT student_trno, student_name, darajah_name
                    FROM student_taqeem
                    WHERE darajah_name = ?
                    ORDER BY student_name
                """, (darajah_name,))
                for student in cur.fetchall():
                    data.append({
                        'Trno': student[0],
                        'Student Name': student[1],
                        'Darajah': student[2] or darajah_name,
                        'Book Review Name': '',
                        'Grade': '',
                        'Marks': '',
                        'Hijri Month': '',
                        'Remarks': '',
                        'Academic Year': academic_year
                    })
            except Exception as e:
                logger.error(f"Error pre-populating template from student_taqeem: {e}")
            finally:
                if conn:
                    conn.close()
                
    if not data:
        # Provide at least one empty row example if no students found or 'All'
        data.append({k: '' for k in headers})
        data[0]['Academic Year'] = academic_year
        data[0]['Darajah'] = darajah_name if darajah_name != 'All' else '2 B M'
        data[0]['Trno'] = 'EX12345'
        data[0]['Student Name'] = 'Example Student'
        
    df = pd.DataFrame(data, columns=headers)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Template')
        
        # Auto-adjust columns width
        worksheet = writer.sheets['Template']
        for i, col in enumerate(df.columns):
            max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
            worksheet.set_column(i, i, max_len)
            
    output.seek(0)
    return output.read()


def delete_book_review(review_name, darajah_name=None, academic_year=None):
    """Delete book review marks by review name and darajah."""
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        query = "DELETE FROM book_review_marks WHERE book_review_name = ?"
        params = [review_name]
        
        if darajah_name and darajah_name != 'All':
            query += " AND darajah_name = ?"
            params.append(darajah_name)
            
        if academic_year:
            query += " AND academic_year = ?"
            params.append(str(academic_year).replace('H', '').strip())
            
        cur.execute(query, params)
        count = cur.rowcount
        conn.commit()
        
        # After deletion, we might want to recalculate taqeem for affected students
        # For simplicity, we trigger a global taqeem update or let it be manual
        
        return count
    except Exception as e:
        logger.error(f"Error deleting book reviews: {e}")
        return 0
    finally:
        if conn:
            conn.close()


def get_all_book_review_sessions():
    """Get a list of unique review names, darajahs and months uploaded."""
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT book_review_name, darajah_name, hijri_month, academic_year, COUNT(*), MAX(uploaded_at)
            FROM book_review_marks
            GROUP BY book_review_name, darajah_name, academic_year
            ORDER BY MAX(uploaded_at) DESC
        """)
        return [{
            'name': r[0],
            'darajah': r[1],
            'month': r[2],
            'academic_year': r[3],
            'count': r[4],
            'uploaded_at': r[5]
        } for r in cur.fetchall()]
    except Exception as e:
        logger.error(f"Error listing review sessions: {e}")
        return []
    finally:
        if conn:
            conn.close()