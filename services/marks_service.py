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
    """Return the current academic year from config."""
    return Config.CURRENT_ACADEMIC_YEAR()


def process_book_review_upload(file, file_type='csv', academic_year=None):
    """
    Process uploaded file containing book review marks (CSV or Excel).
    
    File format requirements:
    - CSV: Required columns: Trno, Marks
    - Excel: Required columns: Trno, Marks (or Trno, Total, Percent for conversion)
    
    Returns: dict with stats and errors
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    if not file:
        return {'success': False, 'error': 'No file uploaded'}
    
    try:
        if file_type.lower() == 'csv':
            return process_csv_file(file, academic_year)
        elif file_type.lower() in ['xlsx', 'xls', 'excel']:
            return process_excel_file(file, academic_year)
        else:
            return {'success': False, 'error': f'Unsupported file type: {file_type}'}
    except Exception as e:
        logger.error(f"Error processing {file_type} file: {e}")
        return {'success': False, 'error': f'Error processing file: {str(e)}'}


def process_csv_file(csv_file, academic_year=None):
    """
    Process CSV file containing book review marks.
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    try:
        # Read CSV content
        if hasattr(csv_file, 'read'):
            content = csv_file.read()
            # Handle bytes if needed
            if isinstance(content, bytes):
                content = content.decode('utf-8')
        else:
            content = csv_file
        
        # Parse CSV
        csv_reader = csv.DictReader(io.StringIO(content))
        
        # Validate required columns
        required_columns = ['Trno', 'Marks']
        optional_columns = ['Name', 'FullName', 'ClassName', 'Remarks', 'AcademicYear']
        
        # Check for required columns
        fieldnames = csv_reader.fieldnames
        if not fieldnames:
            return {'success': False, 'error': 'CSV file has no headers'}
        
        missing_columns = [col for col in required_columns if col not in fieldnames]
        if missing_columns:
            return {'success': False, 'error': f'Missing required columns: {missing_columns}'}
        
        # Process rows
        return process_book_review_rows(csv_reader, academic_year, source='csv')
        
    except Exception as e:
        logger.error(f"Error processing CSV file: {e}")
        return {'success': False, 'error': f'Error processing CSV file: {str(e)}'}


def process_excel_file(excel_file, academic_year=None):
    """
    Process Excel file containing book review marks.
    Supports both direct marks and conversion from Total/Percent.
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    try:
        # Read Excel file
        if hasattr(excel_file, 'read'):
            # File-like object
            excel_data = pd.read_excel(BytesIO(excel_file.read()), sheet_name=None, header=None)
        else:
            # File path
            excel_data = pd.read_excel(excel_file, sheet_name=None, header=None)
        
        processed_results = {
            'success': True,
            'total_processed': 0,
            'total_errors': 0,
            'sheet_results': [],
            'error_messages': []
        }
        
        # Process each sheet
        for sheet_name, df in excel_data.items():
            logger.info(f"Processing sheet: {sheet_name}")
            
            # Convert DataFrame to list of dictionaries
            df = df.where(pd.notnull(df), None)  # Replace NaN with None
            rows = df.to_dict('records')
            
            # Process this sheet
            sheet_result = process_excel_sheet(rows, sheet_name, academic_year)
            
            if not sheet_result:
                logger.warning(f"Process sheet '{sheet_name}' returned None")
                continue

            # Aggregate results
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


def process_excel_sheet(rows, sheet_name, academic_year=None):
    """
    Process rows from an Excel sheet.
    Handles dynamic header detection by scanning for 'Trno'.
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    conn = get_conn()
    try:
        cur = conn.cursor()
        
        success_count = 0
        error_count = 0
        errors = []
        processed_students = []
        
        # 1. Detect Header Row
        header_row_idx = -1
        col_map = {}  # index -> normalized_name
        
        # Scan first 10 rows for a header
        for i, row in enumerate(rows[:10]):
            # Check values in this row
            row_values = [str(v).strip().lower().replace(' ', '').replace('_', '').replace('.', '') for k, v in row.items() if v is not None]
            
            # Check if 'trno' matches any value
            if any(x in ['trno', 'studentid', 'id', 'studenttrno'] for x in row_values):
                header_row_idx = i
                # Build column map
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

        # 2. Process Data Rows
        for row_num, row in enumerate(rows[header_row_idx+1:], start=header_row_idx+2):
            try:
                # Build normalized_row using col_map
                normalized_row = {}
                for col_idx, col_name in col_map.items():
                    if col_idx in row:
                        normalized_row[col_name] = row[col_idx]
                
                # Extract data with flexible column names
                trno = extract_column_value(normalized_row, [
                    'trno', 'studentid', 'id', 'studenttrno', 'its', 'itsid', 'ejamaatid', 'regno'
                ])
                full_name = extract_column_value(normalized_row, [
                    'fullname', 'name', 'studentname', 'full_name', 'engname', 'displayname'
                ])
                marks_str = extract_column_value(normalized_row, [
                    'marks', 'mark', 'score', 'obtained', 'marksobtained', 'taqeem'
                ])
                total_str = extract_column_value(normalized_row, ['total', 'totalscore', 'sum', 'max', 'outof'])
                percent_str = extract_column_value(normalized_row, ['percent', 'percentage', '％'])
                class_name = extract_column_value(normalized_row, ['classname', 'class', 'division', 'div', 'darajah'])
                remarks = extract_column_value(normalized_row, ['remarks', 'note', 'comment', 'review', 'feedback'])
                
                # Additional cleanup for class_name if missing (use sheet name)
                if not class_name and sheet_name:
                    class_name = sheet_name

                # Validate required fields
                if not trno:
                    # Silently skip empty rows (common in Excel)
                    continue
                
                # Clean and format trno - Robust handling
                # Convert to string and strip whitespace
                if isinstance(trno, (int, float)):
                    trno = str(int(trno)).strip()
                else:
                    trno = str(trno).strip()
                
                # Skip if Trno is invalid after cleanup
                if not trno or trno.lower() in ['none', 'nan', 'null', '']:
                    continue
                
                # Calculate marks
                marks = 0
                
                # Case 1: Direct marks provided
                if marks_str is not None and marks_str != '' and marks_str != 'N/A':
                    try:
                        if isinstance(marks_str, (int, float)):
                            marks = float(marks_str)
                        else:
                            marks = float(str(marks_str).strip())
                    except (ValueError, TypeError):
                        # Try other methods
                        pass
                
                # Case 2: Calculate from Total (assuming out of 40)
                if marks == 0 and total_str is not None and total_str != '' and total_str != 'N/A':
                    try:
                        if isinstance(total_str, (int, float)):
                            total_value = float(total_str)
                        else:
                            total_value = float(str(total_str).strip())
                        
                        if total_value <= 40:  # Safety check
                            marks = (total_value / 40) * 30
                    except (ValueError, TypeError):
                        # Try next method
                        pass
                
                # Case 3: Calculate from Percent
                if marks == 0 and percent_str is not None and percent_str != '' and percent_str != 'N/A':
                    try:
                        if isinstance(percent_str, (int, float)):
                            percent_value = float(percent_str)
                        else:
                            percent_str_clean = str(percent_str).strip().replace('%', '').replace('％', '')
                            percent_value = float(percent_str_clean)
                        
                        marks = (percent_value / 100) * 30
                    except (ValueError, TypeError):
                        # Cannot calculate marks
                        errors.append(f"Row {row_num}: Cannot calculate marks for Trno {trno}")
                        error_count += 1
                        continue
                
                # If still no marks, skip this row
                if marks == 0:
                    errors.append(f"Row {row_num}: No valid marks found for Trno {trno}")
                    error_count += 1
                    continue
                
                # Cap marks at 30
                marks = min(marks, 30)
                
                # Find student by Trno
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
                
                # Insert or update book review marks
                cur.execute("""
                    INSERT OR REPLACE INTO book_review_marks (
                        student_username, student_trno, student_name, darajah_name, 
                        academic_year, marks, review_count, remarks, source, uploaded_by, uploaded_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
                """, (
                    username,
                    trno,
                    full_name if full_name else None,
                    class_name if class_name else None,
                    academic_year,
                    marks,
                    remarks if remarks else None,
                    'excel_import',
                    'admin',
                    datetime.now()
                ))
                
                success_count += 1
                processed_students.append(username)
                
            except Exception as e:
                errors.append(f"Row {row_num}: Error processing Trno {row.get('Trno', 'Unknown')}: {str(e)}")
                error_count += 1
                continue
        
        conn.commit()
        
        # Update Taqeem for processed students
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
        conn.close()


def extract_column_value(row_dict, possible_keys):
    """
    Extract value from row dictionary using possible key names.
    """
    for key in possible_keys:
        if key in row_dict:
            value = row_dict[key]
            if value is not None and value != '':
                return value
    return None


def process_book_review_rows(rows, academic_year=None, source='csv'):
    """
    Process book review marks from rows (common function for CSV and Excel).
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    conn = get_conn()
    try:
        cur = conn.cursor()
        
        success_count = 0
        error_count = 0
        errors = []
        processed_students = []
        
        for row_num, row in enumerate(rows, start=2):  # Start from 2 (header is row 1)
            try:
                # Use robust column extraction
                trno = extract_column_value(row, [
                    'Trno', 'trno', 'studentid', 'id', 'studenttrno', 'its', 'itsid', 'ejamaatid', 'regno'
                ])
                marks_str = extract_column_value(row, [
                    'Marks', 'marks', 'mark', 'score', 'obtained', 'marksobtained', 'taqeem'
                ])
                
                # Try to get name from various possible columns
                student_name = extract_column_value(row, [
                    'FullName', 'Name', 'fullname', 'name', 'studentname', 'full_name', 'engname', 'displayname'
                ])
                
                darajah_name = extract_column_value(row, [
                    'ClassName', 'classname', 'class', 'division', 'div', 'darajah'
                ])
                remarks = extract_column_value(row, [
                    'Remarks', 'remarks', 'note', 'comment', 'review', 'feedback'
                ])
                row_academic_year = extract_column_value(row, ['AcademicYear', 'academicyear']) or academic_year
                
                # Validate required fields
                if not trno:
                    errors.append(f"Row {row_num}: Missing Trno")
                    error_count += 1
                    continue
                
                # Clean and format trno - Robust handling
                if isinstance(trno, (int, float)):
                    trno = str(int(trno)).strip()
                else:
                    trno = str(trno).strip()

                if not marks_str:
                    errors.append(f"Row {row_num}: Missing Marks for Trno {trno}")
                    error_count += 1
                    continue
                
                # Parse marks
                try:
                    marks = float(marks_str)
                    if marks < 0 or marks > 100:
                        errors.append(f"Row {row_num}: Marks must be between 0 and 100 for Trno {trno}")
                        error_count += 1
                        continue
                except ValueError:
                    errors.append(f"Row {row_num}: Invalid marks value '{marks_str}' for Trno {trno}")
                    error_count += 1
                    continue
                
                # Try to find student by Trno in users table
                username_from_trno = f"TR{trno}"
                
                # Search for student with this Trno
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
                    # Student not found, use Trno as username
                    username = trno
                    logger.warning(f"Student with Trno {trno} not found in users table, using Trno as username")
                
                # Insert or update book review marks
                cur.execute("""
                    INSERT OR REPLACE INTO book_review_marks (
                        student_username, student_trno, student_name, darajah_name, 
                        academic_year, marks, review_count, remarks, source, uploaded_by, uploaded_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
                """, (
                    username,
                    trno,
                    student_name if student_name else None,
                    darajah_name if darajah_name else None,
                    row_academic_year,
                    marks,
                    remarks if remarks else None,
                    source,
                    'admin',
                    datetime.now()
                ))
                
                success_count += 1
                processed_students.append(username)
                
            except Exception as e:
                errors.append(f"Row {row_num}: Error processing Trno {row.get('Trno', 'Unknown')}: {str(e)}")
                error_count += 1
                continue
        
        conn.commit()
        
        # Update Taqeem for processed students
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
        conn.close()


def get_student_level_weights(darajah_name, academic_year=None):
    """
    Get the physical and digital book weights for a student's class level.
    
    Returns: (physical_weight, digital_weight) as percentages out of 60 total marks
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    conn = get_conn()
    try:
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
            # Default weights if not found (Class 1-7 pattern)
            logger.warning(f"No level weights found for {darajah_name}, using defaults")
            return 40.0, 20.0
    finally:
        conn.close()


def calculate_book_issue_marks(student_username, academic_year=None):
    """
    Calculate marks for book issues (physical + digital).
    
    Now uses dynamic weight from library_programs table for 'Books Issued'.
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
            'max_marks': 0.0
        }
    
    darajah_name = patron.get('darajah')
    borrower_number = patron.get('borrowernumber')
    
    conn_app = get_conn()
    try:
        cur_app = conn_app.cursor()
        
        # Get the allotted marks for "Books Issued" from library_programs
        # Using a more robust query for various naming conventions
        cur_app.execute("""
            SELECT marks FROM library_programs 
            WHERE (title LIKE '%Books Issued%' OR title LIKE '%Book Issue%')
            AND academic_year = ?
            ORDER BY id ASC LIMIT 1
        """, (academic_year,))
        program_result = cur_app.fetchone()
        
        # Default to 60 if not found (historical default)
        books_issued_max_marks = float(program_result['marks']) if program_result and program_result['marks'] is not None else 60.0
        
        # Get class-specific weights (split)
        # Note: student_levels splits 60 marks. We scale this to books_issued_max_marks.
        cur_app.execute("""
            SELECT physical_books_weight, digital_books_weight
            FROM student_levels
            WHERE darajah_name = ? AND academic_year = ?
        """, (darajah_name, academic_year))
        
        level_split = cur_app.fetchone()
        if level_split:
            raw_phys = level_split['physical_books_weight']
            raw_dig = level_split['digital_books_weight']
        else:
            raw_phys, raw_dig = None, None
    except Exception as e:
        logger.error(f"Error fetching marks allotment: {e}")
        books_issued_max_marks = 60.0
        level_split = None
    finally:
        conn_app.close()
    
    if raw_phys is not None and raw_dig is not None:
        # Scale the split to the new max marks
        physical_weight = (raw_phys / 60.0) * books_issued_max_marks
        digital_weight = (raw_dig / 60.0) * books_issued_max_marks
    else:
        # Default 40/20 split scaled to books_issued_max_marks
        physical_weight = (40.0 / 60.0) * books_issued_max_marks
        digital_weight = (20.0 / 60.0) * books_issued_max_marks
    
    # Calculate target books based on weights
    # Total target is 100 books for the year
    total_target = 100
    physical_target = (physical_weight / books_issued_max_marks) * total_target if books_issued_max_marks > 0 else 0
    digital_target = (digital_weight / books_issued_max_marks) * total_target if books_issued_max_marks > 0 else 0
    
    physical_count = 0
    digital_count = 0
    
    if borrower_number:
        try:
            # Get Academic Year bounds (April 1st to Dec 31st/Today)
            start_ay, end_ay = get_ay_bounds()
            
            # Query Koha database for book issues using statistics table for historical accuracy
            conn_koha = get_koha_conn()
            try:
                cur_koha = conn_koha.cursor()
                
                # Get physical books issued (itype != 'DIGITAL') in the academic year
                cur_koha.execute("""
                    SELECT COUNT(DISTINCT it.itemnumber) 
                    FROM statistics s
                    JOIN items it ON s.itemnumber = it.itemnumber
                    WHERE s.borrowernumber = %s
                    AND s.type = 'issue'
                    AND DATE(s.datetime) BETWEEN %s AND %s
                    AND (it.itype != 'DIGITAL' OR it.itype IS NULL)
                """, (borrower_number, start_ay, end_ay))
                
                res = cur_koha.fetchone()
                physical_count = res[0] if res else 0
                
                # Get digital books issued (itype = 'DIGITAL') in the academic year
                cur_koha.execute("""
                    SELECT COUNT(DISTINCT it.itemnumber)
                    FROM statistics s
                    JOIN items it ON s.itemnumber = it.itemnumber
                    WHERE s.borrowernumber = %s
                    AND s.type = 'issue'
                    AND DATE(s.datetime) BETWEEN %s AND %s
                    AND it.itype = 'DIGITAL'
                """, (borrower_number, start_ay, end_ay))
                
                res = cur_koha.fetchone()
                digital_count = res[0] if res else 0
            finally:
                conn_koha.close()
                
        except Exception as e:
            logger.error(f"Error querying Koha for book issues: {e}")
        
    # Calculate marks
    physical_marks = min((physical_count / physical_target) * physical_weight, physical_weight) if physical_target > 0 else 0
    digital_marks = min((digital_count / digital_target) * digital_weight, digital_weight) if digital_target > 0 else 0
    total_marks = physical_marks + digital_marks
    
    return {
        'physical_count': physical_count,
        'digital_count': digital_count,
        'physical_marks': round(physical_marks, 2),
        'digital_marks': round(digital_marks, 2),
        'total': round(total_marks, 2),
        'max_marks': books_issued_max_marks
    }


def get_book_review_marks(student_username, academic_year=None):
    """
    Get book review marks for a student (out of 30).
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    conn = get_conn()
    try:
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
        conn.close()


def get_book_review_marks_by_trno(trno, academic_year=None):
    """
    Get book review marks for a student by Trno (out of 30).
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    conn = get_conn()
    try:
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
        conn.close()


def get_program_attendance_marks(student_username, academic_year=None):
    """
    Calculate library program attendance marks.
    
    Now sums marks from library_program_attendance linked to specific programs.
    The cap is dynamic based on all programs except 'Books Issued' and 'Book Review'.
    Actually, let's just sum all attendance marks.
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    conn = get_conn()
    try:
        cur = conn.cursor()
        
        cur.execute("""
            SELECT SUM(marks) FROM library_program_attendance
            WHERE student_username = ? AND academic_year = ? AND attended = 1
        """, (student_username, academic_year))
        
        result = cur.fetchone()
        
        total_marks = result[0] if result[0] else 0.0
        return round(total_marks, 2)
    finally:
        conn.close()


def get_student_program_participation(student_username, academic_year=None):
    """
    Get a detailed list of programs a student attended.
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    conn = get_conn()
    try:
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
        # Return as list of dicts for template compatibility
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
    finally:
        conn.close()


def calculate_total_taqeem(student_username, academic_year=None):
    """
    Calculate and return the total Taqeem (marks) for a student.
    
    Final total is sum of:
    1. Books Issued (Automatic from Koha)
    2. Book Review (From book_review_marks table)
    3. Program Attendance (Sum of all other programs in library_program_attendance)
    
    Total capped at 100.
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    conn = get_conn()
    total_result = {
        'book_issue': {'total': 0.0, 'physical_count': 0, 'digital_count': 0, 'max_marks': 0.0},
        'book_review': {'marks': 0.0, 'max_marks': 0.0},
        'program_attendance': 0.0,
        'total': 0.0,
        'details': []
    }
    
    try:
        cur = conn.cursor()
        
        # Get all programs for this AY
        cur.execute("SELECT id, title, marks, marks_category FROM library_programs WHERE academic_year = ?", (academic_year,))
        all_progs = cur.fetchall()
        
        running_total = 0.0
        
        for prog in all_progs:
            p_id = prog['id']
            p_title = prog['title']
            p_title_lower = p_title.lower()
            p_max = float(prog['marks'])
            p_cat = prog['marks_category']
            
            prog_marks = 0.0
            
            if 'books issued' in p_title_lower or 'book issue' in p_title_lower:
                # 1. Koha Auto
                bi_data = calculate_book_issue_marks(student_username, academic_year)
                prog_marks = bi_data['total']
                total_result['book_issue'] = bi_data
            elif 'book review' in p_title_lower:
                # 2. Book Review Table
                br_data = get_book_review_marks(student_username, academic_year)
                # Scale if max marks in program differs from 30 (historical default)
                prog_marks = min(br_data['marks'], p_max)
                total_result['book_review']['marks'] = prog_marks
                total_result['book_review']['max_marks'] = p_max
            else:
                # 3. Attendance Table
                cur.execute("""
                    SELECT SUM(marks) as m FROM library_program_attendance 
                    WHERE student_username = ? AND program_id = ? AND academic_year = ? AND attended = 1
                """, (student_username, p_id, academic_year))
                att = cur.fetchone()
                prog_marks = att['m'] if att and att['m'] else 0.0
                total_result['program_attendance'] += prog_marks
            
            running_total += prog_marks
            total_result['details'].append({
                'id': p_id,
                'title': p_title,
                'marks': round(prog_marks, 2),
                'max_marks': p_max
            })
            
        total_result['total'] = round(min(running_total, 100.0), 2)
        
    except Exception as e:
        logger.error(f"Error calculating total taqeem: {e}")
    finally:
        conn.close()
        
    return total_result


def process_program_marks_upload(file, program_id, academic_year=None, uploaded_by='admin'):
    """
    Process manual marks upload for a specific program.
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    if not file:
        return {'success': False, 'error': 'No file uploaded'}
    
    try:
        # Read file
        if file.filename.endswith('.csv'):
            df = pd.read_csv(io.StringIO(file.stream.read().decode("UTF8")))
        else:
            df = pd.read_excel(BytesIO(file.stream.read()))
            
        # Normalize columns
        df.columns = [str(c).strip().lower() for c in df.columns]
        
        # Look for ITS/Student ID and Marks
        trno_col = next((c for c in df.columns if any(x in c for x in ['its', 'trno', 'student', 'id'])), None)
        marks_col = next((c for c in df.columns if any(x in c for x in ['marks', 'score', 'obtained'])), None)
        
        if not trno_col or not marks_col:
            return {'success': False, 'error': f"CSV/Excel must have 'ITS' and 'Marks' columns. Found: {list(df.columns)}"}
            
        conn = get_conn()
        try:
            cur = conn.cursor()
            
            # Get maximum marks for this program
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
                    
                    # Cap marks
                    if marks > max_marks:
                        marks = max_marks
                    
                    # Find student info via Koha
                    patron = find_student_by_identifier(trno)
                    
                    if patron:
                        username = patron.get('userid') or patron.get('cardnumber') or trno
                        name = f"{patron.get('firstname', '')} {patron.get('surname', '')}".strip()
                        darajah = patron.get('darajah')
                    else:
                        username = trno
                        name = f"Ghost Student ({trno})"
                        darajah = "Unknown (Manual Upload)"
                    
                    # Insert attendance
                    cur.execute("""
                        INSERT OR REPLACE INTO library_program_attendance 
                        (student_username, student_name, darajah_name, academic_year, program_name, program_id, marks, attended, uploaded_by)
                        VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
                    """, (username, name, darajah, academic_year, program_title, program_id, marks, uploaded_by))
                    
                    stats['success'] += 1
                    processed_students.append(username)
                except Exception as e:
                    stats['errors'] += 1
                    logger.error(f"Row upload error: {e}")
                    
            conn.commit()
            
            # Update Taqeem for all processed students
            for u in set(processed_students):
                update_student_taqeem(u, academic_year)
                
            return {
                'success': True, 
                'message': f"Successfully uploaded marks for {stats['success']} students to '{program_title}'.",
                'stats': stats
            }
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"Process program marks upload error: {e}")
        return {'success': False, 'error': str(e)}


def update_student_taqeem(student_username, academic_year=None):
    """
    Calculate and update the student_taqeem table with current marks.
    
    Now works even if student is not in the 'users' table by checking Koha.
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    conn_app = get_conn()
    try:
        cur_app = conn_app.cursor()
        
        # Try to get student info from centralized Koha query
        patron = find_student_by_identifier(student_username)

        if patron:
            username = patron.get('userid') or patron.get('cardnumber') or student_username
            trno = patron.get('trno') or student_username
            student_name = f"{patron.get('firstname', '')} {patron.get('surname', '')}".strip()
            darajah_name = patron.get('darajah')
        else:
            # Check local users as second fallback
            cur_app.execute("""
                SELECT u.username, u.darajah_name, u.trno, u.teacher_name
                FROM users u
                WHERE u.username = ? OR u.trno = ?
            """, (student_username, student_username))
            user_row = cur_app.fetchone()
            
            if user_row:
                username, darajah_name, trno, student_name = user_row
            else:
                username = student_username
                trno = student_username
                student_name = student_username
                darajah_name = "Unknown"
        
        # Calculate all marks
        taqeem = calculate_total_taqeem(student_username, academic_year)
        
        # Update or insert into student_taqeem
        cur_app.execute("""
            INSERT OR REPLACE INTO student_taqeem (
                student_username, student_trno, student_name, darajah_name, academic_year,
                physical_books_issued, digital_books_issued,
                physical_books_marks, digital_books_marks, book_issue_total,
                book_review_marks, program_attendance_marks, total_marks,
                last_updated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            username, 
            trno, 
            student_name, 
            darajah_name, 
            academic_year,
            taqeem['book_issue']['physical_count'],
            taqeem['book_issue']['digital_count'],
            taqeem['book_issue']['physical_marks'],
            taqeem['book_issue']['digital_marks'],
            taqeem['book_issue']['total'],
            taqeem['book_review']['marks'],
            taqeem['program_attendance'],
            taqeem['total']
        ))
        
        conn_app.commit()
        return True
    finally:
        conn_app.close()


def update_all_student_taqeem(academic_year=None):
    """
    Update Taqeem for all students.
    
    Includes those in the users table AND those only in marks tables.
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    conn = get_conn()
    try:
        cur = conn.cursor()
        
        # Get user list from multiple sources
        cur.execute("""
            SELECT DISTINCT student_username FROM (
                SELECT username as student_username FROM users WHERE role = 'student'
                UNION
                SELECT student_username FROM library_program_attendance
                UNION
                SELECT student_username FROM book_review_marks
            )
        """)
        students = cur.fetchall()
        
        success_count = 0
        for (username,) in students:
            if update_student_taqeem(username, academic_year):
                success_count += 1
        
        logger.info(f"Updated Taqeem for {success_count} students")
        return success_count
    finally:
        conn.close()


def get_book_review_stats(academic_year=None):
    """
    Get statistics about book review marks.
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    conn = get_conn()
    try:
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
        conn.close()


def export_book_review_marks(academic_year=None, file_format='csv'):
    """
    Export book review marks to CSV or Excel format.
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    conn = get_conn()
    try:
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
        conn.close()
    
    if file_format.lower() == 'excel':
        # Create pandas DataFrame and export to Excel
        import pandas as pd
        
        df = pd.DataFrame(rows, columns=columns)
        
        # Create Excel writer
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Book Review Marks', index=False)
        
        output.seek(0)
        return output, 'book_review_marks.xlsx'
    
    else:
        # Default to CSV
        import io
        
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(columns)
        writer.writerows(rows)
        
        csv_content = output.getvalue()
        output.close()
        
        return csv_content, 'book_review_marks.csv'


def save_taqeem_to_db(student_username, student_trno, academic_year, book_issue_marks, 
                     book_review_marks, program_attendance_marks, total_marks):
    """
    Save calculated Taqeem to the database.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        
        # Get student info
        cur.execute("""
            SELECT darajah_name, teacher_name FROM users WHERE username = ?
        """, (student_username,))
        
        student_info = cur.fetchone()
        if not student_info:
            return False
            
        darajah_name, student_name = student_info
        if not student_name:
            student_name = student_username
        
        # Insert or update taqeem record
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
        conn.close()


def get_review_count(student_username, academic_year=None):
    """
    Get the number of reviews submitted by a student.
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    conn = get_conn()
    try:
        cur = conn.cursor()
        
        cur.execute("""
            SELECT review_count FROM book_review_marks
            WHERE student_username = ? AND academic_year = ?
        """, (student_username, academic_year))
        
        result = cur.fetchone()
        
        return result[0] if result and result[0] else 0
    finally:
        conn.close()


def calculate_book_review_marks(student_username, academic_year=None):
    """
    Calculate book review marks (out of 30).
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    book_review = get_book_review_marks(student_username, academic_year)
    return book_review['marks']


def calculate_program_attendance_marks(student_username, academic_year=None):
    """
    Calculate program attendance marks (out of 10).
    """
    if academic_year is None:
        academic_year = get_current_academic_year()
    
    return get_program_attendance_marks(student_username, academic_year)
