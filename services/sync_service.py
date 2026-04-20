# services/sync_service.py
import logging
import sqlite3
import threading
from typing import Dict, List, Any
from flask import current_app
from config import Config
from db_koha import get_branch_conn, _MockConnection
from db_app import get_conn as get_app_conn
from werkzeug.security import generate_password_hash

logger = logging.getLogger(__name__)

# Global lock to prevent concurrent syncs
_sync_in_progress = False
_sync_lock = threading.Lock()


def sync_all_campuses_patrons():
    """
    Entry point for global patron synchronization.
    Runs the synchronization in a background thread to prevent UI timeout.
    """
    global _sync_in_progress
    
    with _sync_lock:
        if _sync_in_progress:
            logger.warning("Synchronization already in progress. Skipping additional request.")
            return False
        _sync_in_progress = True
        
    # Use a threading to avoid blocking the request
    thread = threading.Thread(target=_perform_sync_wrapper)
    thread.daemon = True
    thread.start()
    return True

def _perform_sync_wrapper():
    """Wrapper to ensure the flag is reset when sync finishes."""
    global _sync_in_progress
    try:
        _perform_sync()
    finally:
        with _sync_lock:
            _sync_in_progress = False


def _perform_sync():
    """Background worker that performs the actual data fetching and upserting."""
    logger.info("Starting global user synchronization from Koha branches...")
    
    active_branches = Config.get_active_branches()
    
    total_added = 0
    total_updated = 0
    
    for branch_cfg in active_branches:
        branch_code = branch_cfg["code"]
        logger.info(f"🔄 Syncing patrons for {branch_code}...")
        try:
            results = _sync_branch_patrons(branch_code)
            total_added += results.get("added", 0)
            total_updated += results.get("updated", 0)
            logger.info(f"✅ {branch_code} sync finished: {results.get('added')} added, {results.get('updated')} updated.")
        except Exception as e:
            logger.error(f"❌ Failed to sync patrons for branch {branch_code}: {e}")
            
    logger.info(f"Global sync completed: {total_added} added, {total_updated} updated.")

def _sync_branch_patrons(branch_code: str) -> Dict[str, int]:
    """Sync patrons from a single branch Koha DB to local app DB."""
    stats = {"added": 0, "updated": 0, "errors": 0}
    
    branch_info = Config.CAMPUS_REGISTRY.get(branch_code, {})
    campus_name = branch_info.get("short_name", branch_code)
    
    # 1. Connect to Koha Branch DB
    koha_conn = get_branch_conn(branch_code)
    if isinstance(koha_conn, _MockConnection):
        logger.warning(f"Branch {branch_code} is using a Mock connection, skipping sync.")
        return stats
    
    try:
        koha_cur = koha_conn.cursor(dictionary=True)
        
        # 2. Fetch all borrowers with relevant fields and attributes
        # We fetch active borrowers primarily
        query = """
            SELECT 
                b.borrowernumber,
                b.cardnumber AS itsid,
                b.firstname,
                b.surname,
                b.email,
                b.categorycode,
                b.branchcode,
                (SELECT attribute FROM borrower_attributes WHERE borrowernumber = b.borrowernumber AND code IN ('Class','STD','CLASS','DAR','CLASS_STD') LIMIT 1) AS darajah_name,
                (SELECT attribute FROM borrower_attributes WHERE borrowernumber = b.borrowernumber AND code = 'TRNO' LIMIT 1) AS trno
            FROM borrowers b
            WHERE (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
              AND (b.debarred IS NULL OR b.debarred = 0)
              AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
        """
        koha_cur.execute(query)
        patrons = koha_cur.fetchall()
        koha_cur.close()
        
        if not patrons:
            logger.info(f"No active patrons found for branch {branch_code}.")
            return stats

        # 3. Connect to App Data DB
        app_conn = get_app_conn()
        app_cur = app_conn.cursor()
        
        current_ay = Config.CLEAN_ACADEMIC_YEAR()

        processed_count = 0
        for p in patrons:
            try:
                itsid = str(p.get("itsid") or "").strip()
                if not itsid:
                    continue
                
                # Extract basic info
                firstname = p.get("firstname") or ""
                surname = p.get("surname") or ""
                name = f"{surname} {firstname}".strip() or f"User {itsid}"
                email = (p.get("email") or f"{itsid}@jamea.org").lower().strip()
                category = (p.get("categorycode") or "").upper()
                darajah = (p.get("darajah_name") or "Unassigned").strip()
                trno = p.get("trno") or itsid

                # Determine Role
                if category.startswith('S'):
                    role = 'student'
                elif category in ['T-KG', 'T', 'TEACHER']:
                    role = 'teacher'
                elif category == 'HO':
                    role = 'admin' # Head Office
                else:
                    role = 'student' # Default fallback
                
                # Check if user exists in local DB
                app_cur.execute("SELECT id FROM users WHERE username = ?", (itsid,))
                user_row = app_cur.fetchone()
                
                if user_row:
                    app_cur.execute("""
                        UPDATE users 
                        SET email = ?, role = ?, darajah_name = ?, class_name = ?, 
                            teacher_name = ?, branch_code = ?, campus_branch = ?, trno = ?
                        WHERE username = ?
                    """, (email, role, darajah, darajah, name, branch_code, campus_name, trno, itsid))
                    stats["updated"] += 1
                else:
                    default_pw = itsid[:4] + "123" if len(itsid) >= 4 else itsid + "123"
                    pw_hash = generate_password_hash(default_pw)
                    
                    app_cur.execute("""
                        INSERT INTO users (username, email, role, password_hash, darajah_name, class_name, teacher_name, branch_code, campus_branch, trno)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (itsid, email, role, pw_hash, darajah, darajah, name, branch_code, campus_name, trno))
                    stats["added"] += 1
                
                # Manage Mappings
                if role == 'teacher':
                    app_cur.execute("SELECT id FROM teacher_darajah_mapping WHERE teacher_username = ? AND darajah_name = ?", (itsid, darajah))
                    if not app_cur.fetchone():
                        app_cur.execute("""
                            INSERT INTO teacher_darajah_mapping 
                            (teacher_username, teacher_name, darajah_name, teacher_email, role, academic_year, campus_branch, branch_code)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (itsid, name, darajah, email, 'class_teacher', current_ay, campus_name, branch_code))
                
                elif role == 'student':
                    app_cur.execute("SELECT id FROM student_darajah_mapping WHERE student_username = ? AND darajah_name = ?", (itsid, darajah))
                    if not app_cur.fetchone():
                        app_cur.execute("""
                            INSERT INTO student_darajah_mapping 
                            (student_username, student_name, darajah_name, academic_year, campus_branch, branch_code)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (itsid, name, darajah, current_ay, campus_name, branch_code))

                processed_count += 1
                # Commit in batches of 100 to avoid long locks and memory issues
                if processed_count % 100 == 0:
                    app_conn.commit()
                    
            except Exception as row_error:
                logger.warning(f"Skipping patron {itsid} due to error: {row_error}")
                stats["errors"] += 1

        app_conn.commit()
        app_conn.close()
        logger.info(f"Sync for branch {branch_code} successful: {stats['added']} added, {stats['updated']} updated.")
        
    except Exception as e:
        logger.error(f"Error syncing {branch_code}: {e}", exc_info=True)
        stats["errors"] += 1
    finally:
        if koha_conn:
            koha_conn.close()
            
    return stats
