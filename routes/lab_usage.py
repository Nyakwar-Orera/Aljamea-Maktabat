from flask import Blueprint, render_template, request, jsonify, current_app, redirect
from db_app import get_conn

bp = Blueprint("lab_usage", __name__, url_prefix="/lab-usage")

@bp.route("/")
def lab_dashboard():
    """
    Redirect to the external Lab Management System.
    """
    return redirect("https://students-report-analyzer-3.onrender.com/")

@bp.route("/api/sessions")
def get_sessions():
    """
    API to fetch lab sessions.
    Filters: date, student_id, active_only
    """
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id, student_name, trno, computer_id, start_time, end_time, status FROM lab_sessions ORDER BY id DESC LIMIT 100")
        rows = cur.fetchall()
        
        sessions = []
        for row in rows:
            sessions.append({
                "id": row["id"],
                "student_name": row["student_name"],
                "trno": row["trno"],
                "computer_id": row["computer_id"],
                "start_time": row["start_time"],
                "end_time": row["end_time"] if row["end_time"] else "-",
                "status": row["status"]
            })
            
        return jsonify({"success": True, "sessions": sessions})
    except Exception as e:
        current_app.logger.error(f"Error fetching lab sessions: {e}")
        return jsonify({"success": False, "error": str(e)})
    finally:
        if 'conn' in locals():
            conn.close()
