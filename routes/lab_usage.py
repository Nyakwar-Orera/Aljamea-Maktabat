from flask import Blueprint, render_template, request, jsonify, current_app, redirect
from db_app import get_conn
import datetime

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
    # Mock data for now until DB table is confirmed
    sessions = [
        {
            "id": 1, 
            "student_name": "Mustafa Burhanuddin", 
            "trno": "28547", 
            "computer_id": "LAB-01", 
            "start_time": "10:00 AM", 
            "end_time": "11:30 AM", 
            "duration": "90 mins",
            "status": "Completed"
        },
        {
            "id": 2, 
            "student_name": "Taher Mufaddal", 
            "trno": "28539", 
            "computer_id": "LAB-05", 
            "start_time": "11:15 AM", 
            "end_time": "-", 
            "duration": "Running",
            "status": "Active"
        }
    ]
    return jsonify({"success": True, "sessions": sessions})
