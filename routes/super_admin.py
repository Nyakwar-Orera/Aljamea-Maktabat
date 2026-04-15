# routes/super_admin.py
from datetime import date, datetime
from flask import Blueprint, render_template, session, redirect, url_for, flash, request, jsonify, current_app
from db_koha import get_koha_conn
from services import koha_queries as KQ
from config import Config

bp = Blueprint("super_admin_bp", __name__)

@bp.before_request
def restrict_to_super_admin():
    """Ensure only Super Admins can access these routes."""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))
    
    if session.get("admin_type") != "super":
        flash("Access Denied: Super Admin privileges required.", "danger")
        return redirect(url_for("dashboard_bp.dashboard"))

@bp.route("/")
@bp.route("/dashboard")
def god_eye():
    """Global 'God's Eye' Dashboard."""
    # Placeholder for parallel aggregation in Plan 01-02
    return render_template("super_admin/dashboard.html", 
                         title="God's Eye Dashboard",
                         active_branches=Config.get_active_branches())

@bp.route("/check_parallel")
def check_parallel():
    """Diagnostic route for parallel engine testing."""
    from services.branch_queries import get_all_branches_summary
    import time
    
    start_time = time.time()
    summaries = get_all_branches_summary()
    end_time = time.time()
    
    return jsonify({
        "status": "success",
        "time_taken": f"{end_time - start_time:.2f} seconds",
        "branch_results": [
            {
                "branch": s["branch_code"],
                "status": s["status"],
                "total_patrons": s["total_patrons"]
            } for s in summaries
        ]
    })
