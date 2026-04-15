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
    """Global 'God's Eye' Dashboard with real-time aggregation."""
    from services.branch_queries import get_all_branches_summary, get_global_aggregate
    
    # 1. Fetch data from all campuses in parallel
    summaries = get_all_branches_summary()
    
    # 2. Compute global aggregate
    aggregate = get_global_aggregate(summaries)
    
    return render_template("super_admin/dashboard.html", 
                         title="God's Eye Dashboard",
                         summaries=summaries,
                         aggregate=aggregate)

@bp.route("/switch/<branch_code>")
def switch_branch(branch_code):
    """Switch context to a specific branch dashboard."""
    if branch_code not in Config.CAMPUS_REGISTRY:
        flash(f"Error: Unknown branch code {branch_code}", "danger")
        return redirect(url_for("super_admin_bp.god_eye"))
    
    # Store selected branch in session
    session["branch_code"] = branch_code
    session["is_super_admin"] = True # Ensure they keep super powers
    
    flash(f"Teleported to {Config.CAMPUS_REGISTRY[branch_code]['short_name']} Dashboard", "success")
    return redirect(url_for("dashboard_bp.dashboard"))

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
