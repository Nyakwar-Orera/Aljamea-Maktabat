# routes/super_admin.py
from datetime import date, datetime
from flask import Blueprint, render_template, session, redirect, url_for, flash, request, jsonify, current_app
from db_koha import get_koha_conn
from services import koha_queries as KQ
from config import Config

from werkzeug.security import generate_password_hash
from db_app import get_conn as get_app_conn
import secrets
import string

bp = Blueprint("super_admin_bp", __name__)

@bp.before_request
def restrict_to_super_admin():
    """Ensure only Super Admins can access these routes."""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))
    
    if not session.get("is_super_admin"):
        flash("Access Denied: Super Admin privileges required.", "danger")
        return redirect(url_for("dashboard_bp.dashboard"))

@bp.route("/")
@bp.route("/dashboard")
def god_eye():
    """Global 'God's Eye' Dashboard with real-time aggregation."""
    from services.branch_queries import (
        get_all_branches_summary, get_global_aggregate, get_global_top_titles,
        get_global_language_distribution, get_global_fiction_stats,
        get_global_top_students, get_global_darajah_performance,
        get_global_top_students_by_sex, get_global_darajah_full_breakdown,
        get_global_top_books_by_lang, get_global_subject_cloud_from_summaries,
        get_global_language_chart_data
    )
    
    # AY Filter
    selected_ay = session.get("selected_ay", "current")
    hijri_year = int(selected_ay) if selected_ay != "current" else None
    
    # 1. Fetch data from all campuses in parallel
    summaries = get_all_branches_summary(hijri_year=hijri_year)
    
    # 2. Compute global aggregate
    aggregate = get_global_aggregate(summaries)
    
    # 3. Aggregate Top Titles & Distribution
    global_top_titles = get_global_top_titles(summaries)
    lang_dist = get_global_language_distribution(summaries)
    lang_chart_data = get_global_language_chart_data(summaries)  # New: formatted for chart
    fiction_stats = get_global_fiction_stats(summaries)
    global_top_students = get_global_top_students(summaries)
    top_students_global_male = get_global_top_students_by_sex('M', limit=10, hijri_year=hijri_year)
    top_students_global_female = get_global_top_students_by_sex('F', limit=10, hijri_year=hijri_year)
    global_darajahs = get_global_darajah_performance(summaries)
    detailed_darajahs = get_global_darajah_full_breakdown(summaries)
    top_books_arabic = get_global_top_books_by_lang(summaries, 'ara', limit=10)
    top_books_english = get_global_top_books_by_lang(summaries, 'eng', limit=10)
    top_books_urdu = get_global_top_books_by_lang(summaries, 'urd', limit=10)
    top_books_lisan = get_global_top_books_by_lang(summaries, 'lis', limit=10)
    
    # CHARTS DATA PROCESSING
    # 1. Monthly Trend Data (Cross-Campus)
    month_labels = []
    monthly_overview = []
    
    # Extract labels from the first available online branch summary
    for s in summaries:
        if s.get("status") == "online" and s.get("monthly_trend"):
            month_labels = s["monthly_trend"].get("labels", [])
            break
            
    for s in summaries:
        if s.get("status") == "online":
            monthly_overview.append({
                "label": s.get("short_name", s.get("branch_code")),
                "data": s.get("monthly_trend", {}).get("values", [0]*len(month_labels)),
                "color": s.get("color", "#888")
            })

    # 2. Language Data - Use the new chart data
    lang_labels = [item["label"] for item in lang_chart_data]
    lang_values = [item["value"] for item in lang_chart_data]
    lang_colors = [item["color"] for item in lang_chart_data]
    
    # 3. Darajah Performance Labels
    darajah_labels = [d.get("name", d.get("Darajah", "Unknown")) for d in global_darajahs[:12]]
    darajah_male = [d.get("male_issues", 0) for d in global_darajahs[:12]]
    darajah_female = [d.get("female_issues", 0) for d in global_darajahs[:12]]
    
    # 4. Marhala Data
    marhala_map = {}
    for s in summaries:
        for m in s.get("top_marhalas", []):
            name = m.get("marhala", m.get("Marhala", "Unknown"))
            issues = m.get("issues", m.get("Issues", 0))
            marhala_map[name] = marhala_map.get(name, 0) + issues
    
    marhala_labels = list(marhala_map.keys())[:8]
    marhala_values = [marhala_map[l] for l in marhala_labels]
    
    # 5. Global Subjects - Format for tag cloud with proper display names
    subject_cloud = get_global_subject_cloud_from_summaries(summaries)
    # Ensure subject names match the image: "500 - Natural Sciences", etc.
    subject_display_map = {
        "000 - Generalities": "000 - Generalities",
        "100 - Philosophy": "100 - Philosophy",
        "200 - Religion": "200 - Religion",
        "300 - Social Sciences": "300 - Social Sciences",
        "400 - Language": "400 - Language",
        "500 - Natural Sciences": "500 - Natural Sciences",
        "600 - Technology": "600 - Technology",
        "700 - The Arts": "700 - The Arts",
        "800 - Literature": "800 - Literature",
        "900 - History & Geography": "900 - History & Geography",
    }
    for item in subject_cloud:
        subj = item.get("Subject", "")
        if subj in subject_display_map:
            item["display_name"] = subject_display_map[subj]
        else:
            item["display_name"] = subj
    
    # 6. Global Insights
    insights = [
        f"Global network is LIVE across {len([s for s in summaries if s.get('status')=='online'])} active campuses.",
        f"Most active class globally: {global_darajahs[0]['name'] if global_darajahs else 'N/A'}.",
        f"Total knowledge exchange: {aggregate.get('total_issues', 0):,} checkouts across all branches.",
        f"Leading language in circulation: {lang_labels[0] if lang_labels else 'N/A'}."
    ]
    
    campus_registry = Config.CAMPUS_REGISTRY
    available_years = KQ.get_available_academic_years()
    from services.koha_queries import get_current_ay_year
    current_ay = get_current_ay_year()
    
    # Compute today's activity for KPI card
    today_checkouts = sum(s.get("today_activity", {}).get("checkouts", 0) for s in summaries if s.get("status") == "online")
    today_checkins = sum(s.get("today_activity", {}).get("checkins", 0) for s in summaries if s.get("status") == "online")
    
    return render_template("super_admin/god_eye.html", 
                         title="God's Eye Dashboard",
                         branch_summaries=summaries,
                         global_kpis=aggregate,
                         top_books_all=global_top_titles,
                         top_books_arabic=top_books_arabic,
                         top_books_english=top_books_english,
                         top_books_urdu=top_books_urdu,
                         top_books_lisan=top_books_lisan,
                         lang_dist=lang_dist,
                         lang_chart_labels=lang_labels,
                         lang_chart_values=lang_values,
                         lang_chart_colors=lang_colors,
                         fiction_stats=fiction_stats,
                         top_students=global_top_students,
                         top_students_global_male=top_students_global_male,
                         top_students_global_female=top_students_global_female,
                         class_perf=global_darajahs,
                         detailed_darajahs=detailed_darajahs,
                         subject_cloud=subject_cloud,
                         insights=insights,
                         branch_order=Config.BRANCH_ORDER,
                         campus_registry=campus_registry,
                         selected_ay=selected_ay,
                         current_academic_year=f"AY {hijri_year or current_ay}H",
                         available_years=available_years,
                         now=datetime.utcnow(),
                         selected_branch=request.args.get('branch', 'all'),
                         primary_cfg=campus_registry.get('AJSN', {}),
                         comparison={"week_labels": month_labels, "weekly_overview": monthly_overview},
                         lang_labels=lang_labels, lang_values=lang_values,
                         lang_stats=[{"language": l, "issue_count": v} for l, v in zip(lang_labels, lang_values)],
                         darajah_labels=darajah_labels, 
                         darajah_male_values=darajah_male, 
                         darajah_female_values=darajah_female,
                         marhala_labels=marhala_labels, marhala_values=marhala_values,
                         today_checkouts=today_checkouts,
                         today_checkins=today_checkins)

@bp.route("/explorer")
def branch_explorer():
    """Visual index of all campuses."""
    from services.branch_queries import get_all_branches_summary
    summaries = get_all_branches_summary()
    return render_template("super_admin/branch_explorer.html", summaries=summaries)

@bp.route("/switch/<branch_code>")
def switch_branch(branch_code):
    """Switch context to a specific branch dashboard."""
    if branch_code not in Config.CAMPUS_REGISTRY:
        flash(f"Error: Unknown branch code {branch_code}", "danger")
        return redirect(url_for("super_admin_bp.god_eye"))
    
    # Store selected branch in session
    session["branch_code"] = branch_code
    session["is_super_admin"] = True
    
    flash(f"Teleported to {Config.CAMPUS_REGISTRY[branch_code]['short_name']} Dashboard", "success")
    return redirect(url_for("dashboard_bp.dashboard"))

@bp.route("/teleport/<branch_code>/<darajah>")
def teleport_to_teacher_dashboard(branch_code, darajah):
    """Special teleport: Switch branch and jump straight to teacher dashboard for a darajah."""
    if branch_code not in Config.CAMPUS_REGISTRY:
        flash(f"Error: Unknown branch code {branch_code}", "danger")
        return redirect(url_for("super_admin_bp.god_eye"))
    
    # Authenticated context switch
    session["branch_code"] = branch_code
    session["is_super_admin"] = True
    
    # Redirect to teacher dashboard with the darajah parameter
    return redirect(url_for("teacher_dashboard_bp.dashboard", darajah=darajah))

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

@bp.route("/compare")
def compare_branches():
    """Detailed benchmarking matrix across all campuses."""
    from services.branch_queries import get_all_branches_summary
    summaries = get_all_branches_summary()
    return render_template("super_admin/compare.html", summaries=summaries)

@bp.route("/users")
def manage_users():
    """Manage global administrative users with server-side pagination."""
    # Pagination parameters
    page = request.args.get('page', 1, type=int)
    search_query = request.args.get('q', '').strip()
    per_page = Config.PAGE_SIZE
    offset = (page - 1) * per_page

    conn = get_app_conn()
    cur = conn.cursor()
    
    # 1. Fetch KPI Counts (Roles)
    cur.execute("SELECT role, COUNT(*) as count FROM users GROUP BY role")
    role_counts = {row['role']: row['count'] for row in cur.fetchall()}
    
    # 2. Fetch KPI Counts (Branches)
    cur.execute("SELECT COALESCE(branch_code, 'Global') as branch_code, COUNT(*) as count FROM users GROUP BY branch_code")
    branch_counts = {row['branch_code']: row['count'] for row in cur.fetchall()}
    
    # 3. Fetch Total User Count for Pagination
    if search_query:
        cur.execute("""
            SELECT COUNT(*) FROM users 
            WHERE username LIKE ? OR teacher_name LIKE ? OR department_name LIKE ?
        """, (f'%{search_query}%', f'%{search_query}%', f'%{search_query}%'))
    else:
        cur.execute("SELECT COUNT(*) FROM users")
    total_users = cur.fetchone()[0]
    total_pages = (total_users + per_page - 1) // per_page

    # 4. Fetch Paginated Users
    if search_query:
        cur.execute("""
            SELECT * FROM users 
            WHERE username LIKE ? OR teacher_name LIKE ? OR department_name LIKE ?
            ORDER BY created_at DESC 
            LIMIT ? OFFSET ?
        """, (f'%{search_query}%', f'%{search_query}%', f'%{search_query}%', per_page, offset))
    else:
        cur.execute("""
            SELECT * FROM users 
            ORDER BY created_at DESC 
            LIMIT ? OFFSET ?
        """, (per_page, offset))
    users = [dict(row) for row in cur.fetchall()]
    
    conn.close()
    
    return render_template("super_admin/users.html", 
                         users=users,
                         role_counts=role_counts,
                         branch_counts=branch_counts,
                         total_users=total_users,
                         current_page=page,
                         total_pages=total_pages,
                         per_page=per_page,
                         campus_registry=Config.CAMPUS_REGISTRY,
                         branch_order=Config.BRANCH_ORDER,
                         page_title="Global User Management")

@bp.route("/users/create", methods=["GET", "POST"])
def create_user():
    """Add a new administrative user to the system."""
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        teacher_name = request.form.get("teacher_name", "").strip()
        email = request.form.get("email", "").strip()
        role = request.form.get("role", "teacher")
        branch_code = request.form.get("branch_code", "").strip() or None
        password = request.form.get("password", "").strip()
        
        if not password:
            password = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(10))
            show_password = True
        else:
            show_password = False
            
        hashed_pw = generate_password_hash(password)
        
        conn = get_app_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO users (username, teacher_name, email, role, branch_code, password_hash)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (username, teacher_name, email, role, branch_code, hashed_pw))
            conn.commit()
            flash(f"User {username} created successfully." + (f" Password: {password}" if show_password else ""), "success")
            return redirect(url_for("super_admin_bp.manage_users"))
        except Exception as e:
            flash(f"Error creating user: {e}", "danger")
        finally:
            conn.close()
            
    return render_template("super_admin/create_user.html", 
                         campus_registry=Config.CAMPUS_REGISTRY,
                         branch_order=Config.BRANCH_ORDER,
                         page_title="Create New User")

@bp.route("/users/toggle_branch/<int:user_id>", methods=["POST"])
def toggle_user_branch(user_id):
    """Update a user's assigned campus branch."""
    branch_code = request.form.get("branch_code", "").strip() or None
    
    conn = get_app_conn()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE users SET branch_code = ? WHERE id = ?", (branch_code, user_id))
        conn.commit()
        flash("User branch updated successfully.", "success")
    except Exception as e:
        flash(f"Error updating user branch: {e}", "danger")
    finally:
        conn.close()
        
    return redirect(url_for("super_admin_bp.manage_users"))

@bp.route("/sync_patrons", methods=["POST"])
def sync_all_patrons():
    """Trigger a global synchronization of patron data."""
    from services.sync_service import sync_all_campuses_patrons
    try:
        sync_all_campuses_patrons()
        flash("Global synchronization started in the background. This may take a few minutes for all branches.", "success")
    except Exception as e:
        flash(f"Error starting synchronization: {e}", "danger")
        
    return redirect(url_for("super_admin_bp.manage_users"))

@bp.route("/search")
def search_all():
    """Global search across all campuses."""
    query = request.args.get("q", "").strip()
    if not query:
        return redirect(url_for("super_admin_bp.god_eye"))
    
    flash(f"Searching for '{query}' across all branches...", "info")
    return render_template("super_admin/search_results.html", 
                         query=query,
                         campus_registry=Config.CAMPUS_REGISTRY)

@bp.route("/branch_dashboard/<branch_code>")
def branch_full_dashboard(branch_code):
    """Alias for switch_branch."""
    return switch_branch(branch_code)

@bp.route("/branch_deep_dive/<branch_code>")
def branch_deep_dive(branch_code):
    """Detailed analytics for a specific branch."""
    session["branch_code"] = branch_code
    flash(f"Deep dive into {branch_code} initiated.", "info")
    return redirect(url_for("dashboard_bp.dashboard"))

@bp.route("/branch_darajahs/<branch_code>")
def branch_darajahs_view(branch_code):
    """Class-wise view for a specific branch."""
    session["branch_code"] = branch_code
    return redirect(url_for("dashboard_bp.dashboard"))

@bp.route("/settings")
def branch_settings():
    """Global branch configurations."""
    flash("Branch settings are currently locked for Multi-Campus Rollout.", "warning")
    return redirect(url_for("super_admin_bp.god_eye"))

@bp.route("/audit")
def audit_log():
    """Global audit logs."""
    flash("Audit log viewing is currently restricted.", "info")
    return redirect(url_for("super_admin_bp.god_eye"))


@bp.route("/api/users_by_role/<role>")
def api_users_by_role(role):
    """API endpoint to get users by role for dashboard popups."""
    conn = get_app_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT username, teacher_name as name, branch_code, darajah_name, role
        FROM users 
        WHERE role = ? 
        ORDER BY teacher_name ASC 
        LIMIT 100
    """, (role,))
    users = [dict(row) for row in cur.fetchall()]
    conn.close()
    return jsonify(users)

@bp.route("/api/users_by_branch/<branch>")
def api_users_by_branch(branch):
    """API endpoint to get users by branch for dashboard popups."""
    if branch == 'Global':
        branch_filter = None
    else:
        branch_filter = branch
        
    conn = get_app_conn()
    cur = conn.cursor()
    if branch_filter:
        cur.execute("""
            SELECT username, teacher_name as name, role, darajah_name, branch_code
            FROM users 
            WHERE branch_code = ? 
            ORDER BY teacher_name ASC 
            LIMIT 100
        """, (branch_filter,))
    else:
        cur.execute("""
            SELECT username, teacher_name as name, role, darajah_name, branch_code
            FROM users 
            WHERE branch_code IS NULL 
            ORDER BY teacher_name ASC 
            LIMIT 100
        """, ())
    users = [dict(row) for row in cur.fetchall()]
    conn.close()
    return jsonify(users)

@bp.route("/api/language-details")
def api_language_details():
    """API endpoint to get language distribution details for a specific language."""
    lang_name = request.args.get("lang", "")
    if not lang_name:
        return jsonify({"error": "No language specified"}), 400
    
    from services.branch_queries import get_global_language_distribution
    lang_dist = get_global_language_distribution()
    
    if lang_name in lang_dist:
        return jsonify({
            "total": lang_dist[lang_name]["total"],
            "branches": lang_dist[lang_name]["branches"]
        })
    else:
        return jsonify({"total": 0, "branches": {}})