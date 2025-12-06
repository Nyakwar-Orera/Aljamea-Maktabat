# routes/dashboard.py
from datetime import date, datetime
from flask import Blueprint, render_template, session, redirect, url_for, flash, request
from db_koha import get_koha_conn
from services import koha_queries as KQ

# Optional Hijri conversion (requires hijri-converter in requirements.txt)
try:
    from hijri_converter import convert as hijri_convert
except ImportError:  # library not installed – we'll just skip Hijri conversion
    hijri_convert = None

bp = Blueprint("dashboard_bp", __name__)

HIJRI_MONTHS = [
    "Muḥarram al-Harām",
    "Safar al-Muzaffar",
    "Rabi al-Awwal",
    "Rabī al-Aakhar",
    "Jamādil Awwal",
    "Jamādā al-ʾŪkhrā",
    "Rajab al-Asab",
    "Shabān al-Karim",
    "Shehrullah al-Moazzam",
    "Shawwāl al-Mukarram",
    "Zilqādah al-Harām",
    "Zilhijjatil Harām",
]

# ---------------- AY WINDOW ----------------
def _ay_bounds():
    """
    Return (start_date, end_date) for current academic window:
    1 April → today (capped at 31 Dec of current year).
    If today is before April, use previous year's AY.
    """
    today = date.today()
    
    if 4 <= today.month <= 12:
        # Current AY started this year
        start = date(today.year, 4, 1)
        end = today
    else:
        # Current AY started last year
        ay_year = today.year - 1
        start = date(ay_year, 4, 1)
        end = min(today, date(ay_year, 12, 31))
    
    return start, end


def _hijri_month_year_label(d: date) -> str:
    """
    Convert a Gregorian date to 'HijriMonth HijriYear H' if hijri-converter is available.
    Fallback: 'Mon YYYY'.
    """
    if not hijri_convert:
        return d.strftime("%b %Y")
    try:
        h = hijri_convert.Gregorian(d.year, d.month, d.day).to_hijri()
        month_name = HIJRI_MONTHS[h.month - 1]
        return f"{month_name} {h.year} H"
    except Exception:
        return d.strftime("%b %Y")


def get_hijri_today() -> str:
    """
    Return today's date in Hijri (DD-MM-YYYY H) if hijri-converter is available.
    Otherwise return an empty string.
    """
    if not hijri_convert:
        return ""
    today = date.today()
    try:
        h = hijri_convert.Gregorian(today.year, today.month, today.day).to_hijri()
        return f"{h.day:02d}-{h.month:02d}-{h.year} H"
    except Exception:
        return ""


def get_hijri_date_label(d: date) -> str:
    """
    Return full Hijri date label: "DD MonthName YYYY H"
    """
    if not hijri_convert:
        return d.strftime("%d %B %Y")
    try:
        h = hijri_convert.Gregorian(d.year, d.month, d.day).to_hijri()
        month_name = HIJRI_MONTHS[h.month - 1]
        return f"{h.day} {month_name} {h.year} H"
    except Exception:
        return d.strftime("%d %B %Y")


# ---------------- UTILITIES ----------------
def get_kpis():
    """
    Fetch core KPIs from Koha DB with accurate patron counts.
    """
    s = KQ.get_summary()
    
    # Use accurate patron counts
    active_patrons = int(s.get("active_patrons", 0))
    student_patrons = int(s.get("student_patrons", 0))
    non_student_patrons = int(s.get("non_student_patrons", 0))
    total_issues = int(s.get("total_issues", 0))
    total_fines = float(s.get("fines_paid", 0.0))
    overdue = int(s.get("overdue", 0))
    total_titles = int(s.get("total_titles", 0))
    total_titles_issued = int(s.get("total_titles_issued", 0))
    
    return {
        "active_patrons": active_patrons,
        "student_patrons": student_patrons,
        "non_student_patrons": non_student_patrons,
        "total_issues": total_issues,
        "total_fines": total_fines,
        "overdue": overdue,
        "total_titles": total_titles,
        "total_titles_issued": total_titles_issued,
        "active_patrons_ay": int(s.get("active_patrons_ay", 0))
    }


def get_today_activity():
    """Return (today_checkouts, today_checkins) from statistics table."""
    return KQ.today_activity()


def get_trends():
    """
    Borrowing trends for the current academic window (AY).
    """
    start, end = _ay_bounds()
    if not start:
        return [], []

    conn = get_koha_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DATE_FORMAT(`datetime`, '%Y-%m') AS ym, COUNT(*) AS cnt
        FROM statistics
        WHERE `type`='issue'
          AND DATE(`datetime`) BETWEEN %s AND %s
        GROUP BY ym
        ORDER BY ym ASC;
        """,
        (start, end),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    labels = []
    values = []

    for ym, cnt in rows:
        try:
            year_str, month_str = str(ym).split("-")
            year = int(year_str)
            month = int(month_str)
            d = date(year, month, 1)
        except Exception:
            d = start
        labels.append(_hijri_month_year_label(d))
        values.append(int(cnt))

    return labels, values


def get_class_distribution():
    """
    Issues by Darajah (class) and gender for the current academic window (AY).
    """
    start, end = _ay_bounds()
    if not start:
        return [], [], []

    conn = get_koha_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            COALESCE(std.attribute, b.branchcode, 'Unknown') AS class_name,
            UPPER(COALESCE(b.sex, '')) AS sex,
            COUNT(*) AS cnt
        FROM statistics s
        JOIN borrowers b ON b.borrowernumber = s.borrowernumber
        LEFT JOIN borrower_attributes std
            ON std.borrowernumber = b.borrowernumber
            AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
        WHERE s.type='issue'
          AND DATE(s.`datetime`) BETWEEN %s AND %s
          AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
          AND (b.debarred IS NULL OR b.debarred = 0)
          AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
        GROUP BY class_name, sex;
        """,
        (start, end),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    from re import search

    agg = {}
    for class_name, sex, cnt in rows:
        cname = (class_name or "Unknown").strip()
        if cname.upper() == "AJSN":
            cname = "Asateza"
        agg.setdefault(cname, {"M": 0, "F": 0})
        if sex in ("M", "F"):
            agg[cname][sex] += int(cnt)

    def class_sort_key(name: str):
        m = search(r"\d+", str(name))
        return (0, int(m.group(0))) if m else (1, 10**9)

    classes_sorted = sorted(agg.keys(), key=class_sort_key)
    labels = classes_sorted
    male_values = [agg[c]["M"] for c in classes_sorted]
    female_values = [agg[c]["F"] for c in classes_sorted]
    return labels, male_values, female_values


def get_class_summary(class_name=None):
    """
    AY-scoped class / Darajah summary.
    """
    start, end = _ay_bounds()
    if not start:
        return []

    conn = get_koha_conn()
    cur = conn.cursor(dictionary=True)
    query = """
    SELECT
      COALESCE(std.attribute, b.branchcode, 'Unknown') AS ClassName,
      COUNT(*) AS BooksIssued,
      COUNT(DISTINCT s.borrowernumber) AS ActivePatrons,
      GROUP_CONCAT(DISTINCT it.ccode ORDER BY it.ccode SEPARATOR ', ') AS Collections
    FROM statistics s
    JOIN borrowers b ON b.borrowernumber = s.borrowernumber
    LEFT JOIN borrower_attributes std
         ON std.borrowernumber = b.borrowernumber
        AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
    JOIN items it ON s.itemnumber = it.itemnumber
    WHERE s.type = 'issue'
      AND DATE(s.`datetime`) BETWEEN %s AND %s
      AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
      AND (b.debarred IS NULL OR b.debarred = 0)
      AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
    """
    params = [start, end]

    if class_name:
        query += " AND std.attribute = %s"
        params.append(class_name)

    query += " GROUP BY ClassName ORDER BY BooksIssued DESC;"
    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    results = []
    for rec in rows:
        cname_clean = (rec.get("ClassName") or "").strip()
        if cname_clean.upper() == "AJSN":
            cname_clean = "Asateza"
        rec["ClassName"] = cname_clean or "Unknown"

        patrons = rec.get("ActivePatrons") or 0
        issues = rec.get("BooksIssued") or 0
        rec["IssuesPerPatron"] = (
            round(float(issues) / float(patrons), 2) if patrons else 0.0
        )
        rec["Collections"] = rec.get("Collections") or "—"

        results.append(rec)

    return results


# ------------- WRAPPERS for existing KQ helpers -------------
def get_department_summary(selected_dept=None):
    """
    Wrapper to fetch department summary.
    """
    if hasattr(KQ, "get_department_summary"):
        try:
            return KQ.get_department_summary(selected_dept)
        except Exception:
            return []
    return []


def get_marhala_distribution():
    """
    Wrapper to fetch Marhala-wise distribution.
    """
    if hasattr(KQ, "get_marhala_distribution"):
        try:
            return KQ.get_marhala_distribution()
        except Exception:
            return [], []
    return [], []


def get_language_top25():
    """
    Wrapper to fetch top 25 titles by language.
    """
    if hasattr(KQ, "get_language_top25"):
        try:
            return KQ.get_language_top25()
        except Exception:
            pass

    return {
        "arabic": {
            "titles": [],
            "counts": [],
            "records": [],
        },
        "english": {
            "titles": [],
            "counts": [],
            "records": [],
        },
    }


def get_top_darajah_summary():
    """
    Wrapper to fetch top Darajah summary.
    """
    if hasattr(KQ, "get_top_darajah_summary"):
        try:
            return KQ.get_top_darajah_summary()
        except Exception:
            return []
    return []


# ---------------- ROUTES ----------------
@bp.route("/", methods=["GET", "POST"])
def dashboard():
    # 🔐 Require login via auth_bp (new system)
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    # 🔐 Require admin role for this dashboard
    role = (session.get("role") or "").lower()
    if role != "admin":
        flash("You must be logged in as an admin to view the Admin Analytics.", "danger")
        return redirect(url_for("auth_bp.login"))

    # --- Handle selection / impersonation for Toggle View ---
    if request.method == "POST":
        selected_class_form = (request.form.get("classSelector") or "").strip()
        selected_dept_form = (request.form.get("departmentSelector") or "").strip()

        if selected_class_form:
            session["class_name"] = selected_class_form

        if selected_dept_form:
            session["department_name"] = selected_dept_form

    # Effective selected class/department
    selected_class = (
        (request.form.get("classSelector") or "").strip()
        or (session.get("class_name") or "").strip()
        or None
    )
    selected_dept = (
        (request.form.get("departmentSelector") or "").strip()
        or (session.get("department_name") or "").strip()
        or None
    )

    # Get AY period for display
    ay_start, ay_end = _ay_bounds()
    ay_period = ""
    if ay_start and ay_end:
        ay_period = f"{get_hijri_date_label(ay_start)} to {get_hijri_date_label(ay_end)}"

    # Fetch data
    kpi_data = get_kpis()
    today_checkouts, today_checkins = get_today_activity()
    trend_labels, trend_values = get_trends()
    class_labels, class_male_values, class_female_values = get_class_distribution()
    marhala_labels, marhala_values = get_marhala_distribution()
    lang_top = get_language_top25()
    top_darajah_rows = get_top_darajah_summary()
    class_summary_rows = get_class_summary(selected_class)
    department_summary_rows = get_department_summary(selected_dept)
    
    # Verification data
    verification_data = KQ.verify_patron_counts() if hasattr(KQ, "verify_patron_counts") else {}

    hijri_today = get_hijri_today()

    # KPI cards – enhanced with more metrics
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
            "key": "total_fines",
            "label": "Fines Paid (AY)",
            "value": f"KSH{kpi_data['total_fines']:,.2f}",
            "icon": "fa-coins",
            "color": "warning",
            "subtext": f"{kpi_data['overdue']:,} currently overdue"
        },
        {
            "key": "today_activity",
            "label": "Today's Activity",
            "value": f"{today_checkouts} / {today_checkins}",
            "icon": "fa-calendar-day",
            "color": "info",
            "subtext": f"Checkouts / Checkins"
        },
    ]

    # Simple insight bullets for admin
    insights = []
    if verification_data and "active_patrons" in verification_data:
        insights.append(
            f"Database contains {verification_data['active_patrons']:,} active patrons "
            f"({verification_data['expired_patrons']:,} expired, {verification_data['debarred_patrons']:,} debarred)."
        )
    
    if department_summary_rows:
        top_dept = department_summary_rows[0]
        insights.append(
            f"Highest-issuing department this AY: {top_dept['Department']} "
            f"({top_dept['BooksIssued']:,} issues, {top_dept['ActivePatrons']:,} active patrons)."
        )
    
    if class_summary_rows:
        top_class = class_summary_rows[0]
        insights.append(
            f"Top Darajah/class this AY: {top_class['ClassName']} "
            f"with {top_class['BooksIssued']:,} issues ({top_class['IssuesPerPatron']} issues per patron)."
        )
    
    if marhala_labels and marhala_values:
        total_marhala = sum(marhala_values)
        if total_marhala:
            idx = max(range(len(marhala_values)), key=lambda i: marhala_values[i])
            dominant = marhala_labels[idx] or "Unknown"
            pct = round((marhala_values[idx] / total_marhala) * 100, 1)
            insights.append(
                f"Most active Marhala this AY: {dominant} ({pct}% of all AY issues)."
            )

    return render_template(
        "dashboard.html",
        hijri_today=hijri_today,
        ay_period=ay_period,
        kpi_cards=kpi_cards,
        trend_labels=trend_labels,
        trend_values=trend_values,
        class_labels=class_labels,
        class_male_values=class_male_values,
        class_female_values=class_female_values,
        marhala_labels=marhala_labels,
        marhala_values=marhala_values,
        marhala_chart_title="Marhala-wise Issues (Academic Year)",
        marhala_legend_position="bottom",
        arabic_titles=lang_top["arabic"]["titles"],
        arabic_issue_counts=lang_top["arabic"]["counts"],
        arabic_top_records=lang_top["arabic"]["records"],
        english_titles=lang_top["english"]["titles"],
        english_issue_counts=lang_top["english"]["counts"],
        english_top_records=lang_top["english"]["records"],
        top_darajah_rows=top_darajah_rows,
        department_summary_rows=department_summary_rows,
        class_summary_rows=class_summary_rows,
        insights=insights,
        selected_class=selected_class,
        selected_dept=selected_dept,
        verification_data=verification_data,
        parse_failed=False,
    )

# routes/dashboard.py (add this function to the existing file)
from services.koha_queries import get_all_classes

@bp.route("/class-explorer")
def class_explorer():
    """Admin-only page to explore and select any class."""
    if not session.get("logged_in"):
        return redirect(url_for("auth_bp.login"))

    role = (session.get("role") or "").lower()
    if role != "admin":
        flash("You must be logged in as an admin to access the Class Explorer.", "danger")
        return redirect(url_for("auth_bp.login"))

    # Get all available classes from Koha
    classes = get_all_classes()
    
    # Group classes by year/level for better organization
    classes_by_year = {}
    for class_info in classes:
        # Extract year/level from class name (e.g., "5 B M" → "5")
        class_name = class_info.get("class_name", "")
        match = re.match(r'^(\d+)', class_name)
        year = match.group(1) if match else "Other"
        
        if year not in classes_by_year:
            classes_by_year[year] = []
        
        classes_by_year[year].append(class_info)

    # Sort years numerically
    sorted_years = sorted(classes_by_year.keys(), key=lambda x: int(x) if x.isdigit() else 999)

    hijri_today = get_hijri_today()
    
    return render_template(
        "class_explorer.html",
        classes_by_year=classes_by_year,
        sorted_years=sorted_years,
        total_classes=len(classes),
        hijri_today=hijri_today,
    )