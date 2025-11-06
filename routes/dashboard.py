# routes/dashboard.py
from flask import Blueprint, render_template, session, redirect, url_for
from db_koha import get_koha_conn
from datetime import date, datetime

bp = Blueprint("dashboard_bp", __name__)

# ---------------- AY WINDOW ----------------
def _ay_bounds():
    """Return (start_date, end_date) for current academic window Apr 1 -> today (max Dec 31)."""
    today = date.today()
    year = today.year
    if today.month < 4:
        # before April, the current AY hasn't started—return an empty window
        return None, None
    start = date(year, 4, 1)
    end = min(today, date(year, 12, 31))
    return start, end


# ---------------- UTILITIES ----------------
def get_kpis():
    """
    Fetch core KPIs from Koha DB for the CURRENT ACADEMIC YEAR window:
      - Total Patrons: total number of borrowers in the Koha database
      - Total Issues: number of 'issue' rows in AY (from statistics)
      - Total Fines Paid: sum of payments in AY (from accountlines)
    """
    start, end = _ay_bounds()
    conn = get_koha_conn()
    cur = conn.cursor()

    # 🧍 Total Patrons (all borrowers)
    cur.execute("SELECT COUNT(*) FROM borrowers;")
    total_patrons = int(cur.fetchone()[0] or 0)

    if not start:
        # AY not started yet → show lifetime patrons, zero issues/fines
        conn.close()
        return total_patrons, 0, 0.0

    # 📚 Total Issues in AY
    cur.execute("""
        SELECT COUNT(*)
        FROM statistics
        WHERE type='issue'
          AND DATE(`datetime`) BETWEEN %s AND %s
    """, (start, end))
    total_issues = int(cur.fetchone()[0] or 0)

    # 💰 Total Fines Paid in AY (payments are stored as negative amounts; flip sign)
    cur.execute("""
        SELECT COALESCE(SUM(
                 CASE
                   WHEN credit_type_code='PAYMENT'
                        AND (status IS NULL OR status <> 'VOID')
                        AND DATE(`date`) BETWEEN %s AND %s
                   THEN -amount
                   ELSE 0
                 END
               ), 0)
        FROM accountlines
    """, (start, end))
    total_fines = float(cur.fetchone()[0] or 0.0)

    conn.close()
    return total_patrons, total_issues, total_fines


def get_today_activity():
    """Return (today_checkouts, today_checkins) from statistics table."""
    conn = get_koha_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT
          SUM(CASE WHEN type='issue'  THEN 1 ELSE 0 END) AS checkouts,
          SUM(CASE WHEN type='return' THEN 1 ELSE 0 END) AS checkins
        FROM statistics
        WHERE DATE(`datetime`) = CURDATE();
    """)
    row = cur.fetchone() or (0, 0)
    conn.close()
    return int(row[0] or 0), int(row[1] or 0)


def get_trends():
    """
    Borrowing trends for the *current* academic window:
    April → current month of the current calendar year, from STATISTICS (type='issue').
    """
    start, end = _ay_bounds()
    if not start:
        return [], []

    conn = get_koha_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT DATE_FORMAT(`datetime`, '%Y-%m') AS ym, COUNT(*) AS cnt
        FROM statistics
        WHERE `type`='issue'
          AND DATE(`datetime`) BETWEEN %s AND %s
        GROUP BY ym
        ORDER BY ym ASC;
    """, (start, end))
    rows = cur.fetchall()
    conn.close()

    # Build continuous month list from April -> current month, no future months
    months = []
    m = date(start.year, 4, 1)
    while m <= end.replace(day=1):
        months.append(m.strftime("%Y-%m"))
        if m.month == 12:
            break
        m = date(m.year, m.month + 1, 1)

    by_month = {ym: int(c) for ym, c in rows}
    labels = months
    values = [by_month.get(ym, 0) for ym in months]
    return labels, values


def get_class_distribution():
    """
    Issues by CLASS (Male vs Female) for the current academic window,
    using STATISTICS (type='issue'), grouped by borrower STD (or branchcode).
    """
    start, end = _ay_bounds()
    if not start:
        return [], [], []

    conn = get_koha_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            COALESCE(std.attribute, b.branchcode, 'Unknown') AS class_name,
            UPPER(COALESCE(b.sex, ''))                       AS sex,
            COUNT(*)                                         AS cnt
        FROM statistics s
        JOIN borrowers b       ON b.borrowernumber = s.borrowernumber
        LEFT JOIN borrower_attributes std
               ON std.borrowernumber = b.borrowernumber
              AND std.code IN ('STD','CLASS','DAR','CLASS_STD')
        WHERE s.type='issue'
          AND DATE(s.`datetime`) BETWEEN %s AND %s
        GROUP BY class_name, sex;
    """, (start, end))
    rows = cur.fetchall()
    conn.close()

    from re import search
    agg = {}
    for class_name, sex, cnt in rows:
        cname = class_name or "Unknown"
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


def get_department_distribution():
    """
    Issues grouped by DEPARTMENT (Koha category) for current academic window,
    from STATISTICS (type='issue').
    """
    start, end = _ay_bounds()
    if not start:
        return [], []

    conn = get_koha_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT COALESCE(c.description, b.categorycode, 'Unknown') AS department,
               COUNT(*) AS cnt
        FROM statistics s
        JOIN borrowers b ON b.borrowernumber = s.borrowernumber
        LEFT JOIN categories c ON c.categorycode = b.categorycode
        WHERE s.type='issue'
          AND DATE(s.`datetime`) BETWEEN %s AND %s
        GROUP BY department
        ORDER BY cnt DESC;
    """, (start, end))
    rows = cur.fetchall()
    conn.close()

    labels = [r[0] for r in rows]
    values = [int(r[1]) for r in rows]
    return labels, values


def get_top_titles(limit=25, arabic=False, non_arabic=False):
    """Top borrowed titles (with optional Arabic / non-Arabic filter). Uses current academic window."""
    start, end = _ay_bounds()
    if not start:
        return [], [], []

    conn = get_koha_conn()
    cur = conn.cursor()

    lang_filter = ""
    if arabic:
        lang_filter = "AND biblio.title REGEXP '[ء-ي]'"
    elif non_arabic:
        lang_filter = "AND biblio.title NOT REGEXP '[ء-ي]'"

    cur.execute(f"""
        SELECT biblio.title, COUNT(*) AS cnt, MAX(DATE(s.`datetime`)) AS last_issued
        FROM statistics s
        JOIN items  USING (itemnumber)
        JOIN biblio USING (biblionumber)
        WHERE s.type='issue'
          AND DATE(s.`datetime`) BETWEEN %s AND %s
          {lang_filter}
        GROUP BY biblio.biblionumber
        ORDER BY cnt DESC
        LIMIT %s;
    """, (start, end, limit))
    rows = cur.fetchall()
    conn.close()

    labels = [r[0] for r in rows]
    values = [int(r[1]) for r in rows]
    dates  = [str(r[2]) for r in rows]
    return labels, values, dates


# ---------------- ROUTES ----------------
@bp.route("/")
def dashboard():
    if not session.get("logged_in"):
        return redirect(url_for("admin_bp.index"))

    # AY-based KPIs
    total_patrons, total_issues, total_fines = get_kpis()

    today_checkouts, today_checkins = get_today_activity()
    trend_labels, trend_values = get_trends()
    class_labels, class_male_values, class_female_values = get_class_distribution()
    dept_labels, dept_values = get_department_distribution()

    top_all_labels, top_all_values, top_all_dates = get_top_titles()
    top_ar_labels, top_ar_values, top_ar_dates = get_top_titles(arabic=True)
    top_non_ar_labels, top_non_ar_values, top_non_ar_dates = get_top_titles(non_arabic=True)

    return render_template(
        "dashboard.html",
        total_patrons=total_patrons,      # Total patrons (borrowers)
        total_issues=total_issues,        # AY issues count
        total_fines=total_fines,          # AY fines paid
        today_checkouts=today_checkouts,
        today_checkins=today_checkins,
        trend_labels=trend_labels,
        trend_values=trend_values,
        class_labels=class_labels,
        class_male_values=class_male_values,
        class_female_values=class_female_values,
        dept_labels=dept_labels,
        dept_values=dept_values,
        top_all_labels=top_all_labels,
        top_all_values=top_all_values,
        top_all_dates=top_all_dates,
        top_ar_labels=top_ar_labels,
        top_ar_values=top_ar_values,
        top_ar_dates=top_ar_dates,
        top_non_ar_labels=top_non_ar_labels,
        top_non_ar_values=top_non_ar_values,
        top_non_ar_dates=top_non_ar_dates,
        parse_failed=False
    )
