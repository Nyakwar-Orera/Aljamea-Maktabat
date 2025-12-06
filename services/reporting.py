from typing import Dict, Any, List, Tuple
import pandas as pd
from services import koha_queries as KQ
from services.exports import dataframe_to_pdf_bytes

# -------------------------
# Dashboard Payload
# -------------------------
def dashboard_payload() -> Dict[str, Any]:
    """
    Build dashboard payload for legacy/other views (AY-based).

    - KQ.get_summary() is AY-scoped.
    - 'active_patrons' = DISTINCT borrowers with ≥1 issue in AY.
    """
    s = KQ.get_summary()

    # Prefer active_patrons if provided, fall back to total_patrons.
    active_patrons = s.get("active_patrons", s.get("total_patrons", 0))

    # class chart (AY-scoped inside koha_queries)
    class_rows = KQ.class_issues()
    class_labels = [r[0] for r in class_rows]
    class_values = [int(r[1]) for r in class_rows]

    # dept chart (AY-scoped)
    dept_rows = KQ.departments_breakdown()
    dept_labels = [r[0] for r in dept_rows]
    dept_values = [int(r[1]) for r in dept_rows]

    # trends (AY monthly, from koha_queries.borrowing_trend_monthly)
    trend_rows = KQ.borrowing_trend_monthly()
    trend_labels = [ym for ym, _ in trend_rows]
    trend_values = [int(cnt) for _, cnt in trend_rows]

    # top titles (All + Arabic via title REGEXP; Non-Arabic = All minus Arabic)
    top_all_rows: List[Tuple] = KQ.top_titles(limit=25, arabic=False, non_arabic=False)
    top_ar_rows: List[Tuple]  = KQ.top_titles(limit=25, arabic=True, non_arabic=False)

    # normalize to dicts for easy subtraction
    def to_map(rows):
        return {
            t: {"count": int(c), "last": str(d) if d is not None else ""}
            for t, c, d in rows
        }

    all_map = to_map(top_all_rows)
    ar_map  = to_map(top_ar_rows)

    # Non-Arabic = titles in ALL that are not in AR
    non_ar_map = {t: v for t, v in all_map.items() if t not in ar_map}
    # Take top 25 of non_ar by count
    non_ar_sorted = sorted(
        non_ar_map.items(),
        key=lambda kv: kv[1]["count"],
        reverse=True
    )[:25]

    # unpack lists for template
    top_all_labels  = [t for t, _, _ in top_all_rows]
    top_all_values  = [int(c) for _, c, _ in top_all_rows]
    top_all_dates   = [str(d) if d is not None else "" for _, _, d in top_all_rows]

    top_ar_labels   = [t for t, _, _ in top_ar_rows]
    top_ar_values   = [int(c) for _, c, _ in top_ar_rows]
    top_ar_dates    = [str(d) if d is not None else "" for _, _, d in top_ar_rows]

    top_non_ar_labels = [t for t, _ in non_ar_sorted]
    top_non_ar_values = [v["count"] for _, v in non_ar_sorted]
    top_non_ar_dates  = [v["last"]  for _, v in non_ar_sorted]

    # today's activity (checkouts/returns) – per day, not AY
    today_checkouts, today_checkins = KQ.today_activity()

    return dict(
        parse_failed=False,
        # Semantics: this is ACTIVE patrons in AY
        total_patrons=active_patrons,
        total_issues=s.get("total_issues", 0),
        total_fines=s.get("fines_paid", 0.0),
        total_titles_issued=s.get("total_titles_issued", 0),
        today_checkouts=today_checkouts,
        today_checkins=today_checkins,
        class_labels=class_labels,
        class_values=class_values,
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
        trend_labels=trend_labels,
        trend_values=trend_values,
    )

# -------------------------
# Individual Student Payload
# -------------------------
def individual_payload(identifier: str) -> dict | None:
    student = KQ.find_student_by_identifier(identifier)
    if not student:
        return None

    borrowed = KQ.borrowed_books_for(student["borrowernumber"])

    # Normalize active vs returned flags
    def norm_returned(x):
        # x can be 0/1 or Yes/No depending on source; coerce to bool
        if isinstance(x, str):
            return x.lower().startswith("y")
        return bool(x)

    info = {
        "borrowernumber": student["borrowernumber"],
        "cardnumber": student.get("cardnumber"),
        "FullName": f"{student.get('surname','')} {student.get('firstname','')}".strip() or None,
        "EduEmail": student.get("email"),
        "ITS ID": student.get("userid"),
        "Patron Category Code": student.get("categorycode"),
        "Patron Category": student.get("category"),
        "Class": student.get("class"),
        "TR Number": student.get("userid"),  # adjust if you store TRNO separately in attributes
        "Total Issues": len([b for b in borrowed if not norm_returned(b.get("returned"))]),
        "Total Fines Paid": 0,  # optional: query accountlines if needed
        "BorrowedBooks": [
            {
                "title": b.get("title"),
                "date_issued": str(b.get("date_issued") or "")[:19],
                "date_due": str(b.get("date_due") or "")[:19],
                "returned": norm_returned(b.get("returned")),
            }
            for b in borrowed
        ],
    }
    return info

# -------------------------
# PDF Export Helpers
# -------------------------
def export_class_pdf(class_name: str) -> bytes | None:
    """Generate PDF report for a specific class."""
    rows = KQ.class_dataframe(class_name)
    if not rows:
        return None
    df = pd.DataFrame(rows)
    return dataframe_to_pdf_bytes(f"Class Report - {class_name}", df)

def export_department_pdf(dept: str) -> bytes | None:
    """Generate PDF report for a specific department."""
    rows = KQ.department_dataframe(dept)
    if not rows:
        return None
    df = pd.DataFrame(rows)
    return dataframe_to_pdf_bytes(f"Department Report - {dept}", df)
