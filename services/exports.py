# services/exports.py
import io
import os
import re
import pandas as pd
from reportlab.platypus import (
    SimpleDocTemplate, LongTable, Table, TableStyle, Paragraph, Spacer
)
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_RIGHT, TA_LEFT
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# Try optional Arabic shaping (recommended)
try:
    import arabic_reshaper  # pip install arabic-reshaper
    from bidi.algorithm import get_display  # pip install python-bidi
    HAS_RTL_SHAPER = True
except Exception:
    HAS_RTL_SHAPER = False

ARABIC_RE = re.compile(r'[\u0600-\u06FF]')

# Candidate font files (first existing one is used)
FONT_CANDIDATES = [
    os.path.join("static", "fonts", "NotoNaskhArabic-Regular.ttf"),
    os.path.join("static", "fonts", "Amiri-Regular.ttf"),
    os.path.join("static", "fonts", "DejaVuSans.ttf"),
    "/usr/share/fonts/truetype/noto/NotoNaskhArabic-Regular.ttf",
    "/usr/share/fonts/truetype/amiri/Amiri-Regular.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    os.path.expanduser(r"~\AppData\Local\Microsoft\Windows\Fonts\DejaVuSans.ttf"),
]

REGISTERED_FONT_NAME = None  # set after registration


def _ensure_font_registered() -> str:
    """Register a Unicode TTF that supports Arabic; return its font name."""
    global REGISTERED_FONT_NAME
    if REGISTERED_FONT_NAME:
        return REGISTERED_FONT_NAME

    for path in FONT_CANDIDATES:
        if path and os.path.exists(path):
            try:
                font_name = os.path.splitext(os.path.basename(path))[0]
                pdfmetrics.registerFont(TTFont(font_name, path))
                REGISTERED_FONT_NAME = font_name
                return font_name
            except Exception:
                pass

    REGISTERED_FONT_NAME = "Helvetica"  # last-resort fallback
    return REGISTERED_FONT_NAME


def _shape_if_rtl(text: str) -> str:
    """
    If text contains Arabic, optionally reshape + apply bidi so it joins correctly.
    """
    if not text:
        return text
    if not ARABIC_RE.search(text):
        return text
    if HAS_RTL_SHAPER:
        try:
            reshaped = arabic_reshaper.reshape(text)
            return get_display(reshaped)
        except Exception:
            return text
    return text


def _shape_df_for_rtl(df: pd.DataFrame) -> pd.DataFrame:
    """Apply RTL shaping to cells and headers that contain Arabic characters."""
    if df is None or df.empty:
        return df
    shaped = df.copy()
    for col in shaped.columns:
        shaped[col] = shaped[col].map(lambda v: _shape_if_rtl(str(v)))
    shaped.columns = [_shape_if_rtl(str(c)) for c in shaped.columns]
    return shaped


def dataframe_to_excel_bytes(df: pd.DataFrame, sheet_name="Sheet1") -> bytes:
    """Convert a Pandas DataFrame into Excel bytes (XLSX)."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    output.seek(0)
    return output.getvalue()


def _is_arabic(text: str) -> bool:
    return bool(ARABIC_RE.search(text or ""))


def _paragraphize(value: str, base_style: ParagraphStyle, ar_style: ParagraphStyle) -> Paragraph:
    """Make a wrapping Paragraph for the cell, with RTL-aware alignment."""
    s = str(value) if value is not None else ""
    s = _shape_if_rtl(s)
    style = ar_style if _is_arabic(s) else base_style
    return Paragraph(s.replace("\n", "<br/>"), style)


def _auto_col_widths(data: list[list[str]], font_name: str, font_size: int, avail_width: float,
                     prefer_wide_idx: int | None = None) -> list[float]:
    """
    Compute column widths based on content (header + sample rows), then scale to fit avail_width.
    Long words get wrapped by Paragraph, but we still try to allocate a sensible width.
    """
    num_cols = len(data[0])
    # Measure using ReportLab's stringWidth (rough, but good enough for allocation)
    max_w = [0.0] * num_cols

    # Sample up to N rows to keep it fast
    sample_rows = data[:1] + data[1:301]

    pad = 12  # padding allowance per cell
    for row in sample_rows:
        for i, cell in enumerate(row):
            text = str(cell or "")
            w = pdfmetrics.stringWidth(text, font_name, font_size) + pad
            if w > max_w[i]:
                max_w[i] = w

    # Set min/max per column (pt)
    MIN_W = 40
    MAX_W = 240
    # Give the "Titles_AY" column (if present) extra max width
    if prefer_wide_idx is not None and 0 <= prefer_wide_idx < num_cols:
        MAX_WS = [MAX_W] * num_cols
        MAX_WS[prefer_wide_idx] = 320
    else:
        MAX_WS = [MAX_W] * num_cols

    raw = [max(MIN_W, min(MAX_WS[i], max_w[i] or MIN_W)) for i in range(num_cols)]
    total = sum(raw) or 1.0
    scale = avail_width / total
    return [w * scale for w in raw]


def dataframe_to_pdf_bytes(title: str, df: pd.DataFrame) -> bytes:
    """
    Convert a Pandas DataFrame into a nicely styled PDF bytes with:
    - Embedded Unicode font (Arabic-safe)
    - Wrapping cells via Paragraph (prevents clipping)
    - Smart column widths that fit the page
    - Proper table splitting across pages & repeating header
    - Automatic landscape when many columns
    """
    font_name = _ensure_font_registered()

    # Prepare/shaped data
    safe_df = df.copy() if df is not None else pd.DataFrame()
    if not safe_df.empty:
        safe_df = safe_df.fillna("")
        safe_df = _shape_df_for_rtl(safe_df)

    shaped_title = _shape_if_rtl(title or "")

    output = io.BytesIO()

    # Switch to landscape for wide tables (heuristic: > 8 columns)
    wide_table = (not safe_df.empty and safe_df.shape[1] > 8)
    pagesize = landscape(A4) if wide_table else A4

    doc = SimpleDocTemplate(
        output,
        pagesize=pagesize,
        leftMargin=24, rightMargin=24, topMargin=28, bottomMargin=24
    )
    avail_width = doc.width

    styles = getSampleStyleSheet()
    # Apply our font everywhere
    for key in ("Title", "Normal", "Heading2", "Heading3"):
        styles[key].fontName = font_name

    # Base cell styles (wrapping on)
    base_cell = ParagraphStyle(
        "Cell",
        parent=styles["Normal"],
        fontName=font_name,
        fontSize=8,
        leading=10,
        wordWrap="CJK",   # enables wrapping for long tokens (URLs, long titles)
        alignment=TA_LEFT # default LTR alignment
    )
    ar_cell = ParagraphStyle(
        "CellAR",
        parent=base_cell,
        alignment=TA_RIGHT  # Arabic lines align right for readability
    )

    elements = [
        Paragraph(shaped_title, styles["Title"]),
        Spacer(1, 12),
    ]

    if safe_df.empty:
        elements.append(Paragraph(_shape_if_rtl("No data"), styles["Normal"]))
        doc.build(elements)
        output.seek(0)
        return output.getvalue()

    # Build table data as Strings first (for width calc), then Paragraphs
    headers = [str(c) for c in safe_df.columns]
    rows = safe_df.astype(str).values.tolist()
    data_str = [headers] + rows

    # Find an index for Titles_AY if present (to prefer a wider column)
    prefer_idx = None
    for i, h in enumerate(headers):
        if h.strip().lower() == "titles_ay":
            prefer_idx = i
            break

    # Compute column widths to fill page width
    col_widths = _auto_col_widths(data_str, font_name, 8, avail_width, prefer_wide_idx=prefer_idx)

    # Now convert all cells to Paragraphs so they wrap (no clipping)
    data = []
    # Header row
    data.append([_paragraphize(h, base_cell, ar_cell) for h in headers])
    # Body rows
    for r in rows:
        data.append([_paragraphize(v, base_cell, ar_cell) for v in r])

    # Use LongTable to split across pages nicely
    table_cls = LongTable if len(data) > 40 else Table
    table = table_cls(data, colWidths=col_widths, repeatRows=1, hAlign="LEFT", splitByRow=1)

    # Styling
    style = TableStyle([
        ("FONTNAME", (0, 0), (-1, -1), font_name),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#003366")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.black),
    ])

    # Alternating row colors (body only)
    for row_num in range(1, len(data)):
        bg_color = colors.whitesmoke if row_num % 2 == 0 else colors.lightgrey
        style.add("BACKGROUND", (0, row_num), (-1, row_num), bg_color)

    table.setStyle(style)
    elements.append(table)

    doc.build(elements)
    output.seek(0)
    return output.getvalue()
