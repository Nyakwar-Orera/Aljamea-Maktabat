# services/exports.py
import io
import os
import re
import pandas as pd
from reportlab.platypus import (
    SimpleDocTemplate, LongTable, Table, TableStyle,
    Paragraph, Spacer, PageBreak
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
    os.path.expanduser(r"~\\AppData\\Local\\Microsoft\\Windows\\Fonts\\DejaVuSans.ttf"),
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

    REGISTERED_FONT_NAME = "Helvetica"  # last resort fallback
    return REGISTERED_FONT_NAME


def _shape_if_rtl(text: str) -> str:
    """If text contains Arabic, optionally reshape + apply bidi so it joins correctly."""
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
    """Compute column widths based on content, scaled to page width."""
    num_cols = len(data[0])
    max_w = [0.0] * num_cols
    sample_rows = data[:1] + data[1:301]
    pad = 12
    for row in sample_rows:
        for i, cell in enumerate(row):
            text = str(cell or "")
            w = pdfmetrics.stringWidth(text, font_name, font_size) + pad
            if w > max_w[i]:
                max_w[i] = w
    MIN_W, MAX_W = 40, 240
    MAX_WS = [MAX_W] * num_cols
    if prefer_wide_idx is not None and 0 <= prefer_wide_idx < num_cols:
        MAX_WS[prefer_wide_idx] = 320
    raw = [max(MIN_W, min(MAX_WS[i], max_w[i] or MIN_W)) for i in range(num_cols)]
    total = sum(raw) or 1.0
    scale = avail_width / total
    return [w * scale for w in raw]


# --------------------------------------------------------------------
# SAFE PDF EXPORT (no more LayoutError)
# --------------------------------------------------------------------
def dataframe_to_pdf_bytes(title: str, df: pd.DataFrame) -> bytes:
    """
    Convert a Pandas DataFrame into styled PDF bytes.
    - Handles Arabic/English text
    - Smart width scaling
    - Auto splits large data into multiple pages
    - Never crashes with LayoutError
    """
    font_name = _ensure_font_registered()
    safe_df = df.copy() if df is not None else pd.DataFrame()
    if not safe_df.empty:
        safe_df = safe_df.fillna("")
        safe_df = _shape_df_for_rtl(safe_df)

    shaped_title = _shape_if_rtl(title or "")
    output = io.BytesIO()
    wide_table = (not safe_df.empty and safe_df.shape[1] > 8)
    pagesize = landscape(A4) if wide_table else A4

    doc = SimpleDocTemplate(output, pagesize=pagesize,
                            leftMargin=24, rightMargin=24,
                            topMargin=28, bottomMargin=24)
    avail_width = doc.width

    styles = getSampleStyleSheet()
    for key in ("Title", "Normal", "Heading2", "Heading3"):
        styles[key].fontName = font_name

    base_cell = ParagraphStyle(
        "Cell", parent=styles["Normal"], fontName=font_name,
        fontSize=8, leading=10, wordWrap="CJK", alignment=TA_LEFT
    )
    ar_cell = ParagraphStyle("CellAR", parent=base_cell, alignment=TA_RIGHT)

    elements = [Paragraph(shaped_title, styles["Title"]), Spacer(1, 12)]

    if safe_df.empty:
        elements.append(Paragraph(_shape_if_rtl("No data available"), styles["Normal"]))
        doc.build(elements)
        output.seek(0)
        return output.getvalue()

    headers = [str(c) for c in safe_df.columns]
    rows = safe_df.astype(str).values.tolist()
    data_str = [headers] + rows

    # Prefer wider "Titles_AY" if present
    prefer_idx = next((i for i, h in enumerate(headers)
                       if h.strip().lower() == "titles_ay"), None)
    col_widths = _auto_col_widths(data_str, font_name, 8, avail_width, prefer_wide_idx=prefer_idx)

    def make_table_chunk(chunk_rows):
        data = [[_paragraphize(h, base_cell, ar_cell) for h in headers]]
        for r in chunk_rows:
            data.append([_paragraphize(v, base_cell, ar_cell) for v in r])
        table = LongTable(data, colWidths=col_widths, repeatRows=1, hAlign="LEFT", splitByRow=1)
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
        for row_num in range(1, len(data)):
            bg = colors.whitesmoke if row_num % 2 == 0 else colors.lightgrey
            style.add("BACKGROUND", (0, row_num), (-1, row_num), bg)
        table.setStyle(style)
        return table

    # Build in chunks (no LayoutError)
    chunk_size = 80
    try:
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            elements.append(make_table_chunk(chunk))
            elements.append(Spacer(1, 12))
            if i + chunk_size < len(rows):
                elements.append(PageBreak())
    except Exception as e:
        elements = [
            Paragraph(_shape_if_rtl("⚠️ Error generating table:"), styles["Normal"]),
            Paragraph(_shape_if_rtl(str(e)), styles["Normal"]),
            Paragraph(_shape_if_rtl("Partial data shown below:"), styles["Normal"]),
        ]
        for i in range(min(50, len(rows))):
            row_text = " | ".join(map(str, rows[i]))
            elements.append(Paragraph(_shape_if_rtl(row_text), styles["Normal"]))

    try:
        doc.build(elements)
    except Exception as e:
        elements = [
            Paragraph(_shape_if_rtl("Report generation failed due to layout limits."), styles["Normal"]),
            Paragraph(_shape_if_rtl(str(e)), styles["Normal"]),
        ]
        doc.build(elements)

    output.seek(0)
    return output.getvalue()
