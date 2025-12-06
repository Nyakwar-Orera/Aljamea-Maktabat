# services/exports.py
import io
import os
import re
import pandas as pd
from datetime import date, datetime
from reportlab.platypus import (
    SimpleDocTemplate, LongTable, Table, TableStyle,
    Paragraph, Spacer, PageBreak, Image
)
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_RIGHT, TA_LEFT, TA_CENTER
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.units import cm
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader

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
# HIJRI DATE HELPERS (with short format)
# --------------------------------------------------------------------
def _hijri_date_short(d: date) -> str:
    """
    Convert Gregorian date to short Hijri format (DD-MM-YY H).
    Example: "05-01-47 H" for 5 Muharram 1447 H.
    """
    try:
        from hijri_converter import convert
        h = convert.Gregorian(d.year, d.month, d.day).to_hijri()
        return f"{h.day:02d}-{h.month:02d}-{str(h.year)[-2:]} H"
    except Exception:
        # Fallback to Gregorian short format
        return d.strftime("%d-%m-%y")


def _hijri_from_any(value) -> str:
    """
    Convert any date-like value (date, datetime, ISO string) to short Hijri date.
    Returns "-" for empty values.
    """
    if not value:
        return "-"
    try:
        if isinstance(value, datetime):
            g = value.date()
        elif isinstance(value, date):
            g = value
        else:
            s = str(value).split(" ")[0]
            g = datetime.strptime(s, "%Y-%m-%d").date()
        return _hijri_date_short(g)
    except Exception:
        return str(value)


# --------------------------------------------------------------------
# SAFE PDF EXPORT (no more LayoutError) - IMPROVED FOR LANDSCAPE
# --------------------------------------------------------------------
def dataframe_to_pdf_bytes(title: str, df: pd.DataFrame, force_landscape: bool = True) -> bytes:
    """
    Convert a Pandas DataFrame into styled PDF bytes.
    - Handles Arabic/English text
    - Smart width scaling
    - Auto splits large data into multiple pages
    - Force landscape mode for better detail capture
    - Never crashes with LayoutError
    """
    font_name = _ensure_font_registered()
    safe_df = df.copy() if df is not None else pd.DataFrame()
    if not safe_df.empty:
        safe_df = safe_df.fillna("")
        safe_df = _shape_df_for_rtl(safe_df)

    shaped_title = _shape_if_rtl(title or "")
    output = io.BytesIO()
    
    # Always use landscape for better detail capture
    pagesize = landscape(A4) if force_landscape else A4

    doc = SimpleDocTemplate(
        output, 
        pagesize=pagesize,
        leftMargin=2.0 * cm,  # Increased margins for landscape
        rightMargin=2.0 * cm,
        topMargin=3.0 * cm,   # More space for header
        bottomMargin=2.5 * cm # More space for footer
    )
    avail_width = doc.width

    styles = getSampleStyleSheet()
    for key in ("Title", "Normal", "Heading2", "Heading3"):
        styles[key].fontName = font_name

    # Create styles with better readability
    base_cell = ParagraphStyle(
        "Cell", 
        parent=styles["Normal"], 
        fontName=font_name,
        fontSize=9,  # Slightly larger for landscape
        leading=11,
        wordWrap="CJK", 
        alignment=TA_LEFT,
        spaceBefore=1,
        spaceAfter=1
    )
    ar_cell = ParagraphStyle("CellAR", parent=base_cell, alignment=TA_RIGHT)
    
    # Title style for landscape
    title_style = ParagraphStyle(
        "LandscapeTitle",
        parent=styles["Title"],
        fontName=font_name,
        fontSize=16,
        alignment=TA_CENTER,
        spaceAfter=16
    )

    elements = [Paragraph(shaped_title, title_style), Spacer(1, 12)]

    if safe_df.empty:
        elements.append(Paragraph(_shape_if_rtl("No data available"), styles["Normal"]))
        doc.build(elements)
        output.seek(0)
        return output.getvalue()

    headers = [str(c) for c in safe_df.columns]
    rows = safe_df.astype(str).values.tolist()
    data_str = [headers] + rows

    # Prefer wider "Titles_AY" or "Title" if present
    prefer_idx = None
    for i, h in enumerate(headers):
        h_lower = h.strip().lower()
        if h_lower in ["titles_ay", "title", "book_title", "titles"]:
            prefer_idx = i
            break
    
    col_widths = _auto_col_widths(data_str, font_name, 9, avail_width, prefer_wide_idx=prefer_idx)

    def make_table_chunk(chunk_rows):
        data = [[_paragraphize(h, base_cell, ar_cell) for h in headers]]
        for r in chunk_rows:
            data.append([_paragraphize(v, base_cell, ar_cell) for v in r])
        
        # Use LongTable for better splitting
        table = LongTable(data, colWidths=col_widths, repeatRows=1, hAlign="LEFT", splitByRow=1)
        
        # Improved table style for landscape
        style = TableStyle([
            ("FONTNAME", (0, 0), (-1, -1), font_name),
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#003366")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
            ("ALIGN", (0, 0), (-1, 0), "CENTER"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),  # Center vertically
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
            ("TOPPADDING", (0, 0), (-1, 0), 8),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
            ("BOX", (0, 0), (-1, -1), 0.5, colors.black),
        ])
        
        # Alternate row colors for better readability
        for row_num in range(1, len(data)):
            bg = colors.HexColor("#f8f9fa") if row_num % 2 == 0 else colors.HexColor("#e9ecef")
            style.add("BACKGROUND", (0, row_num), (-1, row_num), bg)
        
        table.setStyle(style)
        return table

    # Build in chunks (no LayoutError)
    chunk_size = 100  # More rows per page in landscape
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


# --------------------------------------------------------------------
# IMPROVED: helper to create a landscape DocTemplate with header + page numbers
# --------------------------------------------------------------------
def make_doc_with_header_footer(
    buffer: io.BytesIO,
    title: str,
    landscape_mode: bool = True,
    left_margin: float = 1.5 * cm,
    right_margin: float = 1.5 * cm,
    top_margin: float = 2.0 * cm,  # Increased for header
    bottom_margin: float = 1.8 * cm,  # Increased for footer
    show_date: bool = True,
    logo_path: str = None
):
    """
    Create a SimpleDocTemplate and a page callback that:
      - draws the (shaped) title in the header (centered)
      - draws Hijri date if show_date=True
      - draws page numbers in the footer
      - optionally includes logo
      - uses landscape A4 by default for better detail capture
    
    Returns (doc, on_page_callback).
    """
    font_name = _ensure_font_registered()
    shaped_title = _shape_if_rtl(title or "")
    pagesize = landscape(A4) if landscape_mode else A4

    doc = SimpleDocTemplate(
        buffer,
        pagesize=pagesize,
        leftMargin=left_margin,
        rightMargin=right_margin,
        topMargin=top_margin,
        bottomMargin=bottom_margin,
    )

    def _on_page(canvas, doc_):
        canvas.saveState()
        canvas.setFont(font_name, 9)
        
        # Get Hijri date for header
        hijri_date = ""
        if show_date:
            try:
                hijri_date = _hijri_date_short(date.today())
            except Exception:
                hijri_date = date.today().strftime("%d-%b-%Y")
        
        # Draw header line
        canvas.setLineWidth(0.5)
        canvas.line(
            doc_.leftMargin,
            doc_.height + doc_.topMargin - 15,
            doc_.width + doc_.leftMargin,
            doc_.height + doc_.topMargin - 15
        )
        
        # Draw title (centered at top)
        canvas.setFont(font_name, 10)
        canvas.drawCentredString(
            doc_.width / 2 + doc_.leftMargin,
            doc_.height + doc_.topMargin - 12,
            shaped_title,
        )
        
        # Draw date on right side
        if hijri_date:
            canvas.setFont(font_name, 8)
            canvas.drawRightString(
                doc_.width + doc_.leftMargin,
                doc_.height + doc_.topMargin - 12,
                hijri_date,
            )
        
        # Draw logo if provided
        if logo_path and os.path.exists(logo_path):
            try:
                img = ImageReader(logo_path)
                img_width, img_height = img.getSize()
                aspect = img_height / float(img_width)
                display_width = 2.5 * cm
                display_height = display_width * aspect
                
                canvas.drawImage(
                    logo_path,
                    doc_.leftMargin,
                    doc_.height + doc_.topMargin - display_height - 5,
                    width=display_width,
                    height=display_height,
                    mask='auto'
                )
            except Exception:
                pass  # Skip logo if there's an error
        
        # Draw footer line
        canvas.setLineWidth(0.5)
        canvas.line(
            doc_.leftMargin,
            doc_.bottomMargin + 15,
            doc_.width + doc_.leftMargin,
            doc_.bottomMargin + 15
        )
        
        # Draw page number at bottom center
        canvas.setFont(font_name, 8)
        page_num = canvas.getPageNumber()
        canvas.drawCentredString(
            doc_.width / 2 + doc_.leftMargin,
            doc_.bottomMargin - 5,
            f"Page {page_num}",
        )
        
        # Draw timestamp on left footer
        timestamp = datetime.now().strftime("%d-%b-%Y %H:%M")
        canvas.drawString(
            doc_.leftMargin,
            doc_.bottomMargin - 5,
            timestamp,
        )
        
        canvas.restoreState()

    return doc, _on_page


# --------------------------------------------------------------------
# NEW: Function to create multi-section landscape reports
# --------------------------------------------------------------------
def create_landscape_report(
    buffer: io.BytesIO,
    title: str,
    sections: list[dict],
    logo_path: str = None
) -> bytes:
    """
    Create a comprehensive landscape report with multiple sections.
    
    Args:
        buffer: BytesIO buffer to write PDF to
        title: Main report title
        sections: List of section dictionaries, each with:
            - 'title': Section title
            - 'data': DataFrame or list of dicts
            - 'columns': Optional column names (if data is list of dicts)
            - 'table_style': Optional custom TableStyle
        logo_path: Optional path to logo image
    
    Returns: PDF bytes
    """
    font_name = _ensure_font_registered()
    
    # Create document with header/footer
    doc, on_page = make_doc_with_header_footer(
        buffer=buffer,
        title=title,
        landscape_mode=True,
        left_margin=2.0 * cm,
        right_margin=2.0 * cm,
        top_margin=2.5 * cm,
        bottom_margin=2.0 * cm,
        show_date=True,
        logo_path=logo_path
    )
    
    styles = getSampleStyleSheet()
    for key in ("Title", "Normal", "Heading1", "Heading2", "Heading3"):
        styles[key].fontName = font_name
    
    # Create custom styles for landscape
    main_title_style = ParagraphStyle(
        "MainTitle",
        parent=styles["Title"],
        fontSize=18,
        alignment=TA_CENTER,
        spaceAfter=20
    )
    
    section_title_style = ParagraphStyle(
        "SectionTitle",
        parent=styles["Heading1"],
        fontSize=14,
        spaceBefore=20,
        spaceAfter=10
    )
    
    base_cell = ParagraphStyle(
        "Cell",
        parent=styles["Normal"],
        fontName=font_name,
        fontSize=9,
        leading=11,
        wordWrap="CJK",
        alignment=TA_LEFT
    )
    
    ar_cell = ParagraphStyle("CellAR", parent=base_cell, alignment=TA_RIGHT)
    
    elements = []
    
    # Add main title
    elements.append(Paragraph(_shape_if_rtl(title), main_title_style))
    elements.append(Spacer(1, 0.5 * cm))
    
    # Add current Hijri date
    try:
        hijri_date = _hijri_date_short(date.today())
        date_para = Paragraph(f"Hijri Date: {hijri_date}", styles["Normal"])
        elements.append(date_para)
        elements.append(Spacer(1, 1 * cm))
    except Exception:
        pass
    
    # Process each section
    for i, section in enumerate(sections):
        section_title = section.get('title', f'Section {i+1}')
        section_data = section.get('data')
        custom_columns = section.get('columns')
        custom_style = section.get('table_style')
        
        # Add section title
        elements.append(Paragraph(_shape_if_rtl(section_title), section_title_style))
        elements.append(Spacer(1, 0.5 * cm))
        
        if section_data is None:
            elements.append(Paragraph(_shape_if_rtl("No data available"), styles["Normal"]))
            elements.append(Spacer(1, 1 * cm))
            continue
        
        # Convert data to DataFrame if needed
        if isinstance(section_data, list) and section_data and isinstance(section_data[0], dict):
            df = pd.DataFrame(section_data)
            if custom_columns:
                df = df[custom_columns] if set(custom_columns).issubset(df.columns) else df
        elif isinstance(section_data, pd.DataFrame):
            df = section_data
            if custom_columns:
                df = df[custom_columns] if set(custom_columns).issubset(df.columns) else df
        else:
            elements.append(Paragraph(_shape_if_rtl("Invalid data format"), styles["Normal"]))
            continue
        
        # Clean and shape data
        if not df.empty:
            df = df.fillna("")
            df = _shape_df_for_rtl(df)
        
        if df.empty:
            elements.append(Paragraph(_shape_if_rtl("No data available"), styles["Normal"]))
            elements.append(Spacer(1, 1 * cm))
            continue
        
        # Prepare table data
        headers = [str(c) for c in df.columns]
        rows = df.astype(str).values.tolist()
        
        # Calculate column widths
        data_str = [headers] + rows
        col_widths = _auto_col_widths(data_str, font_name, 9, doc.width)
        
        # Create table data with paragraphs
        table_data = []
        table_data.append([_paragraphize(h, base_cell, ar_cell) for h in headers])
        
        for row in rows:
            table_data.append([_paragraphize(v, base_cell, ar_cell) for v in row])
        
        # Create table
        table = Table(table_data, colWidths=col_widths, repeatRows=1)
        
        # Apply table style
        if custom_style:
            table.setStyle(custom_style)
        else:
            # Default table style for landscape
            style = TableStyle([
                ("FONTNAME", (0, 0), (-1, -1), font_name),
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#003366")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                ("ALIGN", (0, 0), (-1, 0), "CENTER"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("BOX", (0, 0), (-1, -1), 1, colors.black),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ])
            
            # Alternate row colors
            for row_num in range(1, len(table_data)):
                bg = colors.HexColor("#f8f9fa") if row_num % 2 == 0 else colors.HexColor("#e9ecef")
                style.add("BACKGROUND", (0, row_num), (-1, row_num), bg)
            
            table.setStyle(style)
        
        elements.append(table)
        
        # Add spacing after table, except for last section
        if i < len(sections) - 1:
            elements.append(Spacer(1, 1 * cm))
    
    # Build document
    doc.build(elements, onFirstPage=on_page, onLaterPages=on_page)
    buffer.seek(0)
    return buffer.getvalue()