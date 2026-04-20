# services/exports.py - COMPREHENSIVE PDF EXPORT WITH LANDSCAPE DEFAULT
import io
import os
import re
import pandas as pd
from typing import Dict, List, Optional, Union, Tuple
from datetime import date, datetime
from reportlab.platypus import (
    SimpleDocTemplate, LongTable, Table, TableStyle,
    Paragraph, Spacer, PageBreak, Image, KeepTogether
)
from reportlab.lib.pagesizes import A4, landscape, portrait, letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_RIGHT, TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.units import cm, inch
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from reportlab.graphics.shapes import Drawing, Rect
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics.charts.piecharts import Pie
from reportlab.graphics.charts.legends import Legend
from reportlab.graphics import renderPDF

# Try optional Arabic shaping (recommended)
try:
    import arabic_reshaper  # pip install arabic-reshaper
    from bidi.algorithm import get_display  # pip install python-bidi
    HAS_RTL_SHAPER = True
except Exception:
    HAS_RTL_SHAPER = False

ARABIC_RE = re.compile(r'[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF]')

# ============================================================================
# FONT MANAGEMENT
# ============================================================================

# Candidate font files (first existing one is used)
FONT_CANDIDATES = [
    os.path.join("static", "fonts", "NotoNaskhArabic-Regular.ttf"),
    os.path.join("static", "fonts", "Amiri-Regular.ttf"),
    os.path.join("static", "fonts", "DejaVuSans.ttf"),
    os.path.join("static", "fonts", "NotoSansArabic-Regular.ttf"),
    "/usr/share/fonts/truetype/noto/NotoNaskhArabic-Regular.ttf",
    "/usr/share/fonts/truetype/amiri/Amiri-Regular.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/truetype/noto/NotoSansArabic-Regular.ttf",
    os.path.expanduser(r"~\\AppData\\Local\\Microsoft\\Windows\\Fonts\\DejaVuSans.ttf"),
]

REGISTERED_FONT_NAME = None


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
                print(f"✓ Using font: {font_name}")
                return font_name
            except Exception as e:
                print(f"✗ Failed to register font {path}: {e}")
                continue

    REGISTERED_FONT_NAME = "Helvetica"  # last resort fallback
    print(f"⚠️ Using fallback font: {REGISTERED_FONT_NAME}")
    return REGISTERED_FONT_NAME


# ============================================================================
# TEXT PROCESSING
# ============================================================================

def _shape_if_rtl(text: str) -> str:
    """If text contains Arabic, optionally reshape + apply bidi so it joins correctly."""
    if not text or not isinstance(text, str):
        return str(text) if text is not None else ""
    
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


def _is_arabic(text: str) -> bool:
    """Check if text contains Arabic characters."""
    return bool(ARABIC_RE.search(str(text) if text else ""))


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def _hijri_date_short(d: date) -> str:
    """Convert Gregorian date to short Hijri format (DD-MM-YY H)."""
    try:
        from hijri_converter import convert
        h = convert.Gregorian(d.year, d.month, d.day).to_hijri()
        return f"{h.day:02d}-{h.month:02d}-{str(h.year)[-2:]} H"
    except Exception:
        return d.strftime("%d-%m-%y")


def _hijri_from_any(value) -> str:
    """Convert any date-like value to short Hijri date."""
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


def _paragraphize(value: str, base_style: ParagraphStyle, ar_style: ParagraphStyle) -> Paragraph:
    """Make a wrapping Paragraph for the cell, with RTL-aware alignment."""
    s = str(value) if value is not None else ""
    s = _shape_if_rtl(s)
    style = ar_style if _is_arabic(s) else base_style
    return Paragraph(s.replace("\n", "<br/>"), style)


def _auto_col_widths(data: List[List[str]], font_name: str, font_size: int, 
                     avail_width: float, prefer_wide_idx: Optional[int] = None) -> List[float]:
    """Compute optimal column widths based on content."""
    num_cols = len(data[0]) if data else 0
    if num_cols == 0:
        return []
    
    max_w = [0.0] * num_cols
    sample_rows = data[:1] + data[1:min(100, len(data))]
    
    for row in sample_rows:
        for i, cell in enumerate(row[:num_cols]):
            text = str(cell or "")
            w = pdfmetrics.stringWidth(text, font_name, font_size) + 15  # padding
            if w > max_w[i]:
                max_w[i] = w
    
    # Set reasonable min/max widths
    MIN_W = 30
    MAX_WS = [200] * num_cols
    
    # Give more width to specific columns
    if prefer_wide_idx is not None and 0 <= prefer_wide_idx < num_cols:
        MAX_WS[prefer_wide_idx] = 350
    
    # Also prioritize common wide columns
    for i, _ in enumerate(max_w):
        if i < len(data[0]):
            col_name = str(data[0][i]).lower()
            if any(keyword in col_name for keyword in ['title', 'name', 'description', 'remarks']):
                MAX_WS[i] = 300
    
    raw = [max(MIN_W, min(MAX_WS[i], max_w[i] or MIN_W)) for i in range(num_cols)]
    total = sum(raw) or 1.0
    
    # Scale to fit available width
    scale = min(1.0, avail_width / total)
    return [w * scale for w in raw]


# ============================================================================
# EXCEL EXPORT
# ============================================================================

def dataframe_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Sheet1", 
                            additional_sheets: Optional[Dict[str, pd.DataFrame]] = None) -> bytes:
    """Convert DataFrame to Excel bytes."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
        
        if additional_sheets:
            for sheet, sheet_df in additional_sheets.items():
                sheet_df.to_excel(writer, index=False, sheet_name=sheet)
    
    output.seek(0)
    return output.getvalue()


# ============================================================================
# PDF EXPORT WITH LANDSCAPE DEFAULT
# ============================================================================

class PDFConfig:
    """Configuration for PDF generation."""
    
    # Default orientations for different report types
    DEFAULT_ORIENTATIONS = {
        'student_profile': 'portrait',      # Better for reading
        'student_report': 'landscape',      # Better for tables
        'darajah_report': 'landscape',      # Usually wide tables
        'marhala_report': 'landscape',      # Wide data
        'monthly_report': 'landscape',      # Timeline data
        'book_list': 'landscape',           # Book metadata
        'teacher_report': 'portrait',       # Less columns
        'statistics': 'landscape',          # Wide statistics
        'analytics': 'landscape',           # Charts and graphs
        'summary': 'portrait',              # Summary views
    }
    
    # Page margins (in cm)
    MARGINS = {
        'landscape': {'left': 1.5, 'right': 1.5, 'top': 2.0, 'bottom': 1.8},
        'portrait': {'left': 2.0, 'right': 2.0, 'top': 2.5, 'bottom': 2.0}
    }
    
    # Font sizes
    FONT_SIZES = {
        'landscape': {'title': 16, 'subtitle': 12, 'heading': 11, 'body': 9, 'small': 8},
        'portrait': {'title': 14, 'subtitle': 11, 'heading': 10, 'body': 8, 'small': 7}
    }


def get_orientation(report_type: str, df: Optional[pd.DataFrame] = None) -> str:
    """Get optimal orientation for report type and data."""
    orientation = PDFConfig.DEFAULT_ORIENTATIONS.get(report_type, 'landscape')
    
    # Auto-adjust based on data if provided
    if df is not None and not df.empty:
        num_cols = len(df.columns)
        if num_cols >= 7:  # Many columns -> landscape
            orientation = 'landscape'
        elif num_cols <= 3:  # Few columns -> portrait
            orientation = 'portrait'
    
    return orientation


def dataframe_to_pdf_bytes(
    title: str, 
    df: pd.DataFrame, 
    orientation: str = 'landscape',
    include_header: bool = True,
    include_footer: bool = True,
    logo_path: Optional[str] = None,
    subtitle: str = "",
    summary_stats: Optional[Dict] = None
) -> bytes:
    """
    Main function to convert DataFrame to PDF with professional formatting.
    
    Args:
        title: Report title
        df: DataFrame to export
        orientation: 'portrait' or 'landscape'
        include_header: Include header with title/date
        include_footer: Include footer with page numbers
        logo_path: Optional path to logo image
        subtitle: Report subtitle
        summary_stats: Dictionary with summary statistics
    
    Returns: PDF bytes
    """
    # Ensure font is registered
    font_name = _ensure_font_registered()
    
    # Prepare data
    safe_df = df.copy() if df is not None else pd.DataFrame()
    if not safe_df.empty:
        safe_df = safe_df.fillna("")
        safe_df = _shape_df_for_rtl(safe_df)
    
    # Create buffer
    output = io.BytesIO()
    
    # Set page size
    is_landscape = orientation.lower() == 'landscape'
    pagesize = landscape(A4) if is_landscape else portrait(A4)
    
    # Create document with appropriate margins
    margins = PDFConfig.MARGINS[orientation]
    doc = SimpleDocTemplate(
        output,
        pagesize=pagesize,
        leftMargin=margins['left'] * cm,
        rightMargin=margins['right'] * cm,
        topMargin=margins['top'] * cm,
        bottomMargin=margins['bottom'] * cm,
    )
    
    # Get font sizes
    font_sizes = PDFConfig.FONT_SIZES[orientation]
    
    # Create styles
    styles = getSampleStyleSheet()
    
    # Title style
    title_style = ParagraphStyle(
        "ReportTitle",
        parent=styles["Title"],
        fontName=font_name,
        fontSize=font_sizes['title'],
        alignment=TA_CENTER,
        spaceAfter=12,
        textColor=colors.HexColor("#003366")
    )
    
    # Subtitle style
    subtitle_style = ParagraphStyle(
        "ReportSubtitle",
        parent=styles["Heading2"],
        fontName=font_name,
        fontSize=font_sizes['subtitle'],
        alignment=TA_CENTER,
        spaceAfter=16,
        textColor=colors.HexColor("#666666")
    )
    
    # Base cell style
    base_cell = ParagraphStyle(
        "Cell",
        parent=styles["Normal"],
        fontName=font_name,
        fontSize=font_sizes['body'],
        leading=font_sizes['body'] + 2,
        wordWrap="CJK",
        alignment=TA_LEFT,
        spaceBefore=2,
        spaceAfter=2
    )
    
    # Arabic cell style (right-aligned)
    ar_cell = ParagraphStyle(
        "CellAR",
        parent=base_cell,
        alignment=TA_RIGHT
    )
    
    # Summary style
    summary_style = ParagraphStyle(
        "Summary",
        parent=styles["Normal"],
        fontName=font_name,
        fontSize=font_sizes['body'],
        backColor=colors.HexColor("#f0f8ff"),
        borderPadding=8,
        borderColor=colors.HexColor("#d0e0f0"),
        borderWidth=1,
        spaceAfter=12
    )
    
    elements = []
    
    # ===== HEADER =====
    if include_header:
        # Title
        elements.append(Paragraph(_shape_if_rtl(title), title_style))
        
        # Subtitle
        if subtitle:
            elements.append(Paragraph(_shape_if_rtl(subtitle), subtitle_style))
        
        # Summary statistics
        if summary_stats and summary_stats:
            summary_text = []
            for key, value in summary_stats.items():
                summary_text.append(f"<b>{key}:</b> {value}")
            summary_html = " | ".join(summary_text)
            elements.append(Paragraph(_shape_if_rtl(summary_html), summary_style))
        
        # Generation info
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            hijri_date = _hijri_date_short(date.today())
            gen_info = f"Generated: {timestamp} | Hijri: {hijri_date}"
        except:
            gen_info = f"Generated: {timestamp}"
        
        info_style = ParagraphStyle(
            "GenInfo",
            parent=styles["Normal"],
            fontName=font_name,
            fontSize=font_sizes['small'],
            alignment=TA_CENTER,
            textColor=colors.grey,
            spaceAfter=12
        )
        elements.append(Paragraph(_shape_if_rtl(gen_info), info_style))
    
    # ===== DATA TABLE =====
    if safe_df.empty:
        elements.append(Paragraph(_shape_if_rtl("No data available"), styles["Normal"]))
    else:
        # Prepare table data
        headers = [str(c) for c in safe_df.columns]
        rows = safe_df.astype(str).values.tolist()
        data_str = [headers] + rows
        
        # Determine column widths
        prefer_idx = None
        for i, h in enumerate(headers):
            h_lower = h.strip().lower()
            if any(keyword in h_lower for keyword in ['title', 'name', 'description', 'remarks', 'note']):
                prefer_idx = i
                break
        
        col_widths = _auto_col_widths(data_str, font_name, font_sizes['body'], doc.width, prefer_idx)
        
        # Create table data with paragraphs
        table_data = []
        table_data.append([_paragraphize(h, base_cell, ar_cell) for h in headers])
        
        for row in rows:
            table_data.append([_paragraphize(v, base_cell, ar_cell) for v in row])
        
        # Create table
        table = LongTable(table_data, colWidths=col_widths, repeatRows=1)
        
        # Apply table style
        style = TableStyle([
            ("FONTNAME", (0, 0), (-1, -1), font_name),
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#003366")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
            ("ALIGN", (0, 0), (-1, 0), "CENTER"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("FONTSIZE", (0, 0), (-1, -1), font_sizes['body']),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("BOX", (0, 0), (-1, -1), 1, colors.black),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ])
        
        # Alternate row colors for better readability
        for row_num in range(1, len(table_data)):
            bg = colors.HexColor("#f8f9fa") if row_num % 2 == 0 else colors.HexColor("#ffffff")
            style.add("BACKGROUND", (0, row_num), (-1, row_num), bg)
        
        table.setStyle(style)
        elements.append(table)
    
    # ===== FOOTER =====
    def add_footer(canvas, doc):
        if not include_footer:
            return
        
        canvas.saveState()
        canvas.setFont(font_name, font_sizes['small'])
        
        # Footer line
        canvas.setLineWidth(0.5)
        footer_y = doc.bottomMargin / 2
        canvas.line(
            doc.leftMargin,
            footer_y,
            doc.width + doc.leftMargin,
            footer_y
        )
        
        # Page number
        page_num = canvas.getPageNumber()
        canvas.drawCentredString(
            doc.width / 2 + doc.leftMargin,
            footer_y - 10,
            f"Page {page_num}"
        )
        
        # Report title on left
        canvas.drawString(
            doc.leftMargin,
            footer_y - 10,
            _shape_if_rtl(title[:30] + "..." if len(title) > 30 else title)
        )
        
        # Date on right
        date_str = datetime.now().strftime("%d-%b-%Y")
        canvas.drawRightString(
            doc.width + doc.leftMargin,
            footer_y - 10,
            date_str
        )
        
        canvas.restoreState()
    
    # Build document
    if include_footer:
        doc.build(elements, onFirstPage=add_footer, onLaterPages=add_footer)
    else:
        doc.build(elements)
    
    output.seek(0)
    return output.getvalue()


# ============================================================================
# SPECIALIZED REPORT FUNCTIONS
# ============================================================================

def create_darajah_landscape_report(
    darajah_name: str,
    students_df: pd.DataFrame,
    teachers: Optional[List[Dict]] = None,
    summary_stats: Optional[Dict] = None
) -> bytes:
    """
    Create comprehensive Darajah report in landscape format.
    
    Args:
        darajah_name: Name of the Darajah
        students_df: DataFrame with student data
        teachers: List of teacher information
        summary_stats: Summary statistics
    
    Returns: PDF bytes
    """
    # Prepare title and subtitle
    title = f"Darajah Report: {darajah_name}"
    
    # Build subtitle with teacher info
    subtitle_parts = []
    if teachers:
        teacher_names = []
        for teacher in teachers:
            name = teacher.get('name', '')
            role = teacher.get('role', '')
            if name:
                teacher_names.append(f"{name} ({role})" if role else name)
        if teacher_names:
            subtitle_parts.append(f"Teachers: {', '.join(teacher_names)}")
    
    if summary_stats:
        stats_text = []
        for key, value in summary_stats.items():
            if key not in ['Darajah', 'Teachers']:
                stats_text.append(f"{key}: {value}")
        if stats_text:
            subtitle_parts.append(" | ".join(stats_text))
    
    subtitle = " | ".join(subtitle_parts) if subtitle_parts else ""
    
    # Add summary statistics to DataFrame if provided
    if summary_stats and not students_df.empty:
        for key, value in summary_stats.items():
            if key not in students_df.columns:
                students_df[key] = [value] * len(students_df)
    
    # Generate PDF
    return dataframe_to_pdf_bytes(
        title=title,
        df=students_df,
        orientation='landscape',
        subtitle=subtitle,
        summary_stats=summary_stats,
        include_header=True,
        include_footer=True
    )


def create_student_landscape_report(
    student_info: Dict,
    borrowed_books: Optional[List[Dict]] = None,
    monthly_stats: Optional[Dict] = None,
    include_taqeem: bool = True
) -> bytes:
    """
    Create comprehensive student report in landscape format.
    
    Args:
        student_info: Student information dictionary
        borrowed_books: List of borrowed books
        monthly_stats: Monthly statistics
        include_taqeem: Include Taqeem marks
    
    Returns: PDF bytes
    """
    # Extract student info
    full_name = student_info.get('FullName', '')
    tr_number = student_info.get('TRNumber', student_info.get('userid', ''))
    darajah = student_info.get('Darajah', '')
    marhala = student_info.get('Marhala', '')
    its_id = student_info.get('ITS ID', '')
    
    # Prepare title and subtitle
    title = f"Student Reading Report: {full_name}"
    subtitle = f"TR: {tr_number} | ITS: {its_id} | Darajah: {darajah} | Marhala: {marhala}"
    
    # Prepare summary statistics
    metrics = student_info.get('Metrics', {})
    summary_stats = {
        'AY Issues': metrics.get('AYIssues', 0),
        'Mustawā': metrics.get('MaxBooksAllowed', 'N/A'),
        'Fees Paid (AY)': f"{metrics.get('FeesPaidAY', 0):.2f}",
        'Outstanding': f"{metrics.get('OutstandingBalance', 0):.2f}",
        'Last Issue': metrics.get('LastIssueDate', 'N/A')
    }
    
    # Prepare borrowed books data
    all_books_data = []
    if borrowed_books:
        for book in borrowed_books:
            all_books_data.append({
                'Title': book.get('title', ''),
                'Collection': book.get('collection', ''),
                'Language': book.get('language', ''),
                'Issued': book.get('_issued_hijri', ''),
                'Due': book.get('_due_hijri', ''),
                'Status': 'Overdue' if book.get('overdue') else 'Returned' if book.get('returned') else 'Active'
            })
    
    # Create DataFrame
    if all_books_data:
        df = pd.DataFrame(all_books_data)
    else:
        df = pd.DataFrame({'Message': ['No borrowed books found']})
    
    # Add monthly stats if available
    if monthly_stats and not df.empty:
        monthly_data = []
        for month, stats in monthly_stats.items():
            monthly_data.append({
                'Month': month,
                'Books Issued': stats.get('count', 0),
                'Target': stats.get('target', 0),
                'Status': stats.get('reco_status', 'N/A'),
                'Reviews': stats.get('review_count', 0)
            })
        
        if monthly_data:
            monthly_df = pd.DataFrame(monthly_data)
            # Merge with main DataFrame if needed
    
    # Generate PDF
    return dataframe_to_pdf_bytes(
        title=title,
        df=df,
        orientation='landscape',
        subtitle=subtitle,
        summary_stats=summary_stats,
        include_header=True,
        include_footer=True
    )


def create_monthly_landscape_report(
    month_label: str,
    data_df: pd.DataFrame,
    summary_stats: Optional[Dict] = None,
    report_type: str = 'monthly'
) -> bytes:
    """
    Create monthly report in landscape format.
    
    Args:
        month_label: Month label (e.g., "Rajab al-Asab 1447 H")
        data_df: Monthly data DataFrame
        summary_stats: Summary statistics
        report_type: Type of report ('monthly', 'reading', 'activity')
    
    Returns: PDF bytes
    """
    # Prepare title based on report type
    if report_type == 'reading':
        title = f"Monthly Reading Report: {month_label}"
    elif report_type == 'activity':
        title = f"Library Activity Report: {month_label}"
    else:
        title = f"Monthly Report: {month_label}"
    
    # Prepare subtitle with statistics
    subtitle_parts = []
    if summary_stats:
        for key, value in summary_stats.items():
            subtitle_parts.append(f"{key}: {value}")
    
    subtitle = " | ".join(subtitle_parts) if subtitle_parts else ""
    
    # Generate PDF
    return dataframe_to_pdf_bytes(
        title=title,
        df=data_df,
        orientation='landscape',
        subtitle=subtitle,
        summary_stats=summary_stats,
        include_header=True,
        include_footer=True
    )


# ============================================================================
# ADVANCED REPORT WITH CHARTS
# ============================================================================

def create_analytical_report_with_charts(
    title: str,
    data_df: pd.DataFrame,
    charts: Optional[List[Dict]] = None,
    orientation: str = 'landscape',
    summary_stats: Optional[Dict] = None
) -> bytes:
    """
    Create advanced report with data tables and charts.
    
    Args:
        title: Report title
        data_df: Main data DataFrame
        charts: List of chart definitions
        orientation: 'portrait' or 'landscape'
        summary_stats: Summary statistics
    
    Returns: PDF bytes
    """
    output = io.BytesIO()
    font_name = _ensure_font_registered()
    
    # Set page size
    is_landscape = orientation.lower() == 'landscape'
    pagesize = landscape(A4) if is_landscape else portrait(A4)
    
    # Create document
    margins = PDFConfig.MARGINS[orientation]
    doc = SimpleDocTemplate(
        output,
        pagesize=pagesize,
        leftMargin=margins['left'] * cm,
        rightMargin=margins['right'] * cm,
        topMargin=margins['top'] * cm,
        bottomMargin=margins['bottom'] * cm,
    )
    
    # Get font sizes
    font_sizes = PDFConfig.FONT_SIZES[orientation]
    
    # Create styles
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "AnalyticalTitle",
        parent=styles["Title"],
        fontName=font_name,
        fontSize=font_sizes['title'] + 2,
        alignment=TA_CENTER,
        spaceAfter=8,
        textColor=colors.HexColor("#2c3e50")
    )
    
    chart_title_style = ParagraphStyle(
        "ChartTitle",
        parent=styles["Heading2"],
        fontName=font_name,
        fontSize=font_sizes['heading'],
        alignment=TA_CENTER,
        spaceAfter=6,
        textColor=colors.HexColor("#34495e")
    )
    
    elements = []
    
    # Title
    elements.append(Paragraph(_shape_if_rtl(title), title_style))
    
    # Summary statistics
    if summary_stats:
        summary_text = []
        for key, value in summary_stats.items():
            summary_text.append(f"<b>{key}:</b> {value}")
        
        summary_style = ParagraphStyle(
            "AnalyticalSummary",
            parent=styles["Normal"],
            fontName=font_name,
            fontSize=font_sizes['body'],
            backColor=colors.HexColor("#ecf0f1"),
            borderPadding=10,
            borderColor=colors.HexColor("#bdc3c7"),
            borderWidth=1,
            spaceAfter=15
        )
        elements.append(Paragraph(_shape_if_rtl(" | ".join(summary_text)), summary_style))
    
    # Add charts if provided
    if charts:
        for chart_def in charts:
            chart_type = chart_def.get('type', 'bar')
            chart_title = chart_def.get('title', 'Chart')
            chart_data = chart_def.get('data', {})
            
            # Add chart title
            elements.append(Paragraph(_shape_if_rtl(chart_title), chart_title_style))
            
            # Create drawing for chart
            drawing = Drawing(400, 200)
            
            if chart_type == 'bar' and 'categories' in chart_data and 'values' in chart_data:
                # Create bar chart
                bc = VerticalBarChart()
                bc.x = 50
                bc.y = 50
                bc.height = 125
                bc.width = 300
                bc.data = [chart_data['values']]
                bc.strokeColor = colors.black
                bc.valueAxis.valueMin = 0
                bc.valueAxis.valueMax = max(chart_data['values']) * 1.2
                bc.categoryAxis.categoryNames = chart_data['categories']
                bc.barLabelFormat = '%d'
                bc.barLabels.nudge = 10
                
                # Set colors
                bc.bars[0].fillColor = colors.HexColor("#3498db")
                
                drawing.add(bc)
                
            elif chart_type == 'pie' and 'labels' in chart_data and 'values' in chart_data:
                # Create pie chart
                pie = Pie()
                pie.x = 150
                pie.y = 50
                pie.width = 150
                pie.height = 150
                pie.data = chart_data['values']
                pie.labels = chart_data['labels']
                pie.slices.strokeWidth = 0.5
                pie.slices.strokeColor = colors.white
                
                # Set slice colors
                colors_list = [
                    colors.HexColor("#3498db"),
                    colors.HexColor("#2ecc71"),
                    colors.HexColor("#e74c3c"),
                    colors.HexColor("#f39c12"),
                    colors.HexColor("#9b59b6")
                ]
                for i, slice in enumerate(pie.slices):
                    slice.fillColor = colors_list[i % len(colors_list)]
                
                drawing.add(pie)
            
            elements.append(drawing)
            elements.append(Spacer(1, 20))
    
    # Add data table
    if not data_df.empty:
        # Prepare table
        safe_df = data_df.copy().fillna("")
        safe_df = _shape_df_for_rtl(safe_df)
        
        headers = [str(c) for c in safe_df.columns]
        rows = safe_df.astype(str).values.tolist()
        
        # Create table data
        base_cell = ParagraphStyle(
            "AnalyticalCell",
            parent=styles["Normal"],
            fontName=font_name,
            fontSize=font_sizes['body'],
            leading=font_sizes['body'] + 2,
            wordWrap="CJK",
            alignment=TA_LEFT
        )
        
        ar_cell = ParagraphStyle(
            "AnalyticalCellAR",
            parent=base_cell,
            alignment=TA_RIGHT
        )
        
        table_data = []
        table_data.append([_paragraphize(h, base_cell, ar_cell) for h in headers])
        
        for row in rows:
            table_data.append([_paragraphize(v, base_cell, ar_cell) for v in row])
        
        # Calculate column widths
        data_str = [headers] + rows
        col_widths = _auto_col_widths(data_str, font_name, font_sizes['body'], doc.width)
        
        # Create table
        table = Table(table_data, colWidths=col_widths, repeatRows=1)
        
        # Apply style
        table.setStyle(TableStyle([
            ("FONTNAME", (0, 0), (-1, -1), font_name),
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2c3e50")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("ALIGN", (0, 0), (-1, 0), "CENTER"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("FONTSIZE", (0, 0), (-1, -1), font_sizes['body']),
        ]))
        
        elements.append(Spacer(1, 20))
        elements.append(Paragraph(_shape_if_rtl("Detailed Data"), chart_title_style))
        elements.append(table)
    
    # Build document
    doc.build(elements)
    output.seek(0)
    return output.getvalue()


# ============================================================================
# QUICK EXPORT FUNCTIONS
# ============================================================================

def export_to_pdf_landscape(df: pd.DataFrame, title: str = "Report", **kwargs) -> bytes:
    """Quick export to PDF in landscape orientation."""
    return dataframe_to_pdf_bytes(title, df, orientation='landscape', **kwargs)


def export_to_pdf_portrait(df: pd.DataFrame, title: str = "Report", **kwargs) -> bytes:
    """Quick export to PDF in portrait orientation."""
    return dataframe_to_pdf_bytes(title, df, orientation='portrait', **kwargs)


def export_to_excel_multisheet(main_df: pd.DataFrame, main_sheet: str = "Main",
                              additional_sheets: Optional[Dict[str, pd.DataFrame]] = None) -> bytes:
    """Export multiple DataFrames to Excel with different sheets."""
    return dataframe_to_excel_bytes(main_df, main_sheet, additional_sheets)


# ============================================================================
# CONVENIENCE WRAPPERS
# ============================================================================

def create_student_reading_report(student_info: Dict, **kwargs) -> bytes:
    """Convenience wrapper for student reading report."""
    return create_student_landscape_report(student_info, **kwargs)


def create_darajah_summary_report(darajah_name: str, students_df: pd.DataFrame, **kwargs) -> bytes:
    """Convenience wrapper for Darajah summary report."""
    return create_darajah_landscape_report(darajah_name, students_df, **kwargs)


def create_monthly_summary_report(month_label: str, data_df: pd.DataFrame, **kwargs) -> bytes:
    """Convenience wrapper for monthly summary report."""
    return create_monthly_landscape_report(month_label, data_df, **kwargs)


# ============================================================================
# BATCH EXPORT FUNCTIONS
# ============================================================================

def create_batch_reports(reports: List[Dict]) -> Dict[str, bytes]:
    """
    Create multiple reports in batch.
    
    Args:
        reports: List of report definitions
    
    Returns: Dictionary with report names as keys and PDF bytes as values
    """
    results = {}
    
    for report in reports:
        report_type = report.get('type', 'dataframe')
        report_name = report.get('name', f"report_{len(results)}")
        
        try:
            if report_type == 'dataframe':
                pdf_bytes = dataframe_to_pdf_bytes(
                    title=report['title'],
                    df=report['data'],
                    orientation=report.get('orientation', 'landscape'),
                    subtitle=report.get('subtitle', ''),
                    summary_stats=report.get('summary_stats', None)
                )
            elif report_type == 'student':
                pdf_bytes = create_student_landscape_report(**report['params'])
            elif report_type == 'darajah':
                pdf_bytes = create_darajah_landscape_report(**report['params'])
            elif report_type == 'monthly':
                pdf_bytes = create_monthly_landscape_report(**report['params'])
            elif report_type == 'analytical':
                pdf_bytes = create_analytical_report_with_charts(**report['params'])
            else:
                continue
            
            results[report_name] = pdf_bytes
            
        except Exception as e:
            print(f"Error creating report {report_name}: {e}")
            # Create error report
            error_df = pd.DataFrame({
                'Error': [f"Failed to generate report: {str(e)[:100]}"],
                'Report': [report_name],
                'Type': [report_type]
            })
            results[f"error_{report_name}"] = dataframe_to_pdf_bytes(
                f"Error Report: {report_name}",
                error_df,
                orientation='portrait'
            )
    
    return results


# ============================================================================
# MAIN EXPORT FUNCTION (Backward Compatibility)
# ============================================================================

def export_dataframe_to_pdf(df: pd.DataFrame, title: str = "Report", 
                           orientation: str = 'landscape') -> bytes:
    """
    Main export function (for backward compatibility).
    
    Args:
        df: DataFrame to export
        title: Report title
        orientation: 'portrait' or 'landscape'
    
    Returns: PDF bytes
    """
    return dataframe_to_pdf_bytes(
        title=title,
        df=df,
        orientation=orientation,
        include_header=True,
        include_footer=True
    )
