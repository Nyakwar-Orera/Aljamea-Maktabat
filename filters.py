# filters.py
import re
from datetime import datetime


def format_number(value):
    """Format number with commas."""
    try:
        if value is None:
            return "0"
        if isinstance(value, str):
            try:
                if 'e' in value.lower() or '.' in value:
                    value = float(value)
                else:
                    value = int(value)
            except (ValueError, TypeError):
                return value
        
        if isinstance(value, (int, float)):
            if isinstance(value, float):
                if value.is_integer():
                    return f"{int(value):,}"
                else:
                    return f"{value:,.2f}"
            else:
                return f"{value:,}"
        return str(value)
    except (ValueError, TypeError):
        return str(value)


def format_date(value, format='%Y-%m-%d'):
    """Format date."""
    if not value:
        return ''
    try:
        if isinstance(value, str):
            for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%Y/%m/%d'):
                try:
                    value = datetime.strptime(value, fmt)
                    break
                except:
                    continue
        if isinstance(value, datetime.datetime):
            return value.strftime(format)
    except:
        pass
    return str(value)


def truncate_text(text, length=50):
    """Truncate text to specified length."""
    if not text:
        return ''
    if len(text) <= length:
        return text
    return text[:length] + '...'


def is_arabic(text):
    """Check if text contains Arabic characters."""
    if not text:
        return False
    arabic_pattern = re.compile(r'[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF]')
    return bool(arabic_pattern.search(str(text)))


def calculate_efficiency(avg_issues):
    """Calculate efficiency rating based on average issues per student."""
    try:
        avg = float(avg_issues)
        if avg >= 5:
            return "Excellent", "success"
        elif avg >= 3:
            return "Good", "primary"
        elif avg >= 1:
            return "Average", "warning"
        else:
            return "Low", "danger"
    except:
        return "Unknown", "secondary"


def darajah_sort_key(darajah_name):
    """Extract sorting key from darajah name."""
    if not darajah_name:
        return (999, '')
    match = re.search(r'\d+', str(darajah_name))
    if match:
        number = int(match.group())
        return (0, number, darajah_name)
    return (1, darajah_name)


def register_filters(app):
    """Register all custom filters with Flask app."""
    app.jinja_env.filters['format_number'] = format_number
    app.jinja_env.filters['format_date'] = format_date
    app.jinja_env.filters['truncate_text'] = truncate_text
    app.jinja_env.filters['is_arabic'] = is_arabic
    
    app.jinja_env.globals.update(
        calculate_efficiency=calculate_efficiency,
        darajah_sort_key=darajah_sort_key,
    )