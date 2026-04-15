# Coding Conventions

**Analysis Date:** 2026-04-15

## General Standards

- **Language:** Python 3.x
- **Style Guide:** Primarily follows PEP 8.
- **Modularity:** Business logic is strictly separated into `services/`. Templates should not contain complex logic; use Jinja2 filters for formatting.

## Naming Conventions

### Files & Directories
- **Directories:** `snake_case` (e.g., `static/`, `routes/`).
- **Python Files:** `snake_case.py` (e.g., `koha_queries.py`).
- **Templates:** Usually `snake_case.html`.

### Code Objects
- **Functions:** `snake_case` (e.g., `get_branch_config`).
- **Classes:** `PascalCase` (e.g., `Config`, `RateLimiter`).
- **Blueprints:** Role-based naming (e.g., `super_admin_bp`).

## UI & Templates

### Jinja2 Filters
- Located in `filters.py`.
- Always use `format_number` for numeric data in tables.
- Use `is_arabic` to apply RTL styling dynamically.
- Use `format_date` for consistent date presentation.

### CSS Strategy
- Custom styles are in `static/css/style.css`.
- Uses Bootstrap 5 for layout and standard components.
- Custom colors: Global "Golden-Brown" theme for Super Admin dashboard.

## Multi-Campus Patterns

### Session Handling
- Always check for `branch_code` in the session after login.
- Use `@login_required` decorator for all protected routes.

### Database Querying
- Use connection pooling from `db_koha.py`.
- Queries should be parameterized to prevent SQL injection.
- Services should handle NULL values gracefully to prevent rendering crashes.

## State Management

- **Local Persistence:** Use the Local SQLite DB (`appdata.db`) for non-Koha data.
- **Caching:** Limited caching implemented for performance-heavy reports.

---

*Convention analysis: 2026-04-15*
