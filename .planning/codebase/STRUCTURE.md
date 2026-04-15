# Directory Structure

**Analysis Date:** 2026-04-15

## High-Level Layout

```text
Aljamea-Maktabat/
├── .planning/              # GSD Planning and context (this project)
├── routes/                 # Flask Blueprints (Controllers)
├── services/               # Business Logic and Data Access Objects
├── static/                 # CSS, JS, and Images
├── templates/              # Jinja2 HTML Templates
├── sql/                    # Raw SQL queries for complex reporting
├── tasks/                  # Background jobs (APScheduler)
├── appdata.db              # Local SQLite database
├── app.py                  # Application entry point
├── config.py               # Global configuration (Multi-campus registry)
└── db_koha.py              # Koha MySQL connection manager
```

## Key Directories

### `routes/`
Contains the application's request handlers organized by user role and domain.
- `admin.py`: Primary administration routes.
- `super_admin.py`: Multi-campus global overview.
- `hod_dashboard.py`: Head of Department analytics.
- `teacher_dashboard.py`: Faculty-specific views.

### `services/`
The business logic layer. These files handle the "heavy lifting."
- `koha_queries.py`: The largest file, containing SQL logic for library data.
- `branch_queries.py`: Logic for cross-campus data retrieval.
- `exports.py`: File generation logic (Excel/ODS).

### `templates/`
HTML structure using Jinja2 inheritance.
- `base.html`: Main layout with sidebar and header.
- `dashboard.html`: The standard campus dashboard.
- `super_admin/`: Templates for the global "God Eye" dashboard.

### `sql/`
Stores plain SQL files which are read by the service layer. This keeps SQL separate from Python code for cleaner readability.

## Convention & Naming

- **Python Files:** `snake_case.py`
- **Blueprints:** Named by role (`auth_bp`, `admin_bp`).
- **Templates:** Mostly located in root `templates/` or subfolders by role.
- **Service functions:** Usually named `get_[entity]_by_[attribute]` (e.g., `get_student_by_id`).

---

*Structure analysis: 2026-04-15*
