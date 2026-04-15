# Technology Stack

**Analysis Date:** 2026-04-15

## Languages

**Primary:**
- Python 3.x - All application logic and service layers

**Secondary:**
- JavaScript - Frontend interactivity (standard browser-based)
- SQL - Database migrations and complex report queries

## Runtime

**Environment:**
- Python 3.x (Likely 3.10+ based on dependency versions)
- Production: Waitress (WSGI server)

**Package Manager:**
- pip - Using `requirements.txt`
- Virtual Environment: `maktabat_venv` detected

## Frameworks

**Core:**
- Flask 2.2.5 - Web framework
- Flask-Login - Authentication management
- Flask-Mail - Email notifications
- Flask-APScheduler - Background task scheduling

**Testing:**
- None detected (Major gap)

**Build/Dev:**
- Docker - `Dockerfile` and `docker-compose.yml` present

## Key Dependencies

**Critical:**
- `mysql-connector-python` 9.0.0 - Primary data access to Koha MySQL instances
- `sqlite-utils` 3.35 - Helpers for local SQLite application database
- `pandas` 2.2.3 - Data processing for reports and analytics
- `reportlab` 4.2.2 - PDF generation for student reports
- `hijri-converter` - Hijri-Gregorian date handling for academic year logic

**Infrastructure:**
- `waitress` 3.0.2 - Production WSGI server
- `python-dotenv` - Environment variable management

## Configuration

**Environment:**
- `.env` files (loaded via `python-dotenv` in `config.py`)
- Key configs: `SECRET_KEY`, `KOHA_DB_*`, `CAMPUS_REGISTRY`

**Build:**
- `Dockerfile` - Container definition
- `Procfile` - Process management for Heroku-like deployments

## Platform Requirements

**Development:**
- Windows/Linux supported (Waitress runs on both)
- Requires MySQL/MariaDB connectivity for Koha databases

**Production:**
- Docker container support
- Persistent volume for `appdata.db` (SQLite)

---

*Stack analysis: 2026-04-15*
*Update after major dependency changes*
