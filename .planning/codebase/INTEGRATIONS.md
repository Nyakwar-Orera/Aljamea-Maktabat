# External Integrations

**Analysis Date:** 2026-04-15

## Primary Integrations

### Koha ILS (MySQL)
- **Type:** External Database (Direct connection via MySQL connector)
- **Role:** Source of truth for library data (Books, Issues, Students, Fines)
- **File:** `db_koha.py`, `services/koha_queries.py`
- **Authentication:** IP-based and User/Password (configured in `.env`)
- **Multi-Campus Multi-Tenant:**
  - One Koha instance per campus branch (AJSN, AJSS, AJSK, AJSM, AJSI).
  - Centralized connection pooling in `db_koha.py`.

### Local Application Database (SQLite)
- **Type:** Local relational database (`appdata.db`)
- **Role:** Application state, audit logs, cached reports, and local admin settings.
- **File:** `db_app.py`, `config.py`
- **Schema Management:** `appdata_init.py` handles table creation.

## Communication Services

### SMTP (Flask-Mail)
- **Type:** Email Service
- **Role:** Sending reports, notifications, and password reset tokens.
- **Provider:** Configurable via `.env` (Defaults to Gmail).
- **File:** `routes/notifications.py`, `email_utils.py`

## Data Exports

### Excel/ODS Support
- **Type:** File export
- **Libraries:** `pandas`, `openpyxl`, `odfpy`, `XlsxWriter`
- **Role:** Exporting reports to Excel/Calc for HODs and Admin.
- **File:** `services/exports.py`

### PDF Generation
- **Type:** Document generation
- **Libraries:** `reportlab`, `arabic-reshaper`, `python-bidi`
- **Role:** Generating dynamic student reports with Arabic RTL support.
- **File:** `routes/reports.py`

## Infrastructure

### Docker / Docker Compose
- **Type:** Containerization
- **Role:** Packaging the application for consistent deployments across environments.
- **File:** `Dockerfile`, `docker-compose.yml`

---

*Integration analysis: 2026-04-15*
