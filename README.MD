# 📚 Maktabat al-Jamea: Institutional Library Intelligence Portal

A professional, role-based dashboard built with **Flask** and seamlessly integrated with **Koha ILS**. It provides real-time analytics, automated scheduled reports, and secure dashboards for administrators, heads of departments (HODs), and class teachers—enabling data-driven decisions and streamlined library management.

A state-of-the-art, role-based library ecosystem designed for **Aljamea-tus-Saifiyah**. This portal seamlessly bridges the gap between the **Koha ILS** backend and a premium, student-facing frontend, offering AI-powered insights, secure digital reading, and comprehensive academic assessment (Taqeem).

---

## 🌟 Modern Core Features

### 🤖 Intelligence Layer (AI Recommendations)
*   **Personalized Discoveries**: Analyzes individual borrowing patterns to suggest the next read.
*   **Marhala Trends**: Real-time identification of popular books within specific academic levels.
*   **Automated Nudges**: AI-driven emails to lapsed borrowers to encourage reading re-engagement.

### 📱 Progressive Web App (PWA)
*   **Mobile-First Experience**: High-performance responsive design with native-app feel.
*   **Offline Access**: Essential features remain available via service worker caching.

### 📖 Secure Digital Library
*   **Protected Reader**: Institutional-themed PDF viewer with secure session-based serving.
*   **HOD Management**: Web-based upload and deletion tools for digital assets.

---

## 🛡️ Production & Security Hardening
*   **Brute-Force Protection**: Persistent Rate Limiter blocking accounts and IPs after failed attempts.
*   **Security Headers**: CSP, HSTS, and X-Frame-Options enabled globally via middleware.
*   **Branded UI**: Premium confirmation modals and professional error fallback pages.

---

## 🗂️ Project Structure: Full Repository Map

```text
Aljamea-Maktabat/
├── app.py                  # Entry Point & Global Middleware
├── appdata_init.py         # Metadata DB (SQLite) Initialization
├── config.py               # Environment & App Configuration
├── db_app.py               # Local SQLite Connection Interface
├── db_koha.py              # Koha ILS (MySQL) Connection Interface
├── email_utils.py          # Unified Email Sending Logic
├── requirements.txt        # Production Dependencies
├── run_laptop.sh           # Unix/Linux Entry Point Script
├── run_windows.bat         # Windows Deployment Script (w/ Venv Management)
├── services/               # Core Business Logic Layer
│   ├── recommendation_service.py   # AI Intelligence Engine
│   ├── marks_service.py            # Taqeem Calculation Logic
│   ├── security_service.py         # Rate Limiting & Audit Protection
│   ├── koha_queries.py             # Optimized SQL for Koha Database
│   ├── exports.py                  # PDF/Excel Generation Logic
│   └── reporting.py                # Student Performance Aggregators
├── routes/                 # Blueprint Definitions
│   ├── admin.py                    # Admin Control & Settings
│   ├── library.py                  # Digital Library Services
│   ├── students.py                 # Student Profiles & Taqeem Views
│   ├── auth.py                     # Secure Authentication Logic
│   ├── profile.py                  # User Profile Management
│   ├── dashboard.py                # Main Analytics Dashboards
│   ├── hod_dashboard.py            # Marhala-level Analytics
│   ├── teacher_dashboard.py        # Darajah-level Tools
│   ├── reports.py                  # On-demand Reporting
│   └── password_reset.py           # Secure Recovery Workflow
├── templates/              # Jinja2 Layouts
│   ├── base.html                   # Global Master Layout
│   ├── student.html                # Premium Student Dashboard
│   ├── library/                    # Digital Portal Templates
│   ├── admin/                      # Admin System Templates
│   ├── emails/                     # Professional HTML Email Templates
│   └── errors/                     # Custom 404/500 Pages
├── static/                 # Production Assets
│   ├── js/sw.js                    # PWA Service Worker Logic
│   ├── js/admin.js                 # Admin UI Interactivity
│   ├── css/style.css               # Main Premium Layout Styles
│   ├── manifest.json               # PWA Identification
│   ├── uploads/digital_books/      # Secure Repository (Protected PDFs)
│   └── offline.html                # PWA Offline Landing Page
├── tasks/                  # Background Automation
│   ├── scheduler.py                # APScheduler Orchestration
│   ├── ai_nudge.py                 # Re-engagement Automations
│   └── monthly_reports.py          # Scheduled Monthly Performance Reports
└── sql/                    # Historical Reference & Schema
    └── koha.sql                    # Baseline Koha Schema Samples
```

---

## 🏗️ Configuration: Required Environment Variables

To run the portal successfully, create a `.env` file in the root directory:

| Variable | Description | Example |
| :--- | :--- | :--- |
| `SECRET_KEY` | Flask Session Security | *any-long-random-string* |
| `KOHA_DB_HOST` | Database host of your Koha Server | `192.168.1.100` |
| `KOHA_DB_USER` | Koha Database Username | `koha_user` |
| `KOHA_DB_PASS` | Koha Database Password | `securePassword` |
| `MAIL_SERVER` | SMTP Server for Reports/Nudges | `smtp.office365.com` |
| `MAIL_USERNAME` | Automated Notification Email | `library@aljamea.edu` |
| `KOHA_OFFLINE` | Use mock data if Koha is unreachable | `false` |
| `APP_SQLITE_PATH` | Path to the metadata database | `appdata.db` |

---

## 🖥️ Institutional Windows Server Deployment

1.  **Python Environment**: Install Python 3.10+ and add it to the System PATH.
2.  **Service Persistence**: Use **NSSM** (Non-Sucking Service Manager) to install `run_windows.bat` as a service.
    ```powershell
    nssm install AljameaLibrary "C:\Path\To\Aljamea-Maktabat\run_windows.bat"
    nssm start AljameaLibrary
    ```
3.  **IIS (Optional)**: Set up IIS with the **URL Rewrite** module to act as a reverse proxy for Port 5000.

---

## 🚀 Deployment & Installation

- **Environment Setup**: `cp .env.example .env`
- **Installation**: `pip install -r requirements.txt` and `python appdata_init.py`
- **Rapid Run**: Double-click `run_windows.bat` (Windows) or `./run_laptop.sh` (Linux).

---

*© 2026 Maktabat al-Jamea Library Portal. Built for Aljamea-tus-Saifiyah.*