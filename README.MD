# Aljamea-Maktabat v3.0.0 — Global Multi-Campus Ecosystem

A professional, high-performance library analytics and reporting dashboard for **Aljamea-tus-Saifiyah**. It provides real-time data visualization, cross-campus management, and student performance tracking for all university branches globally.

---

## 🌟 Institutional Features

### 👁️ God Eye — Super Admin Dashboard
A centralized "command center" for senior leadership.
- **Global Intelligence**: Aggregated KPIs from all 5 campuses (Nairobi, Surat, Karachi, Marol, Sidhpur).
- **Parallel Query Engine**: Real-time cross-campus stats using threaded async execution.
- **Branch Health Monitor**: Live status of all remote database connections.
- **Impersonation Mode**: Securely view single-branch data for troubleshooting or deep-dives.

### 🏛️ Support for Global Branches
Multi-tenant architecture with full data isolation:
- **AJSN** — Nairobi (Kenya)
- **AJSS** — Surat (India)
- **AJSK** — Karachi (Pakistan)
- **AJSM** — Marol (India)
- **AJSI** — Sidhpur (India)

### 📊 Comprehensive Insights
- **Darajah & Marhala Explorers**: Deep-dive analytics for teachers and HODs.
- **Dynamic Periodic Reports**: Automated PDF/Excel reporting on library efficiency.
- **Student Progress**: Complete tracking of reading habits and academic engagement.

---

## 🏗️ Technical Architecture

- **Backend**: Python / Flask (v3.0.0 Framework)
- **Database (Global)**: Multiple Koha MySQL Instances (Centralized connection pooling)
- **Database (Local)**: SQLite (User session & application state management)
- **Production Server**: Waitress (High-performance Windows WSGI)
- **Frontend**: Modern Bootstrap 5, Chart.js, Vanilla CSS Design System

---

## 🚀 Quick Start (Windows Server)

### 1. Installation
```powershell
git clone <repository-url>
cd Aljamea-Maktabat
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configuration
Copy `.env.example` to `.env` and configure your **Koha MySQL** credentials for each campus.

### 3. Execution
```powershell
python appdata_init.py
python app.py
```

---

## 🛡️ Administrative Roles

| Role | Access Level | Capability |
| :--- | :--- | :--- |
| **Super Admin** | **Global** | Full God Eye access, all branches, user creation. |
| **Admin** | **Branch** | Branch-specific dashboard, settings, and reports. |
| **HOD** | **Marhala** | Periodic reports for their specific department. |
| **Teacher** | **Darajah** | Class-level dashboard and student tracking. |

---

## 📜 Documentation
- [Multi-Campus Walkthrough](MultiCampus_Walkthrough.md)
- [Windows Deployment Guide](Windows_Deployment_Guide.md)

---

## ⚖️ License & Credits
© 2026 **Aljamea-tus-Saifiyah — Library (Maktabat al-Jamea)**. All rights reserved.
Built for academic excellence.