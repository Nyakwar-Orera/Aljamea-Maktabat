# Aljamea-Maktabat Multi-Campus Dashboards

## What This Is

Aljamea-Maktabat is a specialized library management and academic analytics portal designed for the Al-Jamea tus-Saifiyah university system. It provides campus-specific administrative dashboards and a global "God Eye" Super Admin interface to manage library stock, student performance, and academic KPIs across five global campuses (Nairobi, Surat, Karachi, Marol, and Sidhpur).

## Core Value

Centralized, real-time visibility into multi-campus library and academic performance to drive institutional excellence.

## Requirements

### Validated

- ✓ **Multi-Campus Branch Registry** — Centralized configuration for 5 global branches.
- ✓ **Branch-Isolated Authentication** — Secure login with session-based campus isolation.
- ✓ **Cross-Database Data Access** — Unified connectivity to Koha (MySQL) and Local App data (SQLite).
- ✓ **Dynamic Reporting Engine** — Robust Excel and PDF export capabilities with RTL (Arabic) support.
- ✓ **Custom UI Formatting** — Specialized Jinja2 filters for academic and library data presentation.

### Active

- [ ] **Global "God Eye" Dashboard** — Implementation of the `super_admin.py` route for cross-campus aggregation.
- [ ] **Branch Explorer** — Visual navigation component for switching between campus views (similar to Darajah explorer).
- [ ] **Parallel Query Engine** — Optimized cross-campus data retrieval to ensure high-performance KPI rendering.
- [ ] **Global KPI Aggregation** — Real-time tracking of active patrons, stock, and book issues across all 5 campuses.
- [ ] **Top Students Globally** — Multi-campus academic ranking and performance visualization.
- [ ] **Golden-Brown Theme Overhaul** — Full UI/UX styling consistent with the institution's branding and login page.
- [ ] **Interactive Visualizations** — Charts with synchronized popup data tables for detailed inspection.
- [ ] **Hierarchical Reporting** — Language-wise, class-wise, and year-wise analytics.

### Out of Scope

- **Direct Koha Data Mutation** — The tool remains a reporting and analytics layer; write-backs to Koha are excluded to maintain library integrity.

## Context

The application acts as a high-level reporting layer sitting on top of multiple Koha Integrated Library System (ILS) instances. Each campus hosts its own Koha database, and this system aggregates and presents that data in a unified academic context. The current push is to finalize the "God Eye" view for the Super Admin role.

## Constraints

- **Tech Stack**: Python 3.x / Flask 2.2.5 — Consistent with current production environment.
- **Database**: Must support concurrent connections to 5+ MySQL instances without pool exhaustion.
- **UI/UX**: Must adhere to the institutional golden-brown branding and support RTL Arabic display.
- **Security**: Super Admin routes must have strictly enforced access controls.

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Multi-Campus Registry | Allows for easy scaling to new branches via simple config updates. | ✓ Good |
| Layered Monolith | Simplifies deployment and maintains clear separation of concerns (Routes/Services). | ✓ Good |
| Parallel Aggegration | Necessary to prevent UI lag when fetching data from five different global databases. | — Pending |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-04-15 after initialization (Brownfield)*
