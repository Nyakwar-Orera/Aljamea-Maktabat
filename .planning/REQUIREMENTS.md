# Requirements: Aljamea-Maktabat Super Admin Dashboard

**Defined:** 2026-04-15
**Core Value:** Centralized, real-time visibility into multi-campus library and academic performance to drive institutional excellence.

## v1 Requirements

### Core Dashboard (God Eye)

- [ ] **DASH-01**: Implementation of the `super_admin.py` route and blueprint.
- [ ] **DASH-02**: Real-time KPI aggregation from all 5 campuses (AJSN, AJSS, AJSK, AJSM, AJSI).
- [ ] **DASH-03**: Display global "Main Page" metrics: Active Patrons, Total Books, Top 10 Books Issued (Global vs Own Campus).
- [ ] **DASH-04**: Secure access control ensuring only Super Admins can access global views.

### Branch Explorer

- [ ] **EXPL-01**: Visual branch navigation component (similar to Darajah/Marhala explorer).
- [ ] **EXPL-02**: Quick-switch functionality allowed to view individual campus admin dashboards.
- [ ] **EXPL-03**: Global filtering capability to isolate analytics by specific branch.

### Analytics & KPIs

- [ ] **ANLY-01**: Parallel Query Engine for multi-threaded data retrieval across 5 MySQL instances.
- [ ] **ANLY-02**: Global Top Students ranking based on academic performance metrics.
- [ ] **ANLY-03**: "Subject of Interest" total distribution across campuses.
- [ ] **ANLY-04**: "Class-wise" and "Year-wise" student/issue counters.
- [ ] **ANLY-05**: Interactive charts for all KPI dimensions.

### Reporting

- [ ] **REPT-01**: Language-wise book distribution report.
- [ ] **REPT-02**: Comparative analysis across branches for key library metrics.
- [ ] **REPT-03**: Specialized categorization for "Fiction & Non-Fiction" stock.
- [ ] **REPT-04**: Popup data tables for all charts to allow granular inspection.

### UI & Theme

- [ ] **UIUX-01**: Implementation of the persistent "Golden-Brown" color scheme.
- [ ] **UIUX-02**: Responsive layout for large-screen administrative monitoring.
- [ ] **UIUX-03**: Consistent iconography and branding aligned with the institution's light-mode UI.

## v2 Requirements

### Advanced Automation
- **AUTO-01**: Monthly automated PDF report generation and email dispatch to HODs.
- **AUTO-02**: Anomaly detection for library issue trends.

### Mobile Optimization
- **MOBI-01**: Dedicated PWA (Progressive Web App) for mobile dashboard access.

## Out of Scope

| Feature | Reason |
|---------|--------|
| Direct Koha Data Entry | System is read-only for analytics to prevent Koha integrity issues. |
| Global Student Registration | Managed directly in Koha or separate SIS; this is a reporting layer. |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| DASH-01 | Phase 1 | Pending |
| DASH-02 | Phase 1 | Pending |
| DASH-04 | Phase 1 | Pending |
| ANLY-01 | Phase 2 | Pending |
| EXPL-01 | Phase 2 | Pending |
| EXPL-02 | Phase 2 | Pending |
| DASH-03 | Phase 3 | Pending |
| ANLY-02 | Phase 3 | Pending |
| ANLY-03 | Phase 3 | Pending |
| ANLY-04 | Phase 3 | Pending |
| REPT-01 | Phase 4 | Pending |
| REPT-02 | Phase 4 | Pending |
| REPT-03 | Phase 4 | Pending |
| EXPL-03 | Phase 5 | Pending |
| ANLY-05 | Phase 5 | Pending |
| REPT-04 | Phase 5 | Pending |
| UIUX-01 | Phase 6 | Pending |
| UIUX-02 | Phase 6 | Pending |
| UIUX-03 | Phase 6 | Pending |

**Coverage:**
- v1 requirements: 19 total
- Mapped to phases: 19
- Unmapped: 0 ✓

---
*Requirements defined: 2026-04-15*
*Last updated: 2026-04-15 after initial definition*
