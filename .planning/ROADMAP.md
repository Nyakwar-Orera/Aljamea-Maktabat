# Roadmap: Aljamea-Maktabat Super Admin Dashboard

## Overview

Implementing the "God Eye" global multi-campus dashboard to provide centralized real-time analytics across all 5 campuses. This involves building a high-performance parallel query engine, a visual branch explorer navigation system, and a complete UI overhaul using a golden-brown institutional theme.

## Phases

- [ ] **Phase 1: Foundation & Routing** - Initialize Super Admin blueprint and the parallel query engine.
- [ ] **Phase 2: Global Analytics Interface** - Implement the "God Eye" dashboard layout and Branch Explorer.
- [ ] **Phase 3: Advanced Reporting & Depth** - Add specialized reports (Class/Year/Language) and interactive data popups.
- [ ] **Phase 4: Thematic Overhaul & Global Ranking** - Apply the persistent golden-brown theme and global performance metrics.

## Phase Details

### Phase 1: Foundation & Routing
**Goal**: Establish the technical backbone for multi-campus aggregation.
**Depends on**: Nothing
**Requirements**: [DASH-01, DASH-04, ANLY-01]
**Success Criteria**:
  1. Super Admin blueprint registered and accessible with correct permissions.
  2. Parallel query engine can fetch basic counts from all 5 campuses simultaneously.
  3. No "500 Internal Server Errors" on the initial global route.
**Plans**: 2 plans

Plans:
- [ ] 01-01: Blueprint setup and Access Control login logic.
- [ ] 01-02: Parallel Query Engine implementation using Python's concurrent.futures.

### Phase 2: Global Analytics Interface
**Goal**: Deliver the visual structure and real-time KPI overview.
**Depends on**: Phase 1
**Requirements**: [DASH-02, EXPL-01, EXPL-02]
**Success Criteria**:
  1. Main dashboard layout displays cards for all 5 branches.
  2. Branch Explorer allows visual navigation between campus dashboards.
  3. KPI cards (Active Patrons, Total Books) populate with aggregated data.
**Plans**: 2 plans

Plans:
- [ ] 02-01: God Eye dashboard layout and KPI card components.
- [ ] 02-02: Branch Explorer UI (visual cards/explorer) and routing logic.

### Phase 3: Advanced Reporting & Depth
**Goal**: Implement granular data views and specialized library reports.
**Depends on**: Phase 2
**Requirements**: [DASH-03, REPT-01, REPT-03, REPT-04]
**Success Criteria**:
  1. "Main Page" metrics (Top 10 Books) display global vs branch comparison.
  2. Language-wise and Fiction/Non-Fiction reports are accurate and interactive.
  3. Chart clicks trigger popup data tables with CSV-equivalent row detail.
**Plans**: 2 plans

Plans:
- [ ] 03-01: Global stock and Top 10 Books reporting logic.
- [ ] 03-02: Language/Fiction specialized reports and chart-to-table popup logic.

### Phase 4: Thematic Overhaul & Global Ranking
**Goal**: Finalize the institutional branding and global student performance views.
**Depends on**: Phase 3
**Requirements**: [ANLY-02, ANLY-03, ANLY-04, UIUX-01, UIUX-02, UIUX-03]
**Success Criteria**:
  1. Global Top Students ranking displays cross-campus performance.
  2. Class-wise and Year-wise counters are fully operational.
  3. UI perfectly matches the "Golden-Brown" institutional brand across all super admin pages.
**Plans**: 2 plans

Plans:
- [ ] 04-01: Academic analytics (Top Students, Class/Year distribution).
- [ ] 04-02: Full CSS/Thematic overhaul and final production polish.

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Foundation & Routing | 0/2 | Not started | - |
| 2. Global Analytics Interface | 0/2 | Not started | - |
| 3. Advanced Reporting & Depth | 0/2 | Not started | - |
| 4. Thematic Overhaul & Global Ranking | 0/2 | Not started | - |

---
*Roadmap defined: 2026-04-15*
*Last updated: 2026-04-15 after initialization*
