# Phase 2: Global Analytics Interface - Context

**Date:** 2026-04-15
**Phase:** 2 (Global Analytics Interface)

## Objective
Implement the visual "God's Eye" dashboard layout and the Branch Explorer navigation system. This phase delivers the first fully functional global overview with real-time aggregated data.

## Scope
- **God's Eye Dashboard (`templates/super_admin/dashboard.html`)**: Implementation of the global KPI overview and branch-specific cards.
- **Branch Explorer**: A visual grid of all 5 campuses with status indicators and navigation buttons.
- **KPI Card Components**: Reusable animated cards for key metrics (Patrons, Books, Issues, Overdues).
- **Interactive Visualizations**: Baseline charts for cross-campus comparisons.

## Requirements
- `DASH-02`: Global dashboard layout with 5 branch cards.
- `EXPL-01`: Branch explorer for visual navigation.
- `EXPL-02`: Click branch to view individual campus admin dashboard.

## Technical Strategy
1.  **Backend Integration**: The `god_eye()` route in `super_admin.py` will call `get_all_branches_summary()` and `get_global_aggregate()` from `services/branch_queries.py`.
2.  **Frontend Layout**: Use a mobile-responsive grid layout.
3.  **UI Brand**: Apply the initial golden-brown CSS variables to these new components.
4.  **Branch Explorer**: Build a visually engaging "Passport" style grid of campuses.

---

*Context initialized: 2026-04-15*
