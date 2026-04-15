# Phase 3: Advanced Reporting & Depth - Context

**Date:** 2026-04-15
**Phase:** 3 (Advanced Reporting & Depth)

## Objective
Implement granular data visualizations and interactive drill-down capabilities. This phase adds "depth" to the dashboard by allowing users to inspect the underlying data behind the charts.

## Scope
- **Global Depth**: Top 10 Books comparison (Branch vs. Global overview).
- **Specialized Reports**: Language-wise analytics and Fiction/Non-Fiction breakdown.
- **Interactive Modals**: Implementation of popup data tables for all Chart.js visualizations.
- **Data Export Strategy**: Ensure popup tables match CSV export depth.

## Requirements
- `DASH-03`: Global stock and Top 10 Books aggregation.
- `REPT-01`: Language distribution report (Arabic/English/Other).
- `REPT-03`: Fiction vs Non-Fiction classification overview.
- `REPT-04`: Chart-to-table popup logic for granular inspection.

## Technical Strategy
1.  **Backend Data Refinement**: Update `branch_queries.py` to aggregate more specific data points (Top Titles by Language, etc.).
2.  **Shared UI Components**: Create `templates/components/popups.html` for reusable modal structures.
3.  **Javascript Bridge**: Create a global JS handler to intercept chart clicks and load data into modals.

---

*Context initialized: 2026-04-15*
