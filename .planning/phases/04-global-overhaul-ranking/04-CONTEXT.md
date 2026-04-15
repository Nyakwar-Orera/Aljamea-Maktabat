# Phase 4: Global Thematic Overhaul & Ranking - Context

**Date:** 2026-04-15
**Phase:** 4 (Global Thematic Overhaul & Ranking)

## Objective
Finalize the Aljamea-Maktabat v3.0 rebranding and implement global academic performance tracking. This phase ensures that the "God's Eye" aesthetic is consistently applied to all users (Students, Teachers, Librarians).

## Scope
- **Global Academic Analytics**: Cross-campus Top Students and Top Marhalas ranking.
- **Thematic Overhaul**: CSS variable synchronization across all dashboards.
- **Role-Specific Dashboards**: Refactoring Student, Teacher, and HOD dashboards to the golden-brown color scheme.
- **Final Visual Polish**: Animations, responsive fixes, and unified layout.

## Requirements
- `ANLY-02`: Global Top Students cross-campus ranking.
- `ANLY-03`: Top Marhala performance across all 5 branches.
- `ANLY-04`: Active Year vs Past Year toggle data persistence.
- `UIUX-01` to `UIUX-04`: Thematic consistency across all user roles.

## Technical Strategy
1.  **Centralized CSS**: Ensure `theme.css` (or equivalent) contains all primary tokens (#C5A059, #8B6914, #F5F0E6).
2.  **Global Ranking Engine**: Update `branch_queries.py` to aggregate student borrower statistics from all branches.
3.  **Template Synchronization**: Iterative updates to all `*_dashboard.html` files to inherit the God's Eye styling.

---

*Context initialized: 2026-04-15*
