# Phase 2: Global Analytics Interface - Research

**Date:** 2026-04-15
**Phase:** 2 (Global Analytics Interface)

## Dashboard Layout & Grid

The "God's Eye" dashboard requires a balance between holistic totals (Global KPIs) and per-campus breakdown.

**Proposed Layout:**
1.  **Top Header**: Project title + Global Toggle (AY 1447H vs 1446H).
2.  **Global KPI Row**: 4-5 high-level counts (Total Students, Books, Global Issues).
3.  **Branch Explorer Grid**: 3-column grid (desktop) displaying campus "Passports".
    - Each passport: Flag + Name + Location + Status indicator (Online/Offline) + Quick Stats + "Go to Dashboard" button.
4.  **Baseline Analytics**: A simple chart showing Current Issues per branch.

## Branch Switching Logic for Super Admin

To implement "Click branch to view admin dashboard" (`EXPL-02`):
- When a Super Admin clicks a branch, the system should update `session['branch_code']` and redirect them to the standard `/dashboard` route.
- A "Return to God's Eye" button must be visible in the navbar (this is already partially handled by the God Eye banner in `base.html`).

## UI Component Design: "Animated Cards"

The USER requested "animated cards for key insights".
- **Tech**: CSS transitions on `hover`, `transform: translateY(-5px)`, and subtle `box-shadow` depth.
- **Glassmorphism**: Use `backdrop-filter: blur(10px)` on cards for a premium institutional feel.

---

*Research: 2026-04-15*
