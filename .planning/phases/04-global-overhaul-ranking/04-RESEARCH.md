# Phase 4: Global Thematic Overhaul & Ranking - Research

**Date:** 2026-04-15
**Phase:** 4 (Global Thematic Overhaul & Ranking)

## Global Student Ranking (The "Star of Aljamea")

To rank students across 5 global campuses:
1.  Fetch Top 50 students from each branch by issue counts.
2.  Deduplicate by `TRNO` (The unique identifier across campuses).
3.  Sort by total global issue count.
4.  Display the Global Top 10 on the God's Eye dashboard.

## Global Thematic Overhaul (CSS Variables)

We will standardize the following CSS variables in a centralized block:
- `--color-primary`: `#C5A059` (Golden)
- `--color-secondary`: `#8B6914` (Dark Golden)
- `--color-bg-light`: `#F5F0E6` (Cream)
- `--color-text-dark`: `#4A3728` (Dark Brown)

Successive dashboards (Student, Teacher) will be updated to use these variables for their cards, buttons, and badges.

## AY Toggle Data Persistence

The USER wants the "Active Year vs Past Year" toggle to be persistent.
- **Implementation**: Store `hijri_year` in the `session` object.
- **AJAX**: All dashboard cards will use an endpoint that respects the `hijri_year` in session.

---

*Research: 2026-04-15*
