# Phase 3: Advanced Reporting & Depth - Research

**Date:** 2026-04-15
**Phase:** 3 (Advanced Reporting & Depth)

## Global Top Titles Aggregation

Merging "Top 10" lists from 5 branches isn't a simple addition, but for dashboard purposes, we will:
1.  Fetch Top 100 titles from each branch (to ensure overlap coverage).
2.  Aggregate by `biblionumber` (if consistent) or Title + Author.
3.  Sort by total `issue_count`.
4.  Display the Global Top 10.

## Interactive Modals for Charts

The goal is to allow a user to click a segment of a bar/pie chart and see the underlying data table.

**Technical Implementation:**
- **On Click**: Intercept `onClick` in Chart.js.
- **State**: Each chart will have a hidden JSON data blob or an AJAX endpoint.
- **Modal**: Populate a Bootstrap 5 modal containing a `DataTable` for sorting/searching.

## Language & Fiction Classification

- **Language**: Map Koha `biblioitems.itemtype` or `marcxml` fields to Arabic/English/Urdu/Other.
- **Classification**: Use Dewey Decimal ranges (or Koha-specific fields) to distinguish Fiction from Non-Fiction.

---

*Research: 2026-04-15*
