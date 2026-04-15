# Phase 1: Foundation & Routing - Context

**Gathered:** 2026-04-15
**Status:** Ready for planning

<domain>
## Phase Boundary

This phase establishes the technical backbone for the Aljamea-Maktabat v3.0.0 overhaul. It focuses on the Super Admin infrastructure and high-performance data retrieval.

### Key Deliverables:
- `super_admin.py` Blueprint setup.
- Parallel Query Engine for multi-campus aggregation.
- Access Control for Super Admin routes.

</domain>

<decisions>
## Implementation Decisions

### Routing & Blueprints
- Use `super_admin.py` logic similar to `dashboard.py` but focused on global aggregation.
- All global routes must be strictly enforced for Super Admin role only (Access Control).

### Performance (Parallel Engine)
- Use `concurrent.futures` to fetch data from 5+ MySQL instances simultaneously.
- Targeted KPIs for initial engine: Active Patrons, Total Books, Book Issues (Global vs Branch).

### UI/UX (Theme Baseline)
- Set up the Custom CSS variables for the golden-brown theme (#C5A059, #8B6914, #F5F0E6, #4A3728) in a base template or global CSS file.
- Ensure Bootstrap 5 is correctly integrated for all new components.

</decisions>

<canonical_refs>
## Canonical References

- `config.py` — Multi-campus registry and connection strings.
- `db_koha.py` — Database connection pooling logic.
- `services/koha_queries.py` — Existing SQL logic for library metrics.
- `routes/dashboard.py` — Reference for route structure and context building.

</canonical_refs>

<specifics>
## Specific Ideas

- **Task 1**: Create `super_admin.py` with the skeletal God's Eye dashboard.
- **Task 12**: Optimize parallel queries for multi-campus data aggregation.
- All cards in future phases will need hover effects and animations (setting baseline now).

</specifics>

<deferred>
## Deferred Ideas

- Full dashboard overhauls for HODs, Teachers, and Students (Phases 3 & 4).
- Branch Explorer UI (Phase 2).
- Popup modals and animations (Phase 3 & 4).

</deferred>

---

*Phase: 01-foundation-routing*
*Context gathered: 2026-04-15 after user's v3.0.0 overhaul request*
