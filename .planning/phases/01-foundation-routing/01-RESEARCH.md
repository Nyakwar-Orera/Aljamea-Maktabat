# Phase 1: Foundation & Routing - Research

**Date:** 2026-04-15
**Phase:** 1 (Foundation & Routing)

## Parallel Query Engine for Multi-Campus Data

The core challenge is fetching real-time KPIs from 5+ geographically distributed MySQL (Koha) instances without blocking the main Flask thread or exceeding database connection limits.

### Technical Approach: `ThreadPoolExecutor`

Python's `concurrent.futures.ThreadPoolExecutor` is the standard for I/O-bound parallelism in Flask.

**Key Requirements:**
1.  **Thread Safety:** Each thread must maintain its own database connection. `mysql-connector-python`'s `MySQLConnectionPool` is the recommended solution.
2.  **Pool Management:** Since each campus is a different host, the `CAMPUS_REGISTRY` in `config.py` should be used to initialize separate pools or a dynamically managed set of connections.
3.  **Error Handling:** Timeouts are critical. If one campus is down, the entire engine should not block. A timeout of 5-10 seconds per query is recommended.

**Draft Architecture:**
```python
def fetch_branch_kpi(branch_code, query_fn):
    # Obtain connection for specific branch
    # Execute query
    # Return result dict
```

### Super Admin Routing & Blueprint

**Structure:**
- **Blueprint:** `super_admin_bp` registered in `app.py`.
- **URL Prefix:** `/super_admin/` (or similar).
- **Access Control:** A middleware or `before_request` hook on the blueprint to check if `session['admin_type'] == 'super'`.

### UI Branding Baseline

**Primary CSS Variables:**
```css
:root {
  --primary-golden: #C5A059;
  --secondary-golden: #8B6914;
  --background-cream: #F5F0E6;
  --text-dark-brown: #4A3728;
}
```

## Pitfalls to Avoid

1.  **Connection Leakage:** Failure to return connections to the pool in a `finally` block will quickly exhaust `max_connections` on the Koha servers.
2.  **Global Interpreter Lock (GIL):** While I/O operations (SQL queries) happen outside the GIL, the processing of 5 DataFrames in the main thread could cause micro-stutters.
3.  **Template Complexity:** The `dashboard.html` logic is complex; `super_admin.html` should leverage partials to avoid duplication.

---

*Research: 2026-04-15*
