# Testing Strategy

**Analysis Date:** 2026-04-15

## Current State: Undocumented / Manual

As of April 2026, the codebase lacks a formal automated testing suite (no `tests/` directory or `pytest` configuration).

### Manual Verification Flow
Currently, features are verified manually through the following steps:
1. **Local Execution:** Running `app.py` or `npm run dev` (if applicable) and navigating the UI.
2. **Database Integrity:** Checking `app.log` for SQL query errors and verifying data consistency in the "God Eye" dashboard against raw Koha results.
3. **Multi-Campus Isolation:** Logging in as different campus admins to ensure data does not leak between branches.

## Gap Analysis

### 1. Unit Testing (Missing)
- **Target:** `services/` layer (e.g., `marks_service.py`, `koha_queries.py` parsing logic).
- **Tooling:** Recommended use of `pytest` with `pytest-mock`.

### 2. Integration Testing (Missing)
- **Target:** Database connection logic in `db_koha.py`.
- **Challenge:** Requires mock MySQL/Koha instances or a dedicated testing database.

### 3. UI / E2E Testing (Missing)
- **Target:** Dashboard rendering and file export functionality.
- **Tooling:** Recommended use of Playwright or Selenium.

## Future Recommendations

- **Immediate Goal:** Implement basic unit tests for the `services/` layer calculations.
- **CI/CD:** Integrate a GitHub Action to run linting and future tests on every push.
- **Regression Testing:** Focus on verifying that changes to one branch's logic do not break others in the multi-campus registry.

---

*Testing analysis: 2026-04-15*
