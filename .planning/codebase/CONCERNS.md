# Areas of Concern

**Analysis Date:** 2026-04-15

## Critical Concerns

### 1. Lack of Automated Testing
- **Risk:** High. Any refactor to the multi-campus logic (e.g., changes to `db_koha.py` or `config.py`) could introduce regressions that are only caught in production.
- **Impact:** Potential data leakage between branches or dashboard crashes.

### 2. Multi-Campus Single Point of Failure
- **Risk:** Medium. The `CAMPUS_REGISTRY` in `config.py` is the central brain. If it is misconfigured, it affects all branches simultaneously.
- **Impact:** Global application downtime.

### 3. Connection Pool Exhaustion
- **Risk:** Medium. Multi-threaded production traffic might exhaust the MySQL connection pool, especially when aggregating data for the Super Admin "God Eye" dashboard.
- **Impact:** 500 Internal Server Errors under high load.

## Technical Debt

- **Monolithic Services:** `koha_queries.py` is over 100KB and handles many disparate domains. It should be refactored into domain-specific services.
- **Legacy Routes:** Some routes still reference legacy campus-specific logic which has supposedly been unified but might still exist in edge cases.
- **Empty Mock Files:** Some files in `services/` or `routes/` might be placeholders or diagnostic scripts left over from development.

## Maintenance Risks

- **Environment Drift:** The app relies heavily on `.env` variables for 5+ different MySQL instances. Managing these across Dev/Staging/Prod environments is error-prone.
- **Nairobi Core Dependency:** Most logic defaults to Nairobi (AJSN) if branch detection fails, which could lead to confusing UX for users from other branches.

---

*Concern analysis: 2026-04-15*
