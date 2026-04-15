# Debug Session: Verify Multi-Campus DB Connectivity

## Metadata
- status: investigating
- trigger: "Ensure all database connections work for all 5 campuses - AJSN, AJSS, AJSK, AJSM, AJSI."
- created: 2026-04-15
- updated: 2026-04-15

## Symptoms
- **Expected**: Successful MySQL query execution against 5 branch databases (AJSN, AJSS, AJSK, AJSM, AJSI).
- **Actual**: AJSI is currently unconfigured in `.env`. Other branches share a single host and may hit pool limits if not properly managed.
- **Error messages**: N/A
- **Timeline**: New project initialization phase.
- **Reproduction**: Attempt connection to each branch listed in `CAMPUS_REGISTRY`.

## Current Focus
- hypothesis: Connection pooling is not yet branch-aware, and AJSI credentials are missing.
- test: Run a diagnostic script to ping all 5 databases.
- expecting: Success for 4 branches, failure for AJSI.
- next_action: Create a connectivity test script.

## Evidence
- timestamp: 2026-04-15T12:26:00Z
  details: Read `config.py` and `.env`. Confirmed AJSI is inactive and AJSS/AJSK/AJSM share the Nairobi host credentials.

## Eliminated
*(none yet)*
