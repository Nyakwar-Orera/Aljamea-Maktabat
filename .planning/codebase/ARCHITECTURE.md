# System Architecture

**Analysis Date:** 2026-04-15

## Architectural Pattern: Layered Monolith

The application follows a standard layered approach but is transitioning towards a multi-tenant, multi-campus architecture.

### 1. Presentation Layer (Flask Routes)
- Uses **Flask Blueprints** to organize the application by domain.
- `routes/auth.py`: Session management and multi-campus login.
- `routes/dashboard.py`: Standard admin dashboard.
- `routes/super_admin.py`: The "God Eye" global dashboard for multi-campus aggregation.
- `routes/reports.py`: High-complexity data reporting and file generation.

### 2. Service Layer (Business Logic)
- Encapsulates domain logic in `services/` directory.
- `services/koha_queries.py`: Complex SQL logic to extract data from Koha instances.
- `services/branch_queries.py`: Multi-campus specific data retrieval and session handling.
- `services/marks_service.py`: Academic performance calculations.

### 3. Data Access Layer
- `db_koha.py`: Manages connections to the external MySQL databases (multi-campus).
- `db_app.py`: Manages connection to the local SQLite database.

## Key Design Patterns

### Branch-Aware Data Isolation
- Most service calls accept a `branch_code` or use the `session.get('branch_code')` to filter queries.
- Data is isolated by branch at the database connection level (different MySQL hosts/DBs).

### Multi-Tenant Model (Config-Driven)
- `config.py` uses a `CAMPUS_REGISTRY` dictionary as the central registry for all available branches.
- New branches can be added by updating `config.py` and adding relevant environment variables.

### Blueprint-Based Routing
- Blueprints are registered in `app.py`, allowing for separation of concerns between standard Admin, HOD, and Super Admin flows.

## Core Data Flow

1. **Request:** User requests a dashboard view (standard or global).
2. **Auth:** `auth.py` verifies the session and campus branch.
3. **Route:** The route handler (e.g. `dashboard.py`) calls one or more services.
4. **Service:** The service (e.g. `koha_queries.py`) establishes a connection to the correct branch's MySQL DB.
5. **Database:** SQL queries are executed, data is returned as dictionaries or DataFrames.
6. **Processing:** Service processes data (filtering, formatting, academic calculations).
7. **Response:** Route renders a Jinja2 template with the final data context.

## State Management
- **Session:** Stores `branch_code`, `username`, and `admin_type`.
- **Local DB:** Stores audit logs and persistent local settings.
- **Environment:** Drives all external service connections.

---

*Architecture analysis: 2026-04-15*
