# Changelog

All notable changes to PG Scheduler will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-02-02

### Added

- **Bulk job scheduling**: New `schedule_bulk()` method for high-performance batch inserts
  - Schedule thousands of jobs in a single database transaction
  - 10-100x faster than individual `schedule()` calls for batch operations
  - New `JobSpec` dataclass for defining job specifications
  - Supports all existing job parameters (priority, retries, custom IDs, misfire grace time)
  - Configurable `batch_size` parameter for chunking large batches (default: 1000)
  - Conflict resolution support (IGNORE, REPLACE, RAISE) applied to entire batch
- **Configurable batch claiming**: New `batch_claim_limit` parameter in `Scheduler.__init__` to control max jobs claimed per batch (default: 10)
- **Cron-based scheduling**: Full support for cron expressions in `@periodic` decorator
  - Use `cron` parameter instead of `every` (e.g., `cron="0 0 * * *"` for daily at midnight)
  - Supports all standard cron syntax: `*/15 * * * *`, `0 9-17 * * MON-FRI`, etc.
  - Requires `croniter>=3.0.0` dependency
- **🌍 Timezone support**: Schedule cron jobs in any timezone
  - Use `timezone` parameter with cron expressions (e.g., `timezone="America/New_York"`)
  - Supports both string timezone names and `ZoneInfo` objects
  - Automatically converts to UTC for internal storage
- **Enhanced `@periodic` decorator**: Now supports both interval and cron-based scheduling modes
- **New priority levels**: Added `JobPriority.HIGH` and `JobPriority.LOW` for finer-grained priority control
- **Per-job misfire grace time configuration**: Jobs can now override the scheduler's default grace time
- Support for `misfire_grace_time=None` to disable job expiration entirely

### Changed

- **Code organization improvements**: Major refactoring for better modularity
  - `JobPriority` moved to `pg_scheduler/job_priority.py`
  - Periodic job functionality moved to `pg_scheduler/periodic.py`
  - Conflict resolution strategies moved to `pg_scheduler/conflict_resolution.py`
  - Vacuum policies moved to `pg_scheduler/vacuum.py`
- `Scheduler.__init__` now accepts `Optional[int]` for `misfire_grace_time` parameter
- `Scheduler.schedule()` now accepts optional `misfire_grace_time` parameter for per-job configuration
- `scheduled_jobs` table schema updated to include `misfire_grace_time INTEGER` column

### Migration Notes

- **Backward Compatible**: Existing code will continue to work without changes
- **Automatic Migration**: The `misfire_grace_time` column is automatically added to existing tables
- **No Breaking Changes**: The API is fully backward compatible

## [0.1.0] - 2025-09-29

### Added

#### Core Features
- **Job Scheduler**: PostgreSQL-based async job scheduling system
- **Periodic Jobs**: `@periodic` decorator for recurring tasks with automatic registration
- **Deduplication**: Cross-replica job deduplication using deterministic job IDs
- **Self-rescheduling**: Automatic rescheduling of periodic jobs after completion
- **Priority System**: Support for `JobPriority.NORMAL` and `JobPriority.CRITICAL`
- **Retry Logic**: Configurable retry attempts with exponential backoff

#### Reliability Features
- **Heartbeat Monitoring**: Detect and recover from crashed workers
- **Orphan Recovery**: Automatic cleanup of abandoned jobs
- **Graceful Shutdown**: Wait for active jobs to complete before stopping
- **Lease-based Execution**: Explicit job ownership with timeouts
- **Atomic Job Claiming**: Race-condition-free job distribution

#### Advanced Features
- **Advisory Locks**: Optional PostgreSQL advisory locks for exclusive execution
- **Vacuum Policies**: Configurable cleanup policies for job lifecycle management
- **Conflict Resolution**: Flexible strategies for handling duplicate job IDs
- **Job Management API**: Runtime control of periodic jobs (enable/disable/trigger)

#### Production Features
- **Multi-replica Support**: Safe deployment across multiple worker instances
- **Comprehensive Logging**: Detailed logging with worker identification
- **Error Handling**: Robust error handling and recovery mechanisms
- **Database Optimization**: Efficient queries and indexing strategies

### Documentation
- Comprehensive README with installation and usage examples
- API documentation for all major components
- Production deployment guidelines
- Docker and Kubernetes configuration examples
- Performance tuning recommendations

### Examples
- Basic job scheduling examples
- Periodic job patterns and best practices
- Production configuration templates
- Integration examples (FastAPI, Django)

### Technical Details
- **Python Support**: 3.9+
- **PostgreSQL Support**: 12+
- **Dependencies**: asyncpg for database connectivity
- **Architecture**: Async/await throughout for maximum performance
- **Database Schema**: Optimized table structure with proper indexing

[0.2.0]: https://github.com/miguelrebelo/pg-scheduler/releases/tag/v0.2.0
[0.1.0]: https://github.com/miguelrebelo/pg-scheduler/releases/tag/v0.1.0
