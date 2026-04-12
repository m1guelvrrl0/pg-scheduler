# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.3] - 2026-03-30

### Added
- **Configurable polling backoff**: New `PollingConfig` dataclass for fine-grained control over job listener polling behavior
  - `min_interval` -- sleep between polls when jobs are being claimed (default: 0.05s)
  - `max_interval` -- upper bound for backoff when idle (default: 2.0s)
  - `backoff_multiplier` -- exponential backoff factor when no jobs are found (default: 1.5)
  - `idle_start_interval` -- initial idle interval before backoff kicks in (default: 0.5s)
  - `semaphore_full_interval` -- sleep when all concurrency slots are occupied (default: 1.0s)
  - `jitter` -- toggle random jitter to prevent thundering herd (default: True)
- New `polling_config` parameter on `Scheduler.__init__` (uses sensible defaults when omitted)

### Changed
- Job listener now uses exponential backoff instead of fixed sleep intervals
  - Polls quickly after claiming jobs, backs off gradually when idle, resets on activity
  - Configurable via `PollingConfig` (previously hardcoded to 0.05s busy / 2.0s idle)
- Semaphore-full sleep is now configurable via `PollingConfig.semaphore_full_interval` (previously hardcoded to 1.0s)
- Error recovery sleep in listener now uses `max_interval` instead of a hardcoded 5s

### Migration Notes
- **Backward Compatible**: Default `PollingConfig` values match the previous hardcoded behavior
- **No Breaking Changes**: `polling_config` is optional; existing code works without modification

## [0.2.2] - 2026-03-16

### Changed

- Updated and streamlined README / PyPI page

## [0.2.1] - 2026-03-15

### Fixed

- **Per-job misfire_grace_time ignored when scheduler default is None**: The job expiration logic was entirely skipped when the scheduler's `misfire_grace_time` was set to `None`, causing jobs with explicit per-job `misfire_grace_time` to never expire. Removed the Python-side guard so the SQL query now always runs and relies on `misfire_grace_time IS NOT NULL` to correctly expire only jobs with an explicit grace time.
- **get_periodic_job_status crashes for cron-based periodic jobs**: Calling `get_periodic_job_status` on a cron-based job raised `AttributeError` because it unconditionally accessed `config.interval.total_seconds()`. Now correctly handles both interval-based and cron-based jobs, returning `interval` or `cron`/`timezone` fields as appropriate.

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
- **Timezone support**: Schedule cron jobs in any timezone
  - Use `timezone` parameter with cron expressions (e.g., `timezone="America/New_York"`)
  - Supports both string timezone names and `ZoneInfo` objects
  - Automatically converts to UTC for internal storage
- **Enhanced `@periodic` decorator**: Now supports both interval and cron-based scheduling modes
  - Interval-based: `@periodic(every=timedelta(minutes=15))`
  - Cron-based: `@periodic(cron="0 0 * * *")`
  - Cron with timezone: `@periodic(cron="0 3 * * SUN", timezone="America/New_York")`
- **New priority levels**: Added `JobPriority.HIGH` and `JobPriority.LOW` for finer-grained priority control
  - `CRITICAL` (1) - Highest priority
  - `HIGH` (3) - High priority
  - `NORMAL` (5) - Default priority
  - `LOW` (8) - Low priority
- **Per-job misfire grace time configuration**: Jobs can now override the scheduler's default grace time via the `misfire_grace_time` parameter in `schedule()`
- Support for `misfire_grace_time=None` to disable job expiration entirely
- Sentinel pattern (`_UNSET`) to distinguish between "parameter not specified" and "explicitly set to None"
- Database migration support for `misfire_grace_time` column (automatically applied to existing databases)

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

[0.2.3]: https://github.com/m1guelvrrl0/pg-scheduler/releases/tag/v0.2.3
[0.2.2]: https://github.com/m1guelvrrl0/pg-scheduler/releases/tag/v0.2.2
[0.2.1]: https://github.com/m1guelvrrl0/pg-scheduler/releases/tag/v0.2.1
[0.2.0]: https://github.com/m1guelvrrl0/pg-scheduler/releases/tag/v0.2.0
[0.1.0]: https://github.com/m1guelvrrl0/pg-scheduler/releases/tag/v0.1.0
