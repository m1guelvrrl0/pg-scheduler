# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **New priority levels**: Added `JobPriority.HIGH` and `JobPriority.LOW` for finer-grained priority control
  - `CRITICAL` (1) - Highest priority
  - `HIGH` (3) - High priority (new!)
  - `NORMAL` (5) - Default priority
  - `LOW` (8) - Low priority (new!)
- **Per-job misfire grace time configuration**: Jobs can now override the scheduler's default grace time via the `misfire_grace_time` parameter in `schedule()`
- Support for `misfire_grace_time=None` to disable job expiration entirely (APScheduler-like behavior)
- Sentinel pattern (`_UNSET`) to distinguish between "parameter not specified" and "explicitly set to None"
- Database migration support for `misfire_grace_time` column (automatically applied to existing databases)
- Comprehensive test suite for misfire grace time functionality with Docker Compose multi-container testing
- Documentation explaining misfire grace time behavior and examples

### Changed
- `Scheduler.__init__` now accepts `Optional[int]` for `misfire_grace_time` parameter (default: 300 seconds)
  - `None` = no jobs expire (global setting)
  - Integer = default grace period in seconds
- `Scheduler.schedule()` now accepts optional `misfire_grace_time` parameter for per-job configuration
  - Not specified = use scheduler default
  - Integer = custom grace period for this job
  - `None` = this job never expires
- `scheduled_jobs` table schema updated to include `misfire_grace_time INTEGER` column
- Job expiration logic updated to check per-job `misfire_grace_time` values
- Updated README with comprehensive misfire grace time examples and use cases

### Migration Notes
- **Backward Compatible**: Existing code will continue to work without changes
- **Automatic Migration**: The `misfire_grace_time` column is automatically added to existing `scheduled_jobs` tables
- **Default Behavior**: Jobs without explicit `misfire_grace_time` will use the scheduler's default (300 seconds, same as before)
- **No Breaking Changes**: The API is fully backward compatible; new parameter is optional

### Technical Details
- Added `_UNSET` sentinel object to differentiate between default and explicit `None` values
- Per-job `misfire_grace_time` is stored in the database and evaluated during job claiming
- Jobs with `NULL` (Python `None`) in `misfire_grace_time` column are never expired

## [0.1.0] - 2025-09-29

### Added
- Initial release of PG Scheduler
- Core job scheduling functionality with PostgreSQL backend
- `@periodic` decorator for recurring jobs with deduplication
- Cross-replica job deduplication using deterministic job IDs
- Self-rescheduling periodic jobs
- PostgreSQL advisory lock support for exclusive execution
- Job priority system (NORMAL, CRITICAL)
- Comprehensive retry logic with exponential backoff
- Vacuum policies for automatic job cleanup
- Graceful shutdown and error handling
- Orphan job recovery for crashed workers
- Heartbeat monitoring and lease-based execution
- Atomic job claiming with race condition protection
- Conflict resolution strategies (RAISE, IGNORE, REPLACE)
- Job management API (enable/disable, manual triggering, status monitoring)
- Comprehensive logging and monitoring
- Production-ready reliability features
- Full async/await support with asyncpg
- Docker and multi-replica deployment support

### Features
- **Scheduler Class**: Main job scheduling engine
- **Periodic Jobs**: `@periodic` decorator with automatic registration
- **Deduplication**: Window-based deduplication across multiple replicas  
- **Priority Queues**: Support for job priorities and execution ordering
- **Vacuum System**: Configurable cleanup policies for job lifecycle management
- **Reliability**: Heartbeat monitoring, orphan recovery, graceful shutdown
- **Management API**: Runtime job control and monitoring
- **Advisory Locks**: Optional PostgreSQL advisory locks for exclusive execution

### Documentation
- Comprehensive README with examples
- API documentation for all major components
- Production deployment guidelines
- Docker configuration examples
- Multi-replica setup instructions
