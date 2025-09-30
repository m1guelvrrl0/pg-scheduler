# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
