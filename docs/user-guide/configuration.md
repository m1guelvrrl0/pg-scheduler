# Configuration

This guide covers all configuration options for PG Scheduler.

## Scheduler Initialization

### Basic Configuration

```python
from pg_scheduler import Scheduler
import asyncpg

db_pool = await asyncpg.create_pool(
    user='scheduler',
    password='password',
    database='scheduler_db',
    host='localhost'
)

scheduler = Scheduler(
    db_pool=db_pool,
    max_concurrent_jobs=25,
    misfire_grace_time=300,
    vacuum_enabled=True,
    vacuum_config=None  # Uses defaults
)
```

### Configuration Parameters

#### `db_pool` (required)
- **Type**: `asyncpg.Pool`
- **Description**: PostgreSQL connection pool
- **Example**: `await asyncpg.create_pool(...)`

#### `max_concurrent_jobs`
- **Type**: `int`
- **Default**: `25`
- **Description**: Maximum number of jobs that can run simultaneously
- **Recommendation**: Set based on your workload and resource constraints

```python
# Low concurrency for CPU-intensive tasks
scheduler = Scheduler(db_pool, max_concurrent_jobs=5)

# High concurrency for I/O-bound tasks
scheduler = Scheduler(db_pool, max_concurrent_jobs=50)
```

#### `misfire_grace_time`
- **Type**: `Optional[int]`
- **Default**: `300` (5 minutes)
- **Description**: Seconds after `execution_time` before jobs expire
- **Special Values**:
  - Integer: Grace period in seconds
  - `None`: Jobs never expire (will run whenever possible)

**Use Cases:**

```python
# Standard grace period (default)
scheduler = Scheduler(db_pool, misfire_grace_time=300)  # 5 minutes

# Strict deadline enforcement
scheduler = Scheduler(db_pool, misfire_grace_time=60)  # 1 minute

# No expiration - always run eventually
scheduler = Scheduler(db_pool, misfire_grace_time=None)
```

See [Misfire Grace Time](#misfire-grace-time) section for detailed behavior.

#### `vacuum_enabled`
- **Type**: `bool`
- **Default**: `True`
- **Description**: Enable automatic cleanup of completed/failed jobs
- **Recommendation**: Keep enabled in production

#### `vacuum_config`
- **Type**: `Optional[VacuumConfig]`
- **Default**: `None` (uses default policies)
- **Description**: Custom vacuum policies for job cleanup
- **See**: [Vacuum Policies](vacuum-policies.md) for details

## Misfire Grace Time

Misfire grace time controls when late jobs should expire instead of running. This is similar to APScheduler's misfire grace time behavior.

### Global Configuration

Set a default grace time for all jobs:

```python
# 5 minute grace period (default)
scheduler = Scheduler(db_pool, misfire_grace_time=300)

# Strict 30 second grace period
scheduler = Scheduler(db_pool, misfire_grace_time=30)

# No expiration - jobs always run eventually
scheduler = Scheduler(db_pool, misfire_grace_time=None)
```

### Per-Job Configuration

Override the default grace time for specific jobs:

```python
# Use scheduler's default grace time
await scheduler.schedule(
    my_function,
    execution_time=datetime.now(UTC) + timedelta(minutes=5)
    # misfire_grace_time not specified
)

# Custom 60 second grace period for this job
await scheduler.schedule(
    my_function,
    execution_time=datetime.now(UTC) + timedelta(minutes=5),
    misfire_grace_time=60
)

# This specific job never expires
await scheduler.schedule(
    my_function,
    execution_time=datetime.now(UTC) + timedelta(minutes=5),
    misfire_grace_time=None
)
```

### Behavior Matrix

| Scheduler Default | Job Parameter | Effective Behavior |
|------------------|---------------|-------------------|
| `300` (default) | Not specified | Expires 300s after execution_time |
| `300` | `60` | Expires 60s after execution_time |
| `300` | `None` | Never expires |
| `None` | Not specified | Never expires |
| `None` | `60` | Expires 60s after execution_time |

### When to Use Each Setting

**Short Grace Time (30-120 seconds):**
- Time-sensitive operations (e.g., real-time notifications)
- Jobs that become irrelevant if late (e.g., flash sale notifications)
- High-frequency jobs where late execution causes queuing issues

```python
# Flash sale notification - must arrive on time
await scheduler.schedule(
    send_flash_sale_notification,
    execution_time=sale_start_time,
    misfire_grace_time=30  # Only 30 seconds to send
)
```

**Medium Grace Time (5-15 minutes, default):**
- General-purpose background tasks
- Report generation
- Scheduled maintenance tasks
- Most typical use cases

```python
# Daily report - can be a few minutes late
await scheduler.schedule(
    generate_daily_report,
    execution_time=datetime.now(UTC).replace(hour=9, minute=0),
    misfire_grace_time=300  # 5 minutes is fine
)
```

**Long Grace Time (30+ minutes):**
- Batch processing jobs
- Non-urgent maintenance tasks
- Jobs with flexible timing requirements

```python
# Weekly cleanup - timing not critical
await scheduler.schedule(
    cleanup_old_data,
    execution_time=next_sunday_midnight,
    misfire_grace_time=3600  # 1 hour grace period
)
```

**No Expiration (`None`):**
- Critical operations that must complete eventually
- Jobs where late execution is better than no execution
- Idempotent operations safe to run at any time

```python
# User data export - must complete regardless of timing
await scheduler.schedule(
    export_user_data,
    execution_time=datetime.now(UTC) + timedelta(hours=1),
    misfire_grace_time=None  # Always run eventually
)
```

### Database Storage

Misfire grace time is stored in the `scheduled_jobs` table:

```sql
SELECT 
    job_name,
    execution_time,
    misfire_grace_time,  -- NULL = never expires, INT = grace seconds
    status
FROM scheduled_jobs;
```

### Migration Notes

- **Backward Compatible**: Existing code works without changes
- **Automatic Migration**: The `misfire_grace_time` column is automatically added to existing databases
- **Default Behavior**: Jobs without explicit `misfire_grace_time` use the scheduler's default (300 seconds)

## Job Scheduling Options

### Priority

PG Scheduler supports four priority levels (lower number = higher priority):

```python
from pg_scheduler import JobPriority

# Critical priority (highest - runs first)
await scheduler.schedule(
    urgent_function,
    execution_time=run_time,
    priority=JobPriority.CRITICAL  # Priority: 1
)

# High priority
await scheduler.schedule(
    important_function,
    execution_time=run_time,
    priority=JobPriority.HIGH  # Priority: 3
)

# Normal priority (default)
await scheduler.schedule(
    regular_function,
    execution_time=run_time,
    priority=JobPriority.NORMAL  # Priority: 5
)

# Low priority (lowest - runs last)
await scheduler.schedule(
    cleanup_function,
    execution_time=run_time,
    priority=JobPriority.LOW  # Priority: 8
)
```

**Priority Ordering:**
Jobs are executed in ascending priority order:
1. `CRITICAL` (1) → Executes first
2. `HIGH` (3)
3. `NORMAL` (5) - Default
4. `LOW` (8) → Executes last

Within the same priority level, jobs are executed in order of their `execution_time`.

### Retry Logic

```python
# Automatic retries with exponential backoff
await scheduler.schedule(
    flaky_api_call,
    execution_time=run_time,
    max_retries=3  # Retry up to 3 times on failure
)
```

### Conflict Resolution

```python
from pg_scheduler import ConflictResolution

# Raise error if job_id exists (default)
await scheduler.schedule(
    my_function,
    execution_time=run_time,
    job_id="custom-id",
    conflict_resolution=ConflictResolution.RAISE
)

# Ignore new job if job_id exists
await scheduler.schedule(
    my_function,
    execution_time=run_time,
    job_id="custom-id",
    conflict_resolution=ConflictResolution.IGNORE
)

# Replace existing job with new parameters
await scheduler.schedule(
    my_function,
    execution_time=run_time,
    job_id="custom-id",
    conflict_resolution=ConflictResolution.REPLACE
)
```

## Environment-Based Configuration

### Production Example

```python
import os
from pg_scheduler import Scheduler, VacuumConfig, VacuumPolicy

# Read from environment variables
MAX_JOBS = int(os.getenv('MAX_CONCURRENT_JOBS', '25'))
MISFIRE_GRACE = int(os.getenv('MISFIRE_GRACE_TIME', '300'))
VACUUM_INTERVAL = int(os.getenv('VACUUM_INTERVAL_MINUTES', '60'))

scheduler = Scheduler(
    db_pool=db_pool,
    max_concurrent_jobs=MAX_JOBS,
    misfire_grace_time=MISFIRE_GRACE,
    vacuum_enabled=True,
    vacuum_config=VacuumConfig(
        completed=VacuumPolicy.after_days(1),
        failed=VacuumPolicy.after_days(7),
        cancelled=VacuumPolicy.after_days(3),
        interval_minutes=VACUUM_INTERVAL
    )
)
```

### Docker Compose Example

```yaml
services:
  scheduler:
    image: my-scheduler:latest
    environment:
      - MAX_CONCURRENT_JOBS=50
      - MISFIRE_GRACE_TIME=300
      - VACUUM_INTERVAL_MINUTES=30
      - DATABASE_URL=postgresql://user:pass@db:5432/scheduler_db
```

## Best Practices

### 1. Set Appropriate Concurrency

```python
# CPU-bound tasks
scheduler = Scheduler(db_pool, max_concurrent_jobs=cpu_count())

# I/O-bound tasks
scheduler = Scheduler(db_pool, max_concurrent_jobs=50)

# Mixed workload
scheduler = Scheduler(db_pool, max_concurrent_jobs=25)
```

### 2. Configure Grace Time Based on Job Nature

```python
# Time-sensitive: short grace time
misfire_grace_time=30

# General tasks: medium grace time (default)
misfire_grace_time=300

# Critical tasks: no expiration
misfire_grace_time=None
```

### 3. Use Vacuum Policies

```python
from pg_scheduler import VacuumConfig, VacuumPolicy

vacuum_config = VacuumConfig(
    completed=VacuumPolicy.after_days(1),    # Clean up successes quickly
    failed=VacuumPolicy.after_days(7),       # Keep failures for debugging
    cancelled=VacuumPolicy.after_days(3),    # Clean up cancellations
    interval_minutes=60                       # Run cleanup hourly
)
```

### 4. Monitor and Tune

```python
# Log scheduler metrics
logger.info(f"Active jobs: {len(scheduler.active_jobs)}")
logger.info(f"Worker ID: {scheduler.worker_id}")

# Check periodic job status
for dedup_key in scheduler.get_periodic_jobs():
    status = scheduler.get_periodic_job_status(dedup_key)
    logger.info(f"Periodic job {status['job_name']}: {status['enabled']}")
```

## Next Steps

- Learn about [Vacuum Policies](vacuum-policies.md)
- Explore [Reliability Features](reliability.md)
- Check [Troubleshooting](troubleshooting.md) for common issues

