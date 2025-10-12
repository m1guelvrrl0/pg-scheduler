# PG Scheduler

[![PyPI version](https://badge.fury.io/py/pg-scheduler.svg)](https://badge.fury.io/py/pg-scheduler)
[![Python versions](https://img.shields.io/pypi/pyversions/pg-scheduler.svg)](https://pypi.org/project/pg-scheduler/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A simple lightweight async first job scheduler for Python that uses PostgreSQL to allow you to schedule and manage the execution of asynchronous tasks.

It's heavily inspired by APScheduler in its API but **horizontally scalable** and much more focused in the features it provides and technologies it uses.

It makes minimal assumptions about how you run it other than that you are in an asyncIO enviroment, use Postgresql and asyncpg. Due to this it can integrate very easily with other technologies.

This library is focused on easy to use and deploy scheduler and not being an all comprehensive job runner so if you need something more advanced and complicated like workers queues and worker producer patterns I believe these are already very well covered in the python ecosystem and I would suggest you to look at those instead.

![PostgreSQL for Everything](docs/images/postgres-meme.webp)

## ⚠️ Project Status

This project is currently in **early development** (v0.1.0). While functional, please note:
- The API may change in future versions
- Recommended for testing and development environments
- Production use should be carefully evaluated

## ✨ Key Features

- **🔄 Periodic Jobs**: Simple `@periodic` decorator for recurring tasks
- **⏰ Cron Scheduling**: Full cron expression support (e.g., `cron="0 0 * * *"`)
- **🌍 Timezone Support**: Schedule jobs in any timezone using `zoneinfo`
- **🔒 Deduplication**: Guarantees exactly one execution per window across replicas  
- **⚡ Self-Rescheduling**: Jobs automatically schedule their next execution
- **🛡️ Advisory Locks**: Optional PostgreSQL advisory locks for exclusive execution
- **🎯 Priority Queues**: Support for job priorities (CRITICAL, HIGH, NORMAL, LOW)
- **🧹 Vacuum Policies**: Automatic cleanup of completed jobs
- **💪 Reliability**: Graceful shutdown, error handling, and orphan recovery

## 📦 Installation

```bash
pip install pg-scheduler
```

### Requirements

- Python 3.9+
- PostgreSQL 12+
- asyncpg
- croniter (for cron-based scheduling)

## 🚀 Quick Start

### Basic Job Scheduling

```python
import asyncio
import asyncpg
from datetime import datetime, timedelta, UTC
from pg_scheduler import Scheduler, JobPriority

async def send_email(recipient: str, subject: str):
    """Example job function"""
    print(f"📧 Sending email to {recipient}: {subject}")
    await asyncio.sleep(1)  # Simulate async work
    print(f"✅ Email sent to {recipient}")

async def main():
    # Create database connection pool
    db_pool = await asyncpg.create_pool(
        user='scheduler',
        password='password',
        database='scheduler_db',
        host='localhost',
        port=5432
    )
    
    # Initialize scheduler
    scheduler = Scheduler(db_pool=db_pool, max_concurrent_jobs=10)
    await scheduler.start()
    
    try:
        # Schedule a job
        job_id = await scheduler.schedule(
            send_email,
            execution_time=datetime.now(UTC) + timedelta(minutes=5),
            args=("user@example.com", "Welcome!"),
            priority=JobPriority.NORMAL,
            max_retries=3
        )
        print(f"Scheduled job: {job_id}")
        
        # Keep running
        await asyncio.sleep(300)  # Run for 5 minutes
        
    finally:
        await scheduler.shutdown()
        await db_pool.close()

if __name__ == "__main__":
    asyncio.run(main())
```

### Periodic Jobs with @periodic Decorator

#### Interval-Based Scheduling

```python
from datetime import timedelta
from pg_scheduler import periodic, JobPriority

@periodic(every=timedelta(minutes=15))
async def cleanup_temp_files():
    """Clean up temporary files every 15 minutes"""
    print("🧹 Cleaning up temporary files...")
    # Your cleanup logic here
    print("✅ Cleanup completed")

@periodic(every=timedelta(hours=1), priority=JobPriority.CRITICAL, max_retries=3)
async def generate_hourly_report():
    """Generate hourly reports with high priority and retries"""
    print("📊 Generating hourly report...")
    # Your report generation logic here
    print("✅ Report generated")
```

#### Cron-Based Scheduling

```python
# Daily backup at midnight UTC
@periodic(cron="0 0 * * *")
async def daily_backup():
    print("🔄 Running daily backup...")

# Every Sunday at 3am in New York timezone
@periodic(cron="0 3 * * SUN", timezone="America/New_York")
async def weekly_report():
    print("📊 Generating weekly report (New York time)...")

# Every 15 minutes using cron syntax
@periodic(cron="*/15 * * * *")
async def frequent_check():
    print("✅ Running frequent health check...")

# Business hours only (9am-5pm Mon-Fri) in London
@periodic(cron="0 9-17 * * MON-FRI", timezone="Europe/London", priority=JobPriority.HIGH)
async def business_hours_task():
    print("💼 Running business hours task...")

# First day of every month at 2am
@periodic(cron="0 2 1 * *")
async def monthly_cleanup():
    print("🧹 Running monthly cleanup...")
```

#### Advisory Locks for Exclusive Execution

```python
# Advisory locks for exclusive execution
@periodic(every=timedelta(minutes=30), use_advisory_lock=True)
async def exclusive_maintenance():
    """Exclusive operation - only one instance across entire cluster"""
    print("🔒 Running exclusive maintenance...")
    # Your maintenance logic here
    print("✅ Maintenance completed")
```

**Note**: Most jobs don't need `use_advisory_lock=True`. Use it only when you need absolute exclusivity across all workers (e.g., database migrations, leader coordination).

## 🔄 Periodic Jobs Features

The `@periodic` decorator provides powerful recurring job functionality with **built-in cross-node deduplication**:

- **Cross-Node Deduplication**: Automatically prevents duplicate executions across multiple nodes sharing the same database
- **Automatic Registration**: Jobs are automatically registered when decorated
- **Self-rescheduling**: Automatically schedules the next execution after completion
- **Priority Support**: Use `JobPriority.NORMAL` or `JobPriority.CRITICAL`
- **Retry Logic**: Configure `max_retries` for failed executions
- **Advisory Locks**: Optional exclusive execution (rarely needed - see note above)
- **Management**: Enable/disable jobs dynamically

### Decorator Parameters

```python
@periodic(
    every=timedelta(minutes=15),        # Required: execution interval
    use_advisory_lock=False,            # Optional: exclusive execution (default dedup is usually sufficient)
    priority=JobPriority.NORMAL,        # Optional: job priority
    max_retries=0,                      # Optional: retry attempts on failure
    job_name=None,                      # Optional: custom job name (auto-generated)
    dedup_key=None,                     # Optional: custom dedup key (auto-generated)
    enabled=True                        # Optional: whether job is enabled
)
```

### Cross-Node Deduplication

The `@periodic` decorator automatically prevents duplicate executions across multiple nodes:

```python
# Multiple nodes running the same code
@periodic(every=timedelta(minutes=5))
async def cleanup_task():
    print("Running cleanup...")

# What happens:
# - Node 1: Schedules job for 10:05 → ✅ Success
# - Node 2: Tries to schedule same job → ❌ "Already exists, ignoring"  
# - Node 3: Tries to schedule same job → ❌ "Already exists, ignoring"
# - Result: Only Node 1 executes the cleanup at 10:05
```

**No configuration needed** - this works automatically for any nodes sharing the same PostgreSQL database.

### Management API

```python
# Get all periodic jobs
periodic_jobs = scheduler.get_periodic_jobs()

# Get status of a specific job
status = scheduler.get_periodic_job_status(dedup_key)

# Enable/disable jobs
scheduler.enable_periodic_job(dedup_key)
scheduler.disable_periodic_job(dedup_key)

# Manually trigger a job
job_id = await scheduler.trigger_periodic_job(dedup_key)
```

## 🎯 Job Scheduling Features

### Priority Support

Four priority levels available (lower number = higher priority):
- `JobPriority.CRITICAL` - Highest priority (executes first)
- `JobPriority.HIGH` - High priority
- `JobPriority.NORMAL` - Default priority
- `JobPriority.LOW` - Low priority (executes last)

Jobs are executed in priority order, with higher priority jobs running before lower priority ones.

### Conflict Resolution
Handle duplicate job IDs with flexible strategies:
- `ConflictResolution.RAISE` (default): Raise error for duplicates
- `ConflictResolution.IGNORE`: Ignore new job, return existing ID
- `ConflictResolution.REPLACE`: Update existing job with new parameters

### Retry Logic
- Configure `max_retries` for failed jobs
- Exponential backoff between retries
- Comprehensive error handling

## 🧹 Vacuum Policies

Automatic cleanup of completed jobs with flexible policies:

```python
from pg_scheduler import VacuumConfig, VacuumPolicy

# Configure cleanup policies
vacuum_config = VacuumConfig(
    completed=VacuumPolicy.after_days(1),    # Clean completed jobs after 1 day
    failed=VacuumPolicy.after_days(7),       # Keep failed jobs for 7 days
    cancelled=VacuumPolicy.after_days(3),    # Clean cancelled jobs after 3 days
    interval_minutes=60,                     # Run vacuum every hour
    track_metrics=True                       # Store vacuum statistics
)

scheduler = Scheduler(db_pool, vacuum_config=vacuum_config)
```

## 🛡️ Reliability Features

### Built-in Reliability
- **Lease-based Execution**: Explicit job ownership with timeouts
- **Heartbeat Monitoring**: Detect and recover from crashed workers
- **Atomic Job Claiming**: Race-condition-free job distribution
- **Orphan Recovery**: Automatic cleanup of abandoned jobs
- **Graceful Shutdown**: Waits for active jobs to complete

### Deduplication
- **Cross-replica Safety**: Same job won't run twice across multiple workers
- **Deterministic Job IDs**: Based on function signature and parameters
- **Window-based Deduplication**: Prevents duplicate executions in time windows

## 🔧 Configuration

### Scheduler Options

```python
scheduler = Scheduler(
    db_pool=db_pool,
    max_concurrent_jobs=25,      # Maximum concurrent job execution
    misfire_grace_time=300,      # Seconds before jobs expire (5 minutes)
    vacuum_enabled=True,         # Enable automatic job cleanup
    vacuum_config=vacuum_config  # Custom vacuum policies
)
```

## 📊 Database Schema

The scheduler automatically creates the required PostgreSQL table:

```sql
CREATE TABLE scheduled_jobs (
    job_id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    job_name TEXT NOT NULL,
    execution_time TIMESTAMPTZ NOT NULL,
    status TEXT DEFAULT 'pending',
    task_data JSONB,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    last_heartbeat TIMESTAMPTZ,
    lease_until TIMESTAMPTZ,
    priority INTEGER DEFAULT 5,
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 0,
    worker_id TEXT,
    error_message TEXT
);
```

## 🚀 Production Deployment

### Docker Example

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
CMD ["python", "app.py"]
```

### Environment Variables

```bash
DATABASE_URL=postgresql://user:pass@localhost:5432/scheduler_db
MAX_CONCURRENT_JOBS=25
MISFIRE_GRACE_TIME=300
VACUUM_ENABLED=true
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📋 TODO / Roadmap

### Planned Features

- **Web UI**: Dashboard for monitoring jobs and scheduler status
- **Job Dependencies**: Define job execution order and dependencies
- **Distributed Locks**: Additional locking mechanisms for complex workflows

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🔗 Links

- **PyPI**: https://pypi.org/project/pg-scheduler/
- **GitHub**: https://github.com/m1guelvrrl0/pg-scheduler
- **Documentation**: https://github.com/m1guelvrrl0/pg-scheduler#readme
- **Issues**: https://github.com/m1guelvrrl0/pg-scheduler/issues