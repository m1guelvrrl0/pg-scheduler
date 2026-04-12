# PG Scheduler

[![PyPI version](https://badge.fury.io/py/pg-scheduler.svg)](https://badge.fury.io/py/pg-scheduler)
[![Python versions](https://img.shields.io/pypi/pyversions/pg-scheduler.svg)](https://pypi.org/project/pg-scheduler/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A simple lightweight async first job scheduler for Python that uses PostgreSQL to allow you to schedule and manage the execution of asynchronous tasks.

It's heavily inspired by APScheduler in its API but **horizontally scalable** and much more focused in the features it provides and technologies it uses.

It makes minimal assumptions about how you run it other than that you are in an asyncIO enviroment, use Postgresql and asyncpg. Due to this it can integrate very easily with other technologies.

This library is focused on easy to use and deploy scheduler and not being an all comprehensive job runner so if you need something more advanced and complicated like workers queues and worker producer patterns I believe these are already very well covered in the python ecosystem and I would suggest you to look at those instead.

## Features

- **Periodic jobs** via `@periodic` decorator (interval or cron expressions)
- **Timezone support** for cron schedules using `zoneinfo`
- **Cross-node deduplication** — exactly-once execution across replicas sharing the same database
- **Priority queues** — CRITICAL, HIGH, NORMAL, LOW
- **Retry logic** with exponential backoff
- **Advisory locks** for exclusive execution when needed
- **Vacuum policies** for automatic cleanup of completed/failed jobs
- **Reliability** — graceful shutdown, heartbeat monitoring, orphan recovery, atomic job claiming

## Installation

```bash
pip install pg-scheduler
```

Requires Python 3.9+, PostgreSQL 12+, asyncpg, and croniter.

## Quick Start

### One-off Job

```python
import asyncio
import asyncpg
from datetime import datetime, timedelta, UTC
from pg_scheduler import Scheduler, JobPriority

async def send_email(recipient: str, subject: str):
    print(f"Sending email to {recipient}: {subject}")
    await asyncio.sleep(1)

async def main():
    db_pool = await asyncpg.create_pool(
        user='scheduler', password='password',
        database='scheduler_db', host='localhost'
    )

    scheduler = Scheduler(db_pool=db_pool, max_concurrent_jobs=10)
    await scheduler.start()

    try:
        job_id = await scheduler.schedule(
            send_email,
            execution_time=datetime.now(UTC) + timedelta(minutes=5),
            args=("user@example.com", "Welcome!"),
            priority=JobPriority.NORMAL,
            max_retries=3
        )
        print(f"Scheduled job: {job_id}")
        await asyncio.sleep(300)
    finally:
        await scheduler.shutdown()
        await db_pool.close()

if __name__ == "__main__":
    asyncio.run(main())
```

### Periodic Jobs

#### Interval-based

```python
from datetime import timedelta
from pg_scheduler import periodic, JobPriority

@periodic(every=timedelta(minutes=15))
async def cleanup_temp_files():
    ...

@periodic(every=timedelta(hours=1), priority=JobPriority.CRITICAL, max_retries=3)
async def generate_hourly_report():
    ...
```

#### Cron-based

```python
@periodic(cron="0 0 * * *")
async def daily_backup():
    ...

@periodic(cron="0 3 * * SUN", timezone="America/New_York")
async def weekly_report():
    ...

@periodic(cron="0 9-17 * * MON-FRI", timezone="Europe/London", priority=JobPriority.HIGH)
async def business_hours_task():
    ...
```

#### Advisory Locks

For operations that must run on exactly one node (e.g., database migrations):

```python
@periodic(every=timedelta(minutes=30), use_advisory_lock=True)
async def exclusive_maintenance():
    ...
```

Most jobs don't need this — the built-in deduplication already prevents duplicate executions.

### Decorator Parameters

```python
@periodic(
    every=timedelta(minutes=15),        # Interval (or use cron= instead)
    cron="*/15 * * * *",                # Cron expression (alternative to every=)
    timezone="UTC",                     # Timezone for cron (string or ZoneInfo)
    use_advisory_lock=False,            # Exclusive execution
    priority=JobPriority.NORMAL,        # CRITICAL, HIGH, NORMAL, LOW
    max_retries=0,                      # Retry attempts on failure
    job_name=None,                      # Custom name (auto-generated if None)
    enabled=True                        # Whether job is active
)
```

## Cross-Node Deduplication

Multiple nodes running the same code automatically deduplicate — no configuration needed:

```python
@periodic(every=timedelta(minutes=5))
async def cleanup_task():
    ...

# Node 1: Schedules job for 10:05 -> succeeds
# Node 2: Tries to schedule same job -> "Already exists, ignoring"
# Node 3: Tries to schedule same job -> "Already exists, ignoring"
# Result: Only one node executes cleanup at 10:05
```

## Management API

```python
scheduler.get_periodic_jobs()                   # List all periodic jobs
scheduler.get_periodic_job_status(dedup_key)    # Status of a specific job
scheduler.enable_periodic_job(dedup_key)        # Enable a job
scheduler.disable_periodic_job(dedup_key)       # Disable a job
await scheduler.trigger_periodic_job(dedup_key) # Trigger immediately
```

## Conflict Resolution

Handle duplicate job IDs:

- `ConflictResolution.RAISE` (default) — error on duplicate
- `ConflictResolution.IGNORE` — keep existing, return its ID
- `ConflictResolution.REPLACE` — update existing with new parameters

## Vacuum Policies

Automatic cleanup of old jobs:

```python
from pg_scheduler import VacuumConfig, VacuumPolicy

vacuum_config = VacuumConfig(
    completed=VacuumPolicy.after_days(1),
    failed=VacuumPolicy.after_days(7),
    cancelled=VacuumPolicy.after_days(3),
    interval_minutes=60,
    track_metrics=True
)

scheduler = Scheduler(db_pool, vacuum_config=vacuum_config)
```

## FastAPI Integration

```python
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

from fastapi import FastAPI
from pg_scheduler import Scheduler, periodic
import asyncpg
import asyncio

@periodic(every=timedelta(minutes=5))
async def cleanup_stale_sessions():
    await asyncio.sleep(0.5)

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db_pool = await asyncpg.create_pool(
        host="localhost", port=5432,
        user="scheduler", password="scheduler123",
        database="scheduler_db",
    )
    app.state.scheduler = Scheduler(app.state.db_pool, max_concurrent_jobs=20)
    await app.state.scheduler.start()
    yield
    await app.state.scheduler.shutdown()
    await app.state.db_pool.close()

app = FastAPI(lifespan=lifespan)

async def send_email(recipient: str):
    await asyncio.sleep(1)

@app.post("/send-email")
async def schedule_email(recipient: str):
    await app.state.scheduler.schedule(
        send_email,
        execution_time=datetime.now() + timedelta(seconds=10),
        args=(recipient,),
    )
    return {"status": "scheduled"}
```

## Configuration

```python
from pg_scheduler import Scheduler, PollingConfig, VacuumConfig, VacuumPolicy

scheduler = Scheduler(
    db_pool=db_pool,
    max_concurrent_jobs=25,      # Concurrent job execution limit (default: 25)
    batch_claim_limit=10,        # Jobs claimed per poll iteration (default: 10)
    misfire_grace_time=300,      # Seconds before pending jobs expire (None to disable)
    polling_config=PollingConfig(
        min_interval=0.05,       # Sleep after a successful claim
        max_interval=2.0,        # Upper bound for idle backoff
        idle_start_interval=0.5, # First idle sleep
        backoff_multiplier=1.5,  # Idle backoff growth factor
    ),
    vacuum_enabled=True,
    vacuum_config=VacuumConfig(
        completed=VacuumPolicy.after_days(1),
        failed=VacuumPolicy.after_days(7),
        cancelled=VacuumPolicy.after_days(3),
        interval_minutes=60,
    ),
)
```

## Contributing

Contributions are welcome. Please feel free to submit a Pull Request.

## License

MIT — see [LICENSE](LICENSE).

## Links

- [PyPI](https://pypi.org/project/pg-scheduler/)
- [GitHub](https://github.com/m1guelvrrl0/pg-scheduler)
- [Issues](https://github.com/m1guelvrrl0/pg-scheduler/issues)
- [Changelog](docs/changelog.md)
