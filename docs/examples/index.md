# Examples

## Simple Job Scheduling

```python
import asyncio
import asyncpg
from datetime import datetime, timedelta, UTC
from pg_scheduler import Scheduler, JobPriority

async def send_email(recipient: str, subject: str):
    print(f"Sending email to {recipient}: {subject}")
    await asyncio.sleep(1)
    print(f"Email sent to {recipient}")

async def main():
    db_pool = await asyncpg.create_pool(
        user="scheduler",
        password="password",
        database="scheduler_db",
        host="localhost",
    )

    scheduler = Scheduler(db_pool)
    await scheduler.start()

    try:
        await scheduler.schedule(
            send_email,
            execution_time=datetime.now(UTC) + timedelta(seconds=10),
            args=("user@example.com", "Welcome!"),
            priority=JobPriority.NORMAL,
        )

        await scheduler.schedule(
            send_email,
            execution_time=datetime.now(UTC) + timedelta(seconds=5),
            args=("admin@example.com", "System Alert"),
            priority=JobPriority.CRITICAL,
            max_retries=3,
        )

        await asyncio.sleep(30)
    finally:
        await scheduler.shutdown()
        await db_pool.close()

asyncio.run(main())
```

## Periodic Jobs

```python
from pg_scheduler import periodic, JobPriority
from datetime import timedelta

@periodic(every=timedelta(minutes=15))
async def cleanup_temp_files():
    print("Cleaning temporary files...")

@periodic(every=timedelta(hours=1), priority=JobPriority.CRITICAL, max_retries=3)
async def generate_reports():
    print("Generating reports...")

@periodic(every=timedelta(minutes=30), use_advisory_lock=True)
async def database_backup():
    print("Starting database backup...")
```

### Cron Expressions

```python
from pg_scheduler import periodic

@periodic(cron="0 0 * * *")
async def daily_midnight_job():
    print("Running at midnight")

@periodic(cron="*/15 * * * *")
async def every_fifteen_minutes():
    print("Running every 15 minutes")

@periodic(cron="0 9-17 * * MON-FRI", timezone="America/New_York")
async def business_hours_task():
    print("Running during business hours (Eastern)")
```

## Bulk Scheduling

```python
from pg_scheduler import Scheduler, JobSpec, JobPriority, ConflictResolution
from datetime import datetime, timedelta, UTC

async def send_notification(user_id: int):
    print(f"Notifying user {user_id}")

jobs = [
    JobSpec(
        func=send_notification,
        execution_time=datetime.now(UTC) + timedelta(seconds=i),
        args=(user_id,),
        priority=JobPriority.NORMAL,
    )
    for i, user_id in enumerate(range(1, 10001))
]

ids = await scheduler.schedule_bulk(
    jobs,
    conflict_resolution=ConflictResolution.IGNORE,
    batch_size=1000,
)
print(f"Scheduled {len(ids)} jobs")
```

## Error Handling and Retries

```python
import random
from pg_scheduler import periodic
from datetime import timedelta

@periodic(every=timedelta(minutes=5), max_retries=3)
async def flaky_job():
    if random.random() < 0.3:
        raise Exception("Random failure occurred")
    print("Job completed successfully")

@periodic(every=timedelta(minutes=10))
async def robust_job():
    try:
        await risky_operation()
    except Exception as e:
        print(f"Operation failed: {e}")
        await fallback_operation()
```

## Job Management

```python
async def manage_periodic_jobs(scheduler):
    jobs = scheduler.get_periodic_jobs()
    print(f"Found {len(jobs)} periodic jobs")

    for dedup_key, config in jobs.items():
        status = scheduler.get_periodic_job_status(dedup_key)
        print(f"Job: {status['job_name']}, Enabled: {status['enabled']}")

        # Disable a specific job
        if status["job_name"] == "cleanup_temp_files":
            scheduler.disable_periodic_job(dedup_key)

        # Manually trigger a job
        if status["job_name"] == "generate_reports":
            job_id = await scheduler.trigger_periodic_job(dedup_key)
            print(f"Manually triggered: {job_id}")
```

## Conflict Resolution

```python
from pg_scheduler import ConflictResolution

# Raise error if job_id already exists (default)
await scheduler.schedule(
    my_function,
    execution_time=run_time,
    job_id="custom-id",
    conflict_resolution=ConflictResolution.RAISE,
)

# Silently skip if job_id already exists
await scheduler.schedule(
    my_function,
    execution_time=run_time,
    job_id="custom-id",
    conflict_resolution=ConflictResolution.IGNORE,
)

# Overwrite the existing job
await scheduler.schedule(
    my_function,
    execution_time=run_time,
    job_id="custom-id",
    conflict_resolution=ConflictResolution.REPLACE,
)
```

## FastAPI Integration

```python
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

from fastapi import FastAPI
import asyncpg
import asyncio

from pg_scheduler import Scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db_pool = await asyncpg.create_pool(
        host="localhost",
        port=5432,
        user="scheduler",
        password="scheduler123",
        database="scheduler_db",
    )
    app.state.scheduler = Scheduler(
        app.state.db_pool,
        max_concurrent_jobs=20,
        misfire_grace_time=120,
    )
    await app.state.scheduler.start()
    yield
    await app.state.scheduler.shutdown()
    await app.state.db_pool.close()


app = FastAPI(lifespan=lifespan)


async def process_order(order_id: str):
    await asyncio.sleep(1)
    print(f"Order {order_id} processed")


@app.post("/orders/{order_id}/process")
async def schedule_order(order_id: str):
    await app.state.scheduler.schedule(
        process_order,
        execution_time=datetime.now() + timedelta(seconds=10),
        args=(order_id,),
    )
    return {"status": "scheduled"}
```

## Production Configuration

```python
import os
from pg_scheduler import (
    Scheduler,
    PollingConfig,
    VacuumConfig,
    VacuumPolicy,
)

MAX_JOBS = int(os.getenv("MAX_CONCURRENT_JOBS", "25"))
MISFIRE_GRACE = int(os.getenv("MISFIRE_GRACE_TIME", "300"))
VACUUM_INTERVAL = int(os.getenv("VACUUM_INTERVAL_MINUTES", "60"))

scheduler = Scheduler(
    db_pool=db_pool,
    max_concurrent_jobs=MAX_JOBS,
    misfire_grace_time=MISFIRE_GRACE,
    batch_claim_limit=30,
    polling_config=PollingConfig(
        min_interval=0.02,
        max_interval=2.0,
    ),
    vacuum_enabled=True,
    vacuum_config=VacuumConfig(
        completed=VacuumPolicy.after_days(1),
        failed=VacuumPolicy.after_days(7),
        cancelled=VacuumPolicy.after_days(3),
        interval_minutes=VACUUM_INTERVAL,
    ),
)
```

### Docker Compose

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
