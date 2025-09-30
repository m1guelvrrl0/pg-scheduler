# Examples

Comprehensive examples showing different use cases for PG Scheduler.

```{toctree}
:maxdepth: 2

basic-examples
periodic-examples
production-examples
integration-examples
```

## Quick Examples

### Simple Job Scheduling

```python
import asyncio
import asyncpg
from datetime import datetime, timedelta, UTC
from pg_scheduler import Scheduler, JobPriority

async def send_email(recipient: str, subject: str):
    print(f"📧 Sending email to {recipient}: {subject}")
    await asyncio.sleep(1)  # Simulate sending
    print(f"✅ Email sent to {recipient}")

async def main():
    db_pool = await asyncpg.create_pool(
        user='scheduler',
        password='password',
        database='scheduler_db',
        host='localhost'
    )
    
    scheduler = Scheduler(db_pool)
    await scheduler.start()
    
    try:
        # Schedule immediate job
        await scheduler.schedule(
            send_email,
            execution_time=datetime.now(UTC) + timedelta(seconds=10),
            args=("user@example.com", "Welcome!"),
            priority=JobPriority.NORMAL
        )
        
        # Schedule high priority job
        await scheduler.schedule(
            send_email,
            execution_time=datetime.now(UTC) + timedelta(seconds=5),
            args=("admin@example.com", "System Alert"),
            priority=JobPriority.CRITICAL,
            max_retries=3
        )
        
        await asyncio.sleep(30)
        
    finally:
        await scheduler.shutdown()
        await db_pool.close()

asyncio.run(main())
```

### Periodic Jobs

```python
from pg_scheduler import periodic, JobPriority
from datetime import timedelta

@periodic(every=timedelta(minutes=15))
async def cleanup_temp_files():
    """Clean temporary files every 15 minutes"""
    print("🧹 Cleaning temporary files...")
    # Your cleanup logic
    print("✅ Cleanup complete")

@periodic(
    every=timedelta(hours=1),
    priority=JobPriority.CRITICAL,
    max_retries=3
)
async def generate_reports():
    """Generate hourly reports with retries"""
    print("📊 Generating reports...")
    # Your report logic
    print("✅ Reports generated")

@periodic(
    every=timedelta(minutes=30),
    use_advisory_lock=True
)
async def database_backup():
    """Exclusive database backup"""
    print("💾 Starting database backup...")
    # Your backup logic
    print("✅ Backup complete")
```

### Error Handling and Retries

```python
import random
from pg_scheduler import periodic
from datetime import timedelta

@periodic(every=timedelta(minutes=5), max_retries=3)
async def flaky_job():
    """Job that sometimes fails"""
    if random.random() < 0.3:  # 30% failure rate
        raise Exception("Random failure occurred")
    
    print("✅ Job completed successfully")

@periodic(every=timedelta(minutes=10))
async def robust_job():
    """Job with built-in error handling"""
    try:
        # Your potentially failing logic
        await risky_operation()
        print("✅ Operation completed")
    except Exception as e:
        print(f"⚠️ Operation failed: {e}")
        # Handle the error gracefully
        await fallback_operation()
```

### Job Management

```python
async def manage_periodic_jobs(scheduler):
    """Example of managing periodic jobs at runtime"""
    
    # Get all periodic jobs
    jobs = scheduler.get_periodic_jobs()
    print(f"Found {len(jobs)} periodic jobs")
    
    for dedup_key, config in jobs.items():
        status = scheduler.get_periodic_job_status(dedup_key)
        print(f"Job: {status['job_name']}")
        print(f"  Interval: {status['interval']}s")
        print(f"  Enabled: {status['enabled']}")
        print(f"  Priority: {status['priority']}")
        
        # Disable a specific job
        if status['job_name'] == 'cleanup_temp_files':
            scheduler.disable_periodic_job(dedup_key)
            print("  → Disabled cleanup job")
        
        # Manually trigger a job
        if status['job_name'] == 'generate_reports':
            job_id = await scheduler.trigger_periodic_job(dedup_key)
            print(f"  → Manually triggered: {job_id}")
```

### Production Configuration

```python
from pg_scheduler import (
    Scheduler, 
    VacuumConfig, 
    VacuumPolicy,
    VacuumTrigger
)

# Configure vacuum policies
vacuum_config = VacuumConfig(
    completed=VacuumPolicy.after_days(1),     # Clean completed jobs after 1 day
    failed=VacuumPolicy.after_days(7),        # Keep failed jobs for 7 days
    cancelled=VacuumPolicy.after_days(3),     # Clean cancelled jobs after 3 days
    interval_minutes=60,                      # Run vacuum every hour
    track_metrics=True                        # Store vacuum statistics
)

# Production scheduler configuration
scheduler = Scheduler(
    db_pool=db_pool,
    max_concurrent_jobs=50,      # Higher concurrency for production
    misfire_grace_time=300,      # 5 minute grace period
    vacuum_config=vacuum_config,
    vacuum_enabled=True
)
```

## Integration Examples

### FastAPI Integration

```python
from fastapi import FastAPI, BackgroundTasks
from pg_scheduler import Scheduler
import asyncpg

app = FastAPI()
scheduler = None

@app.on_event("startup")
async def startup_event():
    global scheduler
    db_pool = await asyncpg.create_pool(...)
    scheduler = Scheduler(db_pool)
    await scheduler.start()

@app.on_event("shutdown")
async def shutdown_event():
    if scheduler:
        await scheduler.shutdown()

@app.post("/schedule-email")
async def schedule_email(recipient: str, subject: str):
    job_id = await scheduler.schedule(
        send_email,
        execution_time=datetime.now(UTC) + timedelta(minutes=5),
        args=(recipient, subject)
    )
    return {"job_id": job_id}
```

### Django Integration

```python
# In your Django app
from django.core.management.base import BaseCommand
from pg_scheduler import Scheduler, periodic
import asyncpg
import asyncio

@periodic(every=timedelta(hours=1))
async def sync_user_data():
    """Sync user data periodically"""
    # Your Django model operations
    pass

class Command(BaseCommand):
    def handle(self, *args, **options):
        asyncio.run(self.run_scheduler())
    
    async def run_scheduler(self):
        db_pool = await asyncpg.create_pool(...)
        scheduler = Scheduler(db_pool)
        await scheduler.start()
        
        try:
            # Keep running
            while True:
                await asyncio.sleep(60)
        finally:
            await scheduler.shutdown()
```
