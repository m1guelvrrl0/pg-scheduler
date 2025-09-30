# Quick Start Guide

This guide will get you up and running with PG Scheduler in minutes.

## Installation

### Requirements

- Python 3.9+
- PostgreSQL 12+

### Install PG Scheduler

```bash
pip install pg-scheduler
```

### Database Setup

Create a PostgreSQL database for the scheduler:

```sql
CREATE DATABASE scheduler_db;
CREATE USER scheduler WITH PASSWORD 'scheduler123';
GRANT ALL PRIVILEGES ON DATABASE scheduler_db TO scheduler;
```

The scheduler will automatically create the required tables when it starts.

## Basic Example

Here's a complete example showing both periodic and manual job scheduling:

```python
import asyncio
import asyncpg
from datetime import datetime, timedelta, UTC
from pg_scheduler import Scheduler, periodic, JobPriority

# Define a periodic job
@periodic(every=timedelta(minutes=15))
async def cleanup_temp_files():
    """Clean up temporary files every 15 minutes"""
    print("🧹 Cleaning up temporary files...")
    # Your cleanup logic here
    await asyncio.sleep(2)  # Simulate work
    print("✅ Cleanup completed")

# Define a regular job function
async def send_welcome_email(user_email: str, user_name: str):
    """Send a welcome email to a new user"""
    print(f"📧 Sending welcome email to {user_name} ({user_email})")
    # Your email sending logic here
    await asyncio.sleep(1)  # Simulate email sending
    print(f"✅ Welcome email sent to {user_name}")

async def main():
    # Create database connection pool
    db_pool = await asyncpg.create_pool(
        user='scheduler',
        password='scheduler123',
        database='scheduler_db',
        host='localhost',
        port=5432
    )
    
    # Initialize scheduler
    scheduler = Scheduler(
        db_pool=db_pool,
        max_concurrent_jobs=10,
        misfire_grace_time=300  # 5 minutes
    )
    
    # Start the scheduler
    await scheduler.start()
    
    try:
        print("🚀 Scheduler started!")
        
        # Schedule a welcome email job
        job_id = await scheduler.schedule(
            send_welcome_email,
            execution_time=datetime.now(UTC) + timedelta(minutes=1),
            args=("user@example.com", "John Doe"),
            priority=JobPriority.NORMAL,
            max_retries=3
        )
        
        print(f"📋 Scheduled welcome email job: {job_id}")
        
        # Check periodic jobs
        periodic_jobs = scheduler.get_periodic_jobs()
        print(f"📋 Found {len(periodic_jobs)} periodic jobs:")
        for dedup_key, config in periodic_jobs.items():
            status = scheduler.get_periodic_job_status(dedup_key)
            print(f"  • {status['job_name']}: every {status['interval']}s")
        
        # Let the scheduler run for a while
        print("⏱️  Running for 5 minutes... (Ctrl+C to stop)")
        await asyncio.sleep(300)
        
    except KeyboardInterrupt:
        print("🛑 Shutting down...")
    finally:
        # Graceful shutdown
        await scheduler.shutdown()
        await db_pool.close()
        print("👋 Goodbye!")

if __name__ == "__main__":
    asyncio.run(main())
```

## What Happens

When you run this example:

1. **Database Setup**: The scheduler creates necessary tables automatically
2. **Periodic Job Registration**: The `@periodic` decorator registers the cleanup job
3. **Manual Job Scheduling**: The welcome email is scheduled for 1 minute from now
4. **Job Execution**: Both jobs will execute at their scheduled times
5. **Self-Rescheduling**: The periodic job automatically schedules its next run
6. **Graceful Shutdown**: All active jobs complete before shutdown

## Next Steps

- Read the [User Guide](user-guide/index.md) for detailed features
- Check out more [Examples](examples/index.md)
- Learn about [Production Deployment](deployment.md)
- Explore the [API Reference](api/index.md)

## Common Patterns

### High-Priority Jobs

```python
@periodic(every=timedelta(minutes=5), priority=JobPriority.CRITICAL)
async def monitor_system():
    """Critical system monitoring"""
    pass
```

### Jobs with Retries

```python
@periodic(every=timedelta(hours=1), max_retries=3)
async def generate_report():
    """Generate reports with retry on failure"""
    pass
```

### Exclusive Jobs (Advisory Locks)

```python
@periodic(every=timedelta(minutes=30), use_advisory_lock=True)
async def database_maintenance():
    """Maintenance that should only run on one worker"""
    pass
```

### Manual Job Management

```python
# Get all periodic jobs
jobs = scheduler.get_periodic_jobs()

# Disable a job
scheduler.disable_periodic_job(dedup_key)

# Manually trigger a job
await scheduler.trigger_periodic_job(dedup_key)

# Check job status
status = scheduler.get_periodic_job_status(dedup_key)
```
