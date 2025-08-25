# Python PG backed Job Scheduler

A simple lightweight async first job scheduler for Python that uses Postgresql to allow you to schedule and manage the execution of asynchronous tasks.
It's heavily inspired by APScheduler in its user friendly API but horizontally scalable and much more focused in the features it provides and technologies it uses.
It makes minimal assumptions about how you run it other than that you are in an asyncIO enviroment, use Postgresql and asyncpg. Due to this it can integrate very easily with other technologies.

This library is focused on easy to use and deploy scheduler and not being an all comprehensive job runner so if you need something more advanced and complicated like workers queues and worker producer patterns I believe these are already very well covered in the python ecosystem and I would suggest you to look at those instead.

## Features

- Schedule async functions to run at specific times or intervals
- Supports one-time and recurring jobs
- Async-first design - works with modern async/await syntax
- Simple and lightweight implementation

## ⚠️ Project Status

This project is currently in **alpha** stage and not ready to be used. Please note:
- The API is not stable and may change without notice
- Currently only supports async functions
- Not recommended for production use without thorough testing

## Requirements

- Python 3.9+
- PostgreSQL 12+

## Quick Start

```python
import asyncio
import asyncpg
from datetime import datetime, timedelta, UTC
from scheduler import Scheduler, JobPriority, ConflictResolution

# Define your async job functions
async def send_email(recipient: str, subject: str):
    """Example job: Send an email"""
    print(f"📧 Sending email to {recipient}: {subject}")
    # Your email sending logic here
    await asyncio.sleep(1)  # Simulate work
    print(f"✅ Email sent to {recipient}")

async def generate_report(report_type: str, user_id: int):
    """Example job: Generate a report"""
    print(f"📊 Generating {report_type} report for user {user_id}")
    # Your report generation logic here
    await asyncio.sleep(2)  # Simulate work
    print(f"✅ Report generated: {report_type}")

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
    scheduler = Scheduler(db_pool=db_pool, max_concurrent_jobs=10)
    await scheduler.start()
    
    try:
        # 1. Schedule job with auto-generated ID
        job_id1 = await scheduler.schedule(
            send_email,
            execution_time=datetime.now(UTC) + timedelta(minutes=5),
            args=("user@example.com", "Welcome!")
        )
        print(f"Scheduled email job: {job_id1}")
        
        # 2. Schedule job with custom ID
        job_id2 = await scheduler.schedule(
            generate_report,
            execution_time=datetime.now(UTC) + timedelta(hours=1),
            args=("monthly", 123),
            job_id="monthly-report-2024-08",
            priority=JobPriority.CRITICAL,
            max_retries=3
        )
        print(f"Scheduled report job: {job_id2}")
        
        # 3. Schedule with conflict resolution
        # This will ignore the duplicate and return existing job_id
        job_id3 = await scheduler.schedule(
            generate_report,
            execution_time=datetime.now(UTC) + timedelta(hours=2),
            args=("monthly", 456),
            job_id="monthly-report-2024-08",  # Same ID as above
            conflict_resolution=ConflictResolution.IGNORE
        )
        print(f"Duplicate job ignored: {job_id3}")
        
        # 4. Cancel a scheduled job
        cancelled = await scheduler.cancel_job(job_id1)
        if cancelled:
            print(f"Job {job_id1} cancelled successfully")
        
        # 5. Replace/update an existing job
        job_id4 = await scheduler.schedule(
            generate_report,
            execution_time=datetime.now(UTC) + timedelta(minutes=30),
            args=("weekly", 789),
            job_id="monthly-report-2024-08",  # Same ID, but will replace
            conflict_resolution=ConflictResolution.REPLACE
        )
        print(f"Job replaced: {job_id4}")
        
        # Let the scheduler run for a while
        await asyncio.sleep(10)
        
    finally:
        # Clean shutdown
        await scheduler.shutdown()
        await db_pool.close()

if __name__ == "__main__":
    asyncio.run(main())
```

## Key Features

### 🎯 **Job Scheduling**
- **Auto-generated IDs**: Let the system create unique job IDs
- **Custom IDs**: Provide your own job IDs for idempotent operations
- **Priority Support**: `JobPriority.NORMAL` (default) or `JobPriority.CRITICAL`
- **Retry Logic**: Configure `max_retries` (defaults to 0 for no retries)

### 🔄 **Conflict Resolution**
Handle duplicate job IDs with flexible strategies:
- `ConflictResolution.RAISE` (default): Raise error for duplicates
- `ConflictResolution.IGNORE`: Ignore new job, return existing ID
- `ConflictResolution.REPLACE`: Update existing job with new parameters

### 🛑 **Job Management**
- **Cancel Jobs**: `await scheduler.cancel_job(job_id)`
- **Graceful Shutdown**: Waits for active jobs to complete
- **Concurrency Control**: Limit concurrent job execution

### 🔧 **Reliability Features**
- **Lease-based Execution**: Explicit job ownership with timeouts
- **Heartbeat Monitoring**: Detect and recover from crashed workers
- **Atomic Job Claiming**: Race-condition-free job distribution
- **Orphan Recovery**: Automatic cleanup of abandoned jobs

## Database Setup

```sql
-- The scheduler automatically creates the required table
-- But you can also create it manually:
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

## Roadmap
Focus for now is making sure everything is tight and working as expected and handles failures gracefully

Strong maybe:
- Job execution via ThreadPoolExecutor (you can actually already run on a thread with asyncio.to_thread)
- Job execution via ProcessPoolExecutor

## Installation

```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
