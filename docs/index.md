# PG Scheduler Documentation

Welcome to PG Scheduler - a robust job scheduler built on PostgreSQL with async I/O support.

```{toctree}
:maxdepth: 2
:caption: Contents:

quickstart
user-guide/index
api/index
examples/index
deployment
changelog
```

## Overview

PG Scheduler provides a comprehensive job scheduling solution with:

- **🔄 Periodic Jobs**: Simple `@periodic` decorator for recurring tasks
- **🔒 Deduplication**: Guarantees exactly one execution per window across replicas  
- **⚡ Self-Rescheduling**: Jobs automatically schedule their next execution
- **🛡️ Advisory Locks**: Optional PostgreSQL advisory locks for exclusive execution
- **🎯 Priority Queues**: Support for job priorities and retry logic
- **🧹 Vacuum Policies**: Automatic cleanup of completed jobs
- **💪 Reliability**: Graceful shutdown, error handling, and orphan recovery

## Quick Start

### Installation

```bash
pip install pg-scheduler
```

### Basic Usage

```python
import asyncio
import asyncpg
from datetime import datetime, timedelta, UTC
from pg_scheduler import Scheduler, periodic, JobPriority

# Simple periodic job
@periodic(every=timedelta(minutes=15))
async def cleanup_temp_files():
    print("🧹 Cleaning up temporary files...")
    await asyncio.sleep(1)
    print("✅ Cleanup completed")

# Manual job scheduling
async def send_email(recipient: str, subject: str):
    print(f"📧 Sending email to {recipient}: {subject}")
    await asyncio.sleep(1)
    print(f"✅ Email sent")

async def main():
    # Create database connection
    db_pool = await asyncpg.create_pool(
        user='scheduler',
        password='password',
        database='scheduler_db',
        host='localhost'
    )
    
    # Initialize scheduler
    scheduler = Scheduler(db_pool=db_pool)
    await scheduler.start()
    
    try:
        # Schedule a job
        job_id = await scheduler.schedule(
            send_email,
            execution_time=datetime.now(UTC) + timedelta(minutes=5),
            args=("user@example.com", "Welcome!"),
            priority=JobPriority.NORMAL
        )
        
        # Let it run
        await asyncio.sleep(300)
        
    finally:
        await scheduler.shutdown()
        await db_pool.close()

if __name__ == "__main__":
    asyncio.run(main())
```

## Features

### Periodic Jobs

The `@periodic` decorator makes recurring jobs simple:

```python
from datetime import timedelta
from pg_scheduler import periodic, JobPriority

@periodic(every=timedelta(hours=1), priority=JobPriority.CRITICAL)
async def generate_reports():
    """Generate hourly reports"""
    # Your code here
    pass
```

### Reliability

Built-in reliability features include:

- **Cross-replica deduplication** - Same job won't run twice
- **Heartbeat monitoring** - Detect crashed workers
- **Orphan recovery** - Clean up abandoned jobs  
- **Graceful shutdown** - Wait for jobs to complete
- **Retry logic** - Automatic retry with exponential backoff

### Project Overview Features

- **PostgreSQL backend** - Reliable, ACID-compliant storage
- **Async/await support** - Built for modern Python
- **Docker friendly** - Easy containerization
- **Monitoring** - Comprehensive logging and metrics
- **Scalable** - Run multiple replicas safely

## Indices and tables

- {ref}`genindex`
- {ref}`modindex`
- {ref}`search`
