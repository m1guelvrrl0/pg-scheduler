# Quick Start

## Requirements

- Python 3.9+
- PostgreSQL 12+

## Installation

```bash
pip install pg-scheduler
```

## Database Setup

Create a PostgreSQL database for the scheduler:

```sql
CREATE DATABASE scheduler_db;
CREATE USER scheduler WITH PASSWORD 'scheduler123';
GRANT ALL PRIVILEGES ON DATABASE scheduler_db TO scheduler;
```

The scheduler creates the `scheduled_jobs` table automatically on first start.

## Standalone Example

```python
import asyncio
import asyncpg
from datetime import datetime, timedelta, UTC
from pg_scheduler import Scheduler

async def send_email(recipient: str, subject: str):
    print(f"Sending email to {recipient}: {subject}")
    await asyncio.sleep(1)
    print(f"Email sent to {recipient}")

async def main():
    db_pool = await asyncpg.create_pool(
        user="scheduler",
        password="scheduler123",
        database="scheduler_db",
        host="localhost",
        port=5432,
    )

    scheduler = Scheduler(db_pool=db_pool)
    await scheduler.start()

    try:
        job_id = await scheduler.schedule(
            send_email,
            execution_time=datetime.now(UTC) + timedelta(seconds=10),
            args=("user@example.com", "Welcome!"),
        )
        print(f"Scheduled job: {job_id}")

        # Keep the process alive so the scheduler can run
        while True:
            await asyncio.sleep(1)
    finally:
        await scheduler.shutdown()
        await db_pool.close()

if __name__ == "__main__":
    asyncio.run(main())
```

## FastAPI Example

A more realistic setup using FastAPI's lifespan protocol to start and stop the
scheduler alongside the web application:

```python
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

from fastapi import FastAPI
import asyncpg
import asyncio
import logging

from pg_scheduler import Scheduler, periodic

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# Periodic tasks are registered at import time via the decorator.
# The scheduler picks them up automatically when it starts.
@periodic(every=timedelta(minutes=5))
async def cleanup_stale_carts():
    logger.info("Cleaning up stale shopping carts")
    await asyncio.sleep(0.5)
    logger.info("Stale carts cleaned")


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


async def process_order(order_id: str, delay: float = 1.0):
    logger.info(f"Processing order {order_id}")
    await asyncio.sleep(delay)
    logger.info(f"Order {order_id} processed")


@app.post("/orders/{order_id}/process")
async def schedule_order(order_id: str):
    await app.state.scheduler.schedule(
        process_order,
        execution_time=datetime.now() + timedelta(seconds=10),
        args=(order_id,),
    )
    return {"status": "scheduled"}
```

Run it with:

```bash
pip install pg-scheduler[examples]
uvicorn myapp:app --host 0.0.0.0 --port 8000
```

## Next Steps

- [User Guide](user-guide/index.md) -- configuration, misfire grace time, priorities, conflict resolution.
- [Performance Tuning](performance.md) -- the knobs that matter and how to size them for your workload.
- [API Reference](api/index.md) -- full class and function documentation.
- [Examples](examples/index.md) -- periodic jobs, bulk scheduling, production configuration.
