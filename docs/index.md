# PG Scheduler

A PostgreSQL-backed async job scheduler for Python. Jobs are stored as rows in a
single table and claimed atomically with `FOR UPDATE SKIP LOCKED`, so there is no
broker, no message queue, and no external dependency beyond Postgres itself.

```{toctree}
:maxdepth: 2
:caption: Contents

quickstart
user-guide/index
performance
api/index
examples/index
changelog
```

## Key Features

- **Periodic jobs** -- `@periodic` decorator with interval or cron expressions, automatic rescheduling, and cross-replica deduplication.
- **Bulk scheduling** -- `schedule_bulk()` inserts thousands of jobs in a single SQL statement using `unnest`.
- **Priority queues** -- four levels (CRITICAL, HIGH, NORMAL, LOW) respected during claim ordering.
- **Reliability** -- heartbeat monitoring, lease-based execution, orphan recovery, graceful shutdown, and retry with exponential backoff.
- **Vacuum policies** -- configurable cleanup of completed, failed, and cancelled jobs.
- **Multi-worker safe** -- run as many instances as you need; `FOR UPDATE SKIP LOCKED` prevents double-execution without any coordination layer.

## How It Works

1. You create an `asyncpg` connection pool and pass it to a `Scheduler`.
2. The scheduler creates the `scheduled_jobs` table on first start (if it does not already exist).
3. A polling loop claims pending rows, executes the associated async function, and marks the row completed or failed.
4. Periodic jobs reschedule themselves after each execution using deterministic IDs for deduplication.

## Indices and Tables

- {ref}`genindex`
- {ref}`modindex`
- {ref}`search`
