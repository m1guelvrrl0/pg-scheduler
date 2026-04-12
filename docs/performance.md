# Performance Tuning

PG Scheduler has no broker, no message queue, and no push notifications. Workers
poll a single PostgreSQL table and claim rows atomically with
`FOR UPDATE SKIP LOCKED`. Because the entire system is just SQL, performance
tuning comes down to four levers -- plus how you deploy the process itself.

## The Big Levers

### `batch_claim_limit`

```python
scheduler = Scheduler(db_pool, batch_claim_limit=50)
```

**Default: 10.** The maximum number of rows a worker claims in a single
`SELECT ... FOR UPDATE SKIP LOCKED ... LIMIT $1` statement per poll iteration.

Each claim is one database round trip. Raising this value means fewer round trips
for the same number of jobs. The trade-off is that a larger batch holds more row
locks for the duration of the UPDATE, which can slow down other workers trying to
claim at the same time.

| Workload | Recommended range |
|---|---|
| Low throughput (< 100 jobs/min) | 5--10 (default) |
| Medium throughput | 20--50 |
| High throughput (> 1 000 jobs/min) | 50--100 |

The effective claim size is actually `min(available_semaphore_slots, batch_claim_limit)`,
so there is no point setting `batch_claim_limit` higher than `max_concurrent_jobs`.

### `max_concurrent_jobs`

```python
scheduler = Scheduler(db_pool, max_concurrent_jobs=100)
```

**Default: 25.** Controls the `asyncio.Semaphore` that limits how many claimed
jobs execute at the same time within a single Python process.

This is the single most important knob. Its ideal value depends on what your
jobs do:

- **Pure I/O** (HTTP calls, database queries, S3 uploads): set this high --
  100, 200, even 500. asyncio can multiplex hundreds of concurrent coroutines
  because they spend most of their time waiting on the network.

- **CPU-bound work** (image processing, PDF generation, heavy computation):
  keep this low -- 2 to 4. asyncio runs on a single thread; a coroutine that
  burns CPU blocks the entire event loop, starving heartbeats and other jobs.
  See [Workload Profiles](#workload-profiles) below for strategies.

- **Mixed** (some I/O, some CPU): a reasonable starting point is 10--25. If
  you can, separate CPU and I/O jobs into different scheduler instances with
  different concurrency settings.

### `polling_config`

```python
from pg_scheduler import Scheduler, PollingConfig

scheduler = Scheduler(
    db_pool,
    polling_config=PollingConfig(
        min_interval=0.01,
        max_interval=1.0,
        idle_start_interval=0.1,
        backoff_multiplier=2.0,
    ),
)
```

Controls how aggressively the worker polls for new jobs. The polling loop uses
exponential backoff: fast when jobs are found, slowing down when idle, resetting
as soon as jobs appear again.

| Field | Default | Effect |
|---|---|---|
| `min_interval` | 0.05 s | Sleep after a successful claim (the "busy" path). Lower = faster pickup, more DB load. |
| `max_interval` | 2.0 s | Upper bound for idle backoff. Also used as sleep after errors. |
| `idle_start_interval` | 0.5 s | First sleep after an empty poll. Grows by `backoff_multiplier` each time. |
| `backoff_multiplier` | 1.5 | Growth factor for consecutive empty polls. |
| `semaphore_full_interval` | 1.0 s | Sleep when all `max_concurrent_jobs` slots are occupied. |
| `jitter` | True | Adds +/-10% random jitter to sleeps, preventing thundering-herd effects across workers. |

**Latency-sensitive** workloads (jobs must start within milliseconds of their
`execution_time`): lower `min_interval` to 0.01 and `idle_start_interval` to
0.1. Expect higher database load.

**Cost-sensitive** deployments (shared Postgres, low job volume): raise
`max_interval` to 5--10 seconds and `idle_start_interval` to 1--2 seconds to
reduce polling overhead.

### `db_pool`

```python
db_pool = await asyncpg.create_pool(
    user="scheduler",
    password="password",
    database="scheduler_db",
    host="localhost",
    min_size=5,
    max_size=20,
)
```

The `asyncpg.Pool` is created by you, not by the scheduler. Its size determines
how many concurrent database operations the scheduler can sustain.

A running scheduler uses connections for:

- **Claiming jobs** -- one connection per poll (short-lived, runs inside a
  transaction).
- **Heartbeats** -- one UPDATE every 30 seconds per running job.
- **Status updates** -- marking jobs completed or failed.
- **Vacuum** -- periodic DELETE of old rows.
- **Your jobs themselves** -- if your job functions also query the database
  through the same pool.

**Rule of thumb:** `max_size` should be at least half of `max_concurrent_jobs`,
but must also account for heartbeat and housekeeping connections. A reasonable
starting point:

```
max_size = max_concurrent_jobs // 2 + 5
```

If your jobs make their own database calls through the same pool, increase
accordingly. If multiple scheduler instances share a pool, multiply.

Too few connections causes coroutines to wait for a free connection, which
increases job latency and can cause heartbeat timeouts. Too many wastes
PostgreSQL backend memory and can hit `max_connections` limits.

## Deployment Patterns

### Single Instance

The simplest deployment: one Python process, one event loop, one scheduler.

```python
scheduler = Scheduler(
    db_pool,
    max_concurrent_jobs=50,
    batch_claim_limit=20,
)
await scheduler.start()
```

This is enough for many production workloads. A single instance processing fast
I/O jobs with `max_concurrent_jobs=100` and `batch_claim_limit=50` can sustain
thousands of jobs per minute.

The ceiling is the Python event loop itself. asyncio is single-threaded, so
CPU-bound work in any coroutine blocks everything. If you hit this limit, scale
horizontally.

### Multiple Instances

Run several processes (containers, replicas, separate machines) pointing at the
same database. Each instance generates its own `worker_id` and claims jobs
independently.

```yaml
# docker-compose.yml
services:
  worker-1:
    image: my-scheduler:latest
    environment:
      - MAX_CONCURRENT_JOBS=50
      - DATABASE_URL=postgresql://user:pass@db:5432/scheduler_db

  worker-2:
    image: my-scheduler:latest
    environment:
      - MAX_CONCURRENT_JOBS=50
      - DATABASE_URL=postgresql://user:pass@db:5432/scheduler_db
```

No coordination is needed between instances. `FOR UPDATE SKIP LOCKED` ensures
each job is claimed by exactly one worker. Adding more instances increases
throughput linearly for I/O-bound workloads.

**Watch out for:**

- **Database connections** -- each instance has its own pool. N instances with
  `max_size=20` means up to N * 20 connections. Stay within PostgreSQL's
  `max_connections` (or use PgBouncer).
- **Periodic job deduplication** -- handled automatically. Periodic jobs use
  deterministic IDs and `ON CONFLICT DO NOTHING`, so only one instance schedules
  each periodic window.
- **Vacuum** -- runs in every instance by default. This is safe (vacuum DELETEs
  are idempotent) but wasteful. Consider disabling vacuum on all but one instance
  if you have many workers.

## Workload Profiles

### Short I/O Tasks

Typical examples: sending webhooks, making API calls, writing to a cache,
dispatching push notifications. Each job takes milliseconds to seconds.

```python
scheduler = Scheduler(
    db_pool,
    max_concurrent_jobs=200,
    batch_claim_limit=50,
    polling_config=PollingConfig(
        min_interval=0.01,
        idle_start_interval=0.1,
    ),
)
```

asyncio excels here. A single instance can handle hundreds of concurrent
in-flight requests because each coroutine spends most of its time in `await`.
The event loop stays responsive, heartbeats fire on time, and job throughput
scales with `max_concurrent_jobs`.

### Long-Running I/O Tasks

Typical examples: large file uploads, data pipeline stages, batch API
pagination. Each job takes minutes.

```python
scheduler = Scheduler(
    db_pool,
    max_concurrent_jobs=10,
    batch_claim_limit=5,
)
```

Lower concurrency keeps memory usage in check and avoids holding too many
database connections open. Be aware of the built-in 1-hour timeout per job
(`asyncio.wait_for` in the execution path) -- if your jobs can exceed this,
consider breaking them into smaller stages.

Multiple instances help here: with 3 instances at `max_concurrent_jobs=10`, you
can run 30 long jobs in parallel without any single process becoming a
bottleneck.

### Pure I/O vs. Mixed CPU + I/O

**Pure I/O** jobs (everything is `await`-ed network calls) scale well with high
concurrency because the event loop is never blocked. This is the ideal workload
for PG Scheduler.

**CPU-bound** work (image resizing, compression, number crunching) blocks the
event loop. While a coroutine is burning CPU, no other coroutine can run -- not
your other jobs, not heartbeats, not the polling loop. This leads to missed
heartbeats and jobs being incorrectly marked as orphaned.

Strategies for CPU-heavy jobs:

1. **Offload to a thread:** wrap the CPU portion in `asyncio.to_thread()`:

   ```python
   import asyncio

   async def resize_image(path: str):
       # CPU-heavy work runs in a thread, event loop stays free
       result = await asyncio.to_thread(do_resize, path)
       # Back on the event loop for I/O
       await upload_to_s3(result)
   ```

2. **Offload to a process pool:** for truly heavy computation, use a
   `ProcessPoolExecutor` to bypass the GIL entirely:

   ```python
   import asyncio
   from concurrent.futures import ProcessPoolExecutor

   pool = ProcessPoolExecutor(max_workers=4)

   async def heavy_computation(data):
       loop = asyncio.get_running_loop()
       result = await loop.run_in_executor(pool, crunch_numbers, data)
       await store_result(result)
   ```

3. **Dedicated instances:** run CPU jobs on separate scheduler instances with
   low `max_concurrent_jobs` (2--4), and keep I/O jobs on instances with high
   concurrency. This prevents CPU work from starving I/O jobs.

## Bulk Writes

When you need to schedule many jobs at once (batch imports, fan-out patterns),
use `schedule_bulk()` instead of calling `schedule()` in a loop:

```python
from pg_scheduler import Scheduler, JobSpec
from datetime import datetime, timedelta, UTC

jobs = [
    JobSpec(
        func=send_notification,
        execution_time=datetime.now(UTC) + timedelta(seconds=i),
        args=(user_id,),
    )
    for i, user_id in enumerate(user_ids)
]

# One SQL statement per 1000 jobs (configurable via batch_size)
await scheduler.schedule_bulk(jobs, batch_size=1000)
```

`schedule_bulk()` uses a single `INSERT ... SELECT FROM unnest(...)` per batch,
which is 10--100x faster than individual `schedule()` calls. The `batch_size`
parameter (default 1000) controls how many rows go into each SQL statement.

## Summary

| Lever | What it controls | Raise when | Lower when |
|---|---|---|---|
| `max_concurrent_jobs` | In-process concurrency | Jobs are I/O-bound | Jobs are CPU-heavy |
| `batch_claim_limit` | Rows claimed per poll | High throughput needed | Few jobs, many workers |
| `polling_config` intervals | Poll frequency | Low latency required | Reducing DB load matters |
| `db_pool` `max_size` | Available connections | High concurrency or shared pool | Connection limits tight |
| Instances (horizontal) | Total parallelism | Single process is saturated | Simplicity preferred |
