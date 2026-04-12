# User Guide

This guide covers the configuration options and features of PG Scheduler in
detail.

```{toctree}
:maxdepth: 2

configuration
```

## Key Concepts

### Jobs vs. Periodic Jobs

- **Jobs** are one-time executions scheduled for a specific `execution_time`.
- **Periodic jobs** are recurring executions registered with the `@periodic`
  decorator. After each run, the job reschedules itself for the next window.

### Deduplication

PG Scheduler prevents duplicate job executions across multiple worker replicas
using:

- **Deterministic job IDs** based on function name and schedule parameters.
- **Database constraints** -- PostgreSQL primary keys prevent duplicate rows.
- **Window-based logic** -- periodic jobs generate time-window keys so the same
  window is never scheduled twice.

### Priority System

Jobs are claimed in priority order (lower number = higher priority):

| Level | Value | Description |
|---|---|---|
| `JobPriority.CRITICAL` | 1 | Claimed before all other jobs |
| `JobPriority.HIGH` | 3 | High priority |
| `JobPriority.NORMAL` | 5 | Default |
| `JobPriority.LOW` | 8 | Claimed last |

Within the same priority, jobs are ordered by `execution_time`.

### Reliability Features

- **Heartbeat monitoring** -- running jobs send a heartbeat every 30 seconds.
  Workers that stop sending heartbeats have their jobs recovered.
- **Lease-based execution** -- each claimed job gets a 60-second lease that is
  renewed by heartbeats. Expired leases trigger recovery.
- **Orphan recovery** -- on startup and periodically, the scheduler reclaims
  jobs left in `running` state by dead workers.
- **Graceful shutdown** -- active jobs are given up to 30 seconds to complete
  before the process exits.
- **Retry logic** -- failed jobs are retried with exponential backoff up to
  `max_retries` times.
