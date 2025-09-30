# API Reference

Complete API documentation for PG Scheduler.

```{toctree}
:maxdepth: 2

scheduler
periodic
enums
exceptions
```

## Core Classes

### Scheduler

The main scheduler class that manages job execution:

```{eval-rst}
.. autoclass:: pg_scheduler.Scheduler
   :members:
   :undoc-members:
   :show-inheritance:
```

### Periodic Decorator

The `@periodic` decorator for recurring jobs:

```{eval-rst}
.. autofunction:: pg_scheduler.periodic
```

## Enums

### JobPriority

```{eval-rst}
.. autoclass:: pg_scheduler.JobPriority
   :members:
   :undoc-members:
   :show-inheritance:
```

### ConflictResolution

```{eval-rst}
.. autoclass:: pg_scheduler.ConflictResolution
   :members:
   :undoc-members:
   :show-inheritance:
```

### VacuumTrigger

```{eval-rst}
.. autoclass:: pg_scheduler.VacuumTrigger
   :members:
   :undoc-members:
   :show-inheritance:
```

## Configuration Classes

### VacuumPolicy

```{eval-rst}
.. autoclass:: pg_scheduler.VacuumPolicy
   :members:
   :undoc-members:
   :show-inheritance:
```

### VacuumConfig

```{eval-rst}
.. autoclass:: pg_scheduler.VacuumConfig
   :members:
   :undoc-members:
   :show-inheritance:
```

## Usage Examples

### Basic Scheduler Usage

```python
import asyncio
import asyncpg
from pg_scheduler import Scheduler

async def my_job(message: str):
    print(f"Processing: {message}")

async def main():
    db_pool = await asyncpg.create_pool(...)
    scheduler = Scheduler(db_pool)
    await scheduler.start()
    
    # Schedule a job
    await scheduler.schedule(
        my_job,
        execution_time=datetime.now(UTC) + timedelta(minutes=5),
        args=("Hello World",)
    )
```

### Periodic Jobs

```python
from pg_scheduler import periodic
from datetime import timedelta

@periodic(every=timedelta(hours=1))
async def hourly_task():
    print("Running hourly task")
```
