# PG Scheduler Test Suite

This directory contains the comprehensive test suite for the PG Scheduler library.

## Test Structure

```
tests/
├── __init__.py
├── conftest.py              # Shared fixtures and test configuration
├── README.md                # This file
├── unit/                    # Unit tests (no database required)
│   ├── test_conflict_resolution.py
│   ├── test_job_priority.py
│   └── test_vacuum.py
├── integration/             # Integration tests (require database)
│   ├── test_basic_scheduling.py
│   ├── test_misfire_grace_time.py
│   ├── test_periodic_cron.py
│   ├── test_periodic_interval.py
│   ├── test_priorities.py
│   └── test_timezone.py
└── race_conditions/         # Multi-instance race condition tests
    ├── README.md
    ├── test_multi_instance.py
    └── verify_results.py
```

## Test Categories

### Unit Tests (26 tests)
Fast tests that don't require a database connection:
- **JobPriority**: Enum values, db_value mapping, roundtrip conversion
- **ConflictResolution**: Enum values and strategies
- **VacuumPolicy & VacuumConfig**: Factory methods, defaults, configuration

**Run unit tests:**
```bash
pytest tests/unit/ -v
```

### Integration Tests
Tests that require a PostgreSQL database:

#### Basic Scheduling
- Simple job scheduling
- Job execution with args/kwargs
- Multiple concurrent jobs
- Custom job IDs
- Conflict resolution strategies (RAISE, IGNORE, REPLACE)
- Job ordering by execution time
- Failed jobs with retries

#### Periodic Jobs (Interval-based)
- Job registration and execution
- Self-rescheduling
- Priority support
- Retry logic
- Enabled/disabled jobs
- Multiple periodic jobs coexisting

#### Periodic Jobs (Cron-based)
- Cron expression parsing
- Various cron patterns (hourly, daily, weekly, monthly, business hours)
- Validation (must have either cron or interval, not both)
- Coexistence with interval-based jobs

#### Timezone Support
- Timezone with cron expressions
- String and ZoneInfo timezone specification
- Multiple timezones
- Timezone affects dedup keys
- UTC time conversion
- Default to UTC when no timezone specified

#### Priority Levels
- All four priority levels (CRITICAL, HIGH, NORMAL, LOW)
- Execution order based on priority
- Priority stored in database
- Priority with retries
- Execution time takes precedence over priority

#### Misfire Grace Time
- Global default (300 seconds)
- Custom global configuration
- Per-job overrides
- `None` means no expiration
- Jobs within grace period execute
- Expired jobs are marked in database
- Zero grace time means immediate expiration

### Race Condition Tests
Specialized tests that run multiple scheduler instances simultaneously:
- **Periodic Job Deduplication**: Verify periodic jobs only execute once per window
- **Cron Job Deduplication**: Verify cron jobs only execute once per trigger
- **Custom Job ID Conflicts**: Test IGNORE/REPLACE conflict resolution
- **Priority Ordering**: Verify priority respected across instances
- **Concurrent Execution**: Multiple jobs run without conflicts
- **Database Integrity**: No duplicate job_ids, no multi-claimed jobs

See [`tests/race_conditions/README.md`](race_conditions/README.md) for detailed documentation.

## Running Tests

### Prerequisites
1. Install test dependencies:
```bash
pip install -e ".[test]"
# or
pip install pytest pytest-asyncio pytest-cov pytest-timeout
```

2. Set up PostgreSQL:
```bash
# Using Docker
docker run -d \
  --name pg-scheduler-test \
  -e POSTGRES_USER=scheduler \
  -e POSTGRES_PASSWORD=scheduler123 \
  -e POSTGRES_DB=scheduler_db \
  -p 5432:5432 \
  postgres:15
```

### Run All Tests
```bash
# All tests
pytest tests/ -v

# With coverage
pytest tests/ -v --cov=pg_scheduler --cov-report=html

# Only unit tests (fast, no database)
pytest tests/unit/ -v

# Only integration tests
pytest tests/integration/ -v

# Specific test file
pytest tests/unit/test_job_priority.py -v

# Specific test
pytest tests/unit/test_job_priority.py::TestJobPriority::test_db_value_mapping -v
```

### Run Tests with Docker Compose
```bash
# Run integration tests in isolated environment
docker compose -f docker-compose.test.yml up --build --abort-on-container-exit

# Run race condition tests (4 instances simultaneously)
docker compose -f docker-compose.race-test.yml up --build --abort-on-container-exit

# View logs
docker compose -f docker-compose.test.yml logs
docker compose -f docker-compose.race-test.yml logs
```

### Using Test Markers
```bash
# Run only unit tests
pytest -m unit

# Run only integration tests
pytest -m integration

# Run only periodic job tests
pytest -m periodic

# Run only cron tests
pytest -m cron

# Run only timezone tests
pytest -m timezone

# Run slow tests
pytest -m slow
```

## GitHub Actions CI

The test suite runs automatically on:
- Push to `main`, `develop`, or `release/*` branches
- Pull requests to `main` or `develop`

The CI pipeline includes:
1. **Unit & Integration Tests**: Tests against real PostgreSQL database
2. **Docker Compose Tests**: Full integration testing in Docker
3. **Code Coverage**: Uploads to Codecov
4. **Linting**: Ruff, Black, isort checks

See `.github/workflows/test.yml` for configuration.

## Writing New Tests

### Unit Test Template
```python
import pytest
from pg_scheduler import YourClass

@pytest.mark.unit
class TestYourClass:
    def test_something(self):
        # No async needed, no database needed
        assert YourClass.method() == expected_value
```

### Integration Test Template
```python
import pytest
from datetime import timedelta

@pytest.mark.integration
class TestYourFeature:
    async def test_with_scheduler(self, scheduler, time_utils):
        # Use scheduler fixture (database initialized and started)
        execution_time = time_utils.future(seconds=5)
        
        job_id = await scheduler.schedule(
            your_job_function,
            execution_time=execution_time
        )
        
        # Wait and verify
        await asyncio.sleep(6)
        assert something_happened()
```

## Fixtures Available

From `conftest.py`:
- `db_pool`: Database connection pool (session scope)
- `clean_db`: Clean database for each test
- `scheduler`: Fully initialized and started scheduler
- `scheduler_no_start`: Scheduler without starting
- `sample_job`: Sample async job function
- `failing_job`: Job that always fails
- `slow_job`: Job that takes time to execute
- `job_counter`: Counter to track job executions
- `time_utils`: Utilities for time manipulation

## Test Database

Tests use PostgreSQL with these default credentials:
- Host: `localhost` (or `PGHOST` env var)
- Port: `5432` (or `PGPORT` env var)
- User: `scheduler` (or `PGUSER` env var)
- Password: `scheduler123` (or `PGPASSWORD` env var)
- Database: `scheduler_db` (or `PGDATABASE` env var)

Each test gets a clean database with tables dropped before and after.

## Coverage Goals

Target coverage: **>90%** for core functionality

Current coverage (unit tests only):
- Unit tests: 100% of tested modules
- Integration tests: Tests all major features

View coverage report:
```bash
pytest tests/ --cov=pg_scheduler --cov-report=html
open htmlcov/index.html
```

## Troubleshooting

### Database Connection Errors
```bash
# Check if PostgreSQL is running
docker ps | grep postgres

# Check logs
docker logs pg-scheduler-test

# Restart database
docker restart pg-scheduler-test
```

### Slow Tests
```bash
# Run with timeout warnings
pytest tests/ -v --timeout=30

# Skip slow tests
pytest tests/ -v -m "not slow"
```

### Test Isolation Issues
```bash
# Run tests one at a time
pytest tests/ -v --maxfail=1

# Verbose database cleanup
pytest tests/ -v -s  # shows print statements
```

## Future Test Additions

- [x] Race condition tests with multiple schedulers ✅
- [ ] Load testing with many concurrent jobs (1000+ jobs)
- [ ] Vacuum policy behavior tests
- [ ] Advisory lock contention tests
- [ ] Network failure recovery tests
- [ ] Database connection pool exhaustion tests
- [ ] Graceful shutdown behavior tests
- [ ] Job cancellation tests

