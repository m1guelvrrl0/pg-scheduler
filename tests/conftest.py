"""
Pytest configuration and fixtures for pg_scheduler tests.
"""

import asyncio
import os
from datetime import UTC, datetime
from typing import AsyncGenerator

import asyncpg
import pytest

from pg_scheduler import Scheduler


# Database connection parameters from environment
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = int(os.getenv("PGPORT", "5432"))
DB_USER = os.getenv("PGUSER", "scheduler")
DB_PASSWORD = os.getenv("PGPASSWORD", "scheduler123")
DB_NAME = os.getenv("PGDATABASE", "scheduler_db")


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
async def db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    """Create a database connection pool for the test session."""
    pool = await asyncpg.create_pool(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        min_size=2,
        max_size=10,
    )
    yield pool
    await pool.close()


@pytest.fixture
async def clean_db(db_pool: asyncpg.Pool) -> AsyncGenerator[asyncpg.Pool, None]:
    """
    Clean database fixture that clears all tables before and after each test.
    """
    # Clean before test
    await db_pool.execute("DROP TABLE IF EXISTS scheduled_jobs CASCADE")
    await db_pool.execute("DROP TABLE IF EXISTS vacuum_stats CASCADE")
    
    yield db_pool
    
    # Clean after test
    await db_pool.execute("DROP TABLE IF EXISTS scheduled_jobs CASCADE")
    await db_pool.execute("DROP TABLE IF EXISTS vacuum_stats CASCADE")


@pytest.fixture
async def scheduler(clean_db: asyncpg.Pool) -> AsyncGenerator[Scheduler, None]:
    """
    Create a fresh scheduler instance for each test.
    """
    sched = Scheduler(clean_db, max_concurrent_jobs=10)
    await sched.initialize_db()
    await sched.start()
    
    yield sched
    
    # Stop scheduler after test
    await sched.shutdown()


@pytest.fixture
async def scheduler_no_start(clean_db: asyncpg.Pool) -> AsyncGenerator[Scheduler, None]:
    """
    Create a scheduler instance without starting it.
    Useful for testing initialization and configuration.
    """
    sched = Scheduler(clean_db, max_concurrent_jobs=10)
    await sched.initialize_db()
    
    yield sched
    
    # Clean up if it was started during test
    try:
        await sched.shutdown()
    except:
        pass


@pytest.fixture
def sample_job():
    """Sample async job function for testing."""
    async def job(x: int = 1, y: int = 2):
        """Sample job that adds two numbers."""
        await asyncio.sleep(0.01)  # Simulate some work
        return x + y
    return job


@pytest.fixture
def failing_job():
    """Sample job that always fails."""
    async def job():
        """Job that raises an exception."""
        await asyncio.sleep(0.01)
        raise ValueError("Intentional test failure")
    return job


@pytest.fixture
def slow_job():
    """Sample job that takes a while to execute."""
    async def job(duration: float = 1.0):
        """Job that sleeps for the specified duration."""
        await asyncio.sleep(duration)
        return f"Slept for {duration}s"
    return job


@pytest.fixture
def mock_time():
    """Helper to get consistent test times."""
    def _get_time(offset_seconds: int = 0):
        return datetime.now(UTC).replace(microsecond=0) + timedelta(seconds=offset_seconds)
    return _get_time


# Import helpers
from datetime import timedelta


@pytest.fixture
def time_utils():
    """Utility functions for time manipulation in tests."""
    class TimeUtils:
        @staticmethod
        def now():
            return datetime.now(UTC)
        
        @staticmethod
        def future(seconds: int = 0, minutes: int = 0, hours: int = 0):
            delta = timedelta(seconds=seconds, minutes=minutes, hours=hours)
            return datetime.now(UTC) + delta
        
        @staticmethod
        def past(seconds: int = 0, minutes: int = 0, hours: int = 0):
            delta = timedelta(seconds=seconds, minutes=minutes, hours=hours)
            return datetime.now(UTC) - delta
    
    return TimeUtils()


@pytest.fixture
async def job_counter():
    """
    Counter to track job executions.
    Useful for testing if jobs ran the expected number of times.
    """
    counter = {"count": 0, "executions": []}
    
    async def count_job(**kwargs):
        counter["count"] += 1
        counter["executions"].append({
            "time": datetime.now(UTC),
            "kwargs": kwargs
        })
    
    counter["job"] = count_job
    return counter


