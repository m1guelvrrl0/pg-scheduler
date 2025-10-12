"""
Integration tests for basic job scheduling functionality.
"""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from pg_scheduler import ConflictResolution, JobPriority, Scheduler


# Module-level test result storage
_test_results = {}


async def simple_test_job(test_id: str):
    """Simple test job that records execution."""
    if test_id not in _test_results:
        _test_results[test_id] = []
    _test_results[test_id].append("executed")


async def args_test_job(test_id: str, x: int, y: int):
    """Test job with args."""
    if test_id not in _test_results:
        _test_results[test_id] = []
    _test_results[test_id].append(x + y)


async def kwargs_test_job(test_id: str, x: int = 0, y: int = 0):
    """Test job with kwargs."""
    if test_id not in _test_results:
        _test_results[test_id] = []
    _test_results[test_id].append(x * y)


async def priority_test_job(test_id: str, priority_name: str):
    """Test job for priority testing."""
    if test_id not in _test_results:
        _test_results[test_id] = []
    _test_results[test_id].append(priority_name)


async def failing_test_job(test_id: str, fail_count: int):
    """Test job that fails a specified number of times."""
    if test_id not in _test_results:
        _test_results[test_id] = {"attempts": 0}
    
    _test_results[test_id]["attempts"] += 1
    
    if _test_results[test_id]["attempts"] <= fail_count:
        raise ValueError(f"Intentional failure {_test_results[test_id]['attempts']}")


@pytest.mark.integration
class TestBasicScheduling:
    """Tests for basic job scheduling operations."""
    
    def setup_method(self):
        """Clear test results before each test."""
        _test_results.clear()
    
    async def test_schedule_simple_job(self, scheduler, time_utils):
        """Test scheduling a simple job."""
        test_id = "test_schedule_simple"
        execution_time = time_utils.future(seconds=1)
        
        job_id = await scheduler.schedule(
            simple_test_job,
            execution_time=execution_time,
            args=(test_id,)
        )
        
        assert job_id is not None
        assert isinstance(job_id, str)
    
    async def test_job_execution(self, scheduler, time_utils):
        """Test that scheduled job actually executes."""
        test_id = "test_execution"
        execution_time = time_utils.future(seconds=1)
        
        await scheduler.schedule(
            simple_test_job,
            execution_time=execution_time,
            args=(test_id,)
        )
        
        # Wait for job to execute
        await asyncio.sleep(3)
        
        assert test_id in _test_results
        assert len(_test_results[test_id]) == 1
    
    async def test_job_with_args(self, scheduler, time_utils):
        """Test scheduling job with positional arguments."""
        test_id = "test_args"
        execution_time = time_utils.future(seconds=1)
        
        await scheduler.schedule(
            args_test_job,
            execution_time=execution_time,
            args=(test_id, 5, 3)
        )
        
        await asyncio.sleep(3)
        assert test_id in _test_results
        assert _test_results[test_id] == [8]
    
    async def test_job_with_kwargs(self, scheduler, time_utils):
        """Test scheduling job with keyword arguments."""
        test_id = "test_kwargs"
        execution_time = time_utils.future(seconds=1)
        
        await scheduler.schedule(
            kwargs_test_job,
            execution_time=execution_time,
            kwargs={"test_id": test_id, "x": 4, "y": 7}
        )
        
        await asyncio.sleep(3)
        assert test_id in _test_results
        assert _test_results[test_id] == [28]
    
    async def test_job_with_args_and_kwargs(self, scheduler, time_utils):
        """Test scheduling job with both args and kwargs."""
        test_id = "test_both"
        execution_time = time_utils.future(seconds=1)
        
        await scheduler.schedule(
            args_test_job,
            execution_time=execution_time,
            args=(test_id, 5),
            kwargs={"y": 5}
        )
        
        await asyncio.sleep(3)
        assert test_id in _test_results
        assert _test_results[test_id] == [10]
    
    async def test_multiple_jobs(self, scheduler, job_counter, time_utils):
        """Test scheduling multiple jobs."""
        execution_time = time_utils.future(seconds=1)
        
        # Schedule 5 jobs
        for i in range(5):
            await scheduler.schedule(
                job_counter["job"],
                execution_time=execution_time,
                kwargs={"index": i}
            )
        
        await asyncio.sleep(3)
        assert job_counter["count"] == 5
    
    async def test_job_with_custom_job_id(self, scheduler, sample_job, time_utils):
        """Test scheduling job with custom job_id."""
        custom_id = "my-custom-job-id-123"
        execution_time = time_utils.future(seconds=1)
        
        job_id = await scheduler.schedule(
            sample_job,
            execution_time=execution_time,
            job_id=custom_id
        )
        
        assert job_id == custom_id
    
    async def test_duplicate_job_id_raises(self, scheduler, sample_job, time_utils):
        """Test that duplicate job_id raises error by default."""
        job_id = "duplicate-id"
        execution_time = time_utils.future(seconds=10)
        
        # First schedule should succeed
        await scheduler.schedule(
            sample_job,
            execution_time=execution_time,
            job_id=job_id
        )
        
        # Second schedule with same ID should raise
        with pytest.raises(ValueError, match="already exists"):
            await scheduler.schedule(
                sample_job,
                execution_time=execution_time,
                job_id=job_id
            )
    
    async def test_duplicate_job_id_ignore(self, scheduler, sample_job, time_utils):
        """Test IGNORE conflict resolution strategy."""
        job_id = "duplicate-id"
        execution_time = time_utils.future(seconds=10)
        
        # First schedule
        first_id = await scheduler.schedule(
            sample_job,
            execution_time=execution_time,
            job_id=job_id
        )
        
        # Second schedule with IGNORE - should return existing job_id
        second_id = await scheduler.schedule(
            sample_job,
            execution_time=execution_time,
            job_id=job_id,
            conflict_resolution=ConflictResolution.IGNORE
        )
        
        assert first_id == second_id == job_id
    
    async def test_duplicate_job_id_replace(self, scheduler, sample_job, time_utils):
        """Test REPLACE conflict resolution strategy."""
        job_id = "replaceable-id"
        first_time = time_utils.future(seconds=10)
        second_time = time_utils.future(seconds=20)
        
        # First schedule
        await scheduler.schedule(
            sample_job,
            execution_time=first_time,
            job_id=job_id,
            priority=JobPriority.NORMAL
        )
        
        # Replace with different execution time and priority
        await scheduler.schedule(
            sample_job,
            execution_time=second_time,
            job_id=job_id,
            priority=JobPriority.HIGH,
            conflict_resolution=ConflictResolution.REPLACE
        )
        
        # Verify the job was replaced (would need to query DB to fully verify)
        # For now, just verify no exception was raised
    
    async def test_job_in_past_executes_immediately(self, scheduler, job_counter, time_utils):
        """Test that job scheduled in the past executes immediately."""
        past_time = time_utils.past(seconds=5)
        
        await scheduler.schedule(
            job_counter["job"],
            execution_time=past_time
        )
        
        # Should execute quickly
        await asyncio.sleep(1)
        assert job_counter["count"] == 1
    
    async def test_job_order_by_time(self, scheduler, time_utils):
        """Test that jobs execute in order of their execution_time."""
        results = []
        
        async def ordered_job(order):
            results.append(order)
        
        now = time_utils.now()
        
        # Schedule in reverse order
        await scheduler.schedule(ordered_job, execution_time=now + timedelta(seconds=3), args=(3,))
        await scheduler.schedule(ordered_job, execution_time=now + timedelta(seconds=1), args=(1,))
        await scheduler.schedule(ordered_job, execution_time=now + timedelta(seconds=2), args=(2,))
        
        await asyncio.sleep(4)
        
        # Should execute in time order, not schedule order
        assert results == [1, 2, 3]
    
    async def test_failed_job_with_retries(self, scheduler, time_utils):
        """Test that failed jobs are retried."""
        attempts = []
        
        async def failing_job():
            attempts.append(datetime.now(UTC))
            if len(attempts) < 3:
                raise ValueError("Not yet!")
            return "success"
        
        execution_time = time_utils.future(seconds=1)
        
        await scheduler.schedule(
            failing_job,
            execution_time=execution_time,
            max_retries=3
        )
        
        await asyncio.sleep(5)
        
        # Should have attempted 3 times (initial + 2 retries before success)
        assert len(attempts) >= 3
    
    async def test_scheduler_max_concurrent_jobs(self, scheduler_no_start, time_utils):
        """Test that max_concurrent_jobs is respected."""
        # Create scheduler with max_concurrent_jobs=2
        scheduler = Scheduler(scheduler_no_start.db_pool, max_concurrent_jobs=2)
        await scheduler.initialize_db()
        await scheduler.start()
        
        active_count = []
        max_concurrent = [0]
        
        async def tracking_job():
            active_count.append(1)
            current = sum(active_count)
            max_concurrent[0] = max(max_concurrent[0], current)
            
            await asyncio.sleep(0.5)
            active_count.pop()
        
        execution_time = time_utils.future(seconds=1)
        
        # Schedule 5 jobs
        for _ in range(5):
            await scheduler.schedule(tracking_job, execution_time=execution_time)
        
        await asyncio.sleep(4)
        
        # Max concurrent should not exceed 2
        assert max_concurrent[0] <= 2
        
        await scheduler.shutdown()

