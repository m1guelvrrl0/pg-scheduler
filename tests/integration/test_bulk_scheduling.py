"""
Integration tests for bulk job scheduling functionality.
"""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from pg_scheduler import ConflictResolution, JobPriority, JobSpec, Scheduler


# Module-level test result storage
_bulk_results = {}


async def bulk_test_job(test_id: str, index: int):
    """Simple test job for bulk scheduling."""
    if test_id not in _bulk_results:
        _bulk_results[test_id] = []
    _bulk_results[test_id].append(index)


async def bulk_args_job(test_id: str, x: int, y: int):
    """Test job with args for bulk scheduling."""
    if test_id not in _bulk_results:
        _bulk_results[test_id] = []
    _bulk_results[test_id].append(x + y)


@pytest.mark.integration
class TestBulkScheduling:
    """Tests for bulk job scheduling operations."""
    
    def setup_method(self):
        """Clear test results before each test."""
        _bulk_results.clear()
    
    async def test_schedule_bulk_empty_list(self, scheduler):
        """Test scheduling an empty list returns empty list."""
        job_ids = await scheduler.schedule_bulk([])
        assert job_ids == []
    
    async def test_schedule_bulk_single_job(self, scheduler, time_utils):
        """Test bulk scheduling with a single job."""
        test_id = "test_bulk_single"
        execution_time = time_utils.future(seconds=1)
        
        jobs = [
            JobSpec(
                func=bulk_test_job,
                execution_time=execution_time,
                args=(test_id, 0),
            )
        ]
        
        job_ids = await scheduler.schedule_bulk(jobs)
        
        assert len(job_ids) == 1
        assert all(isinstance(jid, str) for jid in job_ids)
    
    async def test_schedule_bulk_multiple_jobs(self, scheduler, time_utils):
        """Test bulk scheduling multiple jobs at once."""
        test_id = "test_bulk_multiple"
        base_time = time_utils.future(seconds=1)
        
        jobs = [
            JobSpec(
                func=bulk_test_job,
                execution_time=base_time + timedelta(seconds=i),
                args=(test_id, i),
            )
            for i in range(10)
        ]
        
        job_ids = await scheduler.schedule_bulk(jobs)
        
        assert len(job_ids) == 10
        # All job IDs should be unique
        assert len(set(job_ids)) == 10
    
    async def test_schedule_bulk_execution(self, scheduler, time_utils):
        """Test that bulk scheduled jobs actually execute."""
        test_id = "test_bulk_execution"
        execution_time = time_utils.future(seconds=1)
        
        jobs = [
            JobSpec(
                func=bulk_test_job,
                execution_time=execution_time,
                args=(test_id, i),
            )
            for i in range(5)
        ]
        
        await scheduler.schedule_bulk(jobs)
        
        # Wait for jobs to execute
        await asyncio.sleep(5)
        
        assert test_id in _bulk_results
        assert len(_bulk_results[test_id]) == 5
        assert sorted(_bulk_results[test_id]) == [0, 1, 2, 3, 4]
    
    async def test_schedule_bulk_with_custom_job_ids(self, scheduler, time_utils):
        """Test bulk scheduling with custom job IDs."""
        test_id = "test_bulk_custom_ids"
        execution_time = time_utils.future(seconds=1)
        
        jobs = [
            JobSpec(
                func=bulk_test_job,
                execution_time=execution_time,
                args=(test_id, i),
                job_id=f"custom-bulk-{i}",
            )
            for i in range(3)
        ]
        
        job_ids = await scheduler.schedule_bulk(jobs)
        
        assert len(job_ids) == 3
        assert "custom-bulk-0" in job_ids
        assert "custom-bulk-1" in job_ids
        assert "custom-bulk-2" in job_ids
    
    async def test_schedule_bulk_with_priorities(self, scheduler, time_utils):
        """Test bulk scheduling jobs with different priorities."""
        test_id = "test_bulk_priorities"
        execution_time = time_utils.future(seconds=1)
        
        jobs = [
            JobSpec(
                func=bulk_test_job,
                execution_time=execution_time,
                args=(test_id, 0),
                priority=JobPriority.LOW,
            ),
            JobSpec(
                func=bulk_test_job,
                execution_time=execution_time,
                args=(test_id, 1),
                priority=JobPriority.CRITICAL,
            ),
            JobSpec(
                func=bulk_test_job,
                execution_time=execution_time,
                args=(test_id, 2),
                priority=JobPriority.NORMAL,
            ),
        ]
        
        job_ids = await scheduler.schedule_bulk(jobs)
        
        assert len(job_ids) == 3
    
    async def test_schedule_bulk_conflict_ignore(self, scheduler, time_utils):
        """Test bulk scheduling with IGNORE conflict resolution."""
        test_id = "test_bulk_ignore"
        execution_time = time_utils.future(seconds=10)
        
        # First batch
        jobs1 = [
            JobSpec(
                func=bulk_test_job,
                execution_time=execution_time,
                args=(test_id, i),
                job_id=f"dup-{i}",
            )
            for i in range(3)
        ]
        
        job_ids1 = await scheduler.schedule_bulk(jobs1, conflict_resolution=ConflictResolution.IGNORE)
        
        # Second batch with same job IDs - should be ignored
        jobs2 = [
            JobSpec(
                func=bulk_test_job,
                execution_time=execution_time,
                args=(test_id, i + 100),
                job_id=f"dup-{i}",
            )
            for i in range(3)
        ]
        
        job_ids2 = await scheduler.schedule_bulk(jobs2, conflict_resolution=ConflictResolution.IGNORE)
        
        assert len(job_ids1) == 3
        # With IGNORE, duplicates are skipped so we get fewer returned IDs
        assert len(job_ids2) == 0
    
    async def test_schedule_bulk_conflict_replace(self, scheduler, time_utils):
        """Test bulk scheduling with REPLACE conflict resolution."""
        test_id = "test_bulk_replace"
        execution_time = time_utils.future(seconds=1)
        
        # First batch
        jobs1 = [
            JobSpec(
                func=bulk_test_job,
                execution_time=execution_time,
                args=(test_id, i),
                job_id=f"replace-{i}",
            )
            for i in range(3)
        ]
        
        await scheduler.schedule_bulk(jobs1, conflict_resolution=ConflictResolution.REPLACE)
        
        # Second batch with same job IDs but different args - should replace
        jobs2 = [
            JobSpec(
                func=bulk_test_job,
                execution_time=execution_time,
                args=(test_id, i + 100),
                job_id=f"replace-{i}",
            )
            for i in range(3)
        ]
        
        job_ids2 = await scheduler.schedule_bulk(jobs2, conflict_resolution=ConflictResolution.REPLACE)
        
        assert len(job_ids2) == 3
        
        # Wait for execution and verify replaced values
        await asyncio.sleep(5)
        
        assert test_id in _bulk_results
        # Should have the replaced values (100, 101, 102), not original (0, 1, 2)
        assert sorted(_bulk_results[test_id]) == [100, 101, 102]
    
    async def test_schedule_bulk_with_kwargs(self, scheduler, time_utils):
        """Test bulk scheduling jobs with kwargs."""
        test_id = "test_bulk_kwargs"
        execution_time = time_utils.future(seconds=1)
        
        jobs = [
            JobSpec(
                func=bulk_args_job,
                execution_time=execution_time,
                args=(test_id,),
                kwargs={"x": i, "y": i * 2},
            )
            for i in range(3)
        ]
        
        job_ids = await scheduler.schedule_bulk(jobs)
        
        assert len(job_ids) == 3
        
        await asyncio.sleep(5)
        
        assert test_id in _bulk_results
        # Results should be: 0+0=0, 1+2=3, 2+4=6
        assert sorted(_bulk_results[test_id]) == [0, 3, 6]
    
    async def test_schedule_bulk_with_max_retries(self, scheduler, time_utils):
        """Test bulk scheduling jobs with max_retries."""
        test_id = "test_bulk_retries"
        execution_time = time_utils.future(seconds=1)
        
        jobs = [
            JobSpec(
                func=bulk_test_job,
                execution_time=execution_time,
                args=(test_id, i),
                max_retries=3,
            )
            for i in range(2)
        ]
        
        job_ids = await scheduler.schedule_bulk(jobs)
        
        assert len(job_ids) == 2
    
    async def test_schedule_bulk_validates_async_functions(self, scheduler, time_utils):
        """Test that bulk scheduling validates all functions are async."""
        execution_time = time_utils.future(seconds=1)
        
        def sync_function():
            pass
        
        jobs = [
            JobSpec(
                func=sync_function,
                execution_time=execution_time,
            )
        ]
        
        with pytest.raises(TypeError, match="Expected an async function"):
            await scheduler.schedule_bulk(jobs)
    
    async def test_schedule_bulk_batching(self, scheduler, time_utils):
        """Test that large batches are handled correctly."""
        test_id = "test_bulk_large"
        execution_time = time_utils.future(seconds=1)
        
        # Create more jobs than the default batch size
        num_jobs = 50
        jobs = [
            JobSpec(
                func=bulk_test_job,
                execution_time=execution_time,
                args=(test_id, i),
            )
            for i in range(num_jobs)
        ]
        
        # Use small batch_size to test batching
        job_ids = await scheduler.schedule_bulk(jobs, batch_size=10)
        
        assert len(job_ids) == num_jobs
        assert len(set(job_ids)) == num_jobs  # All unique
    
    async def test_schedule_bulk_mixed_auto_and_custom_ids(self, scheduler, time_utils):
        """Test bulk scheduling with mix of auto-generated and custom IDs."""
        test_id = "test_bulk_mixed_ids"
        execution_time = time_utils.future(seconds=1)
        
        jobs = [
            JobSpec(
                func=bulk_test_job,
                execution_time=execution_time,
                args=(test_id, 0),
                job_id="custom-id-1",
            ),
            JobSpec(
                func=bulk_test_job,
                execution_time=execution_time,
                args=(test_id, 1),
                # No job_id - auto-generated
            ),
            JobSpec(
                func=bulk_test_job,
                execution_time=execution_time,
                args=(test_id, 2),
                job_id="custom-id-2",
            ),
        ]
        
        job_ids = await scheduler.schedule_bulk(jobs)
        
        assert len(job_ids) == 3
        assert "custom-id-1" in job_ids
        assert "custom-id-2" in job_ids

