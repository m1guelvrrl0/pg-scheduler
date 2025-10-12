"""
Integration tests for job priority levels.
"""

import asyncio
from datetime import UTC, datetime

import pytest

from pg_scheduler import JobPriority, Scheduler


@pytest.mark.integration
class TestPriorities:
    """Tests for job priority execution order."""
    
    async def test_priority_execution_order(self, scheduler, time_utils):
        """Test that jobs execute in priority order."""
        results = []
        
        async def priority_job(priority_name):
            results.append(priority_name)
            await asyncio.sleep(0.1)
        
        execution_time = time_utils.future(seconds=1)
        
        # Schedule in random order but with different priorities
        await scheduler.schedule(
            priority_job,
            execution_time=execution_time,
            args=("NORMAL",),
            priority=JobPriority.NORMAL
        )
        
        await scheduler.schedule(
            priority_job,
            execution_time=execution_time,
            args=("CRITICAL",),
            priority=JobPriority.CRITICAL
        )
        
        await scheduler.schedule(
            priority_job,
            execution_time=execution_time,
            args=("LOW",),
            priority=JobPriority.LOW
        )
        
        await scheduler.schedule(
            priority_job,
            execution_time=execution_time,
            args=("HIGH",),
            priority=JobPriority.HIGH
        )
        
        await asyncio.sleep(3)
        
        # Should execute in priority order: CRITICAL, HIGH, NORMAL, LOW
        assert results == ["CRITICAL", "HIGH", "NORMAL", "LOW"]
    
    async def test_critical_priority(self, scheduler, time_utils):
        """Test CRITICAL priority jobs."""
        result = []
        
        async def critical_job():
            result.append("critical")
        
        execution_time = time_utils.future(seconds=1)
        
        await scheduler.schedule(
            critical_job,
            execution_time=execution_time,
            priority=JobPriority.CRITICAL
        )
        
        await asyncio.sleep(2)
        assert result == ["critical"]
    
    async def test_high_priority(self, scheduler, time_utils):
        """Test HIGH priority jobs."""
        result = []
        
        async def high_job():
            result.append("high")
        
        execution_time = time_utils.future(seconds=1)
        
        await scheduler.schedule(
            high_job,
            execution_time=execution_time,
            priority=JobPriority.HIGH
        )
        
        await asyncio.sleep(2)
        assert result == ["high"]
    
    async def test_normal_priority_default(self, scheduler, time_utils):
        """Test that NORMAL is the default priority."""
        result = []
        
        async def normal_job():
            result.append("normal")
        
        execution_time = time_utils.future(seconds=1)
        
        # Don't specify priority - should default to NORMAL
        await scheduler.schedule(
            normal_job,
            execution_time=execution_time
        )
        
        await asyncio.sleep(2)
        assert result == ["normal"]
    
    async def test_low_priority(self, scheduler, time_utils):
        """Test LOW priority jobs."""
        result = []
        
        async def low_job():
            result.append("low")
        
        execution_time = time_utils.future(seconds=1)
        
        await scheduler.schedule(
            low_job,
            execution_time=execution_time,
            priority=JobPriority.LOW
        )
        
        await asyncio.sleep(2)
        assert result == ["low"]
    
    async def test_priority_with_different_execution_times(self, scheduler, time_utils):
        """Test that execution time takes precedence over priority."""
        results = []
        
        async def timed_job(label):
            results.append(label)
        
        now = time_utils.now()
        
        # Low priority but earlier time
        await scheduler.schedule(
            timed_job,
            execution_time=now + timedelta(seconds=1),
            args=("low_early",),
            priority=JobPriority.LOW
        )
        
        # High priority but later time
        await scheduler.schedule(
            timed_job,
            execution_time=now + timedelta(seconds=3),
            args=("high_late",),
            priority=JobPriority.CRITICAL
        )
        
        await asyncio.sleep(4)
        
        # Earlier time should execute first, regardless of priority
        assert results[0] == "low_early"
        assert results[1] == "high_late"
    
    async def test_priority_stored_in_database(self, scheduler, clean_db, time_utils):
        """Test that priority is correctly stored in database."""
        execution_time = time_utils.future(seconds=10)
        
        async def db_job():
            pass
        
        job_id = await scheduler.schedule(
            db_job,
            execution_time=execution_time,
            priority=JobPriority.HIGH,
            job_id="test-priority-job"
        )
        
        # Query database to verify priority
        result = await clean_db.fetchrow(
            "SELECT priority FROM scheduled_jobs WHERE job_id = $1",
            job_id
        )
        
        assert result is not None
        assert result["priority"] == JobPriority.HIGH.db_value  # Should be 3
    
    async def test_all_priority_levels_work(self, scheduler, time_utils):
        """Test that all four priority levels work correctly."""
        from datetime import timedelta
        
        results = []
        
        async def multi_priority_job(level):
            results.append(level)
        
        execution_time = time_utils.future(seconds=1)
        
        priorities = [
            (JobPriority.CRITICAL, "CRITICAL"),
            (JobPriority.HIGH, "HIGH"),
            (JobPriority.NORMAL, "NORMAL"),
            (JobPriority.LOW, "LOW"),
        ]
        
        # Schedule in reverse order
        for priority, label in reversed(priorities):
            await scheduler.schedule(
                multi_priority_job,
                execution_time=execution_time,
                args=(label,),
                priority=priority
            )
        
        await asyncio.sleep(3)
        
        # Should execute in priority order
        assert results == ["CRITICAL", "HIGH", "NORMAL", "LOW"]
    
    async def test_priority_with_retries(self, scheduler, time_utils):
        """Test that priority is maintained through retries."""
        attempts = []
        
        async def failing_priority_job():
            attempts.append(datetime.now(UTC))
            if len(attempts) < 2:
                raise ValueError("Fail once")
        
        execution_time = time_utils.future(seconds=1)
        
        await scheduler.schedule(
            failing_priority_job,
            execution_time=execution_time,
            priority=JobPriority.CRITICAL,
            max_retries=2
        )
        
        await asyncio.sleep(4)
        
        # Should have retried
        assert len(attempts) >= 2


# Import timedelta
from datetime import timedelta


