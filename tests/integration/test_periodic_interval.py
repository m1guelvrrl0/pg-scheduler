"""
Integration tests for interval-based periodic jobs.
"""

import asyncio
from datetime import timedelta

import pytest

from pg_scheduler import JobPriority, Scheduler, periodic


@pytest.mark.integration
@pytest.mark.periodic
class TestPeriodicInterval:
    """Tests for interval-based periodic jobs."""
    
    async def test_periodic_job_registers(self, clean_db):
        """Test that @periodic decorator registers jobs."""
        @periodic(every=timedelta(minutes=5))
        async def test_job():
            pass
        
        # Create scheduler and start it
        sched = Scheduler(clean_db)
        await sched.initialize_db()
        await sched.start()
        
        # Check that periodic job was registered
        jobs = sched.get_periodic_jobs()
        assert len(jobs) > 0
        
        await sched.shutdown()
    
    async def test_periodic_job_executes(self, clean_db, job_counter):
        """Test that periodic job executes."""
        @periodic(every=timedelta(seconds=2))
        async def counting_job():
            await job_counter["job"]()
        
        sched = Scheduler(clean_db)
        await sched.initialize_db()
        await sched.start()
        
        # Wait for at least 2 executions
        await asyncio.sleep(5)
        
        assert job_counter["count"] >= 2
        
        await sched.shutdown()
    
    async def test_periodic_job_reschedules(self, clean_db, job_counter):
        """Test that periodic jobs reschedule themselves."""
        execution_times = []
        
        @periodic(every=timedelta(seconds=2))
        async def tracking_job():
            execution_times.append(asyncio.get_event_loop().time())
            await job_counter["job"]()
        
        sched = Scheduler(clean_db)
        await sched.initialize_db()
        await sched.start()
        
        # Wait for multiple executions
        await asyncio.sleep(7)
        
        assert job_counter["count"] >= 3
        
        # Check that executions are roughly 2 seconds apart
        if len(execution_times) >= 2:
            intervals = [execution_times[i+1] - execution_times[i] 
                        for i in range(len(execution_times)-1)]
            for interval in intervals:
                # Allow some tolerance
                assert 1.5 < interval < 3.0
        
        await sched.shutdown()
    
    async def test_periodic_job_with_priority(self, clean_db):
        """Test periodic job with custom priority."""
        @periodic(every=timedelta(minutes=5), priority=JobPriority.HIGH)
        async def high_priority_job():
            pass
        
        sched = Scheduler(clean_db)
        await sched.initialize_db()
        await sched.start()
        
        jobs = sched.get_periodic_jobs()
        
        # Find our job and check priority
        job_found = False
        for job_config in jobs.values():
            if job_config.func.__name__ == "high_priority_job":
                assert job_config.priority == JobPriority.HIGH
                job_found = True
        
        assert job_found
        
        await sched.shutdown()
    
    async def test_periodic_job_with_retries(self, clean_db):
        """Test periodic job with max_retries."""
        attempts = []
        
        @periodic(every=timedelta(seconds=2), max_retries=2)
        async def flaky_job():
            attempts.append(1)
            if len(attempts) < 2:
                raise ValueError("Temporary failure")
        
        sched = Scheduler(clean_db)
        await sched.initialize_db()
        await sched.start()
        
        await asyncio.sleep(5)
        
        # Should have retried
        assert len(attempts) >= 2
        
        await sched.shutdown()
    
    async def test_periodic_job_disabled(self, clean_db, job_counter):
        """Test that disabled periodic jobs don't execute."""
        @periodic(every=timedelta(seconds=1), enabled=False)
        async def disabled_job():
            await job_counter["job"]()
        
        sched = Scheduler(clean_db)
        await sched.initialize_db()
        await sched.start()
        
        await asyncio.sleep(3)
        
        # Should not have executed
        assert job_counter["count"] == 0
        
        await sched.shutdown()
    
    async def test_periodic_job_custom_name(self, clean_db):
        """Test periodic job with custom name."""
        @periodic(every=timedelta(minutes=5), job_name="my_custom_job")
        async def some_job():
            pass
        
        sched = Scheduler(clean_db)
        await sched.initialize_db()
        await sched.start()
        
        jobs = sched.get_periodic_jobs()
        
        # Find job by custom name
        job_found = any(
            config.job_name == "my_custom_job"
            for config in jobs.values()
        )
        
        assert job_found
        
        await sched.shutdown()
    
    async def test_multiple_periodic_jobs(self, clean_db):
        """Test that multiple periodic jobs can coexist."""
        counters = {"job1": 0, "job2": 0, "job3": 0}
        
        @periodic(every=timedelta(seconds=1))
        async def job1():
            counters["job1"] += 1
        
        @periodic(every=timedelta(seconds=2))
        async def job2():
            counters["job2"] += 1
        
        @periodic(every=timedelta(seconds=3))
        async def job3():
            counters["job3"] += 1
        
        sched = Scheduler(clean_db)
        await sched.initialize_db()
        await sched.start()
        
        await asyncio.sleep(7)
        
        # job1 should run most frequently
        assert counters["job1"] >= 5
        assert counters["job2"] >= 2
        assert counters["job3"] >= 1
        
        await sched.shutdown()
    
    async def test_periodic_job_function_not_async_raises(self):
        """Test that @periodic on non-async function raises error."""
        with pytest.raises(TypeError, match="async functions"):
            @periodic(every=timedelta(minutes=1))
            def sync_job():
                pass
    
    async def test_periodic_job_deduplication(self, clean_db):
        """Test that periodic jobs have deterministic dedup keys."""
        @periodic(every=timedelta(minutes=5))
        async def dedup_job():
            pass
        
        sched = Scheduler(clean_db)
        await sched.initialize_db()
        
        jobs = sched.get_periodic_jobs()
        
        # All jobs should have dedup keys
        for config in jobs.values():
            if config.func.__name__ == "dedup_job":
                assert config.dedup_key is not None
                assert len(config.dedup_key) == 16  # SHA256 hash truncated
        
        await sched.shutdown()


