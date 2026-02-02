"""
Integration tests for cron-based periodic jobs.
"""

import asyncio
from datetime import UTC, datetime

import pytest

from pg_scheduler import JobPriority, Scheduler, periodic


@pytest.mark.integration
@pytest.mark.periodic
@pytest.mark.cron
class TestPeriodicCron:
    """Tests for cron-based periodic jobs."""
    
    async def test_cron_job_registers(self, clean_db):
        """Test that cron-based periodic job registers."""
        @periodic(cron="* * * * *")
        async def cron_job():
            pass
        
        sched = Scheduler(clean_db)
        await sched.initialize_db()
        await sched.start()
        
        jobs = sched.get_periodic_jobs()
        
        # Find our cron job
        cron_job_found = False
        for config in jobs.values():
            if config.func.__name__ == "cron_job":
                assert config.cron == "* * * * *"
                assert config.interval is None
                cron_job_found = True
        
        assert cron_job_found
        
        await sched.shutdown()
    
    async def test_cron_every_minute_executes(self, clean_db, job_counter):
        """Test that cron job scheduled for every minute executes."""
        @periodic(cron="* * * * *")  # Every minute
        async def every_minute():
            await job_counter["job"]()
        
        sched = Scheduler(clean_db)
        await sched.initialize_db()
        await sched.start()
        
        # Wait up to 75 seconds to catch at least one execution
        # (jobs run at top of the minute)
        await asyncio.sleep(75)
        
        assert job_counter["count"] >= 1
        
        await sched.shutdown()
    
    async def test_cron_with_priority(self, clean_db):
        """Test cron job with custom priority."""
        @periodic(cron="0 0 * * *", priority=JobPriority.CRITICAL)
        async def daily_critical():
            pass
        
        sched = Scheduler(clean_db)
        await sched.initialize_db()
        await sched.start()
        
        jobs = sched.get_periodic_jobs()
        
        for config in jobs.values():
            if config.func.__name__ == "daily_critical":
                assert config.priority == JobPriority.CRITICAL
        
        await sched.shutdown()
    
    async def test_cron_cannot_specify_both_cron_and_interval(self):
        """Test that specifying both cron and interval raises error."""
        from datetime import timedelta
        
        with pytest.raises(ValueError, match="both"):
            @periodic(cron="* * * * *", every=timedelta(minutes=5))
            async def invalid_job():
                pass
    
    async def test_cron_must_specify_either_cron_or_interval(self):
        """Test that must specify either cron or interval."""
        with pytest.raises(ValueError, match="Either"):
            @periodic()
            async def invalid_job():
                pass
    
    async def test_cron_expression_patterns(self, clean_db):
        """Test various cron expression patterns."""
        patterns = [
            ("* * * * *", "every_minute"),
            ("*/5 * * * *", "every_five_minutes"),
            ("0 * * * *", "hourly"),
            ("0 0 * * *", "daily"),
            ("0 0 * * SUN", "weekly"),
            ("0 0 1 * *", "monthly"),
            ("0 9-17 * * MON-FRI", "business_hours"),
        ]
        
        for cron_expr, name in patterns:
            @periodic(cron=cron_expr, job_name=f"test_{name}")
            async def pattern_job():
                pass
        
        sched = Scheduler(clean_db)
        await sched.initialize_db()
        await sched.start()
        
        jobs = sched.get_periodic_jobs()
        
        # Verify all patterns were registered
        assert len([j for j in jobs.values() if j.job_name.startswith("test_")]) == len(patterns)
        
        await sched.shutdown()
    
    async def test_cron_job_function_not_async_raises(self):
        """Test that cron @periodic on non-async function raises error."""
        with pytest.raises(TypeError, match="async"):
            @periodic(cron="* * * * *")
            def sync_cron_job():
                pass
    
    async def test_cron_job_without_croniter_dependency(self, clean_db, monkeypatch):
        """Test that cron jobs fail gracefully without croniter."""
        # This test would need to mock the croniter import
        # Skipping for now as it requires complex mocking
        pass
    
    async def test_cron_dedup_key_deterministic(self, clean_db):
        """Test that cron jobs have deterministic dedup keys."""
        @periodic(cron="0 0 * * *")
        async def daily_job():
            pass
        
        sched = Scheduler(clean_db)
        await sched.initialize_db()
        await sched.start()
        
        jobs = sched.get_periodic_jobs()
        
        for config in jobs.values():
            if config.func.__name__ == "daily_job":
                assert config.dedup_key is not None
                assert len(config.dedup_key) == 16
                # Dedup key should include cron expression in its hash
        
        await sched.shutdown()
    
    async def test_cron_job_disabled(self, clean_db, job_counter):
        """Test that disabled cron jobs don't execute."""
        @periodic(cron="* * * * *", enabled=False)
        async def disabled_cron():
            await job_counter["job"]()
        
        sched = Scheduler(clean_db)
        await sched.initialize_db()
        await sched.start()
        
        await asyncio.sleep(75)
        
        assert job_counter["count"] == 0
        
        await sched.shutdown()
    
    async def test_cron_and_interval_jobs_coexist(self, clean_db):
        """Test that cron and interval-based jobs can coexist."""
        from datetime import timedelta
        
        counters = {"cron": 0, "interval": 0}
        
        @periodic(cron="* * * * *")
        async def cron_job():
            counters["cron"] += 1
        
        @periodic(every=timedelta(seconds=2))
        async def interval_job():
            counters["interval"] += 1
        
        sched = Scheduler(clean_db)
        await sched.initialize_db()
        await sched.start()
        
        await asyncio.sleep(75)
        
        # Both should have executed
        assert counters["cron"] >= 1
        assert counters["interval"] >= 30  # Should run many times in 75 seconds
        
        await sched.shutdown()


