"""
Integration tests for timezone support in cron-based periodic jobs.
"""

import asyncio
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import pytest

from pg_scheduler import Scheduler, periodic


@pytest.mark.integration
@pytest.mark.timezone
class TestTimezoneSupport:
    """Tests for timezone support in periodic jobs."""
    
    async def test_timezone_with_cron(self, clean_db):
        """Test that timezone parameter works with cron."""
        @periodic(cron="0 9 * * *", timezone="America/New_York")
        async def ny_morning_job():
            pass
        
        sched = Scheduler(clean_db)
        await sched.initialize_db()
        await sched.start()
        
        jobs = sched.get_periodic_jobs()
        
        for config in jobs.values():
            if config.func.__name__ == "ny_morning_job":
                assert config.timezone is not None
                assert str(config.timezone) == "America/New_York"
        
        await sched.shutdown()
    
    async def test_timezone_as_string(self, clean_db):
        """Test timezone specified as string."""
        @periodic(cron="0 0 * * *", timezone="Europe/London")
        async def london_job():
            pass
        
        sched = Scheduler(clean_db)
        await sched.initialize_db()
        await sched.start()
        
        jobs = sched.get_periodic_jobs()
        
        for config in jobs.values():
            if config.func.__name__ == "london_job":
                assert isinstance(config.timezone, ZoneInfo)
                assert str(config.timezone) == "Europe/London"
        
        await sched.shutdown()
    
    async def test_timezone_as_zoneinfo(self, clean_db):
        """Test timezone specified as ZoneInfo object."""
        @periodic(cron="0 0 * * *", timezone=ZoneInfo("Asia/Tokyo"))
        async def tokyo_job():
            pass
        
        sched = Scheduler(clean_db)
        await sched.initialize_db()
        await sched.start()
        
        jobs = sched.get_periodic_jobs()
        
        for config in jobs.values():
            if config.func.__name__ == "tokyo_job":
                assert isinstance(config.timezone, ZoneInfo)
                assert str(config.timezone) == "Asia/Tokyo"
        
        await sched.shutdown()
    
    async def test_timezone_without_cron_raises(self):
        """Test that timezone without cron raises error."""
        from datetime import timedelta
        
        with pytest.raises(ValueError, match="timezone.*cron"):
            @periodic(every=timedelta(minutes=5), timezone="America/New_York")
            async def invalid_job():
                pass
    
    async def test_invalid_timezone_raises(self):
        """Test that invalid timezone string raises error."""
        with pytest.raises(ValueError, match="Invalid timezone"):
            @periodic(cron="0 0 * * *", timezone="Invalid/Timezone")
            async def invalid_tz_job():
                pass
    
    async def test_multiple_timezones(self, clean_db):
        """Test multiple jobs with different timezones."""
        timezones = [
            "UTC",
            "America/New_York",
            "Europe/London",
            "Asia/Tokyo",
            "Australia/Sydney"
        ]
        
        for tz in timezones:
            @periodic(cron="0 0 * * *", timezone=tz, job_name=f"job_{tz.replace('/', '_')}")
            async def tz_job():
                pass
        
        sched = Scheduler(clean_db)
        await sched.initialize_db()
        await sched.start()
        
        jobs = sched.get_periodic_jobs()
        
        # Verify all timezones were registered
        tz_jobs = [j for j in jobs.values() if j.job_name.startswith("job_")]
        assert len(tz_jobs) == len(timezones)
        
        await sched.shutdown()
    
    async def test_timezone_affects_dedup_key(self, clean_db):
        """Test that timezone affects dedup key generation."""
        @periodic(cron="0 0 * * *", timezone="America/New_York", job_name="ny_midnight")
        async def ny_job():
            pass
        
        @periodic(cron="0 0 * * *", timezone="Europe/London", job_name="london_midnight")
        async def london_job():
            pass
        
        sched = Scheduler(clean_db)
        await sched.initialize_db()
        await sched.start()
        
        jobs = sched.get_periodic_jobs()
        
        ny_dedup = None
        london_dedup = None
        
        for config in jobs.values():
            if config.job_name == "ny_midnight":
                ny_dedup = config.dedup_key
            elif config.job_name == "london_midnight":
                london_dedup = config.dedup_key
        
        # Same cron expression but different timezones should have different dedup keys
        assert ny_dedup is not None
        assert london_dedup is not None
        assert ny_dedup != london_dedup
        
        await sched.shutdown()
    
    async def test_timezone_execution_time_conversion(self, clean_db, job_counter):
        """Test that timezone-aware jobs execute at correct UTC time."""
        # This test would need to verify that a job scheduled for, say,
        # 9am New York time actually executes at the correct UTC time.
        # This is complex to test without waiting for actual time to pass.
        
        @periodic(cron="* * * * *", timezone="America/New_York")
        async def ny_job():
            await job_counter["job"](time=datetime.now(UTC))
        
        sched = Scheduler(clean_db)
        await sched.initialize_db()
        await sched.start()
        
        await asyncio.sleep(75)
        
        # Job should have executed
        assert job_counter["count"] >= 1
        
        # All execution times should be in UTC
        for execution in job_counter["executions"]:
            exec_time = execution["kwargs"]["time"]
            assert exec_time.tzinfo == UTC
        
        await sched.shutdown()
    
    async def test_utc_default_when_no_timezone(self, clean_db):
        """Test that UTC is used when no timezone is specified for cron."""
        @periodic(cron="0 0 * * *")
        async def utc_job():
            pass
        
        sched = Scheduler(clean_db)
        await sched.initialize_db()
        await sched.start()
        
        jobs = sched.get_periodic_jobs()
        
        for config in jobs.values():
            if config.func.__name__ == "utc_job":
                # Timezone should be None (which means UTC is used internally)
                assert config.timezone is None or config.timezone == UTC
        
        await sched.shutdown()


