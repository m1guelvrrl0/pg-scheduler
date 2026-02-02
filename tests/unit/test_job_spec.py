"""
Unit tests for JobSpec dataclass.
"""

from datetime import datetime, UTC

import pytest

from pg_scheduler import JobPriority, JobSpec
from pg_scheduler.job_spec import _UNSET


async def sample_async_func():
    pass


class TestJobSpec:
    """Tests for JobSpec dataclass."""
    
    def test_jobspec_required_fields(self):
        """Test that JobSpec requires func and execution_time."""
        execution_time = datetime.now(UTC)
        
        spec = JobSpec(
            func=sample_async_func,
            execution_time=execution_time,
        )
        
        assert spec.func == sample_async_func
        assert spec.execution_time == execution_time
    
    def test_jobspec_default_values(self):
        """Test JobSpec default values."""
        execution_time = datetime.now(UTC)
        
        spec = JobSpec(
            func=sample_async_func,
            execution_time=execution_time,
        )
        
        assert spec.args == ()
        assert spec.kwargs == {}
        assert spec.priority == JobPriority.NORMAL
        assert spec.max_retries == 0
        assert spec.job_id is None
        assert spec.misfire_grace_time is _UNSET
    
    def test_jobspec_custom_values(self):
        """Test JobSpec with custom values."""
        execution_time = datetime.now(UTC)
        
        spec = JobSpec(
            func=sample_async_func,
            execution_time=execution_time,
            args=(1, 2, 3),
            kwargs={"key": "value"},
            priority=JobPriority.CRITICAL,
            max_retries=5,
            job_id="custom-id",
            misfire_grace_time=300,
        )
        
        assert spec.args == (1, 2, 3)
        assert spec.kwargs == {"key": "value"}
        assert spec.priority == JobPriority.CRITICAL
        assert spec.max_retries == 5
        assert spec.job_id == "custom-id"
        assert spec.misfire_grace_time == 300
    
    def test_jobspec_misfire_grace_time_none(self):
        """Test JobSpec with explicit None misfire_grace_time (no expiration)."""
        execution_time = datetime.now(UTC)
        
        spec = JobSpec(
            func=sample_async_func,
            execution_time=execution_time,
            misfire_grace_time=None,
        )
        
        assert spec.misfire_grace_time is None
    
    def test_jobspec_kwargs_mutable_default(self):
        """Test that kwargs default is not shared between instances."""
        execution_time = datetime.now(UTC)
        
        spec1 = JobSpec(func=sample_async_func, execution_time=execution_time)
        spec2 = JobSpec(func=sample_async_func, execution_time=execution_time)
        
        spec1.kwargs["test"] = 1
        
        assert "test" not in spec2.kwargs
    
    def test_jobspec_all_priorities(self):
        """Test JobSpec with all priority levels."""
        execution_time = datetime.now(UTC)
        
        for priority in [JobPriority.CRITICAL, JobPriority.HIGH, JobPriority.NORMAL, JobPriority.LOW]:
            spec = JobSpec(
                func=sample_async_func,
                execution_time=execution_time,
                priority=priority,
            )
            assert spec.priority == priority

