"""
Unit tests for VacuumPolicy and VacuumConfig.
"""

import pytest

from pg_scheduler import VacuumConfig, VacuumPolicy, VacuumTrigger


@pytest.mark.unit
class TestVacuumTrigger:
    """Tests for VacuumTrigger enum."""
    
    def test_trigger_values(self):
        """Test that VacuumTrigger enum values are correct."""
        assert VacuumTrigger.IMMEDIATE.value == "immediate"
        assert VacuumTrigger.TIME_BASED.value == "time_based"
        assert VacuumTrigger.COUNT_BASED.value == "count_based"
        assert VacuumTrigger.NEVER.value == "never"
    
    def test_enum_iteration(self):
        """Test that we can iterate over all triggers."""
        triggers = list(VacuumTrigger)
        assert len(triggers) == 4


@pytest.mark.unit
class TestVacuumPolicy:
    """Tests for VacuumPolicy dataclass."""
    
    def test_immediate_factory(self):
        """Test immediate() factory method."""
        policy = VacuumPolicy.immediate()
        assert policy.trigger == VacuumTrigger.IMMEDIATE
        assert policy.days is None
        assert policy.keep_count is None
    
    def test_after_days_factory(self):
        """Test after_days() factory method."""
        policy = VacuumPolicy.after_days(7)
        assert policy.trigger == VacuumTrigger.TIME_BASED
        assert policy.days == 7
        assert policy.keep_count is None
    
    def test_keep_last_factory(self):
        """Test keep_last() factory method."""
        policy = VacuumPolicy.keep_last(10)
        assert policy.trigger == VacuumTrigger.COUNT_BASED
        assert policy.keep_count == 10
        assert policy.days is None
    
    def test_never_factory(self):
        """Test never() factory method."""
        policy = VacuumPolicy.never()
        assert policy.trigger == VacuumTrigger.NEVER
        assert policy.days is None
        assert policy.keep_count is None
    
    def test_direct_construction(self):
        """Test direct construction of VacuumPolicy."""
        policy = VacuumPolicy(
            trigger=VacuumTrigger.TIME_BASED,
            days=5
        )
        assert policy.trigger == VacuumTrigger.TIME_BASED
        assert policy.days == 5


@pytest.mark.unit
class TestVacuumConfig:
    """Tests for VacuumConfig dataclass."""
    
    def test_default_construction(self):
        """Test that VacuumConfig applies sensible defaults."""
        config = VacuumConfig()
        
        # Check defaults are applied
        assert config.completed is not None
        assert config.failed is not None
        assert config.cancelled is not None
        
        # Check default values
        assert config.completed.trigger == VacuumTrigger.TIME_BASED
        assert config.completed.days == 1
        
        assert config.failed.trigger == VacuumTrigger.TIME_BASED
        assert config.failed.days == 7
        
        assert config.cancelled.trigger == VacuumTrigger.TIME_BASED
        assert config.cancelled.days == 3
        
        # Check other defaults
        assert config.interval_minutes == 60
        assert config.track_metrics is False
    
    def test_custom_policies(self):
        """Test VacuumConfig with custom policies."""
        config = VacuumConfig(
            completed=VacuumPolicy.immediate(),
            failed=VacuumPolicy.never(),
            cancelled=VacuumPolicy.keep_last(5),
            interval_minutes=30,
            track_metrics=True
        )
        
        assert config.completed.trigger == VacuumTrigger.IMMEDIATE
        assert config.failed.trigger == VacuumTrigger.NEVER
        assert config.cancelled.trigger == VacuumTrigger.COUNT_BASED
        assert config.cancelled.keep_count == 5
        assert config.interval_minutes == 30
        assert config.track_metrics is True
    
    def test_partial_custom_policies(self):
        """Test that only specified policies are overridden."""
        config = VacuumConfig(
            completed=VacuumPolicy.immediate()
        )
        
        # Custom policy
        assert config.completed.trigger == VacuumTrigger.IMMEDIATE
        
        # Default policies still applied
        assert config.failed.trigger == VacuumTrigger.TIME_BASED
        assert config.failed.days == 7
        assert config.cancelled.trigger == VacuumTrigger.TIME_BASED
        assert config.cancelled.days == 3
    
    def test_post_init_called(self):
        """Test that __post_init__ is called and applies defaults."""
        # Create with None values explicitly
        config = VacuumConfig(
            completed=None,
            failed=None,
            cancelled=None
        )
        
        # Defaults should be applied by __post_init__
        assert config.completed is not None
        assert config.failed is not None
        assert config.cancelled is not None

