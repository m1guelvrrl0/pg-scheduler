"""
Unit tests for JobPriority enum.
"""

import pytest

from pg_scheduler import JobPriority


@pytest.mark.unit
class TestJobPriority:
    """Tests for JobPriority enum."""
    
    def test_priority_values(self):
        """Test that priority enum values are correct."""
        assert JobPriority.CRITICAL.value == "critical"
        assert JobPriority.HIGH.value == "high"
        assert JobPriority.NORMAL.value == "normal"
        assert JobPriority.LOW.value == "low"
    
    def test_db_value_mapping(self):
        """Test that db_value returns correct integer values."""
        assert JobPriority.CRITICAL.db_value == 1
        assert JobPriority.HIGH.db_value == 3
        assert JobPriority.NORMAL.db_value == 5
        assert JobPriority.LOW.db_value == 8
    
    def test_priority_ordering(self):
        """Test that priorities are ordered correctly (lower number = higher priority)."""
        priorities = [
            JobPriority.CRITICAL,
            JobPriority.HIGH,
            JobPriority.NORMAL,
            JobPriority.LOW
        ]
        
        db_values = [p.db_value for p in priorities]
        assert db_values == sorted(db_values), "Priorities should be in ascending order"
    
    def test_from_db_value_valid(self):
        """Test conversion from database value to enum."""
        assert JobPriority.from_db_value(1) == JobPriority.CRITICAL
        assert JobPriority.from_db_value(3) == JobPriority.HIGH
        assert JobPriority.from_db_value(5) == JobPriority.NORMAL
        assert JobPriority.from_db_value(8) == JobPriority.LOW
    
    def test_from_db_value_invalid(self):
        """Test that invalid db values default to NORMAL."""
        assert JobPriority.from_db_value(99) == JobPriority.NORMAL
        assert JobPriority.from_db_value(0) == JobPriority.NORMAL
        assert JobPriority.from_db_value(-1) == JobPriority.NORMAL
    
    def test_from_db_value_roundtrip(self):
        """Test that conversion to and from db value works correctly."""
        for priority in JobPriority:
            db_val = priority.db_value
            recovered = JobPriority.from_db_value(db_val)
            assert recovered == priority
    
    def test_repr(self):
        """Test string representation of priority."""
        repr_str = repr(JobPriority.CRITICAL)
        assert "JobPriority" in repr_str
        assert "CRITICAL" in repr_str
        assert "critical" in repr_str
        assert "db_value=1" in repr_str
    
    def test_all_priorities_have_unique_db_values(self):
        """Test that all priorities have unique database values."""
        db_values = [p.db_value for p in JobPriority]
        assert len(db_values) == len(set(db_values)), "All db_values should be unique"
    
    def test_enum_membership(self):
        """Test enum membership."""
        assert JobPriority.CRITICAL in JobPriority
        assert JobPriority.HIGH in JobPriority
        assert JobPriority.NORMAL in JobPriority
        assert JobPriority.LOW in JobPriority
    
    def test_enum_iteration(self):
        """Test that we can iterate over all priorities."""
        priorities = list(JobPriority)
        assert len(priorities) == 4
        assert JobPriority.CRITICAL in priorities
        assert JobPriority.HIGH in priorities
        assert JobPriority.NORMAL in priorities
        assert JobPriority.LOW in priorities

