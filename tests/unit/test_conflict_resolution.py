"""
Unit tests for ConflictResolution enum.
"""

import pytest

from pg_scheduler import ConflictResolution


@pytest.mark.unit
class TestConflictResolution:
    """Tests for ConflictResolution enum."""
    
    def test_conflict_resolution_values(self):
        """Test that ConflictResolution enum values are correct."""
        assert ConflictResolution.RAISE.value == "raise"
        assert ConflictResolution.IGNORE.value == "ignore"
        assert ConflictResolution.REPLACE.value == "replace"
    
    def test_enum_membership(self):
        """Test enum membership."""
        assert ConflictResolution.RAISE in ConflictResolution
        assert ConflictResolution.IGNORE in ConflictResolution
        assert ConflictResolution.REPLACE in ConflictResolution
    
    def test_enum_iteration(self):
        """Test that we can iterate over all strategies."""
        strategies = list(ConflictResolution)
        assert len(strategies) == 3
        assert ConflictResolution.RAISE in strategies
        assert ConflictResolution.IGNORE in strategies
        assert ConflictResolution.REPLACE in strategies
    
    def test_all_strategies_have_unique_values(self):
        """Test that all strategies have unique values."""
        values = [s.value for s in ConflictResolution]
        assert len(values) == len(set(values)), "All values should be unique"
    
    def test_string_values_are_lowercase(self):
        """Test that all values are lowercase strings."""
        for strategy in ConflictResolution:
            assert strategy.value.islower()
            assert isinstance(strategy.value, str)

