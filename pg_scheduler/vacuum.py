"""
Vacuum policies and configuration for PG Scheduler.

This module provides vacuum policies for automatic cleanup of jobs in different
states (completed, failed, cancelled), supporting various cleanup strategies:
- Immediate deletion
- Time-based cleanup (after N days)
- Count-based cleanup (keep last N jobs)
- No automatic cleanup
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class VacuumTrigger(Enum):
    """Vacuum policy trigger types"""
    IMMEDIATE = "immediate"      # Delete immediately on status change
    TIME_BASED = "time_based"    # Delete after X time
    COUNT_BASED = "count_based"  # Keep only last N jobs
    NEVER = "never"              # No automatic cleanup


@dataclass
class VacuumPolicy:
    """Configuration for a vacuum policy"""
    trigger: VacuumTrigger
    days: Optional[int] = None           # For TIME_BASED policies
    keep_count: Optional[int] = None     # For COUNT_BASED policies
    
    @classmethod
    def immediate(cls) -> 'VacuumPolicy':
        """Delete immediately when job reaches this status"""
        return cls(VacuumTrigger.IMMEDIATE)
        
    @classmethod  
    def after_days(cls, days: int) -> 'VacuumPolicy':
        """Delete jobs after N days in this status"""
        return cls(VacuumTrigger.TIME_BASED, days=days)
        
    @classmethod
    def keep_last(cls, count: int) -> 'VacuumPolicy':
        """Keep only the last N jobs per job_name in this status"""
        return cls(VacuumTrigger.COUNT_BASED, keep_count=count)
        
    @classmethod
    def never(cls) -> 'VacuumPolicy':
        """Never automatically clean jobs in this status"""
        return cls(VacuumTrigger.NEVER)


@dataclass
class VacuumConfig:
    """Complete vacuum configuration for the scheduler"""
    completed: VacuumPolicy = None       # Will default to after_days(1)
    failed: VacuumPolicy = None          # Will default to after_days(7)  
    cancelled: VacuumPolicy = None       # Will default to after_days(3)
    interval_minutes: int = 60           # How often to run vacuum
    track_metrics: bool = False          # Whether to store vacuum metrics in DB
    
    def __post_init__(self):
        """Set sensible defaults for None policies"""
        if self.completed is None:
            self.completed = VacuumPolicy.after_days(1)
        if self.failed is None:
            self.failed = VacuumPolicy.after_days(7)
        if self.cancelled is None:
            self.cancelled = VacuumPolicy.after_days(3)

