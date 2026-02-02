"""
Conflict resolution strategies for PG Scheduler.

This module defines how the scheduler handles duplicate job_id conflicts
when scheduling jobs.
"""

from enum import Enum


class ConflictResolution(Enum):
    """Strategies for handling duplicate job_id conflicts"""
    RAISE = "raise"        # Raise ValueError (default, safest)
    IGNORE = "ignore"      # Ignore the new job, return existing job_id  
    REPLACE = "replace"    # Replace/update the existing job with new parameters

