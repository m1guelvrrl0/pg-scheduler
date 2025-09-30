"""
PG Scheduler - A PostgreSQL-based async job scheduler with deduplication and reliability features.

This library provides a robust job scheduling system built on PostgreSQL with features like:
- Periodic jobs with @periodic decorator
- Cross-replica deduplication 
- Self-rescheduling jobs
- Advisory lock support
- Priority queues and retry logic
- Vacuum policies for job cleanup
- Graceful shutdown and error handling
"""

from .scheduler import (
    Scheduler,
    JobPriority,
    ConflictResolution,
    VacuumPolicy,
    VacuumConfig,
    VacuumTrigger,
    periodic,
)

__version__ = "0.1.0"
__author__ = "Miguel Rebelo"
__email__ = "miguel.python.dev@gmail.com"

__all__ = [
    "Scheduler",
    "JobPriority", 
    "ConflictResolution",
    "VacuumPolicy",
    "VacuumConfig", 
    "VacuumTrigger",
    "periodic",
]
