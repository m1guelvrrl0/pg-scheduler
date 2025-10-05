"""
Job priority levels for PG Scheduler.

This module defines the priority system used for job scheduling.
Lower priority numbers indicate higher priority (execute first).
"""

from enum import Enum

# Module-level constants (created once at import time)
_PRIORITY_TO_DB = {
    "critical": 1,
    "high": 3,
    "normal": 5,
    "low": 8
}

_DB_TO_PRIORITY = {
    1: "CRITICAL",
    3: "HIGH",
    5: "NORMAL",
    8: "LOW"
}


class JobPriority(Enum):
    """
    Job priority levels for scheduling.
    
    Lower priority numbers indicate higher priority (jobs execute first).
    Jobs are processed in ascending priority order: 1 → 3 → 5 → 8.
    
    Attributes:
        CRITICAL: Highest priority (value: 1) - executes first
        HIGH: High priority (value: 3) - important tasks
        NORMAL: Default priority (value: 5) - regular tasks
        LOW: Low priority (value: 8) - background tasks, executes last
        
    Example:
        >>> from pg_scheduler import JobPriority, Scheduler
        >>> 
        >>> # Schedule a critical job
        >>> await scheduler.schedule(
        ...     urgent_function,
        ...     execution_time=datetime.now(UTC) + timedelta(minutes=5),
        ...     priority=JobPriority.CRITICAL
        ... )
        >>> 
        >>> # Schedule a low priority cleanup job
        >>> await scheduler.schedule(
        ...     cleanup_function,
        ...     execution_time=datetime.now(UTC) + timedelta(hours=1),
        ...     priority=JobPriority.LOW
        ... )
    """
    
    CRITICAL = "critical"  # Highest priority (value: 1)
    HIGH = "high"          # High priority (value: 3)
    NORMAL = "normal"      # Default priority (value: 5)
    LOW = "low"            # Low priority (value: 8)
    
    @property
    def db_value(self) -> int:
        """
        Get the database integer value for this priority level.
        
        Returns:
            int: Database priority value (1, 3, 5, or 8)
            
        Note:
            Lower numbers = higher priority. Used for ORDER BY in SQL queries.
        """
        return _PRIORITY_TO_DB[self.value]
    
    @classmethod
    def from_db_value(cls, db_value: int) -> 'JobPriority':
        """
        Convert a database integer value back to a JobPriority enum.
        
        Args:
            db_value: The integer priority value from the database (1, 3, 5, or 8)
            
        Returns:
            JobPriority: The corresponding priority enum member
            
        Note:
            If an unknown value is provided, defaults to NORMAL.
            
        Example:
            >>> priority = JobPriority.from_db_value(1)
            >>> assert priority == JobPriority.CRITICAL
            >>> 
            >>> # Unknown values default to NORMAL
            >>> priority = JobPriority.from_db_value(99)
            >>> assert priority == JobPriority.NORMAL
        """
        member_name = _DB_TO_PRIORITY.get(db_value, "NORMAL")
        return cls[member_name]
    
    def __str__(self) -> str:
        """
        String representation of the priority.
        
        Returns:
            str: The priority label (e.g., "critical", "normal")
        """
        return self.value
    
    def __repr__(self) -> str:
        """
        Developer-friendly representation of the priority.
        
        Returns:
            str: Detailed representation with name, label, and db_value
        """
        return f"<JobPriority.{self.name}: {self.value} (db_value={self.db_value})>"

