"""
Periodic job functionality for PG Scheduler.

This module provides the @periodic decorator and related infrastructure for
scheduling recurring jobs with cross-replica deduplication and optional
advisory lock support.

Features:
- Interval-based scheduling (e.g., every 5 minutes)
- Cron expression scheduling (e.g., "0 0 * * *" for daily at midnight)
- Timezone support using zoneinfo (Python 3.9+ standard library)
"""

import datetime
import functools
import hashlib
import inspect
import logging
import struct
from dataclasses import dataclass
from datetime import UTC, timedelta
from typing import Callable, Dict, Optional, Union, TYPE_CHECKING
from zoneinfo import ZoneInfo

try:
    from croniter import croniter
    CRONITER_AVAILABLE = True
except ImportError:
    CRONITER_AVAILABLE = False
    croniter = None

from .job_priority import JobPriority

if TYPE_CHECKING:
    from .scheduler import Scheduler
    from .conflict_resolution import ConflictResolution

logger = logging.getLogger(__name__)


@dataclass
class PeriodicJobConfig:
    """
    Configuration for a periodic job.
    
    Supports two scheduling modes:
    1. Interval-based: Specify `interval` (timedelta)
    2. Cron-based: Specify `cron` (cron expression string)
    
    Args:
        func: The async function to execute periodically
        interval: Time interval between executions (mutually exclusive with cron)
        cron: Cron expression (e.g., "0 0 * * *" for daily at midnight) (mutually exclusive with interval)
        timezone: Timezone for cron scheduling (e.g., "America/New_York", "Europe/London")
                  Uses UTC if not specified. Requires cron to be set.
        use_advisory_lock: Use PostgreSQL advisory locks for exclusive execution
        priority: Job priority (defaults to NORMAL)
        max_retries: Maximum retry attempts for failed executions
        job_name: Custom job name (auto-generated if None)
        dedup_key: Custom deduplication key (auto-generated if None)
        enabled: Whether the periodic job is enabled
    """
    func: Callable
    interval: Optional[timedelta] = None
    cron: Optional[str] = None
    timezone: Optional[Union[str, ZoneInfo]] = None
    use_advisory_lock: bool = False
    priority: 'JobPriority' = None  # Will default to JobPriority.NORMAL
    max_retries: int = 0
    job_name: Optional[str] = None  # Auto-generated from function name if None
    dedup_key: Optional[str] = None  # Auto-generated if None
    enabled: bool = True
    
    def __post_init__(self):
        """Set defaults and generate dedup key"""
        # Validate scheduling mode
        if self.interval is None and self.cron is None:
            raise ValueError("Either 'interval' or 'cron' must be specified")
        if self.interval is not None and self.cron is not None:
            raise ValueError("Cannot specify both 'interval' and 'cron'")
        
        # Validate cron availability
        if self.cron is not None and not CRONITER_AVAILABLE:
            raise ImportError(
                "croniter is required for cron-based scheduling. "
                "Install it with: pip install croniter>=3.0.0"
            )
        
        # Validate timezone
        if self.timezone is not None:
            if self.cron is None:
                raise ValueError("'timezone' can only be used with cron-based scheduling")
            # Convert string timezone to ZoneInfo
            if isinstance(self.timezone, str):
                try:
                    self.timezone = ZoneInfo(self.timezone)
                except Exception as e:
                    raise ValueError(f"Invalid timezone '{self.timezone}': {e}")
        
        # Set defaults
        if self.priority is None:
            self.priority = JobPriority.NORMAL
        if self.job_name is None:
            self.job_name = f"periodic_{self.func.__name__}"
        if self.dedup_key is None:
            # Generate deterministic dedup key based on function and schedule
            func_signature = f"{self.func.__module__}.{self.func.__name__}"
            if self.interval:
                schedule_str = f"interval:{self.interval.total_seconds()}"
            else:
                tz_str = str(self.timezone) if self.timezone else "UTC"
                schedule_str = f"cron:{self.cron}:tz:{tz_str}"
            key_material = f"{func_signature}:{schedule_str}"
            self.dedup_key = hashlib.sha256(key_material.encode()).hexdigest()[:16]


class PeriodicJobRegistry:
    """Registry for periodic jobs"""
    def __init__(self):
        self._periodic_jobs: Dict[str, PeriodicJobConfig] = {}
        self._scheduler: Optional['Scheduler'] = None
    
    def register(self, config: PeriodicJobConfig):
        """Register a periodic job"""
        self._periodic_jobs[config.dedup_key] = config
        schedule_desc = config.cron if config.cron else f"every {config.interval}"
        tz_desc = f" ({config.timezone})" if config.timezone else ""
        logger.debug(f"Registered periodic job: {config.job_name} ({schedule_desc}{tz_desc}, dedup_key={config.dedup_key})")
    
    def set_scheduler(self, scheduler: 'Scheduler'):
        """Set the scheduler instance"""
        self._scheduler = scheduler
    
    def get_jobs(self) -> Dict[str, PeriodicJobConfig]:
        """Get all registered periodic jobs"""
        return self._periodic_jobs.copy()
    
    def _calculate_next_run(self, config: PeriodicJobConfig, base_time: Optional[datetime.datetime] = None) -> datetime.datetime:
        """
        Calculate the next execution time for a periodic job.
        
        Args:
            config: The periodic job configuration
            base_time: Base time to calculate from (defaults to now in appropriate timezone)
        
        Returns:
            Next execution time as timezone-aware datetime
        """
        if config.interval:
            # Interval-based scheduling
            if base_time is None:
                base_time = datetime.datetime.now(UTC)
            return base_time + config.interval
        
        elif config.cron:
            # Cron-based scheduling
            tz = config.timezone or UTC
            if base_time is None:
                base_time = datetime.datetime.now(tz)
            elif base_time.tzinfo != tz:
                # Convert base_time to the correct timezone
                base_time = base_time.astimezone(tz)
            
            # Use croniter to calculate next occurrence
            cron_iter = croniter(config.cron, base_time)
            next_run = cron_iter.get_next(datetime.datetime)
            
            # Ensure the result is timezone-aware
            if next_run.tzinfo is None:
                next_run = next_run.replace(tzinfo=tz)
            
            # Convert to UTC for internal use
            return next_run.astimezone(UTC)
        
        else:
            raise ValueError("Invalid periodic job configuration: neither interval nor cron specified")
    
    async def start_all_jobs(self):
        """Start all enabled periodic jobs"""
        if not self._scheduler:
            raise RuntimeError("No scheduler set on periodic job registry")
        
        for config in self._periodic_jobs.values():
            if config.enabled:
                await self._start_periodic_job(config)
    
    async def _start_periodic_job(self, config: PeriodicJobConfig):
        """Start a single periodic job"""
        # Import here to avoid circular import
        from .conflict_resolution import ConflictResolution
        
        # Calculate next execution time
        next_run = self._calculate_next_run(config)
        
        # Create dedup job ID for this window
        window_key = self._get_window_key(next_run, config)
        job_id = f"periodic:{config.dedup_key}:{window_key}"
        
        try:
            await self._scheduler.schedule(
                self._create_periodic_wrapper(config),
                execution_time=next_run,
                job_id=job_id,
                conflict_resolution=ConflictResolution.IGNORE,  # Dedup across replicas
                priority=config.priority,
                max_retries=config.max_retries
            )
            logger.debug(f"Scheduled periodic job {config.job_name} for {next_run}")
        except Exception as e:
            logger.error(f"Failed to schedule periodic job {config.job_name}: {e}")
    
    def _get_window_key(self, execution_time: datetime.datetime, config: PeriodicJobConfig) -> str:
        """
        Generate a window key for deduplication within time windows.
        
        For interval-based jobs: rounds down to interval boundary
        For cron-based jobs: uses the exact execution timestamp (rounded to minute)
        """
        if config.interval:
            # Interval-based: use window number
            epoch = datetime.datetime(1970, 1, 1, tzinfo=UTC)
            seconds_since_epoch = (execution_time - epoch).total_seconds()
            interval_seconds = config.interval.total_seconds()
            window_number = int(seconds_since_epoch // interval_seconds)
            return str(window_number)
        else:
            # Cron-based: use timestamp rounded to minute for deduplication
            # This ensures same cron schedule time maps to same window key
            timestamp = int(execution_time.timestamp() // 60)  # Round to minute
            return str(timestamp)
    
    def _create_periodic_wrapper(self, config: PeriodicJobConfig):
        """Create a wrapper function that handles periodic job execution and rescheduling"""
        @functools.wraps(config.func)
        async def periodic_wrapper():
            lock_acquired = False
            lock_key = None
            
            try:
                # If advisory lock is enabled, try to acquire lock
                if config.use_advisory_lock:
                    lock_key = self._get_advisory_lock_key(config)
                    lock_acquired = await self._try_acquire_advisory_lock(lock_key)
                    
                    if not lock_acquired:
                        logger.debug(f"Advisory lock for {config.job_name} already held by another worker, skipping execution")
                        return  # Skip execution if lock can't be acquired
                
                # Execute the original function
                if inspect.iscoroutinefunction(config.func):
                    await config.func()
                else:
                    # Handle sync functions
                    config.func()
                
                logger.debug(f"Periodic job {config.job_name} completed successfully")
                
            except Exception as e:
                logger.error(f"Periodic job {config.job_name} failed: {e}")
                raise  # Re-raise to let scheduler handle retries
            
            finally:
                # Release advisory lock if it was acquired
                if config.use_advisory_lock and lock_acquired and lock_key:
                    await self._release_advisory_lock(lock_key)
                
                # Always reschedule for next execution (self-rescheduling)
                if config.enabled:
                    await self._reschedule_periodic_job(config)
        
        # Set function name for scheduler registration
        periodic_wrapper.__name__ = f"periodic_{config.func.__name__}"
        return periodic_wrapper
    
    def _get_advisory_lock_key(self, config: PeriodicJobConfig) -> int:
        """Generate a numeric lock key for PostgreSQL advisory locks"""
        # PostgreSQL advisory locks use bigint (int8), so we need a numeric key
        # Hash the dedup_key to get a consistent numeric value
        hash_bytes = hashlib.sha256(config.dedup_key.encode()).digest()[:8]
        return struct.unpack('>q', hash_bytes)[0]  # Convert to signed 64-bit int
    
    async def _try_acquire_advisory_lock(self, lock_key: int) -> bool:
        """Try to acquire a PostgreSQL advisory lock (non-blocking)"""
        try:
            result = await self._scheduler.db_pool.fetchval(
                "SELECT pg_try_advisory_lock($1);", lock_key
            )
            return bool(result)
        except Exception as e:
            logger.error(f"Failed to acquire advisory lock {lock_key}: {e}")
            return False
    
    async def _release_advisory_lock(self, lock_key: int):
        """Release a PostgreSQL advisory lock"""
        try:
            await self._scheduler.db_pool.execute(
                "SELECT pg_advisory_unlock($1);", lock_key
            )
        except Exception as e:
            logger.error(f"Failed to release advisory lock {lock_key}: {e}")
    
    async def _reschedule_periodic_job(self, config: PeriodicJobConfig):
        """Reschedule the periodic job for the next execution"""
        # Import here to avoid circular import
        from .conflict_resolution import ConflictResolution
        
        try:
            next_run = self._calculate_next_run(config)
            window_key = self._get_window_key(next_run, config)
            job_id = f"periodic:{config.dedup_key}:{window_key}"
            
            await self._scheduler.schedule(
                self._create_periodic_wrapper(config),
                execution_time=next_run,
                job_id=job_id,
                conflict_resolution=ConflictResolution.IGNORE,
                priority=config.priority,
                max_retries=config.max_retries
            )
            logger.debug(f"Rescheduled periodic job {config.job_name} for {next_run}")
            
        except Exception as e:
            logger.error(f"Failed to reschedule periodic job {config.job_name}: {e}")


# Global registry instance
_periodic_registry = PeriodicJobRegistry()


def periodic(every: Optional[timedelta] = None,
            cron: Optional[str] = None,
            timezone: Optional[Union[str, ZoneInfo]] = None,
            use_advisory_lock: bool = False,
            priority: JobPriority = JobPriority.NORMAL,
            max_retries: int = 0,
            job_name: Optional[str] = None,
            dedup_key: Optional[str] = None,
            enabled: bool = True) -> Callable:
    """
    Decorator to mark an async function as a periodic job.
    
    Supports two scheduling modes:
    1. **Interval-based**: Use `every` parameter (e.g., `every=timedelta(minutes=15)`)
    2. **Cron-based**: Use `cron` parameter (e.g., `cron="0 0 * * *"` for daily at midnight)
    
    Features:
    - Guarantees exactly one enqueue per window across many replicas (via dedup key)
    - Self-reschedules at the end of each run
    - Optional advisory-lock protection for exclusive execution
    - Timezone support for cron expressions
    
    Args:
        every: Time interval between executions (timedelta). Mutually exclusive with `cron`.
        cron: Cron expression string (e.g., "0 0 * * *"). Mutually exclusive with `every`.
              Format: minute hour day month day_of_week
              Examples:
                - "0 0 * * *" = daily at midnight
                - "0 0 * * SUN" = every Sunday at midnight
                - "*/15 * * * *" = every 15 minutes
                - "0 9-17 * * MON-FRI" = every hour 9am-5pm on weekdays
        timezone: Timezone for cron scheduling (e.g., "America/New_York", "Europe/London").
                  Can be a string or ZoneInfo object. Only valid with `cron`.
                  Defaults to UTC if not specified.
        use_advisory_lock: Use PostgreSQL advisory locks for exclusive execution across replicas
        priority: Job priority (CRITICAL, HIGH, NORMAL, LOW)
        max_retries: Maximum retry attempts for failed executions
        job_name: Custom job name (auto-generated from function name if None)
        dedup_key: Custom deduplication key (auto-generated if None)
        enabled: Whether the periodic job is enabled
    
    Examples:
        # Interval-based scheduling
        @periodic(every=timedelta(minutes=15))
        async def cleanup_temp_files():
            print("Cleaning up temp files...")
        
        # Cron-based scheduling (daily at midnight UTC)
        @periodic(cron="0 0 * * *")
        async def daily_backup():
            print("Running daily backup...")
        
        # Cron with timezone (every Sunday at 3am EST)
        @periodic(cron="0 3 * * SUN", timezone="America/New_York")
        async def weekly_report():
            print("Generating weekly report...")
        
        # Cron with priority and retries
        @periodic(cron="0 9 * * MON-FRI", timezone="Europe/London", 
                 priority=JobPriority.HIGH, max_retries=3)
        async def business_hours_task():
            print("Running business hours task...")
    
    Raises:
        ValueError: If neither `every` nor `cron` is specified, or both are specified
        ValueError: If `timezone` is specified without `cron`
        ImportError: If `cron` is specified but croniter is not installed
        TypeError: If decorated function is not async
    """
    def decorator(func: Callable) -> Callable:
        # Validate function is async
        if not inspect.iscoroutinefunction(func):
            raise TypeError(f"@periodic can only be applied to async functions, got {type(func)}")
        
        # Create periodic job configuration
        config = PeriodicJobConfig(
            func=func,
            interval=every,
            cron=cron,
            timezone=timezone,
            use_advisory_lock=use_advisory_lock,
            priority=priority,
            max_retries=max_retries,
            job_name=job_name,
            dedup_key=dedup_key,
            enabled=enabled
        )
        
        # Register with global registry
        _periodic_registry.register(config)
        
        # Return the original function (it's still callable directly)
        return func
    
    return decorator

