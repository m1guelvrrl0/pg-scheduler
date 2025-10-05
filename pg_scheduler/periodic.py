"""
Periodic job functionality for PG Scheduler.

This module provides the @periodic decorator and related infrastructure for
scheduling recurring jobs with cross-replica deduplication and optional
advisory lock support.
"""

import asyncio
import datetime
import functools
import hashlib
import inspect
import logging
import struct
from dataclasses import dataclass
from datetime import UTC, timedelta
from typing import Callable, Dict, Optional, TYPE_CHECKING

from .job_priority import JobPriority

if TYPE_CHECKING:
    from .scheduler import Scheduler
    from .conflict_resolution import ConflictResolution

logger = logging.getLogger(__name__)


@dataclass
class PeriodicJobConfig:
    """Configuration for a periodic job"""
    func: Callable
    interval: timedelta
    use_advisory_lock: bool = False  # Unadvisable unless you need Master node pattern where only a specific instance can run this for whatever reason.
    priority: 'JobPriority' = None  # Will default to JobPriority.NORMAL
    max_retries: int = 0
    job_name: Optional[str] = None  # Auto-generated from function name if None
    dedup_key: Optional[str] = None  # Auto-generated if None
    enabled: bool = True
    
    def __post_init__(self):
        """Set defaults and generate dedup key"""
        if self.priority is None:
            self.priority = JobPriority.NORMAL
        if self.job_name is None:
            self.job_name = f"periodic_{self.func.__name__}"
        if self.dedup_key is None:
            # Generate deterministic dedup key based on function and interval
            func_signature = f"{self.func.__module__}.{self.func.__name__}"
            interval_str = f"{self.interval.total_seconds()}"
            key_material = f"{func_signature}:{interval_str}"
            self.dedup_key = hashlib.sha256(key_material.encode()).hexdigest()[:16]


class PeriodicJobRegistry:
    """Registry for periodic jobs"""
    def __init__(self):
        self._periodic_jobs: Dict[str, PeriodicJobConfig] = {}
        self._scheduler: Optional['Scheduler'] = None
    
    def register(self, config: PeriodicJobConfig):
        """Register a periodic job"""
        self._periodic_jobs[config.dedup_key] = config
        logger.info(f"Registered periodic job: {config.job_name} (every {config.interval}, dedup_key={config.dedup_key})")
    
    def set_scheduler(self, scheduler: 'Scheduler'):
        """Set the scheduler instance"""
        self._scheduler = scheduler
    
    def get_jobs(self) -> Dict[str, PeriodicJobConfig]:
        """Get all registered periodic jobs"""
        return self._periodic_jobs.copy()
    
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
        next_run = datetime.datetime.now(UTC) + config.interval
        
        # Create dedup job ID for this window
        window_key = self._get_window_key(next_run, config.interval)
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
            logger.info(f"Scheduled periodic job {config.job_name} for {next_run}")
        except Exception as e:
            logger.error(f"Failed to schedule periodic job {config.job_name}: {e}")
    
    def _get_window_key(self, execution_time: datetime.datetime, interval: timedelta) -> str:
        """Generate a window key for deduplication within time windows"""
        # Round down to the nearest interval boundary
        epoch = datetime.datetime(1970, 1, 1, tzinfo=UTC)
        seconds_since_epoch = (execution_time - epoch).total_seconds()
        interval_seconds = interval.total_seconds()
        window_number = int(seconds_since_epoch // interval_seconds)
        return str(window_number)
    
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
                        logger.info(f"Advisory lock for {config.job_name} already held by another worker, skipping execution")
                        return  # Skip execution if lock can't be acquired
                
                # Execute the original function
                if inspect.iscoroutinefunction(config.func):
                    await config.func()
                else:
                    # Handle sync functions
                    config.func()
                
                logger.info(f"Periodic job {config.job_name} completed successfully")
                
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
            next_run = datetime.datetime.now(UTC) + config.interval
            window_key = self._get_window_key(next_run, config.interval)
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


def periodic(every: timedelta, 
            use_advisory_lock: bool = False,
            priority: JobPriority = JobPriority.NORMAL,
            max_retries: int = 0,
            job_name: Optional[str] = None,
            dedup_key: Optional[str] = None,
            enabled: bool = True) -> Callable:
    """
    Decorator to mark an async function as a periodic job.
    
    Features:
    - Guarantees exactly one enqueue per window across many replicas (via dedup key)
    - Self-reschedules at the end of each run
    - Optional advisory-lock protection (alternative to dedup)
    
    Args:
        every: Time interval between executions (timedelta)
        use_advisory_lock: Use PostgreSQL advisory locks instead of dedup (future feature)
        priority: Job priority (JobPriority.NORMAL or JobPriority.CRITICAL)
        max_retries: Maximum retry attempts for failed executions
        job_name: Custom job name (auto-generated from function name if None)
        dedup_key: Custom deduplication key (auto-generated if None)
        enabled: Whether the periodic job is enabled
    
    Example:
        @periodic(every=timedelta(minutes=15))
        async def cleanup_temp_files():
            # Your periodic task code here
            pass
            
        @periodic(every=timedelta(hours=1), priority=JobPriority.CRITICAL, max_retries=3)
        async def generate_daily_report():
            # Critical periodic task with retries
            pass
    """
    def decorator(func: Callable) -> Callable:
        # Validate function is async
        if not inspect.iscoroutinefunction(func):
            raise TypeError(f"@periodic can only be applied to async functions, got {type(func)}")
        
        # Create periodic job configuration
        config = PeriodicJobConfig(
            func=func,
            interval=every,
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

