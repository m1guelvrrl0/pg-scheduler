import asyncio
import datetime
import json
import asyncpg
import logging
import signal
import sys
import inspect
from datetime import UTC, timedelta
from typing import Optional, Set, Dict, Callable, Any
from enum import Enum
from dataclasses import dataclass
import uuid
import random
import hashlib
import functools
import struct

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class JobPriority(Enum):
    # TODO: Implement enums at DB level, maybe more priority levels?
    """User-friendly job priority levels"""
    NORMAL = "normal"      # Default priority (value: 5)
    CRITICAL = "critical"  # High priority (value: 1) - lower numbers = higher priority
    
    @property
    def db_value(self) -> int:
        """Convert enum to database integer value (lower = higher priority)"""
        return {"normal": 5, "critical": 1}[self.value]
    
    @classmethod
    def from_db_value(cls, db_value: int) -> 'JobPriority':
        """Convert database integer back to enum"""
        mapping = {5: cls.NORMAL, 1: cls.CRITICAL}
        return mapping.get(db_value, cls.NORMAL)

class ConflictResolution(Enum):
    """Strategies for handling duplicate job_id conflicts"""
    RAISE = "raise"        # Raise ValueError (default, safest)
    IGNORE = "ignore"      # Ignore the new job, return existing job_id  
    REPLACE = "replace"    # Replace/update the existing job with new parameters

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

@dataclass
class PeriodicJobConfig:
    """Configuration for a periodic job"""
    func: Callable
    interval: timedelta
    use_advisory_lock: bool = False # Unadvisable unless you need Master node pattern where only a specific instance can run this for whatever reason.
    priority: 'JobPriority' = None  # Will default to JobPriority.NORMAL
    max_retries: int = 0
    job_name: Optional[str] = None  # Auto-generated from function name if None
    dedup_key: Optional[str] = None  # Auto-generated if None
    enabled: bool = True
    
    def __post_init__(self):
        """Set defaults and generate dedup key"""
        if self.priority is None:
            # Import here to avoid circular import
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

class Scheduler:
    HEARTBEAT_THRESHOLD = 120  # 2 minutes (in seconds)
    LEASE_DURATION = 60        # 1 minute lease duration (in seconds)
    WORKER_ID_LENGTH = 8       # For worker identification

    # TODO: Add more configuration options
    def __init__(self, 
                 db_pool: asyncpg.Pool, 
                 max_concurrent_jobs: int = 25, 
                 misfire_grace_time: int = 300,  # 5 minutes default
                 vacuum_config: Optional[VacuumConfig] = None,
                 vacuum_enabled: bool = True):
        """
        Initialize the Scheduler with concurrency control and reliability features.
        
        Args:
            db_pool: Connection to the PostgreSQL database.
            max_concurrent_jobs: Maximum number of jobs to run concurrently
            misfire_grace_time: Seconds after execution_time before jobs expire (like APScheduler)
            vacuum_config: Configuration for job cleanup policies (uses defaults if None)
            vacuum_enabled: Whether to enable automatic vacuum cleanup
        """
        self.db_pool = db_pool
        self.task_map = {}  # Store task functions
        self.is_running = False
        self.is_shutting_down = False
        
        # Generate unique worker ID for this instance
        self.worker_id = str(uuid.uuid4())[:self.WORKER_ID_LENGTH]
        
        # Concurrency control with tracking
        self.job_semaphore = asyncio.Semaphore(max_concurrent_jobs)
        self.active_jobs: Set[str] = set()  # Track active job IDs
        self.active_tasks: Set[asyncio.Task] = set()  # Track active asyncio tasks
        
        # Job expiration policy
        self.misfire_grace_time = misfire_grace_time
        
        # Vacuum configuration
        self.vacuum_enabled = vacuum_enabled
        self.vacuum_config = vacuum_config or VacuumConfig()
        
        # Background tasks
        self.heartbeat_monitor_task = None
        self.listener_task = None
        self.orphan_recovery_task = None
        self.vacuum_task = None
        
        # Periodic jobs
        self.periodic_jobs_enabled = True
        _periodic_registry.set_scheduler(self)
        
        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()
        self.shutdown_task = None
        
        logger.info(f"Scheduler initialized: worker_id={self.worker_id}, "
                   f"max_concurrent={max_concurrent_jobs}, misfire_grace={misfire_grace_time}s")

    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        if sys.platform != 'win32':
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.shutdown_task = asyncio.create_task(self.shutdown())

    async def start(self):
        """Start the scheduler with reliability features"""
        if self.is_running:
            return
        
        self.is_running = True
        self.is_shutting_down = False
        
        try:
            # Initialize database with worker tracking
            await self.initialize_db()
            await self.load_task_functions()
            
            # RELIABILITY: Recover orphaned jobs from previous crashes
            await self.recover_orphaned_jobs()
            
            # Start background tasks
            self.heartbeat_monitor_task = asyncio.create_task(self.monitor_heartbeats())
            self.orphan_recovery_task = asyncio.create_task(self.periodic_orphan_recovery())
            self.listener_task = asyncio.create_task(self.listen_for_jobs())
            
            # Start vacuum task if enabled
            if self.vacuum_enabled:
                self.vacuum_task = asyncio.create_task(self._vacuum_loop())
                logger.info(f"Scheduler started: worker_id={self.worker_id}, vacuum_enabled=True")
            else:
                logger.info(f"Scheduler started: worker_id={self.worker_id}, vacuum_enabled=False")
            
            # Start periodic jobs if enabled
            if self.periodic_jobs_enabled:
                await _periodic_registry.start_all_jobs()
                periodic_count = len(_periodic_registry.get_jobs())
                logger.info(f"Started {periodic_count} periodic jobs")
            
        except Exception as e:
            logger.error(f"Error starting scheduler: {e}")
            self.is_running = False
            await self._cleanup_background_tasks()
            raise

    async def shutdown(self):
        """Shutdown the scheduler gracefully with job completion"""
        if not self.is_running or self.is_shutting_down:
            return

        logger.info(f"Gracefully stopping scheduler {self.worker_id}...")
        self.is_shutting_down = True
        self.is_running = False
        
        # Wait for active jobs to complete (with timeout)
        if self.active_jobs:
            logger.info(f"Waiting for {len(self.active_jobs)} active jobs to complete...")
            timeout = 30  # 30 second timeout
            start_time = asyncio.get_event_loop().time()
            
            while self.active_jobs and (asyncio.get_event_loop().time() - start_time) < timeout:
                await asyncio.sleep(1)
            
            if self.active_jobs:
                logger.warning(f"Timed out waiting for jobs {self.active_jobs} to complete")
        
        # Wait for any remaining active tasks to complete or cancel them
        if self.active_tasks:
            logger.info(f"Waiting for {len(self.active_tasks)} active tasks to complete...")
            await asyncio.wait(self.active_tasks, timeout=10, return_when=asyncio.ALL_COMPLETED)
            
            # Cancel any tasks that didn't complete
            remaining_tasks = [task for task in self.active_tasks if not task.done()]
            if remaining_tasks:
                logger.warning(f"Cancelling {len(remaining_tasks)} remaining tasks")
                for task in remaining_tasks:
                    task.cancel()
                # Wait for cancellation to complete
                await asyncio.gather(*remaining_tasks, return_exceptions=True)
        
        # Clean up background tasks
        await self._cleanup_background_tasks()
        
        # Mark any remaining jobs as failed (they'll be retried by other workers)
        await self._mark_remaining_jobs_failed()
        
        logger.info(f"Scheduler {self.worker_id} stopped gracefully")

    async def _cleanup_background_tasks(self):
        """Clean up all background tasks"""
        tasks = [self.heartbeat_monitor_task, self.listener_task, self.orphan_recovery_task, self.vacuum_task]
        for task in tasks:
            if task and not task.done():
                task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(*[t for t in tasks if t], return_exceptions=True)

    async def _mark_remaining_jobs_failed(self):
        """Mark any jobs still tracked as active as failed for retry by other workers"""
        if not self.active_jobs:
            return
            
        try:
            await self._execute_with_retry("""
                UPDATE scheduled_jobs 
                SET status = 'pending', 
                    last_heartbeat = NULL,
                    lease_until = NULL,
                    worker_id = NULL
                WHERE job_id = ANY($1) AND status = 'running';
            """, list(self.active_jobs))
            
            logger.info(f"Marked {len(self.active_jobs)} jobs for retry by other workers")
            
        except Exception as e:
            logger.error(f"Failed to mark remaining jobs as failed: {e}")

    async def initialize_db(self):
        """Initialize database with worker tracking and reliability features"""
        await self._execute_with_retry("""
            CREATE TABLE IF NOT EXISTS scheduled_jobs (
                job_id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
                job_name TEXT NOT NULL,
                execution_time TIMESTAMPTZ NOT NULL,
                status TEXT DEFAULT 'pending',
                task_data JSONB,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                last_heartbeat TIMESTAMPTZ,
                lease_until TIMESTAMPTZ,  -- Explicit lease expiration
                priority INTEGER DEFAULT 5,
                retry_count INTEGER DEFAULT 0,
                max_retries INTEGER DEFAULT 0,
                worker_id TEXT,  -- Track which worker is processing
                error_message TEXT -- Track last error for debugging
            );
        """)
        
        # Create indexes for reliability and performance
        await self._execute_with_retry("""
            CREATE INDEX IF NOT EXISTS idx_jobs_pending_priority 
            ON scheduled_jobs(status, priority ASC, execution_time ASC)
            WHERE status = 'pending';
        """)
        
        await self._execute_with_retry("""
            CREATE INDEX IF NOT EXISTS idx_jobs_lease_expiration
            ON scheduled_jobs(status, lease_until, execution_time)
            WHERE status = 'pending';
        """)
        
        await self._execute_with_retry("""
            CREATE INDEX IF NOT EXISTS idx_jobs_running_heartbeat 
            ON scheduled_jobs(status, last_heartbeat, worker_id)
            WHERE status = 'running';
        """)
        
        await self._execute_with_retry("""
            CREATE INDEX IF NOT EXISTS idx_jobs_worker_cleanup
            ON scheduled_jobs(worker_id, status)
            WHERE worker_id IS NOT NULL;
        """)
        
        # Create vacuum statistics table if metrics tracking is enabled
        if self.vacuum_config.track_metrics:
            await self._execute_with_retry("""
                CREATE TABLE IF NOT EXISTS vacuum_stats (
                    stat_date DATE PRIMARY KEY DEFAULT CURRENT_DATE,
                    deleted_completed INTEGER DEFAULT 0,
                    deleted_failed INTEGER DEFAULT 0,
                    deleted_cancelled INTEGER DEFAULT 0,
                    last_run TIMESTAMPTZ,
                    worker_id TEXT  -- Track which worker performed the vacuum
                );
            """)
            logger.info("Database initialized with reliability features and vacuum metrics")
        else:
            logger.info("Database initialized with reliability features")

    async def _execute_with_retry(self, query: str, *args, max_retries: int = 3):
        """Execute database query with retry logic for transient failures"""
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                if args:
                    return await self.db_pool.fetch(query, *args)
                else:
                    return await self.db_pool.fetch(query)
                    
            except Exception as e:
                last_exception = e
                if attempt < max_retries - 1:
                    # Exponential backoff for retries
                    delay = (2 ** attempt) * 0.1  # 0.1s, 0.2s, 0.4s
                    logger.warning(f"Database operation failed (attempt {attempt + 1}/{max_retries}), "
                                 f"retrying in {delay}s: {e}")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Database operation failed permanently after {max_retries} attempts: {e}")
        
        raise last_exception

    async def recover_orphaned_jobs(self):
        """Recover jobs that were running when workers crashed"""
        try:
            # Find jobs that were "running" but have stale heartbeats or dead workers
            current_time = datetime.datetime.now(UTC)
            stale_threshold = current_time - datetime.timedelta(seconds=self.HEARTBEAT_THRESHOLD)
            
            recovered_jobs = await self._execute_with_retry("""
                UPDATE scheduled_jobs 
                SET status = 'pending', 
                    worker_id = NULL,
                    last_heartbeat = NULL,
                    lease_until = NULL
                WHERE status = 'running' 
                AND (last_heartbeat < $1 OR last_heartbeat IS NULL)
                RETURNING job_id, job_name, worker_id;
            """, stale_threshold)
            
            if recovered_jobs:
                logger.warning(f"Recovered {len(recovered_jobs)} orphaned jobs from crashed workers")
                for job in recovered_jobs:
                    logger.debug(f"Recovered job {job['job_id']} ({job['job_name']}) "
                               f"from worker {job['worker_id']}")
            else:
                logger.info("No orphaned jobs found during startup")
                
        except Exception as e:
            logger.error(f"Failed to recover orphaned jobs: {e}")
            # Don't raise - continue with scheduler startup

    async def periodic_orphan_recovery(self):
        """Periodically check for and recover orphaned jobs"""
        while self.is_running:
            try:
                await asyncio.sleep(300)  # Check every 5 minutes
                if not self.is_running:
                    break
                    
                await self.recover_orphaned_jobs()
                
            except Exception as e:
                logger.error(f"Error in periodic orphan recovery: {e}")
                await asyncio.sleep(60)  # Back off on errors

    async def schedule(
        self, 
        func, 
        *, 
        execution_time: datetime.datetime,
        args: tuple = (),
        kwargs: dict = None,
        priority: JobPriority = JobPriority.NORMAL,
        max_retries: int = 0,
        job_id: Optional[str] = None,
        conflict_resolution: ConflictResolution = ConflictResolution.RAISE
    ) -> str:
        """
        Schedule an async I/O function to run at a specific time.
        
        Args:
            func: The async function to schedule (must be async I/O only)
            execution_time: datetime object specifying when to run the job
            args: Tuple of positional arguments to pass to the function
            kwargs: Dictionary of keyword arguments to pass to the function
            priority: Job priority using JobPriority enum (NORMAL or CRITICAL)
            max_retries: Maximum retry attempts for failed jobs
            job_id: Optional custom job ID (auto-generated if not provided)
            conflict_resolution: How to handle duplicate job_id (RAISE, IGNORE, REPLACE)
            
        Returns:
            str: The job ID of the scheduled job
        """
        if not inspect.iscoroutinefunction(func):
            raise TypeError(f"Expected an async function, got {type(func)}")
        
        # Store the function in our task map
        self.task_map[func.__name__] = func
        
        # Package the arguments into task_data
        task_data = {
            'args': args,
            'kwargs': kwargs or {}
        }
        
        # Schedule the job in the database
        scheduled_job_id = await self.schedule_job(
            func.__name__, 
            execution_time, 
            task_data, 
            priority=priority,
            max_retries=max_retries,
            job_id=job_id,
            conflict_resolution=conflict_resolution
        )
        
        logger.info(f"Scheduled async function {func.__name__} to run at {execution_time} (priority={priority.value}, job_id={scheduled_job_id})")
        return scheduled_job_id

    async def listen_for_jobs(self):
        """Listen for jobs with reliability and optimized bulk claiming"""
        
        startup_jitter = random.uniform(0, 1.0)
        await asyncio.sleep(startup_jitter)
        
        logger.info(f"Starting job listener [worker={self.worker_id}]")
        
        while self.is_running and not self.is_shutting_down:
            try:
                # Check available semaphore slots
                available_slots = self.job_semaphore._value
                if available_slots <= 0:
                    await asyncio.sleep(1.0)
                    continue
                
                max_claimable = min(available_slots, 5)  # Cap at 5 for reasonable batch size
                
                # Claim jobs from database
                ready_jobs = await self._claim_jobs(max_claimable)
                
                if ready_jobs:
                    logger.info(f"Claimed {len(ready_jobs)} jobs [worker={self.worker_id}] (bulk claiming)")
                    
                    # The semaphore will naturally limit concurrency
                    for job_row in ready_jobs:
                        if not self.is_running:
                            break
                        task = asyncio.create_task(self.execute_job_with_concurrency_control(job_row))
                        self.active_tasks.add(task)
                        task.add_done_callback(self.active_tasks.discard)
                
                # Adaptive sleep
                # TODO: Maybe find a smarter way to do this ie configurableexponential backoff strategy or something
                if ready_jobs:
                    jitter = random.uniform(-0.05, 0.05)
                    await asyncio.sleep(0.05 + jitter)
                else:
                    jitter = random.uniform(-0.2, 0.2)
                    await asyncio.sleep(2.0 + jitter)
                    
            except Exception as e:
                logger.error(f"Error in job listener: {e}")
                await asyncio.sleep(5)

    async def _claim_jobs(self, num_slots: int):
        """Claim jobs from database"""
        try:
            # Use a transaction for atomic job claiming
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    # Calculate expiration cutoff for misfire grace time
                    current_time = datetime.datetime.now(UTC)
                    expiration_cutoff = current_time - datetime.timedelta(seconds=self.misfire_grace_time)
                    
                    # Expire old jobs that are past the misfire grace time
                    expired_jobs = await conn.fetch("""
                        UPDATE scheduled_jobs 
                        SET status = 'expired', worker_id = $1
                        WHERE status = 'pending' 
                        AND execution_time < $2
                        RETURNING job_id, job_name, execution_time;
                    """, self.worker_id, expiration_cutoff)
                    
                    if expired_jobs:
                        logger.warning(f"Expired {len(expired_jobs)} jobs past grace time")
                    
                    # Claim ready jobs atomically using CTE pattern (limited by available semaphore slots)
                    ready_jobs = await conn.fetch("""
                        WITH to_claim AS (
                            SELECT job_id
                            FROM scheduled_jobs
                            WHERE status = 'pending'
                            AND execution_time <= NOW()  -- ready to execute
                            AND (lease_until IS NULL OR lease_until < NOW())  -- not currently leased
                            ORDER BY priority ASC, execution_time ASC, job_id ASC
                            FOR UPDATE SKIP LOCKED
                            LIMIT $1
                        )
                        UPDATE scheduled_jobs j
                        SET status = 'running',
                            last_heartbeat = NOW(),
                            lease_until = NOW() + INTERVAL '60 seconds',
                            worker_id = $2
                        FROM to_claim c
                        WHERE j.job_id = c.job_id
                        RETURNING j.job_id, j.job_name, j.execution_time, j.task_data::text,
                                  j.priority, j.retry_count, j.max_retries;
                    """, num_slots, self.worker_id)
                    
                    # Track claimed jobs
                    for job in ready_jobs:
                        self.active_jobs.add(job['job_id'])
                    
                    return ready_jobs
                    
        except Exception as e:
            logger.error(f"Error claiming jobs with slots: {e}")
            return []

    async def execute_job_with_concurrency_control(self, job_row):
        """Execute job with semaphore control"""
        job_id = job_row['job_id']
        
        # Ensure semaphore is always released
        try:
            async with self.job_semaphore:
                await self.execute_job(job_row)
        except Exception as e:
            logger.error(f"Critical error in job {job_id} execution: {e}")
            # Ensure job is properly marked as failed
            await self._safe_mark_job_failed(job_id, str(e))
        finally:
            # Always remove from active jobs tracking
            self.active_jobs.discard(job_id)



    async def execute_job(self, job_row):
        """Execute job with comprehensive error handling and state management"""
        job_id = job_row['job_id']
        job_name = job_row['job_name']
        priority = JobPriority.from_db_value(job_row.get('priority', 5))
        
        # Start heartbeat task
        heartbeat_task = asyncio.create_task(self.send_heartbeat(job_id))
        
        try:
            # Validate job data
            task_data = json.loads(job_row['task_data'])
            task_func = self.task_map.get(job_name)
            
            # Handle missing function gracefully
            if task_func is None:
                error_msg = f"No task function found for job {job_name}"
                await self._safe_mark_job_failed(job_id, error_msg)
                return
            
            logger.info(f"Executing job {job_id} ({job_name}) [priority={priority.value}] [worker={self.worker_id}]")
            
            # Execute the function with timeout protection
            args = task_data.get('args', ())
            kwargs = task_data.get('kwargs', {})
            
            # Add timeout to prevent runaway jobs
            try:
                await asyncio.wait_for(
                    task_func(*args, **kwargs),
                    timeout=3600  # 1 hour max per job
                )
            except asyncio.TimeoutError:
                raise Exception("Job execution timed out after 1 hour")
            
            # Atomically mark as completed
            success = await self._safe_mark_job_completed(job_id)
            if success:
                logger.info(f"Job {job_id} completed successfully [worker={self.worker_id}]")
            else:
                logger.warning(f"Job {job_id} completed but failed to update status - may be retried")
            
        except Exception as e:
            # Handle job failure with retry logic
            await self._handle_job_failure(job_row, str(e))
            
        finally:
            # Always stop heartbeat task
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

    async def _handle_job_failure(self, job_row, error_message):
        """Handle job failure with proper retry logic"""
        job_id = job_row['job_id']
        retry_count = job_row.get('retry_count', 0)
        max_retries = job_row.get('max_retries', 0)
        
        if retry_count < max_retries:
            # Schedule retry with exponential backoff
            retry_delay = min(2 ** retry_count * 60, 300)  # Max 5 minutes
            retry_time = datetime.datetime.now(UTC) + datetime.timedelta(seconds=retry_delay)
            
            success = await self._safe_schedule_retry(job_id, retry_count + 1, retry_time, error_message)
            
            if success:
                logger.warning(f"Job {job_id} failed (attempt {retry_count + 1}/{max_retries}), "
                             f"retrying at {retry_time}: {error_message}")
            else:
                logger.error(f"Job {job_id} failed and couldn't schedule retry: {error_message}")
        else:
            # Exhausted retries - mark as permanently failed
            await self._safe_mark_job_failed(job_id, error_message)
            logger.error(f"Job {job_id} permanently failed after {max_retries} attempts: {error_message}")

    async def _safe_mark_job_completed(self, job_id: int) -> bool:
        """Safely mark job as completed with verification"""
        try:
            result = await self._execute_with_retry("""
                UPDATE scheduled_jobs 
                SET status = 'completed', 
                    last_heartbeat = NOW(),
                    error_message = NULL
                WHERE job_id = $1 AND status = 'running' AND worker_id = $2
                RETURNING job_id;
            """, job_id, self.worker_id)
            
            return len(result) > 0  # True if update succeeded
            
        except Exception as e:
            logger.error(f"Failed to mark job {job_id} as completed: {e}")
            return False

    async def _safe_mark_job_failed(self, job_id: int, error_message: str):
        """Safely mark job as failed"""
        try:
            await self._execute_with_retry("""
                UPDATE scheduled_jobs 
                SET status = 'failed',
                    error_message = $2,
                    last_heartbeat = NOW()
                WHERE job_id = $1 AND worker_id = $3;
            """, job_id, error_message[:1000], self.worker_id)  # Limit error message length
            
        except Exception as e:
            logger.error(f"Failed to mark job {job_id} as failed: {e}")

    async def _safe_schedule_retry(self, job_id: int, retry_count: int, retry_time: datetime.datetime, error_message: str) -> bool:
        """Safely schedule job retry"""
        try:
            result = await self._execute_with_retry("""
                UPDATE scheduled_jobs 
                SET status = 'pending',
                    retry_count = $2,
                    execution_time = $3,
                    error_message = $4,
                    worker_id = NULL,
                    last_heartbeat = NULL,
                    lease_until = NULL
                WHERE job_id = $1 AND worker_id = $5
                RETURNING job_id;
            """, job_id, retry_count, retry_time, error_message[:1000], self.worker_id)
            
            return len(result) > 0
            
        except Exception as e:
            logger.error(f"Failed to schedule retry for job {job_id}: {e}")
            return False

    async def load_task_functions(self):
        """Load task functions with error handling"""
        try:
            jobs = await self._execute_with_retry("""
                SELECT DISTINCT job_name FROM scheduled_jobs WHERE status IN ('pending', 'running');
            """)
            
            for job in jobs:
                job_name = job['job_name']
                if job_name not in self.task_map:
                    logger.warning(f"No task function found for job: {job_name}")
        except Exception as e:
            logger.error(f"Error loading task functions: {e}")

    async def monitor_heartbeats(self):
        """Monitor heartbeats and lease expiration with enhanced reliability"""
        while self.is_running:
            try:
                current_time = datetime.datetime.now(UTC)
                heartbeat_threshold = current_time - datetime.timedelta(seconds=self.HEARTBEAT_THRESHOLD)
                
                # Check for both stale heartbeats AND expired leases
                failed_jobs = await self._execute_with_retry("""
                    UPDATE scheduled_jobs 
                    SET status = 'pending',
                        worker_id = NULL,
                        last_heartbeat = NULL,
                        lease_until = NULL
                    WHERE status = 'running' 
                    AND (last_heartbeat < $1 OR lease_until < NOW())
                    RETURNING job_id, job_name, worker_id, last_heartbeat, lease_until;
                """, heartbeat_threshold)
                
                for job in failed_jobs:
                    lease_expired = job['lease_until'] and job['lease_until'] < current_time
                    reason = "lease expired" if lease_expired else "stale heartbeat"
                    logger.error(f"Detected stale job {job['job_id']} from worker {job['worker_id']}, "
                               f"marking for retry ({reason}: heartbeat={job['last_heartbeat']}, lease_until={job['lease_until']})")
                
            except Exception as e:
                logger.error(f"Error monitoring heartbeats: {e}")
                
            await asyncio.sleep(60)

    async def send_heartbeat(self, job_id):
        """Send heartbeats with lease renewal"""
        while True:
            try:
                await self._execute_with_retry("""
                    UPDATE scheduled_jobs 
                    SET last_heartbeat = NOW(),
                        lease_until = NOW() + INTERVAL '60 seconds'
                    WHERE job_id = $1 AND status = 'running' AND worker_id = $2;
                """, job_id, self.worker_id)
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat failed for job {job_id}: {e}")
                await asyncio.sleep(30)

    async def _vacuum_loop(self):
        """Background vacuum task that periodically cleans up jobs based on policies"""
        while self.is_running and not self.is_shutting_down:
            try:
                await self._run_vacuum_policies()
                await asyncio.sleep(self.vacuum_config.interval_minutes * 60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Vacuum task error: {e}")
                await asyncio.sleep(60)  # Retry after error

    async def _run_vacuum_policies(self):
        """Execute vacuum policies"""
        # Safety window: Never vacuum jobs that are actively running or recently updated
        safety_window = datetime.datetime.now(UTC) - timedelta(minutes=5)
        
        total_deleted = 0
        for status, policy in [
            ('completed', self.vacuum_config.completed),
            ('failed', self.vacuum_config.failed), 
            ('cancelled', self.vacuum_config.cancelled)
        ]:
            if policy.trigger == VacuumTrigger.NEVER:
                continue
                
            deleted_count = await self._apply_vacuum_policy(status, policy, safety_window)
            total_deleted += deleted_count
            
            if deleted_count > 0:
                logger.info(f"Vacuum: deleted {deleted_count} {status} jobs")
        
        if total_deleted > 0:
            logger.info(f"Vacuum completed: deleted {total_deleted} total jobs")

    async def _apply_vacuum_policy(self, status: str, policy: VacuumPolicy, safety_window: datetime.datetime) -> int:
        """Apply a specific vacuum policy and return count of deleted jobs"""
        try:
            if policy.trigger == VacuumTrigger.IMMEDIATE:
                # Delete all jobs in this status (respecting safety window)
                # NULL heartbeat means job is not active, so it's safe to delete
                query = """
                    DELETE FROM scheduled_jobs 
                    WHERE status = $1 AND (last_heartbeat IS NULL OR last_heartbeat < $2)
                    RETURNING job_id;
                """
                deleted_jobs = await self._execute_with_retry(query, status, safety_window)
                
            elif policy.trigger == VacuumTrigger.TIME_BASED:
                # Delete jobs older than specified days
                cutoff_time = datetime.datetime.now(UTC) - timedelta(days=policy.days)
                query = """
                    DELETE FROM scheduled_jobs 
                    WHERE status = $1 AND created_at < $2 AND (last_heartbeat IS NULL OR last_heartbeat < $3)
                    RETURNING job_id;
                """
                deleted_jobs = await self._execute_with_retry(query, status, cutoff_time, safety_window)
                
            elif policy.trigger == VacuumTrigger.COUNT_BASED:
                # Keep only the last N jobs per job_name
                query = """
                    DELETE FROM scheduled_jobs 
                    WHERE job_id IN (
                        SELECT job_id FROM (
                            SELECT job_id, 
                                   ROW_NUMBER() OVER (PARTITION BY job_name ORDER BY created_at DESC) as rn
                            FROM scheduled_jobs 
                            WHERE status = $1 AND (last_heartbeat IS NULL OR last_heartbeat < $2)
                        ) ranked 
                        WHERE rn > $3
                    )
                    RETURNING job_id;
                """
                deleted_jobs = await self._execute_with_retry(query, status, safety_window, policy.keep_count)
                
            else:
                deleted_jobs = []
            
            deleted_count = len(deleted_jobs)
            
            # Record metrics if enabled
            if deleted_count > 0 and self.vacuum_config.track_metrics:
                await self._record_vacuum_metrics(status, deleted_count)
                
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error applying vacuum policy for {status} jobs: {e}")
            return 0

    async def _record_vacuum_metrics(self, status: str, deleted_count: int):
        """Record vacuum metrics in the database"""
        try:
            await self._execute_with_retry("""
                INSERT INTO vacuum_stats (stat_date, deleted_completed, deleted_failed, deleted_cancelled, last_run, worker_id)
                VALUES (CURRENT_DATE, 
                        CASE WHEN $1 = 'completed' THEN $2 ELSE 0 END,
                        CASE WHEN $1 = 'failed' THEN $2 ELSE 0 END,
                        CASE WHEN $1 = 'cancelled' THEN $2 ELSE 0 END,
                        NOW(), $3)
                ON CONFLICT (stat_date) 
                DO UPDATE SET 
                    deleted_completed = vacuum_stats.deleted_completed + EXCLUDED.deleted_completed,
                    deleted_failed = vacuum_stats.deleted_failed + EXCLUDED.deleted_failed,
                    deleted_cancelled = vacuum_stats.deleted_cancelled + EXCLUDED.deleted_cancelled,
                    last_run = EXCLUDED.last_run,
                    worker_id = EXCLUDED.worker_id;
            """, status, deleted_count, self.worker_id)
        except Exception as e:
            logger.error(f"Failed to record vacuum metrics: {e}")

    async def schedule_job(self, job_name, execution_time, task_data, priority: JobPriority = JobPriority.NORMAL, max_retries: int = 0, job_id: Optional[str] = None, conflict_resolution: ConflictResolution = ConflictResolution.RAISE) -> str:
        """
        Schedule a job by inserting it into the 'scheduled_jobs' table.
        
        Args:
            job_name (str): The name of the job to schedule.
            execution_time (datetime): The time at which the job should be executed.
            task_data (dict): Additional data required for the job execution.
            priority: Job priority using JobPriority enum (NORMAL or CRITICAL)
            max_retries: Maximum retry attempts for failed jobs
            job_id: Optional custom job ID (auto-generated if not provided)
            conflict_resolution: How to handle duplicate job_id (RAISE, IGNORE, REPLACE)
            
        Returns:
            str: The job ID of the scheduled job
            
        Raises:
            ValueError: If the provided job_id already exists and conflict_resolution is RAISE
        """
        json_task_data = json.dumps(task_data)
        
        if job_id is not None:
            # Check if job_id already exists first
            try:
                existing_job = await self._execute_with_retry("""
                    SELECT job_id, status FROM scheduled_jobs WHERE job_id = $1;
                """, job_id)
                
                if existing_job:
                    existing_status = existing_job[0]['status']
                    
                    if conflict_resolution == ConflictResolution.RAISE:
                        raise ValueError(
                            f"Job ID '{job_id}' already exists with status '{existing_status}'. "
                            f"Choose a different job_id or omit it for auto-generation."
                        )
                    elif conflict_resolution == ConflictResolution.IGNORE:
                        logger.info(f"Job ID '{job_id}' already exists, ignoring new job (conflict_resolution=IGNORE)")
                        return job_id
                    elif conflict_resolution == ConflictResolution.REPLACE:
                        # Replace/update the existing job
                        result = await self._execute_with_retry("""
                            UPDATE scheduled_jobs 
                            SET job_name = $2,
                                execution_time = $3,
                                task_data = $4::jsonb,
                                priority = $5,
                                max_retries = $6,
                                status = CASE 
                                    WHEN status IN ('completed', 'failed', 'cancelled', 'expired') THEN 'pending'
                                    ELSE status
                                END,
                                retry_count = 0,
                                error_message = NULL,
                                worker_id = NULL,
                                last_heartbeat = NULL,
                                lease_until = NULL
                            WHERE job_id = $1
                            RETURNING job_id;
                        """, job_id, job_name, execution_time, json_task_data, priority.db_value, max_retries)
                        
                        logger.info(f"Job ID '{job_id}' replaced with new parameters (conflict_resolution=REPLACE)")
                        return result[0]['job_id']
                
                # Insert with custom job_id (now we know it's unique)
                result = await self._execute_with_retry("""
                    INSERT INTO scheduled_jobs (job_id, job_name, execution_time, status, task_data, priority, max_retries)
                    VALUES ($1, $2, $3, 'pending', $4::jsonb, $5, $6)
                    RETURNING job_id;
                """, job_id, job_name, execution_time, json_task_data, priority.db_value, max_retries)
                
            except ValueError:
                # Re-raise our custom error
                raise
            except Exception as e:
                # Handle race condition: someone else inserted the same job_id between our check and insert
                if "duplicate key value violates unique constraint" in str(e):
                    if conflict_resolution == ConflictResolution.RAISE:
                        raise ValueError(
                            f"Job ID '{job_id}' was just created by another process. "
                            f"Choose a different job_id or omit it for auto-generation."
                        )
                    else:
                        # For IGNORE or REPLACE, try again (recursively call with same parameters)
                        logger.warning(f"Race condition detected for job_id '{job_id}', retrying with conflict_resolution={conflict_resolution.value}")
                        return await self.schedule_job(job_name, execution_time, task_data, priority, max_retries, job_id, conflict_resolution)
                else:
                    # Re-raise unexpected errors
                    raise
        else:
            # Let database auto-generate job_id
            result = await self._execute_with_retry("""
                INSERT INTO scheduled_jobs (job_name, execution_time, status, task_data, priority, max_retries)
                VALUES ($1, $2, 'pending', $3::jsonb, $4, $5)
                RETURNING job_id;
            """, job_name, execution_time, json_task_data, priority.db_value, max_retries)
        
        scheduled_job_id = result[0]['job_id']
        logger.info(f"Job scheduled: {job_name} at {execution_time} (priority={priority.value}, job_id={scheduled_job_id})")
        return scheduled_job_id

    async def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a scheduled job by setting its status to 'cancelled'.
        
        Args:
            job_id (str): The ID of the job to cancel
            
        Returns:
            bool: True if the job was successfully cancelled, False otherwise
        """
        try:
            result = await self._execute_with_retry("""
                UPDATE scheduled_jobs 
                SET status = 'cancelled', 
                    worker_id = NULL,
                    last_heartbeat = NULL,
                    lease_until = NULL
                WHERE job_id = $1 
                AND status IN ('pending', 'running')
                RETURNING job_id, job_name, status;
            """, job_id)
            
            if result:
                cancelled_job = result[0]
                logger.info(f"Job cancelled: {cancelled_job['job_name']} (job_id={job_id})")
                
                # Remove from active jobs if it was running
                self.active_jobs.discard(job_id)
                return True
            else:
                logger.warning(f"Job {job_id} could not be cancelled (may not exist or already completed)")
                return False
                
        except Exception as e:
            logger.error(f"Error cancelling job {job_id}: {e}")
            return False

    async def update_job_status(self, job_id, status):
        """
        Atomically update the status of a job.
        """
        await self.db_pool.execute("""
            UPDATE scheduled_jobs
            SET status = $1
            WHERE job_id = $2;
        """, status.value, job_id)

    # Vacuum API Methods
    
    async def run_vacuum(self) -> Dict[str, int]:
        """
        Manually trigger vacuum policies and return statistics.
        
        Returns:
            Dict with counts of deleted jobs by status
        """
        if not self.vacuum_enabled:
            logger.warning("Vacuum is disabled - no cleanup performed")
            return {"completed": 0, "failed": 0, "cancelled": 0}
            
        # Safety window for manual vacuum
        safety_window = datetime.datetime.now(UTC) - timedelta(minutes=5)
        
        results = {}
        for status, policy in [
            ('completed', self.vacuum_config.completed),
            ('failed', self.vacuum_config.failed), 
            ('cancelled', self.vacuum_config.cancelled)
        ]:
            if policy.trigger == VacuumTrigger.NEVER:
                results[status] = 0
                continue
                
            deleted_count = await self._apply_vacuum_policy(status, policy, safety_window)
            results[status] = deleted_count
            
            if deleted_count > 0:
                logger.info(f"Manual vacuum: deleted {deleted_count} {status} jobs")
        
        total = sum(results.values())
        if total > 0:
            logger.info(f"Manual vacuum completed: deleted {total} total jobs")
        
        return results

    async def get_vacuum_stats(self, days: int = 7) -> list:
        """
        Get vacuum statistics for the last N days (requires track_metrics=True).
        
        Args:
            days: Number of days to look back
            
        Returns:
            List of vacuum statistics records
        """
        if not self.vacuum_config.track_metrics:
            logger.warning("Vacuum metrics tracking is disabled - no stats available")
            return []
            
        try:
            return await self._execute_with_retry("""
                SELECT stat_date, 
                       deleted_completed, 
                       deleted_failed, 
                       deleted_cancelled,
                       last_run,
                       worker_id
                FROM vacuum_stats 
                WHERE stat_date >= CURRENT_DATE - make_interval(days => $1)
                ORDER BY stat_date DESC;
            """, days)
        except Exception as e:
            logger.error(f"Error fetching vacuum stats: {e}")
            return []

    async def get_total_vacuum_stats(self) -> dict:
        """
        Get aggregated vacuum statistics across all time and workers (requires track_metrics=True).
        
        Returns:
            Dict with total counts and last vacuum run time
        """
        if not self.vacuum_config.track_metrics:
            logger.warning("Vacuum metrics tracking is disabled - no stats available")
            return {}
            
        try:
            result = await self._execute_with_retry("""
                SELECT SUM(deleted_completed) as total_completed,
                       SUM(deleted_failed) as total_failed, 
                       SUM(deleted_cancelled) as total_cancelled,
                       MAX(last_run) as last_vacuum_run
                FROM vacuum_stats;
            """)
            return dict(result[0]) if result else {}
        except Exception as e:
            logger.error(f"Error fetching total vacuum stats: {e}")
            return {}

    # Periodic Job Management Methods
    
    def get_periodic_jobs(self) -> Dict[str, PeriodicJobConfig]:
        """Get all registered periodic jobs"""
        return _periodic_registry.get_jobs()
    
    def enable_periodic_job(self, dedup_key: str) -> bool:
        """Enable a specific periodic job"""
        jobs = _periodic_registry.get_jobs()
        if dedup_key in jobs:
            jobs[dedup_key].enabled = True
            logger.info(f"Enabled periodic job with dedup_key: {dedup_key}")
            return True
        return False
    
    def disable_periodic_job(self, dedup_key: str) -> bool:
        """Disable a specific periodic job"""
        jobs = _periodic_registry.get_jobs()
        if dedup_key in jobs:
            jobs[dedup_key].enabled = False
            logger.info(f"Disabled periodic job with dedup_key: {dedup_key}")
            return True
        return False
    
    async def trigger_periodic_job(self, dedup_key: str) -> Optional[str]:
        """Manually trigger a periodic job execution"""
        jobs = _periodic_registry.get_jobs()
        if dedup_key not in jobs:
            return None
        
        config = jobs[dedup_key]
        try:
            # Execute immediately with a unique job ID
            manual_job_id = f"manual_periodic:{dedup_key}:{uuid.uuid4().hex[:8]}"
            
            job_id = await self.schedule(
                config.func,
                execution_time=datetime.datetime.now(UTC),
                priority=config.priority,
                max_retries=config.max_retries,
                job_id=manual_job_id,
                conflict_resolution=ConflictResolution.REPLACE
            )
            
            logger.info(f"Manually triggered periodic job {config.job_name}: {job_id}")
            return job_id
            
        except Exception as e:
            logger.error(f"Failed to manually trigger periodic job {config.job_name}: {e}")
            return None
    
    def get_periodic_job_status(self, dedup_key: str) -> Optional[Dict[str, Any]]:
        """Get status information for a periodic job"""
        jobs = _periodic_registry.get_jobs()
        if dedup_key not in jobs:
            return None
        
        config = jobs[dedup_key]
        return {
            "job_name": config.job_name,
            "interval": config.interval.total_seconds(),
            "enabled": config.enabled,
            "priority": config.priority.value,
            "max_retries": config.max_retries,
            "dedup_key": config.dedup_key,
            "use_advisory_lock": config.use_advisory_lock,
            "function_name": config.func.__name__,
            "function_module": config.func.__module__
        }