import asyncio
import datetime
import json
import asyncpg
import logging
import signal
import sys
import inspect
from datetime import UTC, timedelta
from typing import Optional, Set, Dict, Any, Union, List
import uuid
import random

from .conflict_resolution import ConflictResolution
from .job_priority import JobPriority
from .job_spec import JobSpec, _UNSET as _JOBSPEC_UNSET
from .periodic import _periodic_registry, PeriodicJobConfig
from .vacuum import VacuumConfig, VacuumPolicy, VacuumTrigger

# Sentinel value to distinguish "not specified" from "explicitly None"
_UNSET = object()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Scheduler:
    HEARTBEAT_THRESHOLD = 120  # 2 minutes (in seconds)
    LEASE_DURATION = 60        # 1 minute lease duration (in seconds)
    WORKER_ID_LENGTH = 8       # For worker identification

    # TODO: Add more configuration options
    def __init__(self, 
                 db_pool: asyncpg.Pool, 
                 max_concurrent_jobs: int = 25, 
                 misfire_grace_time: Optional[int] = 300,  # 5 minutes default, None = no expiration
                 vacuum_config: Optional[VacuumConfig] = None,
                 vacuum_enabled: bool = True,
                 batch_claim_limit: int = 10):
        """
        Initialize the Scheduler with concurrency control and reliability features.
        
        Args:
            db_pool: Connection to the PostgreSQL database.
            max_concurrent_jobs: Maximum number of jobs to run concurrently
            misfire_grace_time: Default seconds after execution_time before jobs expire.
                              Set to None for no expiration. Can be overridden per-job.
            vacuum_config: Configuration for job cleanup policies (uses defaults if None)
            vacuum_enabled: Whether to enable automatic vacuum cleanup
            batch_claim_limit: Maximum number of jobs to claim in a single batch (default 10)
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
        
        # Batch claiming configuration
        self.batch_claim_limit = batch_claim_limit
        
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
                   f"max_concurrent={max_concurrent_jobs}, batch_claim_limit={batch_claim_limit}, "
                   f"misfire_grace={misfire_grace_time}s")

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
                logger.debug(f"Scheduler started: worker_id={self.worker_id}, vacuum_enabled=True")
            else:
                logger.debug(f"Scheduler started: worker_id={self.worker_id}, vacuum_enabled=False")
            
            # Start periodic jobs if enabled
            if self.periodic_jobs_enabled:
                await _periodic_registry.start_all_jobs()
                periodic_count = len(_periodic_registry.get_jobs())
                logger.debug(f"Started {periodic_count} periodic jobs")
            
        except Exception as e:
            logger.error(f"Error starting scheduler: {e}")
            self.is_running = False
            await self._cleanup_background_tasks()
            raise

    async def shutdown(self):
        """Shutdown the scheduler gracefully with job completion"""
        if not self.is_running or self.is_shutting_down:
            return

        logger.debug(f"Gracefully stopping scheduler {self.worker_id}...")
        self.is_shutting_down = True
        self.is_running = False
        
        # Wait for active jobs to complete (with timeout)
        if self.active_jobs:
            logger.debug(f"Waiting for {len(self.active_jobs)} active jobs to complete...")
            timeout = 30  # 30 second timeout
            start_time = asyncio.get_event_loop().time()
            
            while self.active_jobs and (asyncio.get_event_loop().time() - start_time) < timeout:
                await asyncio.sleep(1)
            
            if self.active_jobs:
                logger.warning(f"Timed out waiting for jobs {self.active_jobs} to complete")
        
        # Wait for any remaining active tasks to complete or cancel them
        if self.active_tasks:
            logger.debug(f"Waiting for {len(self.active_tasks)} active tasks to complete...")
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
        
        logger.debug(f"Scheduler {self.worker_id} stopped gracefully")

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
            
            logger.debug(f"Marked {len(self.active_jobs)} jobs for retry by other workers")
            
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
                error_message TEXT, -- Track last error for debugging
                misfire_grace_time INTEGER  -- Per-job misfire grace time in seconds, NULL = no expiration
            );
        """)
        
        # Add misfire_grace_time column if it doesn't exist (migration for existing tables)
        await self._execute_with_retry("""
            DO $$ 
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'scheduled_jobs' 
                    AND column_name = 'misfire_grace_time'
                ) THEN
                    ALTER TABLE scheduled_jobs ADD COLUMN misfire_grace_time INTEGER;
                END IF;
            END $$;
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
            logger.debug("Database initialized with vacuum metrics")
        else:
            logger.debug("Database initialized")

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
                logger.debug("No orphaned jobs found during startup")
                
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
        conflict_resolution: ConflictResolution = ConflictResolution.RAISE,
        misfire_grace_time: Union[int, None, object] = _UNSET
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
            misfire_grace_time: Seconds after execution_time before job expires.
                              - Not specified (default): uses scheduler's misfire_grace_time
                              - Explicit integer (e.g., 60): job expires after N seconds
                              - Explicit None: job never expires
            
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
        
        # Determine effective misfire_grace_time
        # _UNSET = use scheduler default
        # None = explicit no expiration
        # int = explicit per-job grace time
        if misfire_grace_time is _UNSET:
            effective_misfire_grace_time = self.misfire_grace_time
        else:
            effective_misfire_grace_time = misfire_grace_time
        
        # Schedule the job in the database
        scheduled_job_id = await self.schedule_job(
            func.__name__, 
            execution_time, 
            task_data, 
            priority=priority,
            max_retries=max_retries,
            job_id=job_id,
            conflict_resolution=conflict_resolution,
            misfire_grace_time=effective_misfire_grace_time
        )
        
        logger.debug(f"Scheduled async function {func.__name__} to run at {execution_time} (priority={priority.value}, job_id={scheduled_job_id})")
        return scheduled_job_id

    async def schedule_bulk(
        self,
        jobs: List[JobSpec],
        *,
        conflict_resolution: ConflictResolution = ConflictResolution.IGNORE,
        batch_size: int = 1000,
    ) -> List[str]:
        """
        Schedule multiple jobs in a single bulk operation for improved performance.
        
        Args:
            jobs: List of JobSpec instances defining the jobs to schedule
            conflict_resolution: Strategy for ALL jobs (IGNORE recommended for bulk)
            batch_size: Max jobs per database transaction (default 1000)
            
        Returns:
            List of job IDs in same order as input. Jobs that failed to insert
            will have None in their position.
        """
        if not jobs:
            return []
        
        # Validate all functions are async and register them
        for job in jobs:
            if not inspect.iscoroutinefunction(job.func):
                raise TypeError(f"Expected an async function, got {type(job.func)} for {job.func}")
            self.task_map[job.func.__name__] = job.func
        
        all_job_ids: List[str] = []
        
        # Process in batches
        for i in range(0, len(jobs), batch_size):
            batch = jobs[i:i + batch_size]
            batch_ids = await self._insert_jobs_bulk(batch, conflict_resolution)
            all_job_ids.extend(batch_ids)
        
        logger.debug(f"Bulk scheduled {len(all_job_ids)} jobs")
        return all_job_ids

    async def _insert_jobs_bulk(
        self,
        jobs: List[JobSpec],
        conflict_resolution: ConflictResolution
    ) -> List[str]:
        """Insert a batch of jobs using executemany for optimal performance."""
        # Prepare records for bulk insert
        records = []
        for job in jobs:
            task_data = json.dumps({
                'args': job.args,
                'kwargs': job.kwargs or {}
            })
            
            # Determine effective misfire_grace_time
            if job.misfire_grace_time is _JOBSPEC_UNSET:
                effective_misfire = self.misfire_grace_time
            else:
                effective_misfire = job.misfire_grace_time
            
            records.append((
                job.job_id,  # Can be None for auto-generation
                job.func.__name__,
                job.execution_time,
                task_data,
                job.priority.db_value,
                job.max_retries,
                effective_misfire
            ))
        
        try:
            async with self.db_pool.acquire() as conn:
                if conflict_resolution == ConflictResolution.IGNORE:
                    # Use ON CONFLICT DO NOTHING for idempotent bulk inserts
                    result = await conn.fetch("""
                        INSERT INTO scheduled_jobs 
                            (job_id, job_name, execution_time, status, task_data, priority, max_retries, misfire_grace_time)
                        SELECT 
                            COALESCE(d.job_id, gen_random_uuid()::text),
                            d.job_name,
                            d.execution_time,
                            'pending',
                            d.task_data::jsonb,
                            d.priority,
                            d.max_retries,
                            d.misfire_grace_time
                        FROM unnest($1::text[], $2::text[], $3::timestamptz[], $4::text[], $5::int[], $6::int[], $7::int[]) 
                            AS d(job_id, job_name, execution_time, task_data, priority, max_retries, misfire_grace_time)
                        ON CONFLICT (job_id) DO NOTHING
                        RETURNING job_id;
                    """, 
                        [r[0] for r in records],  # job_ids
                        [r[1] for r in records],  # job_names
                        [r[2] for r in records],  # execution_times
                        [r[3] for r in records],  # task_data
                        [r[4] for r in records],  # priorities
                        [r[5] for r in records],  # max_retries
                        [r[6] for r in records],  # misfire_grace_time
                    )
                    return [row['job_id'] for row in result]
                    
                elif conflict_resolution == ConflictResolution.REPLACE:
                    # Use ON CONFLICT DO UPDATE for upsert behavior
                    result = await conn.fetch("""
                        INSERT INTO scheduled_jobs 
                            (job_id, job_name, execution_time, status, task_data, priority, max_retries, misfire_grace_time)
                        SELECT 
                            COALESCE(d.job_id, gen_random_uuid()::text),
                            d.job_name,
                            d.execution_time,
                            'pending',
                            d.task_data::jsonb,
                            d.priority,
                            d.max_retries,
                            d.misfire_grace_time
                        FROM unnest($1::text[], $2::text[], $3::timestamptz[], $4::text[], $5::int[], $6::int[], $7::int[]) 
                            AS d(job_id, job_name, execution_time, task_data, priority, max_retries, misfire_grace_time)
                        ON CONFLICT (job_id) DO UPDATE SET
                            job_name = EXCLUDED.job_name,
                            execution_time = EXCLUDED.execution_time,
                            task_data = EXCLUDED.task_data,
                            priority = EXCLUDED.priority,
                            max_retries = EXCLUDED.max_retries,
                            misfire_grace_time = EXCLUDED.misfire_grace_time,
                            status = CASE 
                                WHEN scheduled_jobs.status IN ('completed', 'failed', 'cancelled', 'expired') THEN 'pending'
                                ELSE scheduled_jobs.status
                            END,
                            retry_count = 0,
                            error_message = NULL,
                            worker_id = NULL,
                            last_heartbeat = NULL,
                            lease_until = NULL
                        RETURNING job_id;
                    """, 
                        [r[0] for r in records],
                        [r[1] for r in records],
                        [r[2] for r in records],
                        [r[3] for r in records],
                        [r[4] for r in records],
                        [r[5] for r in records],
                        [r[6] for r in records],
                    )
                    return [row['job_id'] for row in result]
                    
                else:  # ConflictResolution.RAISE
                    # Simple insert - will fail on duplicates
                    result = await conn.fetch("""
                        INSERT INTO scheduled_jobs 
                            (job_id, job_name, execution_time, status, task_data, priority, max_retries, misfire_grace_time)
                        SELECT 
                            COALESCE(d.job_id, gen_random_uuid()::text),
                            d.job_name,
                            d.execution_time,
                            'pending',
                            d.task_data::jsonb,
                            d.priority,
                            d.max_retries,
                            d.misfire_grace_time
                        FROM unnest($1::text[], $2::text[], $3::timestamptz[], $4::text[], $5::int[], $6::int[], $7::int[]) 
                            AS d(job_id, job_name, execution_time, task_data, priority, max_retries, misfire_grace_time)
                        RETURNING job_id;
                    """, 
                        [r[0] for r in records],
                        [r[1] for r in records],
                        [r[2] for r in records],
                        [r[3] for r in records],
                        [r[4] for r in records],
                        [r[5] for r in records],
                        [r[6] for r in records],
                    )
                    return [row['job_id'] for row in result]
                    
        except Exception as e:
            logger.error(f"Bulk insert failed: {e}")
            raise

    async def listen_for_jobs(self):
        """Listen for jobs with bulk claiming"""
        
        startup_jitter = random.uniform(0, 1.0)
        await asyncio.sleep(startup_jitter)
        
        logger.debug(f"Starting job listener [worker={self.worker_id}]")
        
        while self.is_running and not self.is_shutting_down:
            try:
                # Check available semaphore slots
                available_slots = self.job_semaphore._value
                if available_slots <= 0:
                    await asyncio.sleep(1.0)
                    continue
                
                max_claimable = min(available_slots, self.batch_claim_limit)
                
                # Claim jobs from database
                ready_jobs = await self._claim_jobs(max_claimable)
                
                if ready_jobs:
                    logger.debug(f"Claimed {len(ready_jobs)} jobs [worker={self.worker_id}] (bulk claiming)")
                    
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
                    # Expire old jobs that are past their misfire grace time
                    # Per-job misfire_grace_time takes precedence over scheduler default
                    # NULL misfire_grace_time = no expiration
                    current_time = datetime.datetime.now(UTC)
                    
                    if self.misfire_grace_time is not None:
                        # Only check for expired jobs if scheduler has a default grace time
                        expired_jobs = await conn.fetch("""
                            UPDATE scheduled_jobs 
                            SET status = 'expired', worker_id = $1
                            WHERE status = 'pending' 
                            AND misfire_grace_time IS NOT NULL  -- Only expire jobs with explicit misfire_grace_time
                            AND execution_time + INTERVAL '1 second' * misfire_grace_time < $2
                            RETURNING job_id, job_name, execution_time, misfire_grace_time;
                        """, self.worker_id, current_time)
                        
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
            
            logger.debug(f"Executing job {job_id} ({job_name}) [priority={priority.value}] [worker={self.worker_id}]")
            
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
                logger.debug(f"Job {job_id} completed successfully [worker={self.worker_id}]")
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
                logger.debug(f"Vacuum: deleted {deleted_count} {status} jobs")
        
        if total_deleted > 0:
            logger.debug(f"Vacuum completed: deleted {total_deleted} total jobs")

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

    async def schedule_job(self, job_name, execution_time, task_data, priority: JobPriority = JobPriority.NORMAL, max_retries: int = 0, job_id: Optional[str] = None, conflict_resolution: ConflictResolution = ConflictResolution.RAISE, misfire_grace_time: Optional[int] = None) -> str:
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
            misfire_grace_time: Per-job misfire grace time in seconds. 
                              None = use scheduler default
                              
        Returns:
            str: The job ID of the scheduled job
            
        Raises:
            ValueError: If the provided job_id already exists and conflict_resolution is RAISE
        """
        json_task_data = json.dumps(task_data)
        
        # misfire_grace_time is already resolved by schedule() method
        # It can be None (no expiration) or int (grace time in seconds)
        
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
                        logger.warning(f"Job ID '{job_id}' already exists, ignoring new job (conflict_resolution=IGNORE)")
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
                                misfire_grace_time = $7,
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
                        """, job_id, job_name, execution_time, json_task_data, priority.db_value, max_retries, misfire_grace_time)
                        
                        logger.debug(f"Job ID '{job_id}' replaced with new parameters (conflict_resolution=REPLACE)")
                        return result[0]['job_id']
                
                # Insert with custom job_id (now we know it's unique)
                result = await self._execute_with_retry("""
                    INSERT INTO scheduled_jobs (job_id, job_name, execution_time, status, task_data, priority, max_retries, misfire_grace_time)
                    VALUES ($1, $2, $3, 'pending', $4::jsonb, $5, $6, $7)
                    RETURNING job_id;
                """, job_id, job_name, execution_time, json_task_data, priority.db_value, max_retries, misfire_grace_time)
                
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
                        return await self.schedule_job(job_name, execution_time, task_data, priority, max_retries, job_id, conflict_resolution, misfire_grace_time)
                else:
                    # Re-raise unexpected errors
                    raise
        else:
            # Let database auto-generate job_id
            result = await self._execute_with_retry("""
                INSERT INTO scheduled_jobs (job_name, execution_time, status, task_data, priority, max_retries, misfire_grace_time)
                VALUES ($1, $2, 'pending', $3::jsonb, $4, $5, $6)
                RETURNING job_id;
            """, job_name, execution_time, json_task_data, priority.db_value, max_retries, misfire_grace_time)
        
        scheduled_job_id = result[0]['job_id']
        logger.debug(f"Job scheduled: {job_name} at {execution_time} (priority={priority.value}, job_id={scheduled_job_id})")
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
                logger.debug(f"Job cancelled: {cancelled_job['job_name']} (job_id={job_id})")
                
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
                logger.debug(f"Manual vacuum: deleted {deleted_count} {status} jobs")
        
        total = sum(results.values())
        if total > 0:
            logger.debug(f"Manual vacuum completed: deleted {total} total jobs")
        
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
            logger.debug(f"Enabled periodic job with dedup_key: {dedup_key}")
            return True
        return False
    
    def disable_periodic_job(self, dedup_key: str) -> bool:
        """Disable a specific periodic job"""
        jobs = _periodic_registry.get_jobs()
        if dedup_key in jobs:
            jobs[dedup_key].enabled = False
            logger.debug(f"Disabled periodic job with dedup_key: {dedup_key}")
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
            
            logger.debug(f"Manually triggered periodic job {config.job_name}: {job_id}")
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