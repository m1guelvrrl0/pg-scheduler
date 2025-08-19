import asyncio
import datetime
import json
import asyncpg
import logging
import signal
import sys
from job import Job, JobStatus
import inspect
from datetime import UTC
from typing import Optional, Set
from enum import Enum
import uuid
import random

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class JobPriority(Enum):
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

class Scheduler:
    HEARTBEAT_THRESHOLD = 120  # 2 minutes (in seconds)
    LEASE_DURATION = 60        # 1 minute lease duration (in seconds)
    MAX_RETRY_ATTEMPTS = 3     # For database operations
    WORKER_ID_LENGTH = 8       # For worker identification

    def __init__(self, 
                 db_pool: asyncpg.Pool, 
                 max_concurrent_jobs: int = 10, 
                 misfire_grace_time: int = 300):  # 5 minutes default
        """
        Initialize the Scheduler with concurrency control and reliability features.
        
        Args:
            db_pool: Connection to the PostgreSQL database.
            max_concurrent_jobs: Maximum number of jobs to run concurrently
            misfire_grace_time: Seconds after execution_time before jobs expire (like APScheduler)
        """
        self.db_pool = db_pool
        self.task_map = {}  # Store task functions
        self.is_running = False
        self.is_shutting_down = False
        
        # Generate unique worker ID for this instance
        self.worker_id = str(uuid.uuid4())[:self.WORKER_ID_LENGTH]
        
        # Concurrency control with tracking
        self.job_semaphore = asyncio.Semaphore(max_concurrent_jobs)
        self.max_concurrent_jobs = max_concurrent_jobs
        self.active_jobs: Set[str] = set()  # Track active job IDs
        
        # Job expiration policy
        self.misfire_grace_time = misfire_grace_time
        
        # Background tasks
        self.heartbeat_monitor_task = None
        self.listener_task = None
        self.orphan_recovery_task = None
        
        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()
        
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
        asyncio.create_task(self.stop())

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
            
            logger.info(f"Scheduler started: worker_id={self.worker_id}")
            
        except Exception as e:
            logger.error(f"Error starting scheduler: {e}")
            self.is_running = False
            await self._cleanup_background_tasks()
            raise

    async def stop(self):
        """Stop the scheduler gracefully with job completion"""
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
        
        # Clean up background tasks
        await self._cleanup_background_tasks()
        
        # Mark any remaining jobs as failed (they'll be retried by other workers)
        await self._mark_remaining_jobs_failed()
        
        logger.info(f"Scheduler {self.worker_id} stopped gracefully")

    async def _cleanup_background_tasks(self):
        """Clean up all background tasks"""
        tasks = [self.heartbeat_monitor_task, self.listener_task, self.orphan_recovery_task]
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
        """Listen for jobs with enhanced reliability"""
        
        startup_jitter = random.uniform(0, 1.0)
        await asyncio.sleep(startup_jitter)
        
        logger.info(f"Starting reliable job listener [worker={self.worker_id}]")
        
        while self.is_running and not self.is_shutting_down:
            try:
                if self.job_semaphore.locked():
                    await asyncio.sleep(1.0)
                    continue
                
                ready_jobs = await self.claim_jobs_atomically()
                
                if ready_jobs:
                    logger.info(f"Claimed {len(ready_jobs)} jobs [worker={self.worker_id}]")
                    
                    for job_row in ready_jobs:
                        if not self.is_running:
                            break
                        asyncio.create_task(self.execute_job_with_concurrency_control(job_row))
                
                # Adaptive sleep
                if ready_jobs:
                    jitter = random.uniform(-0.05, 0.05)
                    await asyncio.sleep(0.1 + jitter)
                else:
                    jitter = random.uniform(-0.2, 0.2)
                    await asyncio.sleep(2.0 + jitter)
                    
            except Exception as e:
                logger.error(f"Error in job listener: {e}")
                await asyncio.sleep(5)

    async def claim_jobs_atomically(self):
        """Claim jobs atomically with lease-based reliability"""
        available_slots = self.job_semaphore._value
        if available_slots <= 0:
            return []
        
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
                    
                    # Claim ready jobs atomically using CTE pattern
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
                    """, min(3, available_slots), self.worker_id)
                    
                    # Track claimed jobs
                    for job in ready_jobs:
                        self.active_jobs.add(job['job_id'])
                    
                    return ready_jobs
                    
        except Exception as e:
            logger.error(f"Error claiming jobs atomically: {e}")
            return []

    async def execute_job_with_concurrency_control(self, job_row):
        """Execute job with full reliability and proper resource management"""
        job_id = job_row['job_id']
        
        # Ensure semaphore is always released
        try:
            async with self.job_semaphore:
                await self.execute_job_with_reliability(job_row)
        except Exception as e:
            logger.error(f"Critical error in job {job_id} execution: {e}")
            # Ensure job is properly marked as failed
            await self._safe_mark_job_failed(job_id, str(e))
        finally:
            # Always remove from active jobs tracking
            self.active_jobs.discard(job_id)

    async def execute_job_with_reliability(self, job_row):
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