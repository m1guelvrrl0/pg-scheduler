import asyncio
import datetime
import json
import asyncpg
import logging
from job import Job, JobStatus
import inspect
from datetime import UTC

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Scheduler:
    HEARTBEAT_THRESHOLD = 120  # 2 minutes (in seconds)

    def __init__(self, db_pool: asyncpg.Pool):
        """
        Initialize the Scheduler.
        
        Args:
            db_pool: Connection to the PostgreSQL database.
        """
        self.db_pool = db_pool
        self.task_map = {}  # Store task functions dynamically
        self.is_running = False
        self.heartbeat_monitor_task = None

    async def start(self):
        """Start the scheduler"""
        if self.is_running:
            return
        
        self.is_running = True
        
        try:
            # Initialize everything
            await self.initialize_db()
            await self.load_task_functions()
            
            # Start the heartbeat monitor
            self.heartbeat_monitor_task = asyncio.create_task(self.monitor_heartbeats())
            
            # Start the listener in the background
            self.listener_task = asyncio.create_task(self.listen_for_jobs())
            
        except Exception as e:
            logger.error(f"Error starting scheduler: {e}")
            self.is_running = False
            if self.heartbeat_monitor_task:
                self.heartbeat_monitor_task.cancel()
            raise

    async def stop(self):
        """Stop the scheduler gracefully"""
        if not self.is_running:
            return

        self.is_running = False
        
        # Cancel all running tasks
        if self.heartbeat_monitor_task:
            self.heartbeat_monitor_task.cancel()
        if hasattr(self, 'listener_task'):
            self.listener_task.cancel()
            
        # Wait for tasks to complete
        await asyncio.gather(
            self.heartbeat_monitor_task, 
            self.listener_task, 
            return_exceptions=True
        )

    async def initialize_db(self):
        """Set up the database: create tables and procedures if they don't exist."""
        max_retries = 5
        retry_delay = 1  # seconds
        
        for attempt in range(max_retries):
            try:
                async with self.db_pool.acquire() as conn:
                    # First create the table
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS scheduled_jobs (
                            job_id SERIAL PRIMARY KEY,
                            job_name TEXT NOT NULL,
                            execution_time TIMESTAMPTZ NOT NULL,
                            status TEXT DEFAULT 'pending',
                            task_data JSONB,
                            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                            last_heartbeat TIMESTAMPTZ
                        );
                    """)

                    # Create or replace the notification function
                    await conn.execute("""
                        CREATE OR REPLACE FUNCTION notify_job_ready() RETURNS TRIGGER AS $$
                        BEGIN
                            -- Notify when a job is ready to run
                            IF NEW.status = 'pending' AND NEW.execution_time <= CURRENT_TIMESTAMP THEN
                                PERFORM pg_notify('ready_jobs', row_to_json(NEW)::text);
                            END IF;
                            RETURN NEW;
                        END;
                        $$ LANGUAGE plpgsql;
                    """)

                    # Create a single trigger
                    await conn.execute("""
                        DROP TRIGGER IF EXISTS job_ready_trigger ON scheduled_jobs;
                        CREATE TRIGGER job_ready_trigger
                        AFTER INSERT OR UPDATE ON scheduled_jobs
                        FOR EACH ROW
                        EXECUTE FUNCTION notify_job_ready();
                    """)

                    # Verify trigger installation
                    triggers = await conn.fetch("""
                        SELECT tgname, tgtype, tgenabled, tgisinternal
                        FROM pg_trigger
                        WHERE tgrelid = 'scheduled_jobs'::regclass;
                    """)
                    
                    for trigger in triggers:
                        logger.info(f"Installed trigger: {trigger}")

                    logger.info("Database initialized successfully.")
                    return  # Success, exit the retry loop
                    
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Database initialization attempt {attempt + 1} failed: {e}")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error("Failed to initialize database after maximum retries")
                    raise

    async def schedule(
        self, 
        func, 
        *, 
        execution_time: datetime.datetime,
        args: tuple = (),
        kwargs: dict = None
    ):
        """
        Schedule a function to run at a specific time.
        
        Args:
            func: The async function to schedule
            execution_time: datetime object specifying when to run the job
            args: Tuple of positional arguments to pass to the function
            kwargs: Dictionary of keyword arguments to pass to the function
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
        await self.schedule_job(func.__name__, execution_time, task_data)
        
        logger.info(f"Scheduled function {func.__name__} to run at {execution_time}")

    async def listen_for_jobs(self):
        """Listen for PostgreSQL notifications for ready jobs."""
        async with self.db_pool.acquire() as conn:
            # Listen only for ready jobs
            await conn.add_listener('ready_jobs', self._handle_notification)
            
            while self.is_running:
                try:
                    # Check for ready jobs
                    ready_jobs = await conn.fetch("""
                        SELECT job_id, job_name, execution_time, status, task_data::text
                        FROM scheduled_jobs
                        WHERE status = 'pending'
                        AND execution_time <= CURRENT_TIMESTAMP;
                    """)
                    
                    # Execute any ready jobs
                    for job_row in ready_jobs:
                        task_data = json.loads(job_row['task_data'])
                        job = Job(
                            job_id=job_row['job_id'],
                            job_name=job_row['job_name'],
                            task_func=self.task_map.get(job_row['job_name']),
                            task_data=task_data,
                            status=JobStatus.PENDING
                        )
                        
                        if job.task_func is None:
                            logger.error(f"No task function found for job {job.job_name}")
                            continue
                            
                        # Execute the job
                        asyncio.create_task(self.execute_job_with_lock(job))
                    
                    await asyncio.sleep(1)
                except Exception as e:
                    logger.error(f"Error in job listener: {e}")
                    await asyncio.sleep(1)

    async def _handle_notification(self, connection, pid, channel, payload):
        """Handle incoming job notifications"""
        try:
            logger.info(f"Received notification on channel {channel}: {payload}")  # Add debug logging
            job_data = json.loads(payload)
            job = Job(
                job_id=job_data['job_id'],
                job_name=job_data['job_name'],
                task_func=self.task_map.get(job_data['job_name']),
                task_data=job_data.get('task_data'),
                status=JobStatus.PENDING
            )
            
            if job.task_func is None:
                logger.error(f"No task function found for job {job.job_name}")
                return
                
            await self.execute_job_with_lock(job)
        except Exception as e:
            logger.error(f"Error handling job notification: {e}")

    async def load_task_functions(self):
        """Load task functions from the task map."""
        # Query the database for existing jobs and their job names
        jobs = await self.db_pool.fetch("""
            SELECT DISTINCT job_name FROM scheduled_jobs;
        """)

        # Log any jobs that don't have a matching function
        for job in jobs:
            job_name = job['job_name']
            if job_name not in self.task_map:
                logger.warning(f"No matching task function found for job: {job_name}")

    async def monitor_heartbeats(self):
        """Periodically check for jobs that have a stale heartbeat and mark them as FAILED or RETRYING."""
        while True:
            # Calculate the threshold for the last valid heartbeat
            current_time = datetime.datetime.now(UTC)
            heartbeat_threshold_time = current_time - datetime.timedelta(seconds=self.HEARTBEAT_THRESHOLD)
            try:
                # Fetch jobs with stale heartbeats (running jobs whose last_heartbeat is too old)
                stale_jobs = await self.db_pool.fetch("""
                    SELECT job_id, job_name, last_heartbeat
                    FROM scheduled_jobs
                    WHERE status = 'running' AND last_heartbeat < $1;
                """, heartbeat_threshold_time)

                for job in stale_jobs:
                    # Mark these jobs as FAILED since their heartbeat is stale
                    await self.update_job_status(job['job_id'], JobStatus.FAILED)
                    logger.error(f"Job {job['job_id']} has been marked as FAILED due to stale heartbeat.")
            except Exception as e:
                logger.error(f"An error occurred while monitoring heartbeats: {e}")
            await asyncio.sleep(60)


    async def send_heartbeat(self, job_id):
        """Send periodic heartbeats while a job is running."""
        while True:
            await self.db_pool.execute("""
                UPDATE scheduled_jobs SET last_heartbeat = NOW()
                WHERE job_id = $1 AND status = 'running';
            """, job_id)
            await asyncio.sleep(30)  # Send heartbeat every 30 seconds

    async def pick_job(self, job_id):
        """
        Atomically pick a job and transition it from PENDING to RUNNING.
        """
        async with self.db_pool.acquire() as conn:
            job_row = await conn.fetchrow("""
                UPDATE scheduled_jobs
                SET status = 'running'
                WHERE job_id = $1 AND status = 'pending'
                RETURNING job_id, job_name, task_data::text;
            """, job_id)

            if job_row:
                logger.info(f"Picked job {job_row['job_id']} for execution.")
                # Parse the JSON task_data
                task_data = json.loads(job_row['task_data'])
                task_func = self.task_map.get(job_row['job_name'])
                
                if task_func is None:
                    logger.error(f"No task function found for job {job_row['job_name']}")
                    return None
                    
                return Job(
                    job_id=job_row['job_id'],
                    job_name=job_row['job_name'],
                    task_func=task_func,
                    task_data=task_data,
                    status=JobStatus.RUNNING
                )
            else:
                logger.info(f"Job {job_id} is no longer available for execution.")
                return None
            
    async def schedule_job(self, job_name, execution_time, task_data):
        """
        Schedule a job by inserting it into the 'scheduled_jobs' table.
        
        Args:
            job_name (str): The name of the job to schedule.
            execution_time (datetime): The time at which the job should be executed.
            task_data (dict): Additional data required for the job execution.
        """
        json_task_data = json.dumps(task_data)
        
        async with self.db_pool.acquire() as conn:
            # Insert the job
            await conn.execute("""
                INSERT INTO scheduled_jobs (job_name, execution_time, status, task_data)
                VALUES ($1, $2, 'pending', $3::jsonb);
            """, job_name, execution_time, json_task_data)
            
            # Debug: Check the job status
            job = await conn.fetchrow("""
                SELECT job_id, job_name, execution_time, status
                FROM scheduled_jobs
                WHERE job_name = $1
                ORDER BY job_id DESC
                LIMIT 1;
            """, job_name)
            
            logger.info(f"Job scheduled: {job}")

    async def execute_job_with_lock(self, job: Job):
        """
        Acquire a lock, schedule and execute a job, ensuring atomic status transitions.
        """
        async with self.db_pool.acquire() as conn:
            # Acquire an advisory lock for the job
            lock_acquired = await conn.fetchval("SELECT pg_try_advisory_lock($1);", job.job_id)
            
            if not lock_acquired:
                logger.warning(f"Could not acquire lock for job {job.job_id}. Another worker may be processing it.")
                return

            logger.info(f"Acquired lock for job {job.job_id}.")

            # Start the heartbeat loop in the background
            heartbeat_task = asyncio.create_task(self.send_heartbeat(job.job_id))

            try:
                # Transition the job status from PENDING to RUNNING using the same connection
                job_row = await conn.fetchrow("""
                    UPDATE scheduled_jobs
                    SET status = 'running'
                    WHERE job_id = $1 AND status = 'pending'
                    RETURNING job_id, job_name, task_data::text;
                """, job.job_id)

                if job_row:
                    logger.info(f"Picked job {job_row['job_id']} for execution.")
                    task_data = json.loads(job_row['task_data'])
                    task_func = self.task_map.get(job_row['job_name'])
                    
                    if task_func is None:
                        logger.error(f"No task function found for job {job_row['job_name']}")
                        return None

                    picked_job = Job(
                        job_id=job_row['job_id'],
                        job_name=job_row['job_name'],
                        task_func=task_func,
                        task_data=task_data,
                        status=JobStatus.RUNNING
                    )

                    # Unpack arguments from task_data
                    args = picked_job.task_data.get('args', ())
                    kwargs = picked_job.task_data.get('kwargs', {})
                    
                    # Execute the function with unpacked arguments
                    result = await picked_job.task_func(*args, **kwargs)

                    # Mark the job as completed
                    await conn.execute("""
                        UPDATE scheduled_jobs
                        SET status = $1
                        WHERE job_id = $2;
                    """, JobStatus.COMPLETED.value, picked_job.job_id)
                    
                    logger.info(f"Job {picked_job.job_id} completed successfully with result: {result}")

            except Exception as e:
                # If something goes wrong, mark the job as failed
                await conn.execute("""
                    UPDATE scheduled_jobs
                    SET status = $1
                    WHERE job_id = $2;
                """, JobStatus.FAILED.value, job.job_id)
                logger.error(f"Job {job.job_id} failed with error: {e}")

            finally:
                # Ensure that the heartbeat task is cancelled
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    logger.info(f"Heartbeat task for job {job.job_id} cancelled.")
                
                # Release the advisory lock using the same connection
                await conn.execute("SELECT pg_advisory_unlock($1);", job.job_id)
                logger.info(f"Released lock for job {job.job_id}.")


    async def update_job_status(self, job_id, status):
        """
        Atomically update the status of a job.
        """
        await self.db_pool.execute("""
            UPDATE scheduled_jobs
            SET status = $1
            WHERE job_id = $2;
        """, status.value, job_id)