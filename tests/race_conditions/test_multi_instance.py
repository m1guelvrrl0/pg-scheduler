"""
Multi-instance test script for race condition testing.

This script is designed to be run by multiple scheduler instances simultaneously
to test for race conditions, job deduplication, and proper concurrency handling.
"""

import asyncio
import os
import logging
from datetime import UTC, datetime, timedelta
import asyncpg

from pg_scheduler import Scheduler, periodic, JobPriority, ConflictResolution


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [INSTANCE-%(instance)s] - %(levelname)s - %(message)s'
)

# Add instance ID to all log messages
instance_id = os.getenv("SCHEDULER_INSTANCE", "unknown")
logger = logging.getLogger(__name__)
logger = logging.LoggerAdapter(logger, {'instance': instance_id})


# Shared counters stored in database
async def increment_counter(pool: asyncpg.Pool, counter_name: str, instance: str):
    """Increment a counter in the database."""
    await pool.execute("""
        INSERT INTO test_counters (counter_name, instance_id, count, timestamp)
        VALUES ($1, $2, 1, $3)
    """, counter_name, instance, datetime.now(UTC))


# Test 1: Periodic job deduplication
@periodic(every=timedelta(seconds=5))
async def periodic_dedup_test():
    """This should only execute once per window across all instances."""
    logger.info("🔄 Periodic dedup test executed")
    pool = await get_db_pool()
    await increment_counter(pool, "periodic_dedup", instance_id)
    await pool.close()


# Test 2: Cron-based periodic job deduplication
@periodic(cron="* * * * *")  # Every minute
async def cron_dedup_test():
    """Cron job should only execute once per minute across all instances."""
    logger.info("⏰ Cron dedup test executed")
    pool = await get_db_pool()
    await increment_counter(pool, "cron_dedup", instance_id)
    await pool.close()


# Test 3: Job with custom ID deduplication
async def custom_id_job(job_number: int, instance: str):
    """Job with fixed ID - should only run once."""
    logger.info(f"📝 Custom ID job #{job_number} executed by instance {instance}")
    pool = await get_db_pool()
    await increment_counter(pool, f"custom_id_{job_number}", instance)
    await pool.close()


# Test 4: Priority-based job
async def priority_job(priority_name: str, instance: str):
    """Test that priority jobs execute in correct order."""
    logger.info(f"⭐ Priority job ({priority_name}) executed by instance {instance}")
    pool = await get_db_pool()
    await increment_counter(pool, f"priority_{priority_name}", instance)
    await pool.close()


# Test 5: Concurrent job execution
async def concurrent_job(job_id: int, instance: str):
    """Job that can run concurrently."""
    logger.info(f"🔀 Concurrent job #{job_id} started by instance {instance}")
    await asyncio.sleep(1)  # Simulate work
    pool = await get_db_pool()
    await increment_counter(pool, f"concurrent_{job_id}", instance)
    await pool.close()
    logger.info(f"✅ Concurrent job #{job_id} completed by instance {instance}")


async def get_db_pool():
    """Create database connection pool."""
    return await asyncpg.create_pool(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        user=os.getenv("PGUSER", "scheduler"),
        password=os.getenv("PGPASSWORD", "scheduler123"),
        database=os.getenv("PGDATABASE", "scheduler_db"),
        min_size=2,
        max_size=10
    )


async def setup_test_tables(pool: asyncpg.Pool):
    """Create test counter table."""
    await pool.execute("""
        CREATE TABLE IF NOT EXISTS test_counters (
            id SERIAL PRIMARY KEY,
            counter_name TEXT NOT NULL,
            instance_id TEXT NOT NULL,
            count INTEGER NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    logger.info("✅ Test tables created")


async def schedule_test_jobs(scheduler: Scheduler):
    """Schedule various test jobs to test race conditions."""
    now = datetime.now(UTC)
    
    # Test 1: Schedule jobs with same custom ID from all instances
    # Only one should succeed, others should be ignored
    for i in range(5):
        try:
            await scheduler.schedule(
                custom_id_job,
                execution_time=now + timedelta(seconds=10 + i),
                args=(i, instance_id),
                job_id=f"dedup-test-{i}",
                conflict_resolution=ConflictResolution.IGNORE
            )
            logger.info(f"✅ Scheduled custom ID job #{i}")
        except Exception as e:
            logger.error(f"❌ Failed to schedule custom ID job #{i}: {e}")
    
    # Test 2: Schedule priority jobs at same time
    priorities = [
        (JobPriority.CRITICAL, "CRITICAL"),
        (JobPriority.HIGH, "HIGH"),
        (JobPriority.NORMAL, "NORMAL"),
        (JobPriority.LOW, "LOW"),
    ]
    
    execution_time = now + timedelta(seconds=20)
    for priority, name in priorities:
        await scheduler.schedule(
            priority_job,
            execution_time=execution_time,
            args=(name, instance_id),
            priority=priority
        )
    logger.info("✅ Scheduled priority jobs")
    
    # Test 3: Schedule many concurrent jobs
    for i in range(10):
        await scheduler.schedule(
            concurrent_job,
            execution_time=now + timedelta(seconds=30),
            args=(i, instance_id)
        )
    logger.info("✅ Scheduled 10 concurrent jobs")
    
    # Test 4: Test job replacement
    replace_job_id = f"replaceable-{instance_id}"
    
    # First schedule
    await scheduler.schedule(
        custom_id_job,
        execution_time=now + timedelta(seconds=40),
        args=(100, instance_id),
        job_id=replace_job_id,
        priority=JobPriority.NORMAL
    )
    
    # Try to replace (some instances may succeed, others fail)
    await asyncio.sleep(0.1)
    try:
        await scheduler.schedule(
            custom_id_job,
            execution_time=now + timedelta(seconds=45),
            args=(101, instance_id),
            job_id=replace_job_id,
            priority=JobPriority.HIGH,
            conflict_resolution=ConflictResolution.REPLACE
        )
        logger.info("✅ Replaced job successfully")
    except Exception as e:
        logger.info(f"ℹ️  Job replacement attempt: {e}")


async def main():
    """Main test execution."""
    logger.info("=" * 80)
    logger.info(f"🚀 STARTING RACE CONDITION TEST - Instance {instance_id}")
    logger.info("=" * 80)
    
    # Create database pool
    pool = await get_db_pool()
    logger.info("✅ Database connection established")
    
    # Setup test tables
    await setup_test_tables(pool)
    
    # Create scheduler
    scheduler = Scheduler(pool, max_concurrent_jobs=5)
    await scheduler.initialize_db()
    logger.info("✅ Scheduler initialized")
    
    # Start scheduler
    await scheduler.start()
    logger.info("✅ Scheduler started")
    
    # Schedule test jobs
    await schedule_test_jobs(scheduler)
    logger.info("✅ Test jobs scheduled")
    
    # Log periodic jobs
    periodic_jobs = scheduler.get_periodic_jobs()
    logger.info(f"📋 Registered {len(periodic_jobs)} periodic jobs")
    
    logger.info("=" * 80)
    logger.info("⏳ Running tests for 2 minutes...")
    logger.info("=" * 80)
    
    # Run for 2 minutes
    try:
        await asyncio.sleep(120)
    except KeyboardInterrupt:
        logger.info("⚠️  Interrupted by user")
    
    logger.info("=" * 80)
    logger.info(f"✅ Test completed for instance {instance_id}")
    logger.info("=" * 80)
    
    # Shutdown
    await scheduler.shutdown()
    await pool.close()
    logger.info("✅ Scheduler stopped")


if __name__ == "__main__":
    asyncio.run(main())

