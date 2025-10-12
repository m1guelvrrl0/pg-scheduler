"""
Test file for cron and timezone features.
This file schedules jobs that will execute within the next few minutes for testing.
"""

import asyncio
import asyncpg
from datetime import datetime, timedelta, UTC
from zoneinfo import ZoneInfo
from pg_scheduler import Scheduler, periodic, JobPriority
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Test 1: Cron job that runs every minute (for quick testing)
@periodic(cron="* * * * *")
async def every_minute_utc():
    """Runs every minute (UTC)"""
    now_utc = datetime.now(UTC)
    logger.info(f"⏰ [CRON-UTC] Running every minute job at {now_utc.strftime('%H:%M:%S')} UTC")


# Test 2: Cron with New York timezone - every minute
@periodic(cron="* * * * *", timezone="America/New_York")
async def every_minute_ny():
    """Runs every minute (New York time)"""
    now_utc = datetime.now(UTC)
    now_ny = now_utc.astimezone(ZoneInfo("America/New_York"))
    logger.info(f"🗽 [CRON-NY] Running every minute job at {now_ny.strftime('%H:%M:%S')} EST/EDT (UTC: {now_utc.strftime('%H:%M:%S')})")


# Test 3: Cron with Tokyo timezone - every minute
@periodic(cron="* * * * *", timezone="Asia/Tokyo")
async def every_minute_tokyo():
    """Runs every minute (Tokyo time)"""
    now_utc = datetime.now(UTC)
    now_tokyo = now_utc.astimezone(ZoneInfo("Asia/Tokyo"))
    logger.info(f"🗾 [CRON-TOKYO] Running every minute job at {now_tokyo.strftime('%H:%M:%S')} JST (UTC: {now_utc.strftime('%H:%M:%S')})")


# Test 4: Cron every 2 minutes
@periodic(cron="*/2 * * * *")
async def every_two_minutes():
    """Runs every 2 minutes"""
    now = datetime.now(UTC)
    logger.info(f"⏱️  [CRON-2MIN] Running every 2 minutes at {now.strftime('%H:%M:%S')} UTC")


# Test 5: Traditional interval-based (for comparison)
@periodic(every=timedelta(minutes=1))
async def interval_one_minute():
    """Traditional interval-based scheduling (every minute)"""
    now = datetime.now(UTC)
    logger.info(f"⏲️  [INTERVAL] Running interval-based job at {now.strftime('%H:%M:%S')} UTC")


# Test 6: Cron with priority
@periodic(cron="* * * * *", priority=JobPriority.HIGH)
async def high_priority_cron():
    """High priority cron job"""
    now = datetime.now(UTC)
    logger.info(f"⭐ [CRON-HIGH] Running high priority cron at {now.strftime('%H:%M:%S')} UTC")


# Test 7: Cron with London timezone
@periodic(cron="* * * * *", timezone="Europe/London")
async def every_minute_london():
    """Runs every minute (London time)"""
    now_utc = datetime.now(UTC)
    now_london = now_utc.astimezone(ZoneInfo("Europe/London"))
    logger.info(f"🇬🇧 [CRON-LONDON] Running every minute job at {now_london.strftime('%H:%M:%S')} GMT/BST (UTC: {now_utc.strftime('%H:%M:%S')})")


async def create_pool():
    """Create database connection pool"""
    # Try Docker hostname first, fallback to localhost
    try:
        return await asyncpg.create_pool(
            user='scheduler',
            password='scheduler123',
            database='scheduler_db',
            host='db',
            port=5432,
            min_size=2,
            max_size=10,
            timeout=5
        )
    except Exception:
        return await asyncpg.create_pool(
            user='scheduler',
            password='scheduler123',
            database='scheduler_db',
            host='localhost',
            port=5432,
            min_size=2,
            max_size=10
        )


async def print_job_summary(pool):
    """Print a summary of scheduled jobs from the database"""
    await asyncio.sleep(2)  # Wait a bit for jobs to be scheduled
    
    query = """
        SELECT 
            job_name,
            status,
            priority,
            execution_time,
            execution_time AT TIME ZONE 'UTC' as exec_time_utc,
            misfire_grace_time
        FROM scheduled_jobs 
        WHERE status IN ('pending', 'running')
        ORDER BY execution_time 
        LIMIT 20
    """
    
    rows = await pool.fetch(query)
    
    logger.info("=" * 100)
    logger.info("📊 SCHEDULED JOBS IN DATABASE:")
    logger.info("=" * 100)
    
    if rows:
        for row in rows:
            exec_time = row['exec_time_utc']
            time_str = exec_time.strftime('%Y-%m-%d %H:%M:%S') if exec_time else 'N/A'
            logger.info(
                f"  • {row['job_name']:30s} | Status: {row['status']:8s} | "
                f"Priority: {row['priority']} | Execution: {time_str} UTC"
            )
    else:
        logger.info("  No pending jobs found")
    
    logger.info("=" * 100)


async def main():
    """Main function to run the scheduler"""
    logger.info("=" * 100)
    logger.info("🚀 STARTING CRON & TIMEZONE FEATURE TEST")
    logger.info("=" * 100)
    
    # Create connection pool
    pool = await create_pool()
    logger.info("✅ Database connection established")
    
    # Create scheduler
    scheduler = Scheduler(pool, max_concurrent_jobs=20)
    logger.info("✅ Scheduler created")
    
    # Initialize database
    await scheduler.initialize_db()
    logger.info("✅ Database initialized")
    
    # Start scheduler (this will register and start all periodic jobs)
    await scheduler.start()
    logger.info("✅ Scheduler started")
    
    logger.info("=" * 100)
    logger.info("📋 REGISTERED PERIODIC JOBS:")
    logger.info("=" * 100)
    logger.info("1. ⏰ every_minute_utc       - Cron: * * * * * (UTC)")
    logger.info("2. 🗽 every_minute_ny        - Cron: * * * * * (America/New_York)")
    logger.info("3. 🗾 every_minute_tokyo     - Cron: * * * * * (Asia/Tokyo)")
    logger.info("4. ⏱️  every_two_minutes     - Cron: */2 * * * * (UTC)")
    logger.info("5. ⏲️  interval_one_minute   - Interval: every 1 minute")
    logger.info("6. ⭐ high_priority_cron    - Cron: * * * * * (HIGH priority)")
    logger.info("7. 🇬🇧 every_minute_london   - Cron: * * * * * (Europe/London)")
    logger.info("=" * 100)
    
    # Print current time in different timezones
    now_utc = datetime.now(UTC)
    now_ny = now_utc.astimezone(ZoneInfo("America/New_York"))
    now_tokyo = now_utc.astimezone(ZoneInfo("Asia/Tokyo"))
    now_london = now_utc.astimezone(ZoneInfo("Europe/London"))
    
    logger.info("")
    logger.info("🌍 CURRENT TIME IN DIFFERENT TIMEZONES:")
    logger.info(f"  UTC:        {now_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"  New York:   {now_ny.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"  Tokyo:      {now_tokyo.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"  London:     {now_london.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info("")
    
    # Print job summary from database
    await print_job_summary(pool)
    
    logger.info("")
    logger.info("⏳ Jobs will start executing at their scheduled times...")
    logger.info("   Watch the logs below for execution confirmations!")
    logger.info("=" * 100)
    
    # Keep running to see the jobs execute
    try:
        await asyncio.sleep(180)  # Run for 3 minutes to see multiple executions
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await scheduler.shutdown()
        await pool.close()
        logger.info("✅ Scheduler stopped")


if __name__ == "__main__":
    asyncio.run(main())

