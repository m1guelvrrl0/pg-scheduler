"""
Example demonstrating cron and timezone support for periodic jobs.

This example shows how to use:
1. Cron expressions for scheduling
2. Timezone-aware scheduling
3. Different cron patterns
"""

import asyncio
import asyncpg
from datetime import timedelta
from pg_scheduler import Scheduler, periodic, JobPriority
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Example 1: Daily task at midnight UTC
@periodic(cron="0 0 * * *")
async def daily_backup():
    """Runs every day at midnight UTC"""
    logger.info("🔄 Running daily backup at midnight UTC...")


# Example 2: Every Sunday at 3am in New York timezone
@periodic(cron="0 3 * * SUN", timezone="America/New_York")
async def weekly_report():
    """Runs every Sunday at 3am EST/EDT"""
    logger.info("📊 Generating weekly report (New York time)...")


# Example 3: Every 15 minutes using cron syntax
@periodic(cron="*/15 * * * *")
async def frequent_check():
    """Runs every 15 minutes"""
    logger.info("✅ Running frequent health check...")


# Example 4: Business hours task (9am-5pm, Mon-Fri) in London
@periodic(cron="0 9-17 * * MON-FRI", timezone="Europe/London", priority=JobPriority.HIGH)
async def business_hours_task():
    """Runs every hour during business hours in London"""
    logger.info("💼 Running business hours task (London time)...")


# Example 5: Start of every hour
@periodic(cron="0 * * * *")
async def hourly_sync():
    """Runs at the start of every hour"""
    logger.info("🔄 Running hourly synchronization...")


# Example 6: First day of the month at 2am
@periodic(cron="0 2 1 * *")
async def monthly_cleanup():
    """Runs on the 1st of every month at 2am UTC"""
    logger.info("🧹 Running monthly cleanup...")


# Example 7: Every 5 minutes in Tokyo timezone
@periodic(cron="*/5 * * * *", timezone="Asia/Tokyo")
async def tokyo_monitor():
    """Runs every 5 minutes in Tokyo timezone"""
    logger.info("🗾 Running Tokyo monitoring task...")


# Example 8: Traditional interval-based scheduling (still supported!)
@periodic(every=timedelta(minutes=10))
async def interval_based_task():
    """Traditional interval-based scheduling"""
    logger.info("⏰ Running interval-based task (every 10 minutes)...")


async def create_pool():
    """Create database connection pool"""
    return await asyncpg.create_pool(
        user='scheduler',
        password='scheduler123',
        database='scheduler_db',
        host='localhost',  # Use 'db' for Docker
        port=5432,
        min_size=2,
        max_size=10
    )


async def main():
    """Main function to run the scheduler"""
    logger.info("Starting PG Scheduler with cron and timezone support...")
    
    # Create connection pool
    pool = await create_pool()
    
    # Create scheduler
    scheduler = Scheduler(pool, max_concurrent_jobs=10)
    
    # Initialize database
    await scheduler.initialize_db()
    
    # Start scheduler (this will register and start all periodic jobs)
    await scheduler.start()
    
    logger.info("=" * 80)
    logger.info("Scheduler started with the following periodic jobs:")
    logger.info("=" * 80)
    logger.info("1. Daily backup: Every day at midnight UTC")
    logger.info("2. Weekly report: Every Sunday at 3am EST/EDT")
    logger.info("3. Frequent check: Every 15 minutes")
    logger.info("4. Business hours: Every hour 9am-5pm Mon-Fri (London)")
    logger.info("5. Hourly sync: Start of every hour")
    logger.info("6. Monthly cleanup: 1st of month at 2am UTC")
    logger.info("7. Tokyo monitor: Every 5 minutes (Tokyo time)")
    logger.info("8. Interval-based: Every 10 minutes")
    logger.info("=" * 80)
    
    # Keep running
    try:
        await asyncio.Event().wait()  # Run forever
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        await scheduler.stop()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())

