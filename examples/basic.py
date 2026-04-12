import asyncio
import asyncpg
from datetime import datetime, timedelta, UTC
from pg_scheduler import Scheduler, JobPriority, periodic
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@periodic(every=timedelta(minutes=15))
async def cleanup_temp_files():
    logger.info("Cleaning up temporary files...")
    await asyncio.sleep(1)
    logger.info("Cleanup completed")


@periodic(every=timedelta(hours=1), priority=JobPriority.CRITICAL, max_retries=3)
async def generate_hourly_report():
    logger.info("Generating hourly report...")
    await asyncio.sleep(2)
    logger.info("Report generated")


@periodic(every=timedelta(minutes=30), use_advisory_lock=True)
async def exclusive_maintenance():
    logger.info("Running exclusive maintenance...")
    await asyncio.sleep(3)
    logger.info("Maintenance completed")


async def example_job(message: str, sleep_time: int = 3, *, optional_kwarg: str = "default"):
    logger.info(f"Starting example job with message: {message}")
    logger.info(f"Optional kwarg value: {optional_kwarg}")
    await asyncio.sleep(sleep_time)
    logger.info("Example job completed!")


async def main():
    pool = await asyncpg.create_pool(
        user='scheduler',
        password='scheduler123',
        database='scheduler_db',
        host='db',
        port=5432,
    )
    scheduler = None

    try:
        scheduler = Scheduler(pool, max_concurrent_jobs=10)

        logger.info("Starting scheduler...")
        await scheduler.start()

        base_time = datetime.now(UTC)

        await scheduler.schedule(
            example_job,
            execution_time=base_time + timedelta(seconds=10),
            args=("Hello from the future!", 5),
            kwargs={"optional_kwarg": "custom value"},
        )

        await scheduler.schedule(
            example_job,
            execution_time=base_time + timedelta(seconds=20),
            args=("Second job!", 3),
        )

        await scheduler.schedule(
            example_job,
            execution_time=base_time + timedelta(seconds=30),
            args=("Third job!",),
            kwargs={"optional_kwarg": "another custom value"},
        )

        periodic_jobs = scheduler.get_periodic_jobs()
        logger.info(f"Registered {len(periodic_jobs)} periodic jobs")
        for dedup_key, config in periodic_jobs.items():
            status = scheduler.get_periodic_job_status(dedup_key)
            logger.info(f"  {status['job_name']}: every {status['interval']}s")

        while True:
            await asyncio.sleep(1)

    except KeyboardInterrupt:
        logger.info("Shutting down scheduler...")
        if scheduler:
            await scheduler.shutdown()
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
