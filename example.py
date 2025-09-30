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

async def create_pool():
    return await asyncpg.create_pool(
        user='scheduler',
        password='scheduler123',
        database='scheduler_db',
        host='db',
        port=5432
    )

async def example_job(message: str, sleep_time: int = 3, *, optional_kwarg: str = "default"):
    logger.info(f"Starting example job with message: {message}")
    logger.info(f"Optional kwarg value: {optional_kwarg}")
    await asyncio.sleep(sleep_time)
    logger.info("Example job completed!")
    

async def main():
    pool = await create_pool()
    scheduler = None
    
    try:
        scheduler = Scheduler(pool)
        
        # Start the scheduler
        logger.info("Starting scheduler...")
        await scheduler.start()

        base_time = datetime.now(UTC)

        await scheduler.schedule(
            example_job,
            execution_time=base_time + timedelta(seconds=10),
            args=("Hello from the future!", 5),
            kwargs={"optional_kwarg": "custom value"}
        )

        await scheduler.schedule(
            example_job,
            execution_time=base_time + timedelta(seconds=20),
            args=("Second job!", 3)
        )

        # Job 3: Using default value for sleep_time
        await scheduler.schedule(
            example_job,
            execution_time=base_time + timedelta(seconds=30),
            args=("Third job!",),
            kwargs={"optional_kwarg": "another custom value"}
        )
        
        while True:
            await asyncio.sleep(1)

    except KeyboardInterrupt:
        logger.info("\nShutting down scheduler...")
        if scheduler:
            await scheduler.stop()
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
