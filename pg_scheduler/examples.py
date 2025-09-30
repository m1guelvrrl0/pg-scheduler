"""
Example usage of PG Scheduler
"""

import asyncio
import asyncpg
from datetime import datetime, timedelta, UTC
from pg_scheduler import Scheduler, JobPriority, periodic


# Example periodic jobs
@periodic(every=timedelta(minutes=15))
async def cleanup_temp_files():
    """Clean up temporary files every 15 minutes"""
    print("Cleaning up temporary files...")
    await asyncio.sleep(1)  # Simulate cleanup work
    print("Cleanup completed")


@periodic(every=timedelta(hours=1), priority=JobPriority.CRITICAL, max_retries=3)
async def generate_hourly_report():
    """Generate hourly reports with high priority and retries"""
    print("Generating hourly report...")
    await asyncio.sleep(2)  # Simulate report generation
    print("Report generated")


@periodic(every=timedelta(minutes=30), use_advisory_lock=True)
async def exclusive_maintenance():
    """Maintenance that should only run on one worker at a time"""
    print("Running exclusive maintenance...")
    await asyncio.sleep(3)  # Simulate maintenance work
    print("Maintenance completed")


# Example regular jobs
async def send_email(recipient: str, subject: str):
    """Example job function"""
    print(f"Sending email to {recipient}: {subject}")
    await asyncio.sleep(1)  # Simulate async work
    print(f"Email sent to {recipient}")


async def process_data(data_id: int, priority: str = "normal"):
    """Example data processing job"""
    print(f"Processing data {data_id} (priority: {priority})")
    await asyncio.sleep(2)  # Simulate processing
    print(f"Data {data_id} processed")


async def example_basic_usage():
    """Basic scheduler usage example"""
    # Create database connection pool
    db_pool = await asyncpg.create_pool(
        user='scheduler',
        password='scheduler123',
        database='scheduler_db',
        host='localhost',
        port=5432
    )
    
    # Initialize scheduler
    scheduler = Scheduler(db_pool=db_pool, max_concurrent_jobs=10)
    await scheduler.start()
    
    try:
        print("Basic Scheduler Example")
        
        # Schedule immediate jobs
        job_id1 = await scheduler.schedule(
            send_email,
            execution_time=datetime.now(UTC) + timedelta(seconds=5),
            args=("user@example.com", "Welcome!"),
            priority=JobPriority.NORMAL
        )
        
        # Schedule with retry logic
        job_id2 = await scheduler.schedule(
            process_data,
            execution_time=datetime.now(UTC) + timedelta(seconds=10),
            args=(123,),
            kwargs={"priority": "high"},
            priority=JobPriority.CRITICAL,
            max_retries=3
        )
        
        print(f"Scheduled jobs: {job_id1}, {job_id2}")
        
        # Let jobs run
        await asyncio.sleep(30)
        
    finally:
        await scheduler.shutdown()
        await db_pool.close()


async def example_periodic_jobs():
    """Periodic jobs example"""
    # Create database connection pool
    db_pool = await asyncpg.create_pool(
        user='scheduler',
        password='scheduler123',
        database='scheduler_db',
        host='localhost',
        port=5432
    )
    
    # Initialize scheduler
    scheduler = Scheduler(db_pool=db_pool, max_concurrent_jobs=5)
    await scheduler.start()
    
    try:
        print("Periodic Jobs Example")
        
        # Show registered periodic jobs
        periodic_jobs = scheduler.get_periodic_jobs()
        print(f"Registered {len(periodic_jobs)} periodic jobs:")
        for dedup_key, config in periodic_jobs.items():
            status = scheduler.get_periodic_job_status(dedup_key)
            print(f"  • {status['job_name']}: every {status['interval']}s")
        
        # Let periodic jobs run
        print("⏱Letting periodic jobs run for 2 minutes...")
        await asyncio.sleep(120)
        
        # Demonstrate manual triggering
        if periodic_jobs:
            first_key = list(periodic_jobs.keys())[0]
            print(f"Manually triggering job: {first_key}")
            manual_job_id = await scheduler.trigger_periodic_job(first_key)
            if manual_job_id:
                print(f"Manual job triggered: {manual_job_id}")
        
        await asyncio.sleep(10)
        
    finally:
        await scheduler.shutdown()
        await db_pool.close()


if __name__ == "__main__":
    print("Choose an example to run:")
    print("1. Basic Usage")
    print("2. Periodic Jobs")
    
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == "1":
        asyncio.run(example_basic_usage())
    elif choice == "2":
        asyncio.run(example_periodic_jobs())
    else:
        print("Invalid choice")
