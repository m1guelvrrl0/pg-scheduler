#!/usr/bin/env python3
"""
Database monitor to track periodic job executions and verify deduplication.
This script monitors the database and logs execution patterns.
"""

import asyncio
import asyncpg
import logging
from datetime import datetime, timedelta, UTC
from collections import defaultdict
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - DB-MONITOR - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def create_pool():
    """Create database connection pool"""
    return await asyncpg.create_pool(
        user='scheduler',
        password='scheduler123',
        database='scheduler_db',
        host='db',
        port=5432,
        min_size=1,
        max_size=3
    )

async def monitor_scheduled_jobs():
    """Monitor the scheduled_jobs table for periodic job patterns"""
    logger.info("📊 Monitoring scheduled_jobs table...")
    
    pool = await create_pool()
    
    try:
        async with pool.acquire() as conn:
            # Get all periodic jobs
            periodic_jobs = await conn.fetch("""
                SELECT job_id, job_name, execution_time, status, worker_id, created_at
                FROM scheduled_jobs
                WHERE job_id LIKE 'periodic:%'
                ORDER BY created_at DESC
                LIMIT 50;
            """)
            
            if periodic_jobs:
                logger.info(f"📋 Found {len(periodic_jobs)} periodic jobs in database:")
                
                # Group by job name
                job_groups = defaultdict(list)
                for job in periodic_jobs:
                    base_name = job['job_name'].replace('periodic_', '')
                    job_groups[base_name].append(job)
                
                for job_name, jobs in job_groups.items():
                    logger.info(f"  🔍 {job_name}: {len(jobs)} entries")
                    
                    # Check for duplicates in same time window
                    time_windows = defaultdict(list)
                    for job in jobs:
                        # Round to minute for window analysis
                        window = job['execution_time'].replace(second=0, microsecond=0)
                        time_windows[window].append(job)
                    
                    duplicates_found = False
                    for window, window_jobs in time_windows.items():
                        if len(window_jobs) > 1:
                            duplicates_found = True
                            logger.warning(f"    ⚠️  DUPLICATE JOBS in window {window}:")
                            for job in window_jobs:
                                logger.warning(f"      - {job['job_id']} (status: {job['status']}, worker: {job['worker_id']})")
                    
                    if not duplicates_found:
                        logger.info(f"    ✅ No duplicates found for {job_name}")
            else:
                logger.info("📋 No periodic jobs found in scheduled_jobs table")
                
    except Exception as e:
        logger.error(f"Error monitoring scheduled jobs: {e}")
    finally:
        await pool.close()

async def monitor_job_executions():
    """Monitor the job execution log for deduplication verification"""
    logger.info("📊 Monitoring job_execution_log table...")
    
    pool = await create_pool()
    
    try:
        async with pool.acquire() as conn:
            # Check if monitoring table exists
            table_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'job_execution_log'
                );
            """)
            
            if not table_exists:
                logger.info("📋 job_execution_log table doesn't exist yet")
                return
            
            # Get recent executions
            executions = await conn.fetch("""
                SELECT job_name, replica_name, executed_at
                FROM job_execution_log
                WHERE executed_at > NOW() - INTERVAL '10 minutes'
                ORDER BY executed_at DESC;
            """)
            
            if executions:
                logger.info(f"📋 Found {len(executions)} job executions in last 10 minutes:")
                
                # Group by job name and time window
                job_analysis = defaultdict(lambda: defaultdict(list))
                
                for exec in executions:
                    job_name = exec['job_name']
                    # Group by minute for deduplication analysis
                    time_window = exec['executed_at'].replace(second=0, microsecond=0)
                    job_analysis[job_name][time_window].append(exec)
                
                for job_name, time_windows in job_analysis.items():
                    logger.info(f"  🔍 {job_name}:")
                    
                    duplicates_found = False
                    for window, execs in time_windows.items():
                        if len(execs) > 1:
                            duplicates_found = True
                            replicas = [e['replica_name'] for e in execs]
                            logger.error(f"    ❌ DUPLICATE EXECUTION at {window}: {replicas}")
                        else:
                            exec = execs[0]
                            logger.info(f"    ✅ {window}: executed by {exec['replica_name']}")
                    
                    if not duplicates_found:
                        logger.info(f"    ✅ No duplicate executions found for {job_name}")
            else:
                logger.info("📋 No job executions found in last 10 minutes")
                
    except Exception as e:
        logger.error(f"Error monitoring job executions: {e}")
    finally:
        await pool.close()

async def generate_deduplication_report():
    """Generate a comprehensive deduplication report"""
    logger.info("📊 Generating deduplication report...")
    
    pool = await create_pool()
    
    try:
        async with pool.acquire() as conn:
            # Summary statistics
            stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_jobs,
                    COUNT(DISTINCT job_name) as unique_job_types,
                    COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_jobs,
                    COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_jobs,
                    COUNT(CASE WHEN status = 'running' THEN 1 END) as running_jobs,
                    COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_jobs
                FROM scheduled_jobs
                WHERE job_id LIKE 'periodic:%';
            """)
            
            if stats:
                logger.info("📊 DEDUPLICATION REPORT")
                logger.info("=" * 50)
                logger.info(f"Total periodic jobs: {stats['total_jobs']}")
                logger.info(f"Unique job types: {stats['unique_job_types']}")
                logger.info(f"Completed: {stats['completed_jobs']}")
                logger.info(f"Pending: {stats['pending_jobs']}")
                logger.info(f"Running: {stats['running_jobs']}")
                logger.info(f"Failed: {stats['failed_jobs']}")
                
                # Check for potential duplicates by analyzing job patterns
                potential_duplicates = await conn.fetch("""
                    WITH job_windows AS (
                        SELECT 
                            job_name,
                            DATE_TRUNC('minute', execution_time) as time_window,
                            COUNT(*) as job_count
                        FROM scheduled_jobs
                        WHERE job_id LIKE 'periodic:%'
                        GROUP BY job_name, DATE_TRUNC('minute', execution_time)
                        HAVING COUNT(*) > 1
                    )
                    SELECT job_name, time_window, job_count
                    FROM job_windows
                    ORDER BY job_count DESC, time_window DESC;
                """)
                
                if potential_duplicates:
                    logger.warning(f"⚠️  Found {len(potential_duplicates)} potential duplicate time windows:")
                    for dup in potential_duplicates:
                        logger.warning(f"  - {dup['job_name']} at {dup['time_window']}: {dup['job_count']} jobs")
                else:
                    logger.info("✅ No duplicate time windows detected - deduplication working correctly!")
                
            logger.info("=" * 50)
                
    except Exception as e:
        logger.error(f"Error generating report: {e}")
    finally:
        await pool.close()

async def monitor_loop():
    """Main monitoring loop"""
    logger.info("🚀 Starting database monitor for periodic job deduplication")
    logger.info("=" * 80)
    
    iteration = 0
    
    try:
        while True:
            iteration += 1
            logger.info(f"🔄 Monitoring iteration {iteration}")
            
            # Monitor different aspects
            await monitor_scheduled_jobs()
            await asyncio.sleep(2)
            
            await monitor_job_executions()
            await asyncio.sleep(2)
            
            if iteration % 3 == 0:  # Every 3rd iteration
                await generate_deduplication_report()
            
            logger.info(f"⏱️  Waiting 30 seconds before next monitoring cycle...")
            await asyncio.sleep(30)
            
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down database monitor...")
    except Exception as e:
        logger.error(f"❌ Error in monitoring loop: {e}")
        raise

async def main():
    """Main function"""
    await monitor_loop()

if __name__ == "__main__":
    asyncio.run(main())
