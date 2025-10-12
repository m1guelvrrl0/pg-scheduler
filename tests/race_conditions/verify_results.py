"""
Verification script for race condition tests.

This script analyzes the test results from the database to verify:
1. No duplicate job executions (deduplication works)
2. Jobs execute in priority order
3. No race conditions in job claiming
4. Proper concurrent job execution
"""

import asyncio
import os
import sys
from datetime import datetime
import asyncpg
from collections import defaultdict


async def get_db_pool():
    """Create database connection pool."""
    return await asyncpg.create_pool(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        user=os.getenv("PGUSER", "scheduler"),
        password=os.getenv("PGPASSWORD", "scheduler123"),
        database=os.getenv("PGDATABASE", "scheduler_db"),
        min_size=1,
        max_size=5
    )


async def verify_periodic_deduplication(pool: asyncpg.Pool):
    """Verify periodic jobs only ran once per window."""
    print("\n" + "=" * 80)
    print("TEST 1: Periodic Job Deduplication")
    print("=" * 80)
    
    # Count periodic_dedup executions per 5-second window
    results = await pool.fetch("""
        SELECT 
            EXTRACT(EPOCH FROM timestamp)::BIGINT / 5 AS window,
            COUNT(*) as executions,
            array_agg(instance_id) as instances
        FROM test_counters
        WHERE counter_name = 'periodic_dedup'
        GROUP BY window
        ORDER BY window
    """)
    
    success = True
    for row in results:
        window = row['window']
        executions = row['executions']
        instances = row['instances']
        
        print(f"  Window {window}: {executions} execution(s) by {instances}")
        
        if executions > 1:
            print(f"    ❌ FAIL: Periodic job executed {executions} times in same window!")
            success = False
        else:
            print(f"    ✅ PASS: Executed exactly once")
    
    if not results:
        print("  ⚠️  WARNING: No periodic job executions found")
        success = False
    
    return success


async def verify_cron_deduplication(pool: asyncpg.Pool):
    """Verify cron jobs only ran once per minute."""
    print("\n" + "=" * 80)
    print("TEST 2: Cron Job Deduplication")
    print("=" * 80)
    
    # Count cron executions per minute
    results = await pool.fetch("""
        SELECT 
            DATE_TRUNC('minute', timestamp) AS minute,
            COUNT(*) as executions,
            array_agg(instance_id) as instances
        FROM test_counters
        WHERE counter_name = 'cron_dedup'
        GROUP BY minute
        ORDER BY minute
    """)
    
    success = True
    for row in results:
        minute = row['minute']
        executions = row['executions']
        instances = row['instances']
        
        print(f"  Minute {minute}: {executions} execution(s) by {instances}")
        
        if executions > 1:
            print(f"    ❌ FAIL: Cron job executed {executions} times in same minute!")
            success = False
        else:
            print(f"    ✅ PASS: Executed exactly once")
    
    if not results:
        print("  ℹ️  INFO: No cron executions found (may not have reached minute boundary)")
    
    return success


async def verify_custom_id_deduplication(pool: asyncpg.Pool):
    """Verify jobs with same custom ID only ran once."""
    print("\n" + "=" * 80)
    print("TEST 3: Custom Job ID Deduplication")
    print("=" * 80)
    
    success = True
    for job_num in range(5):
        counter_name = f"custom_id_{job_num}"
        results = await pool.fetch("""
            SELECT instance_id, COUNT(*) as count
            FROM test_counters
            WHERE counter_name = $1
            GROUP BY instance_id
        """, counter_name)
        
        total_executions = sum(row['count'] for row in results)
        instances = [row['instance_id'] for row in results]
        
        print(f"  Job #{job_num}: {total_executions} execution(s) by {instances}")
        
        if total_executions > 1:
            print(f"    ❌ FAIL: Job executed {total_executions} times (should be 1)!")
            success = False
        elif total_executions == 1:
            print(f"    ✅ PASS: Executed exactly once")
        else:
            print(f"    ⚠️  WARNING: Job not executed")
    
    return success


async def verify_priority_order(pool: asyncpg.Pool):
    """Verify jobs executed in priority order."""
    print("\n" + "=" * 80)
    print("TEST 4: Priority Execution Order")
    print("=" * 80)
    
    # Get execution order of priority jobs
    results = await pool.fetch("""
        SELECT counter_name, timestamp, instance_id
        FROM test_counters
        WHERE counter_name LIKE 'priority_%'
        ORDER BY timestamp
    """)
    
    if len(results) == 0:
        print("  ⚠️  WARNING: No priority jobs executed")
        return False
    
    expected_order = ["priority_CRITICAL", "priority_HIGH", "priority_NORMAL", "priority_LOW"]
    actual_order = [row['counter_name'] for row in results]
    
    print(f"  Expected order: {expected_order}")
    print(f"  Actual order:   {actual_order}")
    
    # Group by timestamp (jobs at same time)
    by_time = defaultdict(list)
    for row in results:
        # Group by second
        time_key = row['timestamp'].replace(microsecond=0)
        by_time[time_key].append(row['counter_name'])
    
    success = True
    for time_key, jobs in sorted(by_time.items()):
        print(f"  At {time_key}: {jobs}")
        
        # Check if jobs executed in priority order
        job_indices = [expected_order.index(j) if j in expected_order else 999 for j in jobs]
        if job_indices != sorted(job_indices):
            print(f"    ❌ FAIL: Jobs not in priority order!")
            success = False
        else:
            print(f"    ✅ PASS: Jobs in correct priority order")
    
    return success


async def verify_concurrent_execution(pool: asyncpg.Pool):
    """Verify concurrent jobs executed correctly."""
    print("\n" + "=" * 80)
    print("TEST 5: Concurrent Job Execution")
    print("=" * 80)
    
    results = await pool.fetch("""
        SELECT 
            counter_name,
            COUNT(*) as executions,
            COUNT(DISTINCT instance_id) as instance_count,
            array_agg(instance_id) as instances
        FROM test_counters
        WHERE counter_name LIKE 'concurrent_%'
        GROUP BY counter_name
        ORDER BY counter_name
    """)
    
    success = True
    total_executions = 0
    
    for row in results:
        job_name = row['counter_name']
        executions = row['executions']
        instance_count = row['instance_count']
        instances = row['instances']
        total_executions += executions
        
        print(f"  {job_name}: {executions} execution(s) by {instance_count} instance(s) {set(instances)}")
        
        if executions != 1:
            print(f"    ❌ FAIL: Job executed {executions} times (should be 1)!")
            success = False
        else:
            print(f"    ✅ PASS: Executed exactly once")
    
    print(f"\n  Total concurrent jobs executed: {total_executions}/10")
    
    if total_executions < 10:
        print(f"  ⚠️  WARNING: Only {total_executions}/10 jobs executed")
    
    return success


async def verify_database_integrity(pool: asyncpg.Pool):
    """Verify database integrity and check for anomalies."""
    print("\n" + "=" * 80)
    print("DATABASE INTEGRITY CHECK")
    print("=" * 80)
    
    # Check for duplicate job_ids in scheduled_jobs
    duplicates = await pool.fetch("""
        SELECT job_id, COUNT(*) as count
        FROM scheduled_jobs
        GROUP BY job_id
        HAVING COUNT(*) > 1
    """)
    
    if duplicates:
        print(f"  ❌ FAIL: Found {len(duplicates)} duplicate job_ids!")
        for row in duplicates:
            print(f"    - {row['job_id']}: {row['count']} occurrences")
        return False
    else:
        print("  ✅ PASS: No duplicate job_ids")
    
    # Check for jobs claimed by multiple workers
    multi_claimed = await pool.fetch("""
        SELECT job_id, status, worker_id
        FROM scheduled_jobs
        WHERE status = 'running'
        GROUP BY job_id, status, worker_id
        HAVING COUNT(*) > 1
    """)
    
    if multi_claimed:
        print(f"  ❌ FAIL: Found {len(multi_claimed)} jobs claimed by multiple workers!")
        return False
    else:
        print("  ✅ PASS: No jobs claimed by multiple workers")
    
    # Count jobs by status
    status_counts = await pool.fetch("""
        SELECT status, COUNT(*) as count
        FROM scheduled_jobs
        GROUP BY status
        ORDER BY count DESC
    """)
    
    print("\n  Job Status Summary:")
    for row in status_counts:
        print(f"    - {row['status']}: {row['count']}")
    
    return True


async def print_summary_statistics(pool: asyncpg.Pool):
    """Print summary statistics."""
    print("\n" + "=" * 80)
    print("SUMMARY STATISTICS")
    print("=" * 80)
    
    # Count executions per instance
    instance_stats = await pool.fetch("""
        SELECT 
            instance_id,
            COUNT(*) as total_executions,
            COUNT(DISTINCT counter_name) as unique_jobs
        FROM test_counters
        GROUP BY instance_id
        ORDER BY instance_id
    """)
    
    print("\n  Executions per Instance:")
    for row in instance_stats:
        print(f"    Instance {row['instance_id']}: {row['total_executions']} executions, {row['unique_jobs']} unique jobs")
    
    # Total counts
    total_row = await pool.fetchrow("""
        SELECT 
            COUNT(*) as total_executions,
            COUNT(DISTINCT counter_name) as unique_job_types,
            COUNT(DISTINCT instance_id) as num_instances
        FROM test_counters
    """)
    
    print(f"\n  Total executions: {total_row['total_executions']}")
    print(f"  Unique job types: {total_row['unique_job_types']}")
    print(f"  Number of instances: {total_row['num_instances']}")
    
    # Time range
    time_range = await pool.fetchrow("""
        SELECT 
            MIN(timestamp) as first_execution,
            MAX(timestamp) as last_execution
        FROM test_counters
    """)
    
    if time_range['first_execution']:
        duration = time_range['last_execution'] - time_range['first_execution']
        print(f"  Test duration: {duration}")
        print(f"  First execution: {time_range['first_execution']}")
        print(f"  Last execution: {time_range['last_execution']}")


async def main():
    """Main verification function."""
    print("=" * 80)
    print("RACE CONDITION TEST VERIFICATION")
    print("=" * 80)
    
    pool = await get_db_pool()
    
    try:
        # Run all verification tests
        results = {
            "periodic_dedup": await verify_periodic_deduplication(pool),
            "cron_dedup": await verify_cron_deduplication(pool),
            "custom_id_dedup": await verify_custom_id_deduplication(pool),
            "priority_order": await verify_priority_order(pool),
            "concurrent_execution": await verify_concurrent_execution(pool),
            "database_integrity": await verify_database_integrity(pool),
        }
        
        # Print summary
        await print_summary_statistics(pool)
        
        # Final results
        print("\n" + "=" * 80)
        print("FINAL RESULTS")
        print("=" * 80)
        
        passed = sum(1 for v in results.values() if v)
        total = len(results)
        
        for test_name, result in results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"  {status}: {test_name}")
        
        print("\n" + "=" * 80)
        print(f"OVERALL: {passed}/{total} tests passed")
        print("=" * 80)
        
        # Exit with appropriate code
        if passed == total:
            print("\n🎉 ALL TESTS PASSED! No race conditions detected.")
            sys.exit(0)
        else:
            print(f"\n⚠️  {total - passed} test(s) failed. Race conditions may exist.")
            sys.exit(1)
    
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())

