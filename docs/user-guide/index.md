# User Guide

This comprehensive guide covers all aspects of using PG Scheduler.

```{toctree}
:maxdepth: 2

basic-usage
periodic-jobs
reliability
vacuum-policies
configuration
troubleshooting
```

## Overview

PG Scheduler is designed to be both simple for basic use cases and powerful for production deployments. This guide will walk you through:

1. **Basic Usage** - Core concepts and simple job scheduling
2. **Periodic Jobs** - Using the `@periodic` decorator effectively  
3. **Reliability** - Understanding deduplication, retries, and error handling
4. **Vacuum Policies** - Managing job lifecycle and cleanup
5. **Configuration** - Tuning the scheduler for your needs
6. **Troubleshooting** - Common issues and solutions

## Key Concepts

### Jobs vs Periodic Jobs

- **Jobs**: One-time executions scheduled for a specific time
- **Periodic Jobs**: Recurring executions that reschedule themselves

### Deduplication

PG Scheduler prevents duplicate job executions across multiple worker replicas using:

- **Deterministic Job IDs**: Based on function signature and parameters
- **Database Constraints**: PostgreSQL primary keys prevent duplicates
- **Window-based Logic**: Time-based deduplication for periodic jobs

### Priority System

Jobs can have different priorities:

- `JobPriority.CRITICAL` - Executes before normal jobs
- `JobPriority.NORMAL` - Default priority

### Reliability Features

- **Heartbeat Monitoring**: Detect crashed workers
- **Orphan Recovery**: Clean up abandoned jobs
- **Graceful Shutdown**: Complete active jobs before stopping
- **Retry Logic**: Automatic retry with exponential backoff
