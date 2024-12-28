from enum import Enum
from dataclasses import dataclass

class JobStatus(Enum):
    PENDING = 'pending'
    RUNNING = 'running'
    COMPLETED = 'completed'
    FAILED = 'failed'
    RETRYING = 'retrying'
    CANCELLED = 'cancelled'

@dataclass
class Job:
    job_id: str
    job_name: str
    task_func: callable
    task_data: dict
    status: JobStatus = JobStatus.PENDING
