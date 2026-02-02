from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Optional, Union

from .job_priority import JobPriority

# Sentinel value to distinguish "not specified" from "explicitly None"
_UNSET = object()


@dataclass
class JobSpec:
    func: Callable
    execution_time: datetime
    args: tuple = ()
    kwargs: dict = field(default_factory=dict)
    priority: JobPriority = JobPriority.NORMAL
    max_retries: int = 0
    job_id: Optional[str] = None
    misfire_grace_time: Union[int, None, object] = _UNSET  # _UNSET = use scheduler default
