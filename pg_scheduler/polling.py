from dataclasses import dataclass


@dataclass
class PollingConfig:
    """Configuration for job polling backoff strategy.
    
    Controls how aggressively the scheduler polls for new jobs. Uses exponential
    backoff when idle (no jobs found) and resets to fast polling when jobs are claimed.

    Args:
        min_interval: Seconds to sleep between polls when jobs are actively being
            claimed. Keep this low for responsive job pickup. (default: 0.05)
        max_interval: Upper bound in seconds for the backoff sleep. The idle
            interval will never exceed this value regardless of how long the
            scheduler has been idle. (default: 2.0)
        backoff_multiplier: Factor by which the idle interval grows after each
            empty poll. For example, 1.5 means each successive idle poll sleeps
            50% longer than the previous one. Must be >= 1.0. (default: 1.5)
        idle_start_interval: Initial sleep in seconds when no jobs are found.
            Subsequent idle polls grow this by ``backoff_multiplier`` until
            ``max_interval`` is reached. Resets when jobs are claimed. (default: 0.5)
        semaphore_full_interval: Seconds to wait before re-checking when all
            concurrency slots are occupied. (default: 1.0)
        jitter: When True, adds ±10% random jitter to each sleep to spread out
            polling across workers and avoid thundering-herd effects. (default: True)
    """
    min_interval: float = 0.05
    max_interval: float = 2.0
    backoff_multiplier: float = 1.5
    idle_start_interval: float = 0.5
    semaphore_full_interval: float = 1.0
    jitter: bool = True

    def __post_init__(self) -> None:
        if self.min_interval < 0:
            raise ValueError("min_interval must be non-negative")
        if self.max_interval < self.min_interval:
            raise ValueError("max_interval must be >= min_interval")
        if self.backoff_multiplier < 1.0:
            raise ValueError("backoff_multiplier must be >= 1.0")
        if self.idle_start_interval < 0:
            raise ValueError("idle_start_interval must be non-negative")
        if self.semaphore_full_interval < 0:
            raise ValueError("semaphore_full_interval must be non-negative")
