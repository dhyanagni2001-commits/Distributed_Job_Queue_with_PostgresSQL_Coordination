# models.py - Data models / dataclasses

from __future__ import annotations
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class JobStatus(str, Enum):
    PENDING     = "pending"
    CLAIMED     = "claimed"
    RUNNING     = "running"
    COMPLETED   = "completed"
    FAILED      = "failed"
    DEAD_LETTER = "dead_letter"


class EventType(str, Enum):
    ENQUEUED      = "enqueued"
    CLAIMED       = "claimed"
    STARTED       = "started"
    COMPLETED     = "completed"
    FAILED        = "failed"
    RETRIED       = "retried"
    DEAD_LETTERED = "dead_lettered"
    LEASE_RENEWED = "lease_renewed"
    LEASE_EXPIRED = "lease_expired"


@dataclass
class Job:
    job_type: str
    payload: Dict[str, Any]
    queue_name: str = "default"
    priority: int = 5
    max_attempts: int = 3
    retry_delay_sec: int = 5
    idempotency_key: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    scheduled_at: Optional[datetime] = None

    # Set after insertion
    id: Optional[uuid.UUID] = None
    status: JobStatus = JobStatus.PENDING
    attempt_count: int = 0
    created_at: Optional[datetime] = None


@dataclass
class ClaimedJob:
    """Represents a job claimed by a worker, ready for execution."""
    job_id: uuid.UUID
    job_type: str
    payload: Dict[str, Any]
    attempt_count: int
    max_attempts: int
    worker_id: str


@dataclass
class JobResult:
    job_id: uuid.UUID
    success: bool
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    duration_sec: float = 0.0


@dataclass
class WorkerInfo:
    id: str
    queue_names: List[str]
    hostname: str
    pid: int
    status: str = "idle"
    started_at: Optional[datetime] = None
    last_heartbeat: Optional[datetime] = None
    jobs_processed: int = 0
    jobs_failed: int = 0
    current_job_id: Optional[uuid.UUID] = None


@dataclass
class QueueStats:
    queue_name: str
    pending: int = 0
    claimed: int = 0
    running: int = 0
    completed: int = 0
    failed: int = 0
    dead_letter: int = 0
    active_workers: int = 0
    throughput_per_min: float = 0.0
    avg_wait_sec: Optional[float] = None
    avg_exec_sec: Optional[float] = None
    p95_exec_sec: Optional[float] = None
    p99_exec_sec: Optional[float] = None


@dataclass
class MetricsSnapshot:
    captured_at: datetime
    queue_name: str
    pending: int
    running: int
    completed: int
    failed: int
    dead_letter: int
    active_workers: int
    throughput_per_min: float
    avg_wait_sec: Optional[float]
    avg_exec_sec: Optional[float]
    p95_exec_sec: Optional[float]
    p99_exec_sec: Optional[float]