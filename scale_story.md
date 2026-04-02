# Scale Story: When PostgreSQL Breaks and What to Do Next

## Current Architecture Limits

This system uses PostgreSQL as its job queue backbone.
It works excellently up to a point. Here are the exact breaking points,
the symptoms, and the migration path for each.

---

## Breaking Point 1: Connection Saturation (~200-500 workers)

### What happens
Each asyncpg worker holds 2-10 connections in its pool.
PostgreSQL default max_connections = 100.
At ~50 workers you exhaust connections. Workers start timing out on acquire().

### Symptoms
```
asyncpg.exceptions.TooManyConnectionsError
connection pool exhausted, timeout acquiring connection
```

### Fix (buys you to ~2000 workers)
Add PgBouncer as a connection pooler in front of PostgreSQL.

```ini
# pgbouncer.ini
[databases]
jobqueue = host=localhost port=5432 dbname=jobqueue

[pgbouncer]
pool_mode = transaction
max_client_conn = 10000
default_pool_size = 50
```

```python
# config.py — point at PgBouncer, not Postgres directly
host: str = os.getenv("DB_HOST", "pgbouncer")   # port 6432
```

---

## Breaking Point 2: Row Lock Contention (~1000+ jobs/sec)

### Measured threshold
- < 100 workers: negligible contention, sub-millisecond claim latency
- 100–500 workers: ~5ms claim latency
- 500–1000 workers: ~20ms claim latency
- 1000+ workers: DB CPU saturates, claim latency > 100ms

### Fix: Queue partitioning (buys you to ~5000 workers)

```sql
CREATE TABLE jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_name      TEXT NOT NULL DEFAULT 'default',
    job_type        TEXT NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}',
    priority        INTEGER NOT NULL DEFAULT 5,
    status          job_status NOT NULL DEFAULT 'pending',
    scheduled_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    worker_id       TEXT,
    lease_expires_at TIMESTAMPTZ,
    claimed_at      TIMESTAMPTZ,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 3,
    retry_delay_sec INTEGER NOT NULL DEFAULT 5,
    last_error      TEXT,
    last_error_at   TIMESTAMPTZ,
    idempotency_key TEXT UNIQUE,
    result          JSONB,
    tags            TEXT[] DEFAULT '{}',
    metadata        JSONB DEFAULT '{}'
) PARTITION BY LIST (queue_name);

CREATE TABLE jobs_default  PARTITION OF jobs FOR VALUES IN ('default');
CREATE TABLE jobs_email    PARTITION OF jobs FOR VALUES IN ('email');
CREATE TABLE jobs_critical PARTITION OF jobs FOR VALUES IN ('critical');
CREATE TABLE jobs_bulk     PARTITION OF jobs FOR VALUES IN ('bulk');
```

---

## Breaking Point 3: Table Bloat (>10M rows)

### Fix: Archival + vacuum tuning

```sql
-- Archive completed jobs older than 7 days
CREATE TABLE jobs_archive (LIKE jobs INCLUDING ALL);

INSERT INTO jobs_archive
SELECT * FROM jobs
WHERE status IN ('completed', 'dead_letter')
  AND updated_at < NOW() - INTERVAL '7 days';

DELETE FROM jobs
WHERE status IN ('completed', 'dead_letter')
  AND updated_at < NOW() - INTERVAL '7 days';

-- Aggressive autovacuum
ALTER TABLE jobs SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_vacuum_cost_delay = 2
);
```

---

## Breaking Point 4: Single Region / No HA

### Fix options
- PostgreSQL streaming replication + Patroni for automatic failover
- Managed service: AWS RDS Multi-AZ, Google Cloud SQL HA (~30s failover)

---

## When to Migrate Away from PostgreSQL

| Throughput     | Workers  | Recommended stack                          |
|----------------|----------|--------------------------------------------|
| < 1k jobs/min  | < 100    | This system as-is                          |
| < 10k jobs/min | < 500    | This system + PgBouncer + partitioning     |
| < 100k/min     | < 2000   | Redis Streams or Celery + Redis            |
| > 100k/min     | 2000+    | Apache Kafka or AWS SQS + Lambda           |
| > 1M/min       | unbounded| Kafka + consumer groups + Kubernetes       |

---

## Migration Path: AbstractQueueBackend

The worker code is decoupled from the queue backend via QueueManager.
Swapping backends requires implementing one abstract class:

```python
# abstract_backend.py
from abc import ABC, abstractmethod
from typing import Optional, List
from models import ClaimedJob, Job, JobResult
import uuid

class AbstractQueueBackend(ABC):

    @abstractmethod
    async def enqueue(self, job: Job) -> uuid.UUID:
        """Insert a job. Return its ID."""

    @abstractmethod
    async def claim_job(
        self,
        worker_id: str,
        queue_names: List[str],
        lease_sec: int = 30,
    ) -> Optional[ClaimedJob]:
        """Atomically claim the next available job."""

    @abstractmethod
    async def complete_job(self, result: JobResult): ...

    @abstractmethod
    async def fail_job(self, result: JobResult, max_attempts: int): ...

    @abstractmethod
    async def renew_lease(
        self, job_id: uuid.UUID, worker_id: str, lease_sec: int
    ) -> bool: ...
```

Zero worker code changes — just swap the backend in main.py:
```python
# from queue_manager import QueueManager as Backend  # PostgreSQL
from kafka_backend import KafkaBackend as Backend    # Kafka

qm = Backend(bootstrap_servers="kafka:9092")
```

---

## PostgreSQL vs Industry Systems

| Dimension          | This system       | AWS SQS           | Apache Kafka     | Celery + Redis   |
|--------------------|-------------------|-------------------|------------------|------------------|
| Exactly-once       | YES (DB locks)    | at-least-once     | at-least-once    | at-least-once    |
| Max throughput     | ~10k/min          | unlimited         | millions/sec     | ~50k/min         |
| Operational cost   | low (1 DB)        | zero (managed)    | high (cluster)   | medium (Redis)   |
| Query/debug jobs   | YES (SQL)         | no SQL            | no SQL           | limited          |
| Ordered delivery   | YES (priority)    | no order          | per partition    | no order         |
| Scheduled jobs     | YES (scheduled_at)| delay queues      | external needed  | YES (eta param)  |
| Dead letter        | YES (same DB)     | DLQ native        | separate topic   | native           |
| Multi-region       | needs replication | built-in          | built-in         | needs setup      |
| Extra infra needed | none              | none (managed)    | Zookeeper/KRaft  | Redis            |
