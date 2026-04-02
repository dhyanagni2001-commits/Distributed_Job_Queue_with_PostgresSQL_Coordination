# Distributed Job Queue

PostgreSQL-backed distributed job queue with exactly-once execution, fault tolerance, and full observability stack.

## Live Results

| Metric | Value |
|--------|-------|
| Jobs completed | 300 / 300 |
| Failure rate | 0% |
| Peak throughput | 242 jobs/min |
| Avg throughput | 102.7 jobs/min |
| Avg execution time | 0.90s |
| p95 execution time | 2.46s |
| p99 execution time | 2.91s |
| Chaos tests passing | 5 / 5 |

## Chaos Test Results

```
✓ idempotency      — same key, 3 calls, 1 job created
✓ exactly_once     — 20 jobs, 5 concurrent workers, zero duplicates
✓ dead_letter      — retry exhaustion after 2 attempts, error stored
✓ priority_order   — HIGH → MED → LOW execution order proven
✓ lease_renewal    — 25s job completed within 30s lease window
```

## Quick Start

```bash
# Option A: Docker (recommended) — runs 3 worker nodes + Postgres + Prometheus + Grafana
docker compose up --build

# Option B: Local
createdb jobqueue2
pip install "asyncpg>=0.30.0" rich==13.7.1
psql -U $USER -d jobqueue2 -f schema.sql
python main.py
```

**Dashboards (Docker only)**

| URL | What |
|-----|------|
| http://localhost:3000 | Grafana (admin / admin) |
| http://localhost:9090 | Prometheus |
| http://localhost:8000/metrics | Raw Prometheus metrics |

## Architecture

```
Producer → PostgreSQL jobs table (status=pending)
                    ↓
        claim_next_job() — SELECT FOR UPDATE SKIP LOCKED
                    ↓
  Worker-01    Worker-02    Worker-03    (3 nodes, concurrent)
      ↓             ↓            ↓
  execute handler (async)
      ↓             ↓            ↓
  complete / retry / dead-letter
                    ↓
  prometheus_exporter → Prometheus → Grafana
```

**Exactly-once guarantee**: PostgreSQL row-level locking (`SELECT FOR UPDATE SKIP LOCKED`) ensures only one worker ever claims a given job. No application-level coordination needed.

**Crash recovery**: Every job holds a lease with an expiry timestamp. If a worker crashes, the lease expires and any other worker automatically reclaims the job — no manual intervention, no dedicated reaper process.

## How a Job Moves Through the System

```
1. ENQUEUE
   INSERT INTO jobs (status='pending')
   ON CONFLICT (idempotency_key) DO UPDATE   ← duplicate-safe

2. CLAIM  ← atomic, exactly-once
   SELECT FOR UPDATE SKIP LOCKED             ← no two workers get same row
   UPDATE status='claimed', lease_expires_at=NOW()+30s

3. EXECUTE
   handler coroutine runs with payload
   lease renewal runs every 10s in background

4a. SUCCESS  → status='completed', result stored
4b. FAILURE  → status='pending', scheduled_at=NOW()+retry_delay (retry)
4c. EXHAUSTED → status='failed' → dead_letter

5. RECOVERY (every worker, every 15s)
   recover_expired_leases() — reset stale claimed jobs → pending
   process_dead_letters()   — move exhausted failed jobs → dead_letter
```

## Project Structure

```
├── schema.sql              # Tables, indexes, 4 stored functions
├── config.py               # All tunable parameters
├── models.py               # Data classes: Job, ClaimedJob, JobResult
├── database.py             # asyncpg connection pool
├── queue_manager.py        # Every DB operation
├── worker.py               # Async worker: poll, lease renewal, heartbeat, recovery
├── metrics.py              # Stats snapshots every 5s
├── dashboard.py            # Rich terminal live UI
├── main.py                 # Entry point + job handlers
├── prometheus_exporter.py  # /metrics HTTP endpoint
├── chaos_test.py           # Fault-tolerance test suite (5 tests)
├── abstract_backend.py     # Interface for swapping queue backends
├── scale_story.md          # When PostgreSQL breaks + migration path
├── Dockerfile
├── docker-compose.yml      # 3 workers + Postgres + Prometheus + Grafana
├── prometheus.yml
├── alerting_rules.yml      # 7 alert rules
└── grafana/
    └── provisioning/       # Auto-imported dashboard + datasource
```

## Configuration

All settings in `config.py`. Override with environment variables.

| Parameter | Default | Notes |
|-----------|---------|-------|
| `poll_interval_sec` | `1.0` | Sleep between polls when queue is empty |
| `lease_duration_sec` | `30` | How long a worker owns a job |
| `lease_renewal_interval_sec` | `10` | How often lease extends during execution |
| `default_max_attempts` | `3` | Retries before dead-letter |
| `default_retry_delay_sec` | `5` | Seconds before retry re-enters queue |
| `recovery_interval_sec` | `15` | How often each worker runs maintenance |
| `heartbeat_interval_sec` | `5` | Worker liveness ping interval |
| `NUM_WORKERS` | `3` | Set via env var |

> **Rule**: `lease_duration_sec` must be greater than `lease_renewal_interval_sec × 2`

## Adding a Job Type

```python
# 1. Define handler
async def handle_my_job(payload: dict) -> dict:
    result = do_something(payload["input"])
    return {"output": result}

# 2. Register on every worker
worker.register_handler("my_job_type", handle_my_job)

# 3. Enqueue from anywhere
job_id = await qm.enqueue(Job(
    job_type="my_job_type",
    payload={"input": "hello"},
    queue_name="default",
    priority=3,
    max_attempts=5,
    idempotency_key="unique-key-123",
))
```

## Scaling Beyond PostgreSQL

| Throughput | Workers | Stack |
|------------|---------|-------|
| < 1k jobs/min | < 100 | This system as-is |
| < 10k jobs/min | < 500 | + PgBouncer + table partitioning |
| < 100k jobs/min | < 2000 | Redis Streams or Celery + Redis |
| > 100k jobs/min | 2000+ | Apache Kafka or AWS SQS |

Swapping backends requires implementing one abstract class (`abstract_backend.py`). Zero worker code changes.

## vs Industry Systems

| | This system | AWS SQS | Apache Kafka | Celery + Redis |
|--|-------------|---------|--------------|----------------|
| Exactly-once | YES | at-least-once | at-least-once | at-least-once |
| Max throughput | ~10k/min | unlimited | millions/sec | ~50k/min |
| Debug with SQL | YES | NO | NO | limited |
| Scheduled jobs | YES | delay queues | external | YES |
| Extra infra | none | none (managed) | Zookeeper | Redis |
| Operational cost | low | zero | high | medium |

## Observability

**Prometheus metrics exposed at `/metrics`:**
- `jobqueue_pending_total` — queue depth
- `jobqueue_throughput_per_minute` — jobs/min
- `jobqueue_p95_exec_seconds` / `jobqueue_p99_exec_seconds` — latency
- `jobqueue_failure_rate` — ratio of failed to total
- `jobqueue_active_workers` — worker liveness
- `jobqueue_worker_alive{worker="..."}` — per-worker health

**Alerting rules (7 total):**
- Queue depth > 500 for 2 min → warning
- Queue depth > 2000 for 1 min → critical
- Failure rate > 5% for 5 min → warning
- Zero active workers for 1 min → critical
- Worker count < 2 for 3 min → warning
- Dead letter growing > 10 in 10 min → warning
- p99 latency > 30s for 5 min → warning

## Useful SQL Queries

```sql
-- Job status breakdown
SELECT status, COUNT(*) FROM jobs GROUP BY status;

-- Dead-lettered jobs with errors
SELECT id, job_type, last_error, attempt_count
FROM jobs WHERE status = 'dead_letter';

-- Throughput over last hour (1-min buckets)
SELECT date_trunc('minute', captured_at) AS minute,
       AVG(throughput_per_min) AS tpm
FROM metrics_snapshots
WHERE captured_at > NOW() - INTERVAL '1 hour'
GROUP BY 1 ORDER BY 1;

-- Slowest job types
SELECT job_type,
       AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) AS avg_sec,
       COUNT(*) AS total
FROM jobs WHERE status = 'completed'
GROUP BY job_type ORDER BY avg_sec DESC;

-- Full audit trail for a job
SELECT event_type, worker_id, message, occurred_at
FROM job_events
WHERE job_id = '<uuid>'
ORDER BY occurred_at;
```

## Requirements

- Python 3.11+
- PostgreSQL 14+
- `asyncpg >= 0.30.0`
- `rich == 13.7.1`
- Docker + Docker Compose (for multi-node mode)