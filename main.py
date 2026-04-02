# main.py - Entry point: wires everything together and runs a demo workload

from __future__ import annotations

import asyncio
import logging
import random
import sys
import uuid
from typing import Any, Dict, Optional

from config import config
from dashboard import Dashboard
from database import apply_schema, close_pool, init_pool
from metrics import MetricsCollector
from models import Job
from queue_manager import QueueManager
from worker import Worker

logging.basicConfig(
    level=getattr(logging, config.log_level),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


# =============================================================================
# Job Handlers
# =============================================================================

async def handle_send_email(payload: Dict[str, Any]) -> Optional[Dict]:
    """Simulate sending an email with variable latency."""
    await asyncio.sleep(random.uniform(0.1, 0.8))
    if random.random() < 0.08:   # 8% failure rate
        raise ConnectionError("SMTP server timeout")
    return {"email_id": str(uuid.uuid4()), "recipient": payload.get("to")}


async def handle_resize_image(payload: Dict[str, Any]) -> Optional[Dict]:
    """Simulate image resizing (CPU-ish, slightly longer)."""
    await asyncio.sleep(random.uniform(0.3, 1.5))
    if random.random() < 0.05:
        raise ValueError("Corrupt image data")
    return {"output_key": f"resized/{payload.get('image_id')}.webp"}


async def handle_generate_report(payload: Dict[str, Any]) -> Optional[Dict]:
    """Simulate a longer report generation task."""
    await asyncio.sleep(random.uniform(1.0, 3.0))
    if random.random() < 0.03:
        raise RuntimeError("Database query timeout")
    return {"report_url": f"https://cdn.example.com/reports/{uuid.uuid4()}.pdf"}


async def handle_send_notification(payload: Dict[str, Any]) -> Optional[Dict]:
    """Fast push notification dispatch."""
    await asyncio.sleep(random.uniform(0.05, 0.2))
    return {"notification_id": str(uuid.uuid4())}


async def handle_data_sync(payload: Dict[str, Any]) -> Optional[Dict]:
    """Simulate syncing data with an external API."""
    await asyncio.sleep(random.uniform(0.5, 2.0))
    if random.random() < 0.12:
        raise IOError("External API rate limited")
    return {"synced_records": random.randint(10, 1000)}


# =============================================================================
# Producer: continuously enqueue demo jobs
# =============================================================================

JOB_TEMPLATES = [
    ("send_email",         {"to": "user@example.com", "subject": "Hello"},      5, 3),
    ("resize_image",       {"image_id": "img-001",    "width": 800},             4, 3),
    ("generate_report",    {"report_type": "monthly", "user_id": 42},            3, 5),
    ("send_notification",  {"user_id": 99,             "message": "New message"}, 5, 2),
    ("data_sync",          {"source": "crm",           "entity": "contacts"},    4, 4),
]


async def producer(qm: QueueManager, num_jobs: int = 200, rate_per_sec: float = 5.0):
    """Enqueue jobs at a controlled rate with random types and idempotency keys."""
    logger.info("Producer starting: %d jobs at %.1f/sec", num_jobs, rate_per_sec)
    interval = 1.0 / rate_per_sec

    for i in range(num_jobs):
        job_type, base_payload, priority, max_attempts = random.choice(JOB_TEMPLATES)

        payload = {**base_payload, "job_seq": i, "run_id": str(uuid.uuid4())[:8]}

        job = Job(
            job_type        = job_type,
            payload         = payload,
            queue_name      = "default",
            priority        = priority,
            max_attempts    = max_attempts,
            retry_delay_sec = random.choice([2, 5, 10]),
            idempotency_key = f"demo-run-{i}",   # deterministic for replay safety
            tags            = ["demo", job_type],
        )

        job_id = await qm.enqueue(job)
        logger.debug("Enqueued job %d/%d: %s → %s", i + 1, num_jobs, job_type, job_id)
        await asyncio.sleep(interval)

    logger.info("Producer finished enqueueing %d jobs", num_jobs)


# =============================================================================
# Main
# =============================================================================

async def main():
    # 1. Init DB
    await init_pool()
    await apply_schema("schema.sql")
    logger.info("Database ready")

    qm = QueueManager()

    # 2. Create workers
    workers = []
    for i in range(config.num_workers):
        w = Worker(
            queue_manager = qm,
            queue_names   = ["default"],
            worker_id     = f"worker-{i+1:02d}",
        )
        # Register all handlers on every worker
        w.register_handler("send_email",        handle_send_email)
        w.register_handler("resize_image",      handle_resize_image)
        w.register_handler("generate_report",   handle_generate_report)
        w.register_handler("send_notification", handle_send_notification)
        w.register_handler("data_sync",         handle_data_sync)
        workers.append(w)

    # 3. Metrics collector
    metrics = MetricsCollector(qm, queue_name="default")

    # 4. Dashboard
    dash = Dashboard(metrics, qm)

    # 5. Gather all coroutines
    tasks = [
        # Workers
        *[asyncio.create_task(w.start()) for w in workers],
        # Metrics snapshots
        asyncio.create_task(
            metrics.start(config.queue.metrics_snapshot_interval_sec)
        ),
        # Producer (enqueue demo jobs)
        asyncio.create_task(producer(qm, num_jobs=300, rate_per_sec=8.0)),
        # Dashboard (blocks with Live display)
        asyncio.create_task(dash.run(refresh_sec=2.0)),
    ]

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("Shutting down…")
    finally:
        for t in tasks:
            t.cancel()
        for w in workers:
            await w.stop()
        metrics.stop()
        await close_pool()
        logger.info("Clean shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())
