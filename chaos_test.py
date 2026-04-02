# chaos_test.py
# Fault-tolerance test suite. Proves: crash recovery, idempotency,
# exactly-once execution, and retry/dead-letter behavior.
# Run: python chaos_test.py

from __future__ import annotations

import asyncio
import logging
import time
import uuid

from database import apply_schema, close_pool, fetch_all, fetch_one, init_pool
from models import Job
from queue_manager import QueueManager
from worker import Worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

PASS = "PASS"
FAIL = "FAIL"


# ── Test 1: Idempotent enqueue ────────────────────────────────────────────────
async def test_idempotency(qm: QueueManager) -> str:
    logger.info("─" * 55)
    logger.info("TEST 1: Idempotent enqueue")
    logger.info("─" * 55)

    key = f"idem-{uuid.uuid4().hex[:8]}"

    id1 = await qm.enqueue(Job(job_type="fast_job", payload={},
                               queue_name="chaos_test", idempotency_key=key))
    id2 = await qm.enqueue(Job(job_type="fast_job", payload={},
                               queue_name="chaos_test", idempotency_key=key))
    id3 = await qm.enqueue(Job(job_type="fast_job", payload={},
                               queue_name="chaos_test", idempotency_key=key))

    if id1 == id2 == id3:
        logger.info("✓ PASS: All 3 enqueue calls returned same ID: %s", id1)
        return PASS
    else:
        logger.error("✗ FAIL: Got different IDs: %s %s %s", id1, id2, id3)
        return FAIL


# ── Test 2: Exactly-once across concurrent workers ────────────────────────────
async def test_exactly_once(qm: QueueManager) -> str:
    logger.info("─" * 55)
    logger.info("TEST 2: Exactly-once under %d concurrent workers", 5)
    logger.info("─" * 55)

    NUM_JOBS = 20
    NUM_WORKERS = 5
    queue = "exactly_once_test"

    # Clean slate
    from database import execute
    await execute(f"DELETE FROM jobs WHERE queue_name='{queue}'")

    job_ids = []
    for i in range(NUM_JOBS):
        jid = await qm.enqueue(Job(
            job_type="counted_job",
            payload={"seq": i},
            queue_name=queue,
            idempotency_key=f"eo-{i}-{uuid.uuid4().hex[:6]}",
        ))
        job_ids.append(str(jid))

    logger.info("Enqueued %d jobs", NUM_JOBS)

    async def handle_counted_job(payload):
        await asyncio.sleep(0.05)
        return {"executed": True}

    workers = []
    tasks = []
    for i in range(NUM_WORKERS):
        w = Worker(qm, queue_names=[queue], worker_id=f"eo-worker-{i}")
        w.register_handler("counted_job", handle_counted_job)
        workers.append(w)
        tasks.append(asyncio.create_task(w.start()))

    # Wait for all jobs to complete (up to 30s)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        row = await fetch_one(
            f"SELECT COUNT(*) AS n FROM jobs WHERE queue_name='{queue}' AND status='completed'"
        )
        if row["n"] >= NUM_JOBS:
            break
        await asyncio.sleep(1)

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    # Verify each completed exactly once
    rows = await fetch_all(
        f"""SELECT id, status,
               (SELECT COUNT(*) FROM job_events
                WHERE job_id=jobs.id AND event_type='completed') AS completions
           FROM jobs WHERE queue_name='{queue}'"""
    )

    failures = [r for r in rows if r["completions"] != 1]
    if failures:
        for f in failures:
            logger.error("✗ Job %s: completions=%d status=%s",
                         f["id"], f["completions"], f["status"])
        return f"FAIL: {len(failures)} jobs not completed exactly once"

    logger.info("✓ PASS: All %d jobs completed exactly once across %d workers",
                NUM_JOBS, NUM_WORKERS)
    return PASS


# ── Test 3: Dead letter after exhausted retries ───────────────────────────────
async def test_dead_letter(qm: QueueManager) -> str:
    logger.info("─" * 55)
    logger.info("TEST 3: Retry exhaustion → dead letter")
    logger.info("─" * 55)

    queue = "dead_letter_test"

    async def always_fails(payload):
        raise RuntimeError("This job always fails on purpose")

    job_id = await qm.enqueue(Job(
        job_type="failing_job",
        payload={},
        queue_name=queue,
        max_attempts=2,
        retry_delay_sec=1,
    ))
    logger.info("Enqueued always-failing job: %s", job_id)

    w = Worker(qm, queue_names=[queue], worker_id="dl-worker")
    w.register_handler("failing_job", always_fails)
    task = asyncio.create_task(w.start())

    # Wait for permanent failure
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        row = await fetch_one(
            "SELECT status, attempt_count FROM jobs WHERE id = $1", job_id
        )
        if row["status"] == "failed":
            break
        await asyncio.sleep(1)

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    row = await fetch_one(
        "SELECT status, attempt_count, last_error FROM jobs WHERE id = $1",
        job_id,
    )

    if row["status"] != "failed":
        return f"FAIL: Expected status=failed, got {row['status']}"
    if row["attempt_count"] < 2:
        return f"FAIL: Expected >= 2 attempts, got {row['attempt_count']}"
    if "always fails" not in (row["last_error"] or ""):
        return "FAIL: Error message not stored"

    logger.info("✓ PASS: Job failed after %d attempts, error stored",
                row["attempt_count"])
    return PASS


# ── Test 4: Priority ordering ─────────────────────────────────────────────────
async def test_priority_ordering(qm: QueueManager) -> str:
    logger.info("─" * 55)
    logger.info("TEST 4: Priority ordering (lower number = higher priority)")
    logger.info("─" * 55)

    queue = "priority_test"
    from database import execute
    await execute(f"DELETE FROM jobs WHERE queue_name='{queue}'")

    execution_order = []

    async def handle_priority_job(payload):
        execution_order.append(payload["priority_label"])
        return {}

    # Enqueue in reverse priority order (low priority first)
    await qm.enqueue(Job(job_type="pj", payload={"priority_label": "LOW"},
                         queue_name=queue, priority=9))
    await qm.enqueue(Job(job_type="pj", payload={"priority_label": "MED"},
                         queue_name=queue, priority=5))
    await qm.enqueue(Job(job_type="pj", payload={"priority_label": "HIGH"},
                         queue_name=queue, priority=1))

    w = Worker(qm, queue_names=[queue], worker_id="priority-worker")
    w.register_handler("pj", handle_priority_job)
    task = asyncio.create_task(w.start())

    # Wait for all 3 jobs
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if len(execution_order) >= 3:
            break
        await asyncio.sleep(0.2)

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    if execution_order != ["HIGH", "MED", "LOW"]:
        return f"FAIL: Expected HIGH→MED→LOW, got {execution_order}"

    logger.info("✓ PASS: Jobs executed in priority order: %s", execution_order)
    return PASS


# ── Test 5: Lease renewal keeps long job alive ────────────────────────────────
async def test_lease_renewal(qm: QueueManager) -> str:
    logger.info("─" * 55)
    logger.info("TEST 5: Lease renewal keeps long-running job alive")
    logger.info("─" * 55)

    queue = "lease_renewal_test"

    # Job that runs longer than a single lease window
    # With lease_duration=30s and renewal every 10s, a 25s job should complete
    async def handle_long_job(payload):
        await asyncio.sleep(25)
        return {"survived": True}

    job_id = await qm.enqueue(Job(
        job_type="long_job",
        payload={},
        queue_name=queue,
    ))

    w = Worker(qm, queue_names=[queue], worker_id="long-job-worker")
    w.register_handler("long_job", handle_long_job)
    task = asyncio.create_task(w.start())

    # Wait up to 40s
    deadline = time.monotonic() + 40
    result_status = None
    while time.monotonic() < deadline:
        row = await fetch_one(
            "SELECT status FROM jobs WHERE id = $1", job_id
        )
        if row["status"] in ("completed", "failed"):
            result_status = row["status"]
            break
        await asyncio.sleep(2)

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    if result_status != "completed":
        return f"FAIL: Expected completed, got {result_status} (lease not renewed?)"

    logger.info("✓ PASS: Long job (25s) completed without lease expiry")
    return PASS


# ── Test 6: Crash recovery (requires short lease in config) ──────────────────
async def test_crash_recovery(qm: QueueManager) -> str:
    logger.info("─" * 55)
    logger.info("TEST 6: Worker crash recovery")
    logger.info("  NOTE: Set lease_duration_sec=8, recovery_interval_sec=5")
    logger.info("        in config.py for this test to run fast")
    logger.info("─" * 55)

    queue = "crash_recovery_test"

    async def handle_never_finishes(payload):
        # This simulates a job that a crashed worker claimed but never finished
        await asyncio.sleep(9999)
        return {}

    job_id = await qm.enqueue(Job(
        job_type="crash_job",
        payload={},
        queue_name=queue,
    ))

    # Start victim worker — will claim the job
    victim = Worker(qm, queue_names=[queue], worker_id="victim-worker")
    victim.register_handler("crash_job", handle_never_finishes)
    victim_task = asyncio.create_task(victim.start())

    # Let it claim
    await asyncio.sleep(3)
    row = await fetch_one("SELECT status, worker_id FROM jobs WHERE id = $1", job_id)
    if row["status"] not in ("claimed", "running"):
        victim_task.cancel()
        await asyncio.gather(victim_task, return_exceptions=True)
        return f"FAIL: Job not claimed within 3s (status={row['status']})"

    logger.info("✓ Job claimed by victim-worker")

    # CRASH the victim (hard cancel, no cleanup)
    victim_task.cancel()
    await asyncio.gather(victim_task, return_exceptions=True)
    logger.info("✓ Victim worker killed (simulated crash)")

    # Start recovery worker
    async def handle_crash_job_fast(payload):
        return {"recovered": True}

    recovery = Worker(qm, queue_names=[queue], worker_id="recovery-worker")
    recovery.register_handler("crash_job", handle_crash_job_fast)
    recovery_task = asyncio.create_task(recovery.start())

    # Wait for recovery (up to 60s)
    deadline = time.monotonic() + 60
    recovered = False
    while time.monotonic() < deadline:
        row = await fetch_one(
            "SELECT status, worker_id FROM jobs WHERE id = $1", job_id
        )
        if row["status"] == "completed":
            logger.info("✓ Job recovered and completed by %s", row["worker_id"])
            recovered = True
            break
        await asyncio.sleep(2)

    recovery_task.cancel()
    await asyncio.gather(recovery_task, return_exceptions=True)

    if not recovered:
        return "FAIL: Job not recovered within 60s"

    logger.info("✓ PASS: Crash recovery confirmed")
    return PASS


# ── Main runner ───────────────────────────────────────────────────────────────
async def main():
    await init_pool()
    await apply_schema("schema.sql")

    qm = QueueManager()

    tests = [
        ("idempotency",     test_idempotency),
        ("exactly_once",    test_exactly_once),
        ("dead_letter",     test_dead_letter),
        ("priority_order",  test_priority_ordering),
        ("lease_renewal",   test_lease_renewal),
        # Uncomment after setting lease_duration_sec=8 in config.py:
        # ("crash_recovery",  test_crash_recovery),
    ]

    results = {}
    for name, fn in tests:
        try:
            results[name] = await fn(qm)
        except Exception as e:
            logger.exception("Test %s raised an exception", name)
            results[name] = f"FAIL: {e}"

    # Summary
    logger.info("")
    logger.info("=" * 55)
    logger.info("CHAOS TEST RESULTS")
    logger.info("=" * 55)
    passed = sum(1 for v in results.values() if v == PASS)
    total  = len(results)
    for name, result in results.items():
        icon = "✓" if result == PASS else "✗"
        logger.info("%s %-22s %s", icon, name, result)
    logger.info("")
    logger.info("Score: %d / %d", passed, total)

    await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
