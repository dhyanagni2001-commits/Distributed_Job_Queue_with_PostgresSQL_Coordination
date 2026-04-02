# worker.py - Async worker with lease renewal, crash recovery, and job handlers

from __future__ import annotations

import asyncio
import logging
import os
import socket
import time
import traceback
import uuid
from typing import Any, Callable, Coroutine, Dict, Optional

from config import config
from models import ClaimedJob, JobResult
from queue_manager import QueueManager

logger = logging.getLogger(__name__)

# Type alias for job handler functions
JobHandler = Callable[[Dict[str, Any]], Coroutine[Any, Any, Optional[Dict[str, Any]]]]


class Worker:
    """
    A single async worker that:
    1. Polls for jobs using SELECT FOR UPDATE SKIP LOCKED
    2. Renews leases while executing (prevents phantom expiry)
    3. Reports success / failure / retry atomically
    4. Sends heartbeats to the workers table
    """

    def __init__(
        self,
        queue_manager: QueueManager,
        queue_names: list[str] | None = None,
        worker_id: str | None = None,
    ):
        self.qm           = queue_manager
        self.queue_names  = queue_names or config.queue.queue_names
        self.worker_id    = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.hostname     = socket.gethostname()
        self.pid          = os.getpid()

        self._handlers: Dict[str, JobHandler] = {}
        self._running   = False
        self._current_job: Optional[ClaimedJob] = None

        # Metrics counters (in-process)
        self.processed  = 0
        self.failed     = 0
        self.errors     = 0

    # ------------------------------------------------------------------
    # Handler registration
    # ------------------------------------------------------------------
    def register(self, job_type: str):
        """Decorator: @worker.register('send_email')"""
        def decorator(fn: JobHandler):
            self._handlers[job_type] = fn
            logger.info("Registered handler for job_type='%s'", job_type)
            return fn
        return decorator

    def register_handler(self, job_type: str, fn: JobHandler):
        self._handlers[job_type] = fn

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self):
        self._running = True
        await self.qm.register_worker(
            self.worker_id, self.queue_names, self.hostname, self.pid
        )
        logger.info("Worker %s started (queues=%s)", self.worker_id, self.queue_names)

        await asyncio.gather(
            self._poll_loop(),
            self._heartbeat_loop(),
            self._recovery_loop(),
        )

    async def stop(self):
        logger.info("Worker %s stopping...", self.worker_id)
        self._running = False
        await self.qm.deregister_worker(self.worker_id)

    # ------------------------------------------------------------------
    # Poll loop
    # ------------------------------------------------------------------
    async def _poll_loop(self):
        while self._running:
            try:
                claimed = await self.qm.claim_job(
                    self.worker_id,
                    self.queue_names,
                    lease_sec=config.queue.lease_duration_sec,
                )
                if claimed:
                    await self._execute(claimed)
                else:
                    await asyncio.sleep(config.queue.poll_interval_sec)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Unexpected error in poll loop for %s", self.worker_id)
                await asyncio.sleep(2)

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------
    async def _execute(self, job: ClaimedJob):
        self._current_job = job
        await self.qm.mark_running(job.job_id, self.worker_id)
        await self.qm.heartbeat(self.worker_id, "busy", job.job_id)

        handler = self._handlers.get(job.job_type)
        if handler is None:
            await self._handle_failure(
                job,
                error=f"No handler registered for job_type='{job.job_type}'",
                duration=0,
            )
            return

        start = time.monotonic()
        lease_task = asyncio.create_task(self._lease_renewal_loop(job))

        try:
            result_data = await handler(job.payload)
            duration = time.monotonic() - start
            lease_task.cancel()

            job_result = JobResult(
                job_id=job.job_id,
                success=True,
                result=result_data or {},
                duration_sec=duration,
            )
            await self.qm.complete_job(job_result)
            await self.qm.increment_worker_stats(self.worker_id, success=True)
            self.processed += 1

            logger.info(
                "✓ Job %s [%s] completed in %.3fs (attempt %d)",
                job.job_id, job.job_type, duration, job.attempt_count,
            )

        except Exception as exc:
            duration = time.monotonic() - start
            lease_task.cancel()
            error_msg = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
            await self._handle_failure(job, error=error_msg, duration=duration)

        finally:
            self._current_job = None
            await self.qm.heartbeat(self.worker_id, "idle", None)

    async def _handle_failure(self, job: ClaimedJob, error: str, duration: float):
        self.failed += 1
        await self.qm.increment_worker_stats(self.worker_id, success=False)

        job_result = JobResult(
            job_id=job.job_id,
            success=False,
            error=error,
            duration_sec=duration,
        )
        await self.qm.fail_job(job_result, job.max_attempts)
        logger.warning(
            "✗ Job %s [%s] failed (attempt %d/%d): %s",
            job.job_id, job.job_type, job.attempt_count, job.max_attempts,
            error[:200],
        )

    # ------------------------------------------------------------------
    # Lease renewal (keeps long-running jobs alive)
    # ------------------------------------------------------------------
    async def _lease_renewal_loop(self, job: ClaimedJob):
        interval = config.queue.lease_renewal_interval_sec
        while True:
            await asyncio.sleep(interval)
            try:
                ok = await self.qm.renew_lease(
                    job.job_id, self.worker_id,
                    lease_sec=config.queue.lease_duration_sec,
                )
                if ok:
                    logger.debug("Lease renewed for job %s", job.job_id)
                else:
                    logger.warning("Could not renew lease for job %s - stopping renewal", job.job_id)
                    break
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Lease renewal error for job %s", job.job_id)

    # ------------------------------------------------------------------
    # Heartbeat loop
    # ------------------------------------------------------------------
    async def _heartbeat_loop(self):
        while self._running:
            try:
                status = "busy" if self._current_job else "idle"
                current = self._current_job.job_id if self._current_job else None
                await self.qm.heartbeat(self.worker_id, status, current)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Heartbeat error for %s", self.worker_id)
            await asyncio.sleep(config.queue.heartbeat_interval_sec)

    # ------------------------------------------------------------------
    # Recovery loop (runs on every worker to handle expired leases)
    # ------------------------------------------------------------------
    async def _recovery_loop(self):
        while self._running:
            await asyncio.sleep(config.queue.recovery_interval_sec)
            try:
                recovered = await self.qm.recover_expired_leases()
                dead      = await self.qm.process_dead_letters()
                if recovered or dead:
                    logger.info("Recovery: %d leases recovered, %d dead-lettered", recovered, dead)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Recovery loop error on %s", self.worker_id)