# metrics.py - Metrics collection, snapshotting, and rich terminal dashboard

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

from database import acquire
from models import MetricsSnapshot, QueueStats
from queue_manager import QueueManager

logger = logging.getLogger(__name__)


class MetricsCollector:
    """
    Periodically snaps queue stats to metrics_snapshots table
    and keeps an in-memory ring buffer for the terminal dashboard.
    """

    BUFFER_SIZE = 120  # keep last 120 snapshots (~10 min at 5s interval)

    def __init__(self, queue_manager: QueueManager, queue_name: str = "default"):
        self.qm         = queue_manager
        self.queue_name = queue_name
        self._snapshots: List[MetricsSnapshot] = []
        self._running   = False

    async def start(self, interval_sec: float = 5.0):
        self._running = True
        logger.info("MetricsCollector started (interval=%.1fs)", interval_sec)
        while self._running:
            try:
                snap = await self._capture()
                self._snapshots.append(snap)
                if len(self._snapshots) > self.BUFFER_SIZE:
                    self._snapshots.pop(0)
                await self._persist(snap)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Metrics capture error")
            await asyncio.sleep(interval_sec)

    def stop(self):
        self._running = False

    @property
    def latest(self) -> Optional[MetricsSnapshot]:
        return self._snapshots[-1] if self._snapshots else None

    @property
    def history(self) -> List[MetricsSnapshot]:
        return list(self._snapshots)

    # ------------------------------------------------------------------
    async def _capture(self) -> MetricsSnapshot:
        stats = await self.qm.get_queue_stats(self.queue_name)
        now   = datetime.now(timezone.utc)
        return MetricsSnapshot(
            captured_at        = now,
            queue_name         = stats.queue_name,
            pending            = stats.pending,
            running            = stats.running,
            completed          = stats.completed,
            failed             = stats.failed,
            dead_letter        = stats.dead_letter,
            active_workers     = stats.active_workers,
            throughput_per_min = stats.throughput_per_min,
            avg_wait_sec       = stats.avg_wait_sec,
            avg_exec_sec       = stats.avg_exec_sec,
            p95_exec_sec       = stats.p95_exec_sec,
            p99_exec_sec       = stats.p99_exec_sec,
        )

    async def _persist(self, snap: MetricsSnapshot):
        async with acquire() as conn:
            await conn.execute(
                """
                INSERT INTO metrics_snapshots (
                    captured_at, queue_name, pending_count, running_count,
                    completed_count, failed_count, dead_letter_count,
                    active_workers, throughput_per_min,
                    avg_wait_sec, avg_exec_sec, p95_exec_sec, p99_exec_sec
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
                """,
                snap.captured_at, snap.queue_name,
                snap.pending, snap.running, snap.completed,
                snap.failed, snap.dead_letter, snap.active_workers,
                snap.throughput_per_min, snap.avg_wait_sec,
                snap.avg_exec_sec, snap.p95_exec_sec, snap.p99_exec_sec,
            )