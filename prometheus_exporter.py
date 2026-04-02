# prometheus_exporter.py
# Exposes /metrics endpoint for Prometheus scraping.
# Run standalone: python prometheus_exporter.py
# Or via docker-compose as the metrics-exporter service.

from __future__ import annotations

import asyncio
import logging
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from typing import Optional

from database import close_pool, init_pool
from queue_manager import QueueManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

METRICS_PORT     = int(os.getenv("METRICS_PORT", "8000"))
SCRAPE_INTERVAL  = float(os.getenv("SCRAPE_INTERVAL_SEC", "15"))
QUEUE_NAME       = os.getenv("QUEUE_NAME", "default")

# Shared metrics dict — written by async loop, read by HTTP handler
_metrics: dict = {}


# ── Prometheus text format renderer ──────────────────────────────────────────

def _g(lines, name, help_text, value, labels=""):
    """Append a gauge metric in Prometheus text format."""
    if value is None:
        return
    lines.append(f"# HELP {name} {help_text}")
    lines.append(f"# TYPE {name} gauge")
    lstr = f"{{{labels}}}" if labels else ""
    lines.append(f"{name}{lstr} {float(value):.6f}")


def _c(lines, name, help_text, value, labels=""):
    """Append a counter metric in Prometheus text format."""
    if value is None:
        return
    lines.append(f"# HELP {name} {help_text}")
    lines.append(f"# TYPE {name} counter")
    lstr = f"{{{labels}}}" if labels else ""
    lines.append(f"{name}{lstr} {float(value):.0f}")


def format_prometheus(metrics: dict) -> str:
    lines = []
    q = f'queue="{QUEUE_NAME}"'

    # Queue depth
    _g(lines, "jobqueue_pending_total",
       "Jobs waiting to be claimed", metrics.get("pending"), q)
    _g(lines, "jobqueue_claimed_total",
       "Jobs claimed but not yet running", metrics.get("claimed"), q)
    _g(lines, "jobqueue_running_total",
       "Jobs currently executing", metrics.get("running"), q)

    # Outcome counters
    _c(lines, "jobqueue_completed_total",
       "Cumulative completed jobs", metrics.get("completed"), q)
    _c(lines, "jobqueue_failed_total",
       "Cumulative permanently failed jobs", metrics.get("failed"), q)
    _c(lines, "jobqueue_dead_letter_total",
       "Cumulative dead-lettered jobs", metrics.get("dead_letter"), q)

    # Throughput
    _g(lines, "jobqueue_throughput_per_minute",
       "Jobs completed in the last 60 seconds", metrics.get("throughput_per_min"), q)

    # Latency
    _g(lines, "jobqueue_avg_wait_seconds",
       "Average wait time in queue (seconds)", metrics.get("avg_wait_sec"), q)
    _g(lines, "jobqueue_avg_exec_seconds",
       "Average execution time (seconds)", metrics.get("avg_exec_sec"), q)
    _g(lines, "jobqueue_p95_exec_seconds",
       "p95 execution latency (seconds)", metrics.get("p95_exec_sec"), q)
    _g(lines, "jobqueue_p99_exec_seconds",
       "p99 execution latency (seconds)", metrics.get("p99_exec_sec"), q)

    # Workers
    _g(lines, "jobqueue_active_workers",
       "Workers with a recent heartbeat", metrics.get("active_workers"), q)

    # Per-worker metrics
    for w in metrics.get("workers", []):
        wlabel = f'worker="{w["id"]}"'
        _g(lines, "jobqueue_worker_jobs_processed",
           "Jobs processed by this worker", w.get("jobs_processed"), wlabel)
        _g(lines, "jobqueue_worker_jobs_failed",
           "Jobs failed by this worker", w.get("jobs_failed"), wlabel)
        alive = 1 if w.get("status") not in ("dead", None) else 0
        _g(lines, "jobqueue_worker_alive",
           "1 if worker heartbeat is recent", alive, wlabel)

    # Derived: failure rate
    completed = float(metrics.get("completed") or 0)
    failed    = float(metrics.get("failed") or 0)
    total = completed + failed
    failure_rate = (failed / total) if total > 0 else 0.0
    _g(lines, "jobqueue_failure_rate",
       "Ratio of failed to total finished jobs", failure_rate, q)

    return "\n".join(lines) + "\n"


# ── HTTP handler ──────────────────────────────────────────────────────────────

class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/metrics":
            body = format_prometheus(_metrics).encode()
            self.send_response(200)
            self.send_header("Content-Type",
                             "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok\n")

        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):
        pass  # suppress default noisy HTTP log


# ── Async collection loop ─────────────────────────────────────────────────────

async def collect_loop(qm: QueueManager):
    global _metrics
    while True:
        try:
            stats   = await qm.get_queue_stats(QUEUE_NAME)
            workers = await qm.get_all_workers()

            _metrics = {
                "pending":            stats.pending,
                "claimed":            stats.claimed,
                "running":            stats.running,
                "completed":          stats.completed,
                "failed":             stats.failed,
                "dead_letter":        stats.dead_letter,
                "active_workers":     stats.active_workers,
                "throughput_per_min": stats.throughput_per_min,
                "avg_wait_sec":       stats.avg_wait_sec,
                "avg_exec_sec":       stats.avg_exec_sec,
                "p95_exec_sec":       stats.p95_exec_sec,
                "p99_exec_sec":       stats.p99_exec_sec,
                "workers":            [dict(w) for w in workers],
            }
            logger.debug(
                "Metrics collected: pending=%d running=%d completed=%d failed=%d",
                stats.pending, stats.running, stats.completed, stats.failed,
            )

        except Exception:
            logger.exception("Failed to collect metrics — will retry")

        await asyncio.sleep(SCRAPE_INTERVAL)


# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    await init_pool()
    qm = QueueManager()

    server = HTTPServer(("0.0.0.0", METRICS_PORT), MetricsHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("Prometheus exporter running on http://0.0.0.0:%d/metrics",
                METRICS_PORT)
    logger.info("Scraping queue '%s' every %.0fs", QUEUE_NAME, SCRAPE_INTERVAL)

    try:
        await collect_loop(qm)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        server.shutdown()
        await close_pool()
        logger.info("Exporter shut down")


if __name__ == "__main__":
    asyncio.run(main())
