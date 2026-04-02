# dashboard.py - Rich terminal dashboard with live metrics

from __future__ import annotations

import asyncio
import statistics
from datetime import datetime, timezone
from typing import List, Optional

from rich import box
from rich.columns import Columns
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich.text import Text

from metrics import MetricsCollector
from models import MetricsSnapshot
from queue_manager import QueueManager

console = Console()


def _fmt(val: Optional[float], unit: str = "", digits: int = 2) -> str:
    if val is None:
        return "[dim]—[/dim]"
    return f"{val:.{digits}f}{unit}"


def build_status_panel(snap: MetricsSnapshot) -> Panel:
    grid = Table.grid(expand=True, padding=(0, 2))
    grid.add_column(justify="left")
    grid.add_column(justify="right")

    def row(label, value, color="white"):
        grid.add_row(
            Text(label, style="dim"),
            Text(str(value), style=color),
        )

    row("Queue",           snap.queue_name,                     "cyan bold")
    row("Pending",         snap.pending,                         "yellow")
    row("Running",         snap.running,                         "blue")
    row("Completed",       snap.completed,                       "green")
    row("Failed",          snap.failed,                          "red")
    row("Dead Letter",     snap.dead_letter,                     "magenta")
    row("Active Workers",  snap.active_workers,                  "cyan")
    row("Throughput/min",  f"{snap.throughput_per_min:.1f}",     "green bold")

    return Panel(grid, title="[bold]Queue Status[/bold]", border_style="cyan", box=box.ROUNDED)


def build_latency_panel(snap: MetricsSnapshot) -> Panel:
    grid = Table.grid(expand=True, padding=(0, 2))
    grid.add_column(justify="left")
    grid.add_column(justify="right")

    def row(label, value):
        grid.add_row(Text(label, style="dim"), Text(value))

    row("Avg Wait",     _fmt(snap.avg_wait_sec, "s"))
    row("Avg Exec",     _fmt(snap.avg_exec_sec, "s"))
    row("p95 Exec",     _fmt(snap.p95_exec_sec, "s"))
    row("p99 Exec",     _fmt(snap.p99_exec_sec, "s"))

    return Panel(grid, title="[bold]Latency Metrics[/bold]", border_style="magenta", box=box.ROUNDED)


def build_sparkline(history: List[MetricsSnapshot], field: str, label: str, color: str) -> Panel:
    """ASCII sparkline for a time-series metric."""
    vals = [getattr(s, field) or 0 for s in history[-40:]]
    if not vals:
        return Panel("[dim]No data[/dim]", title=label)

    max_v = max(vals) or 1
    bars  = " ▁▂▃▄▅▆▇█"
    spark = ""
    for v in vals:
        idx = int((v / max_v) * (len(bars) - 1))
        spark += f"[{color}]{bars[idx]}[/{color}]"

    current = vals[-1]
    avg     = statistics.mean(vals)
    peak    = max(vals)

    info = f" [dim]cur[/dim] [{color}]{current:.1f}[/{color}]  [dim]avg[/dim] [{color}]{avg:.1f}[/{color}]  [dim]peak[/dim] [{color}]{peak:.1f}[/{color}]"
    return Panel(Text.from_markup(spark + "\n" + info), title=f"[bold]{label}[/bold]", box=box.ROUNDED)


def build_health_bar(snap: MetricsSnapshot) -> Panel:
    total = snap.pending + snap.running + snap.completed + snap.failed + snap.dead_letter
    if total == 0:
        return Panel("[dim]No jobs yet[/dim]", title="[bold]Job Health[/bold]", box=box.ROUNDED)

    p = Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=30),
        TextColumn("{task.completed}/{task.total}"),
        expand=False,
    )
    p.add_task("[green]Completed",   completed=snap.completed,   total=total)
    p.add_task("[yellow]Pending",    completed=snap.pending,      total=total)
    p.add_task("[blue]Running",      completed=snap.running,      total=total)
    p.add_task("[red]Failed",        completed=snap.failed,       total=total)
    p.add_task("[magenta]Dead Ltr",  completed=snap.dead_letter,  total=total)
    return Panel(p, title="[bold]Job Health Distribution[/bold]", box=box.ROUNDED)


def build_worker_table(workers) -> Panel:
    t = Table(box=box.SIMPLE, header_style="bold cyan", expand=True)
    t.add_column("Worker ID",   style="cyan",    no_wrap=True)
    t.add_column("Status",      style="white",   justify="center")
    t.add_column("Queues",      style="dim")
    t.add_column("Processed",   justify="right", style="green")
    t.add_column("Failed",      justify="right", style="red")
    t.add_column("Heartbeat",   justify="right", style="dim")

    status_colors = {
        "idle": "green", "busy": "yellow",
        "draining": "cyan", "dead": "red",
    }

    for w in workers:
        age = ""
        if w["last_heartbeat"]:
            hb = w["last_heartbeat"]
            if hb.tzinfo is None:
                hb = hb.replace(tzinfo=timezone.utc)
            delta = (datetime.now(timezone.utc) - hb).total_seconds()
            age   = f"{delta:.0f}s ago"

        color = status_colors.get(w["status"], "white")
        t.add_row(
            w["id"][:20],
            f"[{color}]{w['status']}[/{color}]",
            ", ".join(w["queue_names"]),
            str(w["jobs_processed"]),
            str(w["jobs_failed"]),
            age,
        )

    return Panel(t, title="[bold]Active Workers[/bold]", box=box.ROUNDED, border_style="blue")


def build_event_table(events) -> Panel:
    t = Table(box=box.SIMPLE, header_style="bold", expand=True)
    t.add_column("Time",       style="dim",    no_wrap=True, width=12)
    t.add_column("Event",      width=14)
    t.add_column("Job Type",   style="cyan",   width=16)
    t.add_column("Job ID",     style="dim",    width=12)
    t.add_column("Message",    style="white")

    event_colors = {
        "enqueued":      "white",
        "claimed":       "yellow",
        "started":       "blue",
        "completed":     "green",
        "failed":        "red",
        "retried":       "magenta",
        "dead_lettered": "red bold",
        "lease_renewed": "dim",
    }

    for ev in (events or [])[:15]:
        ts    = ev["occurred_at"]
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        delta = (datetime.now(timezone.utc) - ts).total_seconds()
        age   = f"{delta:.0f}s"
        color = event_colors.get(ev["event_type"], "white")
        t.add_row(
            age,
            f"[{color}]{ev['event_type']}[/{color}]",
            ev["job_type"],
            str(ev["job_id"])[:8],
            (ev["message"] or "")[:60],
        )

    return Panel(t, title="[bold]Recent Events[/bold]", box=box.ROUNDED, border_style="yellow")


class Dashboard:
    def __init__(self, metrics: MetricsCollector, qm: QueueManager):
        self.metrics = metrics
        self.qm      = qm

    async def run(self, refresh_sec: float = 2.0):
        with Live(console=console, refresh_per_second=1/refresh_sec, screen=True) as live:
            while True:
                try:
                    snap    = self.metrics.latest
                    history = self.metrics.history
                    workers = await self.qm.get_all_workers()
                    events  = await self.qm.get_recent_events(20)

                    layout = Layout()
                    layout.split_column(
                        Layout(name="header",  size=3),
                        Layout(name="top",     size=12),
                        Layout(name="middle",  size=10),
                        Layout(name="bottom"),
                    )

                    # Header
                    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    layout["header"].update(
                        Panel(
                            f"[bold cyan]⚡ Distributed Job Queue[/bold cyan]  [dim]{now}[/dim]  "
                            f"[dim]queue=[/dim][cyan]{self.metrics.queue_name}[/cyan]",
                            box=box.HEAVY,
                        )
                    )

                    if snap:
                        layout["top"].split_row(
                            Layout(build_status_panel(snap)),
                            Layout(build_latency_panel(snap)),
                            Layout(build_health_bar(snap)),
                        )
                        layout["middle"].split_row(
                            Layout(build_sparkline(history, "throughput_per_min",
                                                   "Throughput / min", "green")),
                            Layout(build_sparkline(history, "pending",
                                                   "Pending Jobs", "yellow")),
                            Layout(build_sparkline(history, "avg_exec_sec",
                                                   "Avg Exec (s)", "magenta")),
                        )
                    else:
                        layout["top"].update(Panel("[dim]Waiting for first snapshot…[/dim]"))
                        layout["middle"].update(Panel(""))

                    layout["bottom"].split_row(
                        Layout(build_worker_table(workers)),
                        Layout(build_event_table(events)),
                    )

                    live.update(layout)
                    await asyncio.sleep(refresh_sec)

                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    console.print_exception()
                    await asyncio.sleep(refresh_sec)