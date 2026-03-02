"""
main.py — Entry point for the Rideshare Event Producer.

Orchestrates the trip simulator, the Event Hub producer, and the live
Rich console dashboard.

Usage:
    python main.py
"""

from __future__ import annotations

import asyncio
import logging
import signal
from collections import deque
from datetime import datetime, timezone
from typing import Deque

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from config import Config, setup_logging
from producer import RideshareProducer
from trip_simulator import TripSimulator

# ---------------------------------------------------------------------------
# Console dashboard helpers
# ---------------------------------------------------------------------------

_RECENT_EVENTS_MAX = 5

# Colour per event type for the live feed
_EVENT_COLOURS: dict[str, str] = {
    "trip_requested": "cyan",
    "trip_accepted": "green",
    "trip_started": "yellow",
    "trip_completed": "bright_green",
    "trip_cancelled": "red",
}


def _build_dashboard(
    simulator: TripSimulator,
    producer: RideshareProducer,
    recent: Deque[dict],
    start_time: datetime,
) -> Layout:
    """Construct the Rich Layout that represents the live dashboard."""

    # --- Stats panel ---
    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    overall_eps = round(producer.total_sent / max(elapsed, 1), 2)

    stats = Table.grid(padding=(0, 2))
    stats.add_column(style="bold white", justify="right")
    stats.add_column(style="bright_white")

    conn_style = "bright_green" if producer.is_connected else "bright_red"
    conn_label = "CONNECTED" if producer.is_connected else "DISCONNECTED"

    stats.add_row("Event Hub:", Text(conn_label, style=conn_style))
    stats.add_row("Total Events Sent:", f"{producer.total_sent:,}")
    stats.add_row("Active Trips:", str(simulator.active_count))
    stats.add_row("Trips Completed:", f"{simulator.total_completed:,}")
    stats.add_row("Events / sec (rolling):", str(producer.events_per_second))
    stats.add_row("Events / sec (overall):", str(overall_eps))
    stats.add_row("Uptime:", _fmt_duration(elapsed))

    stats_panel = Panel(stats, title="[bold cyan]Live Stats[/]", border_style="cyan")

    # --- Recent events panel ---
    ev_table = Table(
        "Event Type", "Trip ID", "Driver", "Rider", "Vehicle", "Fare $", "Route",
        show_header=True,
        header_style="bold magenta",
        border_style="magenta",
        expand=True,
    )
    for ev in reversed(list(recent)):
        colour = _EVENT_COLOURS.get(ev["event_type"], "white")
        fare = (
            f"{ev['actual_fare_usd']:.2f}"
            if ev["actual_fare_usd"] is not None
            else f"~{ev['estimated_fare_usd']:.2f}"
        )
        ev_table.add_row(
            Text(ev["event_type"], style=colour),
            ev["trip_id"][:8] + "…",
            ev["driver_id"],
            ev["rider_id"],
            ev["vehicle_type"],
            fare,
            f"{ev['pickup_location']['name'][:20]} → {ev['dropoff_location']['name'][:20]}",
        )

    events_panel = Panel(
        ev_table,
        title=f"[bold magenta]Last {_RECENT_EVENTS_MAX} Events[/]",
        border_style="magenta",
    )

    layout = Layout()
    layout.split_column(
        Layout(stats_panel, size=12),
        Layout(events_panel),
    )
    return layout


def _fmt_duration(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


# ---------------------------------------------------------------------------
# Main async loop
# ---------------------------------------------------------------------------

async def run(config: Config) -> None:
    """Main coroutine — starts producer, simulator, and dashboard."""
    logger = logging.getLogger("rideshare.main")
    console = Console()

    simulator = TripSimulator(min_concurrent=config.min_concurrent_trips)
    producer = RideshareProducer(config)

    recent_events: Deque[dict] = deque(maxlen=_RECENT_EVENTS_MAX)
    start_time = datetime.now(timezone.utc)

    # Graceful shutdown on SIGINT / SIGTERM
    stop_event = asyncio.Event()

    def _handle_signal() -> None:
        logger.info("Shutdown signal received.")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    console.print(
        Panel.fit(
            "[bold cyan]Rideshare Event Producer[/]\n"
            "[dim]Indianapolis trip simulation → Azure Event Hub[/]",
            border_style="cyan",
        )
    )

    await producer.start()

    # Dashboard refresh task
    async def _dashboard_loop() -> None:
        with Live(console=console, refresh_per_second=2, screen=True) as live:
            while not stop_event.is_set():
                live.update(
                    _build_dashboard(simulator, producer, recent_events, start_time)
                )
                await asyncio.sleep(0.5)

    # Periodic flush task
    async def _flush_loop() -> None:
        while not stop_event.is_set():
            await asyncio.sleep(config.send_interval_seconds)
            await producer.flush()

    # Event consumption task — reads from simulator, feeds producer
    async def _consume_events() -> None:
        async for event in simulator.events():
            if stop_event.is_set():
                break
            recent_events.append(event)
            await producer.enqueue(event)

    tasks = [
        asyncio.create_task(_dashboard_loop(), name="dashboard"),
        asyncio.create_task(_flush_loop(), name="flush"),
        asyncio.create_task(_consume_events(), name="consume"),
    ]

    # Wait until a stop signal arrives
    await stop_event.wait()

    logger.info("Stopping simulator and producer…")
    await simulator.stop()

    # Cancel running tasks
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    # Final flush and shutdown
    await producer.stop()

    console.print("\n[bold green]Shutdown complete.[/] Goodbye!")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Load config, set up logging, and launch the async event loop."""
    try:
        config = Config.from_env()
    except ValueError as exc:
        # Config errors are user-facing; no stack trace needed
        print(f"\n[ERROR] {exc}\n")
        raise SystemExit(1) from exc

    logger = setup_logging(config.log_level)
    logger.info("Starting Rideshare Producer (min_concurrent=%d)", config.min_concurrent_trips)

    try:
        asyncio.run(run(config))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
