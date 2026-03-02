"""
trip_simulator.py — Simulates realistic rideshare trip lifecycles.

Each Trip is an asyncio Task that advances through states:
  trip_requested → trip_accepted → trip_started → trip_completed
                                                 ↘ trip_cancelled  (≈10 %)
"""

from __future__ import annotations

import asyncio
import logging
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import AsyncIterator, Callable, Optional

logger = logging.getLogger("rideshare.simulator")

# ---------------------------------------------------------------------------
# Static reference data — Indianapolis
# ---------------------------------------------------------------------------

LOCATIONS: list[dict] = [
    {"name": "Indianapolis International Airport", "latitude": 39.7173, "longitude": -86.2944},
    {"name": "Downtown Indianapolis", "latitude": 39.7684, "longitude": -86.1581},
    {"name": "Lucas Oil Stadium", "latitude": 39.7601, "longitude": -86.1639},
    {"name": "Broad Ripple Village", "latitude": 39.8676, "longitude": -86.1428},
    {"name": "Indiana Convention Center", "latitude": 39.7657, "longitude": -86.1623},
    {"name": "Castleton Square Mall", "latitude": 39.8836, "longitude": -86.0617},
    {"name": "University of Indianapolis", "latitude": 39.7097, "longitude": -86.1736},
    {"name": "IUPUI Campus", "latitude": 39.7748, "longitude": -86.1758},
    {"name": "Keystone at the Crossing", "latitude": 39.8717, "longitude": -86.1001},
    {"name": "Mass Ave Arts District", "latitude": 39.7748, "longitude": -86.1482},
    {"name": "Fountain Square", "latitude": 39.7548, "longitude": -86.1482},
    {"name": "Circle Centre Mall", "latitude": 39.7673, "longitude": -86.1628},
    {"name": "Bankers Life Fieldhouse", "latitude": 39.7638, "longitude": -86.1555},
    {"name": "Indianapolis Zoo", "latitude": 39.7667, "longitude": -86.1767},
    {"name": "Crown Hill Cemetery", "latitude": 39.7983, "longitude": -86.1786},
    {"name": "Eagle Creek Park", "latitude": 39.8342, "longitude": -86.2592},
    {"name": "Speedway / IMS", "latitude": 39.7954, "longitude": -86.2353},
    {"name": "Noblesville Town Center", "latitude": 40.0456, "longitude": -86.0086},
    {"name": "Greenwood Park Mall", "latitude": 39.6131, "longitude": -86.1069},
    {"name": "Lawrence", "latitude": 39.8395, "longitude": -86.0253},
]

# Locations considered "downtown" — shorter expected trips
DOWNTOWN_NAMES: set[str] = {
    "Downtown Indianapolis",
    "Lucas Oil Stadium",
    "Indiana Convention Center",
    "Mass Ave Arts District",
    "Fountain Square",
    "Circle Centre Mall",
    "Bankers Life Fieldhouse",
    "Indianapolis Zoo",
}

AIRPORT_NAME = "Indianapolis International Airport"

VEHICLE_TYPES = ["economy", "comfort", "xl", "premium"]

# Base fare per mile by vehicle type (USD)
FARE_PER_MILE: dict[str, float] = {
    "economy": 1.20,
    "comfort": 1.75,
    "xl": 2.00,
    "premium": 3.50,
}

# Base booking fee by vehicle type (USD)
BASE_FARE: dict[str, float] = {
    "economy": 2.00,
    "comfort": 3.00,
    "xl": 4.00,
    "premium": 6.00,
}

PAYMENT_METHODS = ["credit_card", "debit_card", "cash", "wallet"]

# Peak-hour windows (hour ranges, inclusive) for surge pricing
PEAK_HOURS: list[tuple[int, int]] = [(7, 9), (17, 19)]

# State transition delays (seconds) — compressed for simulation speed
# In real life these would be minutes; here we use a 60× speed factor.
STATE_DELAYS: dict[str, tuple[float, float]] = {
    "trip_requested": (1.0, 3.0),    # waiting for driver acceptance
    "trip_accepted": (2.0, 5.0),     # driver en-route to pickup
    "trip_started": (5.0, 20.0),     # trip in progress
}

CANCELLATION_RATE = 0.10


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pick_location() -> dict:
    return dict(random.choice(LOCATIONS))


def _is_peak_hour() -> bool:
    hour = datetime.now(timezone.utc).hour
    return any(start <= hour <= end for start, end in PEAK_HOURS)


def _surge_multiplier() -> float:
    if _is_peak_hour():
        return round(random.uniform(1.5, 3.0), 2)
    return round(random.uniform(1.0, 1.3), 2)


def _estimate_distance(pickup: dict, dropoff: dict) -> float:
    """Rough distance estimate using lat/lon differences (≈ miles)."""
    lat_diff = abs(pickup["latitude"] - dropoff["latitude"])
    lon_diff = abs(pickup["longitude"] - dropoff["longitude"])
    # 1 degree lat ≈ 69 miles; 1 degree lon ≈ 53 miles at Indianapolis latitude
    raw = (lat_diff * 69.0) + (lon_diff * 53.0)
    # Airport trips are always longer; add road-route factor
    if pickup["name"] == AIRPORT_NAME or dropoff["name"] == AIRPORT_NAME:
        raw = max(raw, 8.0)
    elif pickup["name"] in DOWNTOWN_NAMES and dropoff["name"] in DOWNTOWN_NAMES:
        raw = max(raw, 0.5)
    return round(max(raw, 0.5), 2)


def _estimate_duration(distance_miles: float, vehicle: str) -> int:
    """Estimate trip duration in minutes."""
    # Average speed varies by vehicle type (mph)
    avg_speed = {"economy": 28, "comfort": 30, "xl": 28, "premium": 35}[vehicle]
    minutes = (distance_miles / avg_speed) * 60
    # Add pickup/dropoff buffer
    minutes += random.uniform(1.0, 4.0)
    return max(1, round(minutes))


def _calc_fare(distance: float, vehicle: str, surge: float) -> float:
    fare = BASE_FARE[vehicle] + (distance * FARE_PER_MILE[vehicle])
    return round(fare * surge, 2)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Trip data class
# ---------------------------------------------------------------------------

@dataclass
class Trip:
    """Holds the mutable state of a single simulated trip."""

    trip_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    driver_id: str = field(default_factory=lambda: f"driver_{random.randint(1, 50):03d}")
    rider_id: str = field(default_factory=lambda: f"rider_{random.randint(1, 500):03d}")
    vehicle_type: str = field(default_factory=lambda: random.choice(VEHICLE_TYPES))
    pickup_location: dict = field(default_factory=_pick_location)
    dropoff_location: dict = field(default_factory=_pick_location)
    payment_method: str = field(default_factory=lambda: random.choice(PAYMENT_METHODS))
    surge_multiplier: float = field(default_factory=_surge_multiplier)

    # Computed on post-init
    distance_miles: float = field(init=False)
    duration_minutes: int = field(init=False)
    estimated_fare_usd: float = field(init=False)

    # Mutable state
    event_type: str = field(default="trip_requested", init=False)
    status: str = field(default="requested", init=False)
    actual_fare_usd: Optional[float] = field(default=None, init=False)
    driver_rating: Optional[float] = field(default=None, init=False)
    rider_rating: Optional[float] = field(default=None, init=False)
    timestamp: str = field(default_factory=_now_iso, init=False)

    def __post_init__(self) -> None:
        # Ensure pickup != dropoff
        while self.dropoff_location["name"] == self.pickup_location["name"]:
            self.dropoff_location = _pick_location()

        self.distance_miles = _estimate_distance(self.pickup_location, self.dropoff_location)
        self.duration_minutes = _estimate_duration(self.distance_miles, self.vehicle_type)
        self.estimated_fare_usd = _calc_fare(
            self.distance_miles, self.vehicle_type, self.surge_multiplier
        )

    def to_event(self) -> dict:
        """Serialize current trip state to the canonical event schema."""
        self.timestamp = _now_iso()
        return {
            "trip_id": self.trip_id,
            "event_type": self.event_type,
            "timestamp": self.timestamp,
            "driver_id": self.driver_id,
            "rider_id": self.rider_id,
            "vehicle_type": self.vehicle_type,
            "pickup_location": self.pickup_location,
            "dropoff_location": self.dropoff_location,
            "estimated_fare_usd": self.estimated_fare_usd,
            "actual_fare_usd": self.actual_fare_usd,
            "distance_miles": self.distance_miles,
            "duration_minutes": self.duration_minutes,
            "surge_multiplier": self.surge_multiplier,
            "driver_rating": self.driver_rating,
            "rider_rating": self.rider_rating,
            "payment_method": self.payment_method,
            "city": "Indianapolis",
            "status": self.status,
        }


# ---------------------------------------------------------------------------
# Trip lifecycle coroutine
# ---------------------------------------------------------------------------

EventCallback = Callable[[dict], None]


async def run_trip(trip: Trip, on_event: EventCallback) -> None:
    """Advance a trip through all state transitions, firing on_event at each step.

    Args:
        trip: The Trip instance to simulate.
        on_event: Callback invoked synchronously with each event dict.
    """

    async def _emit(event_type: str, status: str) -> None:
        trip.event_type = event_type
        trip.status = status
        on_event(trip.to_event())

    # 1. Requested
    await _emit("trip_requested", "requested")
    await asyncio.sleep(random.uniform(*STATE_DELAYS["trip_requested"]))

    # 2. Accepted (or early cancellation)
    if random.random() < CANCELLATION_RATE / 2:
        await _emit("trip_cancelled", "cancelled")
        return

    await _emit("trip_accepted", "accepted")
    await asyncio.sleep(random.uniform(*STATE_DELAYS["trip_accepted"]))

    # 3. Started (or cancellation after acceptance)
    if random.random() < CANCELLATION_RATE / 2:
        await _emit("trip_cancelled", "cancelled")
        return

    await _emit("trip_started", "in_progress")
    await asyncio.sleep(random.uniform(*STATE_DELAYS["trip_started"]))

    # 4. Completed — fill in final fields
    # Add a small variance (±10 %) to actual fare
    variance = random.uniform(0.90, 1.10)
    trip.actual_fare_usd = round(trip.estimated_fare_usd * variance, 2)
    trip.driver_rating = round(random.uniform(3.5, 5.0), 1)
    trip.rider_rating = round(random.uniform(3.5, 5.0), 1)
    await _emit("trip_completed", "completed")


# ---------------------------------------------------------------------------
# Simulator — manages pool of concurrent trips
# ---------------------------------------------------------------------------

class TripSimulator:
    """Maintains a pool of concurrent trip coroutines and yields events."""

    def __init__(self, min_concurrent: int = 20) -> None:
        self._min_concurrent = min_concurrent
        self._pending_events: asyncio.Queue[dict] = asyncio.Queue()
        self._active_trips: dict[str, asyncio.Task] = {}
        self._total_completed = 0
        self._running = False

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def active_count(self) -> int:
        return len(self._active_trips)

    @property
    def total_completed(self) -> int:
        return self._total_completed

    async def events(self) -> AsyncIterator[dict]:
        """Async generator yielding events as they are produced."""
        self._running = True
        asyncio.get_event_loop().create_task(self._manage_pool())
        while self._running:
            event = await self._pending_events.get()
            yield event

    async def stop(self) -> None:
        self._running = False
        for task in list(self._active_trips.values()):
            task.cancel()
        self._active_trips.clear()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _on_event(self, event: dict) -> None:
        """Synchronous callback — puts the event on the queue."""
        self._pending_events.put_nowait(event)

    def _spawn_trip(self) -> None:
        trip = Trip()
        task = asyncio.get_event_loop().create_task(
            self._trip_wrapper(trip), name=f"trip-{trip.trip_id[:8]}"
        )
        self._active_trips[trip.trip_id] = task

    async def _trip_wrapper(self, trip: Trip) -> None:
        """Run a trip and remove it from the active pool when done."""
        try:
            await run_trip(trip, self._on_event)
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Unexpected error in trip %s", trip.trip_id)
        finally:
            self._active_trips.pop(trip.trip_id, None)
            self._total_completed += 1

    async def _manage_pool(self) -> None:
        """Continuously top up the active trips pool."""
        while self._running:
            deficit = self._min_concurrent - self.active_count
            for _ in range(max(deficit, 0)):
                self._spawn_trip()
            await asyncio.sleep(1.0)
