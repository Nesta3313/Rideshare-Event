# Rideshare Event Producer

A lightweight Python application that simulates real-time rideshare trip events and streams them to **Azure Event Hub**. Designed for Indianapolis trip data, it maintains 20+ concurrent trips at all times, applies realistic surge pricing, and displays a live console dashboard.

---

## Features

- **Realistic trip lifecycle**: `requested → accepted → started → completed` (or `cancelled`)
- **Concurrent simulation**: maintains ≥20 trips in flight simultaneously using `asyncio`
- **Surge pricing**: 1.5–3× multiplier during peak hours (7–9 AM, 5–7 PM UTC)
- **~10% cancellation rate** distributed across pre-acceptance and post-acceptance states
- **Azure Event Hub streaming** with batching and exponential-backoff reconnection
- **Local JSONL backup**: every event is written to `events_backup.jsonl` before sending
- **Rich live dashboard**: active trips, events sent, events/sec, connection status, last 5 events

---

## Project Structure

```
rideshare-producer/
├── main.py              # Entry point, asyncio orchestration, Rich dashboard
├── producer.py          # Azure Event Hub producer (batching + retry logic)
├── trip_simulator.py    # Trip state machine and concurrent pool manager
├── config.py            # Pydantic-style config loaded from .env
├── .env.example         # Template for environment variables
├── requirements.txt     # Python dependencies
└── README.md
```

---

## Setup

### 1. Prerequisites

- Python 3.11+
- An Azure Event Hubs namespace with an Event Hub created

### 2. Install dependencies

```bash
cd rideshare-producer
pip install -r requirements.txt
```

### 3. Configure environment

```bash
cp .env.example .env
```

Edit `.env` and fill in:

| Variable | Description |
|---|---|
| `EVENTHUB_CONNECTION_STRING` | Full connection string from Azure Portal → Event Hubs Namespace → Shared Access Policies |
| `EVENTHUB_NAME` | Name of the specific Event Hub (e.g. `rideshare-events`) |
| `MIN_CONCURRENT_TRIPS` | Minimum number of simultaneous trips (default: `20`) |
| `SEND_INTERVAL_SECONDS` | How often to flush buffered events to Azure (default: `2.5`) |
| `BACKUP_LOG_PATH` | Path for local JSONL backup (default: `events_backup.jsonl`) |
| `LOG_LEVEL` | Python log level: `DEBUG`, `INFO`, `WARNING`, `ERROR` (default: `INFO`) |

### 4. Run

```bash
python main.py
```

Press **Ctrl+C** to gracefully stop. All buffered events will be flushed before exit.

---

## Event Schema

```json
{
  "trip_id": "3f8a1c2d-...",
  "event_type": "trip_started",
  "timestamp": "2025-01-15T14:32:01.123456+00:00",
  "driver_id": "driver_042",
  "rider_id": "rider_317",
  "vehicle_type": "comfort",
  "pickup_location": {
    "name": "Indianapolis International Airport",
    "latitude": 39.7173,
    "longitude": -86.2944
  },
  "dropoff_location": {
    "name": "Downtown Indianapolis",
    "latitude": 39.7684,
    "longitude": -86.1581
  },
  "estimated_fare_usd": 24.50,
  "actual_fare_usd": null,
  "distance_miles": 12.3,
  "duration_minutes": 22,
  "surge_multiplier": 1.0,
  "driver_rating": null,
  "rider_rating": null,
  "payment_method": "credit_card",
  "city": "Indianapolis",
  "status": "in_progress"
}
```

`actual_fare_usd`, `driver_rating`, and `rider_rating` are populated only on `trip_completed` events.

---

## Fare Structure

| Vehicle | Base Fare | Per Mile |
|---------|-----------|----------|
| economy | $2.00 | $1.20 |
| comfort | $3.00 | $1.75 |
| xl      | $4.00 | $2.00 |
| premium | $6.00 | $3.50 |

All fares are multiplied by the `surge_multiplier`. Actual fare on completion varies ±10% from the estimate.

---

## Architecture

```
TripSimulator
  └─ _manage_pool()          ← tops up active trip count every second
       └─ run_trip()         ← state machine coroutine per trip
            └─ on_event()    ← pushes event to asyncio.Queue

main.py
  ├─ _consume_events()       ← reads queue → enqueues to RideshareProducer
  ├─ _flush_loop()           ← periodic flush every SEND_INTERVAL_SECONDS
  └─ _dashboard_loop()       ← Rich Live display

RideshareProducer
  ├─ enqueue()               ← buffers events
  ├─ flush()                 ← writes JSONL backup → sends batch to Event Hub
  └─ _reconnect()            ← exponential backoff on connection failure
```

---

## Troubleshooting

**`EVENTHUB_CONNECTION_STRING is not set`** — Ensure `.env` exists in the same directory as `main.py` and contains the correct variable.

**Events going to backup only** — Check the connection string and Event Hub name. The backup file `events_backup.jsonl` will contain all events produced while disconnected. Once reconnected, new events will stream normally (previously backed-up events are not replayed automatically).

**High memory usage** — Reduce `MIN_CONCURRENT_TRIPS` or increase `SEND_INTERVAL_SECONDS` to lower throughput.
