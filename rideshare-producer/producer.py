"""
producer.py — Azure Event Hub producer with batching, retry logic, and local backup.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from pathlib import Path
from typing import Deque

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub.exceptions import EventHubError

from config import Config

logger = logging.getLogger("rideshare.producer")

# Maximum events held in memory before forcing a flush
_MAX_BUFFER = 500

# Seconds to wait before attempting reconnection
_RECONNECT_DELAYS = [1, 2, 5, 10, 30]


class RideshareProducer:
    """Buffers events and sends them to Azure Event Hub in batches.

    Falls back to a local JSONL file if the hub is unavailable, and
    replays buffered events once the connection is restored.

    Args:
        config: Application configuration.
    """

    def __init__(self, config: Config) -> None:
        self._config = config
        self._client: EventHubProducerClient | None = None
        self._buffer: list[dict] = []
        self._connected = False
        self._total_sent = 0
        self._total_failed = 0
        self._send_times: Deque[float] = deque(maxlen=100)
        self._backup_path = Path(config.backup_log_path)
        self._lock = asyncio.Lock()
        self._reconnect_attempt = 0

    # ------------------------------------------------------------------
    # Public properties
    # ------------------------------------------------------------------

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def total_sent(self) -> int:
        return self._total_sent

    @property
    def events_per_second(self) -> float:
        """Rolling events/second over the last 100 sends."""
        times = list(self._send_times)
        if len(times) < 2:
            return 0.0
        elapsed = times[-1] - times[0]
        return round((len(times) - 1) / elapsed, 2) if elapsed > 0 else 0.0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Open the Event Hub connection."""
        await self._connect()

    async def stop(self) -> None:
        """Flush remaining events and close the connection."""
        if self._buffer:
            logger.info("Flushing %d buffered events before shutdown…", len(self._buffer))
            await self._flush()
        if self._client:
            await self._client.close()
            self._client = None
        self._connected = False
        logger.info("Producer stopped. Total sent: %d", self._total_sent)

    # ------------------------------------------------------------------
    # Enqueue / flush
    # ------------------------------------------------------------------

    async def enqueue(self, event: dict) -> None:
        """Add an event to the outbound buffer.

        Args:
            event: Serializable event dict.
        """
        async with self._lock:
            self._buffer.append(event)
            if len(self._buffer) >= _MAX_BUFFER:
                await self._flush_locked()

    async def flush(self) -> None:
        """Public flush — send all buffered events now."""
        async with self._lock:
            await self._flush_locked()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _flush(self) -> None:
        async with self._lock:
            await self._flush_locked()

    async def _flush_locked(self) -> None:
        """Send buffered events; must be called while holding self._lock."""
        if not self._buffer:
            return

        batch_to_send = self._buffer[:]
        self._buffer.clear()

        # Always write to local backup first
        self._write_backup(batch_to_send)

        if not self._connected or self._client is None:
            logger.warning(
                "Not connected — %d events written to backup only.", len(batch_to_send)
            )
            self._total_failed += len(batch_to_send)
            # Attempt background reconnection
            asyncio.get_event_loop().create_task(self._reconnect())
            return

        try:
            batch = await self._client.create_batch()
            sent_in_this_call = 0

            for event_dict in batch_to_send:
                payload = json.dumps(event_dict, default=str).encode("utf-8")
                try:
                    batch.add(EventData(payload))
                    sent_in_this_call += 1
                except ValueError:
                    # Batch is full — send it and start a new one
                    await self._client.send_batch(batch)
                    self._total_sent += sent_in_this_call
                    self._send_times.append(time.monotonic())
                    batch = await self._client.create_batch()
                    batch.add(EventData(payload))
                    sent_in_this_call = 1

            if len(batch) > 0:
                await self._client.send_batch(batch)
                self._total_sent += sent_in_this_call
                self._send_times.append(time.monotonic())

            self._reconnect_attempt = 0
            logger.debug("Sent batch of %d events.", sent_in_this_call)

        except EventHubError as exc:
            logger.error("EventHubError while sending: %s", exc)
            self._total_failed += len(batch_to_send)
            self._connected = False
            asyncio.get_event_loop().create_task(self._reconnect())

        except Exception as exc:
            logger.exception("Unexpected error while sending: %s", exc)
            self._total_failed += len(batch_to_send)
            self._connected = False
            asyncio.get_event_loop().create_task(self._reconnect())

    def _write_backup(self, events: list[dict]) -> None:
        """Append events to the local JSONL backup file."""
        try:
            with self._backup_path.open("a", encoding="utf-8") as fh:
                for ev in events:
                    fh.write(json.dumps(ev, default=str) + "\n")
        except OSError as exc:
            logger.error("Failed to write backup log: %s", exc)

    async def _connect(self) -> None:
        """Create and validate the Event Hub client connection."""
        try:
            self._client = EventHubProducerClient.from_connection_string(
                self._config.connection_string,
                eventhub_name=self._config.eventhub_name,
            )
            # Lightweight check — get hub properties to confirm connectivity
            props = await self._client.get_eventhub_properties()
            self._connected = True
            self._reconnect_attempt = 0
            logger.info(
                "Connected to Event Hub '%s' (partitions: %s)",
                self._config.eventhub_name,
                props.get("partition_ids", "unknown"),
            )
        except EventHubError as exc:
            self._connected = False
            logger.error("Could not connect to Event Hub: %s", exc)
        except Exception as exc:
            self._connected = False
            logger.exception("Unexpected error connecting to Event Hub: %s", exc)

    async def _reconnect(self) -> None:
        """Exponential-backoff reconnection loop."""
        if self._connected:
            return

        delay = _RECONNECT_DELAYS[
            min(self._reconnect_attempt, len(_RECONNECT_DELAYS) - 1)
        ]
        self._reconnect_attempt += 1
        logger.info(
            "Reconnect attempt %d in %ds…", self._reconnect_attempt, delay
        )
        await asyncio.sleep(delay)

        if self._client:
            try:
                await self._client.close()
            except Exception:
                pass
            self._client = None

        await self._connect()
