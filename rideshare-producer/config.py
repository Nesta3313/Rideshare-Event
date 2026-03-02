"""
config.py — Application configuration loaded from environment variables.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Config:
    """Immutable application configuration."""

    # Azure Event Hub
    connection_string: str
    eventhub_name: str

    # Simulation tuning
    min_concurrent_trips: int = 20
    send_interval_seconds: float = 2.5

    # Local backup
    backup_log_path: str = "events_backup.jsonl"

    # Logging
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "Config":
        """Load and validate configuration from environment variables.

        Raises:
            ValueError: If required environment variables are missing.
        """
        connection_string = os.getenv("EVENTHUB_CONNECTION_STRING", "").strip()
        eventhub_name = os.getenv("EVENTHUB_NAME", "").strip()

        if not connection_string:
            raise ValueError(
                "EVENTHUB_CONNECTION_STRING is not set. "
                "Copy .env.example to .env and fill in your Azure credentials."
            )
        if not eventhub_name:
            raise ValueError(
                "EVENTHUB_NAME is not set. "
                "Copy .env.example to .env and fill in your Event Hub name."
            )

        return cls(
            connection_string=connection_string,
            eventhub_name=eventhub_name,
            min_concurrent_trips=int(os.getenv("MIN_CONCURRENT_TRIPS", "20")),
            send_interval_seconds=float(os.getenv("SEND_INTERVAL_SECONDS", "2.5")),
            backup_log_path=os.getenv("BACKUP_LOG_PATH", "events_backup.jsonl"),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        )


def setup_logging(log_level: str) -> logging.Logger:
    """Configure root logger and return the application logger.

    Args:
        log_level: Standard Python logging level string (e.g. "INFO").

    Returns:
        Configured logger instance.
    """
    numeric_level = getattr(logging, log_level, logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    # Suppress noisy Azure SDK logs unless debugging
    if numeric_level > logging.DEBUG:
        logging.getLogger("azure").setLevel(logging.WARNING)
        logging.getLogger("uamqp").setLevel(logging.WARNING)

    return logging.getLogger("rideshare")
