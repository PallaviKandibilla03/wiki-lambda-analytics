"""
Thread-safe counters and health state describing the ingestion pipeline.

Used by the producer/processor to record throughput, and by the API's
`/metrics/system` and `/health` endpoints to report pipeline health.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class StreamStatistics:
    """Aggregated counters describing the health of the ingestion pipeline."""

    total_received: int = 0
    total_accepted: int = 0
    total_filtered: int = 0
    total_parse_errors: int = 0
    total_reconnects: int = 0
    connected: bool = False
    last_event_at: Optional[float] = None
    started_at: float = field(default_factory=time.time)
    last_error: Optional[str] = None

    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)

    def record_received(self) -> None:
        with self._lock:
            self.total_received += 1

    def record_accepted(self) -> None:
        with self._lock:
            self.total_accepted += 1
            self.last_event_at = time.time()

    def record_filtered(self) -> None:
        with self._lock:
            self.total_filtered += 1

    def record_parse_error(self, message: str) -> None:
        with self._lock:
            self.total_parse_errors += 1
            self.last_error = message

    def record_reconnect(self) -> None:
        with self._lock:
            self.total_reconnects += 1

    def set_connected(self, connected: bool) -> None:
        with self._lock:
            self.connected = connected

    def set_error(self, message: Optional[str]) -> None:
        with self._lock:
            self.last_error = message

    def uptime_seconds(self) -> float:
        with self._lock:
            return time.time() - self.started_at

    def seconds_since_last_event(self) -> Optional[float]:
        with self._lock:
            if self.last_event_at is None:
                return None
            return time.time() - self.last_event_at

    def snapshot(self) -> dict:
        """Return a plain-dict snapshot safe for JSON serialization."""
        with self._lock:
            drop_rate = 0.0
            if self.total_received > 0:
                drop_rate = round(
                    (self.total_filtered + self.total_parse_errors) / self.total_received, 4
                )
            return {
                "connected": self.connected,
                "total_received": self.total_received,
                "total_accepted": self.total_accepted,
                "total_filtered": self.total_filtered,
                "total_parse_errors": self.total_parse_errors,
                "total_reconnects": self.total_reconnects,
                "drop_rate": drop_rate,
                "uptime_seconds": round(time.time() - self.started_at, 2),
                "seconds_since_last_event": (
                    round(time.time() - self.last_event_at, 2) if self.last_event_at else None
                ),
                "last_error": self.last_error,
            }


# Module-level singleton shared across the producer/processor/API.
stream_statistics = StreamStatistics()
