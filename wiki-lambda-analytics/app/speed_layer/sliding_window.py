"""
Thread-safe, time-bounded sliding window of recent WikiEvent instances.

Uses a deque of (event, arrival_time) pairs, purging expired entries as
time advances. Reads and writes are protected by a lock so the window can
be safely shared between the ingestion (speed) worker thread and any
number of reader threads (API request handlers).
"""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Deque, List, Tuple

from app.config.settings import settings
from app.models.wiki_event import WikiEvent


class SlidingWindow:
    """A time-bounded, thread-safe collection of recent WikiEvent instances."""

    def __init__(self, window_size_seconds: int = settings.window_size_seconds) -> None:
        self.window_size_seconds = window_size_seconds
        self._buffer: Deque[Tuple[float, WikiEvent]] = deque()
        self._lock = threading.Lock()

    def add(self, event: WikiEvent) -> None:
        """Add a new event to the window, stamped with the current monotonic time."""
        with self._lock:
            self._buffer.append((time.monotonic(), event))
            self._purge_locked()

    def _purge_locked(self) -> None:
        """Remove expired entries. Caller must already hold `self._lock`."""
        cutoff = time.monotonic() - self.window_size_seconds
        while self._buffer and self._buffer[0][0] < cutoff:
            self._buffer.popleft()

    def purge(self) -> None:
        """Public purge, safe to call periodically from a maintenance thread."""
        with self._lock:
            self._purge_locked()

    def snapshot(self) -> List[WikiEvent]:
        """Return a purged, point-in-time list copy of events currently in the window."""
        with self._lock:
            self._purge_locked()
            return [event for _, event in self._buffer]

    def __len__(self) -> int:
        with self._lock:
            self._purge_locked()
            return len(self._buffer)

    def resize(self, new_size_seconds: int) -> None:
        """Change the window size (e.g. from a config update) and purge immediately."""
        with self._lock:
            self.window_size_seconds = new_size_seconds
            self._purge_locked()


# Module-level singleton shared between the speed worker and the serving layer.
sliding_window = SlidingWindow()
