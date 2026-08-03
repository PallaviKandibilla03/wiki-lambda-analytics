"""
SpeedWorker: consumes WikiEvent instances from its dispatcher queue and
appends them to the Speed Layer's sliding window.
"""

from __future__ import annotations

import logging
import queue
import threading

from app.models.wiki_event import WikiEvent
from app.speed_layer.sliding_window import SlidingWindow

logger = logging.getLogger(__name__)


class SpeedWorker(threading.Thread):
    """Background thread that drains an event queue into the sliding window."""

    def __init__(self, inbound_queue: "queue.Queue[WikiEvent]", window: SlidingWindow) -> None:
        super().__init__(name="SpeedWorker", daemon=True)
        self._queue = inbound_queue
        self._window = window
        self._stop_event = threading.Event()
        self._total_processed = 0

    def stop(self) -> None:
        self._stop_event.set()

    @property
    def total_processed(self) -> int:
        return self._total_processed

    def run(self) -> None:
        logger.info("SpeedWorker started.")
        while not self._stop_event.is_set():
            try:
                event: WikiEvent = self._queue.get(timeout=0.5)
            except queue.Empty:
                # Purge periodically even when idle so the window stays fresh.
                self._window.purge()
                continue
            try:
                self._window.add(event)
                self._total_processed += 1
            except Exception:  # noqa: BLE001 - never let the worker thread die
                logger.exception("SpeedWorker failed to add event to sliding window")
        logger.info("SpeedWorker stopped. Total events processed: %d", self._total_processed)
