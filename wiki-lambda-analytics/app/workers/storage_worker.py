"""
StorageWorker: consumes WikiEvent instances from its dispatcher queue and
batches them into DuckDB via the batch/storage layer.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from typing import List

from app.config.settings import settings
from app.models.wiki_event import WikiEvent
from app.storage.database import DuckDBWrapper

logger = logging.getLogger(__name__)


class StorageWorker(threading.Thread):
    """Background thread that drains an event queue into DuckDB in batches."""

    def __init__(
        self,
        inbound_queue: "queue.Queue[WikiEvent]",
        db: DuckDBWrapper,
        batch_size: int = settings.batch_insert_size,
        flush_interval: float = settings.batch_flush_interval_seconds,
    ) -> None:
        super().__init__(name="StorageWorker", daemon=True)
        self._queue = inbound_queue
        self._db = db
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._stop_event = threading.Event()
        self._total_inserted = 0

    def stop(self) -> None:
        self._stop_event.set()

    @property
    def total_inserted(self) -> int:
        return self._total_inserted

    def run(self) -> None:
        logger.info("StorageWorker started (batch_size=%d, flush_interval=%.1fs)",
                    self._batch_size, self._flush_interval)
        buffer: List[WikiEvent] = []
        last_flush = time.monotonic()

        while not self._stop_event.is_set():
            timeout = max(0.05, self._flush_interval - (time.monotonic() - last_flush))
            try:
                event = self._queue.get(timeout=timeout)
                buffer.append(event)
            except queue.Empty:
                pass

            should_flush = len(buffer) >= self._batch_size or (
                buffer and (time.monotonic() - last_flush) >= self._flush_interval
            )
            if should_flush:
                self._flush(buffer)
                buffer = []
                last_flush = time.monotonic()

        # Final flush on shutdown.
        if buffer:
            self._flush(buffer)
        logger.info("StorageWorker stopped. Total events inserted: %d", self._total_inserted)

    def _flush(self, buffer: List[WikiEvent]) -> None:
        if not buffer:
            return
        try:
            inserted = self._db.insert_batch(buffer)
            self._total_inserted += inserted
        except Exception:  # noqa: BLE001 - never let the worker thread die
            logger.exception("StorageWorker failed to flush a batch of %d events", len(buffer))
