"""
Fan-out dispatcher that forwards validated WikiEvent instances to any
number of registered listener queues (e.g. StorageWorker, SpeedWorker)
without blocking the producer thread.
"""

from __future__ import annotations

import logging
import queue
from typing import Dict, List

from app.config.settings import settings
from app.models.wiki_event import WikiEvent

logger = logging.getLogger(__name__)


class Dispatcher:
    """
    Registers named consumer queues and fans each incoming event out to all
    of them. `dispatch()` is non-blocking: if a consumer's queue is full,
    the event is dropped for that consumer (with a logged warning) rather
    than stalling the whole pipeline.
    """

    def __init__(self, queue_maxsize: int = settings.dispatcher_queue_maxsize) -> None:
        self.queue_maxsize = queue_maxsize
        self._queues: Dict[str, "queue.Queue[WikiEvent]"] = {}
        self._drop_counts: Dict[str, int] = {}

    def register(self, name: str) -> "queue.Queue[WikiEvent]":
        """Register a new named consumer and return its inbound queue."""
        if name in self._queues:
            return self._queues[name]
        q: "queue.Queue[WikiEvent]" = queue.Queue(maxsize=self.queue_maxsize)
        self._queues[name] = q
        self._drop_counts[name] = 0
        logger.info("Dispatcher registered consumer '%s'", name)
        return q

    def dispatch(self, event: WikiEvent) -> None:
        """Fan an event out to every registered consumer queue, non-blocking."""
        for name, q in self._queues.items():
            try:
                q.put_nowait(event)
            except queue.Full:
                self._drop_counts[name] += 1
                if self._drop_counts[name] % 100 == 1:
                    logger.warning(
                        "Consumer '%s' queue is full, dropping events (total dropped: %d)",
                        name,
                        self._drop_counts[name],
                    )

    def consumer_names(self) -> List[str]:
        return list(self._queues.keys())

    def queue_sizes(self) -> Dict[str, int]:
        return {name: q.qsize() for name, q in self._queues.items()}

    def drop_counts(self) -> Dict[str, int]:
        return dict(self._drop_counts)


# Module-level singleton shared between application.py and worker threads.
dispatcher = Dispatcher()
