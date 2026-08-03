"""
Serving layer: coordinates the Speed Layer and Batch Layer, exposing a
single unified interface that the FastAPI application consumes.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from typing import Deque, Dict, List, Optional

import psutil

from app.batch_layer.batch_processor import BatchProcessor
from app.config.settings import settings
from app.models.live_metrics import LiveMetrics
from app.models.wiki_event import WikiEvent
from app.monitoring.stream_statistics import stream_statistics
from app.speed_layer.metrics import SpeedMetricsCalculator
from app.speed_layer.sliding_window import SlidingWindow
from app.speed_layer.trend_detector import TrendDetector
from app.storage.database import DuckDBWrapper
from app.workers.dispatcher import Dispatcher
from app.workers.speed_worker import SpeedWorker
from app.workers.storage_worker import StorageWorker

logger = logging.getLogger(__name__)


class RecentEventsBuffer:
    """Thread-safe fixed-size ring buffer of the most recently ingested events."""

    def __init__(self, maxlen: int = settings.recent_events_buffer_size) -> None:
        self._buffer: Deque[WikiEvent] = deque(maxlen=maxlen)
        self._lock = threading.Lock()

    def add(self, event: WikiEvent) -> None:
        with self._lock:
            self._buffer.append(event)

    def latest(self, n: int) -> List[WikiEvent]:
        with self._lock:
            items = list(self._buffer)
        return items[-n:][::-1]  # most recent first


class ServingLayerService:
    """
    Central coordination point for the platform. Wires together the
    dispatcher, workers, sliding window, and DuckDB batch store, and
    exposes the high-level read API consumed by FastAPI endpoints.
    """

    def __init__(
        self,
        db: DuckDBWrapper,
        window: SlidingWindow,
        dispatcher: Dispatcher,
    ) -> None:
        self.db = db
        self.window = window
        self.dispatcher = dispatcher
        self.batch_processor = BatchProcessor(db)
        self.speed_metrics = SpeedMetricsCalculator(window)
        self.trend_detector = TrendDetector(
            window=window,
            baseline_provider=self.batch_processor.historical_baseline,
        )
        self.recent_events = RecentEventsBuffer()
        self.storage_worker: Optional[StorageWorker] = None
        self.speed_worker: Optional[SpeedWorker] = None
        self._process = psutil.Process()
        self._start_time = time.time()

    def attach_workers(self, storage_worker: StorageWorker, speed_worker: SpeedWorker) -> None:
        """Register worker thread handles so health/metrics endpoints can inspect them."""
        self.storage_worker = storage_worker
        self.speed_worker = speed_worker

    def record_recent_event(self, event: WikiEvent) -> None:
        """Called by the dispatch loop so `/events` can serve the latest N events."""
        self.recent_events.add(event)

    # ------------------------------------------------------------------ #
    # Unified read API
    # ------------------------------------------------------------------ #

    def get_live_metrics(self) -> LiveMetrics:
        """Return current speed-layer metrics, including trending articles."""
        metrics = self.speed_metrics.compute()
        metrics.trending_articles = self.trend_detector.detect()
        return metrics

    def get_historical_metrics(self) -> Dict:
        """Return batch-layer historical/aggregate statistics."""
        return self.batch_processor.system_summary()

    def get_trending_articles(self) -> List[Dict]:
        """Return trending articles as plain dicts (for API/dashboard consumption)."""
        return [t.model_dump() for t in self.trend_detector.detect()]

    def get_edit_rate_history(self, bucket_minutes: int = 5, limit_buckets: int = 48) -> List[Dict]:
        return self.batch_processor.edit_rate_over_time(bucket_minutes, limit_buckets)

    def get_recent_events(self, n: int = 50) -> List[Dict]:
        """Return the last N ingested events (post-filter) as plain dicts."""
        n = max(1, min(n, settings.recent_events_buffer_size))
        return [
            {
                "timestamp": e.to_iso(),
                "title": e.title,
                "user": e.user,
                "anonymous": e.anonymous,
                "bot": e.bot,
                "comment": e.comment,
                "namespace": e.namespace,
                "wiki": e.wiki,
                "server_name": e.server_name,
            }
            for e in self.recent_events.latest(n)
        ]

    def get_system_health(self) -> Dict:
        """Return a consolidated health/status report across all pipeline stages."""
        stats = stream_statistics.snapshot()
        return {
            "status": "healthy" if stats["connected"] else "degraded",
            "producer": {
                "connected": stats["connected"],
                "total_received": stats["total_received"],
                "total_accepted": stats["total_accepted"],
                "total_filtered": stats["total_filtered"],
                "total_parse_errors": stats["total_parse_errors"],
                "total_reconnects": stats["total_reconnects"],
                "drop_rate": stats["drop_rate"],
                "seconds_since_last_event": stats["seconds_since_last_event"],
                "last_error": stats["last_error"],
            },
            "dispatcher": {
                "consumers": self.dispatcher.consumer_names(),
                "queue_sizes": self.dispatcher.queue_sizes(),
                "drop_counts": self.dispatcher.drop_counts(),
            },
            "speed_layer": {
                "window_size_seconds": self.window.window_size_seconds,
                "events_in_window": len(self.window),
                "worker_alive": bool(self.speed_worker and self.speed_worker.is_alive()),
                "total_processed": self.speed_worker.total_processed if self.speed_worker else 0,
            },
            "batch_layer": {
                "worker_alive": bool(self.storage_worker and self.storage_worker.is_alive()),
                "total_inserted": self.storage_worker.total_inserted if self.storage_worker else 0,
            },
            "uptime_seconds": round(time.time() - self._start_time, 2),
        }

    def get_system_metrics(self) -> Dict:
        """Return process-level resource metrics (CPU, memory) plus stream stats."""
        with self._process.oneshot():
            cpu_percent = self._process.cpu_percent(interval=0.1)
            mem_info = self._process.memory_info()
        return {
            "cpu_percent": cpu_percent,
            "memory_rss_mb": round(mem_info.rss / (1024 * 1024), 2),
            "memory_vms_mb": round(mem_info.vms / (1024 * 1024), 2),
            "stream_statistics": stream_statistics.snapshot(),
            "uptime_seconds": round(time.time() - self._start_time, 2),
        }


# Module-level singleton, wired up by application.py at startup.
_service_instance: Optional[ServingLayerService] = None


def set_service(instance: ServingLayerService) -> None:
    global _service_instance
    _service_instance = instance


def get_service() -> ServingLayerService:
    if _service_instance is None:
        raise RuntimeError(
            "ServingLayerService has not been initialized yet. "
            "Ensure application.py has completed startup before serving requests."
        )
    return _service_instance
