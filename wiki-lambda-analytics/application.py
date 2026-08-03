"""
Wiki Lambda Analytics - Main Execution Entry Point.

Spins up:
    1. The DuckDB batch store (schema initialization).
    2. The dispatcher and its registered worker queues.
    3. The StorageWorker (batch layer ingestion) and SpeedWorker (speed layer ingestion) threads.
    4. The SSE ingestion producer thread (Wikimedia EventStreams -> parse -> filter -> dispatch).
    5. The FastAPI service via Uvicorn.

Run with:
    python application.py

The Streamlit dashboard is a separate process; see README.md for how to
launch it alongside this service.
"""

from __future__ import annotations

import logging
import signal
import sys
import threading
from types import FrameType
from typing import Optional

import uvicorn

from app.config.settings import settings
from app.monitoring.stream_statistics import stream_statistics
from app.producer.connection import ResilientSSEReader
from app.producer.parser import EventParser
from app.producer.processor import EventFilter
from app.serving_layer.service import ServingLayerService, set_service
from app.speed_layer.sliding_window import sliding_window
from app.storage.database import get_db
from app.workers.dispatcher import dispatcher
from app.workers.speed_worker import SpeedWorker
from app.workers.storage_worker import StorageWorker

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("wiki_lambda_analytics")


class ProducerThread(threading.Thread):
    """
    Owns the SSE connection lifecycle and the parse -> filter -> dispatch
    pipeline. Runs the resilient reader loop and forwards each accepted
    WikiEvent to the dispatcher for fan-out to registered workers.
    """

    def __init__(self, service: ServingLayerService) -> None:
        super().__init__(name="ProducerThread", daemon=True)
        self._reader = ResilientSSEReader()
        self._parser = EventParser()
        self._filter = EventFilter()
        self._service = service
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()
        self._reader.stop()

    def run(self) -> None:
        logger.info("ProducerThread started, connecting to %s", settings.stream_url)
        for raw_payload in self._reader.events():
            if self._stop_event.is_set():
                break
            event = self._parser.parse(raw_payload)
            if event is None:
                continue
            if not self._filter.process(event):
                continue
            self._service.record_recent_event(event)
            dispatcher.dispatch(event)
        logger.info("ProducerThread stopped.")


def build_application() -> tuple[ServingLayerService, ProducerThread, StorageWorker, SpeedWorker]:
    """Construct and wire together every pipeline component. Does not start threads."""
    db = get_db()

    storage_queue = dispatcher.register("storage")
    speed_queue = dispatcher.register("speed")

    service = ServingLayerService(db=db, window=sliding_window, dispatcher=dispatcher)
    set_service(service)

    storage_worker = StorageWorker(inbound_queue=storage_queue, db=db)
    speed_worker = SpeedWorker(inbound_queue=speed_queue, window=sliding_window)
    service.attach_workers(storage_worker=storage_worker, speed_worker=speed_worker)

    producer_thread = ProducerThread(service=service)

    return service, producer_thread, storage_worker, speed_worker


def main() -> None:
    logger.info("=" * 70)
    logger.info("Wiki Lambda Analytics - starting up")
    logger.info("=" * 70)

    service, producer_thread, storage_worker, speed_worker = build_application()

    storage_worker.start()
    speed_worker.start()
    producer_thread.start()

    def _shutdown(signum: int, frame: Optional[FrameType]) -> None:  # noqa: ARG001
        logger.info("Received signal %s, shutting down gracefully...", signum)
        producer_thread.stop()
        stream_statistics.set_connected(False)
        storage_worker.stop()
        speed_worker.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    logger.info(
        "Pipeline online. FastAPI docs will be available at http://%s:%d/docs",
        settings.api_host if settings.api_host != "0.0.0.0" else "localhost",
        settings.api_port,
    )
    logger.info("Run the dashboard separately with: streamlit run app/dashboard/dashboard.py")

    uvicorn.run(
        "app.api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
        factory=False,
        app_dir=".",
    )


if __name__ == "__main__":
    main()
