"""
Resilient Server-Sent Events (SSE) reader for the Wikimedia EventStreams API.

Implements exponential backoff on dropouts/errors and yields raw JSON
payload strings (the `data:` field of each SSE message) to the caller.
"""

from __future__ import annotations

import logging
import time
from typing import Iterator, Optional

import requests
import sseclient

from app.config.settings import settings
from app.monitoring.stream_statistics import stream_statistics

logger = logging.getLogger(__name__)


class SSEConnectionError(Exception):
    """Raised when the SSE connection cannot be established or is lost."""


class ResilientSSEReader:
    """
    Reads Server-Sent Events from a URL, automatically reconnecting with
    exponential backoff whenever the connection drops or an error occurs.
    """

    def __init__(
        self,
        url: str = settings.stream_url,
        initial_backoff: float = settings.reconnect_initial_backoff_seconds,
        max_backoff: float = settings.reconnect_max_backoff_seconds,
        backoff_multiplier: float = settings.reconnect_backoff_multiplier,
        read_timeout: float = settings.stream_read_timeout_seconds,
    ) -> None:
        self.url = url
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.backoff_multiplier = backoff_multiplier
        self.read_timeout = read_timeout
        self._stop_requested = False
        self._session: Optional[requests.Session] = None

    def stop(self) -> None:
        """Signal the reader loop to stop after the current iteration."""
        self._stop_requested = True
        if self._session is not None:
            try:
                self._session.close()
            except Exception:  # noqa: BLE001 - best-effort cleanup
                pass

    def _open_stream(self) -> sseclient.SSEClient:
        """Open a fresh HTTP streaming connection and wrap it in an SSE client."""
        self._session = requests.Session()
        response = self._session.get(
            self.url,
            stream=True,
            headers={
                "Accept": "text/event-stream",
                "User-Agent": "wiki-lambda-analytics/1.0 (educational streaming demo)",
            },
            timeout=self.read_timeout,
        )
        response.raise_for_status()
        return sseclient.SSEClient(response)

    def events(self) -> Iterator[str]:
        """
        Yield raw `data` payload strings from the SSE stream indefinitely,
        reconnecting with exponential backoff whenever the stream fails.
        """
        backoff = self.initial_backoff
        while not self._stop_requested:
            try:
                logger.info("Connecting to SSE stream at %s", self.url)
                client = self._open_stream()
                stream_statistics.set_connected(True)
                stream_statistics.set_error(None)
                backoff = self.initial_backoff  # reset after a successful connect

                for message in client.events():
                    if self._stop_requested:
                        break
                    if message.event and message.event != "message":
                        # Skip heartbeat/comment style events.
                        continue
                    if not message.data:
                        continue
                    yield message.data

                # Generator exhausted (server closed connection) -> reconnect.
                logger.warning("SSE stream closed by server, will reconnect.")

            except (requests.RequestException, ConnectionError, OSError) as exc:
                stream_statistics.set_connected(False)
                stream_statistics.set_error(str(exc))
                stream_statistics.record_reconnect()
                logger.warning("SSE connection error: %s. Reconnecting in %.1fs", exc, backoff)
            except Exception as exc:  # noqa: BLE001 - never let the producer thread die silently
                stream_statistics.set_connected(False)
                stream_statistics.set_error(str(exc))
                stream_statistics.record_reconnect()
                logger.exception("Unexpected error in SSE reader: %s", exc)
            finally:
                stream_statistics.set_connected(False)
                if self._session is not None:
                    try:
                        self._session.close()
                    except Exception:  # noqa: BLE001
                        pass

            if self._stop_requested:
                break

            time.sleep(backoff)
            backoff = min(backoff * self.backoff_multiplier, self.max_backoff)
