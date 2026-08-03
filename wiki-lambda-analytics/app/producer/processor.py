"""
Event filtering logic enforcing the platform's ingestion criteria:
event_type == 'edit', namespace == 0, bot == False.
"""

from __future__ import annotations

import logging

from app.config.settings import settings
from app.models.wiki_event import WikiEvent
from app.monitoring.stream_statistics import stream_statistics

logger = logging.getLogger(__name__)


class EventFilter:
    """Applies configured filter criteria to incoming WikiEvent instances."""

    def __init__(
        self,
        event_type: str = settings.filter_event_type,
        namespace: int = settings.filter_namespace,
        exclude_bots: bool = settings.filter_exclude_bots,
        wiki: str = settings.filter_wiki,
    ) -> None:
        self.event_type = event_type
        self.namespace = namespace
        self.exclude_bots = exclude_bots
        self.wiki = wiki

    def accepts(self, event: WikiEvent) -> bool:
        """Return True if the event passes all configured filter criteria."""
        if event.event_type != self.event_type:
            return False
        if event.namespace != self.namespace:
            return False
        if self.exclude_bots and event.bot:
            return False
        if self.wiki and event.wiki != self.wiki:
            return False
        return True

    def process(self, event: WikiEvent) -> bool:
        """
        Run an event through the filter, updating monitoring statistics.

        Returns True if the event was accepted, False if it was filtered out.
        """
        stream_statistics.record_received()
        if self.accepts(event):
            stream_statistics.record_accepted()
            return True
        stream_statistics.record_filtered()
        return False