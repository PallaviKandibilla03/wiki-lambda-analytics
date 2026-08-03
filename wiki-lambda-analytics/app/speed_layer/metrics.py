"""
Computes real-time window metrics (total events, unique editors, active
articles, anonymous ratio, events/sec) from a SlidingWindow snapshot.
"""

from __future__ import annotations

from typing import List

from app.models.live_metrics import LiveMetrics
from app.models.wiki_event import WikiEvent
from app.speed_layer.sliding_window import SlidingWindow


class SpeedMetricsCalculator:
    """Derives LiveMetrics from a SlidingWindow's current contents."""

    def __init__(self, window: SlidingWindow) -> None:
        self.window = window

    def compute(self) -> LiveMetrics:
        """Compute and return a fresh LiveMetrics snapshot."""
        events: List[WikiEvent] = self.window.snapshot()
        total_events = len(events)

        if total_events == 0:
            return LiveMetrics(
                window_size=self.window.window_size_seconds,
                total_events=0,
                unique_editors=0,
                active_articles=0,
                anonymous_ratio=0.0,
                events_per_second=0.0,
                trending_articles=[],
            )

        unique_editors = len({e.user for e in events})
        active_articles = len({e.title for e in events})
        anonymous_count = sum(1 for e in events if e.anonymous)
        anonymous_ratio = round(anonymous_count / total_events, 4)
        events_per_second = round(total_events / self.window.window_size_seconds, 4)

        return LiveMetrics(
            window_size=self.window.window_size_seconds,
            total_events=total_events,
            unique_editors=unique_editors,
            active_articles=active_articles,
            anonymous_ratio=anonymous_ratio,
            events_per_second=events_per_second,
            trending_articles=[],  # populated separately by TrendDetector
        )
