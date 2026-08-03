"""
Identifies high-activity ("trending") articles inside the current sliding
window by comparing in-window edit counts against a historical baseline
supplied by the batch layer.

Trend Score = Current Window Edits / Historical Baseline

When an article has no (or a negligible) historical baseline, a
deterministic fallback threshold is applied instead (edits > N).
"""

from __future__ import annotations

from collections import Counter
from typing import Callable, Dict, List

from app.config.settings import settings
from app.models.live_metrics import TrendingArticle
from app.models.wiki_event import WikiEvent
from app.speed_layer.sliding_window import SlidingWindow

# A baseline provider is any callable that, given a list of article titles,
# returns a mapping of title -> historical average edits per window.
BaselineProvider = Callable[[List[str]], Dict[str, float]]


class TrendDetector:
    """Computes trend scores for articles active in the current window."""

    def __init__(
        self,
        window: SlidingWindow,
        baseline_provider: BaselineProvider | None = None,
        min_edits_fallback: int = settings.trend_min_edits_fallback,
        score_threshold: float = settings.trend_score_threshold,
        top_n: int = settings.trending_top_n,
    ) -> None:
        self.window = window
        self.baseline_provider = baseline_provider
        self.min_edits_fallback = min_edits_fallback
        self.score_threshold = score_threshold
        self.top_n = top_n

    def detect(self) -> List[TrendingArticle]:
        """Return the top-N trending articles currently active in the window."""
        events: List[WikiEvent] = self.window.snapshot()
        if not events:
            return []

        edit_counts: Counter[str] = Counter(e.title for e in events)
        titles = list(edit_counts.keys())

        baselines: Dict[str, float] = {}
        if self.baseline_provider is not None:
            try:
                baselines = self.baseline_provider(titles)
            except Exception:  # noqa: BLE001 - baseline lookup must never break trend detection
                baselines = {}

        results: List[TrendingArticle] = []
        for title, recent_edits in edit_counts.items():
            baseline = baselines.get(title, 0.0)

            if baseline and baseline > 0.5:
                trend_score = round(recent_edits / baseline, 4)
                is_trending = trend_score >= self.score_threshold
            else:
                # Deterministic fallback when historical data is minimal/absent.
                trend_score = float(recent_edits)
                is_trending = recent_edits > self.min_edits_fallback

            if is_trending:
                results.append(
                    TrendingArticle(
                        title=title,
                        recent_edits=recent_edits,
                        baseline=round(baseline, 4),
                        trend_score=trend_score,
                    )
                )

        results.sort(key=lambda a: a.trend_score, reverse=True)
        return results[: self.top_n]
