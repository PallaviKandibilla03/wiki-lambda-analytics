"""
Data models describing real-time (speed layer) metrics and trending articles.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from pydantic import BaseModel, Field


class TrendingArticle(BaseModel):
    """A single article flagged as trending within the current sliding window."""

    title: str = Field(..., description="Article title.")
    recent_edits: int = Field(..., description="Number of edits within the current window.")
    baseline: float = Field(..., description="Historical baseline edit rate for comparison.")
    trend_score: float = Field(..., description="recent_edits / baseline (or fallback heuristic score).")


class LiveMetrics(BaseModel):
    """Snapshot of real-time analytics computed by the speed layer."""

    window_size: int = Field(..., description="Size of the sliding window in seconds.")
    total_events: int = Field(..., description="Total events currently held in the window.")
    unique_editors: int = Field(..., description="Count of distinct editors in the window.")
    active_articles: int = Field(..., description="Count of distinct articles edited in the window.")
    anonymous_ratio: float = Field(..., description="Fraction of window events made by anonymous editors.")
    events_per_second: float = Field(..., description="Average events/sec across the window.")
    trending_articles: List[TrendingArticle] = Field(
        default_factory=list, description="Top trending articles in the current window."
    )
    generated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp this metrics snapshot was generated.",
    )
