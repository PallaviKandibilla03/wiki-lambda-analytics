"""
Executes analytical SQL queries directly over DuckDB for the batch layer.

Provides historical/aggregate statistics: total stored events, total
articles, total editors, top edited articles, bot vs human distribution,
namespace breakdown, and a per-article historical baseline edit rate used
by the speed layer's trend detector.
"""

from __future__ import annotations

import logging
from typing import Dict, List

from app.storage.database import DuckDBWrapper

logger = logging.getLogger(__name__)


class BatchProcessor:
    """Runs historical analytical queries against the DuckDB batch store."""

    def __init__(self, db: DuckDBWrapper) -> None:
        self.db = db

    def total_stored_events(self) -> int:
        rows = self.db.query("SELECT COUNT(*) FROM wiki_events;")
        return int(rows[0][0]) if rows else 0

    def total_articles(self) -> int:
        rows = self.db.query("SELECT COUNT(DISTINCT title) FROM wiki_events;")
        return int(rows[0][0]) if rows else 0

    def total_editors(self) -> int:
        rows = self.db.query('SELECT COUNT(DISTINCT "user") FROM wiki_events;')
        return int(rows[0][0]) if rows else 0

    def top_edited_articles(self, limit: int = 20) -> List[Dict]:
        rows = self.db.query(
            """
            SELECT title, COUNT(*) AS edit_count
            FROM wiki_events
            GROUP BY title
            ORDER BY edit_count DESC
            LIMIT ?;
            """,
            [limit],
        )
        return [{"title": r[0], "edit_count": int(r[1])} for r in rows]

    def bot_vs_human_distribution(self) -> Dict[str, int]:
        rows = self.db.query(
            """
            SELECT bot, COUNT(*) AS cnt
            FROM wiki_events
            GROUP BY bot;
            """
        )
        distribution = {"bot": 0, "human": 0}
        for is_bot, cnt in rows:
            distribution["bot" if is_bot else "human"] = int(cnt)
        return distribution

    def namespace_breakdown(self) -> List[Dict]:
        rows = self.db.query(
            """
            SELECT namespace, COUNT(*) AS cnt
            FROM wiki_events
            GROUP BY namespace
            ORDER BY cnt DESC;
            """
        )
        return [{"namespace": int(r[0]), "count": int(r[1])} for r in rows]

    def edit_rate_over_time(self, bucket_minutes: int = 5, limit_buckets: int = 48) -> List[Dict]:
        """Historical edit volume bucketed by time, most recent last."""
        rows = self.db.query(
            f"""
            SELECT
                time_bucket(INTERVAL '{bucket_minutes} minutes', timestamp) AS bucket,
                COUNT(*) AS cnt
            FROM wiki_events
            GROUP BY bucket
            ORDER BY bucket DESC
            LIMIT ?;
            """,
            [limit_buckets],
        )
        rows.reverse()
        return [{"bucket": str(r[0]), "count": int(r[1])} for r in rows]

    def historical_baseline(self, titles: List[str], lookback_windows: int = 12) -> Dict[str, float]:
        """
        Compute a historical average-edits-per-window baseline for each of the
        given article titles, used by the speed layer's trend detector.

        The baseline is the average number of edits per `window_size`-sized
        bucket over the most recent `lookback_windows` buckets of history.
        """
        if not titles:
            return {}
        placeholders = ",".join(["?"] * len(titles))
        try:
            rows = self.db.query(
                f"""
                WITH recent AS (
                    SELECT title, timestamp
                    FROM wiki_events
                    WHERE title IN ({placeholders})
                )
                SELECT title, COUNT(*) AS total_edits,
                       COUNT(DISTINCT date_trunc('hour', timestamp)) AS hour_buckets
                FROM recent
                GROUP BY title;
                """,
                titles,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("historical_baseline query failed: %s", exc)
            return {}

        baselines: Dict[str, float] = {}
        for title, total_edits, hour_buckets in rows:
            buckets = max(hour_buckets, 1)
            baselines[title] = round(total_edits / buckets, 4)
        return baselines

    def system_summary(self) -> Dict:
        """A single consolidated dict of the most commonly needed batch stats."""
        return {
            "total_stored_events": self.total_stored_events(),
            "total_articles": self.total_articles(),
            "total_editors": self.total_editors(),
            "bot_vs_human": self.bot_vs_human_distribution(),
            "namespace_breakdown": self.namespace_breakdown(),
            "top_edited_articles": self.top_edited_articles(limit=10),
        }
