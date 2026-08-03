"""
Thread-safe DuckDB connection wrapper used by the batch (storage) layer.
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Any, Iterable, List, Sequence

import duckdb

from app.config.settings import settings
from app.models.wiki_event import WikiEvent

logger = logging.getLogger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS wiki_events (
    id BIGINT PRIMARY KEY DEFAULT nextval('wiki_events_seq'),
    timestamp TIMESTAMP NOT NULL,
    title VARCHAR NOT NULL,
    "user" VARCHAR,
    anonymous BOOLEAN,
    bot BOOLEAN,
    comment VARCHAR,
    event_type VARCHAR,
    namespace INTEGER,
    wiki VARCHAR,
    server_name VARCHAR
);
"""

_CREATE_SEQUENCE_SQL = "CREATE SEQUENCE IF NOT EXISTS wiki_events_seq START 1;"

_CREATE_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_wiki_events_title ON wiki_events(title);",
    "CREATE INDEX IF NOT EXISTS idx_wiki_events_timestamp ON wiki_events(timestamp);",
    "CREATE INDEX IF NOT EXISTS idx_wiki_events_user ON wiki_events(\"user\");",
]

_INSERT_SQL = """
INSERT INTO wiki_events
    (timestamp, title, "user", anonymous, bot, comment, event_type, namespace, wiki, server_name)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


class DuckDBWrapper:
    """
    A thread-safe wrapper around a single DuckDB connection.

    DuckDB connections are not safe for concurrent use from multiple threads
    without external synchronization, so all access is serialized behind a
    re-entrant lock.
    """

    def __init__(self, db_path: str = settings.duckdb_path) -> None:
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)) or ".", exist_ok=True)
        self._lock = threading.RLock()
        self._conn = duckdb.connect(database=db_path, read_only=False)
        self._initialize_schema()

    def _initialize_schema(self) -> None:
        with self._lock:
            self._conn.execute(_CREATE_SEQUENCE_SQL)
            self._conn.execute(_CREATE_TABLE_SQL)
            for stmt in _CREATE_INDEXES_SQL:
                self._conn.execute(stmt)
            logger.info("DuckDB schema initialized at %s", self.db_path)

    def insert_batch(self, events: Sequence[WikiEvent]) -> int:
        """Insert a batch of WikiEvent rows. Returns the number of rows inserted."""
        if not events:
            return 0
        rows: List[tuple] = [event.to_row() for event in events]
        with self._lock:
            try:
                self._conn.executemany(_INSERT_SQL, rows)
                return len(rows)
            except duckdb.Error as exc:
                logger.error("DuckDB batch insert failed (%d rows): %s", len(rows), exc)
                raise

    def query(self, sql: str, params: Iterable[Any] | None = None) -> List[tuple]:
        """Execute a read-only SQL query and return all resulting rows."""
        with self._lock:
            try:
                if params is not None:
                    result = self._conn.execute(sql, list(params))
                else:
                    result = self._conn.execute(sql)
                return result.fetchall()
            except duckdb.Error as exc:
                logger.error("DuckDB query failed: %s | SQL=%s", exc, sql)
                raise

    def query_df(self, sql: str, params: Iterable[Any] | None = None):
        """Execute a SQL query and return the result as a pandas DataFrame."""
        with self._lock:
            try:
                if params is not None:
                    result = self._conn.execute(sql, list(params))
                else:
                    result = self._conn.execute(sql)
                return result.fetchdf()
            except duckdb.Error as exc:
                logger.error("DuckDB query_df failed: %s | SQL=%s", exc, sql)
                raise

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass


# Module-level singleton, initialized lazily by application.py / service.py.
_db_instance: DuckDBWrapper | None = None
_db_instance_lock = threading.Lock()


def get_db() -> DuckDBWrapper:
    """Return the process-wide DuckDB wrapper singleton, creating it if needed."""
    global _db_instance
    if _db_instance is None:
        with _db_instance_lock:
            if _db_instance is None:
                _db_instance = DuckDBWrapper()
    return _db_instance
