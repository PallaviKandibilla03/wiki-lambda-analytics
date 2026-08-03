"""
Centralized application configuration using Pydantic BaseSettings.

All tunable parameters for the Wiki Lambda Analytics platform are defined
here so that every module (producer, workers, speed layer, batch layer,
API, dashboard) reads from a single source of truth. Values can be
overridden via environment variables or a `.env` file.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application-wide configuration object."""

    model_config = SettingsConfigDict(
        env_prefix="WLA_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Wikimedia EventStreams source ---
    stream_url: str = Field(
        default="https://stream.wikimedia.org/v2/stream/recentchange",
        description="SSE endpoint for Wikimedia recent-changes stream.",
    )

    # --- Speed layer / sliding window ---
    window_size_seconds: int = Field(
        default=600, description="Sliding window size in seconds (default 5 minutes)."
    )

    # --- Storage ---
    duckdb_path: str = Field(
        default="./data/wiki.db", description="Filesystem path to the DuckDB database file."
    )
    batch_insert_size: int = Field(
        default=50, description="Number of events buffered before a batch insert to DuckDB."
    )
    batch_flush_interval_seconds: float = Field(
        default=2.0, description="Max seconds to wait before flushing a partial batch."
    )

    # --- Reconnect / backoff behavior for the SSE producer ---
    reconnect_initial_backoff_seconds: float = Field(
        default=2.0, description="Initial backoff delay after a stream disconnect."
    )
    reconnect_max_backoff_seconds: float = Field(
        default=60.0, description="Maximum backoff delay between reconnect attempts."
    )
    reconnect_backoff_multiplier: float = Field(
        default=2.0, description="Multiplier applied to backoff delay after each failed attempt."
    )
    stream_read_timeout_seconds: float = Field(
        default=30.0, description="Timeout for the underlying HTTP stream read."
    )

    # --- Event filter criteria ---
    filter_event_type: str = Field(default="edit", description="Only accept this event type.")
    filter_namespace: int = Field(default=0, description="Only accept this MediaWiki namespace.")
    filter_exclude_bots: bool = Field(default=True, description="Exclude bot-authored edits.")
    filter_exclude_bots: bool = Field(default=True, description="Exclude bot-authored edits.")
    filter_wiki: str = Field(default="enwiki", description="Only accept events from this wiki (e.g. 'enwiki'). Set to '' to disable.")
    
    # --- Trend detection ---
    trend_min_edits_fallback: int = Field(
        default=1,
        description="Deterministic fallback: flag an article as trending if its "
        "in-window edit count exceeds this threshold and no historical baseline exists.",
    )
    trend_score_threshold: float = Field(
        default=1.2, description="Minimum trend score (current/baseline) to be considered trending."
    )
    trending_top_n: int = Field(default=10, description="Number of trending articles to report.")

    # --- Dispatcher / worker queues ---
    dispatcher_queue_maxsize: int = Field(
        default=10_000, description="Max size of each worker's inbound queue."
    )

    # --- API server ---
    api_host: str = Field(default="0.0.0.0", description="FastAPI bind host.")
    api_port: int = Field(default=8000, description="FastAPI bind port.")

    # --- Dashboard ---
    dashboard_api_base_url: str = Field(
        default="http://localhost:8000", description="Base URL the Streamlit dashboard uses to call the API."
    )
    dashboard_refresh_seconds: float = Field(
        default=3.0, description="Auto-refresh interval for the Streamlit dashboard."
    )

    # --- Misc ---
    recent_events_buffer_size: int = Field(
        default=200, description="Max number of recent raw events retained in memory for the /events endpoint."
    )
    log_level: str = Field(default="INFO", description="Root logging level.")


settings = Settings()
