"""
Pydantic model representing a single normalized Wikimedia recent-change event.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, Field, field_validator


class WikiEvent(BaseModel):
    """A single validated Wikipedia 'recentchange' edit event."""

    timestamp: datetime = Field(..., description="UTC timestamp the event occurred.")
    title: str = Field(..., min_length=1, description="Title of the edited page.")
    user: str = Field(default="unknown", description="Username or IP of the editor.")
    anonymous: bool = Field(default=False, description="True if the editor is unregistered/anonymous.")
    bot: bool = Field(default=False, description="True if the edit was made by a bot account.")
    comment: str = Field(default="", description="Edit summary/comment.")
    event_type: str = Field(default="edit", description="Type of change, e.g. 'edit', 'new', 'log'.")
    namespace: int = Field(default=0, description="MediaWiki namespace ID.")
    wiki: str = Field(default="", description="Wiki database name, e.g. 'enwiki'.")
    server_name: str = Field(default="", description="Domain name of the wiki, e.g. 'en.wikipedia.org'.")

    @field_validator("timestamp", mode="before")
    @classmethod
    def _coerce_timestamp(cls, value: object) -> datetime:
        """Accept both epoch seconds (int/float) and ISO strings, always return UTC datetime."""
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value, tz=timezone.utc)
        if isinstance(value, str):
            try:
                epoch = float(value)
                return datetime.fromtimestamp(epoch, tz=timezone.utc)
            except ValueError:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value
        raise ValueError(f"Unsupported timestamp type: {type(value)!r}")

    @field_validator("comment", mode="before")
    @classmethod
    def _default_comment(cls, value: object) -> str:
        return value if isinstance(value, str) else ""

    def to_iso(self) -> str:
        """Return the event timestamp formatted as an ISO-8601 string."""
        return self.timestamp.astimezone(timezone.utc).isoformat()

    def to_row(self) -> tuple:
        """Return a tuple suitable for DuckDB batch insertion, matching the wiki_events schema."""
        return (
            self.timestamp,
            self.title,
            self.user,
            self.anonymous,
            self.bot,
            self.comment,
            self.event_type,
            self.namespace,
            self.wiki,
            self.server_name,
        )

    model_config = {
        "json_schema_extra": {
            "example": {
                "timestamp": 1732550400,
                "title": "Python (programming language)",
                "user": "SomeEditor",
                "anonymous": False,
                "bot": False,
                "comment": "Fixed typo",
                "event_type": "edit",
                "namespace": 0,
                "wiki": "enwiki",
                "server_name": "en.wikipedia.org",
            }
        }
    }
