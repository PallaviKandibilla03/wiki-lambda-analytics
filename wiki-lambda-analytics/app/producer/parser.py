"""
Parses raw SSE JSON payload strings from Wikimedia EventStreams into
validated WikiEvent model instances, stripping unneeded fields.
"""

from __future__ import annotations

import ipaddress
import json
import logging
from typing import Optional

from pydantic import ValidationError

from app.models.wiki_event import WikiEvent
from app.monitoring.stream_statistics import stream_statistics

logger = logging.getLogger(__name__)


class EventParser:
    """Converts raw Wikimedia recentchange JSON strings into WikiEvent objects."""

    @staticmethod
    def parse(raw_payload: str) -> Optional[WikiEvent]:
        """
        Parse a single raw SSE `data:` payload string.

        Returns a validated WikiEvent, or None if the payload is malformed
        or does not represent a usable recent-change record.
        """
        try:
            raw = json.loads(raw_payload)
        except json.JSONDecodeError as exc:
            stream_statistics.record_parse_error(f"JSON decode error: {exc}")
            logger.debug("Failed to decode SSE payload: %s", exc)
            return None

        if not isinstance(raw, dict):
            stream_statistics.record_parse_error("Payload was not a JSON object")
            return None

        try:
            meta = raw.get("meta") or {}
            event = WikiEvent(
                timestamp=raw.get("timestamp"),
                title=raw.get("title", ""),
                user=raw.get("user", "unknown"),
                anonymous=bool(raw.get("anonymous", False)) or EventParser._looks_anonymous(raw),
                bot=bool(raw.get("bot", False)),
                comment=raw.get("comment") or "",
                event_type=raw.get("type", "edit"),
                namespace=int(raw.get("namespace", 0)),
                wiki=raw.get("wiki", ""),
                server_name=meta.get("domain", raw.get("server_name", "")),
            )
            return event
        except (ValidationError, TypeError, ValueError) as exc:
            stream_statistics.record_parse_error(f"Validation error: {exc}")
            logger.debug("Failed to validate WikiEvent: %s", exc)
            return None

    @staticmethod
    def _looks_anonymous(raw: dict) -> bool:
        """
        Heuristic fallback: Wikimedia recentchange payloads do not always carry
        an explicit anonymous flag. Unregistered editors are identified either by:
          1. A raw IP address as the `user` field (legacy behavior, IPv4/IPv6), or
          2. A temporary, tilde-prefixed pseudonym (e.g. "~2026-40141-62"), which
             is how Wikimedia's IP Masking project now represents logged-out
             editors on wikis where the feature has been rolled out, replacing
             their real IP address for privacy.
        """
        user = str(raw.get("user", "")).strip()
        if not user:
            return False

        if user.startswith("~"):
            return True

        candidate = user.split("/", 1)[0]  # strip CIDR suffix if present, e.g. "2001:db8::/32"
        try:
            ipaddress.ip_address(candidate)
            return True
        except ValueError:
            return False