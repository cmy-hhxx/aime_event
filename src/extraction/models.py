from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ExtractedEvent:
    source_id: str
    event_type: str
    event_time: str | None = None
