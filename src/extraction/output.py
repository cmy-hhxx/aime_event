from __future__ import annotations

from src.extraction.models import ExtractedEvent


def event_to_json(event: ExtractedEvent) -> dict[str, str | None]:
    return {
        "source_id": event.source_id,
        "event_type": event.event_type,
        "event_time": event.event_time,
    }
