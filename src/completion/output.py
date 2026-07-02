from __future__ import annotations

from typing import Any

from src.completion.models import CompletedEvent


def completed_event_to_json(event: CompletedEvent) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "fields": event.fields,
    }
