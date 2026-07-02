from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from src.extraction.models import ExtractedEvent


def event_to_json(event: ExtractedEvent) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "source_id": event.source_id,
        "event_type": event.event_type,
        "event_title": event.event_title,
    }
    if event.event_time:
        payload["event_time"] = event.event_time
    if event.entities:
        payload["entities"] = event.entities
    if event.summary:
        payload["summary"] = event.summary
    if event.evidence:
        payload["evidence"] = event.evidence
    if event.confidence is not None:
        payload["confidence"] = event.confidence
    return payload


def extraction_record_to_json(
    *,
    source: dict[str, Any],
    source_file: str,
    source_line: int,
    model: str,
    response: dict[str, Any],
    include_raw_response: bool,
) -> dict[str, Any]:
    source_id = source_record_id(source)
    events = [_normalize_event(source_id, index, item) for index, item in enumerate(_events(response), start=1)]
    payload: dict[str, Any] = {
        "source_id": source_id,
        "source_file": source_file,
        "source_line": source_line,
        "source_title": source.get("title"),
        "published_at": source_published_at(source),
        "model": model,
        "events": events,
    }
    if include_raw_response:
        payload["raw_response"] = response
    return {key: value for key, value in payload.items() if value not in (None, "", [], {}) or key == "events"}


def error_record_to_json(
    *,
    source: dict[str, Any],
    source_file: str,
    source_line: int,
    model: str,
    error: Exception,
) -> dict[str, Any]:
    return {
        "source_id": source_record_id(source),
        "source_file": source_file,
        "source_line": source_line,
        "source_title": source.get("title"),
        "published_at": source_published_at(source),
        "model": model,
        "events": [],
        "error": type(error).__name__,
        "message": str(error),
    }


def _events(response: dict[str, Any]) -> list[dict[str, Any]]:
    events = response.get("events")
    if not isinstance(events, list):
        return []
    return [event for event in events if isinstance(event, dict)]


def _normalize_event(source_id: str, index: int, event: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        "event_id": f"{source_id}#event{index}",
        "event_type": str(event.get("event_type") or "other"),
        "event_title": str(event.get("event_title") or event.get("title") or ""),
        "event_time": event.get("event_time"),
        "entities": event.get("entities") if isinstance(event.get("entities"), list) else [],
        "summary": event.get("summary"),
        "evidence": event.get("evidence"),
        "confidence": _confidence(event.get("confidence")),
    }
    return {key: value for key, value in normalized.items() if value not in (None, "", [])}


def _confidence(value: Any) -> float | None:
    if value is None:
        return None
    try:
        score = float(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, min(1.0, score))


def source_record_id(source: dict[str, Any]) -> str:
    for key in ("id", "_id", "bizId", "materialId"):
        value = source.get(key)
        if value:
            return str(value)
    return "unknown"


def source_published_at(source: dict[str, Any]) -> str | None:
    value = source.get("published_at") or source.get("ctime")
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    text = str(value).strip()
    if text.isdigit():
        return datetime.fromtimestamp(int(text), tz=timezone.utc).isoformat().replace("+00:00", "Z")
    return text
