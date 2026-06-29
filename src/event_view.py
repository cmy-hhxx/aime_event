from __future__ import annotations

from typing import Any

from src.transform import IMPORTANCE_MAP, REGION_TAGS

_IMPORTANCE_TAGS = set(IMPORTANCE_MAP)


def build_event_record(cleaned: dict[str, Any]) -> dict[str, Any]:
    event: dict[str, Any] = {
        "id": cleaned["id"],
        "content_type": cleaned["content_type"],
        "title": cleaned["title"],
        "published_at": cleaned["published_at"],
    }

    body = (cleaned.get("body") or "").strip()
    if body:
        event["body"] = body

    source = _compact_source(cleaned.get("source") or {})
    if source:
        event["source"] = source

    entities = _compact_entities(cleaned.get("entities") or {})
    if entities:
        event["entities"] = entities

    topics = _topics(cleaned.get("tags") or [])
    if topics:
        event["topics"] = topics

    if cleaned.get("importance"):
        event["importance"] = cleaned["importance"]

    if cleaned.get("regions"):
        event["regions"] = cleaned["regions"]

    notice = _compact_notice(cleaned.get("notice"))
    if notice:
        event["notice"] = notice

    return event


def _compact_source(source: dict[str, Any]) -> dict[str, str]:
    compact = {}
    for key in ("name", "url"):
        value = source.get(key)
        if value:
            compact[key] = str(value)
    return compact


def _compact_entities(entities: dict[str, Any]) -> dict[str, list[dict[str, str]]]:
    compact: dict[str, list[dict[str, str]]] = {}
    for key in ("stocks", "crypto"):
        values = entities.get(key) or []
        if values:
            compact[key] = values
    return compact


def _topics(tags: list[str]) -> list[str]:
    topics = []
    seen = set()
    for tag in tags:
        if tag in REGION_TAGS or tag in _IMPORTANCE_TAGS:
            continue
        if tag not in seen:
            seen.add(tag)
            topics.append(tag)
    return topics


def _compact_notice(notice: Any) -> dict[str, Any] | None:
    if not isinstance(notice, dict):
        return None
    compact = {}
    if notice.get("filing_type"):
        compact["filing_type"] = notice["filing_type"]
    if notice.get("declare_date"):
        compact["declare_date"] = notice["declare_date"]
    attachments = notice.get("attachments") or []
    if attachments:
        compact["attachments"] = attachments
    return compact or None
