from __future__ import annotations

import hashlib


def compute_dedup_key(record: dict) -> tuple[str, str] | None:
    source_url = (record.get("source") or {}).get("url")
    if source_url:
        return f"url:{source_url}", "source_url"

    title = (record.get("title") or "").strip().lower()
    body = record.get("body") or ""
    if title or body:
        content = f"{title}|{body}"
        digest = hashlib.md5(content.encode()).hexdigest()
        return f"hash:{digest}", "content_hash"

    return None


def id_key(record_id: str) -> str:
    return f"id:{record_id}"
