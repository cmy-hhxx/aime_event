from __future__ import annotations

import hashlib
import re
from typing import Any
from urllib.parse import parse_qs, urlsplit

DedupKey = tuple[str, str]

_WHITESPACE_RE = re.compile(r"\s+")
_DENYLIST_PATH_FRAGMENTS = (
    "/arc/outboundfeeds/",
    "/lineup-next/api/",
    "/market-news",
)
_DENYLIST_QUERY_KEYS = {"outputtype", "pagenumber", "limit"}


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return _WHITESPACE_RE.sub(" ", value).strip().lower()


def is_eligible_article_url(url: str | None) -> bool:
    if not url:
        return False

    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return False

    path = parsed.path.lower()
    query = parsed.query.lower()
    if any(fragment in path for fragment in _DENYLIST_PATH_FRAGMENTS):
        return False
    if "/sitemap" in path or "news-sitemap" in path:
        return False

    query_keys = {key.lower() for key in parse_qs(query, keep_blank_values=True)}
    if query_keys & _DENYLIST_QUERY_KEYS:
        return False

    return True


def content_hash(title: str | None, body: str | None) -> str | None:
    normalized_title = normalize_text(title)
    normalized_body = normalize_text(body)
    if not normalized_title and not normalized_body:
        return None
    digest = hashlib.md5(f"{normalized_title}|{normalized_body}".encode()).hexdigest()
    return f"hash:{digest}"


def id_key(record_id: str) -> str:
    return f"id:{record_id}"


def compute_dedup_key(record: dict[str, Any]) -> DedupKey:
    source_url = (record.get("source") or {}).get("url")
    if is_eligible_article_url(source_url):
        return f"url:{source_url}", "source_url"

    digest_key = content_hash(record.get("title"), record.get("body"))
    if digest_key:
        return digest_key, "content_hash"

    return id_key(str(record["id"])), "id"


def finalize_record(
    record: dict[str, Any],
    key: str,
    method: str,
    is_canonical: bool,
    canonical_id: str,
) -> dict[str, Any]:
    out = {k: v for k, v in record.items() if not k.startswith("_")}
    out["dedup"] = {
        "key": key,
        "method": method,
        "is_canonical": is_canonical,
        "canonical_id": canonical_id,
    }
    return out
