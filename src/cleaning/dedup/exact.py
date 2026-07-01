from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any, Iterator
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

DEDUP_VERSION = 4

_WHITESPACE_RE = re.compile(r"\s+")
_SEC_ACCESSION_RE = re.compile(r"/archives/edgar/data/\d+/([^/]+)/", re.IGNORECASE)
_DENYLIST_PATH_FRAGMENTS = (
    "/arc/outboundfeeds/",
    "/lineup-next/api/",
    "/market-news",
)
_DENYLIST_QUERY_KEYS = {"outputtype", "pagenumber", "limit"}
_TRACKING_QUERY_KEYS = {
    "cmpid",
    "fbclid",
    "gclid",
    "igshid",
    "mc_cid",
    "mc_eid",
    "mod",
    "r",
    "ref",
    "ref_src",
    "utm_campaign",
    "utm_content",
    "utm_medium",
    "utm_source",
    "utm_term",
}


@dataclass(frozen=True)
class DedupResult:
    key: str
    method: str
    debug: dict[str, Any]

    def __iter__(self) -> Iterator[str]:
        yield self.key
        yield self.method

    def __getitem__(self, index: int) -> str:
        if index == 0:
            return self.key
        if index == 1:
            return self.method
        raise IndexError(index)


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return _WHITESPACE_RE.sub(" ", value).strip().lower()


def normalize_url(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlsplit(url.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None

    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/")

    query_items = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        lowered = key.lower()
        if lowered.startswith("utm_") or lowered in _TRACKING_QUERY_KEYS:
            continue
        query_items.append((key, value))
    query = urlencode(sorted(query_items), doseq=True)
    return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), path, query, ""))


def _url_reject_reason(url: str | None) -> str | None:
    if not url:
        return "missing_url"

    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return "invalid_url"

    path = parsed.path.lower()
    if any(fragment in path for fragment in _DENYLIST_PATH_FRAGMENTS):
        return "feed_or_api_path"
    if "/sitemap" in path or "news-sitemap" in path:
        return "sitemap_url"

    query_keys = {key.lower() for key, _ in parse_qsl(parsed.query, keep_blank_values=True)}
    if query_keys & _DENYLIST_QUERY_KEYS:
        return "feed_or_list_query"

    return None


def is_eligible_article_url(url: str | None) -> bool:
    return normalize_url(url) is not None and _url_reject_reason(url) is None


def content_hash(title: str | None, body: str | None) -> str | None:
    normalized_title = normalize_text(title)
    normalized_body = normalize_text(body)
    if not normalized_title and not normalized_body:
        return None
    digest = hashlib.sha256(f"{normalized_title}|{normalized_body}".encode()).hexdigest()
    return f"hash:sha256:{digest}"


def id_key(record_id: str) -> str:
    return f"id:{record_id}"


def _notice_attachment_key(record: dict[str, Any]) -> DedupResult | None:
    if record.get("content_type") != "US_NOTICE":
        return None
    notice = record.get("notice")
    attachments = (notice or {}).get("attachments") if isinstance(notice, dict) else None
    if not attachments:
        return None

    first_url = None
    first_normalized = None
    for attachment in attachments:
        if not isinstance(attachment, dict):
            continue
        url = attachment.get("url")
        normalized = normalize_url(str(url)) if url else None
        if normalized and first_url is None:
            first_url = str(url)
            first_normalized = normalized
        accession = _extract_sec_accession(str(url)) if url else None
        if accession:
            return DedupResult(
                key=f"notice:{accession}",
                method="notice_attachment",
                debug={
                    "version": DEDUP_VERSION,
                    "source": "notice_accession",
                    "notice_accession": accession,
                    "notice_attachment_url": str(url),
                    "normalized_url": normalized,
                },
            )

    if first_normalized:
        digest = hashlib.sha256(first_normalized.encode()).hexdigest()
        return DedupResult(
            key=f"notice_url:sha256:{digest}",
            method="notice_attachment",
            debug={
                "version": DEDUP_VERSION,
                "source": "notice_attachment_url",
                "notice_attachment_url": first_url,
                "normalized_url": first_normalized,
                "hash_algorithm": "sha256",
            },
        )
    return None


def _extract_sec_accession(url: str) -> str | None:
    match = _SEC_ACCESSION_RE.search(url)
    return match.group(1) if match else None


def compute_dedup_key(record: dict[str, Any]) -> DedupResult:
    notice_key = _notice_attachment_key(record)
    if notice_key:
        return notice_key

    source_url = (record.get("source") or {}).get("url")
    normalized_url = normalize_url(source_url)
    url_reject_reason = _url_reject_reason(source_url)
    if normalized_url and url_reject_reason is None:
        return DedupResult(
            key=f"url:{normalized_url}",
            method="source_url",
            debug={
                "version": DEDUP_VERSION,
                "source": "source.url",
                "source_url": source_url,
                "normalized_url": normalized_url,
            },
        )

    digest_key = content_hash(record.get("title"), record.get("body"))
    if digest_key:
        debug: dict[str, Any] = {
            "version": DEDUP_VERSION,
            "source": "normalized_title_body",
            "hash_algorithm": "sha256",
            "text_normalization": "trim_lower_collapse_whitespace",
        }
        if source_url:
            debug["source_url"] = source_url
            debug["normalized_url"] = normalized_url
            debug["rejected_url_reason"] = url_reject_reason
        return DedupResult(digest_key, "content_hash", debug)

    record_id = str(record["id"])
    return DedupResult(
        key=id_key(record_id),
        method="id",
        debug={"version": DEDUP_VERSION, "source": "id_fallback"},
    )


def finalize_record(
    record: dict[str, Any],
    key: str,
    method: str,
    is_canonical: bool,
    canonical_id: str,
    debug: dict[str, Any] | None = None,
) -> dict[str, Any]:
    out = {k: v for k, v in record.items() if not k.startswith("_")}
    out["dedup"] = {
        "version": DEDUP_VERSION,
        "key": key,
        "method": method,
        "is_canonical": is_canonical,
        "canonical_id": canonical_id,
        "debug": debug or {"version": DEDUP_VERSION, "source": "unknown"},
    }
    return out
