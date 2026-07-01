from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import orjson

from src.cleaning.ingest.html_utils import html_to_text

REGION_TAGS = {
    "NorthAmerica",
    "Europe",
    "Asia-Pacific",
    "Middle-East",
    "LatinAmerica",
    "Africa",
}

IMPORTANCE_MAP = {
    "us_high_importance": "high",
    "us_mid_importance": "mid",
    "us_low_importance": "low",
}

VALID_CONTENT_TYPES = {
    "US_NEWS",
    "US_ROBOT",
    "US_ARTICLE",
    "US_FLASH",
    "US_POST",
    "US_NOTICE",
}


@dataclass(frozen=True)
class TransformResult:
    record: dict[str, Any] | None
    reason: str | None = None
    message: str | None = None
    raw_id: str | None = None

    @property
    def accepted(self) -> bool:
        return self.record is not None


def reject(reason: str, message: str, raw_id: str | None = None) -> TransformResult:
    return TransformResult(record=None, reason=reason, message=message, raw_id=raw_id)


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _ts_to_iso(ts: int | float | None) -> str | None:
    if not ts:
        return None
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (OSError, OverflowError, TypeError, ValueError):
        return None


def _extract_tags(tag_map: dict[str, Any] | None) -> tuple[list[str], str | None, list[str]]:
    if not tag_map:
        return [], None, []
    tags = []
    importance = None
    regions = []
    for item in tag_map.values():
        code = item.get("code") if isinstance(item, dict) else None
        if not code or str(code).isdigit():
            continue
        tags.append(str(code))
        if code in IMPORTANCE_MAP and importance is None:
            importance = IMPORTANCE_MAP[code]
        if code in REGION_TAGS:
            regions.append(code)
    return tags, importance, regions


def _extract_entities(links: list[Any] | None, crypto_infos: list[Any] | None) -> dict[str, list[dict[str, str]]]:
    stocks: list[dict[str, str]] = []
    crypto: list[dict[str, str]] = []
    seen_stocks: set[str] = set()
    seen_crypto: set[str] = set()

    for link in links or []:
        if not isinstance(link, dict):
            continue
        link_type = link.get("type")
        param = link.get("param") or {}
        if not isinstance(param, dict):
            param = {}
        if link_type == "stock":
            symbol = param.get("stockCode")
            name = param.get("stockName")
            if symbol and symbol not in seen_stocks:
                seen_stocks.add(symbol)
                stocks.append({"symbol": str(symbol), "name": str(name or symbol)})
        elif link_type == "crypto":
            code = param.get("code") or param.get("stockCode") or link.get("word")
            name = param.get("name") or param.get("stockName") or code
            if code and code not in seen_crypto:
                seen_crypto.add(code)
                crypto.append({"code": str(code), "name": str(name or code)})

    for item in crypto_infos or []:
        if not isinstance(item, dict):
            continue
        code = item.get("code")
        name = item.get("name")
        if code and code not in seen_crypto:
            seen_crypto.add(code)
            crypto.append({"code": str(code), "name": str(name or code)})

    return {"stocks": stocks, "crypto": crypto}


def _extract_notice(notice_info: dict[str, Any] | None) -> dict[str, Any] | None:
    if not notice_info:
        return None
    attachments = []
    for att in notice_info.get("attachmentList") or []:
        if not isinstance(att, dict):
            continue
        url = att.get("url")
        if url:
            attachments.append({"url": str(url), "file_type": str(att.get("fileType") or att.get("storeType") or "")})
    return {
        "filing_type": str(notice_info.get("noticeType") or ""),
        "declare_date": str(notice_info.get("declareDate") or ""),
        "attachments": attachments,
    }


def _extract_source(raw: dict[str, Any]) -> dict[str, Any]:
    news = raw.get("news") or {}
    if not isinstance(news, dict):
        news = {}
    author = raw.get("author")
    if not author:
        author_info = raw.get("authorInfo") or {}
        if isinstance(author_info, dict):
            author = author_info.get("name")
    return {
        "name": _str_or_none(news.get("source") or raw.get("source")),
        "url": _str_or_none(news.get("sourceUrl")),
        "author": _str_or_none(author),
    }


def _validate_record(record: dict[str, Any]) -> TransformResult:
    raw_id = str(record.get("id") or "")
    if not raw_id:
        return reject("missing_id", "missing _id")

    content_type = record.get("content_type")
    if content_type not in VALID_CONTENT_TYPES:
        return reject("unknown_content_type", f"unknown content_type: {content_type}", raw_id)

    if not record.get("published_at"):
        return reject("missing_published_at", "missing or invalid ctime", raw_id)

    if not record.get("updated_at"):
        record["updated_at"] = record["published_at"]

    body = (record.get("body") or "").strip()
    notice = record.get("notice")
    if content_type != "US_NOTICE" and not body:
        return reject("empty_body", "non-notice record has empty body", raw_id)

    if content_type == "US_NOTICE":
        attachments = (notice or {}).get("attachments") if isinstance(notice, dict) else None
        if not attachments:
            return reject("missing_notice_attachment", "US_NOTICE record has no attachment URL", raw_id)

    return TransformResult(record=record, raw_id=raw_id)


def transform(raw: dict[str, Any]) -> TransformResult:
    raw_id_value = raw.get("_id")
    raw_id = str(raw_id_value) if raw_id_value else None
    if not raw_id:
        return reject("missing_id", "missing _id")

    body = html_to_text(str(raw.get("content") or ""))
    summary = str(raw.get("summary") or "").strip() or None
    tags, importance, regions = _extract_tags(raw.get("contentTagMap") if isinstance(raw.get("contentTagMap"), dict) else None)
    news = raw.get("news") or {}
    if not isinstance(news, dict):
        news = {}
    extensions = raw.get("extensions") or {}
    if not isinstance(extensions, dict):
        extensions = {}
    content_type = str(raw.get("businessCode") or "")
    published_at = _ts_to_iso(raw.get("ctime"))
    updated_at = _ts_to_iso(raw.get("rtime")) or published_at
    type_code = _int_or_none(raw.get("type") or 0)
    if type_code is None:
        return reject("invalid_type_code", "type is not an integer", raw_id)

    record: dict[str, Any] = {
        "id": raw_id,
        "content_type": content_type,
        "type_code": type_code,
        "title": str(raw.get("title") or "").strip(),
        "body": body,
        "summary": summary,
        "published_at": published_at,
        "updated_at": updated_at,
        "source": _extract_source(raw),
        "entities": _extract_entities(raw.get("links") if isinstance(raw.get("links"), list) else None, raw.get("cryptoInfos") if isinstance(raw.get("cryptoInfos"), list) else None),
        "tags": tags,
        "importance": importance,
        "regions": regions,
        "notice": _extract_notice(raw.get("noticeInfo") if isinstance(raw.get("noticeInfo"), dict) else None) if content_type == "US_NOTICE" else None,
        "meta": {
            "language": _str_or_none(raw.get("language")),
            "gid": _str_or_none(news.get("gid")),
            "material_id": str(raw.get("materialId") or ""),
            "uid": _str_or_none(extensions.get("uid")),
        },
        "_body_len": len(body),
    }
    return _validate_record(record)


def transform_line(line: bytes) -> TransformResult:
    try:
        raw = orjson.loads(line)
    except orjson.JSONDecodeError as exc:
        return reject("invalid_json", str(exc))

    if not isinstance(raw, dict):
        return reject("invalid_record", "line is not a JSON object")

    return transform(raw)
