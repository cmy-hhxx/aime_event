from __future__ import annotations

from datetime import datetime, timezone

from src.html_utils import html_to_text

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


def _ts_to_iso(ts: int | None) -> str | None:
    if not ts:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _extract_tags(tag_map: dict | None) -> tuple[list[str], str | None, list[str]]:
    if not tag_map:
        return [], None, []
    tags = []
    importance = None
    regions = []
    for item in tag_map.values():
        code = item.get("code")
        if not code or code.isdigit():
            continue
        tags.append(code)
        if code in IMPORTANCE_MAP and importance is None:
            importance = IMPORTANCE_MAP[code]
        if code in REGION_TAGS:
            regions.append(code)
    return tags, importance, regions


def _extract_entities(links: list | None, crypto_infos: list | None) -> dict:
    stocks = []
    crypto = []
    seen_stocks = set()
    seen_crypto = set()

    for link in links or []:
        link_type = link.get("type")
        if link_type == "stock":
            param = link.get("param") or {}
            symbol = param.get("stockCode")
            name = param.get("stockName")
            if symbol and symbol not in seen_stocks:
                seen_stocks.add(symbol)
                stocks.append({"symbol": symbol, "name": name or symbol})
        elif link_type == "crypto":
            param = link.get("param") or {}
            code = param.get("code") or param.get("stockCode") or link.get("word")
            name = param.get("name") or param.get("stockName") or code
            if code and code not in seen_crypto:
                seen_crypto.add(code)
                crypto.append({"code": code, "name": name or code})

    for item in crypto_infos or []:
        code = item.get("code")
        name = item.get("name")
        if code and code not in seen_crypto:
            seen_crypto.add(code)
            crypto.append({"code": code, "name": name or code})

    return {"stocks": stocks, "crypto": crypto}


def _extract_notice(notice_info: dict | None) -> dict | None:
    if not notice_info:
        return None
    attachments = []
    for att in notice_info.get("attachmentList") or []:
        url = att.get("url")
        if url:
            attachments.append({"url": url, "file_type": att.get("fileType") or att.get("storeType") or ""})
    return {
        "filing_type": notice_info.get("noticeType") or "",
        "declare_date": notice_info.get("declareDate") or "",
        "attachments": attachments,
    }


def _extract_source(raw: dict) -> dict:
    news = raw.get("news") or {}
    author = raw.get("author")
    if not author:
        author_info = raw.get("authorInfo") or {}
        author = author_info.get("name")
    return {
        "name": news.get("source") or raw.get("source"),
        "url": news.get("sourceUrl"),
        "author": author,
    }


def should_keep(raw: dict, body: str) -> bool:
    if raw.get("valid") == 0:
        title = (raw.get("title") or "").strip()
        if not title and not body:
            notice = _extract_notice(raw.get("noticeInfo"))
            if not notice or not notice.get("attachments"):
                return False
    return True


def transform(raw: dict) -> dict:
    body = html_to_text(raw.get("content") or "")
    summary = (raw.get("summary") or "").strip() or None
    tags, importance, regions = _extract_tags(raw.get("contentTagMap"))
    news = raw.get("news") or {}
    extensions = raw.get("extensions") or {}
    content_type = raw.get("businessCode") or ""

    record = {
        "id": raw["_id"],
        "content_type": content_type,
        "type_code": raw.get("type", 0),
        "title": (raw.get("title") or "").strip(),
        "body": body,
        "summary": summary,
        "published_at": _ts_to_iso(raw.get("ctime")),
        "updated_at": _ts_to_iso(raw.get("rtime")),
        "source": _extract_source(raw),
        "entities": _extract_entities(raw.get("links"), raw.get("cryptoInfos")),
        "tags": tags,
        "importance": importance,
        "regions": regions,
        "notice": _extract_notice(raw.get("noticeInfo")) if content_type == "US_NOTICE" else None,
        "meta": {
            "language": raw.get("language"),
            "gid": news.get("gid"),
            "material_id": raw.get("materialId") or "",
            "uid": extensions.get("uid"),
        },
        "_body_len": len(body),
    }
    return record
