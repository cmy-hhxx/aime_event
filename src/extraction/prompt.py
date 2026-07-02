from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


SYSTEM_PROMPT = """你是金融资讯事件抽取器。请只返回 JSON 对象，不要输出解释文字。
目标：从英文/中文金融新闻、快讯、公告或文章中抽取可交易、可归档的事实事件。

返回格式：
{
  "events": [
    {
      "event_type": "earnings|guidance|merger_acquisition|financing|product|regulation|lawsuit|management_change|analyst_rating|macro|market_move|filing|other",
      "event_title": "简短事件标题",
      "event_time": "ISO-8601 时间或 null",
      "entities": [{"name": "实体名", "type": "company|ticker|crypto|person|organization|country|sector|other", "symbol": "代码或 null"}],
      "summary": "一句话事实摘要",
      "evidence": "原文中支持该事件的短句",
      "confidence": 0.0
    }
  ]
}

规则：
- 没有明确事实事件时返回 {"events": []}。
- 不要编造原文没有的信息；不确定的时间用 null。
- confidence 使用 0 到 1 的数字。
- evidence 必须来自输入文本，尽量短。
"""


def build_user_prompt(record: dict[str, Any], *, max_body_chars: int) -> str:
    payload = {
        "id": _first(record, "id", "_id", "bizId"),
        "content_type": _first(record, "content_type", "businessCode"),
        "published_at": _published_at(record),
        "title": record.get("title"),
        "summary": _first(record, "summary"),
        "body": _truncate_text(_first(record, "body", "content"), max_body_chars),
        "source": record.get("source"),
        "entities": _first(record, "entities", "innerStockInfo", "links"),
        "tags": _first(record, "tags", "contentTagMap", "tagInfo"),
        "importance": record.get("importance"),
        "regions": record.get("regions"),
        "notice": _first(record, "notice", "noticeInfo"),
    }
    compact = {key: value for key, value in payload.items() if value not in (None, "", [], {})}
    return json.dumps(compact, ensure_ascii=False, indent=2)


def _truncate_text(value: Any, max_chars: int) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "\n...[TRUNCATED]"


def _first(record: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = record.get(key)
        if value not in (None, "", [], {}):
            return value
    return None


def _published_at(record: dict[str, Any]) -> str | None:
    value = _first(record, "published_at", "ctime")
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    text = str(value).strip()
    if text.isdigit():
        return datetime.fromtimestamp(int(text), tz=timezone.utc).isoformat().replace("+00:00", "Z")
    return text
