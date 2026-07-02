from __future__ import annotations

import json
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
        "id": record.get("id"),
        "content_type": record.get("content_type"),
        "published_at": record.get("published_at"),
        "title": record.get("title"),
        "summary": record.get("summary"),
        "body": _truncate_text(record.get("body"), max_body_chars),
        "source": record.get("source"),
        "entities": record.get("entities"),
        "tags": record.get("tags"),
        "importance": record.get("importance"),
        "regions": record.get("regions"),
        "notice": record.get("notice"),
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
