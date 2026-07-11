"""notice8k 事件行 -> LLM triage 入选 -> selected/selected_8k.jsonl (供 structure 消费).

与新闻簇 select 平行的 8-K 桥接步: 对 `extract notice8k --date D` 的产物逐条 LLM 判定
(is_valid_event 且 significance>=EVENT_MIN_SIGNIFICANCE 入选), triage 结果断点续跑落
selected/triage_8k.jsonl. 入选行 event_id=EVT8K_<accession>, peak_date=--date 供下游
structure/complete 的 --date 过滤; `_8k` 字段保留溯源信息与正文(structure 直接取用,
不走 members.parquet).

护栏(非配额): 同 (event_type, event_date, 主symbol) 去重, significance 高者留。
跨源去重方向: 8-K 是原始披露、先到——由新闻侧 select 对已存在的 EVT8K 事件去重
(见 select.py dedup_vs_8k), 本步不再对新闻去重。

用法: python -m src.main extract notice8k-select --date 2026-05-29 [--limit N]
输出: selected/selected_8k.jsonl (覆盖式), reports/stage_notice8k_select_summary.json
"""
from __future__ import annotations

import json
import os
import re
import time
from collections import Counter

from src import config
from src.common import llm
from src.extraction import prompts

MAX_SUMMARY_CHARS = 4000    # triage prompt 里摘要/原文节选截断
MAX_KEEP_CHARS = 12000      # 入选行里保留的正文上限(structure 的输入)
ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def load_8k_events(date: str) -> list[dict]:
    """读 notice8k 产物, 过滤掉无正文/无 accession 的行, 生成 event_id."""
    path = f"{config.EVENT_NOTICE_8K_DIR}/US_NOTICE.8k.{date}.jsonl"
    if not os.path.exists(path):
        raise SystemExit(f"未找到 {path}, 请先跑: extract notice8k --date {date}")
    rows = []
    with open(path) as fh:
        for line in fh:
            if not line.strip():
                continue
            r = json.loads(line)
            if not r.get("summary") or not r.get("accession"):
                continue  # 无正文无法判定; 无 accession 无法生成稳定主键
            r["_event_id"] = f"EVT8K_{r['accession']}"
            rows.append(r)
    return rows


def triage_one(row: dict) -> dict:
    user = prompts.NOTICE8K_TRIAGE_USER_TMPL.format(
        event_date=row.get("event_date") or "?",
        item_code=row.get("item_code") or "?",
        event_title=row.get("event_title") or "?",
        summary=(row.get("summary") or "")[:MAX_SUMMARY_CHARS],
        types=prompts.EVENT_TYPES,
    )
    r = llm.chat_json(user, prompts.NOTICE8K_TRIAGE_SYSTEM, model=llm.model_for("triage"))
    r["event_id"] = row["_event_id"]
    return r


def final_select(rows: list[dict], triage: dict[str, dict], date: str,
                 min_significance: int) -> list[dict]:
    valid = []
    for row in rows:
        t = triage.get(row["_event_id"]) or {}
        if t.get("_error") or not t.get("is_valid_event"):
            continue
        sig = int(t.get("significance") or 0)
        if sig < min_significance:
            continue
        d = t.get("event_date") or row.get("event_date") or date
        if not (isinstance(d, str) and ISO_DATE_RE.match(d) and "2000-01-01" <= d <= "2026-08-01"):
            d = row.get("event_date") or date
        syms = [s for s in (t.get("primary_symbols") or []) if isinstance(s, str) and s]
        valid.append({
            "event_id": row["_event_id"], "peak_date": date, "event_date": d,
            **{k: t.get(k) for k in ("event_type", "event_family", "event_subject", "title_cn")},
            "primary_symbols": syms, "significance": sig,
            # 与新闻 selected 行同构的佐证字段(单文档来源)
            "is_recent": True, "n_articles": 1, "n_sources": 1, "n_v2_reactions": 0,
            "score": sig,
            "_8k": {k: row.get(k) for k in ("trace_id", "accession", "cik", "item_code",
                                            "source_url", "body_source", "event_title")}
            | {"summary": (row.get("summary") or "")[:MAX_KEEP_CHARS]},
        })
    # 同 (类型,日期,主symbol) 去重, 分高者留
    best: dict[tuple, dict] = {}
    for v in sorted(valid, key=lambda x: -x["significance"]):
        syms = v.get("primary_symbols") or []
        key = (v["event_type"], v["event_date"],
               syms[0] if syms else (v.get("event_subject") or "")[:30].lower())
        best.setdefault(key, v)
    return list(best.values())


def run(args) -> None:
    os.makedirs(config.EVENT_SELECTED_DIR, exist_ok=True)
    os.makedirs(config.EVENT_REPORT_DIR, exist_ok=True)
    t0 = time.time()
    date = args.date
    rows = load_8k_events(date)
    total = len(rows)
    if getattr(args, "limit", 0):
        rows = rows[: args.limit]
    print(f"[notice8k-select] 送审 8-K {len(rows)}/{total} (date={date})", flush=True)

    triage = llm.run_checkpointed(rows, lambda r: r["_event_id"], triage_one,
                                  f"{config.EVENT_SELECTED_DIR}/triage_8k.jsonl",
                                  workers=args.triage_workers, desc="triage8k")
    picked = final_select(rows, triage, date, config.EVENT_MIN_SIGNIFICANCE)
    with open(f"{config.EVENT_SELECTED_DIR}/selected_8k.jsonl", "w") as fh:
        for v in picked:
            fh.write(json.dumps(v, ensure_ascii=False) + "\n")
    summary = {
        "date": date, "triaged": len(rows), "selected": len(picked),
        "by_type": dict(Counter(v["event_type"] for v in picked).most_common()),
        "elapsed_sec": round(time.time() - t0, 1),
    }
    with open(f"{config.EVENT_REPORT_DIR}/stage_notice8k_select_summary.json", "w") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)
    print(json.dumps(summary, ensure_ascii=False), flush=True)
