"""Stage D: 对入选事件做 LLM 结构化 -> main_event 事实块 + 关系证据行.

流程: 取簇内 top 文章正文(seek 直读原始 jsonl) -> 组 prompt -> DeepSeek 输出严格 JSON.
泄露控制在 prompt 层硬约束: facts 只允许事件日当天及之前可知的信息.

用法: python -m src.main extract structure [--workers 24] [--limit 0]
输出: structured/structured.jsonl (断点续跑), reports/stage_d_summary.json
"""
from __future__ import annotations

import html
import json
import os
import re
import time
from collections import Counter, defaultdict

import duckdb
import orjson

from src import config
from src.common import llm
from src.extraction import prompts

MAX_ARTICLES = 8
MAX_BODY_CHARS = 2800
MAX_BODY_CHARS_8K = 8400  # 8-K 单文档事件, 正文额度放宽到 3 篇新闻的量
GOOD_SOURCES = ("reuters", "bloomberg", "wall street journal", "cnbc", "financial times",
                "marketwatch", "pr newswire", "business wire", "nasdaq", "ainvest wire")

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def clean_body(body: str, limit: int = MAX_BODY_CHARS) -> str:
    text = html.unescape(TAG_RE.sub(" ", body or ""))
    return WS_RE.sub(" ", text).strip()[:limit]


def source_rank(name: str) -> int:
    n = (name or "").lower()
    for i, g in enumerate(GOOD_SOURCES):
        if g in n:
            return i
    return len(GOOD_SOURCES)


def load_selected() -> list[dict]:
    """新闻入选事件 + 8-K 入选事件(selected_8k.jsonl, 存在才读); event_id 互不重叠."""
    events = []
    for name in ("selected_events.jsonl", "selected_8k.jsonl"):
        path = f"{config.EVENT_SELECTED_DIR}/{name}"
        if not os.path.exists(path):
            continue
        with open(path) as fh:
            events.extend(json.loads(l) for l in fh if l.strip())
    return events


def eightk_member(ev: dict) -> dict:
    """8-K 事件没有新闻簇成员, 用公告自身正文构造单篇伪 member."""
    k = ev["_8k"]
    return {
        "id": k.get("trace_id") or ev["event_id"], "pub_date": ev["peak_date"],
        "published_at": ev["peak_date"], "content_type": "US_NOTICE",
        "source_name": "SEC EDGAR 8-K",
        "title": k.get("event_title") or f"Form 8-K Item {k.get('item_code') or '?'}",
        "url": k.get("source_url"),
        "_body": clean_body(k.get("summary") or "", limit=MAX_BODY_CHARS_8K),
    }


def filter_by_peak(events: list[dict], date: str | None) -> list[dict]:
    if not date:
        return events
    return [e for e in events if e.get("peak_date") == date]


def pick_members(event_ids: list[str]) -> dict[str, list[dict]]:
    """每个事件挑 MAX_ARTICLES 篇: 来源质量优先, 其次正文长度, 标题去重."""
    con = duckdb.connect()
    import pyarrow as pa
    con.register("wanted", pa.table({"event_id": event_ids}))
    rows = con.execute(f"""
      SELECT event_id, id, file, "offset", nbytes, pub_date, published_at,
             content_type, source_name, title, body_len
      FROM read_parquet('{config.EVENT_CANDIDATE_DIR}/members.parquet')
      JOIN wanted USING (event_id)
    """).fetch_arrow_table().to_pylist()
    by_event = defaultdict(list)
    for r in rows:
        by_event[r["event_id"]].append(r)
    picked = {}
    for eid, ms in by_event.items():
        ms.sort(key=lambda m: (source_rank(m["source_name"]), -m["body_len"]))
        seen_titles, sel = set(), []
        for m in ms:
            key = WS_RE.sub(" ", m["title"].lower())[:60]
            if key in seen_titles:
                continue
            seen_titles.add(key)
            sel.append(m)
            if len(sel) >= MAX_ARTICLES:
                break
        picked[eid] = sel
    return picked


def fetch_bodies(members: dict[str, list[dict]], v1_dir: str) -> dict[str, str]:
    """按 (file, offset) 排序后 seek 直读, 返回 id -> 清洗后正文."""
    refs = [(m["file"], m["offset"], m["nbytes"], m["id"])
            for ms in members.values() for m in ms]
    refs.sort()
    bodies = {}
    cur_file, fh = None, None
    for fname, offset, nbytes, rid in refs:
        if fname != cur_file:
            if fh:
                fh.close()
            fh = open(os.path.join(v1_dir, fname), "rb")
            cur_file = fname
        fh.seek(offset)
        try:
            rec = orjson.loads(fh.read(nbytes))
            bodies[rid] = clean_body(rec.get("body") or "")
        except Exception:
            bodies[rid] = ""
    if fh:
        fh.close()
    return bodies


def structure_one(ev: dict) -> dict:
    parts = []
    for i, m in enumerate(ev["_members"], 1):
        parts.append(f"--- 文章{i} [{m['pub_date']}] 来源:{m['source_name'] or '?'} ---\n"
                     f"标题: {m['title']}\n正文: {m['_body'] or '(空)'}")
    user = prompts.STRUCTURE_USER_TMPL.format(
        event_date=ev["event_date"], event_type=ev.get("event_type") or "?",
        event_subject=ev.get("event_subject") or "?",
        primary_symbols=",".join(ev.get("primary_symbols") or []) or "(宏观)",
        title_cn=ev.get("title_cn") or "", articles="\n\n".join(parts),
    )
    r = llm.chat_json(user, prompts.STRUCTURE_SYSTEM, model=llm.model_for("structure"), temperature=0.1)
    r["event_id"] = ev["event_id"]
    r["_triage"] = {k: ev.get(k) for k in
                    ("event_date", "event_type", "event_family", "event_subject",
                     "primary_symbols", "significance", "title_cn", "is_recent",
                     "n_articles", "n_sources", "n_v2_reactions", "score", "peak_date")}
    r["_source_ids"] = [m["id"] for m in ev["_members"]]
    r["_source_meta"] = [{"id": m["id"], "pub_date": m["pub_date"], "published_at": m["published_at"],
                          "source": m["source_name"], "title": m["title"],
                          "content_type": m.get("content_type"),
                          "url": m.get("url")} for m in ev["_members"]]
    return r


def run(args) -> None:
    os.makedirs(config.EVENT_STRUCTURED_DIR, exist_ok=True)
    t0 = time.time()
    events = load_selected()
    events = filter_by_peak(events, getattr(args, "date", None))
    if args.limit:
        events = events[: args.limit]
    print(f"[stage_d] 待结构化事件: {len(events)}", flush=True)

    news = [e for e in events if not e.get("_8k")]
    members = pick_members([e["event_id"] for e in news]) if news else {}
    bodies = fetch_bodies(members, config.EVENT_V1_DIR) if news else {}
    print(f"[stage_d] 取正文 {len(bodies)} 篇 ({time.time()-t0:.0f}s)", flush=True)
    for e in events:
        if e.get("_8k"):
            e["_members"] = [eightk_member(e)] if e["_8k"].get("summary") else []
            continue
        ms = members.get(e["event_id"], [])
        for m in ms:
            m["_body"] = bodies.get(m["id"], "")
        e["_members"] = ms

    results = llm.run_checkpointed(
        [e for e in events if e["_members"]],
        key_fn=lambda e: e["event_id"], work_fn=structure_one,
        out_path=f"{config.EVENT_STRUCTURED_DIR}/structured.jsonl",
        workers=args.workers, desc="structure",
    )

    ok = [r for r in results.values() if not r.get("_error")]
    summary = {
        "events_in": len(events), "structured_ok": len(ok),
        "errors": len(results) - len(ok),
        "by_type": dict(Counter(r.get("event_type", "?") for r in ok).most_common(20)),
        "elapsed_sec": round(time.time() - t0, 1),
    }
    with open(f"{config.EVENT_REPORT_DIR}/stage_d_summary.json", "w") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)
    print(json.dumps(summary, ensure_ascii=False), flush=True)
