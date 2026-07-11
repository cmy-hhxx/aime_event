"""extract select: 候选簇阈值送审 -> LLM triage -> 阈值入选(无数量配额).

  送审门(规则): recent n_articles>=EVENT_RECENT_MIN_ARTICLES
               或 (>=EVENT_RECENT_ALT_MIN_ARTICLES 且 n_v2_reactions>=1);
               early n_articles>=EVENT_EARLY_MIN_ARTICLES
  入选门(LLM):  is_valid_event 且 significance>=EVENT_MIN_SIGNIFICANCE
  质量护栏:    (event_type,event_date,主体) 去重; 单 symbol 上限(0=关闭);
               对已存在的 8-K 事件(selected_8k.jsonl)跨源去重——8-K 是原始披露先到,
               新闻侧同主体同事件(同日, 或同类型且日期差<=3天)让位
  --sweep:     不调 API, 输出阈值->送审量对照表供人工定阈值
"""
from __future__ import annotations

import json
import os
import re
import time
from collections import Counter, defaultdict

import duckdb
import pyarrow as pa

from src import config
from src.common import llm
from src.extraction import prompts

ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def gate_where(era_split: str, recent_min: int, recent_alt_min: int, early_min: int,
               date: str | None = None) -> str:
    where = (f"(peak_date >= '{era_split}' AND (n_articles >= {recent_min} "
             f"OR (n_articles >= {recent_alt_min} AND n_v2_reactions >= 1))) "
             f"OR (peak_date < '{era_split}' AND n_articles >= {early_min})")
    if date:
        where = f"({where}) AND peak_date = '{date}'"
    return where


def build_candidates(con, recent_min: int, recent_alt_min: int, early_min: int,
                     date: str | None = None) -> list[dict]:
    where = gate_where(config.EVENT_ERA_SPLIT, recent_min, recent_alt_min, early_min, date)
    return con.execute(f"""
      SELECT event_id, peak_date, first_date, last_date, n_articles, n_sources,
             n_content_types, n_high, n_v2_reactions, track, rep_title, all_symbols,
             substr(peak_date, 1, 7) AS month,
             peak_date >= '{config.EVENT_ERA_SPLIT}' AS is_recent,
             2.0 * ln(1 + n_articles) + 0.5 * n_sources + 0.3 * n_content_types
               + 1.5 * ln(1 + n_high) + 2.0 * ln(1 + n_v2_reactions)
               + CASE WHEN track = 'macro' THEN 1.0 ELSE 0 END AS score
      FROM read_parquet('{config.EVENT_CANDIDATE_DIR}/clusters.parquet')
      WHERE {where}
      ORDER BY peak_date
    """).fetch_arrow_table().to_pylist()


def sweep(con, date: str | None = None) -> None:
    print(f"{'recent_min':>10} {'alt_min':>8} {'early_min':>9} {'recent送审':>10} {'early送审':>9} {'合计':>8}")
    for rm in (3, 4, 5, 6, 8):
        for am in (2, 3):
            if am > rm:
                continue
            for em in (2, 3):
                where = gate_where(config.EVENT_ERA_SPLIT, rm, am, em, date)
                r, e = con.execute(f"""
                  SELECT sum(CASE WHEN peak_date >= '{config.EVENT_ERA_SPLIT}' THEN 1 ELSE 0 END),
                         sum(CASE WHEN peak_date < '{config.EVENT_ERA_SPLIT}' THEN 1 ELSE 0 END)
                  FROM read_parquet('{config.EVENT_CANDIDATE_DIR}/clusters.parquet')
                  WHERE {where}
                """).fetchone()
                r, e = r or 0, e or 0
                print(f"{rm:>10} {am:>8} {em:>9} {r:>10} {e:>9} {r+e:>8}")
    print("\n当前 config 阈值: recent>=%d 或(>=%d 且有研报佐证), early>=%d"
          % (config.EVENT_RECENT_MIN_ARTICLES, config.EVENT_RECENT_ALT_MIN_ARTICLES,
             config.EVENT_EARLY_MIN_ARTICLES))


def fetch_rep_titles(con, event_ids: list[str]) -> dict[str, list]:
    con.register("wanted", pa.table({"event_id": event_ids}))
    rows = con.execute(f"""
      SELECT m.event_id, m.pub_date, m.source_name, m.title
      FROM read_parquet('{config.EVENT_CANDIDATE_DIR}/members.parquet') m
      JOIN wanted USING (event_id)
      QUALIFY row_number() OVER (PARTITION BY m.event_id ORDER BY m.body_len DESC) <= 6
    """).fetchall()
    out = defaultdict(list)
    for eid, d, src, title in rows:
        out[eid].append(f"  [{d}] ({src or '?'}) {title}")
    return out


def triage_one(cand: dict) -> dict:
    user = prompts.TRIAGE_USER_TMPL.format(
        peak_date=cand["peak_date"], first_date=cand["first_date"], last_date=cand["last_date"],
        n_articles=cand["n_articles"], n_sources=cand["n_sources"], n_high=cand["n_high"],
        n_v2=cand["n_v2_reactions"], symbols=cand["all_symbols"] or "(无)",
        titles="\n".join(cand["_titles"]), types=prompts.EVENT_TYPES,
    )
    r = llm.chat_json(user, prompts.TRIAGE_SYSTEM, model=llm.model_for("triage"))
    r["event_id"] = cand["event_id"]
    return r


def final_select(cands: list[dict], triage: dict[str, dict],
                 min_significance: int, per_symbol_cap: int) -> list[dict]:
    valid = []
    for c in cands:
        t = triage.get(c["event_id"]) or {}
        if t.get("_error") or not t.get("is_valid_event"):
            continue
        if int(t.get("significance") or 0) < min_significance:
            continue
        d = t.get("event_date") or c["peak_date"]
        if not (isinstance(d, str) and ISO_DATE_RE.match(d) and "2000-01-01" <= d <= "2026-08-01"):
            d = c["peak_date"]
        valid.append({**c, **{k: t.get(k) for k in
                     ("event_type", "event_family", "event_subject", "primary_symbols",
                      "significance", "title_cn")}, "event_date": d})
    # 护栏1: 同 (类型,日期,主体) 去重, 分高者留
    best: dict[tuple, dict] = {}
    for v in sorted(valid, key=lambda x: (-int(x["significance"] or 0), -x["score"])):
        syms = v.get("primary_symbols") or []
        key = (v["event_type"], v["event_date"],
               syms[0] if syms else (v.get("event_subject") or "")[:30].lower())
        best.setdefault(key, v)
    # 护栏2: 单 symbol 上限(0=关闭); 无数量/时代配额
    picked, sym_cnt = [], Counter()
    for v in sorted(best.values(), key=lambda x: (-int(x["significance"] or 0), -x["score"])):
        syms = v.get("primary_symbols") or []
        if per_symbol_cap and syms and sym_cnt[syms[0]] >= per_symbol_cap:
            continue
        picked.append(v)
        if syms:
            sym_cnt[syms[0]] += 1
    return picked


def _days_apart(d1: str, d2: str) -> int:
    from datetime import date as _d
    try:
        return abs((_d.fromisoformat(d1) - _d.fromisoformat(d2)).days)
    except ValueError:
        return 999


def dedup_vs_8k(picked: list[dict], window_days: int = 3) -> tuple[list[dict], int]:
    """对已存在的 EVT8K 事件跨源去重: 8-K 是原始披露、先到, 新闻让位.

    撞车判定: 共享主 symbol 且 (事件日相同, 或 同 event_type 且日期差<=window_days)。
    后者覆盖 8-K 盘后申报、新闻 T+1 才铺开的时间错位; 类型不同(如财报 8-K vs 次日
    分析师评级)视为衍生新事件, 保留。
    """
    path = f"{config.EVENT_SELECTED_DIR}/selected_8k.jsonl"
    if not os.path.exists(path):
        return picked, 0
    by_sym: dict[str, list[tuple[str, str]]] = defaultdict(list)  # sym -> [(date, type)]
    with open(path) as fh:
        for line in fh:
            if not line.strip():
                continue
            e = json.loads(line)
            for s in e.get("primary_symbols") or []:
                by_sym[s].append((e.get("event_date") or "", e.get("event_type") or ""))
    kept, dropped = [], 0
    for v in picked:
        hit = False
        for s in v.get("primary_symbols") or []:
            for d8, t8 in by_sym.get(s, ()):
                if v["event_date"] == d8 or (v.get("event_type") == t8
                                             and _days_apart(v["event_date"], d8) <= window_days):
                    hit = True
                    break
            if hit:
                break
        if hit:
            dropped += 1
        else:
            kept.append(v)
    return kept, dropped


def run(args) -> None:
    os.makedirs(config.EVENT_SELECTED_DIR, exist_ok=True)
    os.makedirs(config.EVENT_REPORT_DIR, exist_ok=True)
    t0 = time.time()
    con = duckdb.connect()
    con.execute("SET threads TO 32")
    date = getattr(args, "date", None)
    if args.sweep:
        sweep(con, date)
        return
    cands = build_candidates(con, config.EVENT_RECENT_MIN_ARTICLES,
                             config.EVENT_RECENT_ALT_MIN_ARTICLES,
                             config.EVENT_EARLY_MIN_ARTICLES, date)
    total_cands = len(cands)
    if getattr(args, "limit", 0):
        cands = cands[: args.limit]
    n_recent = sum(1 for c in cands if c["is_recent"])
    limit_note = f" limit={args.limit}/{total_cands}" if getattr(args, "limit", 0) else ""
    print(f"[select] 送审候选 {len(cands)} (recent {n_recent}, early {len(cands)-n_recent}){limit_note}", flush=True)
    if args.dry_run:
        return
    titles = fetch_rep_titles(con, [c["event_id"] for c in cands])
    for c in cands:
        c["_titles"] = titles.get(c["event_id"], [f"  [{c['peak_date']}] {c['rep_title']}"])
    triage = llm.run_checkpointed(cands, lambda c: c["event_id"], triage_one,
                                  f"{config.EVENT_SELECTED_DIR}/triage.jsonl",
                                  workers=args.triage_workers, desc="triage")
    picked = final_select(cands, triage, config.EVENT_MIN_SIGNIFICANCE, config.EVENT_PER_SYMBOL_CAP)
    picked, deduped_vs_8k = dedup_vs_8k(picked)
    with open(f"{config.EVENT_SELECTED_DIR}/selected_events.jsonl", "w") as fh:
        for v in picked:
            v.pop("_titles", None)
            fh.write(json.dumps(v, ensure_ascii=False) + "\n")
    summary = {
        "candidates_triaged": len(cands), "selected": len(picked),
        "deduped_vs_8k": deduped_vs_8k,
        "by_era": dict(Counter("recent" if v["is_recent"] else "early" for v in picked)),
        "by_type": dict(Counter(v["event_type"] for v in picked).most_common()),
        "elapsed_sec": round(time.time() - t0, 1),
    }
    with open(f"{config.EVENT_REPORT_DIR}/stage_select_summary.json", "w") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)
    print(json.dumps(summary, ensure_ascii=False), flush=True)
