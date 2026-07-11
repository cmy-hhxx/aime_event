"""Stage F: 组装 FinancialPredictionTrainingCase.v4 格式成品 + 泄露/完整性审计.

输入: structured/structured.jsonl + market/labels.jsonl + market/intraday.jsonl
输出: final/events.jsonl (一行一个完整事件包, 覆盖式), reports/stage_assemble_summary.json

成品必须通过 schema/completion/final_case.schema.json(顶层字段与 schema 严格一致);
缺真实完整分时的事件默认不落 final, --allow-no-intraday 可降级为空占位面板。
"""
from __future__ import annotations

import json
import os
import re
from collections import Counter

import jsonschema

from src import config

SCHEMA = "FinancialPredictionTrainingCase.v4.three_year_event_signal_pack"
LEAKAGE_BOUNDARY = ("S0 pre-event market and S1 official event facts are model-visible; "
                    "S2/S3/S4 prices, follow-on news, analyst reactions and social hindsight "
                    "are hidden labels/audit only.")
RELATION_POLICY = "relation text is visible, but numeric direction/strength labels are not manually assigned"
TRAINING_TREATMENT = "model-visible relation context; future direction learned from hidden labels"
TREATMENT_PRICED_CN = "已打价：进入 1D/5D/20D 未来收益标签；方向和强度由标签学习。"
TREATMENT_UNPRICED_CN = "待补价：先作弱关联召回，补齐行情面板后再决定是否进入监督标签。"

METRICS_TO_TRAIN = ["direction", "return interval", "cross-section rank", "abnormal return vs proxy"]

# 泄露审计: facts 文本中出现晚于事件日的 ISO 日期
ISO_DATE_RE = re.compile(r"20\d{2}-\d{2}-\d{2}")

RELATION_CN_DEFAULT = "事件相关候选"
CASE_ID_RE = re.compile(r"^[A-Z0-9][A-Z0-9_]{5,80}$")
SCHEMA_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                           "schema", "completion", "final_case.schema.json")


SEARCH_KINDS = ("News", "Flash", "Robot", "Post", "Article", "Report", "teleconference", "AI_search")
SEARCH_INDEX_FILES = {  # 类目 -> index parquet 文件名
    "News": "v1_US_NEWS", "Flash": "v1_US_FLASH", "Robot": "v1_US_ROBOT",
    "Post": "v1_US_POST", "Article": "v1_US_ARTICLE",
    "Report": "v2_report", "teleconference": "v2_teleconference",
}
SEARCH_WINDOW = (-3, 1)   # 检索窗: 事件日前 3 天 ~ 后 1 天(峰值报道; 不引入更晚的事后反应)
SEARCH_CAP = 10           # 每类目上限
SEARCH_SNIPPET_CHARS = 200  # summary 摘录长度(seek 直读原文截取)
_V2_KINDS = ("Report", "teleconference")
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")
_SYM_RE = re.compile(r"^[A-Z0-9.\-]{1,8}$")
# 公司名尾缀, 生成标题匹配词时剥掉
_NAME_SUFFIX_RE = re.compile(
    r",?\s+(Inc|Corp|Corporation|Company|Co|Ltd|LLC|plc|Group|Holdings?|Technologies|"
    r"Therapeutics|Pharmaceuticals|Financial|Services)\.?$", re.I)


class RelatedSearcher:
    """按 (primary symbol / 公司名, 事件日窗口) 在 index parquet 里检索各类目相关记录.

    NEWS 索引本导出无 symbols(entities.stocks 缺失), 靠公司名词边界匹配标题;
    其余类目 symbol 与标题双通道。index 目录缺失时 available=False, 调用方退回纯溯源。
    """

    def __init__(self, index_dir: str | None = None):
        self.index_dir = index_dir or config.EVENT_INDEX_DIR
        self.tables = {k: f"{self.index_dir}/{f}.parquet"
                       for k, f in SEARCH_INDEX_FILES.items()
                       if os.path.exists(f"{self.index_dir}/{f}.parquet")}
        self.con = None
        self._cols: dict[str, set[str]] = {}
        if self.tables:
            import duckdb
            self.con = duckdb.connect()

    @property
    def available(self) -> bool:
        return bool(self.con)

    def _columns(self, path: str) -> set[str]:
        if path not in self._cols:
            self._cols[path] = {r[0] for r in
                                self.con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()}
        return self._cols[path]

    def search(self, symbols: list[str], name: str, event_date: str) -> dict[str, list[dict]]:
        if not self.con or not event_date:
            return {}
        from datetime import date, timedelta
        try:
            d = date.fromisoformat(event_date)
        except ValueError:
            return {}
        d0 = (d + timedelta(days=SEARCH_WINDOW[0])).isoformat()
        d1 = (d + timedelta(days=SEARCH_WINDOW[1])).isoformat()
        syms = [s for s in symbols if isinstance(s, str) and _SYM_RE.match(s)]
        conds = [f"(',' || symbols || ',') LIKE '%,{s},%'" for s in syms]
        pat = _title_pattern(name).replace("'", "''")  # SQL 字面量内单引号转义
        if pat:
            conds.append(f"regexp_matches(title, '(?i)\\b{pat}\\b')")
        if not conds:
            return {}
        cond = " OR ".join(conds)
        out = {}
        for kind, path in self.tables.items():
            cols = self._columns(path)
            # seek 直读原文所需的定位列(index parquet 均有; 缺失时退化为只出 title/时间)
            extra = ", file, \"offset\", nbytes" if {"file", "offset", "nbytes"} <= cols else \
                    ", file, offsets" if {"file", "offsets"} <= cols else ""
            rows = self.con.execute(f"""
              SELECT title, published_at{extra} FROM read_parquet('{path}')
              WHERE pub_date BETWEEN '{d0}' AND '{d1}' AND ({cond})
              ORDER BY published_at LIMIT {SEARCH_CAP}
            """).fetchall()
            items = []
            for row in rows:
                it = {"title": str(row[0] or ""), "summary": "", "published_at": row[1], "url": None}
                if len(row) == 5:    # v1: file/offset/nbytes
                    it["url"], it["summary"] = self._read_v1(row[2], row[3], row[4])
                elif len(row) == 4:  # v2: file/offsets(逗号串, 取首段)
                    it["url"], it["summary"] = self._read_v2(row[2], row[3])
                items.append(it)
            out[kind] = items
        return out

    def _read_v1(self, fname: str, offset: int, nbytes: int) -> tuple[str | None, str]:
        """seek 直读 v1 原始行, 取 source.url(退回 dedup.debug 的规范化 url) + body 摘录."""
        try:
            with open(os.path.join(config.EVENT_V1_DIR, fname), "rb") as fh:
                fh.seek(offset)
                rec = json.loads(fh.read(nbytes))
        except Exception:
            return None, ""
        src = rec.get("source")
        url = src.get("url") if isinstance(src, dict) else None
        if not url or url == "#":
            dbg = (rec.get("dedup") or {}).get("debug") or {}
            url = dbg.get("normalized_url") or dbg.get("source_url") or None
        return url, _snippet(rec.get("body") or "")

    def _read_v2(self, fname: str, offsets: str) -> tuple[str | None, str]:
        """seek 直读 v2 首段(offsets 为逗号串), 取 source.url + text 摘录."""
        try:
            first = int(str(offsets).split(",")[0])
            with open(os.path.join(config.EVENT_V2_DIR, fname), "rb") as fh:
                fh.seek(first)
                rec = json.loads(fh.readline())
        except Exception:
            return None, ""
        src = rec.get("source")
        url = src.get("url") if isinstance(src, dict) else None
        if not url and isinstance(src, str):
            m = re.search(r"https?://[^\s'\"}]+", src)
            url = m.group(0) if m else None
        return url, _snippet(rec.get("text") or "")


def _snippet(text: str) -> str:
    """正文/段落 -> summary 摘录: 去 HTML 标签压缩空白后截取."""
    return _WS_RE.sub(" ", _TAG_RE.sub(" ", text)).strip()[:SEARCH_SNIPPET_CHARS]


def _title_pattern(name: str) -> str:
    """公司名/主体 -> 标题匹配的正则片段(取前两词, 转义, 过短则弃用避免噪声)."""
    n = (name or "").strip()
    while True:  # 尾缀可能叠加(如 "Ionis Pharmaceuticals, Inc."), 剥到不动为止
        stripped = _NAME_SUFFIX_RE.sub("", n).strip()
        if stripped == n or not stripped:
            break
        n = stripped
    words = n.split()[:2]
    pat = re.escape(" ".join(words)).replace(r"\ ", r"\s+")
    return pat if len("".join(words)) >= 4 else ""


def _primary_company(r: dict) -> str:
    """事件主体的检索名: 主 symbol 在 relation_rows 里的公司名, 退回 event_subject."""
    prim = ((r.get("_triage") or {}).get("primary_symbols") or [None])[0]
    for row in r.get("relation_rows") or []:
        if row.get("symbol") == prim and row.get("company"):
            return str(row["company"])
    return str((r.get("main_event") or {}).get("event_subject") or "")


def association_search(r: dict, searcher: "RelatedSearcher | None" = None) -> dict:
    """事件相关信息检索 + 结构化实际引用来源的溯源合并, 每类目按时间升序。"""
    result = {k: [] for k in SEARCH_KINDS}
    mapping = {
        "US_NEWS": "News", "US_FLASH": "Flash", "US_ROBOT": "Robot",
        "US_POST": "Post", "US_ARTICLE": "Article", "REPORT": "Report",
        "TELECONFERENCE": "teleconference",
    }
    # 1) 结构化实际用过的来源(溯源, 8-K 公告带 EDGAR url; US_NOTICE 归入 News 类目)
    for item in r.get("_source_meta") or []:
        kind = mapping.get(str(item.get("content_type") or "").upper(), "News")
        result[kind].append({
            "title": str(item.get("title") or ""),
            "summary": "",
            "published_at": item.get("published_at"),
            "url": item.get("url"),
        })
    # 2) 按 (primary symbol/公司名, 事件日窗口) 检索各库回填
    if searcher and searcher.available:
        tri = r.get("_triage") or {}
        event_date = (r.get("main_event") or {}).get("event_date") or tri.get("event_date") or ""
        found = searcher.search(tri.get("primary_symbols") or [], _primary_company(r), event_date)
        for kind, items in found.items():
            result[kind].extend(items)
    # 3) 类目内去重(按标题前缀) + 按时间升序 + 截断
    for kind, items in result.items():
        seen, uniq = set(), []
        for it in sorted(items, key=lambda x: str(x.get("published_at") or "")):
            key = re.sub(r"\s+", " ", it["title"].lower())[:80]
            if key in seen:
                continue
            seen.add(key)
            uniq.append(it)
        result[kind] = uniq[:SEARCH_CAP]
    return result


def schema_issues(case: dict) -> list[str]:
    with open(SCHEMA_PATH) as fh:
        validator = jsonschema.Draft7Validator(json.load(fh))
    issues = []
    for err in sorted(validator.iter_errors(case), key=lambda e: list(e.absolute_path)):
        path = ".".join(str(p) for p in err.absolute_path) or "$"
        issues.append(f"schema {path}: {err.message}")
    return issues


def complete_intraday_panel(panel: dict, event_date: str) -> bool:
    bars = panel.get("bars") or []
    stamps = [str(b.get("timestamp_et") or "") for b in bars]
    return bool(
        panel.get("is_complete_session") is True
        and panel.get("bar_count") == len(bars)
        and len(bars) >= 380
        and stamps == sorted(set(stamps))
        and stamps[0].startswith(f"{event_date} ")
        and stamps[-1].startswith(f"{event_date} ")
        and stamps[0][11:16] <= "09:35"
        and stamps[-1][11:16] >= "15:55"
    )


def leakage_scan(facts: list[dict], event_date: str) -> list[str]:
    hits = []
    for f in facts:
        blob = " ".join(str(f.get(k, "")) for k in ("metric", "value", "context"))
        for d in ISO_DATE_RE.findall(blob):
            if d > event_date:
                hits.append(f"fact 含未来日期 {d}: {blob[:80]}")
    return hits


def assemble(r: dict, mk: dict, intraday: dict | None = None,
             allow_no_intraday: bool = False,
             searcher: RelatedSearcher | None = None) -> tuple[dict | None, list[str]]:
    """structured 记录 + market 记录 -> (case_json, 审计问题列表)."""
    issues = []
    me = r.get("main_event") or {}
    event_date = me.get("event_date") or r["_triage"]["event_date"]
    facts = me.get("facts_publicly_reported") or []
    # LLM 偶尔把 channels 输出成字符串数组而非 {"channel": ...} 对象数组, 两种形状都收
    channels = [c.get("channel") if isinstance(c, dict) else c
                for c in me.get("event_influence_channels") or []]
    channels = [c for c in channels if isinstance(c, str) and c]
    rel_rows = r.get("relation_rows") or []
    labels = mk["labels"]
    priced = set(mk["priced_symbols"])

    if len(facts) < 2 or len(channels) < 2 or len(rel_rows) < 5:
        issues.append(f"结构不完整 facts={len(facts)} channels={len(channels)} rels={len(rel_rows)}")
        return None, issues
    leaks = leakage_scan(facts, event_date)
    if leaks:
        issues.extend(leaks)

    case_id = (r.get("case_id") or "").strip().upper().replace("-", "_")
    if not CASE_ID_RE.match(case_id):
        case_id = f"{(r['_triage'].get('primary_symbols') or ['MACRO'])[0]}_{r['event_id']}"
    case_title = r.get("case_title") or r["_triage"].get("title_cn") or ""

    # 以最多数标的共享的 base/ft 日期做全案日历
    base_dates = Counter(l["base_close_date"] for l in labels)
    base_date = base_dates.most_common(1)[0][0]
    ref = next(l for l in labels if l["base_close_date"] == base_date)
    cal = {
        "event_natural_date": event_date,
        "base_close_date": base_date,
        "first_tradable_session": ref["first_tradable_session"],
        "label_dates": ref["horizon_dates"],
        "label_window": f"{ref['first_tradable_session']}..{ref['horizon_dates']['20d']}",
        "artifact_dates_are_not_model_time": True,
        "leakage_boundary": LEAKAGE_BOUNDARY,
    }
    sym_panel = mk["market_data_symbols"]
    src_ids = [f"SRC_{m['id']}" for m in r.get("_source_meta") or []][:8]

    evidence_rows = []
    for row in rel_rows:
        sym = (row.get("symbol") or "").strip().upper()
        if not sym:
            continue
        is_priced = sym in priced
        status = "priced_and_labeled" if is_priced else "unpriced_weak_candidate_needs_price_panel"
        trace = [{"label": "主事件官方源", "url": me.get("official_source_url") or "", "type": "official"},
                 {"label": "候选标的关系表", "url": "#relationEvidence", "type": "local_ui"}]
        evidence_rows.append({
            "symbol": sym, "company": row.get("company") or sym,
            "priced_label_status": status,
            "relation_type": row.get("relation_type") or "related",
            "relation_path": row.get("relation_path") or [me.get("event_subject") or "", r.get("event_family") or "", sym],
            "evidence_statement": row.get("evidence_statement") or "",
            "evidence_ids": src_ids[:1] or ["SRC_UNKNOWN"],
            "training_treatment": TRAINING_TREATMENT if is_priced else
                "weak relation candidate; procure price panel before using as supervised target",
            "manual_strength_score": None, "manual_direction_label": None,
            "relation_type_cn": row.get("relation_type_cn") or RELATION_CN_DEFAULT,
            "impact_path_cn": row.get("impact_path_cn") or "",
            "source_trace": trace,
            "training_treatment_cn": TREATMENT_PRICED_CN if is_priced else TREATMENT_UNPRICED_CN,
        })

    n_priced = sum(1 for e in evidence_rows if e["priced_label_status"] == "priced_and_labeled")
    if n_priced < 3:
        issues.append(f"已打价标的不足 3 个: {n_priced}")
        return None, issues

    intraday = intraday or {}
    intraday_symbols = {
        s: p for s, p in (intraday.get("symbols") or {}).items()
        if s in priced and complete_intraday_panel(p, event_date)
    }
    if not intraday_symbols and not allow_no_intraday:
        issues.append("缺少已打价标的的完整事件日 1m 分时面板")
        return None, issues
    if not intraday_symbols:
        issues.append("intraday_missing: 缺完整 1m 面板, 降级组装为空占位")
        intraday_provider = "missing"
    else:
        intraday_provider = intraday.get("provider") or "unknown"

    case = {
        "schema": SCHEMA,
        "case_id": case_id,
        "case_title": case_title,
        "event_family": r.get("event_family") or r["_triage"].get("event_family") or "other",
        "main_event": {
            "event_id": case_id,
            "event_subject": me.get("event_subject") or "",
            "event_subject_type": me.get("event_subject_type") or "",
            "event_type": r.get("event_type") or r["_triage"].get("event_type") or "other",
            "event_date": event_date,
            "title": case_title,
            "source_ids": src_ids,
            "official_source_url": me.get("official_source_url"),
            "facts_publicly_reported": facts,
            "event_influence_channels": [
                {"channel": c, "manual_direction": None,
                 "training_role": "learnable_channel_not_manual_score"} for c in channels],
        },
        "supervised_targets_hidden_labels": {
            "label_base": "adjusted close-to-close and tradable open-to-close",
            "label_count": len(labels),
            "labels": labels,
            "metrics_to_train": METRICS_TO_TRAIN,
        },
        "time_dimension_calibration": cal,
        "target_relation_evidence": {"policy": RELATION_POLICY, "rows": evidence_rows},
        "market_data": {"provider": "Yahoo Finance via yfinance", "adjustment": "auto_adjust=True",
                        "symbols": sym_panel},
        "intraday_volume_panel": {
            "provider": intraday_provider,
            "timezone": intraday.get("timezone") or "America/New_York",
            "event_date": event_date,
            "interval": "1m",
            "session_scope": intraday.get("session_scope") or "regular_session_09:30_16:00_ET",
            "symbols": intraday_symbols,
        },
        "associatin_search": association_search(r, searcher),
    }
    validation_issues = schema_issues(case)
    if validation_issues:
        issues.extend(validation_issues)
        return None, issues
    return case, issues


def run(args) -> None:
    os.makedirs(config.EVENT_FINAL_DIR, exist_ok=True)
    date = getattr(args, "date", None)
    allow_no_intraday = getattr(args, "allow_no_intraday", False)

    structured = {}
    with open(f"{config.EVENT_STRUCTURED_DIR}/structured.jsonl") as fh:
        for line in fh:
            r = json.loads(line)
            if not r.get("_error"):
                structured[r["event_id"]] = r
    if date:
        structured = {eid: r for eid, r in structured.items()
                      if (r.get("_triage") or {}).get("peak_date") == date}
    markets = {}
    with open(f"{config.EVENT_MARKET_DIR}/labels.jsonl") as fh:
        for line in fh:
            m = json.loads(line)
            markets[m["event_id"]] = m
    intraday = {}
    intraday_path = f"{config.EVENT_MARKET_DIR}/intraday.jsonl"
    if os.path.exists(intraday_path):
        with open(intraday_path) as fh:
            for line in fh:
                item = json.loads(line)
                intraday[item["event_id"]] = item

    searcher = RelatedSearcher()
    if not searcher.available:
        print("[assemble] index parquet 缺失, associatin_search 仅回填结构化引用来源", flush=True)
    n_ok = n_drop = 0
    seen_case_ids = set()
    all_issues = []
    out = open(f"{config.EVENT_FINAL_DIR}/events.jsonl", "w")
    for eid, r in structured.items():
        if args.max_cases and n_ok >= args.max_cases:
            break
        mk = markets.get(eid)
        if not mk:
            n_drop += 1
            continue
        case, issues = assemble(r, mk, intraday.get(eid), allow_no_intraday, searcher)
        if issues:
            all_issues.append({"event_id": eid, "issues": issues})
        if case is None:
            n_drop += 1
            continue
        if case["case_id"] in seen_case_ids:
            case["case_id"] = f"{case['case_id']}_{eid[-4:]}"
            case["main_event"]["event_id"] = case["case_id"]
        seen_case_ids.add(case["case_id"])
        out.write(json.dumps(case, ensure_ascii=False) + "\n")
        n_ok += 1
    out.close()

    summary = {"structured_in": len(structured), "with_market": sum(1 for e in structured if e in markets),
               "cases_written": n_ok, "dropped": n_drop, "events_with_issues": len(all_issues)}
    with open(f"{config.EVENT_REPORT_DIR}/stage_assemble_summary.json", "w") as fh:
        json.dump({**summary, "issues": all_issues[:200]}, fh, ensure_ascii=False, indent=2)
    print(json.dumps(summary, ensure_ascii=False), flush=True)
