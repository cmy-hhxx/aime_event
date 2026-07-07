"""Stage F: 组装 FinancialPredictionTrainingCase.v4 格式成品 + 泄露/完整性审计.

输入: structured/structured.jsonl + market/labels.jsonl
输出: final/<case_id>.json (每事件一个), final/manifest.jsonl, reports/stage_assemble_summary.json

增强层(分时面板/弱关联搜索链/混杂审计)按用户决策留空, 但保留 schema 占位与 status 标记,
与样例中 intraday_volume_panel.status="missing_..." 的做法一致。
"""
from __future__ import annotations

import json
import os
import re
from collections import Counter

from src import config

SCHEMA = "FinancialPredictionTrainingCase.v4.three_year_event_signal_pack"
OBJECTIVE = ("训练金融预测模型：从真实事件事实、事件前行情和关系证据，预测多标的 1D/5D/20D "
             "hidden return labels，并用弱关联搜索链做混杂审计。")
LEAKAGE_BOUNDARY = ("S0 pre-event market and S1 official event facts are model-visible; "
                    "S2/S3/S4 prices, follow-on news, analyst reactions and social hindsight "
                    "are hidden labels/audit only.")
RELATION_POLICY = "relation text is visible, but numeric direction/strength labels are not manually assigned"
WEAK_POLICY = ("expanded weak symbols are candidate relation context only; unpriced symbols "
               "require price procurement before becoming supervised targets")
TRAINING_TREATMENT = "model-visible relation context; future direction learned from hidden labels"
TREATMENT_PRICED_CN = "已打价：进入 1D/5D/20D 未来收益标签；方向和强度由标签学习。"
TREATMENT_UNPRICED_CN = "待补价：先作弱关联召回，补齐行情面板后再决定是否进入监督标签。"

DATA_DIMENSION_CONTRACT = [
    {"dimension": "event_fact", "field_path": "main_event.facts_publicly_reported",
     "training_role": "model_visible_event_text", "rule": "仅使用事件时点可见的官方/权威事实。"},
    {"dimension": "pre_event_market", "field_path": "market_data.symbols.*.model_input_ohlcv_adjusted_daily",
     "training_role": "model_visible_sequence", "rule": "截止 base_close_date，首个反应交易日之后不可见。"},
    {"dimension": "intraday_full_session_volume", "field_path": "intraday_volume_panel + market_data.symbols.*.intraday_1m_full_session",
     "training_role": "model_visible_or_hidden_by_visibility_mask",
     "rule": "必须存全天 1m 分时和成交量，但输入时按 event_timestamp_et 做可见性掩码；事件后分时只能用于动态更新任务或隐藏审计。"},
    {"dimension": "relation_context", "field_path": "target_relation_evidence.rows",
     "training_role": "model_visible_relation_context", "rule": "写明入池理由，但不打人工方向/强度。"},
    {"dimension": "future_return", "field_path": "supervised_targets_hidden_labels.labels",
     "training_role": "hidden_supervision_and_eval", "rule": "1D/5D/20D 标签仅用于监督和评测。"},
    {"dimension": "weak_association_events", "field_path": "weak_association_ai_search.events",
     "training_role": "search_candidate_or_hidden_audit", "rule": "多轮 AI 搜索得到的弱关联事件需按时间门控，窗口内只审计或另建样本。"},
]

PREDICTION_TASK = {
    "task_name": "event_to_multi_target_return_prediction",
    "model_input_should_include": ["official event facts", "event family/type",
                                   "pre-event OHLCV/features", "target relation context", "source ids"],
    "model_input_must_not_include": ["1D/5D/20D labels", "follow-on events after leakage boundary",
                                     "analyst hindsight", "manual event impact direction",
                                     "manual relation strength"],
    "supervised_targets": ["direction", "return interval", "cross-section rank", "abnormal return vs proxy"],
}

NOT_IN_SAMPLE = [
    {"data_block": "intraday_full_session_price_volume",
     "reason": "现有日频 OHLCV 无法看到全天分时、分钟成交量、事件前后成交量突变和尾盘确认。",
     "effect_if_missing": "模型会把“全天发生过的反应”压缩成一个日 K，无法学习发布前交易、发布后 5/30/120 分钟传导、相关标的先后反应和成交量确认。",
     "procurement_or_build_action": "P0：接入历史 1m 全日 OHLCV+volume+VWAP；每个事件至少覆盖 target_symbols、事件日全场、20 个前序交易日同分钟成交量基线。"},
    {"data_block": "options_implied_move",
     "reason": "缺少事件前预期差，模型只能从历史价格间接推断。",
     "effect_if_missing": "预测置信区间和方向校准较弱。",
     "procurement_or_build_action": "接入期权 IV/skew/volume。"},
    {"data_block": "news_social_trend_tape",
     "reason": "弱关联候选需要 Google/Perplexity/X/趋势热榜多轮搜索。",
     "effect_if_missing": "传播速度、叙事强度和二阶标的发现不足。",
     "procurement_or_build_action": "建立事件后 1h/1d/5d social/news tape。"},
]

METRICS_TO_TRAIN = ["direction", "return interval", "cross-section rank", "abnormal return vs proxy"]

# 泄露审计: facts 文本中出现晚于事件日的 ISO 日期
ISO_DATE_RE = re.compile(r"20\d{2}-\d{2}-\d{2}")

RELATION_CN_DEFAULT = "事件相关候选"
CASE_ID_RE = re.compile(r"^[A-Z0-9][A-Z0-9_]{5,80}$")


def slice_defs(cal: dict, pre_start: str) -> list[dict]:
    return [
        {"slice_id": "S0_PRE_EVENT_MARKET_CONTEXT", "display": "事件前行情上下文",
         "time_range": f"{pre_start} -> {cal['base_close_date']} close", "visible_to_model": True,
         "contains": ["pre-event OHLCV", "compact price features", "candidate target universe"],
         "not_contains": ["future prices", "future labels"], "training_role": "模型输入窗口"},
        {"slice_id": "S1_EVENT_TEXT", "display": "事件事实进入",
         "time_range": cal["event_natural_date"], "visible_to_model": True,
         "contains": ["official event facts", "source ids", "event type and subject"],
         "not_contains": ["post-event analyst reactions", "future labels"], "training_role": "模型输入事件文本"},
        {"slice_id": "S2_1D_LABEL", "display": "1D 反应标签",
         "time_range": cal["label_dates"]["1d"], "visible_to_model": False,
         "contains": ["1D close-to-close", "open gap"], "not_contains": ["model input"],
         "training_role": "hidden label"},
        {"slice_id": "S3_5D_LABEL", "display": "5D 标签",
         "time_range": f"{cal['first_tradable_session']} -> {cal['label_dates']['5d']}",
         "visible_to_model": False, "contains": ["5D return labels", "early drift"],
         "not_contains": ["model input"], "training_role": "hidden label"},
        {"slice_id": "S4_20D_LABEL_AND_AUDIT", "display": "20D 标签与混杂审计",
         "time_range": f"{cal['first_tradable_session']} -> {cal['label_dates']['20d']}",
         "visible_to_model": False,
         "contains": ["20D return labels", "weak associated follow-on candidates", "confounder audit"],
         "not_contains": ["model input"], "training_role": "hidden eval + attribution audit"},
    ]


def leakage_scan(facts: list[dict], event_date: str) -> list[str]:
    hits = []
    for f in facts:
        blob = " ".join(str(f.get(k, "")) for k in ("metric", "value", "context"))
        for d in ISO_DATE_RE.findall(blob):
            if d > event_date:
                hits.append(f"fact 含未来日期 {d}: {blob[:80]}")
    return hits


def assemble(r: dict, mk: dict) -> tuple[dict | None, list[str]]:
    """structured 记录 + market 记录 -> (case_json, 审计问题列表)."""
    issues = []
    me = r.get("main_event") or {}
    event_date = me.get("event_date") or r["_triage"]["event_date"]
    facts = me.get("facts_publicly_reported") or []
    channels = [c.get("channel") for c in me.get("event_influence_channels") or [] if c.get("channel")]
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
    pre_start = None
    for s, panel in sym_panel.items():
        bars = panel["model_input_ohlcv_adjusted_daily"]
        if bars:
            d = bars[0]["date"]
            pre_start = d if pre_start is None or d < pre_start else pre_start
    src_ids = [f"SRC_{m['id']}" for m in r.get("_source_meta") or []][:8]

    evidence_rows, weak_universe = [], []
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
        weak_universe.append({
            "symbol": sym, "status": "priced_target" if is_priced else "weak_unpriced_candidate",
            "relation_type": row.get("relation_type") or "related",
            "why": row.get("impact_path_cn") or row.get("evidence_statement") or "",
            "suggested_data_source": "already_priced_in_market_data" if is_priced
                                      else "price_vendor + official/news/social search",
            "priority": "P0" if is_priced else "P1",
        })

    n_priced = sum(1 for e in evidence_rows if e["priced_label_status"] == "priced_and_labeled")
    if n_priced < 3:
        issues.append(f"已打价标的不足 3 个: {n_priced}")
        return None, issues

    case = {
        "schema": SCHEMA,
        "case_id": case_id,
        "case_title": case_title,
        "display_short_name": r.get("display_short_name") or case_id.split("_")[0],
        "year": int(event_date[:4]),
        "event_family": r.get("event_family") or r["_triage"].get("event_family") or "other",
        "objective": OBJECTIVE,
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
        "time_dimension_calibration": cal,
        "data_dimension_contract": DATA_DIMENSION_CONTRACT,
        "training_time_slices": slice_defs(cal, pre_start or ""),
        "target_relation_evidence": {"policy": RELATION_POLICY, "rows": evidence_rows},
        "market_data": {"provider": "Yahoo Finance via yfinance", "adjustment": "auto_adjust=True",
                        "symbols": sym_panel},
        "pre_event_price_features": {},
        "event_text_features": {
            "event_family": r.get("event_family") or "", "event_type": r.get("event_type") or "",
            "channels": channels,
            "instruction": "Use only S0/S1 visible facts and relation context. "
                           "Predict hidden 1D/5D/20D returns and rank target symbols.",
        },
        "prediction_training_task": PREDICTION_TASK,
        "implemented_data_manifest": [
            {"data_block": "main_event_facts", "status": "implemented"},
            {"data_block": "market_ohlcv_panel", "status": "implemented"},
            {"data_block": "future_return_labels", "status": "implemented"},
            {"data_block": "weak_association_search_chain", "status": "pending_enhancement_layer"},
            {"data_block": "expanded_weak_relation_universe", "status": "implemented_structure_only"},
            {"data_block": "intraday_full_session_volume_panel", "status": "required_p0_schema_ready_data_missing"},
        ],
        "intraday_volume_panel": {
            "status": "missing_real_intraday_for_event_date",
            "current_sample_gap": "流水线 v1 只填日频 OHLCV；分钟分时面板留待增强层。",
            "next_build_action": "按 target_symbols 拉取 event_date 全天 1m bars 并补 20 日分钟基线。",
        },
        "label_window_confounder_audit": [],
        "weak_association_ai_search": {
            "principle": "搜索链分两段：R1-R2 下钻主事件事实、时间和影响通道；R3-R4 外扩弱关联事件、"
                         "标的和社媒传播；R5 做防泄露、混杂和反事实样本控制。",
            "rounds": [], "events": [],
            "status": "pending_enhancement_layer",
        },
        "weak_relation_universe": {
            "policy": WEAK_POLICY,
            "priced_target_count": n_priced,
            "weak_symbol_count": len(weak_universe) - n_priced,
            "symbols": weak_universe,
        },
        "supervised_targets_hidden_labels": {
            "label_base": "adjusted close-to-close and tradable open-to-close",
            "label_count": len(labels),
            "labels": labels,
            "metrics_to_train": METRICS_TO_TRAIN,
        },
        "not_in_current_sample": NOT_IN_SAMPLE,
        "quality_audit": {
            "official_event_source_count": 1 if me.get("official_source_url") else 0,
            "priced_target_count": n_priced,
            "relation_row_count": len(evidence_rows),
            "weak_candidate_event_count": 0,
            "confounder_audit_seed_count": 0,
            "label_complete_1d_5d_20d": all(
                all(k in l["close_to_close_return_pct"] for k in ("1d", "5d", "20d")) for l in labels),
            "train_ready_level": "main_chain_ready_enhancement_pending",
            "known_limitations": ["daily prices only",
                                  "weak association search chain and confounder audit left empty in pipeline v1"],
            "leakage_scan_hits": leaks,
            "structuring_confidence": r.get("confidence"),
            "pipeline": "event_dataset v1 (index->cluster->triage->structure->label->assemble)",
        },
        "provenance": {
            "event_id_internal": r["_triage"] and r["event_id"],
            "cluster_stats": {k: r["_triage"].get(k) for k in
                              ("n_articles", "n_sources", "n_v2_reactions", "significance", "peak_date")},
            "source_articles": r.get("_source_meta") or [],
        },
    }
    return case, issues


def run(args) -> None:
    os.makedirs(config.EVENT_FINAL_DIR, exist_ok=True)

    structured = {}
    with open(f"{config.EVENT_STRUCTURED_DIR}/structured.jsonl") as fh:
        for line in fh:
            r = json.loads(line)
            if not r.get("_error"):
                structured[r["event_id"]] = r
    markets = {}
    with open(f"{config.EVENT_MARKET_DIR}/labels.jsonl") as fh:
        for line in fh:
            m = json.loads(line)
            markets[m["event_id"]] = m

    n_ok = n_drop = 0
    seen_case_ids = set()
    all_issues = []
    manifest = open(f"{config.EVENT_FINAL_DIR}/manifest.jsonl", "w")
    for eid, r in structured.items():
        if args.max_cases and n_ok >= args.max_cases:
            break
        mk = markets.get(eid)
        if not mk:
            n_drop += 1
            continue
        case, issues = assemble(r, mk)
        if issues:
            all_issues.append({"event_id": eid, "issues": issues})
        if case is None:
            n_drop += 1
            continue
        if case["case_id"] in seen_case_ids:
            case["case_id"] = f"{case['case_id']}_{eid[-4:]}"
            case["main_event"]["event_id"] = case["case_id"]
        seen_case_ids.add(case["case_id"])
        with open(f"{config.EVENT_FINAL_DIR}/{case['case_id']}.json", "w") as fh:
            json.dump(case, fh, ensure_ascii=False, indent=1)
        manifest.write(json.dumps({
            "case_id": case["case_id"], "event_id": eid, "event_date": case["main_event"]["event_date"],
            "event_type": case["main_event"]["event_type"], "event_family": case["event_family"],
            "year": case["year"], "n_priced": case["quality_audit"]["priced_target_count"],
            "title": case["case_title"],
        }, ensure_ascii=False) + "\n")
        n_ok += 1
    manifest.close()

    summary = {"structured_in": len(structured), "with_market": sum(1 for e in structured if e in markets),
               "cases_written": n_ok, "dropped": n_drop, "events_with_issues": len(all_issues)}
    with open(f"{config.EVENT_REPORT_DIR}/stage_assemble_summary.json", "w") as fh:
        json.dump({**summary, "issues": all_issues[:200]}, fh, ensure_ascii=False, indent=2)
    print(json.dumps(summary, ensure_ascii=False), flush=True)
