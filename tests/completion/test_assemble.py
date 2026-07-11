import json
from datetime import datetime, timedelta
from pathlib import Path

import jsonschema

from src.completion import assemble

STRUCTURED = {
    "event_id": "EVT_1", "case_id": "ACME_TEST_20250110", "case_title": "T",
    "display_short_name": "Acme", "event_family": "f", "event_type": "earnings_release",
    "confidence": 0.9,
    "main_event": {"event_subject": "Acme", "event_subject_type": "company",
                   "event_date": "2025-01-10", "official_source_url": None,
                   "facts_publicly_reported": [
                       {"metric": "m1", "value": "v1", "context": "c1"},
                       {"metric": "m2", "value": "v2", "context": "c2"}],
                   "event_influence_channels": [{"channel": "c_a"}, {"channel": "c_b"}]},
    "event_timestamp_et": {"session_bucket": "after_market"},
    "relation_rows": [{"symbol": s, "company": s, "relation_type": "peer",
                       "relation_path": ["Acme", "f", s], "evidence_statement": "影响链: x",
                       "relation_type_cn": "同业/替代品", "impact_path_cn": "x"}
                      for s in ("AAA", "BBB", "CCC", "DDD", "EEE")],
    "_triage": {"event_date": "2025-01-10", "primary_symbols": ["AAA"], "n_articles": 6,
                "n_sources": 3, "n_v2_reactions": 1, "significance": 4, "peak_date": "2025-01-10"},
    "_source_meta": [{"id": "a1", "pub_date": "2025-01-10", "published_at": "x",
                      "source": "Reuters", "title": "t"}],
}


def _label(sym):
    return {"symbol": sym, "base_close_date": "2025-01-10", "base_close": 100.0,
            "first_tradable_session": "2025-01-13",
            "close_to_next_open_gap_pct": 1.0,
            "close_to_close_return_pct": {"1d": 1.0, "5d": 2.0, "20d": 3.0},
            "tradable_open_to_close_return_pct": {"1d": 0.5, "5d": 1.5, "20d": 2.5},
            "horizon_dates": {"1d": "2025-01-13", "5d": "2025-01-17", "20d": "2025-02-07"},
            "is_hidden_from_model_input": True, "cross_section_rank_20d": 1}

def _audit_bars():
    return [{"date": f"2025-02-{i:02d}", "open": 1, "high": 1,
             "low": 1, "close": 1, "volume": 1} for i in range(1, 21)]


MARKET = {"event_id": "EVT_1", "event_date": "2025-01-10", "session_bucket": "after_market",
          "priced_symbols": ["AAA", "BBB", "CCC"], "unpriced_symbols": ["DDD", "EEE"],
          "labels": [_label("AAA"), _label("BBB"), _label("CCC")],
          "market_data_symbols": {s: {"model_input_ohlcv_adjusted_daily":
                                      [{"date": "2025-01-10", "open": 1, "high": 1,
                                        "low": 1, "close": 1, "volume": 1}],
                                      "label_audit_ohlcv_adjusted_daily": _audit_bars()}
                                  for s in ("AAA", "BBB", "CCC")}}

def _minute_bars():
    start = datetime(2025, 1, 10, 9, 30)
    return [{"timestamp_et": (start + timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S"),
             "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1,
             "vwap": 1, "trade_count": 0, "dollar_volume": 1} for i in range(390)]


INTRADAY = {
    "event_id": "EVT_1", "event_date": "2025-01-10",
    "provider": "Yahoo Finance via yfinance", "timezone": "America/New_York",
    "interval": "1m", "session_scope": "regular_session_09:30_16:00_ET",
    "symbols": {"AAA": {
        "session_start_et": "2025-01-10 09:30:00",
        "session_end_et": "2025-01-10 15:59:00",
        "bar_count": 390, "is_complete_session": True,
        "bars": _minute_bars(),
    }},
}


def test_leakage_scan_future_date():
    hits = assemble.leakage_scan([{"metric": "m", "value": "涨到 2025-02-01", "context": ""}],
                                 "2025-01-10")
    assert len(hits) == 1
    assert assemble.leakage_scan([{"metric": "m", "value": "截至 2025-01-09", "context": ""}],
                                 "2025-01-10") == []


SCHEMA_TOP_KEYS = [
    "schema", "case_id", "case_title", "event_family", "main_event",
    "supervised_targets_hidden_labels", "time_dimension_calibration",
    "target_relation_evidence", "market_data", "intraday_volume_panel",
    "associatin_search",
]


def test_assemble_produces_v4_case():
    case, issues = assemble.assemble(STRUCTURED, MARKET, INTRADAY)
    assert case is not None and issues == []
    assert case["schema"].startswith("FinancialPredictionTrainingCase.v4")
    assert case["time_dimension_calibration"]["first_tradable_session"] == "2025-01-13"
    rows = case["target_relation_evidence"]["rows"]
    assert {r["symbol"]: r["priced_label_status"] for r in rows}["DDD"] == \
        "unpriced_weak_candidate_needs_price_panel"
    assert case["intraday_volume_panel"]["symbols"]["AAA"]["is_complete_session"] is True
    assert case["associatin_search"]["News"][0]["title"] == "t"
    # 严格模式: 顶层字段与 schema 定义完全一致(含顺序), 不允许额外块
    assert list(case.keys()) == SCHEMA_TOP_KEYS
    schema_path = Path(__file__).parents[2] / "schema/completion/final_case.schema.json"
    jsonschema.validate(case, json.loads(schema_path.read_text()))


def test_assemble_rejects_too_few_priced():
    mk = dict(MARKET, priced_symbols=["AAA"], labels=[_label("AAA")],
              market_data_symbols={"AAA": MARKET["market_data_symbols"]["AAA"]})
    case, issues = assemble.assemble(STRUCTURED, mk, INTRADAY)
    assert case is None and any("3" in i for i in issues)


def test_assemble_rejects_missing_intraday():
    case, issues = assemble.assemble(STRUCTURED, MARKET, None)
    assert case is None and any("1m" in i for i in issues)


def test_assemble_allow_no_intraday_downgrades():
    case, issues = assemble.assemble(STRUCTURED, MARKET, None, allow_no_intraday=True)
    assert case is not None
    panel = case["intraday_volume_panel"]
    assert panel["provider"] == "missing" and panel["symbols"] == {}
    assert any("intraday_missing" in i for i in issues)
    schema_path = Path(__file__).parents[2] / "schema/completion/final_case.schema.json"
    jsonschema.validate(case, json.loads(schema_path.read_text()))


def test_assemble_allow_no_intraday_keeps_complete_panel():
    case, issues = assemble.assemble(STRUCTURED, MARKET, INTRADAY, allow_no_intraday=True)
    assert case is not None
    assert case["intraday_volume_panel"]["symbols"]["AAA"]["is_complete_session"] is True
    assert not any("intraday_missing" in i for i in issues)


# --- associatin_search 检索 ---

def test_title_pattern():
    from src.completion.assemble import _title_pattern
    assert _title_pattern("Best Buy Co., Inc.") == r"Best\s+Buy"
    assert _title_pattern("Ionis Pharmaceuticals, Inc.") == "Ionis"
    assert _title_pattern("Gap") == ""      # 过短弃用, 避免标题噪声
    assert _title_pattern("") == ""


def test_association_search_without_searcher_keeps_provenance():
    from src.completion.assemble import association_search
    r = {"_source_meta": [
        {"content_type": "US_NOTICE", "title": "8-K title", "published_at": "2026-05-29T10:00:00Z",
         "url": "https://sec.gov/x.htm"},
        {"content_type": "US_ROBOT", "title": "robot title", "published_at": "2026-05-28T10:00:00Z"},
    ]}
    a = association_search(r, None)
    assert [i["title"] for i in a["News"]] == ["8-K title"]
    assert a["News"][0]["url"] == "https://sec.gov/x.htm"
    assert [i["title"] for i in a["Robot"]] == ["robot title"]
    assert a["Flash"] == [] and a["AI_search"] == []


def test_related_searcher_retrieves_sorted_by_time(tmp_path, monkeypatch):
    import json as _json

    import duckdb
    from src.completion import assemble as asm
    from src.completion.assemble import RelatedSearcher, association_search
    monkeypatch.setattr(asm.config, "EVENT_V1_DIR", str(tmp_path))
    recs = [  # (id, pub_date, ts, symbols, title, body, source.url)
        ("r2", "2026-05-28", "2026-05-28 08:00", "BBY", "Best Buy Q1 preview", "Preview body", "https://ex.com/a"),
        ("r1", "2026-05-30", "2026-05-30 09:00", "BBY", "Best Buy beats estimates", "<p>Beats  body</p>", None),
        ("r3", "2026-05-29", "2026-05-29 12:00", "", "Best Buy raises outlook", "Outlook body", "https://ex.com/c"),
        ("r4", "2026-05-29", "2026-05-29 10:00", "XYZ", "Unrelated title", "x", None),
        ("r5", "2026-06-15", "2026-06-15 10:00", "BBY", "Too late out of window", "x", None),
    ]
    lines, meta, pos = [], [], 0
    for rid, pd_, ts, sym, title, body, url in recs:
        raw = _json.dumps({"id": rid, "body": body,
                           "source": {"url": url} if url else None,
                           "dedup": {"debug": {"normalized_url": f"https://norm/{rid}"}}}).encode()
        lines.append(raw)
        meta.append((rid, pd_, ts, sym, title, pos, len(raw)))
        pos += len(raw) + 1
    (tmp_path / "US_NEWS.jsonl").write_bytes(b"\n".join(lines) + b"\n")
    values = ",".join(
        f"('{rid}', '{pd_}', TIMESTAMP '{ts}', '{sym}', '{title}', 'US_NEWS.jsonl', {off}, {nb})"
        for rid, pd_, ts, sym, title, off, nb in meta)
    duckdb.connect().execute(
        f'COPY (SELECT * FROM (VALUES {values}) '
        f't(id, pub_date, published_at, symbols, title, file, "offset", nbytes)) '
        f"TO '{tmp_path}/v1_US_NEWS.parquet' (FORMAT PARQUET)")
    s = RelatedSearcher(index_dir=str(tmp_path))
    assert s.available
    found = s.search(["BBY"], "Best Buy Co., Inc.", "2026-05-29")
    titles = [i["title"] for i in found["News"]]
    # 窗口 [-3, +1]: r5 越窗排除; r4 不相关; symbol 或 标题词边界 双通道命中; 按时间升序
    assert titles == ["Best Buy Q1 preview", "Best Buy raises outlook", "Best Buy beats estimates"]
    by_title = {i["title"]: i for i in found["News"]}
    assert by_title["Best Buy Q1 preview"]["url"] == "https://ex.com/a"      # source.url
    assert by_title["Best Buy beats estimates"]["url"] == "https://norm/r1"  # 退回 dedup 规范化 url
    assert by_title["Best Buy beats estimates"]["summary"] == "Beats body"   # 去 HTML 压空白
    assert by_title["Best Buy raises outlook"]["summary"] == "Outlook body"

    # 通过 association_search 合并溯源 + 检索并去重
    r = {"_triage": {"primary_symbols": ["BBY"], "event_date": "2026-05-29"},
         "main_event": {"event_date": "2026-05-29", "event_subject": "Best Buy Q1 earnings"},
         "relation_rows": [{"symbol": "BBY", "company": "Best Buy Co., Inc."}],
         "_source_meta": [{"content_type": "US_NEWS", "title": "Best Buy Q1 preview",
                           "published_at": "2026-05-28T08:00:00"}]}
    a = association_search(r, s)
    assert [i["title"] for i in a["News"]].count("Best Buy Q1 preview") == 1  # 溯源与检索去重
    assert len(a["News"]) == 3
