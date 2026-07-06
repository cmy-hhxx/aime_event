import json

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

MARKET = {"event_id": "EVT_1", "event_date": "2025-01-10", "session_bucket": "after_market",
          "priced_symbols": ["AAA", "BBB", "CCC"], "unpriced_symbols": ["DDD", "EEE"],
          "labels": [_label("AAA"), _label("BBB"), _label("CCC")],
          "market_data_symbols": {s: {"model_input_ohlcv_adjusted_daily":
                                      [{"date": "2025-01-10", "open": 1, "high": 1,
                                        "low": 1, "close": 1, "volume": 1}],
                                      "label_audit_ohlcv_adjusted_daily": []}
                                  for s in ("AAA", "BBB", "CCC")}}


def test_leakage_scan_future_date():
    hits = assemble.leakage_scan([{"metric": "m", "value": "涨到 2025-02-01", "context": ""}],
                                 "2025-01-10")
    assert len(hits) == 1
    assert assemble.leakage_scan([{"metric": "m", "value": "截至 2025-01-09", "context": ""}],
                                 "2025-01-10") == []


def test_assemble_produces_v4_case():
    case, issues = assemble.assemble(STRUCTURED, MARKET)
    assert case is not None and issues == []
    assert case["schema"].startswith("FinancialPredictionTrainingCase.v4")
    assert case["time_dimension_calibration"]["first_tradable_session"] == "2025-01-13"
    rows = case["target_relation_evidence"]["rows"]
    assert {r["symbol"]: r["priced_label_status"] for r in rows}["DDD"] == \
        "unpriced_weak_candidate_needs_price_panel"
    assert case["weak_relation_universe"]["priced_target_count"] == 3
    assert case["intraday_volume_panel"]["status"] == "missing_real_intraday_for_event_date"


def test_assemble_rejects_too_few_priced():
    mk = dict(MARKET, priced_symbols=["AAA"], labels=[_label("AAA")],
              market_data_symbols={"AAA": MARKET["market_data_symbols"]["AAA"]})
    case, issues = assemble.assemble(STRUCTURED, mk)
    assert case is None and any("3" in i for i in issues)
