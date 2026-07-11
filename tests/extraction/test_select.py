# tests/extraction/test_select.py
from src.extraction import select


def _cand(eid, sym="NVDA", score=5.0, recent=True):
    return {"event_id": eid, "peak_date": "2025-01-10" if recent else "2016-05-01",
            "is_recent": recent, "score": score, "all_symbols": sym,
            "n_articles": 6, "n_sources": 3, "n_high": 1, "n_v2_reactions": 0,
            "first_date": "2025-01-09", "last_date": "2025-01-11",
            "rep_title": "t", "track": "company", "n_content_types": 2, "month": "2025-01"}


def _triage(sig, etype="earnings_release", sym="NVDA", date="2025-01-10"):
    return {"is_valid_event": True, "significance": sig, "event_type": etype,
            "event_family": "f", "event_subject": "S", "primary_symbols": [sym],
            "event_date": date, "title_cn": "标题"}


def test_final_select_threshold_no_quota():
    cands = [_cand(f"E{i}", sym=f"S{i}") for i in range(50)]
    triage = {f"E{i}": _triage(3, sym=f"S{i}", date=f"2025-01-{i%27+1:02d}") for i in range(50)}
    picked = select.final_select(cands, triage, min_significance=3, per_symbol_cap=12)
    assert len(picked) == 50  # 无数量配额: 全部过阈值即全部入选


def test_final_select_significance_gate():
    cands = [_cand("E1"), _cand("E2", sym="AMD")]
    triage = {"E1": _triage(3), "E2": _triage(2, sym="AMD")}
    assert [p["event_id"] for p in select.final_select(cands, triage, 3, 12)] == ["E1"]


def test_final_select_dedupe_same_event():
    cands = [_cand("E1", score=9.0), _cand("E2", score=1.0)]
    triage = {"E1": _triage(3), "E2": _triage(3)}  # 同类型同日同主体
    assert len(select.final_select(cands, triage, 3, 12)) == 1


def test_final_select_per_symbol_cap():
    cands = [_cand(f"E{i}") for i in range(15)]
    triage = {f"E{i}": _triage(3, date=f"2025-01-{i+1:02d}") for i in range(15)}
    assert len(select.final_select(cands, triage, 3, 12)) == 12
    assert len(select.final_select(cands, triage, 3, 0)) == 15  # 0=关闭


def test_gate_where_eras():
    w = select.gate_where("2023-07-01", 5, 3, 2)
    assert "2023-07-01" in w and "n_articles >= 5" in w and "n_articles >= 2" in w


def test_final_select_malformed_date_falls_back():
    cands = [_cand("E1")]
    triage = {"E1": _triage(3, date="2025/03/15")}  # 长度10但非ISO, 必须回退 peak_date
    picked = select.final_select(cands, triage, 3, 12)
    assert picked[0]["event_date"] == "2025-01-10"


def test_gate_where_with_date():
    from src.extraction.select import gate_where
    base = gate_where("2023-07-01", 5, 3, 2)
    dated = gate_where("2023-07-01", 5, 3, 2, date="2026-05-29")
    assert dated == f"({base}) AND peak_date = '2026-05-29'"
    assert gate_where("2023-07-01", 5, 3, 2, date=None) == base
