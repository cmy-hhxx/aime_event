import json
from datetime import datetime, timedelta
from types import SimpleNamespace

from src.completion import market
import pandas as pd

DATES = ["2024-03-14", "2024-03-15", "2024-03-18", "2024-03-19", "2024-03-20"]


def test_pct():
    assert market.pct(100.0, 101.5) == 1.5


def test_base_ft_regular_session():
    bi, ft = market.base_ft_indices(DATES, "2024-03-18", "regular")
    assert DATES[bi] == "2024-03-18" and DATES[ft] == "2024-03-19"


def test_base_ft_pre_market():
    bi, ft = market.base_ft_indices(DATES, "2024-03-18", "pre_market")
    assert DATES[bi] == "2024-03-15" and DATES[ft] == "2024-03-18"


def test_base_ft_non_trading_day():
    bi, ft = market.base_ft_indices(DATES, "2024-03-16", "regular")  # 周六
    assert DATES[bi] == "2024-03-15" and DATES[ft] == "2024-03-18"


def test_event_symbols_dedup_and_validate():
    r = {"relation_rows": [{"symbol": "NVDA"}, {"symbol": "nvda!"}, {"symbol": "TSM"}],
         "_triage": {"primary_symbols": ["NVDA"]}}
    assert market.event_symbols(r) == ["NVDA", "TSM"]


def test_build_intraday_symbol_panel_complete_session():
    idx = pd.date_range("2026-05-29 09:30", periods=390, freq="min",
                        tz="America/New_York")
    df = pd.DataFrame({"Open": 10.0, "High": 10.2, "Low": 9.9,
                       "Close": 10.1, "Volume": 100}, index=idx)
    panel = market.build_intraday_symbol_panel(df, "2026-05-29")
    assert panel is not None
    assert panel["bar_count"] == 390
    assert panel["bars"][0]["trade_count"] == 0
    assert panel["bars"][0]["dollar_volume"] > 0


def test_build_intraday_symbol_panel_rejects_partial_session():
    idx = pd.date_range("2026-05-29 12:00", periods=30, freq="min",
                        tz="America/New_York")
    df = pd.DataFrame({"Open": 10.0, "High": 10.2, "Low": 9.9,
                       "Close": 10.1, "Volume": 100}, index=idx)
    assert market.build_intraday_symbol_panel(df, "2026-05-29") is None


def test_import_intraday_rows(tmp_path, monkeypatch):
    src = tmp_path / "bars.jsonl"
    start = datetime(2026, 5, 29, 9, 30)
    with src.open("w") as fh:
        for i in range(390):
            fh.write(json.dumps({
                "event_id": "EVT_1", "event_date": "2026-05-29", "symbol": "AAA",
                "timestamp_et": (start + timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S"),
                "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1,
                "vwap": 1, "trade_count": 1, "dollar_volume": 1,
            }) + "\n")
    monkeypatch.setattr(market.config, "EVENT_REPORT_DIR", str(tmp_path))
    market.run_import_intraday(SimpleNamespace(
        input=str(src), outdir=str(tmp_path), provider="test-provider"))
    record = json.loads((tmp_path / "intraday.jsonl").read_text().splitlines()[0])
    assert record["symbols"]["AAA"]["bar_count"] == 390


def test_filter_by_peak_date():
    from src.completion.market import filter_by_peak_date
    events = [{"event_id": "A", "_triage": {"peak_date": "2026-05-29"}},
              {"event_id": "B", "_triage": {"peak_date": "2026-05-28"}},
              {"event_id": "C"}]
    assert filter_by_peak_date(events, None) == events
    assert [e["event_id"] for e in filter_by_peak_date(events, "2026-05-29")] == ["A"]
