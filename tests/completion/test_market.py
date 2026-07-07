from src.completion import market

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
