from __future__ import annotations

from src.cleaning.dedup.exact import compute_dedup_key


def test_us_post_normalizes_stock_links_and_urls() -> None:
    first = {
        "id": "post-1",
        "content_type": "US_POST",
        "title": '<a class="_dollar_" data-code="TSLA" data-marketid="NASDAQ">',
        "body": "[$TSLA](https://www.ainvest.com/stocks/NASDAQ-TSLA/) A good time for a 50bps cut now. You would think homebuilders would be up today.",
        "source": {},
        "published_at": "2025-09-09T22:30:01Z",
        "entities": {"stocks": []},
    }
    second = {
        **first,
        "id": "post-2",
        "title": "TSLA stock",
        "body": "$TSLA A good time for a 50bps cut now. You would think homebuilders would be up today.",
    }

    first_key = compute_dedup_key(first)
    second_key = compute_dedup_key(second)

    assert first_key.method == "post_fingerprint"
    assert first_key.key == second_key.key
    assert first_key.debug["symbols"] == ["TSLA"]


def test_us_post_short_text_falls_back_to_content_hash() -> None:
    result = compute_dedup_key(
        {
            "id": "post-short",
            "content_type": "US_POST",
            "title": "TSLA",
            "body": "$TSLA wow",
            "source": {},
            "published_at": "2025-09-09T22:30:01Z",
        }
    )

    assert result.method == "content_hash"


def test_us_robot_financial_results_template_key() -> None:
    result = compute_dedup_key(
        {
            "id": "robot-financial",
            "content_type": "US_ROBOT",
            "title": "Financial Results | FactSet 2023 Half-Year Revenue USD 1019.90 Million Net Income USD 268.39 Million",
            "body": "FactSet(FDS) posted the Q2 of its 2023 financial results on 4/3/2023, reporting total revenue of USD 1019.90 million.",
            "source": {},
            "published_at": "2023-04-04T02:00:11Z",
            "entities": {"stocks": [{"symbol": "FDS"}]},
        }
    )

    assert result.method == "robot_template"
    assert result.debug["template"] == "financial_results"
    assert result.debug["symbol"] == "FDS"


def test_us_robot_technical_signal_template_key() -> None:
    result = compute_dedup_key(
        {
            "id": "robot-technical",
            "content_type": "US_ROBOT",
            "title": "TransUnion's 15min chart signals overbought, Bollinger Bands expanding upward",
            "body": "TransUnion technical chart signals overbought with Bollinger Bands expanding upward.",
            "source": {},
            "published_at": "2025-03-01T10:00:00Z",
            "entities": {},
        }
    )

    assert result.method == "robot_template"
    assert result.debug["template"] == "technical_signal"
    assert result.debug["subject"] == "transunion"


def test_us_robot_insider_transaction_template_key() -> None:
    result = compute_dedup_key(
        {
            "id": "robot-insider",
            "content_type": "US_ROBOT",
            "title": "Insider Transactions Reported | German American Bancorp(GABC)saw insider trading",
            "body": "German American Bancorp(GABC) saw insider trading reported today.",
            "source": {},
            "published_at": "2025-03-01T10:00:00Z",
            "entities": {},
        }
    )

    assert result.method == "robot_template"
    assert result.debug["template"] == "insider_transaction"
    assert result.debug["subject"] == "GABC"
