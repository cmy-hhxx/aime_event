from __future__ import annotations

from src.config import NearDuplicateConfig
from src.cleaning.dedup.near import NearDuplicateDetector


def _record(record_id: str, title: str, body: str, content_type: str = "US_NEWS") -> dict:
    return {
        "id": record_id,
        "content_type": content_type,
        "title": title,
        "body": body,
        "published_at": "2026-06-15T00:00:00Z",
        "source": {"url": f"https://example.com/news/{record_id}"},
        "_body_len": len(body),
    }


def test_minhash_near_duplicate_auto_merges_high_confidence_text() -> None:
    config = NearDuplicateConfig(min_body_chars=20)
    detector = NearDuplicateDetector(config)
    body = (
        "Nvidia shares rose after the company reported stronger quarterly earnings "
        "and raised its outlook as demand for AI chips remained resilient."
    )

    first = detector.signature_for(_record("a", "Nvidia shares rise after earnings", body))
    second = detector.signature_for(_record("b", "Nvidia stock rises after earnings", body))

    assert first is not None
    assert second is not None
    decision = detector.decide(first, second)
    assert decision.auto_merged
    assert decision.jaccard >= config.threshold
    assert decision.fuzzy_score >= config.fuzzy_threshold


def test_minhash_does_not_auto_merge_different_financial_events() -> None:
    config = NearDuplicateConfig(min_body_chars=20)
    detector = NearDuplicateDetector(config)
    first = detector.signature_for(
        _record(
            "a",
            "Apple shares rise after earnings",
            "Apple shares rose five percent after the company beat quarterly revenue expectations and lifted guidance.",
        )
    )
    second = detector.signature_for(
        _record(
            "b",
            "Apple shares fall after guidance",
            "Apple shares fell eight percent after management warned that demand in China would weaken next quarter.",
        )
    )

    assert first is not None
    assert second is not None
    decision = detector.decide(first, second)
    assert not decision.auto_merged


def test_short_text_and_notice_records_are_not_near_deduped() -> None:
    detector = NearDuplicateDetector(NearDuplicateConfig(min_body_chars=20))

    assert detector.signature_for(_record("short", "Short", "too short")) is None
    assert detector.signature_for(_record("notice", "Form 4", "long enough text for gate", "US_NOTICE")) is None
