from __future__ import annotations

import orjson

from src.ingest.transform import transform, transform_line


def _raw(**overrides: object) -> dict:
    raw = {
        "_id": "record-1",
        "businessCode": "US_NEWS",
        "type": 0,
        "title": "A title",
        "content": "<p>A body</p>",
        "ctime": 1781568000,
        "rtime": None,
        "news": {"sourceUrl": "https://example.com/a-story", "source": "Example"},
    }
    raw.update(overrides)
    return raw


def test_bad_json_is_rejected() -> None:
    result = transform_line(b'{"broken":')

    assert not result.accepted
    assert result.reason == "invalid_json"


def test_missing_id_is_rejected() -> None:
    raw = _raw()
    raw.pop("_id")

    result = transform(raw)

    assert not result.accepted
    assert result.reason == "missing_id"


def test_non_notice_empty_body_is_rejected() -> None:
    result = transform(_raw(content=""))

    assert not result.accepted
    assert result.reason == "empty_body"


def test_missing_published_at_is_rejected() -> None:
    result = transform(_raw(ctime=None))

    assert not result.accepted
    assert result.reason == "missing_published_at"


def test_invalid_type_code_is_rejected() -> None:
    result = transform(_raw(type="not-an-int"))

    assert not result.accepted
    assert result.reason == "invalid_type_code"


def test_missing_updated_at_falls_back_to_published_at() -> None:
    result = transform_line(orjson.dumps(_raw(rtime=None)))

    assert result.accepted
    assert result.record is not None
    assert result.record["updated_at"] == result.record["published_at"]


def test_notice_can_have_empty_body_with_attachment() -> None:
    result = transform(
        _raw(
            businessCode="US_NOTICE",
            content="",
            noticeInfo={
                "noticeType": "4",
                "declareDate": "2026-06-15",
                "attachmentList": [{"url": "https://www.sec.gov/doc.xml", "fileType": "4"}],
            },
        )
    )

    assert result.accepted
    assert result.record is not None
    assert result.record["body"] == ""
