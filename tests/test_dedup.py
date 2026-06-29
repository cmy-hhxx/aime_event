from __future__ import annotations

from src.dedup.exact import compute_dedup_key, is_eligible_article_url, normalize_url


def _record(url: str, title: str, body: str) -> dict:
    return {
        "id": "r1",
        "title": title,
        "body": body,
        "source": {"url": url},
    }


def test_feed_urls_fall_back_to_content_hash() -> None:
    url = "https://www.reuters.com/arc/outboundfeeds/news-sitemap/?outputType=xml"

    first_key = compute_dedup_key(_record(url, "First story", "Alpha"))
    second_key = compute_dedup_key(_record(url, "Second story", "Beta"))

    assert not is_eligible_article_url(url)
    assert first_key[1] == "content_hash"
    assert second_key[1] == "content_hash"
    assert first_key[0] != second_key[0]
    assert first_key[0].startswith("hash:sha256:")


def test_article_urls_are_url_dedup_keys() -> None:
    url = "https://www.reuters.com/business/a-real-story-2026-06-15/"

    key, method = compute_dedup_key(_record(url, "Story", "Body"))

    assert is_eligible_article_url(url)
    assert key == "url:https://www.reuters.com/business/a-real-story-2026-06-15"
    assert method == "source_url"


def test_tracking_params_fragments_and_trailing_slashes_are_normalized() -> None:
    first = "https://SeekingAlpha.com/news/4603239-story/?utm_source=x&mod=mw_quote_news#source=home"
    second = "https://seekingalpha.com/news/4603239-story?r=abc&utm_campaign=y"

    first_key, first_method = compute_dedup_key(_record(first, "Story", "Body"))
    second_key, second_method = compute_dedup_key(_record(second, "Story rewritten", "Different body"))

    assert normalize_url(first) == "https://seekingalpha.com/news/4603239-story"
    assert first_key == second_key
    assert first_method == second_method == "source_url"


def test_notice_uses_attachment_accession_key() -> None:
    record = {
        "id": "notice-1",
        "title": "Form 4",
        "body": "",
        "content_type": "US_NOTICE",
        "source": {"url": None},
        "notice": {
            "attachments": [
                {
                    "url": "https://www.sec.gov/Archives/edgar/data/1070524/000143774926020619/xslF345X06/rdgdoc.xml",
                    "file_type": "4",
                }
            ]
        },
    }

    result = compute_dedup_key(record)

    assert result[0] == "notice:000143774926020619"
    assert result[1] == "notice_attachment"
    assert result.debug["notice_accession"] == "000143774926020619"
