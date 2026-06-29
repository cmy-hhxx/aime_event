from __future__ import annotations

from src.dedup import compute_dedup_key, is_eligible_article_url


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


def test_article_urls_are_url_dedup_keys() -> None:
    url = "https://www.reuters.com/business/a-real-story-2026-06-15/"

    key, method = compute_dedup_key(_record(url, "Story", "Body"))

    assert is_eligible_article_url(url)
    assert key == f"url:{url}"
    assert method == "source_url"
