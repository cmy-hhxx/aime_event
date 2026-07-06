from __future__ import annotations

import hashlib
import html
import re
from dataclasses import dataclass
from typing import Any, Iterator
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

DEDUP_VERSION = 4

_WHITESPACE_RE = re.compile(r"\s+")
_SEC_ACCESSION_RE = re.compile(r"/archives/edgar/data/\d+/([^/]+)/", re.IGNORECASE)
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_MARKDOWN_LINK_RE = re.compile(r"\[([^\]]+)\]\([^)]+\)")
_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
_DATA_CODE_RE = re.compile(r"data-code=[\"']?([A-Za-z0-9.\-]{1,12})", re.IGNORECASE)
_STOCK_PATH_RE = re.compile(r"/stocks/(?:[A-Z]+-)?([A-Z0-9.\-]{1,12})", re.IGNORECASE)
_CASHTAG_RE = re.compile(r"(?<![A-Za-z0-9_])\$([A-Z][A-Z0-9.\-]{0,11})(?![A-Za-z0-9_])")
_PAREN_TICKER_RE = re.compile(r"\(([A-Z][A-Z0-9.\-]{0,11})\)")
_NON_TEXT_RE = re.compile(r"[^a-z0-9$%.\-]+")
_POST_MIN_NORMALIZED_CHARS = 32
_POST_MIN_TOKENS = 5
_DENYLIST_PATH_FRAGMENTS = (
    "/arc/outboundfeeds/",
    "/lineup-next/api/",
    "/market-news",
)
_DENYLIST_QUERY_KEYS = {"outputtype", "pagenumber", "limit"}
_TRACKING_QUERY_KEYS = {
    "cmpid",
    "fbclid",
    "gclid",
    "igshid",
    "mc_cid",
    "mc_eid",
    "mod",
    "r",
    "ref",
    "ref_src",
    "utm_campaign",
    "utm_content",
    "utm_medium",
    "utm_source",
    "utm_term",
}
_ROBOT_SIGNAL_PATTERNS = (
    ("rsi_overbought", re.compile(r"\brsi\b.*\boverbought\b|\boverbought\b.*\brsi\b", re.IGNORECASE)),
    ("rsi_oversold", re.compile(r"\brsi\b.*\boversold\b|\boversold\b.*\brsi\b", re.IGNORECASE)),
    ("bollinger_up", re.compile(r"bollinger bands? expanding upward", re.IGNORECASE)),
    ("bollinger_down", re.compile(r"bollinger bands? expanding downward", re.IGNORECASE)),
    ("bullish", re.compile(r"\bbullish(?:ness| trend| momentum)?\b", re.IGNORECASE)),
    ("bearish", re.compile(r"\bbearish(?:ness| trend)?\b", re.IGNORECASE)),
    ("overbought", re.compile(r"\boverbought\b", re.IGNORECASE)),
    ("oversold", re.compile(r"\boversold\b", re.IGNORECASE)),
)


@dataclass(frozen=True)
class DedupResult:
    key: str
    method: str
    debug: dict[str, Any]

    def __iter__(self) -> Iterator[str]:
        yield self.key
        yield self.method

    def __getitem__(self, index: int) -> str:
        if index == 0:
            return self.key
        if index == 1:
            return self.method
        raise IndexError(index)


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return _WHITESPACE_RE.sub(" ", value).strip().lower()


def normalize_url(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlsplit(url.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None

    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/")

    query_items = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        lowered = key.lower()
        if lowered.startswith("utm_") or lowered in _TRACKING_QUERY_KEYS:
            continue
        query_items.append((key, value))
    query = urlencode(sorted(query_items), doseq=True)
    return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), path, query, ""))


def _url_reject_reason(url: str | None) -> str | None:
    if not url:
        return "missing_url"

    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return "invalid_url"

    path = parsed.path.lower()
    if any(fragment in path for fragment in _DENYLIST_PATH_FRAGMENTS):
        return "feed_or_api_path"
    if "/sitemap" in path or "news-sitemap" in path:
        return "sitemap_url"

    query_keys = {key.lower() for key, _ in parse_qsl(parsed.query, keep_blank_values=True)}
    if query_keys & _DENYLIST_QUERY_KEYS:
        return "feed_or_list_query"

    return None


def is_eligible_article_url(url: str | None) -> bool:
    return normalize_url(url) is not None and _url_reject_reason(url) is None


def content_hash(title: str | None, body: str | None) -> str | None:
    normalized_title = normalize_text(title)
    normalized_body = normalize_text(body)
    if not normalized_title and not normalized_body:
        return None
    digest = hashlib.sha256(f"{normalized_title}|{normalized_body}".encode()).hexdigest()
    return f"hash:sha256:{digest}"


def _hash_key(prefix: str, value: str) -> str:
    digest = hashlib.sha256(value.encode()).hexdigest()
    return f"{prefix}:sha256:{digest}"


def id_key(record_id: str) -> str:
    return f"id:{record_id}"


def _notice_attachment_key(record: dict[str, Any]) -> DedupResult | None:
    if record.get("content_type") != "US_NOTICE":
        return None
    notice = record.get("notice")
    attachments = (notice or {}).get("attachments") if isinstance(notice, dict) else None
    if not attachments:
        return None

    first_url = None
    first_normalized = None
    for attachment in attachments:
        if not isinstance(attachment, dict):
            continue
        url = attachment.get("url")
        normalized = normalize_url(str(url)) if url else None
        if normalized and first_url is None:
            first_url = str(url)
            first_normalized = normalized
        accession = _extract_sec_accession(str(url)) if url else None
        if accession:
            return DedupResult(
                key=f"notice:{accession}",
                method="notice_attachment",
                debug={
                    "version": DEDUP_VERSION,
                    "source": "notice_accession",
                    "notice_accession": accession,
                    "notice_attachment_url": str(url),
                    "normalized_url": normalized,
                },
            )

    if first_normalized:
        digest = hashlib.sha256(first_normalized.encode()).hexdigest()
        return DedupResult(
            key=f"notice_url:sha256:{digest}",
            method="notice_attachment",
            debug={
                "version": DEDUP_VERSION,
                "source": "notice_attachment_url",
                "notice_attachment_url": first_url,
                "normalized_url": first_normalized,
                "hash_algorithm": "sha256",
            },
        )
    return None


def _extract_sec_accession(url: str) -> str | None:
    match = _SEC_ACCESSION_RE.search(url)
    return match.group(1) if match else None


def _symbol_values(record: dict[str, Any], *texts: str) -> list[str]:
    symbols: set[str] = set()
    entities = record.get("entities")
    stocks = entities.get("stocks") if isinstance(entities, dict) else None
    if isinstance(stocks, list):
        for stock in stocks:
            if not isinstance(stock, dict):
                continue
            symbol = _normalize_symbol(stock.get("symbol"))
            if symbol:
                symbols.add(symbol)

    joined = " ".join(texts)
    for pattern in (_DATA_CODE_RE, _STOCK_PATH_RE, _CASHTAG_RE, _PAREN_TICKER_RE):
        for match in pattern.finditer(joined):
            symbol = _normalize_symbol(match.group(1))
            if symbol:
                symbols.add(symbol)
    return sorted(symbols)[:12]


def _normalize_symbol(value: Any) -> str | None:
    if not value:
        return None
    symbol = str(value).upper().strip().strip(".,;:()[]{}")
    if not re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,11}", symbol):
        return None
    return symbol


def _plain_market_text(value: str | None) -> str:
    if not value:
        return ""
    text = html.unescape(str(value))
    text = _MARKDOWN_LINK_RE.sub(r" \1 ", text)
    text = _DATA_CODE_RE.sub(r" data-code \1 ", text)
    text = _HTML_TAG_RE.sub(" ", text)
    text = _URL_RE.sub(" ", text)
    text = _CASHTAG_RE.sub(r" \1 ", text)
    text = _NON_TEXT_RE.sub(" ", text.lower())
    return _WHITESPACE_RE.sub(" ", text).strip()


def _published_day(record: dict[str, Any]) -> str:
    published_at = str(record.get("published_at") or "")
    return published_at[:10] if len(published_at) >= 10 else ""


def _post_fingerprint_key(record: dict[str, Any]) -> DedupResult | None:
    if record.get("content_type") != "US_POST":
        return None

    title = str(record.get("title") or "")
    body = str(record.get("body") or "")
    normalized_body = _plain_market_text(body)
    tokens = normalized_body.split()
    if len(normalized_body) < _POST_MIN_NORMALIZED_CHARS or len(tokens) < _POST_MIN_TOKENS:
        return None

    symbols = _symbol_values(record, title, body)
    symbol_scope = ",".join(symbols) if symbols else "none"
    key_material = f"symbols={symbol_scope}|body={normalized_body}"
    return DedupResult(
        key=_hash_key("post_fingerprint", key_material),
        method="post_fingerprint",
        debug={
            "version": DEDUP_VERSION,
            "source": "us_post_normalized_body",
            "symbols": symbols,
            "normalized_chars": len(normalized_body),
            "normalized_tokens": len(tokens),
            "text_normalization": "html_markdown_url_cashtag_strip_lower",
        },
    )


def _robot_template_key(record: dict[str, Any]) -> DedupResult | None:
    if record.get("content_type") != "US_ROBOT":
        return None

    title = str(record.get("title") or "")
    body = str(record.get("body") or "")
    text = f"{title}\n{body}"
    normalized = _plain_market_text(text)
    symbols = _symbol_values(record, title, body)
    primary_symbol = symbols[0] if symbols else None

    financial = _financial_result_template_key(record, title, normalized, primary_symbol)
    if financial:
        return financial

    technical = _technical_signal_template_key(record, title, normalized, primary_symbol)
    if technical:
        return technical

    insider = _insider_template_key(record, title, normalized, primary_symbol)
    if insider:
        return insider

    return None


def _financial_result_template_key(
    record: dict[str, Any],
    title: str,
    normalized: str,
    symbol: str | None,
) -> DedupResult | None:
    if not symbol:
        return None
    if not (
        "financial results" in normalized
        or (" revenue " in f" {normalized} " and " net income " in f" {normalized} ")
        or re.search(r"\bq[1-4]\b.*\bearnings\b|\bearnings\b.*\bq[1-4]\b", normalized)
    ):
        return None

    year_match = re.search(r"\b(20\d{2})\b", normalized)
    period_match = re.search(
        r"\b(q[1-4]|first quarter|second quarter|third quarter|fourth quarter|half[- ]year|full year|annual)\b",
        normalized,
    )
    year = year_match.group(1) if year_match else _published_day(record)[:4]
    period = period_match.group(1).replace(" ", "_").replace("-", "_") if period_match else "unknown_period"
    key_material = f"financial_results|{symbol}|{year}|{period}"
    return DedupResult(
        key=f"robot_template:{key_material}",
        method="robot_template",
        debug={
            "version": DEDUP_VERSION,
            "source": "us_robot_financial_results_template",
            "template": "financial_results",
            "symbol": symbol,
            "year": year,
            "period": period,
            "title": title,
        },
    )


def _technical_signal_template_key(
    record: dict[str, Any],
    title: str,
    normalized: str,
    symbol: str | None,
) -> DedupResult | None:
    if "chart signals" not in normalized and "bollinger" not in normalized and " rsi " not in f" {normalized} ":
        return None

    subject = symbol or _title_subject_slug(title)
    if not subject:
        return None

    timeframe_match = re.search(r"\b(\d+\s*min|\d+\s*hour|\d+\s*day|daily|weekly)\b", normalized)
    timeframe = timeframe_match.group(1).replace(" ", "") if timeframe_match else "unknown_timeframe"
    signals = [name for name, pattern in _ROBOT_SIGNAL_PATTERNS if pattern.search(normalized)]
    if not signals:
        return None

    key_material = f"technical_signal|{subject}|{timeframe}|{'+'.join(sorted(set(signals)))}|{_published_day(record)}"
    return DedupResult(
        key=f"robot_template:{key_material}",
        method="robot_template",
        debug={
            "version": DEDUP_VERSION,
            "source": "us_robot_technical_signal_template",
            "template": "technical_signal",
            "subject": subject,
            "timeframe": timeframe,
            "signals": sorted(set(signals)),
            "published_day": _published_day(record),
            "title": title,
        },
    )


def _insider_template_key(
    record: dict[str, Any],
    title: str,
    normalized: str,
    symbol: str | None,
) -> DedupResult | None:
    if "insider transactions reported" not in normalized and "insider trading" not in normalized:
        return None
    subject = symbol or _title_subject_slug(title)
    if not subject:
        return None
    key_material = f"insider_transaction|{subject}|{_published_day(record)}"
    return DedupResult(
        key=f"robot_template:{key_material}",
        method="robot_template",
        debug={
            "version": DEDUP_VERSION,
            "source": "us_robot_insider_transaction_template",
            "template": "insider_transaction",
            "subject": subject,
            "published_day": _published_day(record),
            "title": title,
        },
    )


def _title_subject_slug(title: str) -> str | None:
    text = html.unescape(title)
    subject = re.split(r"'s|\||:|-", text, maxsplit=1)[0]
    subject = _plain_market_text(subject)
    subject = subject.strip(" .-")
    if len(subject) < 3:
        return None
    return "_".join(subject.split()[:6])


def compute_dedup_key(record: dict[str, Any]) -> DedupResult:
    notice_key = _notice_attachment_key(record)
    if notice_key:
        return notice_key

    source_url = (record.get("source") or {}).get("url")
    normalized_url = normalize_url(source_url)
    url_reject_reason = _url_reject_reason(source_url)
    if normalized_url and url_reject_reason is None:
        return DedupResult(
            key=f"url:{normalized_url}",
            method="source_url",
            debug={
                "version": DEDUP_VERSION,
                "source": "source.url",
                "source_url": source_url,
                "normalized_url": normalized_url,
            },
        )

    post_key = _post_fingerprint_key(record)
    if post_key:
        return post_key

    robot_key = _robot_template_key(record)
    if robot_key:
        return robot_key

    digest_key = content_hash(record.get("title"), record.get("body"))
    if digest_key:
        debug: dict[str, Any] = {
            "version": DEDUP_VERSION,
            "source": "normalized_title_body",
            "hash_algorithm": "sha256",
            "text_normalization": "trim_lower_collapse_whitespace",
        }
        if source_url:
            debug["source_url"] = source_url
            debug["normalized_url"] = normalized_url
            debug["rejected_url_reason"] = url_reject_reason
        return DedupResult(digest_key, "content_hash", debug)

    record_id = str(record["id"])
    return DedupResult(
        key=id_key(record_id),
        method="id",
        debug={"version": DEDUP_VERSION, "source": "id_fallback"},
    )


def finalize_record(
    record: dict[str, Any],
    key: str,
    method: str,
    is_canonical: bool,
    canonical_id: str,
    debug: dict[str, Any] | None = None,
) -> dict[str, Any]:
    out = {k: v for k, v in record.items() if not k.startswith("_")}
    out["dedup"] = {
        "version": DEDUP_VERSION,
        "key": key,
        "method": method,
        "is_canonical": is_canonical,
        "canonical_id": canonical_id,
        "debug": debug or {"version": DEDUP_VERSION, "source": "unknown"},
    }
    return out
