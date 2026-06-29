from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from importlib import import_module
from typing import Any
from urllib.parse import urlsplit

from src.config import NearDuplicateConfig
from src.dedup.exact import normalize_text, normalize_url

_TOKEN_RE = re.compile(r"[a-z0-9$%.\-]+")


@dataclass(frozen=True)
class NearSignature:
    record_id: str
    signature: tuple[int, ...]
    shingle_count: int
    title: str
    body: str
    host: str
    published_at: str
    body_len: int


@dataclass(frozen=True)
class NearDecision:
    status: str
    reason: str
    jaccard: float
    fuzzy_score: float
    title_score: float

    @property
    def auto_merged(self) -> bool:
        return self.status == "auto_merged"


class UnionFind:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def find(self, item: str) -> str:
        self.parent.setdefault(item, item)
        if self.parent[item] != item:
            self.parent[item] = self.find(self.parent[item])
        return self.parent[item]

    def union(self, left: str, right: str) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root != right_root:
            self.parent[max(left_root, right_root)] = min(left_root, right_root)

    def groups(self) -> dict[str, set[str]]:
        groups: dict[str, set[str]] = {}
        for item in list(self.parent):
            groups.setdefault(self.find(item), set()).add(item)
        return groups


class NearDuplicateDetector:
    def __init__(self, config: NearDuplicateConfig):
        self.config = config

    def signature_for(self, record: dict[str, Any]) -> NearSignature | None:
        if not self.config.enabled:
            return None
        if record.get("content_type") == "US_NOTICE":
            return None

        body = str(record.get("body") or "").strip()
        if len(body) < self.config.min_body_chars:
            return None

        shingles = self._shingles(body)
        if not shingles:
            return None

        minhash_cls = import_module("datasketch").MinHash
        minhash = minhash_cls(num_perm=self.config.num_perm, seed=self.config.seed)
        for shingle in shingles:
            minhash.update(shingle.encode("utf-8"))

        return NearSignature(
            record_id=str(record["id"]),
            signature=tuple(int(value) for value in minhash.hashvalues),
            shingle_count=len(shingles),
            title=str(record.get("title") or ""),
            body=body,
            host=_source_host(record),
            published_at=str(record.get("published_at") or ""),
            body_len=int(record.get("_body_len", len(body))),
        )

    def band_keys(self, signature: tuple[int, ...]) -> list[tuple[int, str]]:
        band_size = self.config.band_size
        bands = []
        for index in range(0, len(signature), band_size):
            band_no = index // band_size
            values = signature[index : index + band_size]
            if len(values) != band_size:
                continue
            digest = hashlib.sha1(",".join(str(value) for value in values).encode()).hexdigest()
            bands.append((band_no, digest))
        return bands

    def decide(self, left: NearSignature, right: NearSignature) -> NearDecision:
        jaccard = _signature_jaccard(left.signature, right.signature)
        fuzz_module = import_module("rapidfuzz.fuzz")
        fuzzy_score = float(fuzz_module.token_set_ratio(left.body, right.body))
        title_score = float(fuzz_module.token_set_ratio(left.title, right.title))

        if jaccard < self.config.threshold:
            return NearDecision("report_only", "below_minhash_threshold", jaccard, fuzzy_score, title_score)
        if fuzzy_score < self.config.fuzzy_threshold:
            return NearDecision("report_only", "below_fuzzy_threshold", jaccard, fuzzy_score, title_score)
        if left.host != right.host and title_score < self.config.title_threshold:
            return NearDecision("report_only", "different_host_and_title", jaccard, fuzzy_score, title_score)

        gap_days = _date_gap_days(left.published_at, right.published_at)
        if gap_days is not None and gap_days > self.config.max_days_between:
            if title_score < self.config.long_gap_title_threshold:
                return NearDecision("report_only", "published_at_gap", jaccard, fuzzy_score, title_score)

        return NearDecision("auto_merged", "high_confidence_near_duplicate", jaccard, fuzzy_score, title_score)

    def _shingles(self, text: str) -> set[str]:
        tokens = _TOKEN_RE.findall(normalize_text(text))
        size = self.config.shingle_size
        if len(tokens) < size:
            return set()
        return {" ".join(tokens[index : index + size]) for index in range(0, len(tokens) - size + 1)}


def _source_host(record: dict[str, Any]) -> str:
    source = record.get("source") or {}
    normalized = normalize_url(source.get("url")) if isinstance(source, dict) else None
    if not normalized:
        return ""
    return urlsplit(normalized).netloc


def _signature_jaccard(left: tuple[int, ...], right: tuple[int, ...]) -> float:
    if not left or len(left) != len(right):
        return 0.0
    matches = sum(1 for left_value, right_value in zip(left, right) if left_value == right_value)
    return matches / len(left)


def _date_gap_days(left: str, right: str) -> int | None:
    left_dt = _parse_iso(left)
    right_dt = _parse_iso(right)
    if left_dt is None or right_dt is None:
        return None
    return abs((left_dt - right_dt).days)


def _parse_iso(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
