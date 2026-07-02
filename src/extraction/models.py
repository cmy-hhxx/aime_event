from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ExtractedEvent:
    source_id: str
    event_type: str
    event_title: str
    event_time: str | None = None
    entities: list[dict[str, Any]] | None = None
    summary: str | None = None
    evidence: str | None = None
    confidence: float | None = None


@dataclass(frozen=True)
class ExtractionSettings:
    input_path: Path = Path("/mnt/ainvest_content/v3/v1")
    output_dir: Path = Path("/mnt/ainvest_content/v3/v1/extracted")
    env_file: Path = Path(".env")
    base_url: str | None = None
    api_key: str | None = None
    model: str | None = None
    limit: int | None = None
    timeout_seconds: int = 120
    max_retries: int = 2
    log_every_rows: int = 100
    max_body_chars: int = 8000
    temperature: float = 0.0
    max_tokens: int = 1200
    include_raw_response: bool = False
    random_sample: bool = False
    random_seed: int | None = None
