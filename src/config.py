from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path

from src.payload_store import DEFAULT_PAYLOAD_PART_BYTES


@dataclass(frozen=True)
class PathsConfig:
    input_dir: Path = Path("data/raw")
    cleaned_dir: Path = Path("output/cleaned")
    duplicates_dir: Path = Path("output/duplicates")
    rejects_dir: Path = Path("output/rejects")
    event_dir: Path = Path("output/event_input")
    state_dir: Path = Path("state")
    payload_dir: Path = Path("state/payloads")
    reports_dir: Path = Path("reports")


@dataclass(frozen=True)
class RuntimeConfig:
    workers: int = 4
    chunk_size: int = 3_000
    part_size: int = 100_000
    payload_part_bytes: int = DEFAULT_PAYLOAD_PART_BYTES
    target_scale_rows: int = 20_000_000


@dataclass(frozen=True)
class NearDuplicateConfig:
    enabled: bool = True
    num_perm: int = 128
    seed: int = 1
    shingle_size: int = 5
    min_body_chars: int = 160
    threshold: float = 0.92
    fuzzy_threshold: float = 96.0
    title_threshold: float = 90.0
    long_gap_title_threshold: float = 96.0
    max_days_between: int = 14
    max_bucket_size: int = 250
    max_candidate_pairs: int = 1_000_000
    max_report_pairs: int = 10_000

    @property
    def band_size(self) -> int:
        return 4


@dataclass(frozen=True)
class PipelineConfig:
    paths: PathsConfig = field(default_factory=PathsConfig)
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    near_duplicates: NearDuplicateConfig = field(default_factory=NearDuplicateConfig)

    def with_paths(self, **updates: Path) -> PipelineConfig:
        return replace(self, paths=replace(self.paths, **updates))

    def with_runtime(self, **updates: int) -> PipelineConfig:
        return replace(self, runtime=replace(self.runtime, **updates))


DEFAULT_CONFIG = PipelineConfig()
