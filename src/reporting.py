from __future__ import annotations

from typing import Any

from src.config import PipelineConfig
from src.storage import StagingDB


def write_reports(db: StagingDB, config: PipelineConfig) -> dict[str, Any]:
    paths = config.paths
    return db.write_reports(
        paths.reports_dir,
        paths.state_dir / "progress.json",
        paths.cleaned_dir,
        paths.duplicates_dir,
        paths.rejects_dir,
        paths.event_dir,
    )
