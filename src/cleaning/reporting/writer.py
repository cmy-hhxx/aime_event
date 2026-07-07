from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import PipelineConfig
from src.cleaning.reporting.near_pairs import write_near_duplicates
from src.cleaning.reporting.summary import build_summary
from src.cleaning.storage.staging import StagingDB


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")


def write_reports(db: StagingDB, config: PipelineConfig) -> dict[str, Any]:
    paths = config.paths
    reports_dir = paths.reports_dir
    reports_dir.mkdir(parents=True, exist_ok=True)

    completed = db.completed_batches()
    progress_path = paths.state_dir / "progress.json"
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    progress_path.write_text(json.dumps({"completed": completed}, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    with (reports_dir / "batch_stats.jsonl").open("w", encoding="utf-8") as handle:
        for row in db.conn.execute("SELECT * FROM batch_state ORDER BY batch"):
            stats = {
                "batch": row["batch"],
                "status": row["status"],
                "input": row["input_count"],
                "accepted": row["accepted_count"],
                "rejected": row["rejected_count"],
            }
            handle.write(json.dumps(stats, sort_keys=True, ensure_ascii=False) + "\n")

    write_near_duplicates(db, reports_dir / "near_duplicates.jsonl")
    summary = build_summary(
        db,
        paths.cleaned_dir,
        paths.duplicates_dir,
        paths.rejects_dir,
    )
    _write_json(reports_dir / "summary.json", summary)

    index = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "files": {
            "summary.json": {
                "description": "全库汇总统计",
                "schema": "schema/cleaning/summary.schema.json",
            },
            "batch_stats.jsonl": {
                "description": "按 batch 的处理统计",
                "schema": "schema/cleaning/batch_stats.schema.json",
            },
            "near_duplicates.jsonl": {
                "description": "近似去重候选对审计日志",
                "schema": "schema/cleaning/near_duplicates.schema.json",
            },
        },
    }
    _write_json(reports_dir / "index.json", index)

    return summary
