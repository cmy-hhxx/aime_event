from __future__ import annotations

import json
import re
import shutil
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Iterable

import orjson

from src.config import PipelineConfig, validate_config
from src.dedup.exact import finalize_record
from src.ingest.transform import TransformResult, transform_line
from src.output.views import build_cleaned_record, build_event_record
from src.pipeline.writers import PartWriter
from src.reporting import write_reports
from src.storage import PayloadWriter, RejectRow, StagingDB, StateVersionError


def discover_batches(input_dir: Path) -> list[Path]:
    return sorted(input_dir.glob("content_batch_*.ndjson"), key=_batch_sort_key)


def _batch_sort_key(path: Path) -> tuple[int, int | str]:
    match = re.search(r"content_batch_(\d+)\.ndjson$", path.name)
    if match:
        return (0, int(match.group(1)))
    return (1, path.name)


def ingest_batches(config: PipelineConfig, db: StagingDB, force: bool) -> None:
    paths = config.paths
    batches = discover_batches(paths.input_dir)
    if not batches:
        print(f"No batch files found in {paths.input_dir}")
        return

    for batch_file in batches:
        stats = ingest_batch(batch_file, db, config, force)
        if stats is None:
            print(f"Skip (done): {batch_file.name}")
            continue
        print(
            f"Processed: {batch_file.name} "
            f"input={stats.get('input', 0)} accepted={stats.get('accepted', 0)} "
            f"rejected={stats.get('rejected', 0)}"
        )


def ingest_batch(
    batch_file: Path,
    db: StagingDB,
    config: PipelineConfig,
    force: bool,
) -> Counter[str] | None:
    runtime = config.runtime
    batch = batch_file.name
    should_run = db.prepare_batch(batch, batch_file, force=force)
    if not should_run:
        return None

    stats: Counter[str] = Counter()
    pool = ProcessPoolExecutor(max_workers=runtime.workers) if runtime.workers > 1 else None
    payload_writer = PayloadWriter(db.payload_dir, batch, runtime.payload_part_bytes)
    try:
        with batch_file.open("rb") as handle:
            chunk: list[tuple[int, bytes]] = []
            for line_no, raw_line in enumerate(handle, start=1):
                line = raw_line.strip()
                if not line:
                    continue
                chunk.append((line_no, line))
                if len(chunk) >= runtime.chunk_size:
                    stats.update(_flush_chunk(db, payload_writer, batch, chunk, pool))
                    chunk = []

            if chunk:
                stats.update(_flush_chunk(db, payload_writer, batch, chunk, pool))
    finally:
        payload_writer.close()
        if pool:
            pool.shutdown()

    db.complete_batch(batch)
    return stats


def export_outputs(config: PipelineConfig, db: StagingDB) -> dict[str, int]:
    paths = config.paths
    runtime = config.runtime
    part_size = runtime.part_size
    cleaned_tmp = _tmp_output_dir(paths.cleaned_dir)
    cleaned_writer = PartWriter(
        cleaned_tmp,
        "cleaned",
        part_size,
        filename_template="{prefix}_batch{index}.jsonl",
        start_index=1,
    )
    aux_writers = {}
    if runtime.write_aux_outputs:
        aux_writers = {
            "duplicates": PartWriter(_tmp_output_dir(paths.duplicates_dir), "dup", part_size),
            "rejects": PartWriter(_tmp_output_dir(paths.rejects_dir), "reject", part_size),
            "event_input": PartWriter(_tmp_output_dir(paths.event_dir), "event", part_size),
        }
    stats: Counter[str] = Counter()

    try:
        for row in db.canonical_rows():
            record = db.load_record(row)
            cleaned_record = build_cleaned_record(
                finalize_record(
                    record,
                    str(row["dedup_key"]),
                    str(row["dedup_method"]),
                    True,
                    str(row["canonical_id"]),
                    orjson.loads(row["dedup_debug"]),
                )
            )
            cleaned_writer.write(cleaned_record)
            stats["cleaned"] += 1
            if "event_input" in aux_writers:
                aux_writers["event_input"].write(build_event_record(cleaned_record))
                stats["event_input"] += 1

        if "duplicates" in aux_writers:
            for row in db.duplicate_rows():
                record = db.load_record(row)
                duplicate_record = build_cleaned_record(
                    finalize_record(
                        record,
                        str(row["dedup_key"]),
                        str(row["dedup_method"]),
                        False,
                        str(row["canonical_id"]),
                        orjson.loads(row["dedup_debug"]),
                    )
                )
                aux_writers["duplicates"].write(duplicate_record)
                stats["duplicates"] += 1

        if "rejects" in aux_writers:
            for row in db.reject_rows():
                aux_writers["rejects"].write(
                    {
                        "batch": row["batch"],
                        "line_no": row["line_no"],
                        "raw_id": row["raw_id"],
                        "reason": row["reason"],
                        "message": row["message"],
                        "raw_line": row["raw_line"],
                    }
                )
                stats["rejects"] += 1
    finally:
        for writer in [cleaned_writer, *aux_writers.values()]:
            writer.close()

    _replace_output_dir(cleaned_tmp, paths.cleaned_dir)
    if runtime.write_aux_outputs:
        _replace_output_dir(paths.duplicates_dir.parent / f".{paths.duplicates_dir.name}.tmp", paths.duplicates_dir)
        _replace_output_dir(paths.rejects_dir.parent / f".{paths.rejects_dir.name}.tmp", paths.rejects_dir)
        _replace_output_dir(paths.event_dir.parent / f".{paths.event_dir.name}.tmp", paths.event_dir)
    return dict(stats)


def run_pipeline(
    config: PipelineConfig,
    *,
    reset_state: bool = False,
    export_only: bool = False,
    force: bool = False,
) -> None:
    validate_config(config)
    paths = config.paths
    try:
        db = StagingDB(
            paths.state_dir / "dedup.db",
            payload_dir=paths.payload_dir,
            reset=reset_state,
            near_config=config.near_duplicates,
            target_scale_rows=config.runtime.target_scale_rows,
        )
    except StateVersionError as exc:
        raise SystemExit(str(exc)) from exc

    try:
        if not export_only:
            ingest_batches(config, db, force)

        export_stats = export_outputs(config, db)
        summary = write_reports(db, config)
    finally:
        db.close()

    print(f"Exported: {json.dumps(export_stats, sort_keys=True)}")
    near_merged = summary.get("near_duplicates_auto_merged", 0)
    print(
        f"报表已写入 {paths.reports_dir}/："
        f"canonical={summary.get('total_cleaned', 0)}, "
        f"duplicates={summary.get('total_duplicates', 0)}, "
        f"rejected={summary.get('total_rejected', 0)}, "
        f"near_merged={near_merged}"
    )
    print(f"详见 {paths.reports_dir}/README.md 或 {paths.reports_dir}/index.json")


def _flush_chunk(
    db: StagingDB,
    payload_writer: PayloadWriter,
    batch: str,
    chunk: list[tuple[int, bytes]],
    pool: ProcessPoolExecutor | None,
) -> Counter[str]:
    lines = [line for _, line in chunk]
    if pool:
        results = list(pool.map(transform_line, lines, chunksize=64))
    else:
        results = [transform_line(line) for line in lines]
    return _process_results(db, payload_writer, batch, chunk, results)


def _process_results(
    db: StagingDB,
    payload_writer: PayloadWriter,
    batch: str,
    chunk: list[tuple[int, bytes]],
    results: Iterable[TransformResult],
) -> Counter[str]:
    stats: Counter[str] = Counter()
    record_rows = []
    reject_rows = []

    for (line_no, raw_line), result in zip(chunk, results):
        stats["input"] += 1
        if result.record is None:
            stats["rejected"] += 1
            stats[f"reject:{result.reason or 'unknown_reject'}"] += 1
            reject_rows.append(_reject_row(batch, line_no, result, raw_line))
            continue

        stats["accepted"] += 1
        stats[f"type:{result.record['content_type']}"] += 1
        payload_ref = payload_writer.write(result.record)
        row = db.insert_record_rows(batch, line_no, result.record, payload_ref)
        stats[f"dedup:{row[9]}"] += 1
        record_rows.append(row)

    db.add_chunk(batch, record_rows, reject_rows, stats["input"])
    return stats


def _reject_row(
    batch: str,
    line_no: int,
    result: TransformResult,
    raw_line: bytes,
) -> RejectRow:
    return (
        batch,
        line_no,
        result.raw_id,
        result.reason or "unknown_reject",
        result.message or "",
        raw_line.decode("utf-8", errors="replace"),
    )


def _tmp_output_dir(final_dir: Path) -> Path:
    final_dir.parent.mkdir(parents=True, exist_ok=True)
    tmp = final_dir.parent / f".{final_dir.name}.tmp"
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True)
    return tmp


def _replace_output_dir(tmp_dir: Path, final_dir: Path) -> None:
    backup = final_dir.parent / f".{final_dir.name}.bak"
    if backup.exists():
        shutil.rmtree(backup)
    if final_dir.exists():
        final_dir.rename(backup)
    tmp_dir.rename(final_dir)
    if backup.exists():
        shutil.rmtree(backup)
