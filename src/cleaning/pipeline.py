from __future__ import annotations

import json
import shutil
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Iterable

import orjson

from src.config import PipelineConfig, validate_config
from src.cleaning.dedup.exact import finalize_record
from src.cleaning.ingest.transform import TransformResult, transform_line
from src.cleaning.output.views import build_cleaned_record, build_event_record
from src.common.io import PartWriter
from src.common.logging import ProgressLogger, log
from src.common.paths import discover_content_batches, replace_output_dir, tmp_output_dir
from src.cleaning.reporting import write_reports
from src.cleaning.storage import PayloadWriter, RejectRow, StagingDB, StateVersionError


def discover_batches(input_dir: Path) -> list[Path]:
    return discover_content_batches(input_dir)


def ingest_batches(config: PipelineConfig, db: StagingDB, force: bool) -> None:
    paths = config.paths
    batches = discover_batches(paths.input_dir)
    if not batches:
        log(f"No batch files found in {paths.input_dir}")
        return

    log(
        f"Ingest: discovered {len(batches)} batch files in {paths.input_dir}; "
        f"workers={config.runtime.workers} chunk_size={config.runtime.chunk_size}"
    )
    log(f"Ingest: first={batches[0].name} last={batches[-1].name}")
    for index, batch_file in enumerate(batches, start=1):
        size_mb = batch_file.stat().st_size / 1024 / 1024
        log(f"Ingest: start {index}/{len(batches)} {batch_file.name} size={size_mb:.1f}MB")
        stats = ingest_batch(batch_file, db, config, force)
        if stats is None:
            log(f"Skip (done): {batch_file.name}")
            continue
        log(
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
    progress = ProgressLogger(
        f"Ingest {batch}",
        runtime.log_every_rows,
        runtime.log_every_seconds,
    )
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
                    progress.maybe(
                        stats["input"],
                        f"accepted={stats['accepted']:,} rejected={stats['rejected']:,}",
                    )
                    chunk = []

            if chunk:
                stats.update(_flush_chunk(db, payload_writer, batch, chunk, pool))
                progress.maybe(
                    stats["input"],
                    f"accepted={stats['accepted']:,} rejected={stats['rejected']:,}",
                )
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
    log(
        f"Export: start cleaned_dir={paths.cleaned_dir} part_size={part_size:,} "
        f"write_aux_outputs={runtime.write_aux_outputs}"
    )
    log("Export: building winner tables")
    db.build_winner_tables()
    log("Export: winner tables ready")
    cleaned_tmp = tmp_output_dir(paths.cleaned_dir)
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
            "duplicates": PartWriter(tmp_output_dir(paths.duplicates_dir), "dup", part_size),
            "rejects": PartWriter(tmp_output_dir(paths.rejects_dir), "reject", part_size),
            "event_input": PartWriter(tmp_output_dir(paths.event_dir), "event", part_size),
        }
    stats: Counter[str] = Counter()
    cleaned_progress = ProgressLogger("Export cleaned", runtime.log_every_rows, runtime.log_every_seconds)

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
            cleaned_progress.maybe(stats["cleaned"])
            if "event_input" in aux_writers:
                aux_writers["event_input"].write(build_event_record(cleaned_record))
                stats["event_input"] += 1

        if "duplicates" in aux_writers:
            duplicate_progress = ProgressLogger("Export duplicates", runtime.log_every_rows, runtime.log_every_seconds)
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
                duplicate_progress.maybe(stats["duplicates"])

        if "rejects" in aux_writers:
            reject_progress = ProgressLogger("Export rejects", runtime.log_every_rows, runtime.log_every_seconds)
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
                reject_progress.maybe(stats["rejects"])
    finally:
        for writer in [cleaned_writer, *aux_writers.values()]:
            writer.close()

    replace_output_dir(cleaned_tmp, paths.cleaned_dir)
    log(f"Export: replaced cleaned output directory {paths.cleaned_dir}")
    if runtime.write_aux_outputs:
        replace_output_dir(paths.duplicates_dir.parent / f".{paths.duplicates_dir.name}.tmp", paths.duplicates_dir)
        replace_output_dir(paths.rejects_dir.parent / f".{paths.rejects_dir.name}.tmp", paths.rejects_dir)
        replace_output_dir(paths.event_dir.parent / f".{paths.event_dir.name}.tmp", paths.event_dir)
        log("Export: replaced auxiliary output directories")
    return dict(stats)


def save_final_state(paths) -> None:
    source = paths.state_dir
    target = paths.final_state_dir
    if source.resolve() == target.resolve():
        log(f"State: final state already at {target}")
        return
    if not source.exists():
        log(f"State: no local state directory found at {source}")
        return

    tmp_target = target.parent / f".{target.name}.tmp"
    backup = target.parent / f".{target.name}.bak"
    log(f"State: saving local state {source} -> {target}")
    if tmp_target.exists():
        shutil.rmtree(tmp_target)
    if backup.exists():
        shutil.rmtree(backup)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source, tmp_target)
    if target.exists():
        target.rename(backup)
    tmp_target.rename(target)
    if backup.exists():
        shutil.rmtree(backup)
    log(f"State: saved final state to {target}")


def run_pipeline(
    config: PipelineConfig,
    *,
    reset_state: bool = False,
    export_only: bool = False,
    force: bool = False,
) -> None:
    validate_config(config)
    paths = config.paths
    log(
        f"Pipeline: command reset_state={reset_state} export_only={export_only} force={force} "
        f"input={paths.input_dir} state={paths.state_dir} reports={paths.reports_dir}"
    )
    try:
        log("Pipeline: opening staging database")
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
        log("Reports: writing reports")
        summary = write_reports(db, config)
    finally:
        db.close()
    save_final_state(paths)

    log(f"Exported: {json.dumps(export_stats, sort_keys=True)}")
    near_merged = summary.get("near_duplicates_auto_merged", 0)
    log(
        f"报表已写入 {paths.reports_dir}/："
        f"canonical={summary.get('total_cleaned', 0)}, "
        f"duplicates={summary.get('total_duplicates', 0)}, "
        f"rejected={summary.get('total_rejected', 0)}, "
        f"near_merged={near_merged}"
    )
    log(f"详见 {paths.reports_dir}/README.md 或 {paths.reports_dir}/index.json")


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
