from __future__ import annotations

import argparse
import json
import shutil
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, BinaryIO, Iterable

import orjson

from src.dedup import finalize_record
from src.dedup_db import RejectRow, StagingDB, StateVersionError
from src.transform import TransformResult, transform_line

DEFAULT_CHUNK_SIZE = 3000
DEFAULT_PART_SIZE = 100_000


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be >= 1")
    return parsed


class PartWriter:
    def __init__(self, directory: Path, prefix: str, part_size: int):
        self.directory = directory
        self.prefix = prefix
        self.part_size = part_size
        self.count = 0
        self.part_index = 0
        self.handle: BinaryIO | None = None
        directory.mkdir(parents=True, exist_ok=True)

    def write(self, payload: dict[str, Any]) -> None:
        if self.handle is None or self.count % self.part_size == 0:
            self._open_next()
        assert self.handle is not None
        self.handle.write(orjson.dumps(payload) + b"\n")
        self.count += 1

    def close(self) -> None:
        if self.handle is not None:
            self.handle.close()
            self.handle = None

    def _open_next(self) -> None:
        if self.handle is not None:
            self.handle.close()
        path = self.directory / f"{self.prefix}_part_{self.part_index:05d}.ndjson"
        self.handle = path.open("wb")
        self.part_index += 1


def discover_batches(input_dir: Path) -> list[Path]:
    return sorted(input_dir.glob("content_batch_*.ndjson"))


def _decode_line(line: bytes) -> str:
    return line.decode("utf-8", errors="replace")


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
        _decode_line(raw_line),
    )


def _process_results(
    db: StagingDB,
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
        row = db.insert_record_rows(batch, line_no, result.record)
        stats[f"dedup:{row[-1]}"] += 1
        record_rows.append(row)

    db.add_chunk(batch, record_rows, reject_rows, stats["input"])
    return stats


def ingest_batch(
    batch_file: Path,
    db: StagingDB,
    workers: int,
    chunk_size: int,
    force: bool,
) -> Counter[str] | None:
    batch = batch_file.name
    should_run = db.prepare_batch(batch, batch_file, force=force)
    if not should_run:
        return None

    stats: Counter[str] = Counter()
    pool = ProcessPoolExecutor(max_workers=workers) if workers > 1 else None
    try:
        with batch_file.open("rb") as handle:
            chunk: list[tuple[int, bytes]] = []
            for line_no, raw_line in enumerate(handle, start=1):
                line = raw_line.strip()
                if not line:
                    continue
                chunk.append((line_no, line))
                if len(chunk) >= chunk_size:
                    stats.update(_flush_chunk(db, batch, chunk, pool))
                    chunk = []

            if chunk:
                stats.update(_flush_chunk(db, batch, chunk, pool))
    finally:
        if pool:
            pool.shutdown()

    db.complete_batch(batch)
    return stats


def _flush_chunk(
    db: StagingDB,
    batch: str,
    chunk: list[tuple[int, bytes]],
    pool: ProcessPoolExecutor | None,
) -> Counter[str]:
    lines = [line for _, line in chunk]
    if pool:
        results = list(pool.map(transform_line, lines, chunksize=64))
    else:
        results = [transform_line(line) for line in lines]
    return _process_results(db, batch, chunk, results)


def ingest_batches(
    input_dir: Path,
    db: StagingDB,
    workers: int,
    chunk_size: int,
    force: bool,
) -> None:
    batches = discover_batches(input_dir)
    if not batches:
        print(f"No batch files found in {input_dir}")
        return

    for batch_file in batches:
        stats = ingest_batch(batch_file, db, workers, chunk_size, force)
        if stats is None:
            print(f"Skip (done): {batch_file.name}")
            continue
        print(
            f"Processed: {batch_file.name} "
            f"input={stats.get('input', 0)} accepted={stats.get('accepted', 0)} "
            f"rejected={stats.get('rejected', 0)}"
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


def export_outputs(
    db: StagingDB,
    cleaned_dir: Path,
    dup_dir: Path,
    reject_dir: Path,
    part_size: int,
) -> dict[str, int]:
    cleaned_tmp = _tmp_output_dir(cleaned_dir)
    dup_tmp = _tmp_output_dir(dup_dir)
    reject_tmp = _tmp_output_dir(reject_dir)
    writers = [
        PartWriter(cleaned_tmp, "cleaned", part_size),
        PartWriter(dup_tmp, "dup", part_size),
        PartWriter(reject_tmp, "reject", part_size),
    ]
    stats: Counter[str] = Counter()

    try:
        for row in db.canonical_rows():
            record = orjson.loads(row["record_json"])
            writers[0].write(
                finalize_record(
                    record,
                    str(row["dedup_key"]),
                    str(row["dedup_method"]),
                    True,
                    str(row["canonical_id"]),
                )
            )
            stats["cleaned"] += 1

        for row in db.duplicate_rows():
            record = orjson.loads(row["record_json"])
            writers[1].write(
                finalize_record(
                    record,
                    str(row["dedup_key"]),
                    str(row["dedup_method"]),
                    False,
                    str(row["canonical_id"]),
                )
            )
            stats["duplicates"] += 1

        for row in db.reject_rows():
            writers[2].write(
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
        for writer in writers:
            writer.close()

    _replace_output_dir(cleaned_tmp, cleaned_dir)
    _replace_output_dir(dup_tmp, dup_dir)
    _replace_output_dir(reject_tmp, reject_dir)
    return dict(stats)


def run(args: argparse.Namespace) -> None:
    input_dir = Path(args.input)
    cleaned_dir = Path(args.output)
    dup_dir = Path(args.duplicates)
    reject_dir = Path(args.rejects)
    state_dir = Path(args.state)
    reports_dir = Path(args.reports)

    try:
        db = StagingDB(state_dir / "dedup.db", reset=args.reset_state)
    except StateVersionError as exc:
        raise SystemExit(str(exc)) from exc

    try:
        if not args.export_only:
            ingest_batches(input_dir, db, args.workers, args.chunk_size, args.force)

        export_stats = export_outputs(db, cleaned_dir, dup_dir, reject_dir, args.part_size)
        summary = db.write_reports(reports_dir, state_dir / "progress.json")
    finally:
        db.close()

    print(f"Exported: {json.dumps(export_stats, sort_keys=True)}")
    print(f"Done. Summary: {json.dumps(summary, indent=2, sort_keys=True)}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Clean financial consultation NDJSON data")
    parser.add_argument("--input", default="data/raw", help="Input directory with batch files")
    parser.add_argument("--output", default="output/cleaned", help="Cleaned output directory")
    parser.add_argument("--duplicates", default="output/duplicates", help="Duplicates output directory")
    parser.add_argument("--rejects", default="output/rejects", help="Rejected-row quarantine output directory")
    parser.add_argument("--state", default="state", help="State directory (dedup.db, progress.json)")
    parser.add_argument("--reports", default="reports", help="Reports directory")
    parser.add_argument("--workers", type=positive_int, default=4, help="Process pool workers per batch")
    parser.add_argument("--chunk-size", type=positive_int, default=DEFAULT_CHUNK_SIZE, help="Transform/insert chunk size")
    parser.add_argument("--part-size", type=positive_int, default=DEFAULT_PART_SIZE, help="Rows per output part")
    parser.add_argument("--force", action="store_true", help="Safely reprocess completed batches")
    parser.add_argument("--export-only", action="store_true", help="Skip ingest and rebuild outputs/reports from staging DB")
    parser.add_argument("--reset-state", action="store_true", help="Delete staging DB before running")
    return parser


def main() -> None:
    run(build_parser().parse_args())


if __name__ == "__main__":
    main()
