from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import orjson

from src.dedup import compute_dedup_key
from src.dedup_db import DedupDB
from src.transform import should_keep, transform

CHUNK_SIZE = 3000
BATCH_PATTERN = re.compile(r"content_batch_(\d+)\.ndjson$")


def _transform_line(line: bytes) -> dict | None:
    raw = orjson.loads(line)
    record = transform(raw)
    if not should_keep(raw, record["body"]):
        return None
    return record


def _transform_chunk(lines: list[bytes]) -> list[dict | None]:
    return [_transform_line(line) for line in lines]


def _load_progress(path: Path) -> set[str]:
    if not path.exists():
        return set()
    data = json.loads(path.read_text())
    return set(data.get("completed", []))


def _save_progress(path: Path, completed: set[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"completed": sorted(completed)}, indent=2))


def _output_name(batch_file: Path) -> str:
    match = BATCH_PATTERN.search(batch_file.name)
    if match:
        return f"cleaned_batch_{match.group(1)}.ndjson"
    return f"cleaned_{batch_file.stem}.ndjson"


def _finalize_record(record: dict, key: str, method: str, is_canonical: bool) -> dict:
    out = {k: v for k, v in record.items() if k != "_body_len"}
    out["dedup"] = {"key": key, "method": method, "is_canonical": is_canonical}
    return out


def process_batch(
    batch_file: Path,
    cleaned_dir: Path,
    dup_dir: Path,
    dedup_db: DedupDB,
    workers: int,
) -> dict:
    stats = Counter()
    cleaned_path = cleaned_dir / _output_name(batch_file)
    dup_path = dup_dir / cleaned_path.name.replace("cleaned_", "dup_")

    with (
        open(batch_file, "rb") as fin,
        open(cleaned_path, "wb") as fclean,
        open(dup_path, "wb") as fdup,
    ):
        chunk: list[bytes] = []
        pool = ProcessPoolExecutor(max_workers=workers) if workers > 1 else None

        def flush(chunk_lines: list[bytes]):
            if not chunk_lines:
                return
            if pool:
                results = pool.map(_transform_line, chunk_lines, chunksize=64)
                results = list(results)
            else:
                results = [_transform_line(line) for line in chunk_lines]

            for record in results:
                stats["input"] += 1
                if record is None:
                    stats["rejected"] += 1
                    continue

                content_key_method = compute_dedup_key(record)
                if content_key_method:
                    key, method = content_key_method
                else:
                    key, method = f"id:{record['id']}", "id"

                body_len = record.get("_body_len", 0)
                status, ref_id = dedup_db.register(
                    record["id"], content_key_method[0] if content_key_method else None,
                    method, record["content_type"], body_len,
                )

                if status == "new":
                    stats["cleaned"] += 1
                    stats[f"type:{record['content_type']}"] += 1
                    stats[f"dedup:{method}"] += 1
                    fclean.write(orjson.dumps(_finalize_record(record, key, method, True)) + b"\n")
                elif status == "replaced":
                    stats["cleaned"] += 1
                    stats["superseded"] += 1
                    stats[f"type:{record['content_type']}"] += 1
                    stats[f"dedup:{method}"] += 1
                    fclean.write(orjson.dumps(_finalize_record(record, key, method, True)) + b"\n")
                else:
                    stats["duplicates"] += 1
                    stats[f"dedup:{method}"] += 1
                    fdup.write(orjson.dumps(_finalize_record(record, key, method, False)) + b"\n")

        for line in fin:
            line = line.strip()
            if not line:
                continue
            chunk.append(line)
            if len(chunk) >= CHUNK_SIZE:
                flush(chunk)
                chunk = []

        flush(chunk)
        if pool:
            pool.shutdown()

    dedup_db.commit()
    return dict(stats)


def discover_batches(input_dir: Path) -> list[Path]:
    return sorted(input_dir.glob("content_batch_*.ndjson"))


def run(args):
    input_dir = Path(args.input)
    cleaned_dir = Path(args.output)
    dup_dir = Path(args.duplicates)
    state_dir = Path(args.state)
    reports_dir = Path(args.reports)

    for d in (cleaned_dir, dup_dir, state_dir, reports_dir):
        d.mkdir(parents=True, exist_ok=True)

    progress_path = state_dir / "progress.json"
    completed = _load_progress(progress_path)
    dedup_db = DedupDB(state_dir / "dedup.db")

    batch_stats_path = reports_dir / "batch_stats.jsonl"
    total_stats = Counter()
    batches = discover_batches(input_dir)

    if not batches:
        print(f"No batch files found in {input_dir}")
        return

    for batch_file in batches:
        if batch_file.name in completed and not args.force:
            print(f"Skip (done): {batch_file.name}")
            continue

        print(f"Processing: {batch_file.name}")
        stats = process_batch(batch_file, cleaned_dir, dup_dir, dedup_db, args.workers)
        stats["batch"] = batch_file.name
        with open(batch_stats_path, "a") as f:
            f.write(json.dumps(stats) + "\n")

        for k, v in stats.items():
            if k != "batch":
                total_stats[k] += v

        completed.add(batch_file.name)
        _save_progress(progress_path, completed)
        print(
            f"  input={stats.get('input', 0)} cleaned={stats.get('cleaned', 0)} "
            f"dup={stats.get('duplicates', 0)} rejected={stats.get('rejected', 0)}"
        )

    summary = {
        "total_input": total_stats.get("input", 0),
        "total_cleaned": total_stats.get("cleaned", 0),
        "total_duplicates": total_stats.get("duplicates", 0),
        "total_rejected": total_stats.get("rejected", 0),
        "total_superseded": total_stats.get("superseded", 0),
        "dedup_index_size": dedup_db.count(),
        "by_content_type": {k.split(":", 1)[1]: v for k, v in total_stats.items() if k.startswith("type:")},
        "dedup_by_method": {k.split(":", 1)[1]: v for k, v in total_stats.items() if k.startswith("dedup:")},
        "batches_processed": len(completed),
    }
    (reports_dir / "summary.json").write_text(json.dumps(summary, indent=2))
    dedup_db.close()
    print(f"\nDone. Summary: {json.dumps(summary, indent=2)}")


def main():
    parser = argparse.ArgumentParser(description="Clean financial consultation NDJSON data")
    parser.add_argument("--input", default="data/raw", help="Input directory with batch files")
    parser.add_argument("--output", default="output/cleaned", help="Cleaned output directory")
    parser.add_argument("--duplicates", default="output/duplicates", help="Duplicates output directory")
    parser.add_argument("--state", default="state", help="State directory (dedup.db, progress.json)")
    parser.add_argument("--reports", default="reports", help="Reports directory")
    parser.add_argument("--workers", type=int, default=4, help="Process pool workers per batch")
    parser.add_argument("--force", action="store_true", help="Reprocess completed batches")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
