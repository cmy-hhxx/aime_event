from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import orjson


from src.cleaning.storage import directory_size


def _record(index: int, duplicate_every: int, near_every: int) -> dict:
    group = index // duplicate_every if duplicate_every else index
    near_group = index // near_every if near_every else index
    record_id = f"synthetic-{index:09d}"
    if near_every:
        title = f"Synthetic market event {near_group} update {index % near_every}"
        content = (
            f"Company ABC reported synthetic event {near_group}. Revenue improved, shares moved higher, "
            "analysts cited stronger margins and resilient demand, and the company raised its full year outlook. "
            "The benchmark paragraph is intentionally repeated so near-duplicate detection can be measured."
        )
        url = f"https://example.com/news/near-{near_group}-{index}?utm_source=bench#fragment"
    else:
        title = f"Synthetic market event {group}"
        content = f"Company ABC reported event {group}. Revenue changed and shares moved in synthetic row {index}."
        url = f"https://example.com/news/event-{group}?utm_source=bench#fragment"
    return {
        "_id": record_id,
        "businessCode": "US_NEWS",
        "type": 0,
        "title": title,
        "content": f"<p>{content}</p>",
        "ctime": 1781568000 + index,
        "rtime": None,
        "news": {
            "source": "Synthetic",
            "sourceUrl": url,
        },
        "links": [{"type": "stock", "param": {"stockCode": "ABC", "stockName": "ABC Corp"}}],
        "contentTagMap": {
            "1": {"code": "Stock"},
            "2": {"code": "NorthAmerica"},
            "3": {"code": "us_mid_importance"},
        },
        "materialId": f"mat-{index}",
    }


def generate_input(path: Path, rows: int, duplicate_every: int, near_every: int) -> None:
    path.mkdir(parents=True, exist_ok=True)
    output = path / "content_batch_0.ndjson"
    with output.open("wb") as handle:
        for index in range(rows):
            handle.write(orjson.dumps(_record(index, duplicate_every, near_every)) + b"\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a synthetic benchmark and run the cleaning pipeline")
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--duplicate-every", type=int, default=10)
    parser.add_argument("--near-every", type=int, default=0)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--keep-dir", type=Path, default=None)
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory() as tmp_name:
        tmp = args.keep_dir or Path(tmp_name)
        tmp.mkdir(parents=True, exist_ok=True)
        input_dir = tmp / "input"
        generate_input(input_dir, args.rows, args.duplicate_every, args.near_every)

        cmd = [
            sys.executable,
            "-m",
            "src.main",
            "fresh",
            "--input",
            str(input_dir),
            "--output",
            str(tmp / "output" / "cleaned"),
            "--duplicates",
            str(tmp / "output" / "duplicates"),
            "--rejects",
            str(tmp / "output" / "rejects"),
            "--event-output",
            str(tmp / "output" / "event_input"),
            "--state",
            str(tmp / "state"),
            "--payload-dir",
            str(tmp / "payloads"),
            "--reports",
            str(tmp / "reports"),
            "--workers",
            str(args.workers),
        ]

        start = time.monotonic()
        subprocess.run(cmd, cwd=root, check=True)
        elapsed = time.monotonic() - start

        summary = json.loads((tmp / "reports" / "summary.json").read_text())
        output = {
            "rows": args.rows,
            "elapsed_seconds": round(elapsed, 3),
            "rows_per_second": round(args.rows / elapsed, 1) if elapsed else args.rows,
            "input_bytes": directory_size(input_dir),
            "state_bytes": directory_size(tmp / "state"),
            "payload_bytes": directory_size(tmp / "payloads"),
            "output_bytes": directory_size(tmp / "output"),
            "estimated_20m_rows_bytes": summary["storage"]["estimated_20m_rows_bytes"],
            "near_duplicate_candidates": summary.get("near_duplicate_candidates", 0),
            "near_duplicates_auto_merged": summary.get("near_duplicates_auto_merged", 0),
            "near_duplicates_report_only": summary.get("near_duplicates_report_only", 0),
            "summary": summary,
            "work_dir": str(tmp),
        }
        print(json.dumps(output, indent=2, sort_keys=True))

        if args.keep_dir is not None:
            return


if __name__ == "__main__":
    main()
