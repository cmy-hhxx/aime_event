from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import jsonschema
import orjson

ROOT = Path(__file__).resolve().parents[1]


def _raw(
    record_id: str,
    title: str,
    body: str,
    url: str | None = "https://example.com/news/story-1",
    business_code: str = "US_NEWS",
    ctime: int | None = 1781568000,
    notice_info: dict | None = None,
) -> dict:
    return {
        "_id": record_id,
        "businessCode": business_code,
        "type": 12 if business_code == "US_NOTICE" else 0,
        "title": title,
        "content": f"<p>{body}</p>" if body else "",
        "ctime": ctime,
        "rtime": None,
        "news": {"sourceUrl": url, "source": "Example"} if url else {"source": "Example"},
        "noticeInfo": notice_info,
    }


def _write_input(input_dir: Path) -> None:
    input_dir.mkdir(parents=True)
    rows: list[bytes] = [
        orjson.dumps(_raw("short", "Shared URL", "short body")),
        orjson.dumps(_raw("long", "Shared URL", "this body is much longer than the first", "https://example.com/news/story-1")),
        orjson.dumps(
            _raw(
                "feed-a",
                "Reuters story A",
                "Alpha body",
                "https://www.reuters.com/arc/outboundfeeds/news-sitemap/?outputType=xml",
            )
        ),
        orjson.dumps(
            _raw(
                "feed-b",
                "Reuters story B",
                "Beta body",
                "https://www.reuters.com/arc/outboundfeeds/news-sitemap/?outputType=xml",
            )
        ),
        orjson.dumps(
            _raw(
                "feed-a",
                "Reuters story A",
                "Alpha body with extra words",
                "https://www.reuters.com/arc/outboundfeeds/news-sitemap/?outputType=xml",
            )
        ),
        orjson.dumps(
            _raw(
                "notice",
                "Form 4",
                "",
                None,
                "US_NOTICE",
                notice_info={
                    "noticeType": "4",
                    "declareDate": "2026-06-15",
                    "attachmentList": [{"url": "https://www.sec.gov/doc.xml", "fileType": "4"}],
                },
            )
        ),
        orjson.dumps(_raw("empty", "No body", "")),
        orjson.dumps(_raw("missing-time", "No time", "body", ctime=None)),
        b'{"broken":',
    ]
    (input_dir / "content_batch_0.ndjson").write_bytes(b"\n".join(rows) + b"\n")


def _run_pipeline(tmp_path: Path, *extra: str) -> None:
    cmd = [
        sys.executable,
        "-m",
        "src.main",
        "--input",
        str(tmp_path / "input"),
        "--output",
        str(tmp_path / "output" / "cleaned"),
        "--duplicates",
        str(tmp_path / "output" / "duplicates"),
        "--rejects",
        str(tmp_path / "output" / "rejects"),
        "--state",
        str(tmp_path / "state"),
        "--reports",
        str(tmp_path / "reports"),
        "--workers",
        "1",
        "--chunk-size",
        "2",
        "--part-size",
        "2",
        *extra,
    ]
    subprocess.run(cmd, cwd=ROOT, check=True, capture_output=True, text=True)


def _read_parts(directory: Path) -> list[dict]:
    records = []
    for path in sorted(directory.glob("*.ndjson")):
        for line in path.read_bytes().splitlines():
            records.append(orjson.loads(line))
    return records


def test_pipeline_exports_deterministic_global_parts_and_rejects(tmp_path: Path) -> None:
    _write_input(tmp_path / "input")

    _run_pipeline(tmp_path)

    cleaned = _read_parts(tmp_path / "output" / "cleaned")
    duplicates = _read_parts(tmp_path / "output" / "duplicates")
    rejects = _read_parts(tmp_path / "output" / "rejects")
    schema = json.loads((ROOT / "schema" / "cleaned_record.schema.json").read_text())
    validator = jsonschema.Draft7Validator(schema, format_checker=jsonschema.FormatChecker())

    assert len(cleaned) == 4
    assert len(duplicates) == 2
    assert len(rejects) == 3
    assert len({record["dedup"]["key"] for record in cleaned}) == len(cleaned)
    assert {record["id"] for record in cleaned} == {"long", "feed-a", "feed-b", "notice"}

    feed_records = [record for record in cleaned if record["id"].startswith("feed-")]
    assert all(record["dedup"]["method"] == "content_hash" for record in feed_records)

    duplicate_methods = {record["id"]: record["dedup"]["method"] for record in duplicates}
    assert duplicate_methods["short"] == "source_url"
    assert duplicate_methods["feed-a"] == "id"

    for record in cleaned + duplicates:
        validator.validate(record)

    summary = json.loads((tmp_path / "reports" / "summary.json").read_text())
    assert summary["total_input"] == 9
    assert summary["total_cleaned"] == 4
    assert summary["total_duplicates"] == 2
    assert summary["total_rejected"] == 3


def test_resume_export_only_and_force_are_stable(tmp_path: Path) -> None:
    _write_input(tmp_path / "input")

    _run_pipeline(tmp_path)
    first_summary = json.loads((tmp_path / "reports" / "summary.json").read_text())
    _run_pipeline(tmp_path)
    second_summary = json.loads((tmp_path / "reports" / "summary.json").read_text())
    _run_pipeline(tmp_path, "--export-only")
    export_only_summary = json.loads((tmp_path / "reports" / "summary.json").read_text())
    _run_pipeline(tmp_path, "--force")
    force_summary = json.loads((tmp_path / "reports" / "summary.json").read_text())

    assert first_summary == second_summary == export_only_summary == force_summary
    cleaned = _read_parts(tmp_path / "output" / "cleaned")
    assert len({record["dedup"]["key"] for record in cleaned}) == len(cleaned)
