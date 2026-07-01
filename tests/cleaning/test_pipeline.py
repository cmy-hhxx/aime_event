from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import jsonschema
import orjson

ROOT = Path(__file__).resolve().parents[2]


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
    near_body = (
        "Apple shares rose after the company reported stronger quarterly earnings "
        "and raised its full year outlook. Analysts pointed to resilient iPhone demand, "
        "higher services revenue, and improving margins as the main drivers of the move. "
        "The update lifted sentiment across large technology stocks during afternoon trading."
    )
    rows: list[bytes] = [
        orjson.dumps(_raw("short", "Shared URL", "short body")),
        orjson.dumps(_raw("long", "Shared URL", "this body is much longer than the first", "https://example.com/news/story-1")),
        orjson.dumps(
            _raw(
                "seek-short",
                "SeekingAlpha story",
                "short seeking alpha body",
                "https://seekingalpha.com/news/4603239-story#source=home",
            )
        ),
        orjson.dumps(
            _raw(
                "seek-long",
                "SeekingAlpha story rewritten",
                "this seeking alpha body is much longer and should win",
                "https://seekingalpha.com/news/4603239-story/?utm_source=x&mod=mw_quote_news",
            )
        ),
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
                "near-a",
                "Apple shares rise after earnings outlook",
                near_body,
                "https://example.com/news/apple-earnings-a",
            )
        ),
        orjson.dumps(
            _raw(
                "near-b",
                "Apple stock rises after earnings outlook",
                near_body,
                "https://example.com/news/apple-earnings-b",
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
        "--event-output",
        str(tmp_path / "output" / "event_input"),
        "--state",
        str(tmp_path / "state"),
        "--payload-dir",
        str(tmp_path / "payloads"),
        "--final-state-dir",
        str(tmp_path / "final_state"),
        "--reports",
        str(tmp_path / "reports"),
        "--workers",
        "1",
        "--chunk-size",
        "2",
        "--part-size",
        "2",
        "--write-aux-outputs",
        *extra,
    ]
    subprocess.run(cmd, cwd=ROOT, check=True, capture_output=True, text=True)


def _read_parts(directory: Path) -> list[dict]:
    records = []
    for path in sorted(directory.glob("*.jsonl")):
        for line in path.read_bytes().splitlines():
            records.append(orjson.loads(line))
    return records


def _read_jsonl(path: Path) -> list[dict]:
    return [orjson.loads(line) for line in path.read_bytes().splitlines() if line]


def _has_null(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, dict):
        return any(_has_null(item) for item in value.values())
    if isinstance(value, list):
        return any(_has_null(item) for item in value)
    return False


def _semantic_summary(summary: dict) -> dict:
    return {key: value for key, value in summary.items() if key != "storage"}


def test_pipeline_exports_deterministic_global_parts_and_rejects(tmp_path: Path) -> None:
    _write_input(tmp_path / "input")

    _run_pipeline(tmp_path)

    cleaned = _read_parts(tmp_path / "output" / "cleaned")
    duplicates = _read_parts(tmp_path / "output" / "duplicates")
    event_records = _read_parts(tmp_path / "output" / "event_input")
    rejects = _read_parts(tmp_path / "output" / "rejects")
    schema = json.loads((ROOT / "schema" / "cleaning" / "cleaned_record.schema.json").read_text())
    event_schema = json.loads((ROOT / "schema" / "extraction" / "event_record.schema.json").read_text())
    validator = jsonschema.Draft7Validator(schema, format_checker=jsonschema.FormatChecker())
    event_validator = jsonschema.Draft7Validator(event_schema, format_checker=jsonschema.FormatChecker())

    assert len(cleaned) == 6
    assert len(duplicates) == 4
    assert len(event_records) == len(cleaned)
    assert len(rejects) == 3
    assert len({record["dedup"]["key"] for record in cleaned}) == len(cleaned)
    assert {record["id"] for record in cleaned} == {"long", "seek-long", "feed-a", "feed-b", "near-a", "notice"}

    feed_records = [record for record in cleaned if record["id"].startswith("feed-")]
    assert all(record["dedup"]["method"] == "content_hash" for record in feed_records)
    assert all(record["dedup"]["version"] == 4 for record in cleaned + duplicates)

    duplicate_methods = {record["id"]: record["dedup"]["method"] for record in duplicates}
    assert duplicate_methods["short"] == "source_url"
    assert duplicate_methods["seek-short"] == "source_url"
    assert duplicate_methods["feed-a"] == "id"
    assert duplicate_methods["near-b"] == "near_minhash"

    for record in cleaned + duplicates:
        assert not _has_null(record)
        validator.validate(record)
    for record in event_records:
        assert not _has_null(record)
        event_validator.validate(record)
        assert "dedup" not in record
        assert "meta" not in record
        assert "type_code" not in record
        assert "updated_at" not in record

    summary = json.loads((tmp_path / "reports" / "summary.json").read_text())
    assert summary["total_input"] == 13
    assert summary["total_cleaned"] == 6
    assert summary["total_duplicates"] == 4
    assert summary["total_rejected"] == 3
    assert summary["near_duplicate_candidates"] >= 1
    assert summary["near_duplicates_auto_merged"] >= 1
    assert summary["storage"]["db_bytes"] > 0
    assert summary["storage"]["payload_bytes"] > 0
    assert summary["storage"]["estimated_20m_rows_bytes"] > summary["storage"]["db_bytes"]
    near_report = _read_jsonl(tmp_path / "reports" / "near_duplicates.jsonl")
    assert any(record.get("status") == "auto_merged" for record in near_report)


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

    assert _semantic_summary(first_summary) == _semantic_summary(second_summary)
    assert _semantic_summary(first_summary) == _semantic_summary(export_only_summary)
    assert _semantic_summary(first_summary) == _semantic_summary(force_summary)
    cleaned = _read_parts(tmp_path / "output" / "cleaned")
    assert len({record["dedup"]["key"] for record in cleaned}) == len(cleaned)


def test_payload_offsets_can_reload_records(tmp_path: Path) -> None:
    from src.cleaning.storage import StagingDB

    _write_input(tmp_path / "input")
    _run_pipeline(tmp_path)

    db = StagingDB(tmp_path / "state" / "dedup.db", payload_dir=tmp_path / "payloads")
    try:
        row = next(iter(db.canonical_rows()))
        record = db.load_record(row)
        columns = {item[1] for item in db.conn.execute("PRAGMA table_info(records)").fetchall()}
    finally:
        db.close()

    assert record["id"] == row["id"]
    assert "record_json" not in columns
    assert {"payload_path", "payload_offset", "payload_length", "payload_sha256"} <= columns


def test_exports_are_sorted_by_published_at_ascending_across_parts(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True)
    rows = [
        orjson.dumps(_raw("late", "Late", "late body", "https://example.com/late", ctime=300)),
        orjson.dumps(_raw("early", "Early", "early body", "https://example.com/early", ctime=100)),
        orjson.dumps(_raw("middle", "Middle", "middle body", "https://example.com/middle", ctime=200)),
    ]
    (input_dir / "content_batch_0.ndjson").write_bytes(b"\n".join(rows) + b"\n")

    _run_pipeline(tmp_path, "--no-near-dedup")

    cleaned = _read_parts(tmp_path / "output" / "cleaned")
    event_records = _read_parts(tmp_path / "output" / "event_input")

    assert [record["id"] for record in cleaned] == ["early", "middle", "late"]
    assert [record["id"] for record in event_records] == ["early", "middle", "late"]
    assert len(list((tmp_path / "output" / "cleaned").glob("*.jsonl"))) == 2
