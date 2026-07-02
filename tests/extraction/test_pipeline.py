from __future__ import annotations

from pathlib import Path

import orjson

from src.extraction.client import parse_json_object
from src.extraction.models import ExtractionSettings
from src.extraction.pipeline import discover_cleaned_files, run_pipeline


class FakeClient:
    model = "fake-model"

    def complete_json(self, **kwargs):
        assert "Apple" in kwargs["user_prompt"]
        return {
            "events": [
                {
                    "event_type": "product",
                    "event_title": "Apple launches a new device",
                    "event_time": None,
                    "entities": [{"name": "Apple", "type": "company", "symbol": "AAPL"}],
                    "summary": "Apple launched a new device.",
                    "evidence": "Apple launched a new device",
                    "confidence": 0.92,
                }
            ]
        }


def test_parse_json_object_from_markdown_fence() -> None:
    assert parse_json_object('```json\n{"events": []}\n```') == {"events": []}


def test_discover_cleaned_files_uses_numeric_order(tmp_path: Path) -> None:
    for name in ("cleaned_batch10.jsonl", "cleaned_batch2.jsonl", "cleaned_batch1.jsonl"):
        (tmp_path / name).write_text("", encoding="utf-8")

    assert [path.name for path in discover_cleaned_files(tmp_path)] == [
        "cleaned_batch1.jsonl",
        "cleaned_batch2.jsonl",
        "cleaned_batch10.jsonl",
    ]


def test_run_pipeline_with_fake_client(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    record = {
        "id": "r1",
        "content_type": "US_NEWS",
        "title": "Apple launched a new device",
        "body": "Apple launched a new device in California.",
        "published_at": "2026-01-01T00:00:00Z",
    }
    (input_dir / "cleaned_batch1.jsonl").write_bytes(orjson.dumps(record) + b"\n")

    stats = run_pipeline(
        ExtractionSettings(input_path=input_dir, output_dir=output_dir, limit=1),
        client=FakeClient(),
    )

    assert stats == {"input": 1, "events": 1, "errors": 0}
    lines = (output_dir / "event_batch1.jsonl").read_bytes().splitlines()
    assert len(lines) == 1
    payload = orjson.loads(lines[0])
    assert payload["source_id"] == "r1"
    assert payload["events"][0]["event_type"] == "product"
