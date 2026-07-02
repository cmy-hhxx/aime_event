from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _run_help(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "src.main", *args],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )


def test_top_level_help_lists_pipeline_stages() -> None:
    result = _run_help("--help")

    assert "clean" in result.stdout
    assert "extract" in result.stdout
    assert "complete" in result.stdout


def test_clean_help_keeps_cleaning_options() -> None:
    result = _run_help("clean", "--help")

    assert "--workers" in result.stdout
    assert "--no-near-dedup" in result.stdout


def test_placeholder_stage_help_is_available() -> None:
    extract = _run_help("extract", "--help")
    complete = _run_help("complete", "--help")

    assert "事件抽取" in extract.stdout
    assert "事件补全" in complete.stdout


def test_legacy_help_keeps_cleaning_options() -> None:
    result = _run_help("--help")

    assert "--workers" in result.stdout
    assert "--no-near-dedup" in result.stdout
