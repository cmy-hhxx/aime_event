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


def test_extract_subcommands_help(capsys):
    import pytest
    from src.cli.main import main
    for argv in (["extract", "--help"], ["complete", "--help"]):
        with pytest.raises(SystemExit) as e:
            main(argv)
        assert e.value.code == 0
    out = capsys.readouterr().out
    assert "assemble" in out
    assert "fetch-intraday" in out
    assert "import-intraday" in out


def test_run_all_removed():
    import pytest
    from src.cli.main import main
    with pytest.raises(SystemExit):
        main(["run-all"])


def test_date_flag_parses_and_validates():
    import argparse

    import pytest
    from src.cli.main import build_event_parser, iso_date

    for stage, step in (("extract", "cluster"), ("extract", "select"),
                        ("extract", "structure"), ("extract", "all"),
                        ("complete", "fetch"), ("complete", "label"),
                        ("complete", "assemble"), ("complete", "all")):
        args = build_event_parser(stage).parse_args([step, "--date", "2026-05-29"])
        assert args.date == "2026-05-29"

    with pytest.raises(SystemExit):
        build_event_parser("extract").parse_args(["select", "--date", "2026-13-01"])
    with pytest.raises(argparse.ArgumentTypeError):
        iso_date("not-a-date")


def test_allow_no_intraday_flag():
    from src.cli.main import build_event_parser
    args = build_event_parser("complete").parse_args(["assemble", "--allow-no-intraday"])
    assert args.allow_no_intraday is True
    args = build_event_parser("complete").parse_args(["all"])
    assert args.allow_no_intraday is False and args.date is None


def test_notice8k_subcommands_parse():
    from src.cli.main import build_event_parser
    p = build_event_parser("extract")
    args = p.parse_args(["notice8k", "--date", "2026-05-29", "--no-backfill"])
    assert args.step == "notice8k" and args.date == "2026-05-29" and args.no_backfill
    args = p.parse_args(["notice8k", "--day", "7"])
    assert args.day == 7 and args.date is None
    args = p.parse_args(["notice8k-select", "--date", "2026-05-29", "--limit", "5"])
    assert args.step == "notice8k-select" and args.limit == 5 and args.triage_workers == 12
    import pytest
    with pytest.raises(SystemExit):   # --date 与 --day 互斥
        p.parse_args(["notice8k", "--date", "2026-05-29", "--day", "3"])
    with pytest.raises(SystemExit):   # notice8k-select 必须给 --date
        p.parse_args(["notice8k-select"])
