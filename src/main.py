from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path

from src.config import DEFAULT_CONFIG, NearDuplicateConfig, PathsConfig, PipelineConfig, RuntimeConfig
from src.pipeline import run_pipeline


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be >= 1")
    return parsed


def positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be > 0")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Clean AIME event NDJSON data")
    parser.add_argument(
        "command",
        nargs="?",
        choices=("run", "fresh", "export"),
        default="run",
        help="run uses config defaults, fresh rebuilds state, export rebuilds outputs from state",
    )
    parser.add_argument("--input", help="Advanced override: input directory")
    parser.add_argument("--output", help="Advanced override: cleaned output directory")
    parser.add_argument("--duplicates", help="Advanced override: duplicates output directory")
    parser.add_argument("--rejects", help="Advanced override: rejects output directory")
    parser.add_argument("--event-output", help="Advanced override: event input output directory")
    parser.add_argument("--state", help="Advanced override: state directory")
    parser.add_argument("--payload-dir", help="Advanced override: payload directory")
    parser.add_argument("--reports", help="Advanced override: reports directory")
    parser.add_argument("--workers", type=positive_int, help="Advanced override: process workers")
    parser.add_argument("--chunk-size", type=positive_int, help="Advanced override: transform chunk size")
    parser.add_argument("--part-size", type=positive_int, help="Advanced override: rows per output part")
    parser.add_argument("--payload-part-bytes", type=positive_int, help="Advanced override: payload part bytes")
    parser.add_argument("--near-min-body-chars", type=positive_int, help="Advanced override: near dedup body length gate")
    parser.add_argument("--near-threshold", type=positive_float, help="Advanced override: MinHash Jaccard threshold")
    parser.add_argument("--near-fuzzy-threshold", type=positive_float, help="Advanced override: RapidFuzz token set threshold")
    parser.add_argument("--no-near-dedup", action="store_true", help="Disable near-duplicate auto dedup")
    parser.add_argument("--force", action="store_true", help="Safely reprocess completed batches")
    parser.add_argument("--export-only", action="store_true", help="Compatibility alias for the export command")
    parser.add_argument("--reset-state", action="store_true", help="Compatibility alias for the fresh command")
    return parser


def config_from_args(args: argparse.Namespace, base: PipelineConfig = DEFAULT_CONFIG) -> PipelineConfig:
    path_updates = {}
    if args.input:
        path_updates["input_dir"] = Path(args.input)
    if args.output:
        path_updates["cleaned_dir"] = Path(args.output)
    if args.duplicates:
        path_updates["duplicates_dir"] = Path(args.duplicates)
    if args.rejects:
        path_updates["rejects_dir"] = Path(args.rejects)
    if args.event_output:
        path_updates["event_dir"] = Path(args.event_output)
    if args.state:
        path_updates["state_dir"] = Path(args.state)
    if args.payload_dir:
        path_updates["payload_dir"] = Path(args.payload_dir)
    if args.reports:
        path_updates["reports_dir"] = Path(args.reports)

    runtime_updates = {}
    for key in ("workers", "chunk_size", "part_size", "payload_part_bytes"):
        value = getattr(args, key)
        if value is not None:
            runtime_updates[key] = value

    near_updates = {}
    if args.no_near_dedup:
        near_updates["enabled"] = False
    if args.near_min_body_chars is not None:
        near_updates["min_body_chars"] = args.near_min_body_chars
    if args.near_threshold is not None:
        near_updates["threshold"] = args.near_threshold
    if args.near_fuzzy_threshold is not None:
        near_updates["fuzzy_threshold"] = args.near_fuzzy_threshold

    return PipelineConfig(
        paths=replace(base.paths, **path_updates) if path_updates else base.paths,
        runtime=replace(base.runtime, **runtime_updates) if runtime_updates else base.runtime,
        near_duplicates=replace(base.near_duplicates, **near_updates) if near_updates else base.near_duplicates,
    )


def main() -> None:
    args = build_parser().parse_args()
    command = args.command
    run_pipeline(
        config_from_args(args),
        reset_state=args.reset_state or command == "fresh",
        export_only=args.export_only or command == "export",
        force=args.force,
    )


if __name__ == "__main__":
    main()
