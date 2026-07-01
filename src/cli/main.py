from __future__ import annotations

import argparse
import sys
from dataclasses import replace
from pathlib import Path

from src.config import DEFAULT_CONFIG, PipelineConfig


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
    parser = argparse.ArgumentParser(
        description="AIME 事件流水线：cleaning / extraction / completion",
        epilog=(
            "阶段入口：clean, extract, complete, run-all。"
            "兼容旧命令：run, fresh, export 仍直接执行 cleaning。"
        ),
    )
    add_cleaning_arguments(parser)
    return parser


def build_clean_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m src.main clean",
        description="清洗 AIME 事件 NDJSON 数据（日常配置请改 src/config.py）",
    )
    add_cleaning_arguments(parser)
    return parser


def build_stage_parser(stage: str, description: str) -> argparse.ArgumentParser:
    return argparse.ArgumentParser(
        prog=f"python -m src.main {stage}",
        description=description,
    )


def add_cleaning_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "command",
        nargs="?",
        choices=("run", "fresh", "export"),
        default="run",
        help="run=增量处理, fresh=全量重建, export=仅从 state 重导出",
    )
    parser.add_argument("--input", help="高级覆盖：原始输入目录")
    parser.add_argument("--output", help="高级覆盖：cleaned 输出目录")
    parser.add_argument("--duplicates", help="高级覆盖：duplicates 输出目录")
    parser.add_argument("--rejects", help="高级覆盖：rejects 输出目录")
    parser.add_argument("--event-output", help="高级覆盖：event_input 输出目录")
    parser.add_argument("--state", help="高级覆盖：state 目录")
    parser.add_argument("--payload-dir", help="高级覆盖：payload 目录")
    parser.add_argument("--final-state-dir", help="高级覆盖：最终保存 state 的目录")
    parser.add_argument("--reports", help="高级覆盖：reports 目录")
    parser.add_argument("--workers", type=positive_int, help="高级覆盖：并行进程数")
    parser.add_argument("--chunk-size", type=positive_int, help="高级覆盖：transform 分块行数")
    parser.add_argument("--part-size", type=positive_int, help="高级覆盖：每个输出分片最大行数")
    parser.add_argument("--payload-part-bytes", type=positive_int, help="高级覆盖：payload 分片字节数")
    parser.add_argument("--log-every-rows", type=positive_int, help="高级覆盖：每处理多少行打印一次进度")
    parser.add_argument("--log-every-seconds", type=positive_int, help="高级覆盖：至少每隔多少秒打印一次进度")
    parser.add_argument("--near-min-body-chars", type=positive_int, help="高级覆盖：近似去重正文最短字符数")
    parser.add_argument("--near-threshold", type=positive_float, help="高级覆盖：MinHash Jaccard 阈值")
    parser.add_argument("--near-fuzzy-threshold", type=positive_float, help="高级覆盖：RapidFuzz 正文相似度阈值")
    parser.add_argument("--no-near-dedup", action="store_true", help="关闭近似去重自动合并")
    parser.add_argument("--write-aux-outputs", action="store_true", help="写出 duplicates/rejects/event_input 辅助文件")
    parser.add_argument("--force", action="store_true", help="强制重跑已完成的 batch")
    parser.add_argument("--export-only", action="store_true", help="兼容别名：等同 export 命令")
    parser.add_argument("--reset-state", action="store_true", help="兼容别名：等同 fresh 命令")


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
    if args.final_state_dir:
        path_updates["final_state_dir"] = Path(args.final_state_dir)
    if args.reports:
        path_updates["reports_dir"] = Path(args.reports)

    runtime_updates = {}
    for key in (
        "workers",
        "chunk_size",
        "part_size",
        "payload_part_bytes",
        "log_every_rows",
        "log_every_seconds",
    ):
        value = getattr(args, key)
        if value is not None:
            runtime_updates[key] = value
    if args.write_aux_outputs:
        runtime_updates["write_aux_outputs"] = True

    near_updates = {}
    if args.no_near_dedup:
        near_updates["enabled"] = False
    if args.near_min_body_chars is not None:
        near_updates["min_body_chars"] = args.near_min_body_chars
    if args.near_threshold is not None:
        near_updates["threshold"] = args.near_threshold
    if args.near_fuzzy_threshold is not None:
        near_updates["fuzzy_threshold"] = args.near_fuzzy_threshold

    config = PipelineConfig(
        paths=replace(base.paths, **path_updates) if path_updates else base.paths,
        runtime=replace(base.runtime, **runtime_updates) if runtime_updates else base.runtime,
        near_duplicates=replace(base.near_duplicates, **near_updates) if near_updates else base.near_duplicates,
    )
    return config


def run_cleaning_from_args(args: argparse.Namespace) -> None:
    from src.cleaning.pipeline import run_pipeline

    command = args.command
    run_pipeline(
        config_from_args(args),
        reset_state=args.reset_state or command == "fresh",
        export_only=args.export_only or command == "export",
        force=args.force,
    )


def main(argv: list[str] | None = None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    stage = argv[0] if argv else None

    if stage == "clean":
        args = build_clean_parser().parse_args(argv[1:])
        run_cleaning_from_args(args)
        return
    if stage == "extract":
        build_stage_parser("extract", "事件抽取阶段入口（当前为占位实现）").parse_args(argv[1:])

        from src.extraction.pipeline import run_pipeline as run_extraction_pipeline

        run_extraction_pipeline()
        return
    if stage == "complete":
        build_stage_parser("complete", "事件补全阶段入口（当前为占位实现）").parse_args(argv[1:])

        from src.completion.pipeline import run_pipeline as run_completion_pipeline

        run_completion_pipeline()
        return
    if stage == "run-all":
        from src.completion.pipeline import run_pipeline as run_completion_pipeline
        from src.extraction.pipeline import run_pipeline as run_extraction_pipeline

        args = build_clean_parser().parse_args(argv[1:])
        run_cleaning_from_args(args)
        run_extraction_pipeline()
        run_completion_pipeline()
        return

    args = build_parser().parse_args(argv)
    run_cleaning_from_args(args)


if __name__ == "__main__":
    main()
