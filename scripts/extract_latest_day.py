from __future__ import annotations

"""从一个 jsonl 抽取"最新一天"的全部记录。

最新一天 = 文件内某日期字段(默认顶层 `published_at`)取到的最大自然日。
记录未按时间排序,故两遍流式:第一遍扫出最大日,第二遍透传该日原始行。
非破坏性:只读源文件、新建输出文件,不改动源文件。内存占用与文件大小无关。

用法:
    # 默认:抽 data/export_2025-07-08/v1/US_NOTICE.jsonl 的最新一天,
    # 输出到同目录 US_NOTICE.latest_day.jsonl
    python scripts/extract_latest_day.py data/export_2025-07-08/v1/US_NOTICE.jsonl

    python scripts/extract_latest_day.py <src.jsonl> -o <out.jsonl>
    python scripts/extract_latest_day.py <src.jsonl> --field declare_date  # 换日期字段
    python scripts/extract_latest_day.py <src.jsonl> --dry-run             # 只报最新一天与条数
"""

import argparse
import sys
from pathlib import Path

import orjson


def _day_of(record: dict, field: str) -> str:
    """取记录的日期字段并截到自然日(YYYY-MM-DD);缺失/异常返回空串。"""
    value = record.get(field)
    if not isinstance(value, str):
        return ""
    return value[:10]


def find_latest_day(src: Path, field: str) -> tuple[str, int]:
    """第一遍:扫全文件,返回(最大自然日, 该日记录数)。"""
    latest = ""
    count = 0
    with src.open("rb") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                day = _day_of(orjson.loads(line), field)
            except orjson.JSONDecodeError:
                continue
            if not day:
                continue
            if day > latest:
                latest, count = day, 1
            elif day == latest:
                count += 1
    return latest, count


def write_day(src: Path, out: Path, field: str, day: str) -> int:
    """第二遍:把 `day` 当天的原始行逐字透传到 out,返回写出条数。"""
    written = 0
    with src.open("rb") as fin, out.open("wb") as fout:
        for line in fin:
            if not line.strip():
                continue
            try:
                if _day_of(orjson.loads(line), field) == day:
                    fout.write(line if line.endswith(b"\n") else line + b"\n")
                    written += 1
            except orjson.JSONDecodeError:
                continue
    return written


def main() -> None:
    ap = argparse.ArgumentParser(description="抽取 jsonl 的最新一天记录")
    ap.add_argument("src", type=Path, help="源 jsonl,如 data/export_2025-07-08/v1/US_NOTICE.jsonl")
    ap.add_argument("-o", "--out", type=Path, default=None,
                    help="输出 jsonl,默认 <src 同目录>/<stem>.latest_day.jsonl")
    ap.add_argument("--field", default="published_at", help="用于定日的字段(默认 published_at)")
    ap.add_argument("--dry-run", action="store_true", help="只报最新一天与条数,不写文件")
    a = ap.parse_args()

    if not a.src.is_file():
        sys.exit(f"源文件不存在:{a.src}")

    print(f"[1/2] 扫描 {a.src} 的最新一天(字段 {a.field})…", flush=True)
    latest, count = find_latest_day(a.src, a.field)
    if not latest:
        sys.exit(f"未在字段 {a.field} 找到任何有效日期")
    print(f"      最新一天 = {latest},预计 {count:,} 条", flush=True)

    if a.dry_run:
        return

    out = a.out or a.src.with_name(f"{a.src.stem}.latest_day.jsonl")
    out.parent.mkdir(parents=True, exist_ok=True)
    print(f"[2/2] 写出到 {out}…", flush=True)
    written = write_day(a.src, out, a.field, latest)
    print(f"完成:{written:,} 条 -> {out}", flush=True)


if __name__ == "__main__":
    main()
