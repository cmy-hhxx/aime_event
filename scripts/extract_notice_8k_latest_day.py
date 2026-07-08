from __future__ import annotations

"""从 US_NOTICE.jsonl 抽取"最新一天"的 8-K 记录。

8-K 判定:notice.attachments 里存在 file_type 精确等于 "8-K" 的附件
(不含 8-K/A 修正案)。最新一天 = 8-K 记录自身取到的最大自然日
(按顶层 published_at),保证输出非空。

记录未按时间排序,故两遍流式:第一遍扫出 8-K 的最大日,第二遍透传该日的
8-K 原始行。非破坏性:只读源、新建输出,内存占用与文件大小无关。

用法:
    # 默认抽 data/export_2025-07-08/v1/US_NOTICE.jsonl 最新一天的 8-K,
    # 输出到同目录 US_NOTICE.8k.latest_day.jsonl
    python scripts/extract_notice_8k_latest_day.py

    python scripts/extract_notice_8k_latest_day.py <src.jsonl> -o <out.jsonl>
    python scripts/extract_notice_8k_latest_day.py --dry-run   # 只报最新一天与条数
"""

import argparse
import sys
from pathlib import Path

import orjson

DEFAULT_SRC = Path("data/export_2025-07-08/v1/US_NOTICE.jsonl")


def is_8k(record: dict) -> bool:
    """附件 file_type 精确等于 8-K(排除 8-K/A 等修正案)。"""
    attachments = (record.get("notice") or {}).get("attachments") or []
    return any(a.get("file_type") == "8-K" for a in attachments)


def day_of(record: dict) -> str:
    """published_at 截到自然日(YYYY-MM-DD);缺失/异常返回空串。"""
    value = record.get("published_at")
    return value[:10] if isinstance(value, str) else ""


def find_latest_8k_day(src: Path) -> tuple[str, int]:
    """第一遍:扫全文件,返回(8-K 的最大自然日, 该日 8-K 条数)。"""
    latest = ""
    count = 0
    with src.open("rb") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                record = orjson.loads(line)
            except orjson.JSONDecodeError:
                continue
            if not is_8k(record):
                continue
            day = day_of(record)
            if not day:
                continue
            if day > latest:
                latest, count = day, 1
            elif day == latest:
                count += 1
    return latest, count


def write_8k_day(src: Path, out: Path, day: str) -> int:
    """第二遍:把 `day` 当天的 8-K 原始行逐字透传到 out,返回写出条数。"""
    written = 0
    with src.open("rb") as fin, out.open("wb") as fout:
        for line in fin:
            if not line.strip():
                continue
            try:
                record = orjson.loads(line)
            except orjson.JSONDecodeError:
                continue
            if is_8k(record) and day_of(record) == day:
                fout.write(line if line.endswith(b"\n") else line + b"\n")
                written += 1
    return written


def main() -> None:
    ap = argparse.ArgumentParser(description="抽取 US_NOTICE 最新一天的 8-K 记录")
    ap.add_argument("src", type=Path, nargs="?", default=DEFAULT_SRC,
                    help=f"源 jsonl(默认 {DEFAULT_SRC})")
    ap.add_argument("-o", "--out", type=Path, default=None,
                    help="输出 jsonl,默认 <src 同目录>/<stem>.8k.latest_day.jsonl")
    ap.add_argument("--dry-run", action="store_true", help="只报最新一天与条数,不写文件")
    a = ap.parse_args()

    if not a.src.is_file():
        sys.exit(f"源文件不存在:{a.src}")

    print(f"[1/2] 扫描 {a.src} 的最新一天 8-K…", flush=True)
    latest, count = find_latest_8k_day(a.src)
    if not latest:
        sys.exit("未在文件中找到任何 8-K 记录")
    print(f"      最新一天 = {latest},预计 {count:,} 条 8-K", flush=True)

    if a.dry_run:
        return

    out = a.out or a.src.with_name(f"{a.src.stem}.8k.latest_day.jsonl")
    out.parent.mkdir(parents=True, exist_ok=True)
    print(f"[2/2] 写出到 {out}…", flush=True)
    written = write_8k_day(a.src, out, latest)
    print(f"完成:{written:,} 条 8-K -> {out}", flush=True)


if __name__ == "__main__":
    main()
