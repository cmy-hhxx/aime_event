from __future__ import annotations

"""临时脚本:把 data/export_2025-07-08 下混装的 cleaned_batch*.jsonl 按数据类型拆分。

两套导出的类型字段不同:
  - v1: 顶层 `content_type` (US_NEWS / US_ROBOT / US_ARTICLE / US_NOTICE / US_FLASH / US_POST)
  - v2: 顶层 `source_type` (notice / teleconference / report)

单遍流式:逐行读取原始 batch,按类型透传原始行到 by_type/<version>/<type>.jsonl。
非破坏性:不改动、不删除原始 batch 文件,只新建 by_type/ 目录。

用法:
    python scripts/split_export_by_type.py                 # 处理 v1 和 v2
    python scripts/split_export_by_type.py --version v2    # 只处理 v2
    python scripts/split_export_by_type.py --dry-run       # 只统计,不写文件
"""

import argparse
import sys
from collections import Counter
from pathlib import Path

import orjson

# 每个版本用于路由的类型字段
TYPE_FIELD = {
    "v1": "content_type",
    "v2": "source_type",
}

UNKNOWN = "_unknown"


def split_version(base: Path, version: str, dry_run: bool) -> None:
    field = TYPE_FIELD[version]
    src_dir = base / version
    out_dir = base / "by_type" / version
    if not src_dir.is_dir():
        print(f"[{version}] 跳过:目录不存在 {src_dir}")
        return

    batches = sorted(src_dir.glob("cleaned_batch*.jsonl"))
    if not batches:
        print(f"[{version}] 跳过:未找到 cleaned_batch*.jsonl")
        return

    if not dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)

    handles: dict[str, object] = {}
    counts: Counter = Counter()
    blank = 0
    bad = 0

    print(f"[{version}] 开始:{len(batches)} 个文件,类型字段=`{field}`,输出→ {out_dir}")
    try:
        for i, batch in enumerate(batches, 1):
            with batch.open("rb") as fh:
                for raw in fh:
                    if not raw.strip():
                        blank += 1
                        continue
                    try:
                        obj = orjson.loads(raw)
                    except orjson.JSONDecodeError:
                        bad += 1
                        continue
                    kind = obj.get(field) or UNKNOWN
                    kind = str(kind)
                    counts[kind] += 1
                    if dry_run:
                        continue
                    out = handles.get(kind)
                    if out is None:
                        out = (out_dir / f"{kind}.jsonl").open("wb")
                        handles[kind] = out
                    # 透传原始行(确保以换行结尾),不重新序列化
                    out.write(raw if raw.endswith(b"\n") else raw + b"\n")
            if i % 25 == 0 or i == len(batches):
                print(f"[{version}]   已处理 {i}/{len(batches)} 个文件")
    finally:
        for out in handles.values():
            out.close()

    total = sum(counts.values())
    print(f"[{version}] 完成:共 {total} 条  (空行 {blank},解析失败 {bad})")
    for kind, n in counts.most_common():
        print(f"[{version}]   {kind:<16} {n:>12,}")
    print()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="按类型拆分导出数据")
    parser.add_argument(
        "--base",
        type=Path,
        default=Path("data/export_2025-07-08"),
        help="导出根目录 (默认 data/export_2025-07-08)",
    )
    parser.add_argument(
        "--version",
        choices=["v1", "v2"],
        action="append",
        help="只处理指定版本,可重复;缺省处理全部",
    )
    parser.add_argument("--dry-run", action="store_true", help="只统计不写文件")
    args = parser.parse_args(argv)

    versions = args.version or ["v1", "v2"]
    for version in versions:
        split_version(args.base, version, args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
