from __future__ import annotations

import re
import shutil
from pathlib import Path


def discover_content_batches(input_dir: Path) -> list[Path]:
    return sorted(input_dir.glob("content_batch_*.ndjson"), key=_batch_sort_key)


def tmp_output_dir(final_dir: Path) -> Path:
    final_dir.parent.mkdir(parents=True, exist_ok=True)
    tmp = final_dir.parent / f".{final_dir.name}.tmp"
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True)
    return tmp


def replace_output_dir(tmp_dir: Path, final_dir: Path) -> None:
    backup = final_dir.parent / f".{final_dir.name}.bak"
    if backup.exists():
        shutil.rmtree(backup)
    if final_dir.exists():
        final_dir.rename(backup)
    tmp_dir.rename(final_dir)
    if backup.exists():
        shutil.rmtree(backup)


def _batch_sort_key(path: Path) -> tuple[int, int | str]:
    match = re.search(r"content_batch_(\d+)\.ndjson$", path.name)
    if match:
        return (0, int(match.group(1)))
    return (1, path.name)
