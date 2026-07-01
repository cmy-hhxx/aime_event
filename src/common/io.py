from __future__ import annotations

from pathlib import Path
from typing import Any, BinaryIO

import orjson


class PartWriter:
    def __init__(
        self,
        directory: Path,
        prefix: str,
        part_size: int,
        *,
        filename_template: str = "{prefix}_part_{index:05d}.jsonl",
        start_index: int = 0,
    ):
        self.directory = directory
        self.prefix = prefix
        self.part_size = part_size
        self.filename_template = filename_template
        self.count = 0
        self.part_index = start_index
        self.handle: BinaryIO | None = None
        directory.mkdir(parents=True, exist_ok=True)

    def write(self, payload: dict[str, Any]) -> None:
        if self.handle is None or self.count % self.part_size == 0:
            self._open_next()
        assert self.handle is not None
        self.handle.write(orjson.dumps(payload) + b"\n")
        self.count += 1

    def close(self) -> None:
        if self.handle is not None:
            self.handle.close()
            self.handle = None

    def _open_next(self) -> None:
        if self.handle is not None:
            self.handle.close()
        path = self.directory / self.filename_template.format(prefix=self.prefix, index=self.part_index)
        self.handle = path.open("wb")
        self.part_index += 1
