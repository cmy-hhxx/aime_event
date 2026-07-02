from __future__ import annotations

import hashlib
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

import orjson

DEFAULT_PAYLOAD_PART_BYTES = 512 * 1024 * 1024


@dataclass(frozen=True)
class PayloadRef:
    path: str
    offset: int
    length: int
    sha256: str


class PayloadWriter:
    def __init__(self, payload_dir: Path, batch: str, part_bytes: int = DEFAULT_PAYLOAD_PART_BYTES):
        self.payload_dir = payload_dir
        self.batch_stem = Path(batch).stem
        self.part_bytes = part_bytes
        self.part_index = 0
        self.current_size = 0
        self.handle: BinaryIO | None = None
        self.current_path: Path | None = None
        payload_dir.mkdir(parents=True, exist_ok=True)

    def write(self, record: dict) -> PayloadRef:
        payload = orjson.dumps(record) + b"\n"
        if self.handle is None or self.current_size + len(payload) > self.part_bytes:
            self._open_next()
        assert self.handle is not None
        assert self.current_path is not None
        offset = self.current_size
        self.handle.write(payload)
        self.current_size += len(payload)
        return PayloadRef(
            path=self.current_path.name,
            offset=offset,
            length=len(payload),
            sha256=hashlib.sha256(payload).hexdigest(),
        )

    def close(self) -> None:
        if self.handle is not None:
            self.handle.close()
            self.handle = None

    def _open_next(self) -> None:
        if self.handle is not None:
            self.handle.close()
        self.current_path = self.payload_dir / f"{self.batch_stem}_part_{self.part_index:05d}.ndjson"
        self.handle = self.current_path.open("wb")
        self.current_size = 0
        self.part_index += 1


def reset_payload_dir(payload_dir: Path) -> None:
    if payload_dir.exists():
        shutil.rmtree(payload_dir)
    payload_dir.mkdir(parents=True, exist_ok=True)


def delete_payload_files(payload_dir: Path, payload_paths: list[str]) -> None:
    for payload_path in payload_paths:
        path = payload_dir / payload_path
        if path.exists():
            path.unlink()


def load_payload(payload_dir: Path, ref: PayloadRef) -> dict:
    path = payload_dir / ref.path
    with path.open("rb") as handle:
        handle.seek(ref.offset)
        payload = handle.read(ref.length)
    digest = hashlib.sha256(payload).hexdigest()
    if digest != ref.sha256:
        raise ValueError(f"payload checksum mismatch for {ref.path}@{ref.offset}")
    return orjson.loads(payload)


def directory_size(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())
