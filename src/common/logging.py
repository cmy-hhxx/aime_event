from __future__ import annotations

import time


def log(message: str) -> None:
    print(message, flush=True)


class ProgressLogger:
    def __init__(self, label: str, every_rows: int, every_seconds: int):
        self.label = label
        self.every_rows = every_rows
        self.every_seconds = every_seconds
        self.started_at = time.monotonic()
        self.last_logged_at = self.started_at
        self.next_rows = every_rows

    def maybe(self, rows: int, details: str = "") -> None:
        now = time.monotonic()
        if rows < self.next_rows and now - self.last_logged_at < self.every_seconds:
            return
        elapsed = max(now - self.started_at, 0.001)
        rate = rows / elapsed
        suffix = f" {details}" if details else ""
        log(f"{self.label}: rows={rows:,} elapsed={elapsed:.1f}s rate={rate:,.0f}/s{suffix}")
        self.last_logged_at = now
        while rows >= self.next_rows:
            self.next_rows += self.every_rows
