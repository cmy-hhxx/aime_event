from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class CompletedEvent:
    event_id: str
    fields: dict[str, Any] = field(default_factory=dict)
