from __future__ import annotations

from typing import Any

from src.cleaning.output.cleaning import prune_public_record

def build_cleaned_record(record: dict[str, Any]) -> dict[str, Any]:
    return prune_public_record(record)
