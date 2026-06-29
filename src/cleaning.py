from __future__ import annotations

from typing import Any


def prune_empty_fields(value: Any) -> Any:
    """Remove None and empty optional containers from public output records."""
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, list):
        items = [prune_empty_fields(item) for item in value]
        compact_items = [item for item in items if item is not None]
        return compact_items or None
    if isinstance(value, dict):
        compact: dict[str, Any] = {}
        for key, item in value.items():
            pruned = prune_empty_fields(item)
            if pruned is not None:
                compact[key] = pruned
        return compact or None
    return value


def prune_public_record(record: dict[str, Any]) -> dict[str, Any]:
    pruned = prune_empty_fields(record)
    return pruned if isinstance(pruned, dict) else {}
