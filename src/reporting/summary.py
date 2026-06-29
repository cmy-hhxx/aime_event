from __future__ import annotations

import sqlite3
from collections import Counter
from pathlib import Path

from src.storage.payload import directory_size
from src.storage.staging import StagingDB


def _single_int(conn: sqlite3.Connection, query: str) -> int:
    row = conn.execute(query).fetchone()
    return int(row[0]) if row else 0


def _group_counts(conn: sqlite3.Connection, query: str) -> dict[str, int]:
    rows = conn.execute(query).fetchall()
    counts: Counter[str] = Counter()
    for row in rows:
        if row[0] is not None:
            counts[str(row[0])] = int(row[1])
    return dict(counts)


def build_storage_summary(
    db: StagingDB,
    total_input: int,
    cleaned_dir: Path,
    dup_dir: Path,
    reject_dir: Path,
    event_dir: Path,
) -> dict[str, int]:
    db.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    db_bytes = sum(
        Path(f"{db.db_path}{suffix}").stat().st_size
        for suffix in ("", "-wal", "-shm")
        if Path(f"{db.db_path}{suffix}").exists()
    )
    payload_bytes = directory_size(db.payload_dir)
    cleaned_bytes = directory_size(cleaned_dir)
    duplicates_bytes = directory_size(dup_dir)
    rejects_bytes = directory_size(reject_dir)
    event_bytes = directory_size(event_dir)
    total_bytes = db_bytes + payload_bytes + cleaned_bytes + duplicates_bytes + rejects_bytes + event_bytes
    estimated = int(total_bytes / total_input * db.target_scale_rows) if total_input else 0
    return {
        "db_bytes": db_bytes,
        "payload_bytes": payload_bytes,
        "cleaned_bytes": cleaned_bytes,
        "duplicates_bytes": duplicates_bytes,
        "rejects_bytes": rejects_bytes,
        "event_input_bytes": event_bytes,
        "total_pipeline_bytes": total_bytes,
        "estimated_20m_rows_bytes": estimated,
    }


def build_summary(
    db: StagingDB,
    cleaned_dir: Path,
    dup_dir: Path,
    reject_dir: Path,
    event_dir: Path,
) -> dict[str, int | dict[str, int]]:
    db.build_winner_tables()
    conn = db.conn
    accepted = _single_int(conn, "SELECT COUNT(*) FROM records")
    rejected = _single_int(conn, "SELECT COUNT(*) FROM rejects")
    total_input = _single_int(conn, "SELECT COALESCE(SUM(input_count), 0) FROM batch_state")
    cleaned = _single_int(
        conn,
        "SELECT COUNT(*) FROM dedup_winners WHERE id NOT IN (SELECT loser_id FROM near_duplicate_losers)",
    )
    duplicates = _single_int(conn, "SELECT COUNT(*) FROM duplicate_records")

    return {
        "total_input": total_input,
        "total_accepted": accepted,
        "total_cleaned": cleaned,
        "total_duplicates": duplicates,
        "total_rejected": rejected,
        "batches_completed": len(db.completed_batches()),
        "by_content_type": _group_counts(
            conn,
            """SELECT content_type, COUNT(*) FROM dedup_winners
               WHERE id NOT IN (SELECT loser_id FROM near_duplicate_losers)
               GROUP BY content_type""",
        ),
        "dedup_by_method": _group_counts(
            conn,
            """SELECT dedup_method, COUNT(*) FROM dedup_winners
               WHERE id NOT IN (SELECT loser_id FROM near_duplicate_losers)
               GROUP BY dedup_method""",
        ),
        "duplicate_by_method": _group_counts(
            conn,
            "SELECT dedup_method, COUNT(*) FROM duplicate_records GROUP BY dedup_method",
        ),
        "rejects_by_reason": _group_counts(conn, "SELECT reason, COUNT(*) FROM rejects GROUP BY reason"),
        "near_duplicate_candidates": _single_int(conn, "SELECT COUNT(*) FROM near_candidate_pairs"),
        "near_duplicates_auto_merged": _single_int(conn, "SELECT COUNT(*) FROM near_duplicate_losers"),
        "near_duplicates_report_only": _single_int(
            conn,
            "SELECT COUNT(*) FROM near_candidate_pairs WHERE status = 'report_only'",
        ),
        "storage": build_storage_summary(db, total_input, cleaned_dir, dup_dir, reject_dir, event_dir),
    }
