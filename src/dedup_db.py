from __future__ import annotations

import json
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Optional

import orjson

from src.dedup import compute_dedup_key

SCHEMA_VERSION = "2"


class StateVersionError(RuntimeError):
    pass


RecordRow = tuple[str, int, str, str, str, str, str, int, str, str]
RejectRow = tuple[str, int, Optional[str], str, str, str]


class StagingDB:
    def __init__(self, db_path: Path, reset: bool = False):
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        if reset:
            self._delete_db_files()
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._ensure_schema()

    def _delete_db_files(self) -> None:
        for suffix in ("", "-wal", "-shm"):
            path = Path(f"{self.db_path}{suffix}")
            if path.exists():
                path.unlink()

    def _table_exists(self, table_name: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        ).fetchone()
        return row is not None

    def _ensure_schema(self) -> None:
        if self._table_exists("dedup_index") and not self._table_exists("metadata"):
            raise StateVersionError(
                f"{self.db_path} uses the old dedup schema. Re-run with --reset-state to rebuild staging state."
            )

        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS records (
                batch TEXT NOT NULL,
                line_no INTEGER NOT NULL,
                id TEXT NOT NULL,
                content_type TEXT NOT NULL,
                published_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                record_json BLOB NOT NULL,
                body_len INTEGER NOT NULL,
                dedup_key TEXT NOT NULL,
                dedup_method TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (batch, line_no)
            );

            CREATE TABLE IF NOT EXISTS rejects (
                batch TEXT NOT NULL,
                line_no INTEGER NOT NULL,
                raw_id TEXT,
                reason TEXT NOT NULL,
                message TEXT NOT NULL,
                raw_line TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (batch, line_no)
            );

            CREATE TABLE IF NOT EXISTS batch_state (
                batch TEXT PRIMARY KEY,
                path TEXT NOT NULL,
                status TEXT NOT NULL,
                input_count INTEGER NOT NULL DEFAULT 0,
                accepted_count INTEGER NOT NULL DEFAULT 0,
                rejected_count INTEGER NOT NULL DEFAULT 0,
                started_at TEXT DEFAULT CURRENT_TIMESTAMP,
                completed_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_records_id ON records(id);
            CREATE INDEX IF NOT EXISTS idx_records_dedup ON records(dedup_key);
            CREATE INDEX IF NOT EXISTS idx_records_batch ON records(batch);
            CREATE INDEX IF NOT EXISTS idx_rejects_batch ON rejects(batch);
            """
        )

        version = self.conn.execute("SELECT value FROM metadata WHERE key = 'schema_version'").fetchone()
        if version is None:
            self.conn.execute(
                "INSERT INTO metadata (key, value) VALUES ('schema_version', ?)",
                (SCHEMA_VERSION,),
            )
        elif version["value"] != SCHEMA_VERSION:
            raise StateVersionError(
                f"{self.db_path} has schema_version={version['value']}; expected {SCHEMA_VERSION}. "
                "Re-run with --reset-state to rebuild staging state."
            )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def is_batch_complete(self, batch: str) -> bool:
        row = self.conn.execute(
            "SELECT status FROM batch_state WHERE batch = ?",
            (batch,),
        ).fetchone()
        return bool(row and row["status"] == "complete")

    def prepare_batch(self, batch: str, path: Path, force: bool = False) -> bool:
        if self.is_batch_complete(batch) and not force:
            return False

        with self.conn:
            self.conn.execute("DELETE FROM records WHERE batch = ?", (batch,))
            self.conn.execute("DELETE FROM rejects WHERE batch = ?", (batch,))
            self.conn.execute("DELETE FROM batch_state WHERE batch = ?", (batch,))
            self.conn.execute(
                """INSERT INTO batch_state (batch, path, status, input_count, accepted_count, rejected_count)
                   VALUES (?, ?, 'running', 0, 0, 0)""",
                (batch, str(path)),
            )
        return True

    def add_chunk(
        self,
        batch: str,
        records: Iterable[RecordRow],
        rejects: Iterable[RejectRow],
        input_count: int,
    ) -> None:
        record_rows = list(records)
        reject_rows = list(rejects)
        with self.conn:
            if record_rows:
                self.conn.executemany(
                    """INSERT INTO records (
                           batch, line_no, id, content_type, published_at, updated_at,
                           record_json, body_len, dedup_key, dedup_method
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    record_rows,
                )
            if reject_rows:
                self.conn.executemany(
                    """INSERT INTO rejects (batch, line_no, raw_id, reason, message, raw_line)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    reject_rows,
                )
            self.conn.execute(
                """UPDATE batch_state
                   SET input_count = input_count + ?,
                       accepted_count = accepted_count + ?,
                       rejected_count = rejected_count + ?
                   WHERE batch = ?""",
                (input_count, len(record_rows), len(reject_rows), batch),
            )

    def complete_batch(self, batch: str) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE batch_state SET status = 'complete', completed_at = CURRENT_TIMESTAMP WHERE batch = ?",
                (batch,),
            )

    def completed_batches(self) -> list[str]:
        rows = self.conn.execute(
            "SELECT batch FROM batch_state WHERE status = 'complete' ORDER BY batch"
        ).fetchall()
        return [str(row["batch"]) for row in rows]

    def insert_record_rows(self, batch: str, line_no: int, record: dict[str, Any]) -> RecordRow:
        dedup_key, dedup_method = compute_dedup_key(record)
        return (
            batch,
            line_no,
            str(record["id"]),
            str(record["content_type"]),
            str(record["published_at"]),
            str(record["updated_at"]),
            orjson.dumps(record).decode(),
            int(record.get("_body_len", len(str(record.get("body") or "")))),
            dedup_key,
            dedup_method,
        )

    def canonical_rows(self) -> Iterable[sqlite3.Row]:
        return self.conn.execute(
            """
            WITH id_ranked AS (
                SELECT
                    records.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY id
                        ORDER BY body_len DESC, published_at DESC, id ASC, batch ASC, line_no ASC
                    ) AS id_rank
                FROM records
            ),
            id_winners AS (
                SELECT * FROM id_ranked WHERE id_rank = 1
            ),
            dedup_ranked AS (
                SELECT
                    id_winners.*,
                    FIRST_VALUE(id) OVER (
                        PARTITION BY dedup_key
                        ORDER BY body_len DESC, published_at DESC, id ASC, batch ASC, line_no ASC
                    ) AS canonical_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY dedup_key
                        ORDER BY body_len DESC, published_at DESC, id ASC, batch ASC, line_no ASC
                    ) AS dedup_rank
                FROM id_winners
            )
            SELECT record_json, dedup_key, dedup_method, id, canonical_id
            FROM dedup_ranked
            WHERE dedup_rank = 1
            ORDER BY published_at DESC, id ASC, batch ASC, line_no ASC
            """
        )

    def duplicate_rows(self) -> Iterable[sqlite3.Row]:
        return self.conn.execute(
            """
            WITH id_ranked AS (
                SELECT
                    records.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY id
                        ORDER BY body_len DESC, published_at DESC, id ASC, batch ASC, line_no ASC
                    ) AS id_rank
                FROM records
            ),
            id_winners AS (
                SELECT * FROM id_ranked WHERE id_rank = 1
            ),
            dedup_ranked AS (
                SELECT
                    id_winners.*,
                    FIRST_VALUE(id) OVER (
                        PARTITION BY dedup_key
                        ORDER BY body_len DESC, published_at DESC, id ASC, batch ASC, line_no ASC
                    ) AS canonical_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY dedup_key
                        ORDER BY body_len DESC, published_at DESC, id ASC, batch ASC, line_no ASC
                    ) AS dedup_rank
                FROM id_winners
            ),
            final_by_id AS (
                SELECT id, canonical_id FROM dedup_ranked
            ),
            content_duplicates AS (
                SELECT record_json, dedup_key, dedup_method, id, canonical_id, published_at, batch, line_no
                FROM dedup_ranked
                WHERE dedup_rank > 1
            ),
            id_duplicates AS (
                SELECT
                    id_ranked.record_json,
                    'id:' || id_ranked.id AS dedup_key,
                    'id' AS dedup_method,
                    id_ranked.id,
                    final_by_id.canonical_id,
                    id_ranked.published_at,
                    id_ranked.batch,
                    id_ranked.line_no
                FROM id_ranked
                JOIN final_by_id ON final_by_id.id = id_ranked.id
                WHERE id_ranked.id_rank > 1
            )
            SELECT record_json, dedup_key, dedup_method, id, canonical_id
            FROM (
                SELECT * FROM content_duplicates
                UNION ALL
                SELECT * FROM id_duplicates
            )
            ORDER BY published_at DESC, id ASC, batch ASC, line_no ASC
            """
        )

    def reject_rows(self) -> Iterable[sqlite3.Row]:
        return self.conn.execute(
            """SELECT batch, line_no, raw_id, reason, message, raw_line
               FROM rejects
               ORDER BY batch ASC, line_no ASC"""
        )

    def write_reports(self, reports_dir: Path, progress_path: Path) -> dict[str, Any]:
        reports_dir.mkdir(parents=True, exist_ok=True)
        completed = self.completed_batches()
        progress_path.parent.mkdir(parents=True, exist_ok=True)
        progress_path.write_text(json.dumps({"completed": completed}, indent=2) + "\n")

        with (reports_dir / "batch_stats.jsonl").open("w", encoding="utf-8") as handle:
            for row in self.conn.execute("SELECT * FROM batch_state ORDER BY batch"):
                stats = {
                    "batch": row["batch"],
                    "status": row["status"],
                    "input": row["input_count"],
                    "accepted": row["accepted_count"],
                    "rejected": row["rejected_count"],
                }
                handle.write(json.dumps(stats, sort_keys=True) + "\n")

        summary = self.summary()
        (reports_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        return summary

    def summary(self) -> dict[str, Any]:
        accepted = _single_int(self.conn, "SELECT COUNT(*) FROM records")
        rejected = _single_int(self.conn, "SELECT COUNT(*) FROM rejects")
        total_input = _single_int(self.conn, "SELECT COALESCE(SUM(input_count), 0) FROM batch_state")
        cleaned = _single_int(
            self.conn,
            """
            WITH id_ranked AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY id
                    ORDER BY body_len DESC, published_at DESC, id ASC, batch ASC, line_no ASC
                ) AS id_rank
                FROM records
            ),
            id_winners AS (
                SELECT * FROM id_ranked WHERE id_rank = 1
            ),
            dedup_ranked AS (
                SELECT ROW_NUMBER() OVER (
                    PARTITION BY dedup_key
                    ORDER BY body_len DESC, published_at DESC, id ASC, batch ASC, line_no ASC
                ) AS dedup_rank
                FROM id_winners
            )
            SELECT COUNT(*) FROM dedup_ranked WHERE dedup_rank = 1
            """,
        )

        canonical_type_counts = self._canonical_group_counts("content_type")
        canonical_method_counts = self._canonical_group_counts("dedup_method")
        duplicate_method_counts = self._duplicate_method_counts()
        reject_reason_counts = _group_counts(self.conn, "SELECT reason, COUNT(*) FROM rejects GROUP BY reason")

        return {
            "total_input": total_input,
            "total_accepted": accepted,
            "total_cleaned": cleaned,
            "total_duplicates": accepted - cleaned,
            "total_rejected": rejected,
            "batches_completed": len(self.completed_batches()),
            "by_content_type": canonical_type_counts,
            "dedup_by_method": canonical_method_counts,
            "duplicate_by_method": duplicate_method_counts,
            "rejects_by_reason": reject_reason_counts,
        }

    def _canonical_group_counts(self, column: str) -> dict[str, int]:
        if column not in {"content_type", "dedup_method"}:
            raise ValueError(f"unsupported group column: {column}")
        return _group_counts(
            self.conn,
            f"""
            WITH id_ranked AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY id
                    ORDER BY body_len DESC, published_at DESC, id ASC, batch ASC, line_no ASC
                ) AS id_rank
                FROM records
            ),
            id_winners AS (
                SELECT * FROM id_ranked WHERE id_rank = 1
            ),
            dedup_ranked AS (
                SELECT {column}, ROW_NUMBER() OVER (
                    PARTITION BY dedup_key
                    ORDER BY body_len DESC, published_at DESC, id ASC, batch ASC, line_no ASC
                ) AS dedup_rank
                FROM id_winners
            )
            SELECT {column}, COUNT(*) FROM dedup_ranked WHERE dedup_rank = 1 GROUP BY {column}
            """,
        )

    def _duplicate_method_counts(self) -> dict[str, int]:
        rows = self.conn.execute(
            """
            WITH id_ranked AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY id
                    ORDER BY body_len DESC, published_at DESC, id ASC, batch ASC, line_no ASC
                ) AS id_rank
                FROM records
            ),
            id_winners AS (
                SELECT * FROM id_ranked WHERE id_rank = 1
            ),
            dedup_ranked AS (
                SELECT dedup_method, ROW_NUMBER() OVER (
                    PARTITION BY dedup_key
                    ORDER BY body_len DESC, published_at DESC, id ASC, batch ASC, line_no ASC
                ) AS dedup_rank
                FROM id_winners
            ),
            methods AS (
                SELECT dedup_method FROM dedup_ranked WHERE dedup_rank > 1
                UNION ALL
                SELECT 'id' AS dedup_method FROM id_ranked WHERE id_rank > 1
            )
            SELECT dedup_method, COUNT(*) FROM methods GROUP BY dedup_method
            """
        ).fetchall()
        return {str(row[0]): int(row[1]) for row in rows}


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
