from __future__ import annotations

import json
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Optional

from src.dedup import DEDUP_VERSION, compute_dedup_key, normalize_text
from src.payload_store import PayloadRef, delete_payload_files, directory_size, load_payload, reset_payload_dir

SCHEMA_VERSION = "3"
TARGET_SCALE_ROWS = 20_000_000


class StateVersionError(RuntimeError):
    pass


RecordRow = tuple[
    str,
    int,
    str,
    str,
    str,
    str,
    int,
    str,
    str,
    str,
    str,
    str,
    int,
    int,
    str,
]
RejectRow = tuple[str, int, Optional[str], str, str, str]


class StagingDB:
    def __init__(self, db_path: Path, payload_dir: Path, reset: bool = False):
        self.db_path = db_path
        self.payload_dir = payload_dir
        self._winners_valid = False
        db_path.parent.mkdir(parents=True, exist_ok=True)
        if reset:
            self._delete_db_files()
            reset_payload_dir(payload_dir)
        else:
            payload_dir.mkdir(parents=True, exist_ok=True)
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

        if self._table_exists("metadata"):
            version = self.conn.execute("SELECT value FROM metadata WHERE key = 'schema_version'").fetchone()
            if version is not None and version["value"] != SCHEMA_VERSION:
                raise StateVersionError(
                    f"{self.db_path} has schema_version={version['value']}; expected {SCHEMA_VERSION}. "
                    "Re-run with --reset-state to rebuild staging state."
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
                body_len INTEGER NOT NULL,
                title_norm TEXT NOT NULL,
                dedup_key TEXT NOT NULL,
                dedup_method TEXT NOT NULL,
                dedup_debug TEXT NOT NULL,
                payload_path TEXT NOT NULL,
                payload_offset INTEGER NOT NULL,
                payload_length INTEGER NOT NULL,
                payload_sha256 TEXT NOT NULL,
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
            CREATE INDEX IF NOT EXISTS idx_records_title_norm ON records(title_norm);
            CREATE INDEX IF NOT EXISTS idx_rejects_batch ON rejects(batch);
            """
        )

        version = self.conn.execute("SELECT value FROM metadata WHERE key = 'schema_version'").fetchone()
        if version is None:
            self.conn.execute(
                "INSERT INTO metadata (key, value) VALUES ('schema_version', ?)",
                (SCHEMA_VERSION,),
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

        rows = self.conn.execute(
            "SELECT DISTINCT payload_path FROM records WHERE batch = ?",
            (batch,),
        ).fetchall()
        delete_payload_files(self.payload_dir, [str(row["payload_path"]) for row in rows])

        with self.conn:
            self.conn.execute("DELETE FROM records WHERE batch = ?", (batch,))
            self.conn.execute("DELETE FROM rejects WHERE batch = ?", (batch,))
            self.conn.execute("DELETE FROM batch_state WHERE batch = ?", (batch,))
            self.conn.execute(
                """INSERT INTO batch_state (batch, path, status, input_count, accepted_count, rejected_count)
                   VALUES (?, ?, 'running', 0, 0, 0)""",
                (batch, str(path)),
            )
        self._winners_valid = False
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
                           body_len, title_norm, dedup_key, dedup_method, dedup_debug,
                           payload_path, payload_offset, payload_length, payload_sha256
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
        self._winners_valid = False

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

    def insert_record_rows(
        self,
        batch: str,
        line_no: int,
        record: dict[str, Any],
        payload_ref: PayloadRef,
    ) -> RecordRow:
        dedup = compute_dedup_key(record)
        return (
            batch,
            line_no,
            str(record["id"]),
            str(record["content_type"]),
            str(record["published_at"]),
            str(record["updated_at"]),
            int(record.get("_body_len", len(str(record.get("body") or "")))),
            normalize_text(record.get("title")),
            dedup.key,
            dedup.method,
            json.dumps(dedup.debug, sort_keys=True),
            payload_ref.path,
            payload_ref.offset,
            payload_ref.length,
            payload_ref.sha256,
        )

    def build_winner_tables(self) -> None:
        if self._winners_valid:
            return
        with self.conn:
            self.conn.executescript(
                f"""
                DROP TABLE IF EXISTS id_winners;
                DROP TABLE IF EXISTS dedup_winners;
                DROP TABLE IF EXISTS duplicate_records;

                CREATE TABLE id_winners AS
                WITH id_ranked AS (
                    SELECT
                        records.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY id
                            ORDER BY body_len DESC, published_at DESC, id ASC, batch ASC, line_no ASC
                        ) AS id_rank
                    FROM records
                )
                SELECT * FROM id_ranked WHERE id_rank = 1;

                CREATE INDEX idx_id_winners_id ON id_winners(id);
                CREATE INDEX idx_id_winners_dedup ON id_winners(dedup_key);
                CREATE INDEX idx_id_winners_title_norm ON id_winners(title_norm);

                CREATE TABLE dedup_winners AS
                WITH dedup_ranked AS (
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
                SELECT * FROM dedup_ranked WHERE dedup_rank = 1;

                CREATE INDEX idx_dedup_winners_id ON dedup_winners(id);
                CREATE INDEX idx_dedup_winners_title_norm ON dedup_winners(title_norm);

                CREATE TABLE duplicate_records AS
                WITH id_ranked AS (
                    SELECT
                        records.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY id
                            ORDER BY body_len DESC, published_at DESC, id ASC, batch ASC, line_no ASC
                        ) AS id_rank
                    FROM records
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
                    SELECT
                        batch, line_no, id, content_type, published_at, updated_at, body_len,
                        title_norm, dedup_key, dedup_method, dedup_debug, payload_path,
                        payload_offset, payload_length, payload_sha256, canonical_id
                    FROM dedup_ranked
                    WHERE dedup_rank > 1
                ),
                id_duplicates AS (
                    SELECT
                        id_ranked.batch,
                        id_ranked.line_no,
                        id_ranked.id,
                        id_ranked.content_type,
                        id_ranked.published_at,
                        id_ranked.updated_at,
                        id_ranked.body_len,
                        id_ranked.title_norm,
                        'id:' || id_ranked.id AS dedup_key,
                        'id' AS dedup_method,
                        '{{"version":{DEDUP_VERSION},"source":"id_duplicate"}}' AS dedup_debug,
                        id_ranked.payload_path,
                        id_ranked.payload_offset,
                        id_ranked.payload_length,
                        id_ranked.payload_sha256,
                        final_by_id.canonical_id
                    FROM id_ranked
                    JOIN final_by_id ON final_by_id.id = id_ranked.id
                    WHERE id_ranked.id_rank > 1
                )
                SELECT * FROM content_duplicates
                UNION ALL
                SELECT * FROM id_duplicates;

                CREATE INDEX idx_duplicate_records_id ON duplicate_records(id);
                """
            )
        self._winners_valid = True

    def canonical_rows(self) -> Iterable[sqlite3.Row]:
        self.build_winner_tables()
        return self.conn.execute(
            """SELECT * FROM dedup_winners
               ORDER BY published_at DESC, id ASC, batch ASC, line_no ASC"""
        )

    def duplicate_rows(self) -> Iterable[sqlite3.Row]:
        self.build_winner_tables()
        return self.conn.execute(
            """SELECT * FROM duplicate_records
               ORDER BY published_at DESC, id ASC, batch ASC, line_no ASC"""
        )

    def reject_rows(self) -> Iterable[sqlite3.Row]:
        return self.conn.execute(
            """SELECT batch, line_no, raw_id, reason, message, raw_line
               FROM rejects
               ORDER BY batch ASC, line_no ASC"""
        )

    def load_record(self, row: sqlite3.Row) -> dict[str, Any]:
        ref = PayloadRef(
            path=str(row["payload_path"]),
            offset=int(row["payload_offset"]),
            length=int(row["payload_length"]),
            sha256=str(row["payload_sha256"]),
        )
        return load_payload(self.payload_dir, ref)

    def write_reports(
        self,
        reports_dir: Path,
        progress_path: Path,
        cleaned_dir: Path,
        dup_dir: Path,
        reject_dir: Path,
        event_dir: Path,
    ) -> dict[str, Any]:
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

        self.write_near_duplicates(reports_dir / "near_duplicates.jsonl")
        summary = self.summary(cleaned_dir, dup_dir, reject_dir, event_dir)
        (reports_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        return summary

    def write_near_duplicates(self, path: Path) -> int:
        self.build_winner_tables()
        groups = self.conn.execute(
            """SELECT title_norm, COUNT(*) AS count
               FROM dedup_winners
               WHERE title_norm != ''
               GROUP BY title_norm
               HAVING count > 1
               ORDER BY count DESC, title_norm ASC
               LIMIT 10000"""
        ).fetchall()
        written = 0
        with path.open("w", encoding="utf-8") as handle:
            for group in groups:
                rows = self.conn.execute(
                    """SELECT * FROM dedup_winners
                       WHERE title_norm = ?
                       ORDER BY published_at DESC, id ASC
                       LIMIT 20""",
                    (group["title_norm"],),
                ).fetchall()
                records = [self.load_record(row) for row in rows]
                handle.write(
                    json.dumps(
                        {
                            "type": "same_normalized_title",
                            "title_norm": group["title_norm"],
                            "count": group["count"],
                            "records": [
                                {
                                    "id": record["id"],
                                    "content_type": record["content_type"],
                                    "title": record["title"],
                                    "published_at": record["published_at"],
                                }
                                for record in records
                            ],
                        },
                        sort_keys=True,
                    )
                    + "\n"
                )
                written += 1
        return written

    def summary(self, cleaned_dir: Path, dup_dir: Path, reject_dir: Path, event_dir: Path) -> dict[str, Any]:
        self.build_winner_tables()
        accepted = _single_int(self.conn, "SELECT COUNT(*) FROM records")
        rejected = _single_int(self.conn, "SELECT COUNT(*) FROM rejects")
        total_input = _single_int(self.conn, "SELECT COALESCE(SUM(input_count), 0) FROM batch_state")
        cleaned = _single_int(self.conn, "SELECT COUNT(*) FROM dedup_winners")
        duplicates = _single_int(self.conn, "SELECT COUNT(*) FROM duplicate_records")

        canonical_type_counts = _group_counts(
            self.conn,
            "SELECT content_type, COUNT(*) FROM dedup_winners GROUP BY content_type",
        )
        canonical_method_counts = _group_counts(
            self.conn,
            "SELECT dedup_method, COUNT(*) FROM dedup_winners GROUP BY dedup_method",
        )
        duplicate_method_counts = _group_counts(
            self.conn,
            "SELECT dedup_method, COUNT(*) FROM duplicate_records GROUP BY dedup_method",
        )
        reject_reason_counts = _group_counts(self.conn, "SELECT reason, COUNT(*) FROM rejects GROUP BY reason")

        return {
            "total_input": total_input,
            "total_accepted": accepted,
            "total_cleaned": cleaned,
            "total_duplicates": duplicates,
            "total_rejected": rejected,
            "batches_completed": len(self.completed_batches()),
            "by_content_type": canonical_type_counts,
            "dedup_by_method": canonical_method_counts,
            "duplicate_by_method": duplicate_method_counts,
            "rejects_by_reason": reject_reason_counts,
            "storage": self.storage_summary(total_input, cleaned_dir, dup_dir, reject_dir, event_dir),
        }

    def storage_summary(
        self,
        total_input: int,
        cleaned_dir: Path,
        dup_dir: Path,
        reject_dir: Path,
        event_dir: Path,
    ) -> dict[str, int]:
        self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        db_bytes = sum(Path(f"{self.db_path}{suffix}").stat().st_size for suffix in ("", "-wal", "-shm") if Path(f"{self.db_path}{suffix}").exists())
        payload_bytes = directory_size(self.payload_dir)
        cleaned_bytes = directory_size(cleaned_dir)
        duplicates_bytes = directory_size(dup_dir)
        rejects_bytes = directory_size(reject_dir)
        event_bytes = directory_size(event_dir)
        total_bytes = db_bytes + payload_bytes + cleaned_bytes + duplicates_bytes + rejects_bytes + event_bytes
        estimated = int(total_bytes / total_input * TARGET_SCALE_ROWS) if total_input else 0
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
