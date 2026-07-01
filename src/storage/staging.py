from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Optional

from src.config import NearDuplicateConfig
from src.dedup.exact import compute_dedup_key, normalize_text
from src.storage.payload import PayloadRef, delete_payload_files, load_payload, reset_payload_dir
from src.storage.winners import WinnerMixin

SCHEMA_VERSION = "4"


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


class StagingDB(WinnerMixin):
    def __init__(
        self,
        db_path: Path,
        payload_dir: Path,
        reset: bool = False,
        near_config: NearDuplicateConfig | None = None,
        target_scale_rows: int = 20_000_000,
    ):
        self.db_path = db_path
        self.payload_dir = payload_dir
        self.near_config = near_config or NearDuplicateConfig()
        self.target_scale_rows = target_scale_rows
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
                f"{self.db_path} uses the old dedup schema. Re-run with `python -m src.main fresh` "
                "to rebuild staging state."
            )

        if self._table_exists("metadata"):
            version = self.conn.execute("SELECT value FROM metadata WHERE key = 'schema_version'").fetchone()
            if version is not None and version["value"] != SCHEMA_VERSION:
                raise StateVersionError(
                    f"{self.db_path} has schema_version={version['value']}; expected {SCHEMA_VERSION}. "
                    "Re-run with `python -m src.main fresh` to rebuild staging state."
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

            CREATE TABLE IF NOT EXISTS near_signatures (
                id TEXT PRIMARY KEY,
                signature_json TEXT NOT NULL,
                shingle_count INTEGER NOT NULL,
                host TEXT NOT NULL,
                published_at TEXT NOT NULL,
                body_len INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS near_buckets (
                band_no INTEGER NOT NULL,
                bucket_key TEXT NOT NULL,
                id TEXT NOT NULL,
                PRIMARY KEY (band_no, bucket_key, id)
            );

            CREATE TABLE IF NOT EXISTS near_candidate_pairs (
                left_id TEXT NOT NULL,
                right_id TEXT NOT NULL,
                bucket_hits INTEGER NOT NULL,
                status TEXT NOT NULL,
                reason TEXT NOT NULL,
                minhash_jaccard REAL NOT NULL,
                fuzzy_score REAL NOT NULL,
                title_score REAL NOT NULL,
                canonical_id TEXT,
                PRIMARY KEY (left_id, right_id)
            );

            CREATE TABLE IF NOT EXISTS near_duplicate_losers (
                loser_id TEXT PRIMARY KEY,
                canonical_id TEXT NOT NULL,
                cluster_id TEXT NOT NULL,
                dedup_key TEXT NOT NULL,
                dedup_debug TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_near_buckets_key ON near_buckets(band_no, bucket_key);
            CREATE INDEX IF NOT EXISTS idx_near_pairs_status ON near_candidate_pairs(status);
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

    def canonical_rows(self) -> Iterable[sqlite3.Row]:
        self.build_winner_tables()
        return self.conn.execute(
            """SELECT * FROM dedup_winners
               WHERE id NOT IN (SELECT loser_id FROM near_duplicate_losers)
               ORDER BY published_at ASC, id ASC, batch ASC, line_no ASC"""
        )

    def duplicate_rows(self) -> Iterable[sqlite3.Row]:
        self.build_winner_tables()
        return self.conn.execute(
            """SELECT * FROM duplicate_records
               ORDER BY published_at ASC, id ASC, batch ASC, line_no ASC"""
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

    def load_exact_winner(self, record_id: str) -> dict[str, Any]:
        row = self.conn.execute("SELECT * FROM dedup_winners WHERE id = ?", (record_id,)).fetchone()
        if row is None:
            return {"id": record_id}
        return self.load_record(row)
