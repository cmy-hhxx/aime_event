from __future__ import annotations

import json
import sqlite3
from collections import Counter
from itertools import combinations
from pathlib import Path
from typing import Any, Iterable, Optional

from src.dedup import DEDUP_VERSION, compute_dedup_key, normalize_text
from src.config import NearDuplicateConfig
from src.near_dedup import NearDecision, NearDuplicateDetector, NearSignature, UnionFind
from src.payload_store import PayloadRef, delete_payload_files, directory_size, load_payload, reset_payload_dir

SCHEMA_VERSION = "4"
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
    def __init__(
        self,
        db_path: Path,
        payload_dir: Path,
        reset: bool = False,
        near_config: NearDuplicateConfig | None = None,
        target_scale_rows: int = TARGET_SCALE_ROWS,
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

    def build_winner_tables(self) -> None:
        if self._winners_valid:
            return
        with self.conn:
            self.conn.executescript(
                f"""
                DROP TABLE IF EXISTS id_winners;
                DROP TABLE IF EXISTS dedup_winners;

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
                """
            )

        self._build_near_duplicate_tables()

        with self.conn:
            self.conn.executescript(
                f"""
                DROP TABLE IF EXISTS duplicate_records;

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
                    SELECT
                        dedup_ranked.id,
                        COALESCE(near_duplicate_losers.canonical_id, dedup_ranked.canonical_id) AS canonical_id
                    FROM dedup_ranked
                    LEFT JOIN near_duplicate_losers
                        ON near_duplicate_losers.loser_id = dedup_ranked.canonical_id
                ),
                content_duplicates AS (
                    SELECT
                        dedup_ranked.batch,
                        dedup_ranked.line_no,
                        dedup_ranked.id,
                        dedup_ranked.content_type,
                        dedup_ranked.published_at,
                        dedup_ranked.updated_at,
                        dedup_ranked.body_len,
                        dedup_ranked.title_norm,
                        dedup_ranked.dedup_key,
                        dedup_ranked.dedup_method,
                        dedup_ranked.dedup_debug,
                        dedup_ranked.payload_path,
                        dedup_ranked.payload_offset,
                        dedup_ranked.payload_length,
                        dedup_ranked.payload_sha256,
                        COALESCE(near_duplicate_losers.canonical_id, dedup_ranked.canonical_id) AS canonical_id
                    FROM dedup_ranked
                    LEFT JOIN near_duplicate_losers
                        ON near_duplicate_losers.loser_id = dedup_ranked.canonical_id
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
                ),
                near_duplicates AS (
                    SELECT
                        dedup_winners.batch,
                        dedup_winners.line_no,
                        dedup_winners.id,
                        dedup_winners.content_type,
                        dedup_winners.published_at,
                        dedup_winners.updated_at,
                        dedup_winners.body_len,
                        dedup_winners.title_norm,
                        near_duplicate_losers.dedup_key,
                        'near_minhash' AS dedup_method,
                        near_duplicate_losers.dedup_debug,
                        dedup_winners.payload_path,
                        dedup_winners.payload_offset,
                        dedup_winners.payload_length,
                        dedup_winners.payload_sha256,
                        near_duplicate_losers.canonical_id
                    FROM dedup_winners
                    JOIN near_duplicate_losers
                        ON near_duplicate_losers.loser_id = dedup_winners.id
                )
                SELECT * FROM content_duplicates
                UNION ALL
                SELECT * FROM id_duplicates
                UNION ALL
                SELECT * FROM near_duplicates;

                CREATE INDEX idx_duplicate_records_id ON duplicate_records(id);
                """
            )
        self._winners_valid = True

    def _build_near_duplicate_tables(self) -> None:
        with self.conn:
            self.conn.executescript(
                """
                DELETE FROM near_signatures;
                DELETE FROM near_buckets;
                DELETE FROM near_candidate_pairs;
                DELETE FROM near_duplicate_losers;
                """
            )

        if not self.near_config.enabled:
            return

        detector = NearDuplicateDetector(self.near_config)
        rows = self.conn.execute(
            """SELECT * FROM dedup_winners
               ORDER BY published_at DESC, id ASC, batch ASC, line_no ASC"""
        ).fetchall()
        row_by_id = {str(row["id"]): row for row in rows}
        signature_by_id: dict[str, NearSignature] = {}

        signature_rows = []
        bucket_rows = []
        for row in rows:
            record = self.load_record(row)
            signature = detector.signature_for(record)
            if signature is None:
                continue
            signature_by_id[signature.record_id] = signature
            signature_rows.append(
                (
                    signature.record_id,
                    json.dumps(signature.signature),
                    signature.shingle_count,
                    signature.host,
                    signature.published_at,
                    signature.body_len,
                )
            )
            for band_no, bucket_key in detector.band_keys(signature.signature):
                bucket_rows.append((band_no, bucket_key, signature.record_id))

        with self.conn:
            if signature_rows:
                self.conn.executemany(
                    """INSERT INTO near_signatures
                       (id, signature_json, shingle_count, host, published_at, body_len)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    signature_rows,
                )
            if bucket_rows:
                self.conn.executemany(
                    """INSERT INTO near_buckets (band_no, bucket_key, id)
                       VALUES (?, ?, ?)""",
                    bucket_rows,
                )

        pair_hits = self._near_candidate_pair_hits()
        decisions: dict[tuple[str, str], NearDecision] = {}
        union_find = UnionFind()
        candidate_rows = []
        for (left_id, right_id), bucket_hits_count in sorted(pair_hits.items()):
            left = signature_by_id[left_id]
            right = signature_by_id[right_id]
            decision = detector.decide(left, right)
            decisions[(left_id, right_id)] = decision
            if decision.auto_merged:
                union_find.union(left_id, right_id)
            candidate_rows.append(
                (
                    left_id,
                    right_id,
                    bucket_hits_count,
                    decision.status,
                    decision.reason,
                    decision.jaccard,
                    decision.fuzzy_score,
                    decision.title_score,
                    None,
                )
            )

        loser_rows = self._near_loser_rows(union_find, row_by_id, decisions)
        canonical_by_loser = {row[0]: row[1] for row in loser_rows}
        candidate_rows = [
            (*row[:-1], canonical_by_loser.get(row[0]) or canonical_by_loser.get(row[1]))
            for row in candidate_rows
        ]

        with self.conn:
            if candidate_rows:
                self.conn.executemany(
                    """INSERT INTO near_candidate_pairs (
                           left_id, right_id, bucket_hits, status, reason, minhash_jaccard,
                           fuzzy_score, title_score, canonical_id
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    candidate_rows,
                )
            if loser_rows:
                self.conn.executemany(
                    """INSERT INTO near_duplicate_losers
                       (loser_id, canonical_id, cluster_id, dedup_key, dedup_debug)
                       VALUES (?, ?, ?, ?, ?)""",
                    loser_rows,
                )

    def _near_candidate_pair_hits(self) -> Counter[tuple[str, str]]:
        pair_hits: Counter[tuple[str, str]] = Counter()
        max_pairs = self.near_config.max_candidate_pairs
        buckets = self.conn.execute(
            """SELECT band_no, bucket_key, COUNT(*) AS count
               FROM near_buckets
               GROUP BY band_no, bucket_key
               HAVING count BETWEEN 2 AND ?
               ORDER BY count DESC""",
            (self.near_config.max_bucket_size,),
        ).fetchall()
        for bucket in buckets:
            rows = self.conn.execute(
                """SELECT id FROM near_buckets
                   WHERE band_no = ? AND bucket_key = ?
                   ORDER BY id""",
                (bucket["band_no"], bucket["bucket_key"]),
            ).fetchall()
            ids = [str(row["id"]) for row in rows]
            for pair in combinations(ids, 2):
                pair_hits[pair] += 1
                if len(pair_hits) >= max_pairs:
                    return pair_hits
        return pair_hits

    def _near_loser_rows(
        self,
        union_find: UnionFind,
        row_by_id: dict[str, sqlite3.Row],
        decisions: dict[tuple[str, str], NearDecision],
    ) -> list[tuple[str, str, str, str, str]]:
        rows = []
        for members in union_find.groups().values():
            if len(members) < 2:
                continue
            winner_id = min(members, key=lambda item: _canonical_sort_key(row_by_id[item]))
            cluster_id = f"near:{winner_id}"
            for loser_id in sorted(members - {winner_id}):
                decision = _best_decision_for(loser_id, members, decisions)
                debug = {
                    "version": DEDUP_VERSION,
                    "source": "minhash_lsh",
                    "cluster_id": cluster_id,
                    "minhash_threshold": self.near_config.threshold,
                    "fuzzy_threshold": self.near_config.fuzzy_threshold,
                    "scores": {
                        "minhash_jaccard": round(decision.jaccard, 6),
                        "fuzzy_score": round(decision.fuzzy_score, 6),
                        "title_score": round(decision.title_score, 6),
                    },
                    "reason": decision.reason,
                }
                rows.append((loser_id, winner_id, cluster_id, cluster_id, json.dumps(debug, sort_keys=True)))
        return rows

    def canonical_rows(self) -> Iterable[sqlite3.Row]:
        self.build_winner_tables()
        return self.conn.execute(
            """SELECT * FROM dedup_winners
               WHERE id NOT IN (SELECT loser_id FROM near_duplicate_losers)
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
        rows = self.conn.execute(
            """SELECT *
               FROM near_candidate_pairs
               ORDER BY status ASC, minhash_jaccard DESC, fuzzy_score DESC, left_id ASC, right_id ASC
               LIMIT ?""",
            (self.near_config.max_report_pairs,),
        ).fetchall()
        written = 0
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                left = self._load_exact_winner(str(row["left_id"]))
                right = self._load_exact_winner(str(row["right_id"]))
                handle.write(
                    json.dumps(
                        {
                            "type": "near_minhash",
                            "status": row["status"],
                            "reason": row["reason"],
                            "left_id": row["left_id"],
                            "right_id": row["right_id"],
                            "canonical_id": row["canonical_id"],
                            "bucket_hits": row["bucket_hits"],
                            "scores": {
                                "minhash_jaccard": row["minhash_jaccard"],
                                "fuzzy_score": row["fuzzy_score"],
                                "title_score": row["title_score"],
                            },
                            "left": _report_record(left),
                            "right": _report_record(right),
                        },
                        sort_keys=True,
                    )
                    + "\n"
                )
                written += 1
        return written

    def _load_exact_winner(self, record_id: str) -> dict[str, Any]:
        row = self.conn.execute("SELECT * FROM dedup_winners WHERE id = ?", (record_id,)).fetchone()
        if row is None:
            return {"id": record_id}
        return self.load_record(row)

    def summary(self, cleaned_dir: Path, dup_dir: Path, reject_dir: Path, event_dir: Path) -> dict[str, Any]:
        self.build_winner_tables()
        accepted = _single_int(self.conn, "SELECT COUNT(*) FROM records")
        rejected = _single_int(self.conn, "SELECT COUNT(*) FROM rejects")
        total_input = _single_int(self.conn, "SELECT COALESCE(SUM(input_count), 0) FROM batch_state")
        cleaned = _single_int(
            self.conn,
            "SELECT COUNT(*) FROM dedup_winners WHERE id NOT IN (SELECT loser_id FROM near_duplicate_losers)",
        )
        duplicates = _single_int(self.conn, "SELECT COUNT(*) FROM duplicate_records")

        canonical_type_counts = _group_counts(
            self.conn,
            """SELECT content_type, COUNT(*) FROM dedup_winners
               WHERE id NOT IN (SELECT loser_id FROM near_duplicate_losers)
               GROUP BY content_type""",
        )
        canonical_method_counts = _group_counts(
            self.conn,
            """SELECT dedup_method, COUNT(*) FROM dedup_winners
               WHERE id NOT IN (SELECT loser_id FROM near_duplicate_losers)
               GROUP BY dedup_method""",
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
            "near_duplicate_candidates": _single_int(self.conn, "SELECT COUNT(*) FROM near_candidate_pairs"),
            "near_duplicates_auto_merged": _single_int(
                self.conn,
                "SELECT COUNT(*) FROM near_duplicate_losers",
            ),
            "near_duplicates_report_only": _single_int(
                self.conn,
                "SELECT COUNT(*) FROM near_candidate_pairs WHERE status = 'report_only'",
            ),
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
        estimated = int(total_bytes / total_input * self.target_scale_rows) if total_input else 0
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


def _canonical_sort_key(row: sqlite3.Row) -> tuple[int, float, str, str, int]:
    return (
        -int(row["body_len"]),
        -_timestamp(row["published_at"]),
        str(row["id"]),
        str(row["batch"]),
        int(row["line_no"]),
    )


def _timestamp(value: str) -> float:
    from datetime import datetime

    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def _best_decision_for(
    loser_id: str,
    members: set[str],
    decisions: dict[tuple[str, str], NearDecision],
) -> NearDecision:
    relevant = []
    for other_id in members - {loser_id}:
        pair = tuple(sorted((loser_id, other_id)))
        if len(pair) != 2:
            continue
        decision = decisions.get(pair)
        if decision is not None:
            relevant.append(decision)
    if not relevant:
        return NearDecision("auto_merged", "cluster_member", 0.0, 0.0, 0.0)
    return max(relevant, key=lambda item: (item.jaccard, item.fuzzy_score, item.title_score))


def _report_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": record.get("id"),
        "content_type": record.get("content_type"),
        "title": record.get("title"),
        "published_at": record.get("published_at"),
    }
