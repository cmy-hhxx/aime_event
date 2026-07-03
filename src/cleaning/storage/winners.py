from __future__ import annotations

import json
import sqlite3
import time
from collections import OrderedDict
from itertools import combinations
from typing import Any

from src.cleaning.dedup.exact import DEDUP_VERSION
from src.cleaning.dedup.near import NearDecision, NearDuplicateDetector, NearSignature, UnionFind


def _log(message: str) -> None:
    print(message, flush=True)


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


class _SignatureCache:
    def __init__(self, limit: int = 10_000):
        self.limit = limit
        self.values: OrderedDict[str, NearSignature] = OrderedDict()

    def get(self, record_id: str) -> NearSignature | None:
        value = self.values.get(record_id)
        if value is None:
            return None
        self.values.move_to_end(record_id)
        return value

    def put(self, record_id: str, value: NearSignature) -> None:
        self.values[record_id] = value
        self.values.move_to_end(record_id)
        if len(self.values) > self.limit:
            self.values.popitem(last=False)


def _batched_insert(conn: sqlite3.Connection, sql: str, rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return
    conn.executemany(sql, rows)
    rows.clear()


def _near_candidate_filter(config: Any) -> tuple[str, list[Any]]:
    filters = ["content_type != 'US_NOTICE'", "body_len >= ?"]
    params: list[Any] = [int(config.min_body_chars)]
    if config.dedup_methods:
        placeholders = ", ".join("?" for _ in config.dedup_methods)
        filters.append(f"dedup_method IN ({placeholders})")
        params.extend(str(method) for method in config.dedup_methods)
    return " AND ".join(filters), params


class WinnerMixin:
    conn: sqlite3.Connection
    near_config: Any
    _winners_valid: bool

    def load_record(self, row: sqlite3.Row) -> dict[str, Any]: ...

    def build_winner_tables(self) -> None:
        if self._winners_valid:
            return
        started_at = time.monotonic()
        total_records = self.conn.execute("SELECT COUNT(*) AS count FROM records").fetchone()["count"]
        _log(f"Dedup: build winner tables start records={total_records:,}")
        with self.conn:
            _log("Dedup: exact step creating id_winners and dedup_winners")
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
                CREATE INDEX idx_dedup_winners_near_filter
                    ON dedup_winners(dedup_method, content_type, body_len);
                """
            )
        id_count = self.conn.execute("SELECT COUNT(*) AS count FROM id_winners").fetchone()["count"]
        dedup_count = self.conn.execute("SELECT COUNT(*) AS count FROM dedup_winners").fetchone()["count"]
        _log(f"Dedup: exact winners ready id_winners={id_count:,} dedup_winners={dedup_count:,}")

        self._build_near_duplicate_tables()

        with self.conn:
            _log("Dedup: creating duplicate_records")
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
        duplicate_count = self.conn.execute("SELECT COUNT(*) AS count FROM duplicate_records").fetchone()["count"]
        elapsed = time.monotonic() - started_at
        _log(f"Dedup: winner tables ready duplicates={duplicate_count:,} elapsed={elapsed:.1f}s")
        self._winners_valid = True

    def _build_near_duplicate_tables(self) -> None:
        _log("Dedup: resetting optional similarity tables")
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
            _log("Dedup: optional similarity merge skipped")
            return

        started_at = time.monotonic()
        detector = NearDuplicateDetector(self.near_config)
        total_winners = self.conn.execute("SELECT COUNT(*) AS count FROM dedup_winners").fetchone()["count"]
        filter_sql, filter_params = _near_candidate_filter(self.near_config)
        near_candidates = self.conn.execute(
            f"SELECT COUNT(*) AS count FROM dedup_winners WHERE {filter_sql}",
            filter_params,
        ).fetchone()["count"]
        methods = ",".join(self.near_config.dedup_methods) if self.near_config.dedup_methods else "all"
        _log(
            "Dedup: near-duplicate signatures start "
            f"total_winners={total_winners:,} candidates={near_candidates:,} "
            f"min_body_chars={self.near_config.min_body_chars:,} dedup_methods={methods}"
        )

        signature_rows: list[tuple[Any, ...]] = []
        bucket_rows: list[tuple[Any, ...]] = []
        signature_count = 0
        rows = self.conn.execute(
            f"""SELECT * FROM dedup_winners
                WHERE {filter_sql}
                ORDER BY published_at DESC, id ASC, batch ASC, line_no ASC""",
            filter_params,
        )
        with self.conn:
            for index, row in enumerate(rows, start=1):
                record = self.load_record(row)
                signature = detector.signature_for(record)
                if signature is None:
                    continue
                signature_count += 1
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
                if len(signature_rows) >= 5_000:
                    _batched_insert(
                        self.conn,
                        """INSERT INTO near_signatures
                           (id, signature_json, shingle_count, host, published_at, body_len)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        signature_rows,
                    )
                if len(bucket_rows) >= 50_000:
                    _batched_insert(
                        self.conn,
                        """INSERT INTO near_buckets (band_no, bucket_key, id)
                           VALUES (?, ?, ?)""",
                        bucket_rows,
                    )
                if index % 50_000 == 0:
                    elapsed = time.monotonic() - started_at
                    _log(
                        f"Dedup: near signatures rows={index:,} signatures={signature_count:,} "
                        f"elapsed={elapsed:.1f}s"
                    )
            _batched_insert(
                self.conn,
                """INSERT INTO near_signatures
                   (id, signature_json, shingle_count, host, published_at, body_len)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                signature_rows,
            )
            _batched_insert(
                self.conn,
                """INSERT INTO near_buckets (band_no, bucket_key, id)
                   VALUES (?, ?, ?)""",
                bucket_rows,
            )
        _log(f"Dedup: near signatures ready signatures={signature_count:,}")

        pair_count = self._build_near_candidate_pair_hits()
        _log(f"Dedup: near candidate pairs={pair_count:,}")
        union_find = UnionFind()
        candidate_rows: list[tuple[Any, ...]] = []
        signature_cache = _SignatureCache()
        pair_rows = self.conn.execute(
            """SELECT left_id, right_id, bucket_hits
               FROM near_pair_hits
               ORDER BY left_id, right_id"""
        )
        with self.conn:
            for index, row in enumerate(pair_rows, start=1):
                left_id = str(row["left_id"])
                right_id = str(row["right_id"])
                left = self._load_near_signature(left_id, signature_cache)
                right = self._load_near_signature(right_id, signature_cache)
                if left is None or right is None:
                    continue
                decision = detector.decide(left, right)
                if decision.auto_merged:
                    union_find.union(left_id, right_id)
                candidate_rows.append(
                    (
                        left_id,
                        right_id,
                        int(row["bucket_hits"]),
                        decision.status,
                        decision.reason,
                        decision.jaccard,
                        decision.fuzzy_score,
                        decision.title_score,
                        None,
                    )
                )
                if len(candidate_rows) >= 5_000:
                    _batched_insert(
                        self.conn,
                        """INSERT INTO near_candidate_pairs (
                               left_id, right_id, bucket_hits, status, reason, minhash_jaccard,
                               fuzzy_score, title_score, canonical_id
                           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        candidate_rows,
                    )
                if index % 50_000 == 0:
                    elapsed = time.monotonic() - started_at
                    _log(f"Dedup: near decisions pairs={index:,} elapsed={elapsed:.1f}s")
            _batched_insert(
                self.conn,
                """INSERT INTO near_candidate_pairs (
                       left_id, right_id, bucket_hits, status, reason, minhash_jaccard,
                       fuzzy_score, title_score, canonical_id
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                candidate_rows,
            )

        loser_rows = self._near_loser_rows(union_find)
        _log(f"Dedup: near losers={len(loser_rows):,}")
        with self.conn:
            if loser_rows:
                self.conn.executemany(
                    """INSERT INTO near_duplicate_losers
                       (loser_id, canonical_id, cluster_id, dedup_key, dedup_debug)
                       VALUES (?, ?, ?, ?, ?)""",
                    loser_rows,
                )
                self.conn.execute(
                    """UPDATE near_candidate_pairs
                       SET canonical_id = COALESCE(
                           (SELECT canonical_id FROM near_duplicate_losers
                            WHERE loser_id = near_candidate_pairs.left_id),
                           (SELECT canonical_id FROM near_duplicate_losers
                            WHERE loser_id = near_candidate_pairs.right_id)
                       )
                       WHERE canonical_id IS NULL"""
                )

    def _load_near_signature(self, record_id: str, cache: _SignatureCache) -> NearSignature | None:
        cached = cache.get(record_id)
        if cached is not None:
            return cached
        row = self.conn.execute(
            """SELECT dedup_winners.*, near_signatures.signature_json,
                      near_signatures.shingle_count,
                      near_signatures.host AS near_host,
                      near_signatures.published_at AS near_published_at,
                      near_signatures.body_len AS near_body_len
               FROM dedup_winners
               JOIN near_signatures ON near_signatures.id = dedup_winners.id
               WHERE dedup_winners.id = ?""",
            (record_id,),
        ).fetchone()
        if row is None:
            return None
        record = self.load_record(row)
        body = str(record.get("body") or "").strip()
        signature = NearSignature(
            record_id=record_id,
            signature=tuple(int(value) for value in json.loads(row["signature_json"])),
            shingle_count=int(row["shingle_count"]),
            title=str(record.get("title") or ""),
            body=body,
            host=str(row["near_host"] or ""),
            published_at=str(row["near_published_at"] or ""),
            body_len=int(row["near_body_len"]),
        )
        cache.put(record_id, signature)
        return signature

    def _build_near_candidate_pair_hits(self) -> int:
        max_pairs = self.near_config.max_candidate_pairs
        with self.conn:
            self.conn.execute("DROP TABLE IF EXISTS near_pair_hits")
            self.conn.execute(
                """CREATE TEMP TABLE near_pair_hits (
                       left_id TEXT NOT NULL,
                       right_id TEXT NOT NULL,
                       bucket_hits INTEGER NOT NULL,
                       PRIMARY KEY (left_id, right_id)
                   )"""
            )

        pair_rows: list[tuple[str, str]] = []
        pair_count = 0
        bucket_rows = self.conn.execute(
            """SELECT band_no, bucket_key, COUNT(*) AS count
               FROM near_buckets
               GROUP BY band_no, bucket_key
               HAVING count BETWEEN 2 AND ?
               ORDER BY count DESC""",
            (self.near_config.max_bucket_size,),
        )
        for bucket_index, bucket in enumerate(bucket_rows, start=1):
            rows = self.conn.execute(
                """SELECT id FROM near_buckets
                   WHERE band_no = ? AND bucket_key = ?
                   ORDER BY id""",
                (bucket["band_no"], bucket["bucket_key"]),
            ).fetchall()
            ids = [str(row["id"]) for row in rows]
            for left_id, right_id in combinations(ids, 2):
                pair_rows.append((left_id, right_id))
                if len(pair_rows) >= 10_000:
                    pair_count = self._flush_near_pair_hits(pair_rows)
                    if pair_count >= max_pairs:
                        _log(f"Dedup: near candidate pair cap reached pairs={pair_count:,}")
                        return pair_count
            if bucket_index % 1_000 == 0:
                pair_count = self._flush_near_pair_hits(pair_rows)
                _log(f"Dedup: near buckets={bucket_index:,} candidate_pairs={pair_count:,}")
                if pair_count >= max_pairs:
                    _log(f"Dedup: near candidate pair cap reached pairs={pair_count:,}")
                    return pair_count
        pair_count = self._flush_near_pair_hits(pair_rows)
        return pair_count

    def _flush_near_pair_hits(self, pair_rows: list[tuple[str, str]]) -> int:
        if pair_rows:
            with self.conn:
                self.conn.executemany(
                    """INSERT INTO near_pair_hits (left_id, right_id, bucket_hits)
                       VALUES (?, ?, 1)
                       ON CONFLICT(left_id, right_id)
                       DO UPDATE SET bucket_hits = bucket_hits + 1""",
                    pair_rows,
                )
            pair_rows.clear()
        return self.conn.execute("SELECT COUNT(*) AS count FROM near_pair_hits").fetchone()["count"]

    def _best_decision_for(self, loser_id: str, members: set[str]) -> NearDecision:
        best: NearDecision | None = None
        for other_id in members - {loser_id}:
            left_id, right_id = sorted((loser_id, other_id))
            row = self.conn.execute(
                """SELECT status, reason, minhash_jaccard, fuzzy_score, title_score
                   FROM near_candidate_pairs
                   WHERE left_id = ? AND right_id = ?""",
                (left_id, right_id),
            ).fetchone()
            if row is None:
                continue
            decision = NearDecision(
                str(row["status"]),
                str(row["reason"]),
                float(row["minhash_jaccard"]),
                float(row["fuzzy_score"]),
                float(row["title_score"]),
            )
            if best is None or (decision.jaccard, decision.fuzzy_score, decision.title_score) > (
                best.jaccard,
                best.fuzzy_score,
                best.title_score,
            ):
                best = decision
        return best or NearDecision("auto_merged", "cluster_member", 0.0, 0.0, 0.0)

    def _winner_id_for_members(self, members: set[str]) -> str:
        placeholders = ",".join("?" for _ in members)
        rows = self.conn.execute(
            f"""SELECT id, body_len, published_at, batch, line_no
                FROM dedup_winners
                WHERE id IN ({placeholders})""",
            tuple(members),
        ).fetchall()
        return str(min(rows, key=_canonical_sort_key)["id"])

    def _near_loser_rows(
        self,
        union_find: UnionFind,
    ) -> list[tuple[str, str, str, str, str]]:
        rows = []
        for members in union_find.groups().values():
            if len(members) < 2:
                continue
            winner_id = self._winner_id_for_members(members)
            cluster_id = f"near:{winner_id}"
            for loser_id in sorted(members - {winner_id}):
                decision = self._best_decision_for(loser_id, members)
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
