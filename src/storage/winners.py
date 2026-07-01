from __future__ import annotations

import json
import sqlite3
import time
from collections import Counter
from itertools import combinations
from typing import Any

from src.dedup.exact import DEDUP_VERSION
from src.dedup.near import NearDecision, NearDuplicateDetector, NearSignature, UnionFind


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
        _log("Dedup: resetting near-duplicate tables")
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
            _log("Dedup: near-duplicate merge disabled")
            return

        started_at = time.monotonic()
        detector = NearDuplicateDetector(self.near_config)
        rows = self.conn.execute(
            """SELECT * FROM dedup_winners
               ORDER BY published_at DESC, id ASC, batch ASC, line_no ASC"""
        ).fetchall()
        row_by_id = {str(row["id"]): row for row in rows}
        signature_by_id: dict[str, NearSignature] = {}
        _log(f"Dedup: near-duplicate signatures start candidates={len(rows):,}")

        signature_rows = []
        bucket_rows = []
        for index, row in enumerate(rows, start=1):
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
            if index % 50_000 == 0:
                elapsed = time.monotonic() - started_at
                _log(f"Dedup: near signatures rows={index:,} elapsed={elapsed:.1f}s")

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
        _log(f"Dedup: near candidate pairs={len(pair_hits):,}")
        decisions: dict[tuple[str, str], NearDecision] = {}
        union_find = UnionFind()
        candidate_rows = []
        for index, ((left_id, right_id), bucket_hits_count) in enumerate(sorted(pair_hits.items()), start=1):
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
            if index % 50_000 == 0:
                elapsed = time.monotonic() - started_at
                _log(f"Dedup: near decisions pairs={index:,} elapsed={elapsed:.1f}s")

        loser_rows = self._near_loser_rows(union_find, row_by_id, decisions)
        _log(f"Dedup: near losers={len(loser_rows):,}")
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
