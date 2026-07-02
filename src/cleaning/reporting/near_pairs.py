from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.cleaning.storage.staging import StagingDB


def _report_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": record.get("id"),
        "content_type": record.get("content_type"),
        "title": record.get("title"),
        "published_at": record.get("published_at"),
    }


def write_near_duplicates(db: StagingDB, path: Path) -> int:
    db.build_winner_tables()
    rows = db.conn.execute(
        """SELECT *
           FROM near_candidate_pairs
           ORDER BY status ASC, minhash_jaccard DESC, fuzzy_score DESC, left_id ASC, right_id ASC
           LIMIT ?""",
        (db.near_config.max_report_pairs,),
    ).fetchall()
    written = 0
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            left = db.load_exact_winner(str(row["left_id"]))
            right = db.load_exact_winner(str(row["right_id"]))
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
