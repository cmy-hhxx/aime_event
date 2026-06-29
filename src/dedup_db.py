from __future__ import annotations

import sqlite3
from pathlib import Path


class DedupDB:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS dedup_index (
                dedup_key TEXT PRIMARY KEY,
                method TEXT NOT NULL,
                canonical_id TEXT NOT NULL,
                content_type TEXT,
                body_len INTEGER NOT NULL DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_canonical ON dedup_index(canonical_id)")
        self.conn.commit()

    def _get(self, key: str) -> tuple[str, int] | None:
        row = self.conn.execute(
            "SELECT canonical_id, body_len FROM dedup_index WHERE dedup_key = ?",
            (key,),
        ).fetchone()
        return (row[0], row[1]) if row else None

    def _upsert(self, key: str, method: str, canonical_id: str, content_type: str, body_len: int):
        self.conn.execute(
            """INSERT INTO dedup_index (dedup_key, method, canonical_id, content_type, body_len)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(dedup_key) DO UPDATE SET
                 canonical_id = excluded.canonical_id,
                 content_type = excluded.content_type,
                 body_len = excluded.body_len""",
            (key, method, canonical_id, content_type, body_len),
        )

    def register(
        self,
        record_id: str,
        content_key: str | None,
        method: str,
        content_type: str,
        body_len: int,
    ) -> tuple[str, bool]:
        """
        Returns (status, canonical_id_for_dup_ref).
        status: 'new' | 'duplicate' | 'replaced'
        """
        id_k = f"id:{record_id}"
        if self._get(id_k):
            return "duplicate", self._get(id_k)[0]

        if content_key:
            existing = self._get(content_key)
            if existing:
                existing_id, existing_len = existing
                if body_len > existing_len:
                    self._upsert(content_key, method, record_id, content_type, body_len)
                    self._upsert(id_k, "id", record_id, content_type, body_len)
                    return "replaced", existing_id
                return "duplicate", existing_id

        self._upsert(id_k, "id", record_id, content_type, body_len)
        if content_key:
            self._upsert(content_key, method, record_id, content_type, body_len)
        return "new", None

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()

    def count(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) FROM dedup_index").fetchone()
        return row[0] if row else 0
