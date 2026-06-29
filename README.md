# aime_event

Clean and deduplicate financial consultation NDJSON batches.

The pipeline is intentionally two-phase:

1. `ingest`: parse raw `content_batch_*.ndjson`, transform valid records, and store candidates/rejects in SQLite staging.
2. `export`: select final canonicals from staging and write global output parts atomically.

This avoids the old streaming-output bug where a later, longer canonical could not remove an earlier `cleaned` row.

## Setup

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

## Run

Fresh run against the default directories:

```bash
.venv/bin/python -m src.main --reset-state
```

Custom directories:

```bash
.venv/bin/python -m src.main \
  --input data/raw \
  --output output/cleaned \
  --duplicates output/duplicates \
  --rejects output/rejects \
  --state state \
  --reports reports \
  --workers 4 \
  --chunk-size 3000 \
  --part-size 100000
```

Useful modes:

- `--export-only`: rebuild `output/*` and reports from the existing staging DB.
- `--force`: safely reprocess already completed batches by deleting only that batch from staging first.
- `--reset-state`: delete the staging DB and rebuild from raw input. Required when moving from the old `dedup_index` state schema.

## Outputs

- `output/cleaned/cleaned_part_00000.ndjson`: final canonical records.
- `output/duplicates/dup_part_00000.ndjson`: duplicate records with `dedup.canonical_id`.
- `output/rejects/reject_part_00000.ndjson`: invalid JSON, missing required fields, empty non-notice bodies, and other rejected rows.
- `reports/batch_stats.jsonl`: per-batch ingest status from SQLite.
- `reports/summary.json`: global counts rebuilt from SQLite, so resume/export-only runs stay consistent.
- `state/dedup.db`: SQLite staging state.
- `state/progress.json`: human-readable completed-batch snapshot generated from SQLite.

## Dedup Rules

Records with the same raw `_id` are folded first. Among repeated IDs, canonical selection is deterministic:

```text
body_len DESC, published_at DESC, id ASC
```

The surviving ID candidates are then deduplicated by primary key:

1. Eligible article URL: `url:{source.url}`.
2. Feed/list/API URL or no URL: `hash:{md5(normalized_title|normalized_body)}`.
3. Empty title/body fallback: `id:{id}`.

Feed/list/API URL denylist includes Reuters outbound sitemap/feed URLs, sitemap URLs, Bloomberg lineup API URLs, `outputType=xml`, `pageNumber`/`limit` list queries, and `/market-news`.

## Quality Gates

Run tests:

```bash
.venv/bin/python -m pytest -q
```

Run type checking:

```bash
pyright src
```

Check a generated cleaned directory has no duplicate canonical keys:

```bash
jq -r '.dedup.key' output/cleaned/*.ndjson | sort | uniq -d
```

The executable cleaned-record contract is `schema/cleaned_record.schema.json`. The JSONC file is documentation only.
