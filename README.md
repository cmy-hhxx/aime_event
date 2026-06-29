# aime_event

Clean, deduplicate, and prepare financial consultation NDJSON batches.

The pipeline has three outputs:

- `cleaned`: full audit records with provenance and dedup metadata.
- `duplicates`: records folded by id or dedup key, with `dedup.canonical_id`.
- `event_input`: compact records for financial event concept extraction.

## Setup

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

## Run

Fresh run:

```bash
.venv/bin/python -m src.main --reset-state
```

Custom run:

```bash
.venv/bin/python -m src.main \
  --input data/raw \
  --output output/cleaned \
  --duplicates output/duplicates \
  --rejects output/rejects \
  --event-output output/event_input \
  --state state \
  --payload-dir state/payloads \
  --reports reports \
  --workers 4 \
  --chunk-size 3000 \
  --part-size 100000
```

Useful modes:

- `--export-only`: rebuild outputs/reports from existing v3 staging state.
- `--force`: reprocess completed batches by deleting that batch's SQLite rows and payload files first.
- `--reset-state`: delete staging DB and payloads before ingest. Required when moving from v1/v2 state.
- `--payload-dir`: place transformed-record payloads on a larger disk while keeping SQLite state elsewhere.

## Storage Model

SQLite v3 stores only lightweight indexing fields: ids, dedup keys, timestamps, body length, title normalization, and payload offsets. Full transformed records are written to `state/payloads/*_part_*.ndjson`.

This avoids the earlier v2 behavior where `state/dedup.db` stored complete `record_json` payloads. `reports/summary.json` includes storage totals and a linear 20M-row estimate:

```json
{
  "storage": {
    "db_bytes": 0,
    "payload_bytes": 0,
    "cleaned_bytes": 0,
    "event_input_bytes": 0,
    "estimated_20m_rows_bytes": 0
  }
}
```

For large runs, budget disk for raw input, SQLite state, payloads, cleaned output, duplicates, rejects, event input, and temporary export files. On this machine, the current free space is not enough for raw + all generated artifacts at 20M rows.

## Dedup Rules

Canonical selection is deterministic:

```text
body_len DESC, published_at DESC, id ASC, batch ASC, line_no ASC
```

Dedup v3 key precedence:

1. `US_NOTICE`: SEC accession from attachment URL, or SHA-256 of normalized attachment URL.
2. Eligible article URL: normalized `source.url`.
3. Feed/list/API URL or no eligible URL: SHA-256 of normalized title/body.
4. Empty title/body fallback: raw id.

URL normalization lowercases scheme/host, removes fragments, strips common tracking params such as `utm_*`, `mod`, and `r`, sorts remaining query parameters, and normalizes trailing slashes. Feed/list/API URLs are still denied as URL keys, including Reuters outboundfeeds/sitemap, Bloomberg lineup API, `outputType=xml`, `pageNumber`/`limit`, and `/market-news`.

Near duplicates are not automatically removed. `reports/near_duplicates.jsonl` records conservative candidates such as repeated normalized titles for manual review.

## Schemas

- `schema/cleaned_record.schema.json`: full audit contract.
- `schema/event_record.schema.json`: compact extraction input contract.
- `schema/cleaned_record.schema.jsonc`: human-readable schema notes.

`cleaned` keeps audit fields: `type_code`, `updated_at`, `source.author`, `meta`, raw-derived `tags`, and `dedup`.

`event_input` intentionally omits audit-only fields: `dedup`, `meta`, `type_code`, `summary`, `updated_at`, empty source/entity/topic arrays, and null notices. Raw `tags` become `topics` after removing importance and region tags.

## Quality Gates

Run tests:

```bash
.venv/bin/python -m pytest -q
```

Run type checking:

```bash
pyright src
```

Check cleaned has no duplicate canonical keys:

```bash
jq -r '.dedup.key' output/cleaned/*.ndjson | sort | uniq -d
```

Check event input is smaller than cleaned:

```bash
du -h output/cleaned output/event_input
```

Run a synthetic benchmark:

```bash
.venv/bin/python scripts/benchmark_synthetic.py --rows 100000 --workers 4
```
