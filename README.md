# aime_event

Clean raw AIME event NDJSON into audit records and compact financial event extraction input.

## What It Produces

- `output/cleaned/cleaned_part_*.ndjson`: canonical audit records with provenance and dedup metadata.
- `output/duplicates/dup_part_*.ndjson`: exact or high-confidence near duplicates, each with `dedup.canonical_id`.
- `output/event_input/event_part_*.ndjson`: compact records for event concept extraction.
- `output/rejects/reject_part_*.ndjson`: invalid raw lines and reject reasons.
- `reports/summary.json`: counts, storage totals, and near-duplicate statistics.

Public `cleaned`, `duplicates`, and `event_input` records do not contain `null`. Empty optional fields are omitted.

## Run

Install dependencies:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

Normal run:

```bash
.venv/bin/python -m src.main
```

Rebuild from raw input:

```bash
.venv/bin/python -m src.main fresh
```

Re-export from existing state:

```bash
.venv/bin/python -m src.main export
```

Daily configuration lives in `src/config.py`. CLI flags still exist for tests and one-off overrides, but routine runs should use the commands above.

## Configuration

`src/config.py` contains:

- Paths: raw input, output directories, state DB, payloads, reports.
- Runtime: workers, chunk size, output part size, payload part size.
- Near dedup: MinHash permutations, shingle size, body length gate, Jaccard threshold, RapidFuzz threshold, bucket limits.

State schema is v4. If an older `state/dedup.db` exists, run:

```bash
.venv/bin/python -m src.main fresh
```

## Dedup

Exact dedup runs first:

1. Fold repeated raw ids.
2. Use SEC notice accession or attachment URL for `US_NOTICE`.
3. Use normalized article URL for eligible article pages.
4. Fall back to SHA-256 of normalized title and body.

High-confidence near dedup then runs on exact canonical records only. It uses `datasketch` MinHash signatures for candidate discovery and `rapidfuzz` token-set similarity for confirmation. It does not auto-merge notices, short bodies, or low-confidence candidate pairs. Auto-merged records use `dedup.method = "near_minhash"`.

## Validate

```bash
.venv/bin/python -m pytest -q
pyright src
jq -r '.dedup.key' output/cleaned/*.ndjson | sort | uniq -d
jq -n '[inputs | .. | select(. == null)] | length' output/cleaned/*.ndjson
jq -n '[inputs | .. | select(. == null)] | length' output/duplicates/*.ndjson
jq -n '[inputs | .. | select(. == null)] | length' output/event_input/*.ndjson
```

Synthetic benchmark:

```bash
.venv/bin/python scripts/benchmark_synthetic.py --rows 100000 --workers 4 --near-every 5
```
