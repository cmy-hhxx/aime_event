# AGENTS.md

This file is for future coding agents working on this repository. It summarizes the current project state, expected workflow, and sharp edges.

## Project State

- Repository: `aime_event`
- Current working branch for the stage-layout refactor: `refactor/pipeline-stages`
- Base feature branch already pushed: `feat/v3-v1-cleaned-export`
- Remote: `https://github.com/cmy-hhxx/aime_event.git`
- The repository is being reorganized into three stages:
  - `cleaning`: implemented data cleaning, deduplication, time ordering, sharded JSONL export
  - `extraction`: placeholder package and CLI entrypoint for future event extraction
  - `completion`: placeholder package and CLI entrypoint for future event completion

## Repository Layout

```text
docs/                 Stage documentation
schema/               Stage-specific JSON Schemas
scripts/              Utility and benchmark scripts
src/
  main.py             python -m src.main compatibility entrypoint
  config.py           Current default runtime and path configuration
  cli/                Unified CLI routing
  common/             Shared IO, logging, path, and SQLite helpers
  cleaning/           Implemented cleaning pipeline
  extraction/         Event extraction placeholder
  completion/         Event completion placeholder
tests/                Tests grouped by stage/common
```

Key docs:

- `README.md`: repo overview and quick commands
- `docs/cleaning.md`: cleaning and dedup details
- `docs/pipeline.md`: stage data flow
- `docs/extraction.md`: extraction placeholder plan
- `docs/completion.md`: completion placeholder plan

## Default Data Contract

```text
/mnt/ainvest_content/v1/content_batch_*.ndjson
/mnt/ainvest_content/v3/v1/cleaned_batch*.jsonl
/mnt/ainvest_content/v3/v1/state/dedup.db
/mnt/ainvest_content/v3/v1/extracted/event_batch*.jsonl
/mnt/ainvest_content/v3/v1/completed/completed_batch*.jsonl
/mnt/ainvest_content/v3/v1/reports/
```

Important cleaning defaults are in `src/config.py`:

- `INPUT_DIR = "/mnt/ainvest_content/v1"`
- `CLEANED_DIR = "/mnt/ainvest_content/v3/v1"`
- active runtime state defaults to `/tmp/aime_event/v1/state` to avoid Ceph random IO
- final state is copied back to `/mnt/ainvest_content/v3/v1/state`
- `PART_SIZE = 200_000`
- output is sorted by `published_at ASC`, then stable tie-breakers

## CLI

Preferred stage commands:

```bash
python -m src.main clean fresh --workers 48 --no-near-dedup
python -m src.main clean export
python -m src.main extract
python -m src.main complete
python -m src.main run-all
```

Legacy cleaning commands remain compatible:

```bash
python -m src.main fresh
python -m src.main export
python -m src.main run
```

`extract`, `complete`, and `run-all` exist as routing entrypoints, but extraction and completion are not implemented yet.

## Verification

Use a local virtualenv when available:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m compileall -q src
.venv/bin/python -m pytest -q
.venv/bin/python -m src.main --help
.venv/bin/python -m src.main clean --help
```

Current expected test status on this branch: all tests pass.

## Operational Constraints

- Do not run the full cleaning pipeline unless explicitly asked.
- If testing cleaning behavior, use tiny temporary inputs and override all output/state paths, including `--final-state-dir`, so tests do not write to `/mnt`.
- Do not use git on the remote machine. Make commits and pushes from the local clone.
- Remote deployment/code, when needed, is under `/mnt/ainvest_content/v1/code/aime_event`; results belong under `/mnt/ainvest_content/v3/v1`.
- Do not commit SSH keys, tokens, host-specific credentials, or generated large data outputs.
- Preserve existing user changes if the worktree is dirty; do not reset or revert unrelated files.

## Notes for Future Refactors

- Keep `src/config.py` as a single config file until extraction/completion have real settings.
- Do not introduce a `src/aime_event/` package layer unless packaging requirements change.
- Do not add an `orchestration/` package; if `run-all` grows, keep orchestration in `src/cli/main.py` unless it becomes materially complex.
- Prefer moving reusable mechanics into `src/common/` only when at least two stages need them.
