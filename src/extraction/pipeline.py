from __future__ import annotations

import os
import re
import shutil
from pathlib import Path
from typing import Any, Protocol

import orjson

from src.common.logging import ProgressLogger, log
from src.extraction.client import ChatCompletionClient, call_with_retries, load_env_file
from src.extraction.models import ExtractionSettings
from src.extraction.output import error_record_to_json, extraction_record_to_json
from src.extraction.prompt import SYSTEM_PROMPT, build_user_prompt


class ExtractionClient(Protocol):
    model: str

    def complete_json(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
        max_tokens: int,
    ) -> dict[str, Any]:
        ...


def run_pipeline(settings: ExtractionSettings | None = None, client: ExtractionClient | None = None) -> dict[str, int]:
    settings = settings or ExtractionSettings()
    load_env_file(settings.env_file)
    resolved_client = client or _build_client(settings)
    input_files = discover_cleaned_files(settings.input_path)
    if not input_files:
        raise SystemExit(f"未找到抽取输入文件：{settings.input_path}")

    log(
        f"Extraction: start files={len(input_files)} input={settings.input_path} "
        f"output={settings.output_dir} model={resolved_client.model} limit={settings.limit or 'none'}"
    )
    tmp_dir = _tmp_output_dir(settings.output_dir)
    stats = {"input": 0, "events": 0, "errors": 0}
    progress = ProgressLogger("Extraction", settings.log_every_rows, 15)

    try:
        for batch_index, input_file in enumerate(input_files, start=1):
            output_file = tmp_dir / f"event_batch{batch_index}.jsonl"
            log(f"Extraction: file {batch_index}/{len(input_files)} {input_file.name} -> {output_file.name}")
            with input_file.open("rb") as reader, output_file.open("wb") as writer:
                for source_line, raw_line in enumerate(reader, start=1):
                    if settings.limit is not None and stats["input"] >= settings.limit:
                        break
                    line = raw_line.strip()
                    if not line:
                        continue
                    record = orjson.loads(line)
                    result = extract_one(record, input_file.name, source_line, resolved_client, settings)
                    stats["input"] += 1
                    stats["events"] += len(result.get("events") or [])
                    if result.get("error"):
                        stats["errors"] += 1
                    writer.write(orjson.dumps(result) + b"\n")
                    progress.maybe(
                        stats["input"],
                        f"events={stats['events']:,} errors={stats['errors']:,}",
                    )
            if settings.limit is not None and stats["input"] >= settings.limit:
                break
        _replace_output_dir(tmp_dir, settings.output_dir)
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise

    log(f"Extraction: done input={stats['input']:,} events={stats['events']:,} errors={stats['errors']:,}")
    return stats


def extract_one(
    record: dict[str, Any],
    source_file: str,
    source_line: int,
    client: ExtractionClient,
    settings: ExtractionSettings,
) -> dict[str, Any]:
    try:
        response = call_with_retries(
            lambda: client.complete_json(
                system_prompt=SYSTEM_PROMPT,
                user_prompt=build_user_prompt(record, max_body_chars=settings.max_body_chars),
                temperature=settings.temperature,
                max_tokens=settings.max_tokens,
            ),
            max_retries=settings.max_retries,
        )
        return extraction_record_to_json(
            source=record,
            source_file=source_file,
            source_line=source_line,
            model=client.model,
            response=response,
            include_raw_response=settings.include_raw_response,
        )
    except Exception as exc:
        return error_record_to_json(
            source=record,
            source_file=source_file,
            source_line=source_line,
            model=client.model,
            error=exc,
        )


def discover_cleaned_files(input_path: Path) -> list[Path]:
    if input_path.is_file():
        return [input_path]
    return sorted(input_path.glob("cleaned_batch*.jsonl"), key=_cleaned_sort_key)


def _build_client(settings: ExtractionSettings) -> ChatCompletionClient:
    base_url = settings.base_url or os.environ.get("AIME_EXTRACTION_BASE_URL") or os.environ.get("OPENAI_BASE_URL")
    api_key = settings.api_key or os.environ.get("AIME_EXTRACTION_API_KEY") or os.environ.get("OPENAI_API_KEY")
    model = settings.model or os.environ.get("AIME_EXTRACTION_MODEL") or os.environ.get("OPENAI_MODEL")
    missing = []
    if not base_url:
        missing.append("OPENAI_BASE_URL/AIME_EXTRACTION_BASE_URL")
    if not api_key:
        missing.append("OPENAI_API_KEY/AIME_EXTRACTION_API_KEY")
    if not model:
        missing.append("OPENAI_MODEL/AIME_EXTRACTION_MODEL")
    if missing:
        raise SystemExit(f"缺少 API 配置：{', '.join(missing)}。请填写 .env 或传 CLI 参数。")
    return ChatCompletionClient(
        base_url=base_url,
        api_key=api_key,
        model=model,
        timeout_seconds=settings.timeout_seconds,
    )


def _cleaned_sort_key(path: Path) -> tuple[int, int | str]:
    match = re.search(r"cleaned_batch(\d+)\.jsonl$", path.name)
    if match:
        return (0, int(match.group(1)))
    return (1, path.name)


def _tmp_output_dir(final_dir: Path) -> Path:
    final_dir.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = final_dir.parent / f".{final_dir.name}.tmp"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True)
    return tmp_dir


def _replace_output_dir(tmp_dir: Path, final_dir: Path) -> None:
    backup = final_dir.parent / f".{final_dir.name}.bak"
    if backup.exists():
        shutil.rmtree(backup)
    if final_dir.exists():
        final_dir.rename(backup)
    tmp_dir.rename(final_dir)
    if backup.exists():
        shutil.rmtree(backup)
