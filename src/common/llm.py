"""DeepSeek(OpenAI 兼容) LLM 客户端: JSON 输出 + 并发 + 逐条落盘断点续跑."""
from __future__ import annotations

import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from dotenv import dotenv_values

_ENV = dotenv_values(Path(__file__).resolve().parents[2] / ".env")
BASE_URL = _ENV.get("OPENAI_BASE_URL", "https://api.deepseek.com")
API_KEY = _ENV.get("OPENAI_API_KEY", "")
MODEL = _ENV.get("OPENAI_MODEL", "deepseek-v4-flash")
MODEL_STRUCTURE = _ENV.get("OPENAI_MODEL_STRUCTURE", "") or MODEL

_client = None
JSON_RE = re.compile(r"\{.*\}", re.S)


def model_for(purpose: str) -> str:
    return MODEL_STRUCTURE if purpose == "structure" else MODEL


def _get_client():
    global _client
    if _client is None:
        from openai import OpenAI
        _client = OpenAI(base_url=BASE_URL, api_key=API_KEY, timeout=120)
    return _client


def parse_json_text(text: str) -> dict:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        m = JSON_RE.search(text)
        if m:
            try:
                return json.loads(m.group(0))
            except json.JSONDecodeError:
                pass
        raise ValueError(f"无法从文本解析 JSON: {text[:200]}")


def chat_json(user: str, system: str = "", model: str | None = None,
              temperature: float = 0.2, max_retries: int = 4) -> dict:
    msgs = ([{"role": "system", "content": system}] if system else []) + \
           [{"role": "user", "content": user}]
    last_err: Exception | None = None
    for attempt in range(max_retries):
        try:
            resp = _get_client().chat.completions.create(
                model=model or MODEL, messages=msgs, temperature=temperature,
                response_format={"type": "json_object"},
            )
            text = resp.choices[0].message.content or ""
            return parse_json_text(text)
        except Exception as e:  # 限流/网络/解析失败统一退避重试
            last_err = e
            time.sleep(min(2 ** attempt * 2, 30))
    raise RuntimeError(f"LLM 调用失败(重试{max_retries}次): {last_err}")


def run_checkpointed(items: list[dict], key_fn, work_fn, out_path: str,
                     workers: int = 24, desc: str = "llm") -> dict[str, dict]:
    """并发跑 work_fn(item)->dict, 结果按 key 逐行落盘 out_path, 重跑自动跳过已完成."""
    done: dict[str, dict] = {}
    if os.path.exists(out_path):
        with open(out_path) as fh:
            for line in fh:
                try:
                    r = json.loads(line)
                    done[r["_key"]] = r
                except Exception:
                    pass
    todo = [it for it in items if key_fn(it) not in done]
    print(f"[{desc}] 共 {len(items)}, 已完成 {len(done)}, 待跑 {len(todo)}", flush=True)
    if not todo:
        return done
    lock = threading.Lock()
    t0 = time.time()
    with open(out_path, "a") as out, ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(work_fn, it): it for it in todo}
        n_ok = n_err = 0
        for fut in as_completed(futs):
            it = futs[fut]
            key = key_fn(it)
            try:
                r = fut.result()
                r["_key"] = key
                n_ok += 1
            except Exception as e:
                r = {"_key": key, "_error": str(e)[:500]}
                n_err += 1
            with lock:
                out.write(json.dumps(r, ensure_ascii=False) + "\n")
                out.flush()
                done[key] = r
            if (n_ok + n_err) % 50 == 0:
                print(f"[{desc}] {n_ok+n_err}/{len(todo)} ok={n_ok} err={n_err} "
                      f"({time.time()-t0:.0f}s)", flush=True)
    print(f"[{desc}] 完成: ok={n_ok} err={n_err}", flush=True)
    return done
