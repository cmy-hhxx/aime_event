# eventpack 融入 aime_event 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 event_dataset 六阶段流水线替换式融入 aime_event 的 extraction/completion 阶段，select 改为阈值抽取，产出 v4 事件训练包。

**Architecture:** aime_event 三阶段语义变为 clean → extract(index/cluster/select/structure) → complete(fetch/label/assemble)。新模块从 `~/同花顺实习/projects/event_dataset/src/` 移植（该目录与服务器 `/mnt/ainvest_content/v1/code/event_dataset` 一致，为移植唯一来源），select 按已批准 spec 重写为阈值抽取。旧逐条抽取代码删除。

**Tech Stack:** Python≥3.10(服务器)/3.9兼容(本地 mac 仅跑单测), duckdb, pyarrow, orjson, rapidfuzz, yfinance, openai SDK(DeepSeek), pytest。

**Spec:** aime_event 仓库 `docs/superpowers/specs/2026-07-06-eventpack-integration-design.md`（分支 feat/event-dataset）

## Global Constraints

- **ceph-fuse 并发红线**：任何扫原始 jsonl 的步骤 workers 默认 6，禁超 10（实测 12 拥塞、48 卡死）
- 语料输入：v1=`/mnt/ainvest_content/v3/v1`，v2=`/mnt/ainvest_content/v3/v2`；输出根=`/mnt/ainvest_content/v3/event_dataset`
- 模型：triage=`deepseek-v4-flash`(即 .env OPENAI_MODEL)，structure=`.env OPENAI_MODEL_STRUCTURE=deepseek-v4-pro`，缺省回退 OPENAI_MODEL
- 抽取无数量/时代配额：规则送审门 + LLM `is_valid_event && significance>=3`；质量护栏仅 (类型,日期,主体) 去重 + 单 symbol 上限 12（0=关闭）
- 增强层字段（分时/弱关联链/混杂审计）留空占位 + status 标记
- 所有源码 `from __future__ import annotations`（本地 3.9 跑单测）
- 单测禁止网络与真实数据路径；LLM 网络调用不测，只测纯逻辑
- 提交信息结尾带 `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`

---

### Task 1: 本地克隆与双端 git 准备

**Files:**
- Create: `~/同花顺实习/projects/aime_event_dev/`（本地克隆，后续所有任务的工作目录）
- Create(克隆内): `docs/superpowers/plans/2026-07-06-eventpack-integration.md`（本文件移入）

**Interfaces:**
- Produces: 本地可运行 pytest 的克隆仓库（分支 feat/event-dataset）；服务器端可接收 push

- [ ] **Step 1: 服务器端允许推送**

```bash
ssh pdf2json 'cd /mnt/ainvest_content/v1/code/aime_event && git checkout refactor/pipeline-stages && git config receive.denyCurrentBranch updateInstead'
```
Expected: `Switched to branch 'refactor/pipeline-stages'`

- [ ] **Step 2: 本地克隆并切分支**

```bash
cd ~/同花顺实习/projects && git clone pdf2json:/mnt/ainvest_content/v1/code/aime_event aime_event_dev
cd aime_event_dev && git checkout feat/event-dataset && git log --oneline -2
```
Expected: 首行为 `c1a3d58 docs: eventpack 融入设计文档...`

- [ ] **Step 3: 本地 venv + 现有依赖 + pytest 基线**

```bash
cd ~/同花顺实习/projects/aime_event_dev
python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt
.venv/bin/python -m pytest -q
```
Expected: 全部 PASS（基线绿）。若有环境性失败记录下来，不属于本计划修复范围。

- [ ] **Step 4: 计划文档入库并提交**

```bash
cp ~/同花顺实习/projects/event_dataset/docs-plan-2026-07-06-eventpack-integration.md docs/superpowers/plans/2026-07-06-eventpack-integration.md
git add docs/superpowers/plans/ && git commit -m "docs: eventpack 融入实施计划

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: requirements 扩充 + config EVENTPACK 区段

**Files:**
- Modify: `requirements.txt`
- Modify: `src/config.py`（"用户配置区"末尾追加）
- Test: `tests/common/test_event_config.py`

**Interfaces:**
- Produces: `src.config` 新增常量（后续所有任务引用，命名以 EVENT_ 为前缀）：
  `EVENT_V1_DIR, EVENT_V2_DIR, EVENT_OUT_ROOT, EVENT_INDEX_DIR, EVENT_CANDIDATE_DIR, EVENT_SELECTED_DIR, EVENT_STRUCTURED_DIR, EVENT_MARKET_DIR, EVENT_FINAL_DIR, EVENT_REPORT_DIR`(str)；
  `EVENT_INDEX_WORKERS=6, EVENT_TITLE_MAX_CHARS=400`(int)；
  `EVENT_ERA_SPLIT="2023-07-01"`；
  `EVENT_RECENT_MIN_ARTICLES=5, EVENT_RECENT_ALT_MIN_ARTICLES=3, EVENT_EARLY_MIN_ARTICLES=2, EVENT_MIN_SIGNIFICANCE=3, EVENT_PER_SYMBOL_CAP=12`(int)；
  `EVENT_FETCH_START="2013-06-01", EVENT_FETCH_END="2026-07-01"`

- [ ] **Step 1: 写失败测试**

```python
# tests/common/test_event_config.py
from src import config


def test_eventpack_paths_exist():
    assert config.EVENT_V1_DIR == "/mnt/ainvest_content/v3/v1"
    assert config.EVENT_INDEX_DIR.startswith(config.EVENT_OUT_ROOT)


def test_eventpack_thresholds():
    assert config.EVENT_INDEX_WORKERS <= 10  # ceph-fuse 红线
    assert config.EVENT_MIN_SIGNIFICANCE == 3
    assert config.EVENT_EARLY_MIN_ARTICLES <= config.EVENT_RECENT_MIN_ARTICLES
```

- [ ] **Step 2: 跑测试确认失败**

Run: `.venv/bin/python -m pytest tests/common/test_event_config.py -q`
Expected: FAIL `AttributeError: ... EVENT_V1_DIR`

- [ ] **Step 3: 实现**

`requirements.txt` 追加（保留原有行）：

```text
pyarrow>=15.0
duckdb>=1.0
pandas>=2.0
openai>=1.40
python-dotenv>=1.0
yfinance>=0.2.40
```

`src/config.py` 用户配置区末尾追加：

```python
# --- eventpack: 事件训练包流水线 (extract/complete 阶段) ---
EVENT_V1_DIR = "/mnt/ainvest_content/v3/v1"  # 清洗后新闻语料(仅精确去重)
EVENT_V2_DIR = "/mnt/ainvest_content/v3/v2"  # 研报/电话会段落
EVENT_OUT_ROOT = "/mnt/ainvest_content/v3/event_dataset"
EVENT_INDEX_DIR = f"{EVENT_OUT_ROOT}/index"
EVENT_CANDIDATE_DIR = f"{EVENT_OUT_ROOT}/candidates"
EVENT_SELECTED_DIR = f"{EVENT_OUT_ROOT}/selected"
EVENT_STRUCTURED_DIR = f"{EVENT_OUT_ROOT}/structured"
EVENT_MARKET_DIR = f"{EVENT_OUT_ROOT}/market"
EVENT_FINAL_DIR = f"{EVENT_OUT_ROOT}/final"
EVENT_REPORT_DIR = f"{EVENT_OUT_ROOT}/reports"
# ceph-fuse 并发红线: 实测 48 卡死 / 12 拥塞 / 6 正常, 禁超 10
EVENT_INDEX_WORKERS = 6
EVENT_TITLE_MAX_CHARS = 400
# 阈值抽取(无数量配额): 规则送审门 + LLM significance 门
EVENT_ERA_SPLIT = "2023-07-01"
EVENT_RECENT_MIN_ARTICLES = 5      # 近三年送审: n_articles>=5
EVENT_RECENT_ALT_MIN_ARTICLES = 3  # 或 n_articles>=3 且有研报佐证
EVENT_EARLY_MIN_ARTICLES = 2       # 早年送审: n_articles>=2
EVENT_MIN_SIGNIFICANCE = 3         # LLM 入选门
EVENT_PER_SYMBOL_CAP = 12          # 单 symbol 事件上限, 0=关闭
EVENT_FETCH_START = "2013-06-01"   # yfinance 拉取窗
EVENT_FETCH_END = "2026-07-01"
```

- [ ] **Step 4: 跑测试确认通过**

Run: `.venv/bin/pip install -q -r requirements.txt && .venv/bin/python -m pytest tests/common/test_event_config.py -q`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add requirements.txt src/config.py tests/common/test_event_config.py
git commit -m "feat(config): eventpack 配置区段与依赖

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: src/common/llm.py 并发 LLM 客户端

**Files:**
- Create: `src/common/llm.py`
- Test: `tests/common/test_llm.py`

**Interfaces:**
- Consumes: 仓库根 `.env`（OPENAI_BASE_URL/OPENAI_API_KEY/OPENAI_MODEL/OPENAI_MODEL_STRUCTURE）
- Produces:
  - `parse_json_text(text: str) -> dict`（JSON 解析，带 `{...}` 提取回退，失败抛 ValueError）
  - `chat_json(user: str, system: str = "", model: str | None = None, temperature: float = 0.2, max_retries: int = 4) -> dict`
  - `model_for(purpose: str) -> str`：`"triage"`→OPENAI_MODEL；`"structure"`→OPENAI_MODEL_STRUCTURE 缺省回退 OPENAI_MODEL
  - `run_checkpointed(items: list[dict], key_fn, work_fn, out_path: str, workers: int = 24, desc: str = "llm") -> dict[str, dict]`：并发执行、逐条 JSONL 落盘（含 `_key`，异常记 `_error`）、重跑按 `_key` 跳过

- [ ] **Step 1: 写失败测试**

```python
# tests/common/test_llm.py
import json

import pytest

from src.common import llm


def test_parse_json_text_plain():
    assert llm.parse_json_text('{"a": 1}') == {"a": 1}


def test_parse_json_text_wrapped():
    assert llm.parse_json_text('前缀```json\n{"a": {"b": 2}}\n```后缀') == {"a": {"b": 2}}


def test_parse_json_text_invalid():
    with pytest.raises(ValueError):
        llm.parse_json_text("not json at all")


def test_run_checkpointed_resume(tmp_path):
    out = str(tmp_path / "ck.jsonl")
    items = [{"k": "a"}, {"k": "b"}, {"k": "c"}]
    calls = []

    def work(it):
        calls.append(it["k"])
        if it["k"] == "b":
            raise RuntimeError("boom")
        return {"v": it["k"].upper()}

    r1 = llm.run_checkpointed(items, lambda it: it["k"], work, out, workers=2)
    assert r1["a"]["v"] == "A" and "_error" in r1["b"]
    # 重跑: 已成功和已失败的都不再调 work(失败记录也落盘, 重试需删文件)
    calls.clear()
    r2 = llm.run_checkpointed(items, lambda it: it["k"], work, out, workers=2)
    assert calls == [] and len(r2) == 3
    with open(out) as fh:
        assert len(fh.readlines()) == 3
```

- [ ] **Step 2: 跑测试确认失败**

Run: `.venv/bin/python -m pytest tests/common/test_llm.py -q`
Expected: FAIL `ModuleNotFoundError: No module named 'src.common.llm'`

- [ ] **Step 3: 实现**

以 `~/同花顺实习/projects/event_dataset/src/llm.py` 为底本移植，修改点：

1. 头部替换为（.env 从仓库根读，OpenAI client 惰性初始化避免 import 时要求 key）：

```python
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
```

2. `chat_json` 与底本一致，但内部用 `_get_client()` 和 `parse_json_text`（重试退避逻辑不变）。
3. `run_checkpointed` 原样保留（签名见 Interfaces）。

- [ ] **Step 4: 跑测试确认通过**

Run: `.venv/bin/python -m pytest tests/common/test_llm.py -q`
Expected: 4 passed

- [ ] **Step 5: 提交**

```bash
git add src/common/llm.py tests/common/test_llm.py
git commit -m "feat(common): 并发 LLM 客户端(断点续跑/双模型)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: src/extraction/index.py（建索引，移植）

**Files:**
- Create: `src/extraction/index.py`
- Test: `tests/extraction/test_index.py`

**Interfaces:**
- Consumes: `src.config` 的 EVENT_* 路径常量
- Produces:
  - `norm_date(ts: str | None) -> str`（越界→""）、`v1_extract(rec: dict) -> dict`、`_as_dict(v) -> dict`
  - `run(args: argparse.Namespace) -> None`，args 含 `limit:int, workers:int, fresh:bool`
  - 产物：`EVENT_INDEX_DIR/v1_*.parquet, v2_*.parquet, sympairs_*.parquet`；`EVENT_REPORT_DIR/stage_a_summary.json`

- [ ] **Step 1: 写失败测试**

```python
# tests/extraction/test_index.py
from src.extraction import index


def test_norm_date_bounds():
    assert index.norm_date("2024-03-18T11:00:00Z") == "2024-03-18"
    assert index.norm_date("1970-01-01T00:00:00Z") == ""   # 脏时间戳
    assert index.norm_date("2027-06-29T00:00:00Z") == ""   # 未来脏时间戳
    assert index.norm_date(None) == ""


def test_as_dict_variants():
    assert index._as_dict({"a": 1}) == {"a": 1}
    assert index._as_dict("{'a': 1}") == {"a": 1}   # v2 字符串化 dict
    assert index._as_dict("broken{") == {}
    assert index._as_dict(None) == {}


def test_v1_extract_notice():
    rec = {"id": "x1", "content_type": "US_NOTICE", "title": "Form 6-K",
           "published_at": "2014-06-03T11:13:14Z",
           "notice": {"filing_type": "009", "declare_date": "2014-05-19"},
           "dedup": {"key": "notice:000119"}}
    row = index.v1_extract(rec)
    assert row["accession"] == "000119" and row["pub_date"] == "2014-06-03"
    assert row["symbols"] == "" and row["body_len"] == 0
```

- [ ] **Step 2: 跑测试确认失败**

Run: `.venv/bin/python -m pytest tests/extraction/test_index.py -q`
Expected: FAIL `ModuleNotFoundError`

- [ ] **Step 3: 移植实现**

复制 `~/同花顺实习/projects/event_dataset/src/stage_a_index.py` → `src/extraction/index.py`，修改点：

1. 删除 `sys.path.insert` 与 `import config`，改 `from src import config`；全文 `config.V1_DIR`→`config.EVENT_V1_DIR`、`config.V2_DIR`→`config.EVENT_V2_DIR`、`config.INDEX_DIR`→`config.EVENT_INDEX_DIR`、`config.REPORT_DIR`→`config.EVENT_REPORT_DIR`、`config.TITLE_MAX_CHARS`→`config.EVENT_TITLE_MAX_CHARS`、`config.INDEX_WORKERS`→`config.EVENT_INDEX_WORKERS`
2. `main()` 改名 `run(args)`：删掉函数内 argparse 构建，直接使用传入 args（保留 --limit/--workers/--fresh 语义与断点续跑/原子写逻辑）
3. 文件尾 `if __name__ == "__main__":` 块删除（统一走 CLI）

- [ ] **Step 4: 跑测试确认通过**

Run: `.venv/bin/python -m pytest tests/extraction/test_index.py -q`
Expected: 3 passed

- [ ] **Step 5: 提交**

```bash
git add src/extraction/index.py tests/extraction/test_index.py
git commit -m "feat(extract): index 建索引步(断点续跑/原子写)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: src/extraction/cluster.py（聚类，移植）

**Files:**
- Create: `src/extraction/cluster.py`
- Test: `tests/extraction/test_cluster.py`

**Interfaces:**
- Consumes: `EVENT_INDEX_DIR` 下 parquet；`EVENT_CANDIDATE_DIR`
- Produces:
  - `tokens(title: str) -> frozenset`、`DSU`、`cluster_bucket(items: list[dict]) -> list[list[int]]`（items 按 pub_date 升序，元素含 title/pub_date 键）
  - `run(args)`，args 含 `workers:int, shards:int`
  - 产物：`EVENT_CANDIDATE_DIR/clusters.parquet`（列含 event_id,n_articles,n_sources,n_content_types,first_date,last_date,peak_date,n_high,track,rep_title,all_symbols,n_v2_reactions）与 `members.parquet`

- [ ] **Step 1: 写失败测试**

```python
# tests/extraction/test_cluster.py
from src.extraction import cluster


def test_tokens_stopwords():
    t = cluster.tokens("NVIDIA stock surges after the new AI chip report")
    assert "nvidia" in t and "ai" in t and "the" not in t and "stock" not in t


def _item(title, date):
    return {"title": title, "pub_date": date}


def test_cluster_bucket_same_story():
    items = [
        _item("Frasers Group launches takeover bid for Accent Group", "2026-06-15"),
        _item("Accent Group jumps on Frasers takeover bid", "2026-06-16"),
        _item("Fed holds interest rates steady in June meeting", "2026-06-16"),
    ]
    groups = sorted(cluster.cluster_bucket(items), key=len, reverse=True)
    assert len(groups) == 2 and sorted(groups[0]) == [0, 1]


def test_cluster_bucket_window_split():
    items = [
        _item("Acme Corp announces quarterly dividend increase", "2026-01-05"),
        _item("Acme Corp announces quarterly dividend increase", "2026-03-01"),
    ]
    assert len(cluster.cluster_bucket(items)) == 2  # 超出 3 天窗不连边
```

- [ ] **Step 2: 跑测试确认失败**

Run: `.venv/bin/python -m pytest tests/extraction/test_cluster.py -q`
Expected: FAIL `ModuleNotFoundError`

- [ ] **Step 3: 移植实现**

复制 `~/同花顺实习/projects/event_dataset/src/stage_b_candidates.py` → `src/extraction/cluster.py`，修改点：

1. import 与路径常量替换（同 Task 4 第 1 点；`config.CANDIDATE_DIR`→`config.EVENT_CANDIDATE_DIR`）
2. `main()` → `run(args)`（argparse 移除，`__main__` 块删除）
3. 其余逻辑（过滤规则/宏观主题正则/分桶聚类/跨桶合并/v2 佐证 join）原样保留

- [ ] **Step 4: 跑测试确认通过**

Run: `.venv/bin/python -m pytest tests/extraction/test_cluster.py -q`
Expected: 3 passed

- [ ] **Step 5: 提交**

```bash
git add src/extraction/cluster.py tests/extraction/test_cluster.py
git commit -m "feat(extract): cluster 事件候选聚类步

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: src/extraction/prompts.py + select.py（阈值抽取，重写）

**Files:**
- Create: `src/extraction/prompts.py`（triage 与 structure 两组 prompt 常量）
- Create: `src/extraction/select.py`（全新实现）
- Test: `tests/extraction/test_select.py`

**Interfaces:**
- Consumes: `clusters.parquet`/`members.parquet`；`src.common.llm.run_checkpointed/chat_json/model_for`；config 阈值常量
- Produces:
  - `prompts.TRIAGE_SYSTEM: str`、`prompts.TRIAGE_USER_TMPL: str`（含 {peak_date}{first_date}{last_date}{n_articles}{n_sources}{n_high}{n_v2}{symbols}{titles}{types} 槽位）；`prompts.STRUCTURE_SYSTEM/STRUCTURE_USER_TMPL`（Task 7 用，槽位 {event_date}{event_type}{event_subject}{primary_symbols}{title_cn}{articles}）
  - `select.gate_where(era_split: str, recent_min: int, recent_alt_min: int, early_min: int) -> str`（返回 SQL WHERE 片段）
  - `select.final_select(cands: list[dict], triage: dict[str, dict], min_significance: int, per_symbol_cap: int) -> list[dict]`
  - `select.run(args)`，args 含 `sweep:bool, triage_workers:int, dry_run:bool`
  - 产物：`EVENT_SELECTED_DIR/triage.jsonl`（断点续跑）、`selected_events.jsonl`（行含 event_id,event_date,event_type,event_family,event_subject,primary_symbols,significance,title_cn,peak_date,n_articles,n_sources,n_v2_reactions,score,is_recent）

- [ ] **Step 1: 写失败测试**

```python
# tests/extraction/test_select.py
from src.extraction import select


def _cand(eid, sym="NVDA", score=5.0, recent=True):
    return {"event_id": eid, "peak_date": "2025-01-10" if recent else "2016-05-01",
            "is_recent": recent, "score": score, "all_symbols": sym,
            "n_articles": 6, "n_sources": 3, "n_high": 1, "n_v2_reactions": 0,
            "first_date": "2025-01-09", "last_date": "2025-01-11",
            "rep_title": "t", "track": "company", "n_content_types": 2, "month": "2025-01"}


def _triage(sig, etype="earnings_release", sym="NVDA", date="2025-01-10"):
    return {"is_valid_event": True, "significance": sig, "event_type": etype,
            "event_family": "f", "event_subject": "S", "primary_symbols": [sym],
            "event_date": date, "title_cn": "标题"}


def test_final_select_threshold_no_quota():
    cands = [_cand(f"E{i}", sym=f"S{i}") for i in range(50)]
    triage = {f"E{i}": _triage(3, sym=f"S{i}", date=f"2025-01-{i%27+1:02d}") for i in range(50)}
    picked = select.final_select(cands, triage, min_significance=3, per_symbol_cap=12)
    assert len(picked) == 50  # 无数量配额: 全部过阈值即全部入选


def test_final_select_significance_gate():
    cands = [_cand("E1"), _cand("E2", sym="AMD")]
    triage = {"E1": _triage(3), "E2": _triage(2, sym="AMD")}
    assert [p["event_id"] for p in select.final_select(cands, triage, 3, 12)] == ["E1"]


def test_final_select_dedupe_same_event():
    cands = [_cand("E1", score=9.0), _cand("E2", score=1.0)]
    triage = {"E1": _triage(3), "E2": _triage(3)}  # 同类型同日同主体
    assert len(select.final_select(cands, triage, 3, 12)) == 1


def test_final_select_per_symbol_cap():
    cands = [_cand(f"E{i}") for i in range(15)]
    triage = {f"E{i}": _triage(3, date=f"2025-01-{i+1:02d}") for i in range(15)}
    assert len(select.final_select(cands, triage, 3, 12)) == 12
    assert len(select.final_select(cands, triage, 3, 0)) == 15  # 0=关闭


def test_gate_where_eras():
    w = select.gate_where("2023-07-01", 5, 3, 2)
    assert "2023-07-01" in w and "n_articles >= 5" in w and "n_articles >= 2" in w
```

- [ ] **Step 2: 跑测试确认失败**

Run: `.venv/bin/python -m pytest tests/extraction/test_select.py -q`
Expected: FAIL `ModuleNotFoundError`

- [ ] **Step 3: 实现 prompts.py**

从 `~/同花顺实习/projects/event_dataset/src/stage_c_select.py` 取 `TRIAGE_SYSTEM/TRIAGE_USER_TMPL/EVENT_TYPES`，从 `stage_d_structure.py` 取 `SYSTEM/USER_TMPL`（更名 `STRUCTURE_SYSTEM/STRUCTURE_USER_TMPL`），集中放入 `src/extraction/prompts.py`，内容原样不改。

- [ ] **Step 4: 实现 select.py**

```python
"""extract select: 候选簇阈值送审 -> LLM triage -> 阈值入选(无数量配额).

  送审门(规则): recent n_articles>=EVENT_RECENT_MIN_ARTICLES
               或 (>=EVENT_RECENT_ALT_MIN_ARTICLES 且 n_v2_reactions>=1);
               early n_articles>=EVENT_EARLY_MIN_ARTICLES
  入选门(LLM):  is_valid_event 且 significance>=EVENT_MIN_SIGNIFICANCE
  质量护栏:    (event_type,event_date,主体) 去重; 单 symbol 上限(0=关闭)
  --sweep:     不调 API, 输出阈值->送审量对照表供人工定阈值
"""
from __future__ import annotations

import json
import os
import time
from collections import Counter, defaultdict

import duckdb
import pyarrow as pa

from src import config
from src.common import llm
from src.extraction import prompts


def gate_where(era_split: str, recent_min: int, recent_alt_min: int, early_min: int) -> str:
    return (f"(peak_date >= '{era_split}' AND (n_articles >= {recent_min} "
            f"OR (n_articles >= {recent_alt_min} AND n_v2_reactions >= 1))) "
            f"OR (peak_date < '{era_split}' AND n_articles >= {early_min})")


def build_candidates(con, recent_min: int, recent_alt_min: int, early_min: int) -> list[dict]:
    where = gate_where(config.EVENT_ERA_SPLIT, recent_min, recent_alt_min, early_min)
    return con.execute(f"""
      SELECT event_id, peak_date, first_date, last_date, n_articles, n_sources,
             n_content_types, n_high, n_v2_reactions, track, rep_title, all_symbols,
             substr(peak_date, 1, 7) AS month,
             peak_date >= '{config.EVENT_ERA_SPLIT}' AS is_recent,
             2.0 * ln(1 + n_articles) + 0.5 * n_sources + 0.3 * n_content_types
               + 1.5 * ln(1 + n_high) + 2.0 * ln(1 + n_v2_reactions)
               + CASE WHEN track = 'macro' THEN 1.0 ELSE 0 END AS score
      FROM read_parquet('{config.EVENT_CANDIDATE_DIR}/clusters.parquet')
      WHERE {where}
      ORDER BY peak_date
    """).fetch_arrow_table().to_pylist()


def sweep(con) -> None:
    print(f"{'recent_min':>10} {'alt_min':>8} {'early_min':>9} {'recent送审':>10} {'early送审':>9} {'合计':>8}")
    for rm in (3, 4, 5, 6, 8):
        for am in (2, 3):
            if am > rm:
                continue
            for em in (2, 3):
                where = gate_where(config.EVENT_ERA_SPLIT, rm, am, em)
                r, e = con.execute(f"""
                  SELECT sum(CASE WHEN peak_date >= '{config.EVENT_ERA_SPLIT}' THEN 1 ELSE 0 END),
                         sum(CASE WHEN peak_date < '{config.EVENT_ERA_SPLIT}' THEN 1 ELSE 0 END)
                  FROM read_parquet('{config.EVENT_CANDIDATE_DIR}/clusters.parquet')
                  WHERE {where}
                """).fetchone()
                r, e = r or 0, e or 0
                print(f"{rm:>10} {am:>8} {em:>9} {r:>10} {e:>9} {r+e:>8}")
    print("\n当前 config 阈值: recent>=%d 或(>=%d 且有研报佐证), early>=%d"
          % (config.EVENT_RECENT_MIN_ARTICLES, config.EVENT_RECENT_ALT_MIN_ARTICLES,
             config.EVENT_EARLY_MIN_ARTICLES))


def fetch_rep_titles(con, event_ids: list[str]) -> dict[str, list]:
    con.register("wanted", pa.table({"event_id": event_ids}))
    rows = con.execute(f"""
      SELECT m.event_id, m.pub_date, m.source_name, m.title
      FROM read_parquet('{config.EVENT_CANDIDATE_DIR}/members.parquet') m
      JOIN wanted USING (event_id)
      QUALIFY row_number() OVER (PARTITION BY m.event_id ORDER BY m.body_len DESC) <= 6
    """).fetchall()
    out = defaultdict(list)
    for eid, d, src, title in rows:
        out[eid].append(f"  [{d}] ({src or '?'}) {title}")
    return out


def triage_one(cand: dict) -> dict:
    user = prompts.TRIAGE_USER_TMPL.format(
        peak_date=cand["peak_date"], first_date=cand["first_date"], last_date=cand["last_date"],
        n_articles=cand["n_articles"], n_sources=cand["n_sources"], n_high=cand["n_high"],
        n_v2=cand["n_v2_reactions"], symbols=cand["all_symbols"] or "(无)",
        titles="\n".join(cand["_titles"]), types=prompts.EVENT_TYPES,
    )
    r = llm.chat_json(user, prompts.TRIAGE_SYSTEM, model=llm.model_for("triage"))
    r["event_id"] = cand["event_id"]
    return r


def final_select(cands: list[dict], triage: dict[str, dict],
                 min_significance: int, per_symbol_cap: int) -> list[dict]:
    valid = []
    for c in cands:
        t = triage.get(c["event_id"]) or {}
        if t.get("_error") or not t.get("is_valid_event"):
            continue
        if int(t.get("significance") or 0) < min_significance:
            continue
        d = t.get("event_date") or c["peak_date"]
        if not (isinstance(d, str) and len(d) == 10 and "2000-01-01" <= d <= "2026-08-01"):
            d = c["peak_date"]
        valid.append({**c, **{k: t.get(k) for k in
                     ("event_type", "event_family", "event_subject", "primary_symbols",
                      "significance", "title_cn")}, "event_date": d})
    # 护栏1: 同 (类型,日期,主体) 去重, 分高者留
    best: dict[tuple, dict] = {}
    for v in sorted(valid, key=lambda x: (-int(x["significance"] or 0), -x["score"])):
        syms = v.get("primary_symbols") or []
        key = (v["event_type"], v["event_date"],
               syms[0] if syms else (v.get("event_subject") or "")[:30].lower())
        best.setdefault(key, v)
    # 护栏2: 单 symbol 上限(0=关闭); 无数量/时代配额
    picked, sym_cnt = [], Counter()
    for v in sorted(best.values(), key=lambda x: (-int(x["significance"] or 0), -x["score"])):
        syms = v.get("primary_symbols") or []
        if per_symbol_cap and syms and sym_cnt[syms[0]] >= per_symbol_cap:
            continue
        picked.append(v)
        if syms:
            sym_cnt[syms[0]] += 1
    return picked


def run(args) -> None:
    os.makedirs(config.EVENT_SELECTED_DIR, exist_ok=True)
    os.makedirs(config.EVENT_REPORT_DIR, exist_ok=True)
    t0 = time.time()
    con = duckdb.connect()
    con.execute("SET threads TO 32")
    if args.sweep:
        sweep(con)
        return
    cands = build_candidates(con, config.EVENT_RECENT_MIN_ARTICLES,
                             config.EVENT_RECENT_ALT_MIN_ARTICLES, config.EVENT_EARLY_MIN_ARTICLES)
    n_recent = sum(1 for c in cands if c["is_recent"])
    print(f"[select] 送审候选 {len(cands)} (recent {n_recent}, early {len(cands)-n_recent})", flush=True)
    if args.dry_run:
        return
    titles = fetch_rep_titles(con, [c["event_id"] for c in cands])
    for c in cands:
        c["_titles"] = titles.get(c["event_id"], [f"  [{c['peak_date']}] {c['rep_title']}"])
    triage = llm.run_checkpointed(cands, lambda c: c["event_id"], triage_one,
                                  f"{config.EVENT_SELECTED_DIR}/triage.jsonl",
                                  workers=args.triage_workers, desc="triage")
    picked = final_select(cands, triage, config.EVENT_MIN_SIGNIFICANCE, config.EVENT_PER_SYMBOL_CAP)
    with open(f"{config.EVENT_SELECTED_DIR}/selected_events.jsonl", "w") as fh:
        for v in picked:
            v.pop("_titles", None)
            fh.write(json.dumps(v, ensure_ascii=False) + "\n")
    summary = {
        "candidates_triaged": len(cands), "selected": len(picked),
        "by_era": dict(Counter("recent" if v["is_recent"] else "early" for v in picked)),
        "by_type": dict(Counter(v["event_type"] for v in picked).most_common()),
        "elapsed_sec": round(time.time() - t0, 1),
    }
    with open(f"{config.EVENT_REPORT_DIR}/stage_select_summary.json", "w") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)
    print(json.dumps(summary, ensure_ascii=False), flush=True)
```

- [ ] **Step 5: 跑测试确认通过**

Run: `.venv/bin/python -m pytest tests/extraction/test_select.py -q`
Expected: 6 passed

- [ ] **Step 6: 提交**

```bash
git add src/extraction/prompts.py src/extraction/select.py tests/extraction/test_select.py
git commit -m "feat(extract): select 阈值抽取(sweep/triage/无配额入选)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: src/extraction/structure.py（LLM 结构化，移植+双模型）

**Files:**
- Create: `src/extraction/structure.py`
- Test: `tests/extraction/test_structure.py`

**Interfaces:**
- Consumes: `selected_events.jsonl`、`members.parquet`、v1 原始文件 seek 读、`prompts.STRUCTURE_*`、`llm.model_for("structure")`
- Produces:
  - `clean_body(body: str) -> str`、`source_rank(name: str) -> int`
  - `run(args)`，args 含 `workers:int, limit:int`
  - 产物：`EVENT_STRUCTURED_DIR/structured.jsonl`（断点续跑；行含 case_id/main_event/relation_rows/event_timestamp_et/confidence/_triage/_source_meta 等）

- [ ] **Step 1: 写失败测试**

```python
# tests/extraction/test_structure.py
from src.extraction import structure


def test_clean_body_strips_html():
    s = structure.clean_body("<p>Hello <a href='x'>world</a> &amp; more</p>")
    assert s == "Hello world & more"


def test_clean_body_truncates():
    assert len(structure.clean_body("x" * 99999)) == structure.MAX_BODY_CHARS


def test_source_rank_prefers_wires():
    assert structure.source_rank("Reuters News") < structure.source_rank("some blog")
    assert structure.source_rank(None) == structure.source_rank("unknown src")
```

- [ ] **Step 2: 跑测试确认失败**

Run: `.venv/bin/python -m pytest tests/extraction/test_structure.py -q`
Expected: FAIL `ModuleNotFoundError`

- [ ] **Step 3: 移植实现**

复制 `~/同花顺实习/projects/event_dataset/src/stage_d_structure.py` → `src/extraction/structure.py`，修改点：

1. import/路径常量替换（同前；`config.SELECTED_DIR/STRUCTURED_DIR/CANDIDATE_DIR/V1_DIR`→对应 EVENT_ 常量）
2. `SYSTEM/USER_TMPL` 常量删除，改 `from src.extraction import prompts`，`structure_one` 内引用 `prompts.STRUCTURE_SYSTEM/STRUCTURE_USER_TMPL`
3. `llm.chat_json(...)` 调用加 `model=llm.model_for("structure")`
4. `from src.common import llm`；`main()`→`run(args)`；`__main__` 块删除

- [ ] **Step 4: 跑测试确认通过**

Run: `.venv/bin/python -m pytest tests/extraction/test_structure.py -q`
Expected: 3 passed

- [ ] **Step 5: 提交**

```bash
git add src/extraction/structure.py tests/extraction/test_structure.py
git commit -m "feat(extract): structure LLM结构化步(deepseek-v4-pro)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: src/completion/market.py（行情打标，移植）

**Files:**
- Create: `src/completion/market.py`
- Test: `tests/completion/test_market.py`

**Interfaces:**
- Consumes: `structured.jsonl`；yfinance（仅 fetch 子步、本地跑）
- Produces:
  - `event_symbols(r: dict) -> list[str]`、`pct(a: float, b: float) -> float`
  - `base_ft_indices(dates: list[str], ev_date: str, bucket: str) -> tuple[int, int]`（新抽出的纯函数：返回 base 下标与 first-tradable 下标；`bucket=="pre_market"` 用 bisect_left-1，否则 bisect_right-1）
  - `run_fetch(args)`（args: batch,pause,structured,outdir）与 `run_label(args)`
  - 产物：`market/prices_daily.parquet`、`market/labels.jsonl`、`reports/stage_label_summary.json`

- [ ] **Step 1: 写失败测试**

```python
# tests/completion/test_market.py
from src.completion import market

DATES = ["2024-03-14", "2024-03-15", "2024-03-18", "2024-03-19", "2024-03-20"]


def test_pct():
    assert market.pct(100.0, 101.5) == 1.5


def test_base_ft_regular_session():
    bi, ft = market.base_ft_indices(DATES, "2024-03-18", "regular")
    assert DATES[bi] == "2024-03-18" and DATES[ft] == "2024-03-19"


def test_base_ft_pre_market():
    bi, ft = market.base_ft_indices(DATES, "2024-03-18", "pre_market")
    assert DATES[bi] == "2024-03-15" and DATES[ft] == "2024-03-18"


def test_base_ft_non_trading_day():
    bi, ft = market.base_ft_indices(DATES, "2024-03-16", "regular")  # 周六
    assert DATES[bi] == "2024-03-15" and DATES[ft] == "2024-03-18"


def test_event_symbols_dedup_and_validate():
    r = {"relation_rows": [{"symbol": "NVDA"}, {"symbol": "nvda!"}, {"symbol": "TSM"}],
         "_triage": {"primary_symbols": ["NVDA"]}}
    assert market.event_symbols(r) == ["NVDA", "TSM"]
```

- [ ] **Step 2: 跑测试确认失败**

Run: `.venv/bin/python -m pytest tests/completion/test_market.py -q`
Expected: FAIL `ModuleNotFoundError`

- [ ] **Step 3: 移植实现**

复制 `~/同花顺实习/projects/event_dataset/src/stage_e_market.py` → `src/completion/market.py`，修改点：

1. import/路径常量替换（`config.MARKET_DIR/STRUCTURED_DIR/REPORT_DIR`→EVENT_ 常量；`FETCH_START/END`→`config.EVENT_FETCH_START/END`）
2. `cmd_label` 内那段 bisect 逻辑抽成模块级纯函数并复用：

```python
def base_ft_indices(dates: list[str], ev_date: str, bucket: str) -> tuple[int, int]:
    """返回 (base_close 下标, first_tradable 下标); pre_market 事件 base 用前一交易日."""
    import bisect
    if bucket == "pre_market":
        bi = bisect.bisect_left(dates, ev_date) - 1
    else:
        bi = bisect.bisect_right(dates, ev_date) - 1
    return bi, bi + 1
```

3. `cmd_fetch`→`run_fetch`、`cmd_label`→`run_label`；模块内 `main()`/argparse/`__main__` 删除；label 汇总文件名改 `stage_label_summary.json`

- [ ] **Step 4: 跑测试确认通过**

Run: `.venv/bin/python -m pytest tests/completion/test_market.py -q`
Expected: 5 passed

- [ ] **Step 5: 提交**

```bash
git add src/completion/market.py tests/completion/test_market.py
git commit -m "feat(complete): market 行情拉取与1D/5D/20D打标

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 9: src/completion/assemble.py（v4 组装，移植）

**Files:**
- Create: `src/completion/assemble.py`
- Test: `tests/completion/test_assemble.py`

**Interfaces:**
- Consumes: `structured.jsonl` + `labels.jsonl`
- Produces:
  - `leakage_scan(facts: list[dict], event_date: str) -> list[str]`
  - `assemble(r: dict, mk: dict) -> tuple[dict | None, list[str]]`
  - `run(args)`，args 含 `max_cases:int`
  - 产物：`EVENT_FINAL_DIR/<CASE_ID>.json`、`manifest.jsonl`、`reports/stage_assemble_summary.json`

- [ ] **Step 1: 写失败测试**

```python
# tests/completion/test_assemble.py
import json

from src.completion import assemble

STRUCTURED = {
    "event_id": "EVT_1", "case_id": "ACME_TEST_20250110", "case_title": "T",
    "display_short_name": "Acme", "event_family": "f", "event_type": "earnings_release",
    "confidence": 0.9,
    "main_event": {"event_subject": "Acme", "event_subject_type": "company",
                   "event_date": "2025-01-10", "official_source_url": None,
                   "facts_publicly_reported": [
                       {"metric": "m1", "value": "v1", "context": "c1"},
                       {"metric": "m2", "value": "v2", "context": "c2"}],
                   "event_influence_channels": [{"channel": "c_a"}, {"channel": "c_b"}]},
    "event_timestamp_et": {"session_bucket": "after_market"},
    "relation_rows": [{"symbol": s, "company": s, "relation_type": "peer",
                       "relation_path": ["Acme", "f", s], "evidence_statement": "影响链: x",
                       "relation_type_cn": "同业/替代品", "impact_path_cn": "x"}
                      for s in ("AAA", "BBB", "CCC", "DDD", "EEE")],
    "_triage": {"event_date": "2025-01-10", "primary_symbols": ["AAA"], "n_articles": 6,
                "n_sources": 3, "n_v2_reactions": 1, "significance": 4, "peak_date": "2025-01-10"},
    "_source_meta": [{"id": "a1", "pub_date": "2025-01-10", "published_at": "x",
                      "source": "Reuters", "title": "t"}],
}


def _label(sym):
    return {"symbol": sym, "base_close_date": "2025-01-10", "base_close": 100.0,
            "first_tradable_session": "2025-01-13",
            "close_to_next_open_gap_pct": 1.0,
            "close_to_close_return_pct": {"1d": 1.0, "5d": 2.0, "20d": 3.0},
            "tradable_open_to_close_return_pct": {"1d": 0.5, "5d": 1.5, "20d": 2.5},
            "horizon_dates": {"1d": "2025-01-13", "5d": "2025-01-17", "20d": "2025-02-07"},
            "is_hidden_from_model_input": True, "cross_section_rank_20d": 1}

MARKET = {"event_id": "EVT_1", "event_date": "2025-01-10", "session_bucket": "after_market",
          "priced_symbols": ["AAA", "BBB", "CCC"], "unpriced_symbols": ["DDD", "EEE"],
          "labels": [_label("AAA"), _label("BBB"), _label("CCC")],
          "market_data_symbols": {s: {"model_input_ohlcv_adjusted_daily":
                                      [{"date": "2025-01-10", "open": 1, "high": 1,
                                        "low": 1, "close": 1, "volume": 1}],
                                      "label_audit_ohlcv_adjusted_daily": []}
                                  for s in ("AAA", "BBB", "CCC")}}


def test_leakage_scan_future_date():
    hits = assemble.leakage_scan([{"metric": "m", "value": "涨到 2025-02-01", "context": ""}],
                                 "2025-01-10")
    assert len(hits) == 1
    assert assemble.leakage_scan([{"metric": "m", "value": "截至 2025-01-09", "context": ""}],
                                 "2025-01-10") == []


def test_assemble_produces_v4_case():
    case, issues = assemble.assemble(STRUCTURED, MARKET)
    assert case is not None and issues == []
    assert case["schema"].startswith("FinancialPredictionTrainingCase.v4")
    assert case["time_dimension_calibration"]["first_tradable_session"] == "2025-01-13"
    rows = case["target_relation_evidence"]["rows"]
    assert {r["symbol"]: r["priced_label_status"] for r in rows}["DDD"] == \
        "unpriced_weak_candidate_needs_price_panel"
    assert case["weak_relation_universe"]["priced_target_count"] == 3
    assert case["intraday_volume_panel"]["status"] == "missing_real_intraday_for_event_date"


def test_assemble_rejects_too_few_priced():
    mk = dict(MARKET, priced_symbols=["AAA"], labels=[_label("AAA")],
              market_data_symbols={"AAA": MARKET["market_data_symbols"]["AAA"]})
    case, issues = assemble.assemble(STRUCTURED, mk)
    assert case is None and any("3" in i for i in issues)
```

- [ ] **Step 2: 跑测试确认失败**

Run: `.venv/bin/python -m pytest tests/completion/test_assemble.py -q`
Expected: FAIL `ModuleNotFoundError`

- [ ] **Step 3: 移植实现**

复制 `~/同花顺实习/projects/event_dataset/src/stage_f_assemble.py` → `src/completion/assemble.py`，修改点：

1. import/路径常量替换（`config.STRUCTURED_DIR/MARKET_DIR/FINAL_DIR/REPORT_DIR`→EVENT_ 常量）
2. `main()`→`run(args)`（`args.max_cases`），`__main__` 块删除；汇总文件名改 `stage_assemble_summary.json`
3. 其余（v4 各固定块/审计/case_id 唯一化）原样保留

- [ ] **Step 4: 跑测试确认通过**

Run: `.venv/bin/python -m pytest tests/completion/test_assemble.py -q`
Expected: 3 passed

- [ ] **Step 5: 提交**

```bash
git add src/completion/assemble.py tests/completion/test_assemble.py
git commit -m "feat(complete): assemble v4组装与泄露审计

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 10: CLI 重写 + 删除旧代码 + schema

**Files:**
- Modify: `src/cli/main.py`（重写 extract/complete 分支，删 run-all 与旧 extraction 引用）
- Delete: `src/extraction/{pipeline,prompt,models,output,client}.py`、`src/completion/{models,output,pipeline}.py`、`tests/extraction/test_pipeline.py`、`schema/extraction/event_record.schema.json`、`schema/completion/completed_event.schema.json`
- Create: `schema/extraction/selected_event.schema.json`、`schema/completion/final_case.schema.json`
- Modify: `src/extraction/__init__.py`、`src/completion/__init__.py`（docstring 更新）
- Test: 更新 `tests/common/test_cli.py`

**Interfaces:**
- Consumes: Task 4-9 各模块的 `run/run_fetch/run_label(args)`
- Produces: `python -m src.main extract index|cluster|select|structure|all ...`、`python -m src.main complete fetch|label|assemble|all ...`

- [ ] **Step 1: 更新 CLI 测试（先失败）**

在 `tests/common/test_cli.py` 追加：

```python
def test_extract_subcommands_help(capsys):
    import pytest
    from src.cli.main import main
    for argv in (["extract", "--help"], ["complete", "--help"]):
        with pytest.raises(SystemExit) as e:
            main(argv)
        assert e.value.code == 0
    out = capsys.readouterr().out
    assert "assemble" in out


def test_run_all_removed():
    import pytest
    from src.cli.main import main
    with pytest.raises(SystemExit):
        main(["run-all"])
```

Run: `.venv/bin/python -m pytest tests/common/test_cli.py -q` → Expected: 新增用例 FAIL

- [ ] **Step 2: 重写 src/cli/main.py 的 extract/complete 分支**

保留 clean 相关全部代码（`add_cleaning_arguments/config_from_args/run_cleaning_from_args/build_clean_parser/build_parser/positive_*`）。删除 `from src.extraction.models import ExtractionSettings`、`build_extract_parser`、`extraction_settings_from_args`、`run-all` 分支。`main()` 的 extract/complete 分支替换为：

```python
def build_event_parser(stage: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=f"python -m src.main {stage}",
        description=("事件抽取：index建索引 / cluster聚类 / select阈值筛选 / structure LLM结构化"
                     if stage == "extract" else
                     "事件补全：fetch拉行情(本地Mac跑) / label打标 / assemble组装v4"),
    )
    sub = parser.add_subparsers(dest="step", required=True)
    if stage == "extract":
        p = sub.add_parser("index", help="扫描 v1+v2 建 parquet 索引(断点续跑)")
        p.add_argument("--workers", type=positive_int, default=None,
                       help="ceph-fuse 红线: 默认取 config(6), 禁超 10")
        p.add_argument("--limit", type=int, default=0)
        p.add_argument("--fresh", action="store_true")
        p = sub.add_parser("cluster", help="事件候选聚类")
        p.add_argument("--workers", type=positive_int, default=32)
        p.add_argument("--shards", type=positive_int, default=96)
        p = sub.add_parser("select", help="阈值筛选(--sweep 看阈值表)")
        p.add_argument("--sweep", action="store_true")
        p.add_argument("--dry-run", action="store_true")
        p.add_argument("--triage-workers", type=positive_int, default=24)
        p = sub.add_parser("structure", help="LLM 结构化(先 --limit 5 验收)")
        p.add_argument("--workers", type=positive_int, default=24)
        p.add_argument("--limit", type=int, default=0)
        sub.add_parser("all", help="顺序跑 index->cluster->select->structure(阈值确定后用)")
    else:
        from src import config
        p = sub.add_parser("fetch", help="yfinance 拉日线面板(在本地 Mac 跑)")
        p.add_argument("--batch", type=positive_int, default=40)
        p.add_argument("--pause", type=float, default=2.0)
        p.add_argument("--structured", default=f"{config.EVENT_STRUCTURED_DIR}/structured.jsonl")
        p.add_argument("--outdir", default=config.EVENT_MARKET_DIR)
        sub.add_parser("label", help="离线计算 1D/5D/20D 标签")
        p = sub.add_parser("assemble", help="组装 v4 成品 + 审计")
        p.add_argument("--max-cases", type=int, default=0)
        sub.add_parser("all", help="label -> assemble (fetch 需单独在本地跑)")
    return parser


def run_extract(args: argparse.Namespace) -> None:
    from src import config
    from src.extraction import cluster, index, select, structure
    steps = {"index": index.run, "cluster": cluster.run,
             "select": select.run, "structure": structure.run}
    if args.step == "all":
        import argparse as ap
        index.run(ap.Namespace(workers=None, limit=0, fresh=False))
        cluster.run(ap.Namespace(workers=32, shards=96))
        select.run(ap.Namespace(sweep=False, dry_run=False, triage_workers=24))
        structure.run(ap.Namespace(workers=24, limit=0))
        return
    if args.step == "index" and args.workers is None:
        args.workers = config.EVENT_INDEX_WORKERS
    steps[args.step](args)


def run_complete(args: argparse.Namespace) -> None:
    from src.completion import assemble, market
    if args.step == "fetch":
        market.run_fetch(args)
    elif args.step == "label":
        market.run_label(args)
    elif args.step == "assemble":
        assemble.run(args)
    else:  # all = label -> assemble
        import argparse as ap
        market.run_label(ap.Namespace())
        assemble.run(ap.Namespace(max_cases=0))
```

`main()` 分支改为：

```python
    if stage == "extract":
        run_extract(build_event_parser("extract").parse_args(argv[1:]))
        return
    if stage == "complete":
        run_complete(build_event_parser("complete").parse_args(argv[1:]))
        return
    if stage == "run-all":
        raise SystemExit("run-all 已移除: 请按 docs/pipeline.md 分阶段执行(select 需人工定阈值)")
```

- [ ] **Step 3: 删除旧代码与旧 schema**

```bash
git rm src/extraction/pipeline.py src/extraction/prompt.py src/extraction/models.py \
       src/extraction/output.py src/extraction/client.py \
       src/completion/models.py src/completion/output.py src/completion/pipeline.py \
       tests/extraction/test_pipeline.py \
       schema/extraction/event_record.schema.json schema/completion/completed_event.schema.json
```

`src/extraction/__init__.py` 改为 `"""事件抽取: index -> cluster -> select -> structure."""`；
`src/completion/__init__.py` 改为 `"""事件补全: fetch -> label -> assemble."""`

- [ ] **Step 4: 新 schema 文件**

`schema/extraction/selected_event.schema.json`：

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "SelectedEvent",
  "description": "extract select 产物 selected_events.jsonl 单行。",
  "type": "object",
  "additionalProperties": true,
  "required": ["event_id", "event_date", "event_type", "significance"],
  "properties": {
    "event_id": {"type": "string", "minLength": 1},
    "event_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
    "event_type": {"type": "string"},
    "event_family": {"type": ["string", "null"]},
    "event_subject": {"type": ["string", "null"]},
    "primary_symbols": {"type": ["array", "null"], "items": {"type": "string"}},
    "significance": {"type": "integer", "minimum": 3, "maximum": 5},
    "title_cn": {"type": ["string", "null"]},
    "n_articles": {"type": "integer", "minimum": 1}
  }
}
```

`schema/completion/final_case.schema.json`：

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "FinancialPredictionTrainingCaseV4",
  "description": "complete assemble 产物 final/<CASE_ID>.json 关键字段校验(增强层允许留空占位)。",
  "type": "object",
  "additionalProperties": true,
  "required": ["schema", "case_id", "case_title", "year", "event_family", "main_event",
               "time_dimension_calibration", "target_relation_evidence", "market_data",
               "supervised_targets_hidden_labels", "quality_audit"],
  "properties": {
    "schema": {"const": "FinancialPredictionTrainingCase.v4.three_year_event_signal_pack"},
    "case_id": {"type": "string", "pattern": "^[A-Z0-9][A-Z0-9_]{5,90}$"},
    "year": {"type": "integer", "minimum": 2000},
    "main_event": {
      "type": "object",
      "required": ["event_id", "event_date", "event_type", "facts_publicly_reported",
                   "event_influence_channels"],
      "properties": {
        "event_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
        "facts_publicly_reported": {"type": "array", "minItems": 2},
        "event_influence_channels": {"type": "array", "minItems": 2}
      }
    },
    "target_relation_evidence": {
      "type": "object",
      "properties": {"rows": {"type": "array", "minItems": 5}}
    },
    "supervised_targets_hidden_labels": {
      "type": "object",
      "properties": {"labels": {"type": "array", "minItems": 3}}
    }
  }
}
```

- [ ] **Step 5: 全量验证**

```bash
.venv/bin/python -m compileall -q src && .venv/bin/python -m pytest -q
.venv/bin/python -m src.main extract --help && .venv/bin/python -m src.main complete --help
```
Expected: pytest 全绿；两个 help 正常输出且含各子命令

- [ ] **Step 6: 提交**

```bash
git add -A
git commit -m "feat(cli)!: extract/complete 子命令组接管, 删除旧逐条抽取实现

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 11: 文档重写

**Files:**
- Modify: `docs/extraction.md`、`docs/completion.md`、`docs/pipeline.md`、`README.md`

**Interfaces:**
- Consumes: 既有 RUNBOOK 内容（`~/同花顺实习/projects/event_dataset/RUNBOOK.md`）与本计划命令
- Produces: 用户可照跑的完整命令序列（docs/pipeline.md 为运行手册主文档）

- [ ] **Step 1: 重写 docs/pipeline.md**

以 RUNBOOK.md 为底本改写：命令全部替换为 `python -m src.main extract ...` / `complete ...` 形式；保留 ceph-fuse 红线段落、各阶段完成标志与预期数字、常见问题表、"跑完发我什么"清单；补充 select --sweep 人工定阈值流程与 `.env` 需增加 `OPENAI_MODEL_STRUCTURE=deepseek-v4-pro` 的说明。

- [ ] **Step 2: 重写 docs/extraction.md 与 docs/completion.md**

extraction.md：四个子步的输入/输出/参数/断点续跑语义（对应本计划 Task 4-7 的 Interfaces 块内容）。completion.md：fetch 本地跑的原因与 scp 流程、label 日历规则（pre_market 用前一交易日）、assemble 审计项清单。

- [ ] **Step 3: 更新 README.md**

"目录结构"与"默认数据约定"两节：extraction/completion 描述替换为新子步；数据约定追加 `/mnt/ainvest_content/v3/event_dataset/{index,candidates,selected,structured,market,final,reports}`；删除对 `extracted/event_batch*.jsonl`、`completed/completed_batch*.jsonl` 的约定。

- [ ] **Step 4: 提交**

```bash
git add docs/ README.md
git commit -m "docs: eventpack 流水线运行手册与阶段文档

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 12: 推送服务器 + 服务器冒烟

**Files:**
- 无代码改动；服务器端环境与验证

**Interfaces:**
- Produces: 服务器上可运行的 feat/event-dataset 分支 + 冒烟通过记录；用户接手全量运行

- [ ] **Step 1: 推送并切换服务器分支**

```bash
cd ~/同花顺实习/projects/aime_event_dev && git push origin feat/event-dataset
ssh pdf2json 'cd /mnt/ainvest_content/v1/code/aime_event && git checkout feat/event-dataset && git log --oneline -1'
```

- [ ] **Step 2: 服务器依赖 + .env 补充**

```bash
ssh pdf2json 'cd /mnt/ainvest_content/v1/code/aime_event && .venv/bin/pip install -q -r requirements.txt && grep -q OPENAI_MODEL_STRUCTURE .env || echo "OPENAI_MODEL_STRUCTURE=deepseek-v4-pro" >> .env'
```

- [ ] **Step 3: 服务器跑单测 + CLI 冒烟**

```bash
ssh pdf2json 'cd /mnt/ainvest_content/v1/code/aime_event && .venv/bin/python -m pytest -q && .venv/bin/python -m src.main extract --help >/dev/null && echo SMOKE_CLI_OK'
```
Expected: 全绿 + `SMOKE_CLI_OK`

- [ ] **Step 4: 小样本端到端冒烟（index 2 文件 → cluster → select --dry-run → structure 2 条）**

```bash
ssh pdf2json 'cd /mnt/ainvest_content/v1/code/aime_event && \
  .venv/bin/python -m src.main extract index --limit 2 --workers 4 --fresh && \
  .venv/bin/python -m src.main extract cluster --workers 8 --shards 16 && \
  .venv/bin/python -m src.main extract select --dry-run && \
  .venv/bin/python -m src.main extract select --sweep'
```
Expected: 各步正常退出；sweep 打出阈值对照表（仅基于 2+2 文件的部分数据，数字仅验证链路）

```bash
ssh pdf2json 'cd /mnt/ainvest_content/v1/code/aime_event && .venv/bin/python -m src.main extract structure --limit 2 --workers 2'
```
Expected: `structured/structured.jsonl` 出现 2 行且无 `_error`（真实调用 v4-pro，费用分级）
注意: 冒烟产物基于部分索引，交给用户全量跑之前执行
`rm -rf /mnt/ainvest_content/v3/event_dataset/{index,candidates,selected,structured}` 清场。

- [ ] **Step 5: 冒烟通过后清场并汇报**

```bash
ssh pdf2json 'rm -rf /mnt/ainvest_content/v3/event_dataset/{index,candidates,selected,structured} && echo CLEANED'
```
向用户输出 docs/pipeline.md 中的全量运行序列，交接运行。

---

## Self-Review 记录

- **Spec 覆盖**：spec §3 仓库结构→Task 2-11；§4 数据适配→config(Task 2)+index --fresh(Task 12 交接命令)；§5.3 阈值抽取→Task 6；§5.4 双模型→Task 3/7；§5.5 fetch 本地→Task 8+CLI(Task 10)+docs(Task 11)；§5.6 组装→Task 9；§7 测试→各 task TDD+Task 12 冒烟；§8 git→Task 1/12。无缺口。
- **占位符扫描**：Task 11 文档步为内容指令（底本+改写要点+必含清单），不含 TBD；移植类 Task 以"复制源文件+精确修改点清单"表达，源文件路径唯一且存在。
- **类型一致性**：`run(args)` 约定统一；`run_fetch/run_label` 与 CLI 分发一致；`model_for("structure")` 与 llm.py 定义一致；EVENT_ 常量名在 Task 2 定义、后续引用一致。
