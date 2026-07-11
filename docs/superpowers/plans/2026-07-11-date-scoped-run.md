# 按日期跑事件流水线(--date)实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给 extract cluster/select/structure/all 与 complete fetch/label/assemble/all 增加可选 `--date YYYY-MM-DD`,并给 assemble 增加 `--allow-no-intraday` 降级,支持本地按单日跑出 final 成品。

**Architecture:** 过滤链 = cluster 建池 SQL 按 `pub_date ± 7天` 粗筛 → select 按 `peak_date = date` 精筛 → structure/fetch/label/assemble 按已存在于记录里的 `peak_date` 过滤。不传 `--date` 时零行为变化。

**Tech Stack:** Python argparse / duckdb SQL 字符串拼接 / pytest。

**设计文档:** `docs/superpowers/specs/2026-07-11-date-scoped-run-design.md`

## Global Constraints

- 不传 `--date` 时所有输出必须与现状逐字节等价(默认值 None,所有过滤短路)。
- 读取 args 上可能缺失的新属性一律用 `getattr(args, "date", None)` / `getattr(args, "allow_no_intraday", False)`,与现有 `getattr(args, "limit", 0)` 风格一致(兼容测试与内部 Namespace 调用)。
- **本计划不执行 git commit**:工作区在 main 分支且 `src/cli/main.py`、`tests/common/test_cli.py` 等同名文件已有用户未提交改动,提交会把用户 WIP 混入;所有改动保留在工作区,最后统一报告。
- 运行测试统一用 `python3 -m pytest <path> -v`(仓库根目录执行)。

---

### Task 1: CLI 加 `--date` / `--allow-no-intraday` 参数与透传

**Files:**
- Modify: `src/cli/main.py`(`build_event_parser`、`run_extract`、`run_complete`,顶部加 `iso_date`)
- Test: `tests/common/test_cli.py`

**Interfaces:**
- Produces: `iso_date(value: str) -> str`(argparse type,非法日期抛 ArgumentTypeError);各 step 的 `args.date: str | None`;assemble/complete-all 的 `args.allow_no_intraday: bool`。后续任务的 run(args) 都靠这些属性。

- [x] **Step 1: 写失败测试**(追加到 `tests/common/test_cli.py`)

```python
def test_date_flag_parses_and_validates():
    import pytest
    from src.cli.main import build_event_parser, iso_date

    for stage, step in (("extract", "cluster"), ("extract", "select"),
                        ("extract", "structure"), ("extract", "all"),
                        ("complete", "fetch"), ("complete", "label"),
                        ("complete", "assemble"), ("complete", "all")):
        args = build_event_parser(stage).parse_args([step, "--date", "2026-05-29"])
        assert args.date == "2026-05-29"

    with pytest.raises(SystemExit):
        build_event_parser("extract").parse_args(["select", "--date", "2026-13-01"])
    import argparse
    with pytest.raises(argparse.ArgumentTypeError):
        iso_date("not-a-date")


def test_allow_no_intraday_flag():
    from src.cli.main import build_event_parser
    args = build_event_parser("complete").parse_args(["assemble", "--allow-no-intraday"])
    assert args.allow_no_intraday is True
    args = build_event_parser("complete").parse_args(["all"])
    assert args.allow_no_intraday is False and args.date is None
```

- [x] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/common/test_cli.py::test_date_flag_parses_and_validates -v`
Expected: FAIL(`iso_date` 不存在 / unrecognized arguments: --date)

- [x] **Step 3: 实现**

`src/cli/main.py` 顶部 import 区加:

```python
from datetime import date as _date
```

`positive_float` 后加:

```python
def iso_date(value: str) -> str:
    try:
        _date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be YYYY-MM-DD")
    return value
```

`build_event_parser` 中(保持现有行不动,只加参数;`label` 与两个 `all` 需要接住返回值):

```python
    if stage == "extract":
        ...
        p = sub.add_parser("cluster", help="事件候选聚类")
        p.add_argument("--workers", type=positive_int, default=32)
        p.add_argument("--shards", type=positive_int, default=96)
        p.add_argument("--date", type=iso_date, default=None, help="只聚类该日 ±7 天的报道")
        p = sub.add_parser("select", help="阈值筛选(--sweep 看阈值表)")
        ...原有参数...
        p.add_argument("--date", type=iso_date, default=None, help="只送审 peak_date=该日的候选")
        p = sub.add_parser("structure", help="LLM 结构化(先 --limit 5 验收)")
        ...原有参数...
        p.add_argument("--date", type=iso_date, default=None, help="只结构化 peak_date=该日的入选事件")
        p = sub.add_parser("all", help="顺序跑 index->cluster->select->structure(阈值确定后用)")
        p.add_argument("--date", type=iso_date, default=None, help="按单日跑: 透传给 cluster/select/structure")
    else:
        p = sub.add_parser("fetch", ...)
        ...原有参数...
        p.add_argument("--date", type=iso_date, default=None, help="只拉 peak_date=该日事件的 symbol")
        ...
        p = sub.add_parser("label", help="离线计算 1D/5D/20D 标签")
        p.add_argument("--date", type=iso_date, default=None, help="只打 peak_date=该日的事件")
        p = sub.add_parser("assemble", help="组装 v4 成品 + 审计")
        p.add_argument("--max-cases", type=int, default=0)
        p.add_argument("--date", type=iso_date, default=None, help="只组装 peak_date=该日的事件")
        p.add_argument("--allow-no-intraday", action="store_true",
                       help="缺完整 1m 面板时降级组装(panel 置空占位)而非丢弃")
        p = sub.add_parser("all", help="label -> assemble (fetch 需单独在本地跑)")
        p.add_argument("--date", type=iso_date, default=None)
        p.add_argument("--allow-no-intraday", action="store_true")
```

`run_extract` 的 all 分支改为:

```python
        index.run(ap.Namespace(workers=config.EVENT_INDEX_WORKERS, limit=0, fresh=False))
        cluster.run(ap.Namespace(workers=32, shards=96, date=args.date))
        select.run(ap.Namespace(sweep=False, dry_run=False, triage_workers=24, date=args.date))
        structure.run(ap.Namespace(workers=24, limit=0, date=args.date))
```

`run_complete` 的 all 分支改为:

```python
        market.run_label(ap.Namespace(date=args.date))
        assemble.run(ap.Namespace(max_cases=0, date=args.date,
                                  allow_no_intraday=args.allow_no_intraday))
```

- [x] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/common/test_cli.py -v`
Expected: 全部 PASS(含原有用例)

### Task 2: cluster 建池 SQL 按 pub_date 窗口粗筛

**Files:**
- Modify: `src/extraction/cluster.py`(新增 `pub_date_between`,`run` 内 base CTE 的 WHERE 使用)
- Test: `tests/extraction/test_cluster.py`

**Interfaces:**
- Consumes: `args.date`(Task 1)
- Produces: `pub_date_between(date_str: str | None, margin_days: int = 7) -> str`(返回 `" AND pub_date BETWEEN '...' AND '...'"` 或 `""`)

- [x] **Step 1: 写失败测试**(追加到 `tests/extraction/test_cluster.py`)

```python
def test_pub_date_between_clause():
    from src.extraction.cluster import pub_date_between
    assert pub_date_between(None) == ""
    clause = pub_date_between("2026-05-29")
    assert clause == " AND pub_date BETWEEN '2026-05-22' AND '2026-06-05'"
```

- [x] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/extraction/test_cluster.py::test_pub_date_between_clause -v`
Expected: FAIL with ImportError

- [x] **Step 3: 实现**

`cluster.py` 常量区之后加(模块已 `import datetime as dt`):

```python
def pub_date_between(date_str: str | None, margin_days: int = 7) -> str:
    """--date 单日跑: 建池只留该日 ±margin 的报道(聚类滑窗 3 天, 留链式簇余量)."""
    if not date_str:
        return ""
    d = dt.date.fromisoformat(date_str)
    return (f" AND pub_date BETWEEN '{d - dt.timedelta(days=margin_days)}'"
            f" AND '{d + dt.timedelta(days=margin_days)}'")
```

`run()` 中 base CTE 的 WHERE 尾部(`AND {tech_cond}` 之后)拼接:

```python
    date_cond = pub_date_between(getattr(args, "date", None))
    # base CTE 内:
        AND {tech_cond}{date_cond}
```

(即 f-string SQL 里 `AND {tech_cond}` 改为 `AND {tech_cond}{date_cond}`,并在 SQL 构造前算好 `date_cond`。)

- [x] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/extraction/test_cluster.py -v`
Expected: 全部 PASS

### Task 3: select 按 peak_date 精筛(含 sweep/dry-run)

**Files:**
- Modify: `src/extraction/select.py`(`gate_where` 加 date 参数,`build_candidates`/`sweep`/`run` 接线)
- Test: `tests/extraction/test_select.py`

**Interfaces:**
- Consumes: `args.date`(Task 1)
- Produces: `gate_where(era_split, recent_min, recent_alt_min, early_min, date: str | None = None) -> str`(date 非空时整体加括号并 `AND peak_date = date`);`build_candidates(con, recent_min, recent_alt_min, early_min, date=None)`;`sweep(con, date=None)`

- [x] **Step 1: 写失败测试**(追加到 `tests/extraction/test_select.py`)

```python
def test_gate_where_with_date():
    from src.extraction.select import gate_where
    base = gate_where("2023-07-01", 5, 3, 2)
    dated = gate_where("2023-07-01", 5, 3, 2, date="2026-05-29")
    assert dated == f"({base}) AND peak_date = '2026-05-29'"
    assert gate_where("2023-07-01", 5, 3, 2, date=None) == base
```

- [x] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/extraction/test_select.py::test_gate_where_with_date -v`
Expected: FAIL with TypeError(unexpected keyword argument 'date')

- [x] **Step 3: 实现**

```python
def gate_where(era_split: str, recent_min: int, recent_alt_min: int, early_min: int,
               date: str | None = None) -> str:
    where = (f"(peak_date >= '{era_split}' AND (n_articles >= {recent_min} "
             f"OR (n_articles >= {recent_alt_min} AND n_v2_reactions >= 1))) "
             f"OR (peak_date < '{era_split}' AND n_articles >= {early_min})")
    if date:
        where = f"({where}) AND peak_date = '{date}'"
    return where
```

`build_candidates` 签名加 `date: str | None = None`,内部 `gate_where(..., date)`;
`sweep(con)` 改为 `sweep(con, date: str | None = None)`,循环内 `gate_where(era_split, rm, am, em, date)`;
`run()` 中:

```python
    date = getattr(args, "date", None)
    if args.sweep:
        sweep(con, date)
        return
    cands = build_candidates(con, config.EVENT_RECENT_MIN_ARTICLES,
                             config.EVENT_RECENT_ALT_MIN_ARTICLES,
                             config.EVENT_EARLY_MIN_ARTICLES, date)
```

- [x] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/extraction/test_select.py -v`
Expected: 全部 PASS

### Task 4: structure 按 peak_date 过滤入选事件

**Files:**
- Modify: `src/extraction/structure.py`(新增 `filter_by_peak`,`run` 内 `load_selected()` 之后使用)
- Test: `tests/extraction/test_structure.py`

**Interfaces:**
- Consumes: `args.date`;selected_events.jsonl 行的顶层 `peak_date` 字段(select 落盘时自带)
- Produces: `filter_by_peak(events: list[dict], date: str | None) -> list[dict]`

- [x] **Step 1: 写失败测试**(追加到 `tests/extraction/test_structure.py`)

```python
def test_filter_by_peak():
    from src.extraction.structure import filter_by_peak
    events = [{"event_id": "A", "peak_date": "2026-05-29"},
              {"event_id": "B", "peak_date": "2026-05-28"}]
    assert filter_by_peak(events, None) == events
    assert [e["event_id"] for e in filter_by_peak(events, "2026-05-29")] == ["A"]
```

- [x] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/extraction/test_structure.py::test_filter_by_peak -v`
Expected: FAIL with ImportError

- [x] **Step 3: 实现**

`structure.py` 的 `load_selected` 之后加:

```python
def filter_by_peak(events: list[dict], date: str | None) -> list[dict]:
    if not date:
        return events
    return [e for e in events if e.get("peak_date") == date]
```

`run()` 中 `events = load_selected()` 后插入:

```python
    events = filter_by_peak(events, getattr(args, "date", None))
```

- [x] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/extraction/test_structure.py -v`
Expected: 全部 PASS

### Task 5: complete fetch/label 按 _triage.peak_date 过滤

**Files:**
- Modify: `src/completion/market.py`(新增 `filter_by_peak_date`,`run_fetch`/`run_label` 使用)
- Test: `tests/completion/test_market.py`

**Interfaces:**
- Consumes: `args.date`;structured.jsonl 行的 `_triage.peak_date`(structure_one 已写入)
- Produces: `filter_by_peak_date(events: list[dict], date: str | None) -> list[dict]`(Task 6 的 assemble.run 也 import 它)

- [x] **Step 1: 写失败测试**(追加到 `tests/completion/test_market.py`)

```python
def test_filter_by_peak_date():
    from src.completion.market import filter_by_peak_date
    events = [{"event_id": "A", "_triage": {"peak_date": "2026-05-29"}},
              {"event_id": "B", "_triage": {"peak_date": "2026-05-28"}},
              {"event_id": "C"}]
    assert filter_by_peak_date(events, None) == events
    assert [e["event_id"] for e in filter_by_peak_date(events, "2026-05-29")] == ["A"]
```

- [x] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/completion/test_market.py::test_filter_by_peak_date -v`
Expected: FAIL with ImportError

- [x] **Step 3: 实现**

`market.py` 的 `load_structured` 之后加:

```python
def filter_by_peak_date(events: list[dict], date: str | None) -> list[dict]:
    """--date 单日跑: 按 triage 报道高峰日过滤(与 select --date 同口径)."""
    if not date:
        return events
    return [r for r in events if (r.get("_triage") or {}).get("peak_date") == date]
```

`run_fetch` 中 `events = load_structured(args.structured)` 后、`all_syms = ...` 前插入:

```python
    events = filter_by_peak_date(events, getattr(args, "date", None))
```

`run_label` 中 `events = load_structured(...)` 后同样插入一行。

- [x] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/completion/test_market.py -v`
Expected: 全部 PASS

### Task 6: assemble 的 --date 过滤 + --allow-no-intraday 降级

**Files:**
- Modify: `src/completion/assemble.py`(`assemble()` 加 `allow_no_intraday` 参数;`run()` 加 date 过滤与 manifest 标注)
- Test: `tests/completion/test_assemble.py`

**Interfaces:**
- Consumes: `args.date` / `args.allow_no_intraday`;`market.filter_by_peak_date`(Task 5)
- Produces: `assemble(r, mk, intraday=None, allow_no_intraday=False)`;降级 case 的 `intraday_volume_panel == {"provider": "missing", ..., "symbols": {}}`;manifest 行多一个键 `"intraday_missing": bool`

- [x] **Step 1: 写失败测试**(追加到 `tests/completion/test_assemble.py`)

```python
def test_assemble_allow_no_intraday_downgrades():
    case, issues = assemble.assemble(STRUCTURED, MARKET, None, allow_no_intraday=True)
    assert case is not None
    panel = case["intraday_volume_panel"]
    assert panel["provider"] == "missing" and panel["symbols"] == {}
    assert any("intraday_missing" in i for i in issues)
    schema_path = Path(__file__).parents[2] / "schema/completion/final_case.schema.json"
    jsonschema.validate(case, json.loads(schema_path.read_text()))


def test_assemble_allow_no_intraday_keeps_complete_panel():
    case, issues = assemble.assemble(STRUCTURED, MARKET, INTRADAY, allow_no_intraday=True)
    assert case is not None
    assert case["intraday_volume_panel"]["symbols"]["AAA"]["is_complete_session"] is True
    assert not any("intraday_missing" in i for i in issues)
```

- [x] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/completion/test_assemble.py -v -k allow_no_intraday`
Expected: FAIL with TypeError(unexpected keyword argument 'allow_no_intraday')

- [x] **Step 3: 实现**

`assemble()` 签名改为:

```python
def assemble(r: dict, mk: dict, intraday: dict | None = None,
             allow_no_intraday: bool = False) -> tuple[dict | None, list[str]]:
```

`assemble.py:244-260` 一段改为:

```python
    intraday = intraday or {}
    intraday_symbols = {
        s: p for s, p in (intraday.get("symbols") or {}).items()
        if s in priced and complete_intraday_panel(p, event_date)
    }
    if not intraday_symbols and not allow_no_intraday:
        issues.append("缺少已打价标的的完整事件日 1m 分时面板")
        return None, issues
    if not intraday_symbols:
        issues.append("intraday_missing: 缺完整 1m 面板, 降级组装为空占位")
        intraday_provider = "missing"
    else:
        intraday_provider = intraday.get("provider") or "unknown"
    known_limitations = [
        "association search currently contains corpus source evidence; AI_search remains empty",
    ]
    if intraday_provider == "missing":
        known_limitations.insert(0,
            "intraday 1m panel missing; procure bars and re-run assemble to backfill")
    if "Yahoo" in intraday_provider:
        ...(原有两行不动)...
```

case 字典里 `implemented_data_manifest` 的分时行改为:

```python
            {"data_block": "intraday_full_session_volume_panel",
             "status": "implemented_1m" if intraday_symbols else "missing_pending_import"},
```

(`intraday_volume_panel` 块本身不用改——`symbols: intraday_symbols` 为空 dict、
`provider: intraday_provider` 为 "missing",required 键齐全即 schema 合法。)

`run(args)` 中,文件顶部加 `from src.completion.market import filter_by_peak_date`;
structured 读取后插入过滤,循环内传开关,manifest 行加标注:

```python
    date = getattr(args, "date", None)
    allow_no_intraday = getattr(args, "allow_no_intraday", False)
    if date:
        structured = {eid: r for eid, r in structured.items()
                      if (r.get("_triage") or {}).get("peak_date") == date}
    ...
        case, issues = assemble(r, mk, intraday.get(eid), allow_no_intraday)
    ...
        manifest.write(json.dumps({
            ...原有键...,
            "intraday_missing": case["intraday_volume_panel"]["provider"] == "missing",
        }, ensure_ascii=False) + "\n")
```

- [x] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/completion/test_assemble.py -v`
Expected: 全部 PASS(含原有 `test_assemble_rejects_missing_intraday` 默认路径不变)

### Task 7: 全量回归 + 端到端冒烟

- [x] **Step 1: 全量测试**

Run: `python3 -m pytest tests/ -v`
Expected: 全部 PASS

- [x] **Step 2: CLI 冒烟(不动数据)**

Run: `python3 -m src.main extract all --help && python3 -m src.main complete all --help`
Expected: help 文本包含 `--date`;`complete all` 包含 `--allow-no-intraday`

## Self-Review 记录

- 规格覆盖:CLI 变更→Task 1;cluster/select/structure/fetch/label/assemble 六个过滤点→Task 2-6;降级占位与 manifest 标注→Task 6;测试要求→各任务 Step 1 + Task 7。覆盖设计文档全部小节。
- 占位符扫描:无 TBD/“适当处理”;所有代码步骤附完整代码。
- 类型一致性:`iso_date` 返回 str;`filter_by_peak`(structure,顶层 peak_date)与 `filter_by_peak_date`(market,_triage.peak_date)是两个不同数据形状的函数,命名刻意区分;assemble 复用 market 的版本 inline 实现(run 内 dict 推导),接口块已写明。
