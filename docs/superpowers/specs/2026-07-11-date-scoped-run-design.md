# 按日期跑事件流水线(--date)设计

日期:2026-07-11 · 状态:已获用户批准

## 目标

在本地(Mac,窗口子集数据)支持传入具体日期(如 `2026-05-29`)跑 extract + complete,
直接产出该日事件的 `final/<CASE_ID>.json` 成品,不必全量跑。

## 需求决策(用户已确认)

1. **日期语义**:`peak_date`(报道高峰日)严格等于指定日;LLM 结构化后不再按
   `event_date` 二次过滤。
2. **1m 分时缺失**:`assemble` 新增 `--allow-no-intraday` 开关降级——缺完整 1m 面板
   时事件仍落 final,`intraday_volume_panel` 写缺失占位;默认关闭,行为不变。
3. **命令形态**:各子命令加可选 `--date`;`extract all` / `complete all` 透传,
   合并为端到端少数几条命令。

## CLI 变更(src/cli/main.py)

- `extract cluster / select / structure / all` 加可选 `--date YYYY-MM-DD`(格式校验)。
- `complete fetch / label / assemble / all` 加可选 `--date`;
  `assemble` 与 `complete all` 另加 `--allow-no-intraday`。
- `extract index`、`fetch-intraday`、`import-intraday` 不动。
- 不传 `--date` 时所有行为与现状完全一致。
- `extract all --date`:index 照常跑(断点续跑,已建则秒级跳过)→ cluster → select
  → structure,全部透传 `--date`。单日送审量小,select 自动跑可接受。
- `complete all --date --allow-no-intraday`:label → assemble 透传两参数;
  fetch 仍单独跑(网络限流需反复重跑)。

## 各步过滤逻辑

| 步骤 | 过滤点 |
|---|---|
| `cluster --date` | 建池 SQL 加 `AND pub_date BETWEEN date-7天 AND date+7天`(聚类滑窗 3 天,留链式簇余量);产出仍写 `candidates/`(覆盖式) |
| `select --date` | `build_candidates` WHERE 加 `AND peak_date = date`;`--sweep`/`--dry-run` 同样生效 |
| `structure --date` | `load_selected()` 后按 `e["peak_date"] == date` 过滤 |
| `complete fetch --date` | `load_structured()` 后按 `_triage.peak_date == date` 过滤再收集 symbols |
| `complete label --date` | 同上过滤 events |
| `complete assemble --date` | 同上过滤 structured |

过滤键 `_triage.peak_date` 已由 `structure_one` 写入 structured.jsonl
(src/extraction/structure.py:122),全链路现成。

## --allow-no-intraday 降级(src/completion/assemble.py)

缺完整 1m 面板时(`complete_intraday_panel` 不通过或无 intraday 记录):

- 不再 `return None` 丢弃 case;
- `intraday_volume_panel` 写占位:required 键齐全(provider="missing"、symbols={} 等),
  schema 合法(symbols 无 minProperties);
- manifest 行与 `quality_audit` issues 标注 `intraday_missing`,拿到 1m 数据后可重跑补齐。

## 已知取舍

- `selected_events.jsonl`、`labels.jsonl`、`manifest.jsonl` 为覆盖式写入,`--date` 跑会
  覆盖之前全量跑的这几个文件(本地无全量结果,接受;服务器全量跑不受影响——不传 --date)。
- 聚类滑窗支持链式连边,簇跨度可超 ±7 天,极端情况下 `--date` 的簇边界与全量跑略有差异;
  对报道集中 1-2 天的典型单日事件无影响,接受。
- `triage.jsonl`、`structured.jsonl` 是按 event_id 断点续跑的 append 文件,多日期跑自然
  积累、互不干扰,下游靠 `--date` 过滤。

## 测试

- `tests/common/test_cli.py`:各子命令 `--date` 解析、非法日期报错、`extract all`/
  `complete all` 透传。
- select/structure 的日期过滤逻辑单测。
- `tests/completion/test_assemble.py`:`--allow-no-intraday` 开/关两态(缺 1m 时落/不落
  final、占位 panel 过 schema 校验)。

## 用户本地 05-29 跑法(实现后)

```bash
source .env
python3 -m src.main extract all --date 2026-05-29
python3 -m src.main complete fetch --date 2026-05-29 \
  --structured data/event_dataset/structured/structured.jsonl --outdir data/event_dataset/market
python3 -m src.main complete all --date 2026-05-29 --allow-no-intraday
# 结果: data/event_dataset/final/<CASE_ID>.json
```
