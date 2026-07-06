# Pipeline 运行手册

三阶段：**clean → extract → complete**。本文是全量运行的操作手册；各阶段内部细节见
[cleaning.md](cleaning.md) / [extraction.md](extraction.md) / [completion.md](completion.md)。

```text
/mnt/ainvest_content/v1/content_batch_*.ndjson            原始语料
  -> clean
/mnt/ainvest_content/v3/v1/cleaned_batch*.jsonl           清洗后新闻 (v2 研报另有 /v3/v2)
  -> extract (index -> cluster -> select -> structure)
/mnt/ainvest_content/v3/event_dataset/structured/structured.jsonl
  -> complete (fetch[本地Mac] -> label -> assemble)
/mnt/ainvest_content/v3/event_dataset/final/<CASE_ID>.json  v4 事件训练包成品
```

## 0. 环境准备

```bash
cd /mnt/ainvest_content/v1/code/aime_event
# 新容器建议重建 venv (python >= 3.10):
rm -rf .venv && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
# .env 需包含(structure 步用更强模型):
#   OPENAI_BASE_URL / OPENAI_API_KEY / OPENAI_MODEL=deepseek-v4-flash
#   OPENAI_MODEL_STRUCTURE=deepseek-v4-pro
grep -c OPENAI .env   # 应 >= 4
```

所有产出写到 `/mnt/ainvest_content/v3/event_dataset/`，各阶段汇总在其 `reports/`。

## ⚠️ ceph-fuse 并发红线（重要）

`/mnt/ainvest_content/*` 是 ceph-fuse 挂载，扫原始大文件的并发瓶颈在 FUSE 守护进程，
**与容器核数无关**：48 并发实测全员 D 状态卡死（挂载退化到 1.7MB/s），12 并发仍拥塞
（FUSE max_background=12），**6 并发正常**。`extract index` 的 `--workers` 缺省即 6，
新容器最多试探到 10：用 `grep read_bytes /proc/<pid>/io` 隔几秒看两次，数字在涨才健康。
其余阶段读的是小 parquet，并发不受此限制。

## 1. extract index（建索引，IO 重）

```bash
nohup .venv/bin/python -m src.main extract index --fresh \
  > /mnt/ainvest_content/v3/event_dataset/reports/index.log 2>&1 &
tail -f /mnt/ainvest_content/v3/event_dataset/reports/index.log
```

- **首次必须 `--fresh`**：旧索引指向已删除的 no-near-dedup 目录，全部作废
- 之后默认断点续跑（按输出文件跳过；写入原子，中途杀掉无半截文件）
- 完成标志：日志末行 `{"elapsed_sec": ..., "v1_rows": ...}`，预期 v1_rows≈2460万、failed 空

## 2. extract cluster（聚类，CPU 重）

```bash
nohup .venv/bin/python -m src.main extract cluster --workers 32 \
  > /mnt/ainvest_content/v3/event_dataset/reports/cluster.log 2>&1 &
```

- 完成看 `reports/stage_b_summary.json` 的 `clusters_ge3`（≥3 篇报道的簇数），预期数万以上；
  <5000 说明过滤过狠，反馈调参

## 3. extract select（阈值筛选，先看表定阈值再花钱）

```bash
# 3a. 阈值对照表(不调 API): 各组合下送审量
.venv/bin/python -m src.main extract select --sweep
# 3b. 若要改阈值: 编辑 src/config.py 的 EVENT_RECENT_MIN_ARTICLES 等
# 3c. dry-run 确认送审量(不调 API):
.venv/bin/python -m src.main extract select --dry-run
# 3d. 正式跑(DeepSeek flash, 断点续跑):
nohup .venv/bin/python -m src.main extract select \
  > /mnt/ainvest_content/v3/event_dataset/reports/select.log 2>&1 &
```

- **无数量配额**：入选 = 规则送审门 + LLM 判定 `is_valid_event 且 significance≥3`，
  总量是阈值的自然结果；护栏仅去重 + 单 symbol 上限（config EVENT_PER_SYMBOL_CAP，0=关闭）
- 完成看 `reports/stage_select_summary.json` 的 `selected` 与 by_era/by_type 分布

## 4. extract structure（LLM 结构化，deepseek-v4-pro）

```bash
# 先 5 条人工验收质量(facts 是否混入事后信息、关系标的是否合理):
.venv/bin/python -m src.main extract structure --limit 5 --workers 2
# OK 后全量(断点续跑):
nohup .venv/bin/python -m src.main extract structure \
  > /mnt/ainvest_content/v3/event_dataset/reports/structure.log 2>&1 &
```

## 5. complete fetch（本地 Mac 跑——服务器连不上 Yahoo）

```bash
# 本地 Mac:
mkdir -p ~/event_e && scp pdf2json:/mnt/ainvest_content/v3/event_dataset/structured/structured.jsonl ~/event_e/
cd <本地 aime_event 克隆> && python3 -m src.main complete fetch \
  --structured ~/event_e/structured.jsonl --outdir ~/event_e
# 增量式: 已拉过的 symbol 自动跳过, 被限流等几分钟重跑同一命令
ssh pdf2json 'mkdir -p /mnt/ainvest_content/v3/event_dataset/market'
scp ~/event_e/prices_daily.parquet pdf2json:/mnt/ainvest_content/v3/event_dataset/market/
```

## 6. complete label + assemble（容器，纯计算，分钟级）

```bash
.venv/bin/python -m src.main complete all   # = label -> assemble
```

- `reports/stage_label_summary.json`: `labeled`/`skipped_few_symbols`（窗口不完整或 <3 有效标的作废，正常损耗）
- `reports/stage_assemble_summary.json`: `cases_written`/`events_with_issues`（泄露扫描/结构审计命中，需抽查）
- 成品: `final/<CASE_ID>.json` + `final/manifest.jsonl`

## 常见问题

| 现象 | 处理 |
|---|---|
| index worker 全 D 状态、日志停滞 | ceph-fuse 拥塞：kill 后降 --workers 重跑（断点续跑不丢） |
| select/structure 大量 `_error` | 限流。**注意**：`_error` 行也会被断点续跑当作已完成跳过，重试前先剔除失败行再降 workers 重跑：`grep -v '"_error"' selected/triage.jsonl > t && mv t selected/triage.jsonl`（structure 对 structured.jsonl 同理） |
| yfinance 大面积失败 | 本地网络/限流：等 10 分钟重跑 fetch（增量） |
| 某阶段全部重来 | index 加 `--fresh`；cluster 直接重跑；select/structure 删对应 jsonl；fetch 删 parquet |

## 跑完之后

把以下发给数据集负责人做质量审计：
1. `reports/` 下全部 `*_summary.json`
2. `final/manifest.jsonl`
3. 随机 3 个 `final/*.json`（与 `事件格式.json` 逐字段核对）

旧命令兼容性：`clean` 与旧 `run/fresh/export` 入口不变；`run-all` 已移除
（select 需人工定阈值，无法无人值守串联全程）。
