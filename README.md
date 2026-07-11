# aime_event

面向 AInvest 内容数据的事件流水线仓库，三阶段全部可用：**cleaning** 清洗去重、
**extraction** 事件抽取（索引→聚类→阈值筛选→LLM 结构化）、**completion** 事件补全
（行情打标→v4 训练包组装）。最终产出 `FinancialPredictionTrainingCase.v4` 事件训练包。

## 快速开始

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

```bash
# 清洗(已跑完可跳过):
.venv/bin/python -m src.main clean fresh --workers 48 --no-near-dedup
# 事件抽取四子步:
.venv/bin/python -m src.main extract index --fresh
.venv/bin/python -m src.main extract cluster
.venv/bin/python -m src.main extract select --sweep   # 看阈值表, 再正式跑 select
.venv/bin/python -m src.main extract structure --limit 5   # 验收后去掉 --limit
# 事件补全(fetch 在本地 Mac 跑, 见 docs/pipeline.md):
.venv/bin/python -m src.main complete fetch --structured ... --outdir ...
.venv/bin/python -m src.main complete fetch-intraday --event-date YYYY-MM-DD
# Yahoo 1m 超出最近 30 天时：
.venv/bin/python -m src.main complete import-intraday --input ... --provider ...
.venv/bin/python -m src.main complete all   # = label -> assemble
```

完整运行手册（含 ceph-fuse 并发红线、断点续跑、故障处理）：**[docs/pipeline.md](docs/pipeline.md)**

- [docs/cleaning.md](docs/cleaning.md)：清洗、去重、排序、分片与报表
- [docs/extraction.md](docs/extraction.md)：索引/聚类/阈值筛选/LLM 结构化
- [docs/completion.md](docs/completion.md)：行情拉取/打标/v4 组装与审计

## 目录结构

```text
docs/                 # 各阶段说明文档
schema/               # 各阶段 JSON Schema
scripts/              # 一次性工具和压测脚本
src/
  main.py             # python -m src.main 兼容入口
  config.py           # 当前默认配置(含 EVENT_* 事件流水线区段)
  cli/                # 统一 CLI
  common/             # 跨阶段通用 IO、日志、路径、SQLite、LLM 客户端
  cleaning/           # 数据清洗与去重
  extraction/         # 事件抽取: index/cluster/select/structure + prompts
  completion/         # 事件补全: market(fetch+label)/assemble
tests/                # 按阶段组织的测试
```

## 默认数据约定

```text
/mnt/ainvest_content/v1/content_batch_*.ndjson       原始输入
/mnt/ainvest_content/v3/v1/cleaned_batch*.jsonl      清洗输出(新闻)
/mnt/ainvest_content/v3/v2/cleaned_batch*.jsonl      研报/电话会段落(佐证)
/mnt/ainvest_content/v3/v1/state/dedup.db
/mnt/ainvest_content/v3/v1/reports/                  清洗报表
/mnt/ainvest_content/v3/event_dataset/               事件流水线产出根
  index/ candidates/ selected/ structured/ market/ final/ reports/
```

## 验证

```bash
.venv/bin/python -m compileall -q src
.venv/bin/python -m pytest -q
.venv/bin/python -m src.main --help
.venv/bin/python -m src.main clean --help
```
