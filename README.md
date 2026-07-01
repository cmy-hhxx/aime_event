# aime_event

面向 AInvest 内容数据的事件流水线仓库。当前已实现 **cleaning** 数据清洗与去重；后续预留
**extraction** 事件抽取和 **completion** 事件补全两个阶段。

## 快速开始

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

清洗阶段：

```bash
# 新阶段入口
.venv/bin/python -m src.main clean fresh --workers 48 --no-near-dedup

# 兼容旧入口
.venv/bin/python -m src.main fresh --workers 48 --no-near-dedup
```

更多阶段说明：

- [docs/cleaning.md](docs/cleaning.md)：清洗、去重、排序、分片与报表
- [docs/extraction.md](docs/extraction.md)：事件抽取阶段规划
- [docs/completion.md](docs/completion.md)：事件补全阶段规划
- [docs/pipeline.md](docs/pipeline.md)：三阶段输入输出衔接

## 目录结构

```text
docs/                 # 各阶段说明文档
schema/               # 各阶段 JSON Schema
scripts/              # 一次性工具和压测脚本
src/
  main.py             # python -m src.main 兼容入口
  config.py           # 当前默认配置
  cli/                # 统一 CLI
  common/             # 跨阶段通用 IO、日志、路径、SQLite 工具
  cleaning/           # 数据清洗与去重
  extraction/         # 事件抽取阶段占位
  completion/         # 事件补全阶段占位
tests/                # 按阶段组织的测试
```

## 默认数据约定

```text
/mnt/ainvest_content/v1/content_batch_*.ndjson
/mnt/ainvest_content/v3/v1/cleaned_batch*.jsonl
/mnt/ainvest_content/v3/v1/state/dedup.db
/mnt/ainvest_content/v3/v1/extracted/event_batch*.jsonl
/mnt/ainvest_content/v3/v1/completed/completed_batch*.jsonl
/mnt/ainvest_content/v3/v1/reports/
```

## 验证

```bash
.venv/bin/python -m compileall -q src
.venv/bin/python -m pytest -q
.venv/bin/python -m src.main --help
.venv/bin/python -m src.main clean --help
```
