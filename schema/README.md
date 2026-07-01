# Schema 文件说明

本目录定义流水线各阶段的输出格式。`.json` 为 JSON Schema（Draft-07），可用于 `jsonschema` 校验；`.jsonc` 为带中文注释的人类可读说明。

## 输出记录格式

| 文件 | 用途 | 校验目录 |
|------|------|----------|
| [cleaning/cleaned_record.schema.json](cleaning/cleaned_record.schema.json) | 完整审计格式（CleanedRecord） | `output/cleaned/`、`output/duplicates/` |
| [cleaning/cleaned_record.schema.jsonc](cleaning/cleaned_record.schema.jsonc) | CleanedRecord 中文说明 + 去重规则 | — |
| [extraction/event_record.schema.json](extraction/event_record.schema.json) | 事件抽取精简格式（EventRecord） | `output/event_input/` |
| [extraction/event_record.schema.jsonc](extraction/event_record.schema.jsonc) | EventRecord 中文说明 | — |
| [completion/completed_event.schema.json](completion/completed_event.schema.json) | 事件补全输出格式（CompletedEvent） | `output/completed/` |

### CleanedRecord vs EventRecord

- **CleanedRecord**：保留 `dedup`、`meta`、`tags`、`source.author` 等审计字段，用于溯源与质量审计。
- **EventRecord**：从 CleanedRecord 投影，去掉审计字段，`tags` 转为 `topics`（已去除重要性与地区标签），供下游事件概念抽取使用。

两种格式均遵循：**不输出 `null`，空字段省略**。

## 报表格式

| 文件 | 用途 | 输出路径 |
|------|------|----------|
| [cleaning/summary.schema.json](cleaning/summary.schema.json) | 全库汇总统计 | `reports/summary.json` |
| [cleaning/batch_stats.schema.json](cleaning/batch_stats.schema.json) | 按 batch 统计（NDJSON 每行） | `reports/batch_stats.jsonl` |
| [cleaning/near_duplicates.schema.json](cleaning/near_duplicates.schema.json) | 近似去重候选对审计 | `reports/near_duplicates.jsonl` |

报表字段详解见 [reports/README.md](../reports/README.md)。

## 去重 v4 规则摘要

1. 折叠重复原始 `id`
2. `US_NOTICE` 使用 SEC accession 或附件 URL
3. 合格文章 URL 规范化后做 URL 去重
4. Feed/列表/API URL 回退到标题+正文 SHA-256
5. 精确 canonical 记录可经 MinHash + RapidFuzz 近似去重合并（`dedup.method = near_minhash`）
