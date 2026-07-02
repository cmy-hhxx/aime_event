# 报表说明

本目录由流水线每次运行时重新生成。打开 [index.json](index.json) 可查看各文件说明与对应 schema 路径。

## summary.json

全库汇总统计（单个 JSON 对象）。

| 字段 | 含义 |
|------|------|
| `total_input` | 所有 batch 原始行数之和 |
| `total_accepted` | 通过校验并入库的记录数 |
| `total_cleaned` | 最终 canonical 记录数（不含被近似去重吞掉的 loser） |
| `total_duplicates` | 重复记录数（精确 + 近似） |
| `total_rejected` | 被拒绝的原始行数 |
| `batches_completed` | 已完成的 batch 数量 |
| `by_content_type` | canonical 按 `content_type` 分布 |
| `dedup_by_method` | canonical 的去重方法分布 |
| `duplicate_by_method` | 重复记录的去重方法分布 |
| `rejects_by_reason` | 拒绝原因分布（如 `empty_body`） |
| `near_duplicate_candidates` | 近似去重候选对总数 |
| `near_duplicates_auto_merged` | 自动合并的对数 |
| `near_duplicates_report_only` | 仅记录、未合并的对数 |
| `storage` | 各目录字节数，含 `estimated_20m_rows_bytes`（按当前行均大小估算 2000 万行存储） |

Schema：[schema/cleaning/summary.schema.json](../schema/cleaning/summary.schema.json)

## batch_stats.jsonl

每个 batch 一行（NDJSON），字段：

- `batch`：文件名，如 `content_batch_0.ndjson`
- `status`：`running` 或 `complete`
- `input` / `accepted` / `rejected`：行数统计

Schema：[schema/cleaning/batch_stats.schema.json](../schema/cleaning/batch_stats.schema.json)

## near_duplicates.jsonl

近似去重审计日志，每行一对候选记录（最多写入 `NEAR_MAX_REPORT_PAIRS` 对，见 `src/config.py`）。

| 字段 | 含义 |
|------|------|
| `status` | `auto_merged`（已合并）或 `report_only`（仅记录） |
| `reason` | 决策原因，如 `high_confidence_near_duplicate`、`below_minhash_threshold`、`below_fuzzy_threshold`、`different_host_and_title`、`published_at_gap` |
| `left` / `right` | 候选记录摘要（id、title、content_type、published_at） |
| `scores` | `minhash_jaccard`、`fuzzy_score`、`title_score` |
| `canonical_id` | 合并后保留的记录 ID；`report_only` 时为 null |

Schema：[schema/cleaning/near_duplicates.schema.json](../schema/cleaning/near_duplicates.schema.json)

## 相关文件

- `state/progress.json`：已完成 batch 列表（写在 state 目录，非本目录）
