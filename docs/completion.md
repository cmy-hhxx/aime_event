# Completion 阶段

事件补全阶段读取 extraction 输出的事件记录，补齐缺失字段并写出最终事件数据。

## 当前状态

该阶段目前只有目录和 CLI 入口占位，尚未接入具体补全逻辑。直接运行
`python -m src.main complete` 会返回未实现提示。

## 预期输入输出

```text
输入：/mnt/ainvest_content/v3/v1/extracted/event_batch*.jsonl
输出：/mnt/ainvest_content/v3/v1/completed/completed_batch*.jsonl
Schema：schema/completion/completed_event.schema.json
```

## 后续实现点

- 定义需要补全的事件字段及来源优先级。
- 明确失败、低置信度和人工复核记录的输出方式。
- 保持按 JSONL 分片输出，避免一次性加载全量数据。
