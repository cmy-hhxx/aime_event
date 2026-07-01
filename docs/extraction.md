# Extraction 阶段

事件抽取阶段读取 cleaning 输出的 `cleaned_batch*.jsonl`，生成结构化事件记录。

## 当前状态

该阶段目前只有目录和 CLI 入口占位，尚未接入具体抽取逻辑。直接运行
`python -m src.main extract` 会返回未实现提示。

## 预期输入输出

```text
输入：/mnt/ainvest_content/v3/v1/cleaned_batch*.jsonl
输出：/mnt/ainvest_content/v3/v1/extracted/event_batch*.jsonl
Schema：schema/extraction/event_record.schema.json
```

## 后续实现点

- 定义事件类型、事件主体、时间、标的、证据句等字段。
- 明确抽取模型或规则入口。
- 保持输出 JSONL 分片，便于和 cleaning/completion 串联。
