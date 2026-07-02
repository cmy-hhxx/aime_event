# Extraction 阶段

事件抽取阶段读取 cleaning 输出的 `cleaned_batch*.jsonl`，生成结构化事件记录。

## 当前状态

该阶段使用 OpenAI-compatible chat completions API 做 prompt 抽取。先填写仓库根目录下
`.env`，再用小样本试跑。

## 预期输入输出

```text
输入：/mnt/ainvest_content/v3/v1/cleaned_batch*.jsonl
输出：/mnt/ainvest_content/v3/v1/extracted/event_batch*.jsonl
Schema：schema/extraction/event_record.schema.json
```

## 运行

```bash
python -m src.main extract --limit 20
```

调 prompt 时如果前几条 cleaned 样本事件密度太低，可以直接从 raw batch 随机抽样：

```bash
python -m src.main extract \
  --input /mnt/ainvest_content/v1/content_batch_1.ndjson \
  --random-sample \
  --limit 20
```

常用参数：

- `--input`：cleaned 输入目录或单个 JSONL 文件。
- `--output`：抽取输出目录。
- `--model` / `--base-url` / `--api-key`：覆盖 `.env` 中的 API 配置。
- `--limit`：最多处理多少条，建议先用 20 或 100 验证 prompt。
- `--random-sample`：从输入 JSONL/NDJSON 中随机抽样，需配合 `--limit`。
- `--random-seed`：固定抽样种子，便于复现同一批样本。
- `--include-raw-response`：调 prompt 时保留模型原始 JSON 响应。

输出每行对应一条 cleaned 输入，`events` 数组中保存 0 到多个事件；失败记录会保留
`error` 和 `message`，便于重跑前排查。
