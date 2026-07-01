# Pipeline 阶段衔接

仓库按三阶段组织：cleaning → extraction → completion。

## 数据流

```text
/mnt/ainvest_content/v1/content_batch_*.ndjson
  -> cleaning
/mnt/ainvest_content/v3/v1/cleaned_batch*.jsonl
  -> extraction
/mnt/ainvest_content/v3/v1/extracted/event_batch*.jsonl
  -> completion
/mnt/ainvest_content/v3/v1/completed/completed_batch*.jsonl
```

## CLI

```bash
python -m src.main clean fresh
python -m src.main clean export
python -m src.main extract
python -m src.main complete
python -m src.main run-all
```

旧清洗命令仍兼容：

```bash
python -m src.main fresh
python -m src.main export
python -m src.main run
```

## 状态

cleaning 已实现；extraction 和 completion 目前是阶段入口与目录占位。
