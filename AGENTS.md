# AGENTS.md

本文件供后续在本仓库工作的编码 agent 阅读,概述项目当前状态、预期工作流与坑点。
内容应与真实代码保持一致;若发现与实际不符,请就地更新本文件(它被当作项目的持久记忆)。

## 项目状态

- 仓库:`aime_event`
- Remote:`https://github.com/cmy-hhxx/aime_event.git`
- 主分支:`main`(功能开发经 `feat/*` 分支走 PR 合入;最近一次为 `feat/event-dataset` → PR #4)
- 三阶段事件流水线**全部可用**,产出 `FinancialPredictionTrainingCase.v4` 事件训练包:
  - `cleaning`:清洗、去重(精确 + 可选近似)、时间排序、分片 JSONL 导出 —— 已实现
  - `extraction`:事件抽取(索引 → 聚类 → 阈值筛选 → LLM 结构化)—— 已实现
  - `completion`:事件补全(yfinance 行情打标 → v4 训练包组装 + 审计)—— 已实现

## 目录结构

```text
docs/                 各阶段说明文档 + pipeline 运行手册
schema/               各阶段 JSON Schema(cleaning / extraction / completion)
scripts/              一次性工具与压测脚本(export_window / split_export_by_type / benchmark_synthetic)
src/
  main.py             python -m src.main 兼容入口
  config.py           全局运行时与路径配置(单文件)
  cli/main.py         统一 CLI 路由(clean / extract / complete / run-all)
  common/             共享 IO、日志、路径、SQLite 等基础设施
  cleaning/           已实现的清洗流水线
  extraction/         事件抽取:index / cluster / select / structure / prompts
  completion/         事件补全:market / assemble
tests/                按阶段/common 分组的测试
```

关键文档:

- `README.md`:仓库总览与快速命令
- `docs/pipeline.md`:完整运行手册(ceph-fuse 并发红线、断点续跑、故障处理)
- `docs/cleaning.md`:清洗、去重、排序、分片与报表
- `docs/extraction.md`:索引 / 聚类 / 阈值筛选 / LLM 结构化
- `docs/completion.md`:行情拉取 / 打标 / v4 组装与审计

## 数据契约

清洗阶段(见 `src/config.py`):

```text
/mnt/ainvest_content/v1/                  原始 NDJSON 输入(INPUT_DIR)
/mnt/ainvest_content/v3/v1/               清洗后 canonical 输出(CLEANED_DIR)
/mnt/ainvest_content/v3/v1/state/         最终 state(FINAL_STATE_DIR);运行中 state 在 /tmp/aime_event/v1/state
/mnt/ainvest_content/v3/v1/reports/       统计报表(REPORTS_DIR)
```

事件抽取/补全阶段(三个根目录支持环境变量覆盖:`EVENT_V1_DIR` / `EVENT_V2_DIR` / `EVENT_OUT_ROOT`):

```text
EVENT_V1_DIR   = /mnt/ainvest_content/v3/v1            清洗后新闻语料(仅精确去重)
EVENT_V2_DIR   = /mnt/ainvest_content/v3/v2            研报/电话会段落
EVENT_OUT_ROOT = /mnt/ainvest_content/v3/event_dataset 抽取/补全产物根
  └─ index/ candidates/ selected/ structured/ market/ final/ reports/
```

清洗关键默认(`src/config.py`):

- `INPUT_DIR = /mnt/ainvest_content/v1`,`CLEANED_DIR = /mnt/ainvest_content/v3/v1`
- 运行中 state/payload 默认落 `/tmp/aime_event/v1/state`,减少 Ceph 随机 IO;结束后拷回 `FINAL_STATE_DIR`
- `PART_SIZE = 200_000`(每分片最大行数),`WORKERS = 4`,`CHUNK_SIZE = 20_000`
- 输出按 `published_at ASC` 排序,再接稳定的 tie-breaker
- 近似去重默认关闭(`NEAR_DEDUP_ENABLED = False`)

抽取关键默认(`src/config.py`):

- `EVENT_INDEX_WORKERS = 6`(index 并发上限,勿超以免击穿 ceph-fuse)
- `EVENT_ERA_SPLIT = 2023-07-01` 划分早年/近三年送审门槛
- 送审门槛:近三年 `n_articles>=5`(或 `>=3` 且有研报佐证),早年 `>=2`;`EVENT_MIN_SIGNIFICANCE = 3`

## CLI

```bash
# 清洗
python -m src.main clean fresh --workers 48 --no-near-dedup
python -m src.main clean export

# 事件抽取(四子步;阈值确定后可用 all 顺序跑)
python -m src.main extract index --fresh          # 扫描 v1+v2 建 parquet 索引(断点续跑)
python -m src.main extract cluster                # 事件候选聚类
python -m src.main extract select --sweep         # 先看阈值表,再正式跑 select
python -m src.main extract structure --limit 5    # LLM 结构化,先 --limit 验收后去掉
python -m src.main extract all                    # index -> cluster -> select -> structure

# 事件补全(fetch 需单独在本地 Mac 跑,见 docs/pipeline.md)
python -m src.main complete fetch --structured ... --outdir ...   # yfinance 拉日线面板
python -m src.main complete label                 # 离线计算 1D/5D/20D 标签
python -m src.main complete assemble              # 组装 v4 成品 + 审计
python -m src.main complete all                   # label -> assemble

python -m src.main run-all
```

清洗的旧命令仍兼容:`python -m src.main fresh|export|run`。

## 验证

优先使用本地 virtualenv:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m compileall -q src
.venv/bin/python -m pytest -q
.venv/bin/python -m src.main --help
.venv/bin/python -m src.main clean --help
```

当前分支预期:全部测试通过。

## 运行约束(坑点)

- 未被明确要求时,不要跑完整清洗/抽取流水线。
- 测试清洗/抽取行为时,用极小的临时输入,并覆盖所有输出/state 路径(含 `--final-state-dir`),避免写入 `/mnt`。
- **ceph-fuse 并发红线**:index 并发回退到 `EVENT_INDEX_WORKERS`,勿擅自调高;详见 `docs/pipeline.md`。
- `complete fetch` 依赖 yfinance,须在本地 Mac 运行,不在远端跑。
- 不要在远端机器上用 git;commit/push 都从本地 clone 做。
- 远端部署代码在 `/mnt/ainvest_content/v1/code/aime_event`;结果产物归 `/mnt/ainvest_content/v3/`。
- 不要提交 SSH key、token、主机凭据或生成的大数据产物。
- 工作区脏时保留用户既有改动,不要 reset/revert 无关文件。

## 重构须知

- extraction/completion 已有真实配置前,保持 `src/config.py` 为单一配置文件。
- 除非打包需求变化,不要引入 `src/aime_event/` 包层。
- 不要新增 `orchestration/` 包;`run-all` 若变复杂,orchestration 仍留在 `src/cli/main.py`。
- 仅当至少两个阶段都需要时,才把可复用逻辑下沉到 `src/common/`。
