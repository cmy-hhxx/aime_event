# AGENTS.md

本文件供后续在本仓库工作的编码 agent 阅读,概述项目当前状态、预期工作流与坑点。
内容应与真实代码保持一致;若发现与实际不符,请就地更新本文件(它被当作项目的持久记忆)。

## 项目状态

- 仓库:`aime_event`
- Remote:`https://github.com/cmy-hhxx/aime_event.git`
- 主分支:`main`(功能开发经 `feat/*` 分支走 PR 合入)
- 三阶段事件流水线**全部可用**,产出 `FinancialPredictionTrainingCase.v4` 事件训练包:
  - `cleaning`:清洗、去重(精确 + 可选近似)、时间排序、分片 JSONL 导出 —— 已实现
  - `extraction`:事件抽取(索引 → 聚类 → 阈值筛选 → LLM 结构化)—— 已实现
  - `completion`:事件补全(yfinance（或同花顺内部 api） 行情打标 → v4 训练包组装 )—— 已实现
- News/Flash 事件漏斗已完成本地实验,但**尚未固化进 `src/extraction/cluster.py` 主管道**:
  - `scripts/template_miner.py`:标题模板骨架挖掘 + 跨期 holdout 验证
  - `scripts/funnel_experiment.py`:七类硬过滤、双轨聚类、阈值扫描与候选簇导出
  - 210 万条 US_FLASH/US_NEWS 实测在 `n>=5` 时产出 12,457 个候选簇;详见 `docs/news_flash_event_extraction.md`

## 目录结构

```text
docs/                 各阶段说明文档 + pipeline 运行手册
schema/               各阶段 JSON Schema(cleaning / extraction / completion)
scripts/              数据导出、News/Flash 漏斗实验与模板挖掘工具
src/
  main.py             python -m src.main 兼容入口
  config.py           全局运行时与路径配置(单文件)
  cli/main.py         统一 CLI 路由(clean / extract / complete;run-all 仅保留退出提示)
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
- `docs/news_flash_event_extraction.md`:News/Flash 漏斗方案、实验数据、质量审计、泛化验证与运行命令
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

# run-all 已移除;select 阈值需人工确认,请按 docs/pipeline.md 分阶段执行
```

清洗的旧命令仍兼容:`python -m src.main fresh|export|run`。

News/Flash 漏斗实验(本地 Mac 运行,产物写入已忽略的 `output/`):

```bash
python3 scripts/template_miner.py --v1-dir <window>/v1 \
  --mode mine --emit output/templates.txt --out output/miner.json
python3 scripts/funnel_experiment.py --v1-dir <window>/v1 \
  --tmpdir output/funnel --templates output/templates.txt \
  --dump output/clusters_ge5.jsonl --out output/funnel.json
```

只调聚类参数时可加 `--skip-a` 复用事件池;完整命令和产物契约见 `docs/news_flash_event_extraction.md`。

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

当前 `main` 在本地 `.venv` 的实测基线(2026-07-11):`52 passed, 6 failed`。失败项是已存在状态,不是文档更新引入:

- 3 个 cleaning 测试仍按“默认启用近似去重”编写,但 `NEAR_DEDUP_ENABLED=False`,因此 signature 为空且 pipeline 多保留 1 条。
- 2 个 CLI help 测试期望显示 `--no-near-dedup`,但该参数当前被 `argparse.SUPPRESS` 隐藏。
- 1 个 config 测试被本地 `.env` 的 `EVENT_*` 窗口路径覆盖;且 python-dotenv 不会展开其中的 `$PWD`,当前读到字面量 `$PWD/...`。

后续修复时先确定“代码默认行为”还是“旧测试期望”为真实契约,不要为了让测试变绿盲目重启全量近似去重。

## 运行约束(坑点)

- 未被明确要求时,不要跑完整清洗/抽取流水线。
- `run-all` 已从 CLI 移除,`select --sweep` 后必须人工确认阈值,不要恢复无人值守的全链路默认。
- 测试清洗/抽取行为时,用极小的临时输入,并覆盖所有输出/state 路径(含 `--final-state-dir`),避免写入 `/mnt`。
- **ceph-fuse 并发红线**:index 并发回退到 `EVENT_INDEX_WORKERS`,勿擅自调高;详见 `docs/pipeline.md`。
- `complete fetch` 依赖 yfinance,须在本地 Mac 运行,不在远端跑。
- 不要在远端机器上用 git;commit/push 都从本地 clone 做。
- 远端部署代码在 `/mnt/ainvest_content/v1/code/aime_event`;结果产物归 `/mnt/ainvest_content/v3/`。
- 不要提交 SSH key、token、主机凭据或生成的大数据产物。
- 工作区脏时保留用户既有改动,不要 reset/revert 无关文件。
- `reports/` 和 `output/` 是实验/运行产物,已由 `.gitignore` 排除;不要把大结果回提到仓库。

## 重构须知

- 保持 `src/config.py` 为 extraction/completion 的单一配置文件。
- 除非打包需求变化,不要引入 `src/aime_event/` 包层。
- 不要新增 `orchestration/` 包,也不要恢复 `run-all` 的无人值守编排;select 的人工阈值门保留在分阶段 CLI 流程中。
- 仅当至少两个阶段都需要时,才把可复用逻辑下沉到 `src/common/`。
- News/Flash 漏斗的当前下一步是先验收 `clusters_ge5.jsonl` 的 LLM triage,再决定是否放宽到 `n>=3`;未验收前不要直接替换主管道聚类逻辑。
