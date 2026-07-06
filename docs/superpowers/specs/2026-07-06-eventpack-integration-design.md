# 设计文档：event_dataset 流水线融入 aime_event

- 日期：2026-07-06
- 状态：已获用户批准（会话内逐节确认）
- 目标分支：`feat/event-dataset`（自 `refactor/pipeline-stages` 切出）

## 1. 背景与目标

aime_event 已完成 cleaning 阶段重跑，产出 `/mnt/ainvest_content/v3/v1/cleaned_batch*.jsonl`
（123 文件，~2456 万条，仅精确去重）。独立开发的 event_dataset 六阶段流水线
（索引→聚类→筛选→结构化→行情打标→组装）需要融入 aime_event，成为其
extract / complete 两个阶段的正式实现，最终产出
`FinancialPredictionTrainingCase.v4.three_year_event_signal_pack` 格式的事件训练包。

交付物：合入的代码 + 用户可直接照跑的完整命令序列（用户自行运行所有阶段）。

## 2. 已确认的决策记录（2026-07-06 用户确认）

| # | 决策项 | 结论 |
|---|---|---|
| 1 | 融入方式 | 替换式：新实现接管 `src/extraction/` 与 `src/completion/` |
| 2 | 旧代码去留 | 直接删除（git 历史可找回），client.py 重试/JSON 解析逻辑吸收进 `src/common/llm.py` |
| 3 | 抽取数量 | **不设数量/时代配额，靠阈值抽取**，总量为阈值的自然结果 |
| 4 | triage 入选门槛 | `is_valid_event=true 且 significance≥3` |
| 5 | 模型 | triage=`deepseek-v4-flash`；structure=`deepseek-v4-pro`（key 已验证两者可用） |
| 6 | git | 新分支 `feat/event-dataset`，验收后合并 |
| 7 | 增强层 | 沿用前期决策：分时面板/弱关联搜索链/混杂审计留空占位（status 标记） |
| 8 | 行情源 | yfinance，fetch 子步在用户本地 Mac 跑（服务器无法访问 Yahoo） |

## 3. 仓库结构（融入后）

```
aime_event/
  config.py                 # 新增 EVENTPACK 配置区段（路径/阈值/模型/并发红线）
  src/
    cleaning/               # 不动
    extraction/             # 替换: index.py / cluster.py / select.py / structure.py / prompts.py
    completion/             # 替换: market.py（fetch+label 两个子命令）/ assemble.py
    common/                 # 复用 io/logging/paths；新增 llm.py（并发+逐条落盘断点续跑）
    cli/main.py             # 重写 extract / complete 子命令组
  schema/extraction/selected_event.schema.json    # 更新：筛选产物格式
  schema/completion/final_case.schema.json        # 新增：v4 成品关键字段校验
  docs/{extraction,completion,pipeline}.md        # 重写；运行手册内容并入
  tests/{extraction,completion}/                  # 替换为新单测
```

CLI（沿用 `python -m src.main` 惯例）：

```
python -m src.main extract index|cluster|select|structure|all
python -m src.main complete fetch|label|assemble|all
```

`complete fetch` 支持 `--structured/--outdir`，在本地 Mac 运行后将
`prices_daily.parquet` 传回容器；因此 `complete all` 仅串联 label+assemble，
fetch 必须单独执行。`extract all` 串联四步但 select 首次需先人工看 `--sweep` 定阈值，
故 all 仅用于阈值确定后的重跑场景。

删除清单：旧 `src/extraction/{pipeline,prompt,models,output,client}.py`、
`src/completion/{models,output,pipeline}.py`、`tests/extraction/test_pipeline.py`、
旧 `schema/extraction/event_record.schema.json`、`schema/completion/completed_event.schema.json`。

独立的 event_dataset 目录（`/mnt/ainvest_content/v1/code/event_dataset` 与本地
`~/同花顺实习/projects/event_dataset`）在合入后归档不再维护。

## 4. 数据输入适配

- v1 输入：`/mnt/ainvest_content/v3/v1`（此前为已删除的 `no-near-dedup/` 子目录）。
  仅精确去重的语料对聚类有利：同事件多篇报道保留为热度信号。
- v2 输入：`/mnt/ainvest_content/v3/v2` 不变（研报/电话会段落，按 doc_id 聚合）。
- 输出根：`/mnt/ainvest_content/v3/event_dataset/{index,candidates,selected,structured,market,final,reports}` 不变。
- **旧 35 个索引分片作废**（指向已删除的旧文件，字节偏移失效）：index 步首次运行用 `--fresh`。

## 5. 各阶段规格

### 5.1 extract index（原 Stage A）
- 输入：v1 123 文件 + v2 29 文件；输出：记录级/doc 级 parquet 索引 + (symbol,name) 词典分片。
- 断点续跑（按输出文件存在性跳过）+ 原子写（.tmp+rename）。
- **ceph-fuse 并发红线：默认 workers=6**。实测 48 并发全员 D 状态卡死、挂载退化至
  1.7MB/s；12 并发仍拥塞（FUSE max_background=12）。workers 与容器核数无关。

### 5.2 extract cluster（原 Stage B）
- 事件新闻池过滤：类型 ∈ {US_NEWS,US_FLASH,US_ARTICLE,US_ROBOT}、有日期、body≥200 字、
  剔除技术面模板 tags、剔除纯 crypto。
- 公司轨按 symbol 分桶（文章进其全部 symbol 桶）、宏观轨按主题正则分桶；
  桶内 3 天滑窗 + 标题 token Jaccard 预筛 + token_set_ratio≥72 连边，并查集聚簇；
  跨桶按成员重合率≥0.3 合并；v2 研报/电话会按 symbol×日期窗 join 出佐证计数。
- workers=32 可用（读 parquet 索引，不扫原始大文件）。

### 5.3 extract select（原 Stage C，本次核心重设计：阈值抽取）
- **送审门（规则阈值，config 可调）**，初始值：
  - recent（peak_date≥2023-07-01）：`n_articles≥5` 或 `(n_articles≥3 且 n_v2_reactions≥1)`
  - early（<2023-07-01）：`n_articles≥2`
- `select --sweep`：不调 API，输出阈值→候选量对照表（按时代×阈值组合），
  用户看表定阈值后正式跑。
- **入选门（LLM）**：triage（flash）判定 `is_valid_event=true 且 significance≥3`。
- 不设数量配额、时代配额、earnings 占比上限。仅保留两个质量护栏：
  同 `(event_type,event_date,主体)` 去重；单 symbol 事件数上限（默认 12，设 0 关闭）。
- triage 结果逐条落盘 `selected/triage.jsonl`，断点续跑。

### 5.4 extract structure（原 Stage D）
- 每事件取簇内 top8 文章（来源质量优先、标题去重），seek 直读 v1 原文，
  HTML 清洗后每篇截断 2800 字符。
- structure 模型 `deepseek-v4-pro`；prompt 硬性泄露规则：facts 仅限事件日当天及之前
  可知信息，禁止价格反应/事后评级/后续进展；关系标的 8-14 个含 ≥1 行业 ETF，
  只写影响链不打方向强度。
- 全量前先 `--limit 5` 由用户人工验收质量。

### 5.5 complete fetch / label（原 Stage E）
- fetch（本地 Mac）：汇总全部事件 symbol 去重后逐 symbol 拉全时段日线
  （auto_adjust=True，与样例对齐已验证：NVDA 2024-03-18 close 88.302 vs 样例 88.3022），
  增量续拉，产出 `market/prices_daily.parquet` 传回容器。
- label（容器）：base_close 规则——pre_market 事件用前一交易日收盘，其余用事件日
  （非交易日则其前最近交易日）；first_tradable 为其后首个交易日。
  产出 S0 前置 15 交易日 OHLCV、标签审计窗 26 交易日、1D/5D/20D close-to-close 与
  open-to-close 收益、开盘跳空、截面 20D 排名。有效标的 <3 的事件作废。

### 5.6 complete assemble（原 Stage F）
- 组装 v4 全字段；增强层字段留空占位 + status 标记（与样例
  `intraday_volume_panel.status="missing_..."` 做法一致）。
- 审计：facts 未来日期泄露扫描、标签完整性、结构完整性（facts≥2/channels≥2/关系行≥5、
  已打价≥3）、case_id 唯一化。
- 产出 `final/<CASE_ID>.json` + `final/manifest.jsonl` + 汇总报表。

## 6. LLM 配置

`.env` 新增 `OPENAI_MODEL_STRUCTURE=deepseek-v4-pro`；`OPENAI_MODEL` 继续作为
triage 与缺省回退。`src/common/llm.py` 提供 `chat_json`（重试退避+JSON 提取）与
`run_checkpointed`（线程池并发+逐条落盘+按 key 跳过已完成）。

## 7. 测试与验收

- pytest 单测：聚类连边（tokens/Jaccard/并查集）、base_close/first_tradable 日历规则、
  收益标签计算、泄露扫描、v2 doc 聚合。旧 extraction 测试删除。
- 冒烟：各阶段 `--limit` 小样本；`python -m compileall -q src`；`python -m src.main --help`。
- 全量后验收：随机 3 个 final JSON 对照 `事件格式.json` 逐字段核对；
  `reports/stage_*_summary.json` 汇总数字合理性。

## 8. git 计划

`refactor/pipeline-stages` → 切 `feat/event-dataset`；提交序列：
① 删旧 extraction/completion ② 新 extraction 四步 ③ 新 completion 三步
④ CLI/config/schema ⑤ docs/tests。用户验收后决定合并。

## 9. 运行序列（交付时提供）

实现完成后在 docs/pipeline.md 提供完整命令序列：环境准备（新容器重建 venv）→
`extract index --fresh --workers 6` → `extract cluster` → `extract select --sweep`
（用户定阈值）→ `extract select` → `extract structure --limit 5`（人工验收）→
`extract structure` → 本地 `complete fetch` → 传回 → `complete label` →
`complete assemble` → 验收材料清单。

## 10. 风险与已知边界

- 早年（2014-2020）新闻稀疏且以无正文 SEC 公告为主，阈值抽取下早年事件数自然偏少，属预期。
- ceph-fuse 并发红线必须遵守（见 5.1）；新容器可从 6 逐步试探到 10。
- yfinance 对退市/改名标的缺数据：按 `unpriced_weak_candidate_needs_price_panel` 降级，
  不阻塞样本（有效标的 <3 才作废）。
- LLM 结构化的 facts 质量依赖 prompt 泄露规则 + assemble 泄露扫描双重防线，
  仍需人工抽检兜底。
