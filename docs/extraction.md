# Extraction 阶段

事件抽取采用**漏斗式**四子步（不是逐条调 LLM）：约 2460 万条清洗后新闻先索引、再聚类成
事件候选簇、按阈值筛选、最后只对入选事件做 LLM 结构化。

```text
输入：/mnt/ainvest_content/v3/v1/cleaned_batch*.jsonl (新闻)
     /mnt/ainvest_content/v3/v2/cleaned_batch*.jsonl (研报/电话会段落, 佐证信号)
输出：/mnt/ainvest_content/v3/event_dataset/{index,candidates,selected,structured}/
```

## extract index（src/extraction/index.py）

全量扫描 v1+v2 建 parquet 轻量索引（含文件字节偏移，下游 seek 直读正文不再全扫）。
v2 段落按 doc_id 聚合到文档级；同时统计 (symbol,name) 词典分片。

- 断点续跑：按输出文件存在性跳过；`--fresh` 全部重建；写入原子（.tmp+rename）
- `--workers` 缺省取 `EVENT_INDEX_WORKERS=6`，**ceph-fuse 红线禁超 10**（见 pipeline.md）
- 脏时间戳（<2000 或 >2026-08）在 `norm_date` 置空，下游按无日期过滤

## extract cluster（src/extraction/cluster.py）

从索引过滤"事件相关"新闻池（去技术面模板 tags、纯 crypto、无正文/无日期），
公司轨按 symbol 分桶（一篇文章进其全部 symbol 桶）、宏观轨按主题正则分桶；
桶内按时间排序，3 天滑窗 + 标题 token Jaccard(≥0.15) 预筛 + token_set_ratio(≥72)
连边并查集聚簇；跨桶按成员重合率(≥0.3)合并碎片簇；join v2 研报/电话会数量作
`n_v2_reactions` 佐证信号。

- 输出 `candidates/clusters.parquet`（簇统计）+ `members.parquet`（成员及正文偏移）
- 读 parquet 不扫原始文件，`--workers 32` 安全

## extract select（src/extraction/select.py）

**阈值抽取，无数量/时代配额**：

1. 规则送审门（config 可调）：近三年(≥EVENT_ERA_SPLIT) `n_articles≥5` 或
   `≥3 且有研报佐证`；早年 `n_articles≥2`
2. LLM 粗筛（triage，`OPENAI_MODEL` 即 flash）：判定是否"可定日期的离散市场事件"，
   给 event_type/subject/日期/significance(1-5)
3. 入选 = `is_valid_event 且 significance≥EVENT_MIN_SIGNIFICANCE(3)`
4. 质量护栏（非配额）：同 (类型,日期,主体) 去重保分高者；单 symbol 上限
   `EVENT_PER_SYMBOL_CAP(12)`，设 0 关闭

- `--sweep`：输出阈值→送审量对照表（零 API 调用），供人工定阈值
- `--dry-run`：只统计送审量
- triage 结果逐条落盘 `selected/triage.jsonl`，中断重跑自动续（按 event_id 跳过）
- 产物 `selected/selected_events.jsonl`，格式见 `schema/extraction/selected_event.schema.json`

## extract structure（src/extraction/structure.py）

对每个入选事件取簇内 top8 文章（来源质量排序 + 标题去重），seek 直读 v1 原文并清洗
HTML（每篇截 2800 字符），调 `OPENAI_MODEL_STRUCTURE`（deepseek-v4-pro）产出：
main_event 事实块（facts/channels）、事件时间戳估计、8-14 个关系标的行（含 ≥1 行业 ETF）。

- prompt 硬性泄露规则：facts 仅限事件日当天及之前可知信息，禁止价格反应/事后评级/后续进展；
  关系行只写影响链不打方向强度（方向由标签学习）
- 断点续跑同上；全量前先 `--limit 5` 人工验收
- prompts 文本集中在 `src/extraction/prompts.py`

## News/Flash 漏斗实验工具（scripts/，本地 Mac 运行）

针对 Notice 覆盖不了的事件（加息/罢工/关税/反垄断等），对原始 v1 导出的 US_FLASH+US_NEWS
做"模板挖掘 → 逐层过滤 → 双轨聚类 → 阈值扫描"，产出候选事件簇供 triage。
方案、逐层过滤数、审计与泛化实证见 `docs/news_flash_event_extraction.md`。

```bash
mkdir -p output

# ① 挖模板(全窗口, ~1 分钟)
python3 scripts/template_miner.py \
    --v1-dir data/export_2025-07-08/v1 \
    --mode mine --emit output/templates.txt --out output/miner.json

# ② 漏斗: 逐层过滤+聚类+阈值扫描(~5 分钟)
python3 scripts/funnel_experiment.py \
    --v1-dir data/export_2025-07-08/v1 \
    --tmpdir output/funnel \
    --templates output/templates.txt \
    --dump output/clusters_ge5.jsonl \
    --out output/funnel.json
# 产物: output/funnel.json          逐层计数+阈值扫描+top25 大簇
#       output/clusters_ge5.jsonl   size>=5 候选簇(含采样标题), 下游 triage 输入
#       output/funnel/pool.jsonl    事件池(id/date/title/track/bucket)

# ③(可选) 泛化性 holdout 实验: 上半窗挖模板, 下半窗验证跨期保持率
python3 scripts/template_miner.py \
    --v1-dir data/export_2025-07-08/v1 \
    --mode holdout --out output/holdout.json
```

- 换时间窗/新数据：改 `--v1-dir` 重复①②；只调聚类参数可加 `--skip-a` 复用已有池
- 全量实测：210 万条 → n≥5 共 12,457 候选簇（macro 1,162 / general 11,295）
- 本导出 NEWS 无 `entities.stocks`（FLASH 仅 17% 有），general 轨以稀有词分桶替代 symbol 分桶

## 与 completion 的衔接

`structured/structured.jsonl` 每行一个结构化事件（含 `_triage`/`_source_meta` 溯源），
是 complete fetch/label/assemble 的唯一输入。
