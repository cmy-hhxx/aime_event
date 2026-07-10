# News/Flash 事件抽取：方案、实验与运行手册

> 数据：`data/export_2025-07-08/v1`（US_FLASH 412,144 + US_NEWS 1,689,381，2025-07-08 → 2026-06-29）
> 需求锚点：`us_stock_10_event_prediction_training_cases_20260630.html` 的 10 个训练案例
> 代码：`scripts/template_miner.py`（模板挖掘）+ `scripts/funnel_experiment.py`（漏斗）
> 结果 JSON：`reports/funnel_experiment/`

## TL;DR

Notice/8-K 只能覆盖 10 案例中的 1 个（财报）；加息、罢工、关税、反垄断等事件在 News/Flash 里。
逐条 LLM 抽取在 210 万条上不可行（2/3 非事件、一事最多被重复报道 200 次、~2B tokens）。
本方案 = **模板挖掘 + 七类硬过滤 + 双轨聚类 + 阈值门**，把 210 万条压到 **12,457 个候选簇**
（~170× LLM 调用节省），全流程本地 Mac ~10 分钟。审计：macro 轨真事件率 ~65%（达标），
general 轨 ~45%（可用，靠 triage 兜底，改进路径明确）。泛化性经时间外推实证：
模板跨期保持率 82%、抽检零误杀，换时间窗零人工重挖。

## 1. 为什么不能只用 Notice、也不能逐条抽

**需求侧**：10 案例中仅 NVDA 财报能靠 8-K 全覆盖。FOMC 决议、UAW 罢工、FDA 批准、
SEC 批准 ETP、产品发布、反垄断裁决、政府计划、关税——主体是央行/工会/法院/政府，不发 SEC 文件。

**Notice 逐条抽取成立的三个前提，News/Flash 全部不满足**（均为实测）：

| Notice 前提 | News/Flash 现实 | 实测证据 |
|---|---|---|
| 一份文件 ≈ 一个事件 | 2/3 的记录不是事件 | 随机 60 条 FLASH：数据播报 27% + 发言引述 18% + 传闻 7% + 价格异动 5% |
| 全网唯一（accession） | 一事被报几十到几百次 | 2025-12-10 FOMC 日：决议本体 27 条改写、Powell 记者会拆成 132 条逐句快讯 |
| 主体/时间权威 | 提及≠主体、发布≠事件时间 | "Wells Fargo"+"shutdown" 共现 1,193 条，绝大多数是分析师顺带提及 |

逐条 LLM = 210 万次调用（~2B tokens），且产物仍需去重/聚合/佐证。这三道工序放 LLM 前
（便宜的近似）就是漏斗；每个逐条问题对应一层过滤或一个聚类机制——架构是被问题逼出来的，不是设计偏好。

## 2. 漏斗设计与最终逐层过滤数

```text
L0 原始 → L1 有效性 → L2 模板tags → L3 七类硬过滤正则 → L3b 挖掘模板 → L4 长度准入
→ L5 精确去重 → L6 轨道分配(macro主题桶/general) → 聚类(3天滑窗+稀有词锚定连边+跨桶合并) → n≥K 门
```

| 层 | US_FLASH | US_NEWS | 说明 |
|---|---:|---:|---|
| L0 原始 | 412,144 | 1,689,381 | |
| L2 模板 tags | −27,880 | −244,586 | Technical Analysis / today_mover 等 |
| L3 硬过滤（7 类正则） | −64,250 | −262,392 | 数据播报/价格异动/复盘/评级/例行PR/持仓流/垃圾模板 |
| L3b 挖掘模板（820 个） | −12,413 | −16,240 | 数据驱动，见 §4 |
| L4 过短 | −9,938 | −27,998 | body<200 且标题<40 |
| L5 精确重复 | −1,221 | −13,017 | 同日同词袋 |
| **入池** | **296,984** | **1,125,280** | 其中 FLASH 17 万条仅凭标题准入（原管道 body≥200 会全丢） |
| L6 macro 轨 | 35,936 | 85,947 | 10 主题正则桶 + tags 召回门（importance=high） |

聚类后阈值扫描（池 142 万 → 候选簇）：

| 门槛 | 簇数 | macro | general |
|---|---:|---:|---:|
| n≥3 | 37,209 | 3,341 | 33,868 |
| **n≥5（推荐起步）** | **12,457** | **1,162** | **11,295** |
| n≥8 | 5,159 | 517 | 4,642 |

关键机制备注：

- **稀有词锚定连边**（general 轨）：连边须共享 ≥1 个 df 低于池的 0.14% 的"主体词"。
  没有它，"Why Iamgold Stock Surged Today" 和 "Why Deere Stock Surged Today" Jaccard=0.5 直接连边，
  模板标题经传递闭包成 53,598 条巨簇（第 1 轮实验实测）；加上后最大簇降到 9k 以下。
- **数据硬伤**：本导出 NEWS 全量 0 条个股实体（`entities.stocks` 不存在），FLASH 仅 17% 有——
  所以 general 轨用稀有词分桶替代 symbol 分桶。上游修实体后应切回主体分桶（见 §6）。

## 3. 质量审计：是否达标

分层抽样 100 簇（macro 40 + general 60）逐簇人工分类：

| 轨道 | 真事件率 | 主要噪声 | 判定 |
|---|---|---|---|
| macro（1,162 簇） | **~65%** | 国际数据播报漏网 12%、Market Talk 评论、`strike/treasury` 词义歧义 | ✅ 可直接送 triage |
| general（11,295 簇） | **~45%** | 模板流退化成"每主体一小簇"存活（评级变体/例行公司流/追踪流） | ⚠️ 可用，triage 兜底 |

macro 轨样本中的真事件：美欧 15% 关税协议、钢铝铜 25% 关税、Glencore 罢工、Fed 主席提名、
政府停摆、伊朗停火破裂——**加息/罢工/关税这类当初 Notice 抽不到的目标事件确实抽到了**。
general 轨真事件：Spirit 二次破产、Cencora $3.5B 并购、CME 24/7 加密期货、三星 HBM4 认证等。

估算：12,457 簇中 ~6,000 个真事件簇。全量送 triage 浪费 ~50% 调用（flash 模型可接受）；
triage prompt 应把"模板流/例行公告/数据播报/追踪流"写成显式拒绝类（簇内标题"同构多主体"特征 LLM 一眼可判）。

## 4. 泛化性（要在近几年数据上跑）

手写正则是本窗口打地鼠所得，**部分**泛化（`est.`/`y/y`/`target price` 等财经行文惯例稳定；
"Laps the Stock Market" 等媒体栏目名会过期）。方案的泛化单位是**流程**而非规则：

1. **模板骨架挖掘**（零人工）：标题掩码（数字→#，大写词串→@）成骨架，
   满足［≥20 次 + ≥10 个不同主体 + 头部来源占比 ≥60% + ≥3 个固定词］判模板。
   **时间外推实证**（H1=2025 下半年挖，H2=2026 上半年验证）：跨期保持率 **82.1%**、
   抽检 40 条**零误杀**、漂移多为已知家族措辞变体。每个新窗口重挖 ~1 分钟。
2. **校准循环**（脚本化）：新窗口跑漏斗看 top25 大簇 → 补正则 → 重跑，每轮 ~5 分钟，
   2-3 轮收敛（本次实践 53,598→9,159）。
3. **常量自适应**：稀有词阈值为池大小相对比例；n≥K 按窗口 sweep（老年份新闻稀应降 K，
   对齐现有管道 `EVENT_ERA_SPLIT` 先例）。
4. **两个漂移监控**：逐层计数表当数据指纹（层淘汰率突变 = schema/内容漂移告警，
   NEWS 实体缺失就是这样发现的）；FOMC/CPI 日历召回验收任何年份免费可用。

## 5. 运行手册

依赖：`rapidfuzz`（已在 requirements.txt）。从仓库根目录运行，产物写 `output/`（已 gitignore）。

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
# 产物: output/funnel.json     逐层计数+阈值扫描+top25 大簇
#       output/clusters_ge5.jsonl  size>=5 候选簇(含采样标题), 下游 triage 的输入
#       output/funnel/pool.jsonl   事件池(id/date/title/track/bucket)

# ③(可选) 泛化性 holdout 实验: H1 挖 H2 验证
python3 scripts/template_miner.py \
    --v1-dir data/export_2025-07-08/v1 \
    --mode holdout --out output/holdout.json

# 换时间窗/新数据: 改 --v1-dir 重复 ①②; 只重跑聚类可加 --skip-a 复用池
```

## 6. 下一步（按优先级）

1. **triage 接入**：`clusters_ge5.jsonl` 直接作 LLM triage 输入（判"可定日期离散事件"+
   显式负类 + significance），从 n≥5 起步验收，再决定放宽到 n≥3。
2. **实体回填**：v2 entities + FLASH 17% 已有 stocks 建 (symbol,name) 词典回填 NEWS 主体，
   general 轨升级为主体分桶，从机制上分开"一主体多报道"与"一模板多主体"。
3. **规则固化回主管道**：L2/L3/L3b + 稀有词锚定合入 `src/extraction/cluster.py`，
   逐层计数表作为每次运行的质量监控报表。
4. macro 轨小修：国际数据播报正则、`— Market Talk` 后缀、`treasury share` 歧义排除。
5. 官方锚点（`official_source_url`）补齐：日历类锚点静态给出，其余 backlog。

## 附：历史实验记录（结果 JSON 见 reports/funnel_experiment/）

| 文件 | 内容 |
|---|---|
| `funnel_iter{1,2,3}.json` | 三轮规则迭代的逐层计数与聚类统计（最大簇 53,598→16,341→9,159） |
| `funnel_final.json` | 最终生产序列（含 L3b 模板层）结果 |
| `miner_full.json` | 时间外推 holdout 实验（保持率 82.1%） |
| `audit_sample.json` | 100 簇人工审计抽样 |
