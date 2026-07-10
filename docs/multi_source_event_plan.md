# 非 Notice 事件抽取方案（宏观 / 监管 / 罢工 / 政策 / 产品发布）

> 数据源：`data/export_2025-07-08`（覆盖 2025-07-08 → 2026-06-29，约一年）
> 需求锚点：`us_stock_10_event_prediction_training_cases_20260630.html` 的 10 个训练案例
> 结论一句话：**不需要新建管道。现有 extract 漏斗（index→cluster→select→structure）本来就吃全量 v1 新闻，问题出在三个可定位的召回缺口上；补上缺口 + 新增一条"日历驱动"轨道，即可覆盖 10 案例中 Notice 覆盖不了的 8 类事件。**

---

## 1. 需求侧：10 个案例的事件族 vs Notice 覆盖能力

解析 HTML 内嵌 JSON（`<script id="data">`）得到 10 个案例：

| # | event_family | event_type | 案例 | 8-K/Notice 能否覆盖 |
|---|---|---|---|---|
| 1 | central_bank_policy | monetary_policy_statement | FOMC 加息至 5.25%-5.50% | ❌ Fed 不发 SEC 文件 |
| 2 | labor_supply_chain | labor_strike | UAW Stand Up Strike | ❌ 罢工方是工会，车企 8-K 通常滞后且不完整 |
| 3 | biotech_regulatory | fda_approval | FDA 批准 Zepbound | ⚠️ 部分：公司常发 8-K，但锚点是 FDA 公告 |
| 4 | crypto_market_structure | regulatory_approval | SEC 批准现货 Bitcoin ETP | ⚠️ 部分：发行人有 424B/8-A，事件本体是 SEC 命令 |
| 5 | ai_hardware_platform | product_platform_launch | NVIDIA Blackwell 发布 | ❌ 产品发布会不走 8-K |
| 6 | consumer_ai_platform | developer_platform_release | Apple Intelligence 发布 | ❌ 同上 |
| 7 | platform_antitrust | antitrust_court_decision | Google 搜索垄断裁决 | ❌ 法院判决不是公司公告 |
| 8 | ai_infrastructure_capex | ai_datacenter_program | Stargate Project | ❌ 白宫/合资宣布，无 8-K 锚点 |
| 9 | macro_trade_policy | tariff_policy_change | 互惠关税调整 | ❌ 行政命令 |
| 10 | ai_earnings_guidance | earnings_release | NVIDIA FY2027 Q1 财报 | ✅ 8-K Item 2.02 |

**10 个里只有 1 个能靠 Notice 全覆盖**。案例对事件记录的要求（`main_event` 结构）：
`event_subject`（可以是 Fed/工会/法院，不必是上市公司）、`event_type`、`event_date`、
`facts_publicly_reported`（仅事件日当天可知）、`event_influence_channels`（只写通道不打方向）、
`official_source_url`（官方锚点，evidence_grade=official_or_primary）。
前 5 项现有 structure 阶段的产出结构已对齐；**官方锚点 URL 是唯一数据里没有的东西**（见 §5.4）。

## 2. 供给侧：export_2025-07-08 里有什么

| 文件 | 条数 | 对非 Notice 事件的价值 |
|---|---|---|
| v1/US_FLASH | 412,144 | ★★★ 一句话快讯，时效最强；自带 `tags`（CentralBanking / Monetary Policy / Regulation / Policy / Trade Agreements / GeopoliticalConflict / Earnings…）+ `importance`（high 37% / mid 51% / low 12%）+ `regions` |
| v1/US_NEWS | 1,689,381 | ★★★ 正文完整的新闻，宏观/公司/加密都有 |
| v1/US_ARTICLE | 1,131,603 | ★★ 深度分析，作佐证与背景，不作事件锚点 |
| v1/US_ROBOT | 2,815,853 | ★ 模板播报，基本无增量 |
| v1/US_POST | 2,589,595 | ★ 社媒帖子，案例里只用于"社媒传播"扩散链（R3-R4），不作事件源 |
| v2/report + teleconference | 974,161 | ★★ 佐证信号（现有 `n_v2_reactions` 已在用） |

宏观信号量（US_FLASH 全文件关键词计数）：**tariff 21,722 / strike 9,028 / rate cut 7,662 / CPI 3,670 / rate hike 3,485 / FOMC 1,339 / antitrust 1,110 / nonfarm 388 / "FDA approv" 283**。
→ 原始信号非常充足，问题只在于管道有没有把它们放进事件池。

## 3. 差距诊断：现有管道为什么抽不到这些事件

对 `src/extraction/cluster.py` 的核对结论，三个缺口按影响排序：

### 缺口 A：`body_len >= 200` 把 74% 的 FLASH 挡在池外（实测 10 万条抽样：74.3% body<200）
FLASH 是 title≈body 的一句话事件（"Fed cuts rates 25bp" 这种），天然过不了 200 字门槛。
**最及时、信噪比最高的宏观源基本整体缺席。**

### 缺口 B：宏观轨只有 5 个主题桶，且全靠标题英文正则
现有 `MACRO_TOPICS`：fed_policy / inflation_jobs / trade_tariff / regulation_policy / energy_geo。
对照 10 案例缺：**labor_strike、fda_regulatory、crypto_market_structure、gov_program/ai_capex、geopolitical_conflict**。
另外正则只匹配标题，中文标题的 NEWS（数据中英混合）全部漏召回。

### 缺口 C：FLASH 的结构化 tags / importance 没有参与召回
67% 的 FLASH `entities=null` → 进不了公司轨；标题又常常不含正则关键词（如 "Powell: …" 的发言快讯）。
但这些记录自带 `Monetary Policy` / `Regulation` 等 tags 和 `importance=high`，是现成的召回信号。
注意：**tags 噪声偏高**（实测有 Taiwan 政治新闻被打上 CentralBanking），只能作召回门，精度交给聚类+LLM triage。

### 结构性问题 D：日历型宏观事件不该靠"聚类发现"
FOMC 决议、CPI/NFP 发布的**日期是完全可预知的公开日历**。用滑窗聚类去"发现"FOMC 是绕远路：
决议日有上千条 flash，聚类容易碎片化（声明/记者会/点阵图各聚一簇），阈值难调。
可预知事件应该反过来：**先有日历锚点，再按日期窗口检索证据**。

## 4. 方案总览：双轨制

```text
                    ┌─ 轨道 1（新增）日历驱动 scheduled ──> FOMC / CPI / NFP / 财报
非 Notice 事件 ──┤
                    └─ 轨道 2（增强）聚类发现 unscheduled ─> 罢工 / 关税 / 反垄断 / FDA / 产品发布 / 政府计划
```

### 4.1 轨道 1：日历驱动（新子步 `extract scheduled`，或并入 cluster 前置）

1. **内置事件日历**（一次性静态数据，窗口内全部可枚举）：
   - FOMC 决议日：2025-07-30, 09-17, 10-29, 12-10, 2026-01-28, 03-18, 04-29, 06-17（8 次，官网日历核对后写死在 config/静态文件）
   - CPI / NFP / PCE 发布日：BLS/BEA 官网日历，同样窗口内可枚举
   - （财报走既有 8-K 管道，不进此轨）
2. 对每个日历锚点，在 `[T-1, T+1]` 窗口内检索：`importance=high` 且（tags 命中 CentralBanking/Monetary Policy 或标题命中该主题正则）的 FLASH/NEWS，直接构成事件簇（跳过相似度聚类）。
3. 簇直通 select 的 LLM triage（不受 n_articles 门槛限制——日历事件必真实），再走 structure。
4. 好处：召回率 100%（日期已知）、`event_date` 零误差、`official_source_url` 可以随日历一起静态给出（federalreserve.gov / bls.gov 的 URL 模式固定），直接满足案例的 official_or_primary 要求。

### 4.2 轨道 2：聚类发现增强（改 `cluster.py`，三处小改动）

1. **FLASH 准入**（对应缺口 A）：池过滤条件改为
   `body_len >= 200 OR (content_type = 'US_FLASH' AND length(title) >= 40)`。
   预估新增池行数 ~30 万级，聚类是 O(桶内滑窗)，可控。
2. **主题桶扩充**（对应缺口 B），`MACRO_TOPICS` 新增：
   ```python
   "labor_strike":    r"\b(strikes?|walkouts?|work stoppage|lockout|uaw|teamsters|union (vote|contract|deal|action))\b",
   "fda_regulatory":  r"\b(fda (approv|reject|clear)|advisory committee|complete response letter|crl|biologics license)\b",
   "crypto_structure":r"\b(spot (bitcoin|ether|ethereum) et[fp]|crypto legislation|stablecoin (bill|act)|genius act)\b",
   "gov_program":     r"\b(stargate|executive order on ai|chips act|infrastructure (bill|act)|stimulus)\b",
   "geopolitical":    r"\b(sanctions?|export ban|military (strike|action)|ceasefire|invasion)\b",
   ```
   ⚠️ `strike` 一词多义（罢工/军事打击/期权行权价），labor_strike 桶信噪比会偏低——没关系，
   聚类相似度会把不同语义拆开，triage 负责最终判伪。先跑 `--sweep` 看送审量再定。
3. **tags 召回门**（对应缺口 C）：宏观轨准入条件从"n_symbols=0 且标题命中正则"放宽为
   "n_symbols=0 且（标题命中正则 **或** tags∩宏观标签集≠∅ 且 importance=high）"。
   命中 tags 但无正则 bucket 的，按 `macro_tag_<主标签>` 分桶。
   前置工作：确认 `tags` 已进 v1 parquet 索引（index 阶段现有 `importance` 列，tags 若缺需补列重建索引——ceph 红线 workers≤6，全量重建约数小时，可断点续跑）。

**注意不动的东西**：公司轨已经能接住"带 symbol 的非 Notice 事件"（UAW 罢工新闻带 F/GM/STLA、
Blackwell 发布带 NVDA、Zepbound 带 LLY），它们会在公司桶聚簇并跨桶合并——FLASH 准入后这条轨的
召回也自动变好。**宏观轨 n_symbols=0 的限制不需要放宽**，避免公司/宏观两轨大量重复。

### 4.3 select / structure 适配

- **select**：宏观簇的 n_articles 分布与公司簇差异大（关税日几百条、FDA 批准可能十几条），
  对 macro 轨**单独 sweep 一张阈值表**，可用"簇内 importance=high 占比"作为辅助送审信号
  （高重要性快讯占比高的小簇也放行）。
- **triage prompt**：event_type 枚举补齐 10 案例的类型（monetary_policy_statement / labor_strike /
  tariff_policy_change / antitrust_court_decision / regulatory_approval / product_platform_launch /
  government_program），并允许 `event_subject_type ∈ {central_bank, government, court, labor_union, company}`。
- **structure prompt**：无 symbol 事件的"8-14 个关系标的行"要求输出跨资产 ETF
  （SPY/QQQ/XLF/TLT/GLD/USO 等）+ 受影响个股，与案例 1 的"跨资产方向与横截面排序"目标对齐；
  防泄露规则（facts 仅事件日前可知、不打方向）已有，无需改。

### 4.4 官方锚点（`official_source_url`）补齐——列为 backlog，不阻塞主流程

数据里只有新闻源 URL，案例要求 official_or_primary 锚点（federalreserve.gov / fda.gov / uaw.org…）。
分两步：日历轨的锚点随静态日历直接给出（零成本）；发现轨先落 `anchor_status=pending`，
后续用 LLM 结构化产出的 (event_type, subject, date) 拼官方 URL 模式或人工补一批高频模式
（FDA press release、法院 PACER、白宫 EO 的 URL 规律都很强）。

## 5. 验证方案（窗口内 ground truth 回归）

10 案例本身多数在 2023-2025，落在数据窗口（2025-07→2026-06）内的只有 NVDA FY2027Q1 财报。
所以验证不用这 10 例，改用**窗口内可枚举的已知事件清单**：

1. **日历轨**：8 次 FOMC + 12 次 CPI + 12 次 NFP，要求召回 100%、event_date 全对。
2. **发现轨抽检**：窗口内挑 10-15 个公开已知的非日历事件（如 2025H2-2026H1 的关税调整、
   大型罢工、重要反垄断裁决、明星药 FDA 批准、旗舰产品发布——从 FLASH 高频簇里反查即可拟清单），
   逐个检查是否成簇、是否过 select、structure 产出是否合格。
3. **负样本**：抽 20 个 triage 拒绝的宏观簇人工看误杀率。

## 6. 实施排期（按依赖顺序）

| 步骤 | 内容 | 规模 |
|---|---|---|
| 1 | index 补 `tags` 列（若缺）并重建 v1 索引 | 改 ~10 行 + 重建跑批（断点续跑） |
| 2 | cluster：FLASH 准入 + 主题桶扩充 + tags 召回门 | 改 ~40 行 |
| 3 | `extract scheduled`：静态日历 + 窗口检索直通 triage | 新增 ~150 行 + 日历数据文件 |
| 4 | select：macro 轨独立 sweep；triage/structure prompt 扩枚举 | 改 prompt + 阈值实验 |
| 5 | 验证：日历轨 100% 召回 + 发现轨抽检清单 | 人工半天 |
| 6 | backlog：official_source_url 补齐 | 不阻塞 |

成本量级：新增送审簇主要来自宏观轨扩充，先 `--sweep`/`--dry-run` 出量再定阈值，
triage 用 flash 模型，预计新增送审在千级，可控。

## 7. 风险与坑点

- **ceph-fuse 红线**：重建索引 workers 严禁超 `EVENT_INDEX_WORKERS=6`（docs/pipeline.md）。
- **FLASH 重复轰炸**：同一宏观事件快讯高度同质（"Fed cuts 25bp" 十几家来源各发一条），
  聚类 Jaccard 阈值在一句话标题上偏严格的问题不大，但 `MAX_BUCKET_WINDOW=400` 在 FOMC 日
  可能不够——日历轨把最热的日历事件接走后，发现轨压力大减，这也是双轨的隐性好处。
- **中文标题**：US_NEWS 有中文内容，正则召回不了；tags 召回门部分缓解，剩余部分接受漏召
  （FLASH 英文为主，宏观事件几乎必有英文快讯兜底）。
- **一词多义**（strike/appeal 等）：召回宽、triage 严，是现有漏斗既定哲学，不需要新机制。
