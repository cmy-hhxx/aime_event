# News / Flash 逐条抽取事件：会面临什么问题（第一性原理实证）

> 前提：Notice 已知怎么抽。本文只回答一个问题——对 US_NEWS（169 万条）和 US_FLASH（41 万条）
> 采用最朴素的方案"**每条记录 → LLM → 抽出 0..n 个事件**"，会碰到什么。
> 所有结论都来自 `data/export_2025-07-08` 的实测抽样，不预设任何机制。

---

## 0. 为什么 Notice 逐条成立，先说清楚

Notice（SEC 文件）逐条抽取之所以行得通，靠三个前提：

1. **一份文件 ≈ 一个事件锚**：8-K 就是"公司发生了值得披露的事"本身；
2. **全网唯一**：accession number 保证同一事件只有一份文件，无重复；
3. **主体和时间权威**：filer 就是事件主体，filing date 就是事件时间。

下面的实证会逐条展示：**News/Flash 把这三个前提全部打破**。

## 1. 数据画像（实测）

| | US_FLASH | US_NEWS |
|---|---|---|
| 条数 | 412,144 | 1,689,381 |
| 形态 | title≈body 的一句话 | 完整文章，body p50=2,209 字符（p10=123 / p90=5,443） |
| entities 为空 | 66.8% | 少数 |
| 纯 crypto 实体 | — | 16.1% |
| 中文标题 | — | 0.7%（另散见德语等） |
| 结构化辅助 | tags + importance(high 37%/mid 51%/low 12%) + regions | tags + importance |
| 时间窗 | 2025-07-08 → 2026-06-29 | 同 |

## 2. 问题清单（每条配实测证据）

### P1｜一条 ≠ 一个事件：记录和事件不是一一对应

随机抽 60 条 FLASH 逐条人工分类，"值得建模的离散事件"只占约 **1/3**，其余是四类失配：

| 失配类型 | 占比(60 条样本) | 实例 |
|---|---|---|
| **数据/行情播报**（是数字不是事件） | ~27% | "LME tin inventory falls 15 tons"、"US six-week bills draw 4.265%"、"Tomra 30 gross margin 44%"、"Nasdaq 100 drops 3%" |
| **发言/评论引述**（是话语不是事件） | ~18% | "Apollo's Kleinman: PE industry lost its way"、"OSFI head: banks have ample capital"、"Mimura: Refrain from commenting" |
| **传闻/意向/预告**（事件还没发生） | ~7% | "Muyuan **said to** gauge interest for $1.5B HK listing"、"Ineos **mulls** asset sale"、"Xi **to meet** Carney" |
| **价格异动播报**（是反应不是原因） | ~5% | "MongoDB shares fall 22% in worst day since March 2025"、"Tharimmune shares surge over 100%" |

NEWS 更糟：随机 32 条里真事件仅 ~6-8 条，其余是复盘（"Pre-Market Most Active"）、
listicle/观点（"The 3 Numbers Every American Should Check"、"Tesla Will Crash In 2026"）、
回顾展望（"How Likely Is It That the Stock Market Crashes… Here's What History Tells"）、
体育（"Hawks have added incentive to extend Pelicans' skid"）。

**逐条方案的代价**：LLM 必须对全部 210 万条做"是不是事件"判定，其中约 2/3 的调用注定输出"否"。
且"传闻→确认→细节"是跨条演进链，单条视角分不清 rumor 和 confirmed。

### P2｜同一事件被报几十到几百次：重复不是长尾，是主体

拿 2025-12-10（FOMC 降息日）实测 FLASH：

- 当天共 **1,509 条** flash；
- 其中 Fed/FOMC/Powell 相关 **201 条**；
- 决议本体（"Fed cuts …"）就有 **27 条**互为改写（"Fed cuts benchmark rate target range to 3.5%-3.75% in 9-3 vote" / "Fed delivers surprise cut: U.S. policy rate slashed to 3.75%" / "Fed cuts IOR to 3.65%"…）；
- Powell 记者会被拆成 **132 条**逐句快讯（"Powell: Labor market appears to be gradually cooling"、"Powell: inflation remains somewhat elevated"…），
  还有同一句话两个来源两种拼写（"WH Sr. Adviser **Hassett**: …" / "**Hasset**: …"）。

**逐条方案的代价**：一次 FOMC 会被抽成 ~200 个"事件"。事件级去重躲不掉，只是从 LLM 之前
推迟到了 LLM 之后——先花 200 次调用再合并，而不是先合并再花 1 次调用。
碎片还各自残缺：决议数字、投票分歧、点阵图、记者会口径分散在不同条目里，单条抽取的每个"事件"都不完整，
**聚合不仅是去重，还是把一个事件的事实拼完整的唯一途径**。

### P3｜反过来：同关键词 ≠ 同一事件

对 NEWS 全量 grep `anti-dumping`：命中 **611 条**，但抽出标题一看是**几十个互不相干的事件**——
美对印太阳能 123% 关税、美对华石墨 93.5%、欧盟对华移动起重机立案、越南对泰国糖、墨西哥对美钢管……

**逐条方案的代价**：抽完之后想按关键词/类型归并去重是行不通的；归并必须基于
（主体, 类型, 日期, 标题相似度）的组合判断——这本质上就是在做聚类，无论放在 LLM 前还是后。

### P4｜实体被提及 ≠ 事件主体

对 NEWS 全量 grep `Wells Fargo`+`shutdown` 共现：**1,193 条**，抽样看标题——绝大多数是
"Wells Fargo 分析师评论政府停摆"、"财报文引用 Wells Fargo 观点"这类**顺带提及**，
和"Wells Fargo 自身发生了事件"毫无关系。

**逐条方案的代价**：LLM 逐条抽 subject 时，长文章里的次要提及会诱发大量错误归属
（尤其 NEWS 正文 p90=5.4k 字符，一篇文章提及 5-10 个实体很常见）；
FLASH 则相反——66.8% 无 entities，主体全靠 LLM 从一句话里裸猜，"SYK: factored into outlook"
这种缺主语的电话会碎片根本无从判断。

### P5｜重要性（significance）无法单条判定

单看一条 "Fitch affirms New Zealand's TSB Bank at BBB+"（例行）和一条
"Fed delivers surprise cut"（重磅），LLM 可以靠常识打分；但大量中间地带——
某公司 $1B 投资、某并购、某 FDA 批准——**到底市场在不在乎，单条文本里没有答案**。
市场关注度的天然代理是：多少家来源报了、研报/电话会有没有反应、持续了几天。
这些全部是**跨条信号**，逐条抽取在结构上拿不到。
（FLASH 自带 importance 可以参考，但实测标签噪声不小：台湾政治新闻被打上 CentralBanking。）

### P6｜事件时间 ≠ 发布时间

- 回顾/展望文：published_at 是发文日，谈的是过去或未来的事（"…Here's What History Tells Us"）；
- 周期数据类："PBOC Adds Gold for **14th Month**"——事件日是数据覆盖月，不是发文日；
- 时区裁剪：美东盘后事件的 UTC published_at 落在次日。

**逐条方案的代价**：event_date 必须由 LLM 从正文推断，回顾文会批量产出"旧闻假事件"，
污染训练集的时间切片（这正是 10 案例 HTML 里反复强调的防泄露红线）。

### ~~P7｜范围噪声~~（已裁决：不采纳，数据以美股为主，不做地域过滤）

60 条 FLASH 样本里非美内容 ≥ **17%**（赞比亚央行、巴基斯坦 OMO、土耳其/南非/沙特财报、阿曼外交）；
NEWS 32 条样本里非美/非市场 ≥ **30%**（印度阿萨姆邦 GDP、英国水务、瑞士火灾、NBA、德语区银行业），
另有 16.1% 纯 crypto。

（原分析：FLASH 非美内容 ≥17%、NEWS 非美/非市场 ≥30%。用户裁决：以美股为主不特地过滤，
非美内容交给聚类佐证门槛和 triage 自然淘汰。保留本节仅作记录。）

### P8｜规模与成本：210 万次调用，换来一个仍需聚合的中间产物

- NEWS：169 万条 × (body 截断 2,800 字符 ≈ 700 tokens + prompt) ≈ **~1.7B input tokens**
- FLASH：41 万条 × (~400 tokens) ≈ **~0.2B input tokens**
- 合计 **~2B tokens、210 万次调用**，即便用 flash 级模型也是真实成本和数天的吞吐；
- 而且按 P1-P4，产出的 ~210 万行原始"事件"中：约 2/3 是"非事件"判定、
  真事件平均被重复抽取数次到数百次(P2)、主体/时间/重要性字段带系统性误差(P4-P6)——
  **聚合、去重、佐证这三道工序一道都省不掉，只是全部被挤到了 LLM 之后。**

## 3. 小结：每个问题各自"逼出"什么机制

不预设方案，只做推导——如果坚持逐条抽取，上面每个问题都强制你补一个机制：

| 问题 | 被逼出的机制 | 放在 LLM 前做的便宜近似 |
|---|---|---|
| P1 非事件占 2/3 | 抽取前/后加"是否事件"分类 | 规则过滤（模板 tags、数据播报模式、价格异动模式） |
| P2 一事百报 | 事件级合并 + 事实拼装 | 标题相似度聚类（同一事件的改写标题天然相似） |
| P3 同词不同事 | 合并键 = (主体,类型,日期,相似度) | 聚类的连边条件本身 |
| P4 提及≠主体 | 主体消歧 | symbol 分桶（一篇进其所有 symbol 桶，靠簇投票定主体） |
| P5 单条无重要性 | 跨条佐证计数 | n_articles / n_v2_reactions 门槛 |
| P6 发布≠事件时间 | 时间推断 + 回顾文过滤 | 簇内时间分布(真事件报道集中在 1-3 天内) |
| P7 全球混合流 | 范围过滤 | region/crypto/正则前置过滤 |
| P8 210 万次调用 | —— | 以上所有近似的总效果：LLM 只看代表性样本 |

也就是说：**"先聚类后抽取"不是外加的设计偏好，而是把逐条方案无论如何都要做的
去重/佐证/拼装工序，从 LLM 之后（贵）挪到 LLM 之前（便宜）的必然结果。**
现有漏斗（index→cluster→select→structure）正是这套推导的实现——它的问题不在架构，
而在召回参数（FLASH 被 body_len≥200 挡掉 74%、宏观主题桶只有 5 类、tags 未参与召回，
详见 `multi_source_event_plan.md` §3）。

逐条方案有一个真实优点需要记录在案：**不依赖聚类召回，单源独家的小事件也能抽到**
（聚类门槛 n_articles≥2/5 天然漏掉独家报道）。这个优点值得在漏斗之外补一条窄通道
（如仅对 importance=high 且过了 P1 规则过滤的独家 FLASH 做逐条抽取），
但那是下一步的设计讨论，不属于本文范围。

---

*实测口径备注：FLASH 60 条随机样本人工分类；NEWS 6,008 条抽样统计 + 32 条标题人工分类；
FOMC 日全量扫描；anti-dumping / Wells Fargo 为 NEWS 全文件 grep。*
