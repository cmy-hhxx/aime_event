# 1.3 万候选簇质量审计：是否达到预期？

> 对象：`funnel_experiment.py` 第 3 轮产出的 **13,185 个 n≥5 候选簇**（macro 1,183 / general 12,002）
> 方法：按 size 分层随机抽 100 簇（macro 40 + general 60），逐簇读成员标题人工分类
> 样本文件：job tmp `audit_sample.json`；簇 dump：`clusters_ge5.jsonl`（`--dump` 生成）
> 注意：分层按 size 排序抽样，大簇被过采样；60 簇样本的比例估计误差约 ±12%

## 结论先行

| 轨道 | 真事件占比 | 判定 |
|---|---|---|
| **macro（1,183 簇）** | **~65%**（26/40） | ✅ 达到预期，可直接送 triage |
| **general（12,002 簇）** | **~45%**（27/60） | ⚠️ 未达预期，噪声主形态=小簇化的模板流 |

总体：13,185 簇中估计 **~6,000-6,500 个真事件簇**。作为 210 万条 → 6 千事件的漏斗，
方向正确；但 general 轨一半送审预算会花在噪声上，triage 兜底可行（成本翻倍仍远小于逐条），
不过有两个便宜的修复应该先做（见 §4）。

## 1. macro 轨明细（40 簇人工分类）

| 类别 | 数量 | 例子 |
|---|---:|---|
| ✅ 真离散事件 | 26 | 美欧 15% 关税协议(#13)、钢铝铜 25% 关税(#21)、Glencore 罢工(#31)、Fed 主席提名时间线(#35)、Kraken 收购 Small Exchange(#11)、罗马尼亚封禁 Polymarket(#2) |
| ❌ 数据/例行播报漏网 | 5 | 欧洲各国 CPI 快讯(#6)、各国失业率(#19)、国债拍卖预告(#30)、CFTC 周度持仓(#37) |
| ❌ 评论/发言碎片 | 4 | "Market Talk" 油价评论(#3)、Bessent 采访逐句(#38) |
| ⚠️ 混簇（2+ 事件被链入一簇） | 4 | Ripple FCA + Crescent Biopharma(#8)、伊朗战争拨款 + Ebola 拨款(#26) |
| ❌ 模板流误入 | 1 | 日本公司库藏股处分 PR 链(#9，"Treasury Share" 误命中 treasury 正则) |

发现的具体问题（都可修）：

- **词义歧义**：`strike` 把 IDF 空袭(#10)、乌克兰导弹(#34) 收进 labor_strike 桶——事件是真的，
  桶名错了，triage 会改类型，无伤大局；`treasury` 歧义则真正引入了噪声(#9)。
- **国际数据播报漏网**：`XX CPI YoY actual (forecast, previous)` 模式非常规整，一条正则可杀。
- **`— Market Talk` / `-- Market Talk` 后缀**是道琼斯评论体裁标记，一条正则可杀。
- crypto 公司事件在 macro_tag_Regulation 里占比很高（Ripple/Visa/Kraken……）——是 tags 召回门
  带进来的真事件，只是"宏观"名不副实，对训练集反而是补充（crypto_market_structure 正是 10 案例之一）。

## 2. general 轨明细（60 簇人工分类）

| 类别 | 数量 | 例子 |
|---|---:|---|
| ✅ 真离散事件 | 27 | Spirit 二次破产(#78)、Cencora/Covetrus $3.5B 合并(#75)、CME 24/7 加密期货(#62)、Trump 起诉 Murdoch(#56)、三星 HBM4 认证(#82)、Bostic 退休(#94)、Rezolve 敌意收购(#73) |
| ❌ 模板流（同模板不同主体成小簇） | 12 | "A Glimpse Into Expert Outlook Through N Analysts"(71条,#48)、"Dips More Than Broader Market"(#71/#84)、"Laps the Stock Market"(#79)、"call volume above normal"(#67)、"Shares Gap Down to"(#92)、评级动词前置变体("Bernstein cuts X target price",#89;"Raised to Buy From Hold",#99) |
| ❌ 例行公司流 | 8 | Form D 融资流(#40)、offering closing 流(#41)、分红宣布流(#46)、会议 Transcript 流(#63)、Nasdaq 合规通知(#70) |
| ❌ 数据播报漏网 | 4 | SGX 橡胶价格(32条,#51)、JGB 收益率变体(#55)、Nymex 交割意向(#80)、葡语开盘播报(#96) |
| ⚠️ 追踪流/主题链（半事件） | 9 | 印度 IPO 认购逐日更新(#47/#76)、ETF 资金流出主题链(#49)、鲸鱼持仓追踪(#74) |

**关键判断：第 2 轮的稀有词锚定成功阻止了模板巨簇，但模板流没有消失——
它退化成"每个主体一个小簇"，以数千个 5-10 条的小簇形态存活下来，刚好卡在 n≥5 门槛上方。**
这正是缺实体锚定的代价：无法区分"同一主体的多源报道"（事件）和"同一模板的多主体套用"（噪声）。

（尝试了自动骨架检测——掩码数字/大写词后判簇内同构——只命中 1.8%，因为跨来源模板措辞
变体多，8 条采样标题内精确骨架匹配太严格。判"模板 vs 事件"本质是 triage 的活。）

## 3. 对照预期逐条裁决

| 预期 | 结果 |
|---|---|
| 漏斗末端以真事件为主 | macro ✅（65%）；general ⚠️（45%） |
| 稀有词锚定压制模板链 | ✅ 巨簇消失（53,598→9,159）；但模板流转为小簇存活 ⚠️ |
| L3 硬过滤覆盖主要噪声 | ⚠️ 7 类规则杀了 33 万条，但动词前置评级/国际数据播报/Zacks 系模板漏网 |
| 宏观事件（加息/罢工/关税）能被抽到 | ✅ 关税协议、罢工、Fed 人事、监管审查都在样本中出现 |
| 送审成本可控 | ✅ 1.3 万次 triage（含 ~50% 浪费）≪ 210 万次逐条 |

## 4. 达标前要做的两件事（+一件可选）

1. **L3 规则补漏**（半小时）：`— Market Talk` 后缀、`actual .* \(forecast` 数据模式、
   `(cuts?|raises?) .{0,30} target price`、`raised to (buy|hold|sell) from`、
   `(dips|laps) .* broader market`、Form D/offering closing/transcript 模板、treasury share disposal。
   预计把 general 噪声簇再砍 1/4~1/3。
2. **triage prompt 明确负类**：把"模板流/例行公告/数据播报/追踪流"写成显式拒绝类别，
   并利用簇内标题列表作为输入（模板流的"同构多主体"特征对 LLM 一眼可判）——
   这是把人工审计的判别标准直接交给 triage。
3. （可选，最高杠杆但工作量大）**实体回填**：v2 + FLASH 17% 已有实体建词典回填 NEWS 主体，
   general 轨升级为主体分桶，从机制上分开"一主体多报道"与"一模板多主体"。

## 5. 给用户的直接回答

**部分达到预期。** macro 轨质量达标、known 宏观事件（关税/罢工/Fed）都能抽到、
压缩比达标（160×）；general 轨真事件率 ~45% 低于预期，噪声形态明确（模板流小簇化）、
成因明确（无实体锚定 + 规则覆盖不足）、修复路径明确（上面 3 条）。
n≥5 直接全量送 triage 也能用（浪费 ~50% 调用，约 6-7k 次无效 triage，flash 模型成本可接受），
但先花半天做修复 1+2 更划算。
