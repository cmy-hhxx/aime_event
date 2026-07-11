"""LLM prompt 常量: triage(送审门后 LLM 粗筛) 与 structure(Task 7 结构化)."""
from __future__ import annotations

EVENT_TYPES = ("earnings_release|ma_deal|product_launch|regulatory_action|fda_approval|"
               "monetary_policy|macro_data|legal_ruling|guidance_change|capital_return|"
               "labor_action|tariff_policy|ipo_listing|management_change|analyst_day|"
               "partnership|security_incident|other")

TRIAGE_SYSTEM = """你是金融事件数据集的筛选员。给你一簇新闻的标题/日期/来源/关联股票,判断它是否是一个\
"可用于股价预测训练的离散事件"。合格标准:
1. 单一、可定自然日的事件(公告/裁决/发布/数据发布),不是主题综述、日常盘面回顾、榜单、推荐文
2. 对美股(或美股ADR/ETF)定价有信息量; 纯加密货币/纯外国本地市场(无美股标的)不合格
3. 簇内标题应指向同一事件; 若明显混杂多个事件,以最主要的事件为准
只输出 JSON。"""

TRIAGE_USER_TMPL = """新闻簇信息:
- 峰值日: {peak_date} (首报 {first_date}, 末报 {last_date})
- 报道数 {n_articles}, 独立来源 {n_sources}, 高重要性标记 {n_high}, 事件后研报/电话会数 {n_v2}
- 关联股票: {symbols}
- 代表标题(带日期):
{titles}

输出 JSON:
{{
  "is_valid_event": true/false,
  "reject_reason": null 或 简短原因,
  "event_type": "{types}" 中之一,
  "event_family": "英文snake_case短语,如 ai_hardware_platform",
  "event_subject": "事件主体,如 NVIDIA Blackwell platform / FOMC July meeting",
  "primary_symbols": ["最核心的美股代码,最多3个,宏观事件可为空数组"],
  "event_date": "YYYY-MM-DD,事件自然日(公告/发生日,通常=首报日或其前一天)",
  "significance": 1到5整数(5=FOMC决议/巨头财报/重大并购级, 3=值得单独建样本, 1=噪声),
  "title_cn": "中文事件标题,格式如: 主体+动作+市场含义"
}}"""

NOTICE8K_TRIAGE_SYSTEM = """你是金融事件数据集的筛选员。给你一份 SEC 8-K 当期报告的信息(条目号/标题/摘要或原文节选),\
判断它是否是一个"可用于股价预测训练的离散事件"。合格标准:
1. 单一、可定自然日的公司事件(重大协议/并购/高管变动/融资发行/临床数据/业绩与指引等),\
不是例行程序性披露(股东会投票结果、章程修订、纯报表附录、常规展期)
2. 对该美股(或美股ADR/ETF)定价有信息量; 发行人无美股交易代码则不合格
3. primary_symbols 给发行人的真实美股代码(从公司名推断); 不确定宁可给空数组并 is_valid_event=false
只输出 JSON。"""

NOTICE8K_TRIAGE_USER_TMPL = """8-K 公告信息:
- SEC 申报日: {event_date}
- 条目号 Item: {item_code} (如 1.01=重大协议, 2.02=业绩, 5.02=高管变动, 8.01=其他重大事件)
- 标题: {event_title}
- 摘要/原文节选:
{summary}

输出 JSON:
{{
  "is_valid_event": true/false,
  "reject_reason": null 或 简短原因,
  "event_type": "{types}" 中之一,
  "event_family": "英文snake_case短语,如 ai_hardware_platform",
  "event_subject": "事件主体,如 NVIDIA Blackwell platform / Best Buy Q1 earnings",
  "primary_symbols": ["发行人的美股代码,最多3个"],
  "event_date": "YYYY-MM-DD,事件自然日(公告披露的事件发生/签署日,通常=申报日或其前几天)",
  "significance": 1到5整数(5=巨头财报/重大并购级, 3=值得单独建样本, 1=例行披露噪声),
  "title_cn": "中文事件标题,格式如: 主体+动作+市场含义"
}}"""

STRUCTURE_SYSTEM = """你是金融事件训练数据的结构化标注员。基于一簇同一事件的新闻报道,产出该事件的结构化训练字段。

硬性泄露规则(最重要):
1. facts_publicly_reported 只能包含事件自然日(event_date)当天及之前公开可知的事实
2. 严禁写入: 事件后的股价反应/涨跌幅、分析师事后评级调整、事件后续进展、任何 event_date 之后日期的信息
3. 如果报道里混有事后信息,只提取其中回溯描述的事件本身事实
4. 不得编造数字; facts 的 value 必须能在给定文章中找到依据

关系标的规则:
1. relation_rows 给 8-14 个美股/美股ADR/行业ETF 标的,必须包含≥1个行业ETF作为因子代理
2. 覆盖多种关系: 直接主体、同业竞品、供应链上下游、大客户/渠道、ETF代理; 宏观事件给利率敏感/行业敞口标的
3. evidence_statement 写"影响链: ..."格式,说明入池理由; 不打方向和强度
4. symbol 必须是真实存在的交易代码,不确定的宁可不给
只输出 JSON。"""

STRUCTURE_USER_TMPL = """事件线索(来自筛选阶段):
- 事件日估计: {event_date}
- 事件类型: {event_type} | 主体: {event_subject} | 核心标的: {primary_symbols}
- 中文标题草稿: {title_cn}

同簇报道原文(已截断, 按信息量排序):
{articles}

输出 JSON:
{{
  "case_id": "主体缩写_事件关键词_YYYYMMDD, 全大写下划线, 如 NVDA_BLACKWELL_GTC_20240318",
  "case_title": "中文: 主体+事件+市场含义, 如 'NVIDIA Blackwell 平台发布：AI 训练/推理基础设施换代'",
  "display_short_name": "2-4词英文短名",
  "event_family": "英文snake_case, 如 ai_hardware_platform",
  "event_type": "英文snake_case, 如 product_platform_launch",
  "main_event": {{
    "event_subject": "事件主体英文名",
    "event_subject_type": "主体类型snake_case",
    "event_date": "YYYY-MM-DD 事件自然日(以文章内证据为准,可修正筛选阶段的估计)",
    "official_source_url": "文章中出现的官方来源URL, 没有则 null",
    "facts_publicly_reported": [
      {{"metric": "事实维度名(中文)", "value": "事实内容(可中英混合,含关键数字)", "context": "对训练的含义说明(中文)"}}
    ],
    "event_influence_channels": [{{"channel": "影响渠道snake_case"}}]
  }},
  "event_timestamp_et": {{"timestamp_et": "HH:MM 或 null", "session_bucket": "pre_market|regular|after_market|unknown", "precision": "exact|estimated|unknown"}},
  "relation_rows": [
    {{"symbol": "代码", "company": "公司名", "relation_type": "英文snake_case",
      "relation_path": ["事件主体", "event_family", "代码"],
      "evidence_statement": "影响链: ...", "relation_type_cn": "直接暴露|同业/替代品|供应链/基础设施|客户/渠道|ETF/因子代理|事件相关候选",
      "impact_path_cn": "一句话影响路径"}}
  ],
  "facts_count_check": "facts数量3-6, channels数量3-6",
  "confidence": 0.0到1.0
}}"""
