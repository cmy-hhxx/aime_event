# Completion 阶段

事件补全把 extraction 的结构化事件补齐**行情面板与 1D/5D/20D 隐藏标签**，组装成
`FinancialPredictionTrainingCase.v4.three_year_event_signal_pack` 训练包。

```text
输入：/mnt/ainvest_content/v3/event_dataset/structured/structured.jsonl
输出：/mnt/ainvest_content/v3/event_dataset/market/  (日线、1m 分时、标签)
     /mnt/ainvest_content/v3/event_dataset/final/events.jsonl (一行一个事件包)
Schema：schema/completion/final_case.schema.json (关键字段校验)
```

## complete fetch（src/completion/market.py，**在本地 Mac 跑**）

服务器（国内机房）访问 Yahoo 超时，yfinance 拉取必须在本地跑，流程见 pipeline.md 第 5 节。
汇总所有事件的 relation symbol 去重后，**每个 symbol 只拉一次**全时段日线
（EVENT_FETCH_START~END，auto_adjust=True 与样例对齐），产出 `prices_daily.parquet`
传回容器。增量式：面板里已有的 symbol 自动跳过，限流后重跑同一命令即可。
拉不到的 symbol 记入 `fetch_failed.json`，后续按 unpriced 降级不阻塞样本。

## complete fetch-intraday / import-intraday

```bash
# Yahoo 仅适合实际最近 30 天内的事件日：
.venv/bin/python -m src.main complete fetch-intraday --event-date YYYY-MM-DD

# 更早日期使用其他行情源的逐 bar JSONL：
.venv/bin/python -m src.main complete import-intraday \
  --input /path/to/intraday_1m.jsonl --provider <provider-name>
```

`fetch-intraday` 只保留覆盖常规交易时段首尾且不少于 380 根的面板。Yahoo 1m
没有成交笔数和交易所 VWAP：兼容输出中 `trade_count=0`，`vwap` 使用分钟 OHLC
typical-price 代理，并在 `quality_audit.known_limitations` 披露。

`import-intraday` 输入每行一根 bar，必须包含：`event_id,event_date,symbol,timestamp_et,`
`open,high,low,close,volume,vwap,trade_count,dollar_volume`。5 月 29 日已超出 Yahoo
1m 实际 30 天窗口，必须走该入口导入真实行情。

## complete label（src/completion/market.py，容器）

离线纯计算，日历规则（`base_ft_indices`）：

- `pre_market` 事件：base_close = 前一交易日收盘，first_tradable = 事件日
- 其余（盘中/盘后/unknown）：base_close = 事件日收盘（非交易日取其前最近交易日），
  first_tradable = 下一交易日
- S0 可见窗 = base 前 15 个交易日；审计窗 = first_tradable 起 20 个交易日
- 标签：1D/5D/20D close-to-close 与 open-to-close 收益、开盘跳空、截面 20D 排名
- 有效标的 <3 的事件作废（`skipped_few_symbols`）；窗口不完整（新股/退市）同样跳过该标的

## complete assemble（src/completion/assemble.py，容器）

按 v4 schema 组装全字段并做审计：

- `intraday_volume_panel` 必须至少包含一个已打价标的的完整事件日 1m 面板
- `associatin_search`（沿用 schema 当前拼写）= 结构化已引用来源（溯源，8-K 带 EDGAR url）
  + **按事件检索回填**（assemble.RelatedSearcher）：以 primary symbol + 主体公司名（词边界
  匹配标题；本导出 NEWS 索引无 symbols，只走标题通道）在 index parquet（v1 五类 +
  v2 report/teleconference）检索事件日 [-3, +1] 天窗口的相关记录，每类目去重后按
  published_at 升序、上限 10 条；index 缺失时自动退回纯溯源（单测不依赖大文件）
- 检索命中会按 index 里的 (file, offset) **seek 直读原文**补 `url` 与 `summary`
  （正文摘录 200 字符）：url 取 v1 `source.url`（退回 dedup 规范化 url）/ v2 段落
  `source.url`。url 覆盖率由数据源决定——News ~85%、Report/电话会 100%（PDF/OSS 链接）、
  Flash/Post/Article 为 AInvest 自产内容无外链，null 属正常
- 审计项：facts 未来日期泄露扫描（命中记 `quality_audit.leakage_scan_hits`）、
  结构完整性（facts≥2/channels≥2/关系行≥5/已打价≥3，不达标丢弃）、
  标签完整性、case_id 唯一化
- 关系标的按是否有行情标 `priced_and_labeled` / `unpriced_weak_candidate_needs_price_panel`
- `provenance` 保留簇统计与源文章清单，可回溯到原始新闻 id
- 写文件前使用 `jsonschema.Draft7Validator` 验证；失败详情进入 assemble report

`complete all` = label → assemble（fetch 必须先单独在本地跑完并传回面板）。
