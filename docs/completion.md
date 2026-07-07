# Completion 阶段

事件补全把 extraction 的结构化事件补齐**行情面板与 1D/5D/20D 隐藏标签**，组装成
`FinancialPredictionTrainingCase.v4.three_year_event_signal_pack` 训练包。

```text
输入：/mnt/ainvest_content/v3/event_dataset/structured/structured.jsonl
输出：/mnt/ainvest_content/v3/event_dataset/market/  (价格面板+标签)
     /mnt/ainvest_content/v3/event_dataset/final/<CASE_ID>.json + manifest.jsonl
Schema：schema/completion/final_case.schema.json (关键字段校验)
```

## complete fetch（src/completion/market.py，**在本地 Mac 跑**）

服务器（国内机房）访问 Yahoo 超时，yfinance 拉取必须在本地跑，流程见 pipeline.md 第 5 节。
汇总所有事件的 relation symbol 去重后，**每个 symbol 只拉一次**全时段日线
（EVENT_FETCH_START~END，auto_adjust=True 与样例对齐），产出 `prices_daily.parquet`
传回容器。增量式：面板里已有的 symbol 自动跳过，限流后重跑同一命令即可。
拉不到的 symbol 记入 `fetch_failed.json`，后续按 unpriced 降级不阻塞样本。

## complete label（src/completion/market.py，容器）

离线纯计算，日历规则（`base_ft_indices`）：

- `pre_market` 事件：base_close = 前一交易日收盘，first_tradable = 事件日
- 其余（盘中/盘后/unknown）：base_close = 事件日收盘（非交易日取其前最近交易日），
  first_tradable = 下一交易日
- S0 可见窗 = base 前 15 个交易日；审计窗 = first_tradable 起 26 个交易日
- 标签：1D/5D/20D close-to-close 与 open-to-close 收益、开盘跳空、截面 20D 排名
- 有效标的 <3 的事件作废（`skipped_few_symbols`）；窗口不完整（新股/退市）同样跳过该标的

## complete assemble（src/completion/assemble.py，容器）

按 v4 schema 组装全字段并做审计：

- 增强层留空占位 + status 标记（`intraday_volume_panel.status="missing_real_intraday_for_event_date"`、
  `weak_association_ai_search.status="pending_enhancement_layer"`），与样例做法一致
- 审计项：facts 未来日期泄露扫描（命中记 `quality_audit.leakage_scan_hits`）、
  结构完整性（facts≥2/channels≥2/关系行≥5/已打价≥3，不达标丢弃）、
  标签完整性、case_id 唯一化
- 关系标的按是否有行情标 `priced_and_labeled` / `unpriced_weak_candidate_needs_price_panel`
- `provenance` 保留簇统计与源文章清单，可回溯到原始新闻 id

`complete all` = label → assemble（fetch 必须先单独在本地跑完并传回面板）。
