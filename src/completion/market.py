"""Stage E: yfinance 日线/分时行情对齐 + 1D/5D/20D 隐藏标签.

两步:
  fetch  - 汇总所有事件的 symbol, 每个 symbol 拉一次全时段日线(auto_adjust), 存 prices_daily.parquet
           (可在服务器或本地 Mac 跑, 谁的网络好用谁; 面板文件可搬运)
  label  - 离线计算: base_close/first_tradable 日历规则, S0 可见窗口, 1D/5D/20D 标签, 截面排名

  fetch-intraday - 拉事件日常规交易时段 1m OHLCV, 存 intraday.jsonl

输出: market/prices_daily.parquet, market/intraday.jsonl, market/labels.jsonl,
      reports/stage_label_summary.json
"""
from __future__ import annotations

import json
import os
import re
import time
from collections import defaultdict
from datetime import date, timedelta

from src import config

SYM_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,5}$")
ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
PRE_SESSIONS = 15    # S0 事件前可见交易日数
POST_SESSIONS = 20   # schema 要求的 20D 标签审计窗
MIN_COMPLETE_1M_BARS = 380  # 常规美股交易日理论 390 根, 容许少量无成交分钟


def load_structured(path: str) -> list[dict]:
    out = []
    with open(path) as fh:
        for line in fh:
            r = json.loads(line)
            if r.get("_error"):
                continue
            out.append(r)
    return out


def filter_by_peak_date(events: list[dict], date: str | None) -> list[dict]:
    """--date 单日跑: 按 triage 报道高峰日过滤(与 select --date 同口径)."""
    if not date:
        return events
    return [r for r in events if (r.get("_triage") or {}).get("peak_date") == date]


def event_symbols(r: dict) -> list[str]:
    syms = []
    for row in r.get("relation_rows") or []:
        s = (row.get("symbol") or "").strip().upper()
        if SYM_RE.match(s) and s not in syms:
            syms.append(s)
    for s in r.get("_triage", {}).get("primary_symbols") or []:
        s = (s or "").strip().upper()
        if SYM_RE.match(s) and s not in syms:
            syms.insert(0, s)
    return syms[:16]


# ---------------- fetch ----------------
def run_fetch(args) -> None:
    import pandas as pd
    import yfinance as yf

    os.makedirs(args.outdir, exist_ok=True)
    os.makedirs(config.EVENT_REPORT_DIR, exist_ok=True)
    events = load_structured(args.structured)
    events = filter_by_peak_date(events, getattr(args, "date", None))
    all_syms = sorted({s for r in events for s in event_symbols(r)})
    print(f"[fetch] 事件 {len(events)}, 去重 symbol {len(all_syms)}", flush=True)

    panel_path = f"{args.outdir}/prices_daily.parquet"
    have: set[str] = set()
    frames = []
    if os.path.exists(panel_path):
        old = pd.read_parquet(panel_path)
        have = set(old["symbol"].unique())
        frames.append(old)
        print(f"[fetch] 已有 {len(have)} symbols, 增量拉取", flush=True)
    todo = [s for s in all_syms if s not in have]

    failed = []
    t0 = time.time()
    for i in range(0, len(todo), args.batch):
        chunk = todo[i: i + args.batch]
        try:
            df = yf.download(chunk, start=config.EVENT_FETCH_START, end=config.EVENT_FETCH_END, interval="1d",
                             auto_adjust=True, group_by="ticker", threads=8, progress=False)
        except Exception as e:
            print(f"[fetch] batch {i} 失败: {e}", flush=True)
            failed.extend(chunk)
            time.sleep(20)
            continue
        for s in chunk:
            try:
                sub = df[s].dropna(subset=["Close"]) if len(chunk) > 1 else df.dropna(subset=["Close"])
                if sub.empty:
                    failed.append(s)
                    continue
                out = sub.reset_index()[["Date", "Open", "High", "Low", "Close", "Volume"]]
                out.columns = ["date", "open", "high", "low", "close", "volume"]
                out["date"] = out["date"].dt.strftime("%Y-%m-%d")
                out.insert(0, "symbol", s)
                frames.append(out)
            except Exception:
                failed.append(s)
        done = min(i + args.batch, len(todo))
        print(f"[fetch] {done}/{len(todo)} failed={len(failed)} ({time.time()-t0:.0f}s)", flush=True)
        time.sleep(args.pause)

    if frames:
        pd.concat(frames, ignore_index=True).drop_duplicates(["symbol", "date"]) \
          .to_parquet(panel_path, index=False)
    with open(f"{args.outdir}/fetch_failed.json", "w") as fh:
        json.dump(sorted(set(failed)), fh)
    print(f"[fetch] 完成, 失败 symbol: {len(set(failed))}", flush=True)


# ---------------- fetch intraday ----------------
def _f(value: object) -> float:
    return round(float(value), 6)


def build_intraday_symbol_panel(df, event_date: str) -> dict | None:
    """把 yfinance 单标的 1m DataFrame 规范化为 final schema 的 symbol panel.

    Yahoo 不提供逐分钟成交笔数或交易所 VWAP。为保持 schema 不变，trade_count
    使用兼容值 0；vwap 使用分钟 typical-price 代理，并在 final quality_audit 中披露。
    只有覆盖常规交易时段首尾且 bar 数充足的面板才返回。
    """
    import pandas as pd

    if df is None or df.empty:
        return None
    frame = df.copy()
    if isinstance(frame.columns, pd.MultiIndex):
        frame.columns = frame.columns.get_level_values(0)
    required = {"Open", "High", "Low", "Close", "Volume"}
    if not required.issubset(frame.columns):
        return None
    frame = frame.dropna(subset=["Open", "High", "Low", "Close"])
    if frame.empty:
        return None
    idx = pd.DatetimeIndex(frame.index)
    if idx.tz is None:
        idx = idx.tz_localize("UTC")
    idx = idx.tz_convert("America/New_York")
    frame.index = idx
    frame = frame[frame.index.strftime("%Y-%m-%d") == event_date]
    frame = frame.between_time("09:30", "15:59")
    if frame.empty:
        return None
    first_hm = frame.index[0].strftime("%H:%M")
    last_hm = frame.index[-1].strftime("%H:%M")
    complete = len(frame) >= MIN_COMPLETE_1M_BARS and first_hm <= "09:35" and last_hm >= "15:55"
    if not complete:
        return None

    bars = []
    for ts, row in frame.iterrows():
        typical = (float(row["High"]) + float(row["Low"]) + float(row["Close"])) / 3
        volume = max(0, int(row["Volume"] or 0))
        bars.append({
            "timestamp_et": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "open": _f(row["Open"]), "high": _f(row["High"]),
            "low": _f(row["Low"]), "close": _f(row["Close"]),
            "volume": volume,
            "vwap": _f(typical),
            "trade_count": 0,
            "dollar_volume": _f(typical * volume),
        })
    return {
        "session_start_et": bars[0]["timestamp_et"],
        "session_end_et": bars[-1]["timestamp_et"],
        "bar_count": len(bars),
        "is_complete_session": True,
        "bars": bars,
    }


def run_fetch_intraday(args) -> None:
    """按事件逐标的拉取 Yahoo 实际近 30 天内可用的 yfinance 1m 面板。"""
    import yfinance as yf

    os.makedirs(args.outdir, exist_ok=True)
    os.makedirs(config.EVENT_REPORT_DIR, exist_ok=True)
    events = load_structured(args.structured)
    if args.event_date:
        events = [r for r in events if ((r.get("main_event") or {}).get("event_date")
                                        or r.get("_triage", {}).get("event_date")) == args.event_date]
    out_path = os.path.join(args.outdir, "intraday.jsonl")
    n_symbols = n_complete = 0
    with open(out_path, "w") as out:
        for i, r in enumerate(events, 1):
            event_date = ((r.get("main_event") or {}).get("event_date")
                          or r.get("_triage", {}).get("event_date"))
            if not (isinstance(event_date, str) and ISO_DATE_RE.match(event_date)):
                continue
            end = (date.fromisoformat(event_date) + timedelta(days=1)).isoformat()
            symbols, failed = {}, []
            for symbol in event_symbols(r):
                n_symbols += 1
                try:
                    df = yf.download(symbol, start=event_date, end=end, interval="1m",
                                     auto_adjust=False, prepost=False, actions=False,
                                     repair=True, progress=False, threads=False)
                    panel = build_intraday_symbol_panel(df, event_date)
                except Exception as exc:
                    print(f"[intraday] {r['event_id']} {symbol} 失败: {exc}", flush=True)
                    panel = None
                if panel is None:
                    failed.append(symbol)
                else:
                    symbols[symbol] = panel
                    n_complete += 1
                time.sleep(args.pause)
            out.write(json.dumps({
                "event_id": r["event_id"], "event_date": event_date,
                "provider": "Yahoo Finance via yfinance",
                "timezone": "America/New_York", "interval": "1m",
                "session_scope": "regular_session_09:30_16:00_ET",
                "symbols": symbols, "failed_symbols": failed,
            }, ensure_ascii=False) + "\n")
            print(f"[intraday] {i}/{len(events)} {r['event_id']} complete={len(symbols)} "
                  f"failed={len(failed)}", flush=True)
    summary = {"events": len(events), "symbols_requested": n_symbols,
               "complete_symbol_panels": n_complete, "output": out_path}
    with open(f"{config.EVENT_REPORT_DIR}/stage_intraday_summary.json", "w") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)
    print(json.dumps(summary, ensure_ascii=False), flush=True)


def run_import_intraday(args) -> None:
    """导入逐分钟 JSONL。

    输入每行一根 bar，必需字段：event_id,event_date,symbol,timestamp_et,
    open,high,low,close,volume,vwap,trade_count,dollar_volume。
    """
    os.makedirs(args.outdir, exist_ok=True)
    os.makedirs(config.EVENT_REPORT_DIR, exist_ok=True)
    grouped: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    required = ("timestamp_et", "open", "high", "low", "close", "volume",
                "vwap", "trade_count", "dollar_volume")
    with open(args.input) as fh:
        for line_no, line in enumerate(fh, 1):
            if not line.strip():
                continue
            row = json.loads(line)
            missing = [k for k in ("event_id", "event_date", "symbol", *required) if row.get(k) is None]
            if missing:
                raise ValueError(f"intraday 输入第 {line_no} 行缺字段: {','.join(missing)}")
            key = (str(row["event_id"]), str(row["event_date"]), str(row["symbol"]).upper())
            grouped[key].append({k: row[k] for k in required})

    by_event: dict[tuple[str, str], dict[str, dict]] = defaultdict(dict)
    rejected = []
    for (event_id, event_date, symbol), bars in grouped.items():
        bars.sort(key=lambda b: b["timestamp_et"])
        complete = (len(bars) >= MIN_COMPLETE_1M_BARS
                    and bars[0]["timestamp_et"][11:16] <= "09:35"
                    and bars[-1]["timestamp_et"][11:16] >= "15:55")
        if not complete:
            rejected.append({"event_id": event_id, "symbol": symbol, "bars": len(bars)})
            continue
        by_event[(event_id, event_date)][symbol] = {
            "session_start_et": bars[0]["timestamp_et"],
            "session_end_et": bars[-1]["timestamp_et"],
            "bar_count": len(bars), "is_complete_session": True, "bars": bars,
        }

    out_path = os.path.join(args.outdir, "intraday.jsonl")
    with open(out_path, "w") as out:
        for (event_id, event_date), symbols in sorted(by_event.items()):
            out.write(json.dumps({
                "event_id": event_id, "event_date": event_date,
                "provider": args.provider, "timezone": "America/New_York",
                "interval": "1m", "session_scope": "regular_session_09:30_16:00_ET",
                "symbols": symbols, "failed_symbols": [],
            }, ensure_ascii=False) + "\n")
    summary = {"events": len(by_event), "complete_symbol_panels": sum(map(len, by_event.values())),
               "rejected_panels": rejected, "output": out_path}
    with open(f"{config.EVENT_REPORT_DIR}/stage_intraday_import_summary.json", "w") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)
    print(json.dumps(summary, ensure_ascii=False), flush=True)


# ---------------- label ----------------
def pct(a: float, b: float) -> float:
    return round((b / a - 1) * 100, 3)


def base_ft_indices(dates: list[str], ev_date: str, bucket: str) -> tuple[int, int]:
    """返回 (base_close 下标, first_tradable 下标); pre_market 事件 base 用前一交易日."""
    import bisect
    if bucket == "pre_market":
        bi = bisect.bisect_left(dates, ev_date) - 1
    else:
        bi = bisect.bisect_right(dates, ev_date) - 1
    return bi, bi + 1


def run_label(args) -> None:
    import pandas as pd

    events = load_structured(f"{config.EVENT_STRUCTURED_DIR}/structured.jsonl")
    events = filter_by_peak_date(events, getattr(args, "date", None))
    panel = pd.read_parquet(f"{config.EVENT_MARKET_DIR}/prices_daily.parquet")
    panel = panel.sort_values(["symbol", "date"])
    by_sym = {s: g.reset_index(drop=True) for s, g in panel.groupby("symbol")}
    print(f"[label] 事件 {len(events)}, 面板 symbols {len(by_sym)}", flush=True)

    out_path = f"{config.EVENT_MARKET_DIR}/labels.jsonl"
    n_ok = n_skip = 0
    with open(out_path, "w") as out:
        for r in events:
            # LLM 产出的 event_date 需严格 ISO 校验, 畸形日期会让 bisect 静默错位
            ev_date = (r.get("main_event") or {}).get("event_date")
            if not (isinstance(ev_date, str) and ISO_DATE_RE.match(ev_date)):
                ev_date = r["_triage"]["event_date"]
            if not (isinstance(ev_date, str) and ISO_DATE_RE.match(ev_date)):
                n_skip += 1
                continue
            bucket = (r.get("event_timestamp_et") or {}).get("session_bucket") or "unknown"
            syms = event_symbols(r)
            per_sym, labels = {}, []
            for s in syms:
                g = by_sym.get(s)
                if g is None:
                    continue
                dates = g["date"].tolist()
                # base_close: pre_market 事件用前一交易日, 其余用事件日(或其前最近交易日)
                bi, ft = base_ft_indices(dates, ev_date, bucket)
                if bi < PRE_SESSIONS or ft + POST_SESSIONS > len(dates):
                    continue  # 窗口不完整(新股/退市/数据缺失)
                win = g.iloc[bi - PRE_SESSIONS + 1: bi + 1]
                audit = g.iloc[ft: ft + POST_SESSIONS]
                base_close = float(g.loc[bi, "close"])
                h = {"1d": ft, "5d": ft + 4, "20d": ft + 19}
                rec = {
                    "symbol": s,
                    "base_close_date": dates[bi], "base_close": round(base_close, 4),
                    "first_tradable_session": dates[ft],
                    "close_to_next_open_gap_pct": pct(base_close, float(g.loc[ft, "open"])),
                    "close_to_close_return_pct": {k: pct(base_close, float(g.loc[i, "close"])) for k, i in h.items()},
                    "tradable_open_to_close_return_pct": {k: pct(float(g.loc[ft, "open"]), float(g.loc[i, "close"])) for k, i in h.items()},
                    "horizon_dates": {k: dates[i] for k, i in h.items()},
                    "is_hidden_from_model_input": True,
                }
                labels.append(rec)
                per_sym[s] = {
                    "model_input_ohlcv_adjusted_daily": [
                        {"date": w.date, "open": round(w.open, 4), "high": round(w.high, 4),
                         "low": round(w.low, 4), "close": round(w.close, 4), "volume": int(w.volume)}
                        for w in win.itertuples()],
                    "label_audit_ohlcv_adjusted_daily": [
                        {"date": w.date, "open": round(w.open, 4), "high": round(w.high, 4),
                         "low": round(w.low, 4), "close": round(w.close, 4), "volume": int(w.volume)}
                        for w in audit.itertuples()],
                }
            if len(labels) < 3:  # 有效标的太少, 样本作废
                n_skip += 1
                continue
            ranked = sorted(labels, key=lambda x: -x["close_to_close_return_pct"]["20d"])
            for rank, rec in enumerate(ranked, 1):
                rec["cross_section_rank_20d"] = rank
            out.write(json.dumps({
                "event_id": r["event_id"],
                "event_date": ev_date, "session_bucket": bucket,
                "priced_symbols": [x["symbol"] for x in labels],
                "unpriced_symbols": [s for s in syms if s not in per_sym],
                "labels": labels, "market_data_symbols": per_sym,
            }, ensure_ascii=False) + "\n")
            n_ok += 1

    summary = {"events_in": len(events), "labeled": n_ok, "skipped_few_symbols": n_skip}
    with open(f"{config.EVENT_REPORT_DIR}/stage_label_summary.json", "w") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)
    print(json.dumps(summary, ensure_ascii=False), flush=True)
