"""Stage E: yfinance 行情对齐 + 1D/5D/20D 隐藏标签.

两步:
  fetch  - 汇总所有事件的 symbol, 每个 symbol 拉一次全时段日线(auto_adjust), 存 prices_daily.parquet
           (可在服务器或本地 Mac 跑, 谁的网络好用谁; 面板文件可搬运)
  label  - 离线计算: base_close/first_tradable 日历规则, S0 可见窗口, 1D/5D/20D 标签, 截面排名

输出: market/prices_daily.parquet, market/labels.jsonl, reports/stage_label_summary.json
"""
from __future__ import annotations

import json
import os
import re
import time

from src import config

SYM_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,5}$")
ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
PRE_SESSIONS = 15    # S0 事件前可见交易日数
POST_SESSIONS = 26   # 标签审计窗交易日数(>=20d horizon + 余量)


def load_structured(path: str) -> list[dict]:
    out = []
    with open(path) as fh:
        for line in fh:
            r = json.loads(line)
            if r.get("_error"):
                continue
            out.append(r)
    return out


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
    events = load_structured(args.structured)
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
                if bi < PRE_SESSIONS or ft + POST_SESSIONS >= len(dates):
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
