"""News/Flash 事件抽取漏斗实验: 逐层记录过滤数.

第一性推导(docs/per_record_extraction_problems.md)的落地验证:
  Pass A 逐条过滤(每层计数) -> 事件池
  Pass B 分桶聚类(公司轨/宏观轨) -> 簇规模分布 + 送审阈值扫描

不做地域过滤(P7 裁决: 数据以美股为主). 中间产物写 --tmpdir, 结果 JSON 写 --out.

用法:
  python3 scripts/funnel_experiment.py --v1-dir data/export_2025-07-08/v1 \
      --tmpdir /tmp/funnel --out reports/funnel_experiment.json
"""
from __future__ import annotations

import argparse
import json
import os
import re
import time
from collections import Counter, defaultdict

from rapidfuzz import fuzz

# ---------------- Pass A: 逐条过滤规则 ----------------
TECH_NOISE_TAGS = ("Technical Analysis", "Volatility Metrics", "today_mover",
                   "personal_finance", "Top Gainers", "Top Losers")

# 非事件标题规则(硬过滤, 按首个命中归因)
R_DATA_PRINT = re.compile(
    r"\b([1-4]Q|Q[1-4]|[12]H|FY\d{2}|div/shr|est\.|y/y|m/m|q/q|allotted|"
    r"inventor(y|ies)|loss/share|loss per share|EPS|comp sales|net sales|"
    r"revenue \$?\d|profit \d|gross margin|NIC \d|"
    r"shares? (traded|change hands) in a block|yield on|"
    r"(first|second|third|fourth)[- ]quarter (\d{4} )?(financial |fiscal )?(results|earnings)|"
    r"reports? (first|second|third|fourth) quarter|financial results)\b", re.I)
R_PRICE_MOVE = re.compile(
    r"\b(shares? (rise|rose|fall|fell|surge|drop|climb|slip|jump|gain|extend)|"
    r"stock (rises?|falls?|surges?|drops?|jumps?|surged|fell|jumped|dropped|soared|plunged)|"
    r"index (rose|fell|rises|falls|gains|drops)|"
    r"futures (are )?(steady|higher|lower|rise|fall|point)|"
    r"why .{0,40}(stock|shares) (surged|soared|fell|rose|dropped|jumped|plunged|moved|is (up|down))|"
    r"(rises?|falls?|drops?|gains?|climbs?|slides?|surged?|soar(s|ed)?|plunged?|tumbled?|jumped|slipped|up|down) "
    r"(over |more than |nearly |about )?\d+(\.\d+)?%)\b", re.I)
R_RECAP = re.compile(
    r"\b(stock market today|market today|pre-?market|after-?hours|most active|"
    r"week ahead|wall street (brunch|breakfast)|daily (turnover|recap)|"
    r"top analyst (forecasts|calls)|things to know|what to watch|morning (bid|brief))\b", re.I)
# 评级/例行 PR/垃圾模板(第二轮迭代新增: 首轮 top25 暴露的模板链)
R_ANALYST = re.compile(
    r"\b((maintains?|reiterates?|initiates?|resumes?|keeps?) .{0,50}"
    r"(rating|recommendation|coverage|outperform|underperform|neutral|overweight|underweight|buy|sell|hold)|"
    r"(is )?(maintained|reiterated|upgraded|downgraded) (at|by)|"
    r"given '.{0,25}' rating|new analyst forecast|"
    r"price target (raised|lowered|cut|maintained|to \$)|"
    r"(fitch|moody'?s( ratings)?|s&p( global( ratings)?)?) .{0,12}"
    r"(affirms?|assigns?|revises?|places?|withdraws?))\b", re.I)
R_ROUTINE_PR = re.compile(
    r"\b(to (participate|present|speak) (in|at)|presents? at .{0,50}conference|"
    r"conference call|webcast|fireside chat|investor (day|conference)|annual meeting of (stock|share)holders|"
    r"resignation of (board|company) secretary|schedules? board meeting|board meeting to (review|approve)|"
    r"annual general meeting|cash dividend on the way|declares? (quarterly|monthly) (cash )?dividend)\b", re.I)
R_HOLDINGS = re.compile(
    r"\b((buys?|sells?|adds?|trims?|boosts?|cuts?|acquires?) [\d,.]+ shares? of|"
    r"13[fF] (filing|filer)|lobbying (update|disclosure)|"
    r"(position|stake) (increased|decreased|boosted|trimmed|raised|lowered))\b", re.I)
R_JUNK = re.compile(
    r"(\bhoroscope\b|word of the day|\bcrossword\b|\bwordle\b|\brecipe\b|"
    r"^table:|earnings summary table|\brev nt\$|"
    r"how much \$?\d+ invested|if you (had )?invested \$?\d|gf (score|value)|-- gf |"
    r"golden cross|marubozu|\bkdj\b|death cross)", re.I)

HARD_RULES = [("data_print", R_DATA_PRINT), ("price_move", R_PRICE_MOVE), ("recap", R_RECAP),
              ("analyst_rating", R_ANALYST), ("routine_pr", R_ROUTINE_PR),
              ("holdings_flow", R_HOLDINGS), ("junk_template", R_JUNK)]

# 宏观主题桶(现有 5 类 + 方案新增 5 类)
MACRO_TOPICS = {
    "fed_policy": r"\b(fomc|federal reserve|fed (cuts?|hikes?|holds?|chair|minutes)|powell|interest rate (decision|cut|hike))\b",
    "inflation_jobs": r"\b(cpi|pce|ppi|inflation (data|report|rate)|payrolls|jobs report|unemployment rate)\b",
    "trade_tariff": r"\b(tariffs?|trade (war|deal|talks)|export (controls?|ban|curbs)|anti-dumping)\b",
    "regulation_policy": r"\b(white house|congress|treasury|sec (approves?|charges?|sues?)|antitrust|doj|ftc|supreme court|executive order|government shutdown)\b",
    "energy_geo": r"\b(opec|crude (oil|prices)|oil prices)\b",
    "labor_strike": r"\b(strikes?|walkouts?|work stoppage|lockout|uaw|teamsters|union (vote|contract|deal|action))\b",
    "fda_regulatory": r"\b(fda (approv|reject|clear)|advisory committee|complete response letter|biologics license)\b",
    "crypto_structure": r"\b(spot (bitcoin|ether|ethereum) et[fp]|crypto legislation|stablecoin (bill|act)|genius act)\b",
    "gov_program": r"\b(stargate|executive order on ai|chips act|infrastructure (bill|act)|stimulus)\b",
    "geopolitical": r"\b(sanctions?|export ban|military (strike|action)|ceasefire|invasion)\b",
}
MACRO_RE = {k: re.compile(v, re.I) for k, v in MACRO_TOPICS.items()}

# tags 召回门(仅 importance=high 时生效; tags 噪声高, 只作召回)
MACRO_TAGS = frozenset({"CentralBanking", "Monetary Policy", "Trade Agreements",
                        "GeopoliticalConflict", "Regulation", "Policy", "Foreign Policy"})

SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,5}$")
DATE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})")

# ---------------- 模板骨架(与 template_miner 共享) ----------------
CAPWORD = re.compile(r"^[A-Z][A-Za-z.&'’\-]*$|^[A-Z0-9.\-]{2,8}$")


def skeleton(title: str) -> tuple:
    """掩码标题 -> (骨架, 主体元组). 含数字 token -> '#', 连续大写词串折叠为 '@'."""
    toks, subs, prev_at = [], [], False
    for w in title.split():
        ws = w.strip("(),:;\"'“”|")
        if any(ch.isdigit() for ch in ws):
            toks.append("#")
            prev_at = False
        elif ws and CAPWORD.match(ws):
            if not prev_at:
                toks.append("@")
            subs.append(ws.lower())
            prev_at = True
        else:
            toks.append(ws.lower())
            prev_at = False
    return " ".join(toks), tuple(subs)

STOP = frozenset("""a an the of for to in on at as and or with by from is are was were be been its it this
that after amid over under up down new says said report reports stock stocks shares share price prices
market markets today week""".split())
TOKEN_RE = re.compile(r"[a-z0-9]+")

WINDOW_DAYS = 3
JACCARD_MIN = 0.15
TOKEN_SET_MIN = 72
MERGE_OVERLAP = 0.3
MAX_BUCKET_WINDOW = 400


def tokens(title: str) -> frozenset:
    return frozenset(t for t in TOKEN_RE.findall(title.lower()) if t not in STOP and len(t) > 1)


def pass_a(v1_dir: str, tmpdir: str, templates: set | None = None) -> dict:
    """逐条过滤, 每层计数; 幸存者写入池文件.

    templates: template_miner 挖出的模板骨架集合, 作 L3b 数据驱动过滤层.
    """
    os.makedirs(tmpdir, exist_ok=True)
    funnel: dict = {}
    pool_path = os.path.join(tmpdir, "pool.jsonl")
    seen_titles: set = set()
    with open(pool_path, "w") as pool:
        for src in ("US_FLASH", "US_NEWS"):
            t0 = time.time()
            c = Counter()
            path = os.path.join(v1_dir, f"{src}.jsonl")
            with open(path) as fh:
                for line in fh:
                    c["L0_raw"] += 1
                    try:
                        r = json.loads(line)
                    except json.JSONDecodeError:
                        c["L1_drop_badjson"] += 1
                        continue
                    title = (r.get("title") or "").strip()
                    m = DATE_RE.match(r.get("published_at") or "")
                    if not title or not m:
                        c["L1_drop_no_title_or_date"] += 1
                        continue
                    date = m.group(1)
                    if not ("2000-01-01" <= date <= "2026-08-01"):
                        c["L1_drop_bad_date"] += 1
                        continue
                    c["L1_valid"] += 1

                    tags = r.get("tags") or []
                    if any(t in TECH_NOISE_TAGS for t in tags):
                        c["L2_drop_tech_noise_tags"] += 1
                        continue
                    c["L2_pass"] += 1

                    hit = next((name for name, rx in HARD_RULES if rx.search(title)), None)
                    if hit:
                        c[f"L3_drop_{hit}"] += 1
                        continue
                    c["L3_pass"] += 1

                    if templates and skeleton(title)[0] in templates:
                        c["L3b_drop_template_mined"] += 1
                        continue
                    c["L3b_pass"] += 1

                    # 正文/标题长度准入: body>=200 或 标题够长(FLASH 一句话准入)
                    body_len = len(r.get("body") or "")
                    if body_len < 200 and len(title) < 40:
                        c["L4_drop_too_short"] += 1
                        continue
                    if body_len < 200:
                        c["L4_admitted_by_title_only"] += 1  # 观察量, 不减池
                    c["L4_pass"] += 1

                    key = (date, " ".join(sorted(tokens(title))))
                    if key in seen_titles:
                        c["L5_drop_exact_dup"] += 1
                        continue
                    seen_titles.add(key)
                    c["L5_pass"] += 1

                    # 轨道分配.
                    # 注意: 本导出 v1 全部记录 entities 均无 stocks(实测全文件 0 条),
                    # 现有管道的"公司轨按 symbol 分桶"在这份数据上不可用;
                    # 此处仍统计以备后续数据修复, 非宏观记录走 general 轨(稀有 token 分桶).
                    ent = r.get("entities") or {}
                    syms = []
                    for e in (ent.get("stocks") or []):
                        code = (e.get("symbol") or "").strip()
                        if SYMBOL_RE.match(code):
                            syms.append(code)
                    syms = sorted(set(syms))
                    if syms:
                        c["L6_has_stock_entities"] += 1  # 观察量
                    macro_bucket = next((f"macro_{k}" for k, rx in MACRO_RE.items() if rx.search(title)), None)
                    if not macro_bucket and r.get("importance") == "high" and MACRO_TAGS & set(tags):
                        main = sorted(MACRO_TAGS & set(tags))[0]
                        macro_bucket = f"macro_tag_{main.replace(' ', '_')}"
                        c["L6_macro_via_tags_gate"] += 1  # 观察量

                    if macro_bucket:
                        c["L6_track_macro"] += 1
                        track, bucket = "macro", macro_bucket
                    else:
                        c["L6_track_general"] += 1
                        track, bucket = "general", ""  # bucket 由 Pass B 按稀有 token 决定

                    pool.write(json.dumps({"id": r.get("id"), "src": src, "date": date,
                                           "title": title, "track": track, "bucket": bucket},
                                          ensure_ascii=False) + "\n")
                    c["L6_pool_records"] += 1
            funnel[src] = dict(c)
            print(f"[pass_a] {src}: {c['L0_raw']} raw -> {c.get('L6_pool_records', 0)} pooled "
                  f"({time.time()-t0:.0f}s)", flush=True)
    return funnel


class DSU:
    def __init__(self, n: int):
        self.p = list(range(n))

    def find(self, x: int) -> int:
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a: int, b: int):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[rb] = ra


RARE_DF_RATIO = 0.0014  # 稀有词阈值 = 池大小 * 此比例(本导出 145 万池 ≈ 2000), 跨语料自适应
_RARE_DF_MAX = [2000]   # 运行时由 pass_b 按池大小设定


def cluster_bucket(rows: list, df: Counter | None = None, require_rare: bool = False) -> list:
    """桶内 3 天滑窗 + Jaccard 预筛 + token_set_ratio 连边; 返回 cluster 标号列表.

    require_rare: 连边额外要求共享 >=1 个稀有 token(主体词锚定).
    首轮实验教训: 无主体锚定时, "Why X Stock Surged Today" 这类模板标题会把
    不同公司经传递闭包链成万条级巨簇.
    """
    rows.sort(key=lambda r: r["date"])
    toks = [r.get("toks") or tokens(r["title"]) for r in rows]
    dsu = DSU(len(rows))
    for i in range(len(rows)):
        di = rows[i]["date"]
        lo = max(0, i - MAX_BUCKET_WINDOW)
        for j in range(i - 1, lo - 1, -1):
            if (dt_days(rows[j]["date"], di)) > WINDOW_DAYS:
                break
            ti, tj = toks[i], toks[j]
            if not ti or not tj:
                continue
            shared = ti & tj
            if len(shared) / max(1, len(ti | tj)) < JACCARD_MIN:
                continue
            if require_rare and df is not None and not any(df[t] <= _RARE_DF_MAX[0] for t in shared):
                continue
            if fuzz.token_set_ratio(rows[i]["title"].lower(), rows[j]["title"].lower()) >= TOKEN_SET_MIN:
                dsu.union(i, j)
    return [dsu.find(i) for i in range(len(rows))]


def dt_days(d1: str, d2: str) -> int:
    y1, m1, dd1 = int(d1[:4]), int(d1[5:7]), int(d1[8:10])
    y2, m2, dd2 = int(d2[:4]), int(d2[5:7]), int(d2[8:10])
    import datetime as _dt
    return abs((_dt.date(y2, m2, dd2) - _dt.date(y1, m1, dd1)).days)


def pass_b(tmpdir: str, dump_path: str | None = None) -> dict:
    """分桶聚类 + 跨桶合并 + 阈值扫描.

    macro 记录用主题桶; general 记录(本导出无个股实体, 无法按 symbol 分桶)
    按标题 3 个最稀有 token 各进一桶(同事件改写标题必共享稀有词, 跨桶合并去重).
    """
    t0 = time.time()
    rows_all: list = []
    df: Counter = Counter()
    with open(os.path.join(tmpdir, "pool.jsonl")) as fh:
        for line in fh:
            r = json.loads(line)
            r["toks"] = tokens(r["title"])
            rows_all.append(r)
            df.update(r["toks"])
    _RARE_DF_MAX[0] = max(200, int(len(rows_all) * RARE_DF_RATIO))
    print(f"[pass_b] pool {len(rows_all)} records, {len(df)} tokens, "
          f"rare_df_max={_RARE_DF_MAX[0]} ({time.time()-t0:.0f}s)", flush=True)

    buckets: dict = defaultdict(list)
    solo: list = []  # 全部 token df=1 -> 不可能与任何行共词, 必为单例簇
    for r in rows_all:
        if r["track"] == "macro":
            buckets[r["bucket"]].append(r)
            continue
        rare = [t for t in sorted(r["toks"], key=lambda t: (df[t], t))[:3] if df[t] >= 2]
        if rare:
            for t in rare:
                buckets[f"g_{t}"].append(r)
        else:
            solo.append(r)
    del rows_all
    print(f"[pass_b] {len(buckets)} buckets + {len(solo)} solo ({time.time()-t0:.0f}s)", flush=True)

    # 桶内聚类
    cluster_members: dict = defaultdict(set)   # gcid -> set(record id)
    cluster_meta: dict = {}                    # gcid -> (bucket, track, sample_title, dates)
    cluster_titles: dict = defaultdict(list)   # gcid -> 采样标题(供审计 dump)
    n_done = 0
    for b, rows in buckets.items():
        is_general = not b.startswith("macro_")
        labels = cluster_bucket(rows, df=df, require_rare=is_general)
        track = "general" if is_general else "macro"
        for lab, r in zip(labels, rows):
            gcid = f"{b}::{lab}"
            cluster_members[gcid].add(r["id"])
            if dump_path and len(cluster_titles[gcid]) < 8:
                cluster_titles[gcid].append(f'[{r["date"]}|{r["src"][3:]}] {r["title"][:130]}')
            if gcid not in cluster_meta:
                cluster_meta[gcid] = {"bucket": b, "track": track, "title": r["title"],
                                      "d0": r["date"], "d1": r["date"]}
            else:
                m = cluster_meta[gcid]
                m["d0"], m["d1"] = min(m["d0"], r["date"]), max(m["d1"], r["date"])
        n_done += 1
        if n_done % 5000 == 0:
            print(f"[pass_b] clustered {n_done}/{len(buckets)} buckets ({time.time()-t0:.0f}s)", flush=True)
    print(f"[pass_b] intra-bucket clusters: {len(cluster_members)} ({time.time()-t0:.0f}s)", flush=True)

    # 跨桶合并: 共享成员占小簇比例 >= MERGE_OVERLAP
    gcids = list(cluster_members)
    idx = {g: i for i, g in enumerate(gcids)}
    by_record: dict = defaultdict(list)
    for g, mem in cluster_members.items():
        for rid in mem:
            by_record[rid].append(g)
    dsu = DSU(len(gcids))
    pair_shared: Counter = Counter()
    for rid, gs in by_record.items():
        if len(gs) < 2:
            continue
        gs = sorted(gs)
        for i in range(len(gs)):
            for j in range(i + 1, len(gs)):
                pair_shared[(gs[i], gs[j])] += 1
    for (ga, gb), shared in pair_shared.items():
        if shared / min(len(cluster_members[ga]), len(cluster_members[gb])) >= MERGE_OVERLAP:
            dsu.union(idx[ga], idx[gb])
    merged: dict = defaultdict(set)
    merged_meta: dict = {}
    for g in gcids:
        root = gcids[dsu.find(idx[g])]
        merged[root] |= cluster_members[g]
        m, src = merged_meta.get(root), cluster_meta[g]
        if m is None:
            merged_meta[root] = dict(src)
        else:
            m["d0"], m["d1"] = min(m["d0"], src["d0"]), max(m["d1"], src["d1"])
            if src["track"] == "macro":
                m["track"] = "macro"  # 混合簇按 macro 记
    print(f"[pass_b] merged clusters: {len(merged)} ({time.time()-t0:.0f}s)", flush=True)

    # solo(全稀有词)记录直接计为单例簇
    n_solo = len(solo)

    # 统计与阈值扫描
    sizes = {g: len(mem) for g, mem in merged.items()}
    dist = Counter()
    for s in sizes.values():
        dist["1" if s == 1 else "2" if s == 2 else "3-4" if s <= 4 else
             "5-9" if s <= 9 else "10-49" if s <= 49 else "50-199" if s <= 199 else "200+"] += 1
    if dump_path:
        root_titles: dict = defaultdict(list)  # 一遍聚合: 根簇 -> 采样标题
        for g2, ts in cluster_titles.items():
            root = gcids[dsu.find(idx[g2])]
            if len(root_titles[root]) < 8:
                root_titles[root].extend(ts[: 8 - len(root_titles[root])])
        with open(dump_path, "w") as f:
            for g, mem in merged.items():
                if len(mem) < 5:
                    continue
                m = merged_meta[g]
                f.write(json.dumps({"size": len(mem), "track": m["track"], "bucket": m["bucket"],
                                    "span": f'{m["d0"]}..{m["d1"]}', "titles": root_titles.get(g, [])},
                                   ensure_ascii=False) + "\n")
        print(f"[pass_b] dumped size>=5 clusters -> {dump_path}", flush=True)

    dist["1"] += n_solo
    # 链化诊断: size>=5 的簇中时间跨度 >7 天的占比(真事件报道应集中在几天内)
    spans = [dt_days(merged_meta[g]["d0"], merged_meta[g]["d1"])
             for g, s in sizes.items() if s >= 5]
    chain_stat = {"n_size5plus": len(spans),
                  "span_gt7d": sum(1 for s in spans if s > 7),
                  "span_gt30d": sum(1 for s in spans if s > 30)}
    sweep = {}
    for k in (2, 3, 5, 8):
        ok = [g for g, s in sizes.items() if s >= k]
        sweep[f"n>={k}"] = {
            "clusters": len(ok),
            "macro": sum(1 for g in ok if merged_meta[g]["track"] == "macro"),
            "general": sum(1 for g in ok if merged_meta[g]["track"] == "general"),
        }
    top = sorted(sizes.items(), key=lambda x: -x[1])[:25]
    top_view = [{"size": s, "bucket": merged_meta[g]["bucket"], "track": merged_meta[g]["track"],
                 "span": f'{merged_meta[g]["d0"]}..{merged_meta[g]["d1"]}',
                 "title": merged_meta[g]["title"][:110]} for g, s in top]
    return {"n_clusters": len(merged) + n_solo, "n_solo_singletons": n_solo,
            "size_dist": dict(dist), "chain_stat": chain_stat,
            "sweep": sweep, "top25": top_view}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--v1-dir", required=True)
    ap.add_argument("--tmpdir", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--skip-a", action="store_true", help="复用已有 pool.jsonl")
    ap.add_argument("--dump", default=None, help="把 size>=5 簇(含采样标题)落盘到此路径供审计")
    ap.add_argument("--templates", default=None,
                    help="template_miner 产出的模板骨架文件, 作 L3b 数据驱动过滤层")
    args = ap.parse_args()

    templates = None
    if args.templates:
        templates = {line.rstrip("\n") for line in open(args.templates) if line.strip()}
        print(f"[main] loaded {len(templates)} mined templates", flush=True)

    result = {}
    if not args.skip_a:
        result["funnel"] = pass_a(args.v1_dir, args.tmpdir, templates=templates)
    else:
        prev = json.load(open(args.out))
        result["funnel"] = prev.get("funnel", {})
    result["clustering"] = pass_b(args.tmpdir, dump_path=args.dump)
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(result, f, ensure_ascii=False, indent=1)
    print(f"[done] -> {args.out}", flush=True)


if __name__ == "__main__":
    main()
