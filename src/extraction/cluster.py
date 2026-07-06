"""Stage B: 从 v1 索引聚类出事件候选簇.

思路:
  1. duckdb 过滤出"事件相关"新闻池(去技术面模板/纯crypto/无正文)
  2. 公司轨: 每篇文章进它所有 symbol 的桶; 宏观轨: 无 symbol 但命中宏观主题词
  3. 桶内按时间排序, 3 天滑窗 + 标题相似度(token Jaccard 预筛 + token_set_ratio)连边, 并查集聚簇
  4. 跨桶合并共享成员的簇(同一事件在 NVDA 桶和 TSM 桶各聚出一簇的碎片问题)
  5. join v2 研报/电话会计数作为佐证信号

用法: python -m src.main extract cluster [--workers 32]
输出: candidates/clusters.parquet, candidates/members.parquet, reports/stage_b_summary.json
"""
from __future__ import annotations

import datetime as dt
import glob
import json
import os
import re
import shutil
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from rapidfuzz import fuzz

from src import config

# ---------------- 过滤规则 ----------------
TECH_NOISE_TAGS = ("Technical Analysis", "Volatility Metrics", "today_mover",
                   "personal_finance", "Top Gainers", "Top Losers")
MACRO_TOPICS = {
    "fed_policy": r"\b(fomc|federal reserve|fed (cuts?|hikes?|holds?|chair|minutes)|powell|interest rate (decision|cut|hike))\b",
    "inflation_jobs": r"\b(cpi|pce|ppi|inflation (data|report|rate)|payrolls|jobs report|unemployment rate)\b",
    "trade_tariff": r"\b(tariffs?|trade (war|deal|talks)|export (controls?|ban|curbs))\b",
    "regulation_policy": r"\b(white house|congress|treasury|sec (approves?|charges?|sues?)|antitrust|doj|ftc|supreme court|executive order|government shutdown)\b",
    "energy_geo": r"\b(opec|crude (oil|prices)|oil prices)\b",
}

STOP = frozenset("""a an the of for to in on at as and or with by from is are was were be been its it this
that after amid over under up down new says said report reports stock stocks shares share price prices
market markets today week""".split())
TOKEN_RE = re.compile(r"[a-z0-9]+")

WINDOW_DAYS = 3
JACCARD_MIN = 0.15
TOKEN_SET_MIN = 72
MERGE_OVERLAP = 0.3  # 跨桶簇合并的成员重合率
MAX_BUCKET_WINDOW = 400  # 滑窗内最多回看条数, 防热门桶 O(n^2) 爆炸


def tokens(title: str) -> frozenset:
    return frozenset(t for t in TOKEN_RE.findall(title.lower()) if t not in STOP and len(t) > 1)


class DSU:
    def __init__(self, n: int):
        self.p = list(range(n))

    def find(self, x: int) -> int:
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[rb] = ra


def cluster_bucket(items: list[dict]) -> list[list[int]]:
    """items 已按 pub_date 排序; 返回按下标分组的簇."""
    n = len(items)
    if n == 1:
        return [[0]]
    toks = [tokens(it["title"]) for it in items]
    dates = [dt.date.fromisoformat(it["pub_date"]) for it in items]
    dsu = DSU(n)
    left = 0
    for i in range(n):
        while (dates[i] - dates[left]).days > WINDOW_DAYS:
            left += 1
        lo = max(left, i - MAX_BUCKET_WINDOW)
        ti = toks[i]
        if not ti:
            continue
        for j in range(lo, i):
            tj = toks[j]
            if not tj:
                continue
            inter = len(ti & tj)
            if inter == 0 or inter / (len(ti) + len(tj) - inter) < JACCARD_MIN:
                continue
            if fuzz.token_set_ratio(items[i]["title"], items[j]["title"]) >= TOKEN_SET_MIN:
                dsu.union(i, j)
    groups = defaultdict(list)
    for i in range(n):
        groups[dsu.find(i)].append(i)
    return list(groups.values())


def process_shard(shard_file: str) -> str:
    tbl = pq.read_table(shard_file)
    by_bucket = defaultdict(list)
    for r in tbl.to_pylist():
        by_bucket[r["bucket"]].append(r)
    out = []
    for bucket, items in by_bucket.items():
        items.sort(key=lambda r: r["pub_date"])
        for gi, group in enumerate(cluster_bucket(items)):
            cid = f"{bucket}|{items[group[0]]['pub_date']}|{gi}"
            for idx in group:
                r = dict(items[idx])
                r.pop("shard", None)
                r["bucket_cluster_id"] = cid
                out.append(r)
    out_file = shard_file.replace("pool_shard", "clustered_shard")
    pq.write_table(pa.Table.from_pylist(out), out_file)
    return out_file


def run(args) -> None:
    os.makedirs(config.EVENT_CANDIDATE_DIR, exist_ok=True)
    os.makedirs(config.EVENT_REPORT_DIR, exist_ok=True)
    tmp_dir = os.path.join(config.EVENT_CANDIDATE_DIR, "tmp")
    shutil.rmtree(tmp_dir, ignore_errors=True)
    os.makedirs(tmp_dir)

    t0 = time.time()
    con = duckdb.connect()
    con.execute("SET threads TO 48")
    con.execute("SET memory_limit='400GB'")

    tech_cond = " AND ".join(f"tags NOT LIKE '%{t}%'" for t in TECH_NOISE_TAGS)
    macro_cases = " ".join(
        f"WHEN regexp_matches(lower(title), '{pat}') THEN 'macro_{k}'"
        for k, pat in MACRO_TOPICS.items()
    )

    # 1. 事件新闻池
    con.execute(f"""
    CREATE TEMP TABLE pool AS
    WITH base AS (
      SELECT id, file, "offset", nbytes, pub_date, published_at, content_type, title,
             symbols, importance, source_name, body_len, has_crypto, n_symbols
      FROM read_parquet('{config.EVENT_INDEX_DIR}/v1_*.parquet')
      WHERE content_type IN ('US_NEWS','US_FLASH','US_ARTICLE','US_ROBOT')
        AND pub_date <> '' AND body_len >= 200 AND title <> ''
        AND {tech_cond}
    ),
    company AS (
      SELECT b.* EXCLUDE (has_crypto), 'company' AS track, u.sym AS bucket
      FROM base b, UNNEST(string_split(b.symbols, ',')) AS u(sym)
      WHERE b.n_symbols BETWEEN 1 AND 8
        AND regexp_matches(u.sym, '^[A-Z][A-Z0-9.\\-]{{0,5}}$')
    ),
    macro AS (
      SELECT b.* EXCLUDE (has_crypto), 'macro' AS track,
             CASE {macro_cases} END AS bucket
      FROM base b
      WHERE b.n_symbols = 0 AND NOT b.has_crypto
    )
    SELECT * FROM company
    UNION ALL
    SELECT * FROM macro WHERE bucket IS NOT NULL
    """)
    n_pool = con.execute("SELECT count(*) FROM pool").fetchone()[0]
    print(f"[stage_b] 事件新闻池(含多symbol复制): {n_pool} 行 ({time.time()-t0:.0f}s)", flush=True)

    # 2. 按桶 hash 分 shard(同桶必同 shard), 落盘供并行聚类
    con.execute(f"""
    COPY (SELECT *, hash(bucket) % {args.shards} AS shard FROM pool)
    TO '{tmp_dir}' (FORMAT PARQUET, PARTITION_BY (shard), OVERWRITE_OR_IGNORE)
    """)
    shard_files = []
    for d in sorted(os.listdir(tmp_dir)):
        p = os.path.join(tmp_dir, d)
        if os.path.isdir(p) and d.startswith("shard="):
            for k, f in enumerate(sorted(os.listdir(p))):
                dst = os.path.join(tmp_dir, f"pool_shard_{d.split('=')[1]}_{k}.parquet")
                os.rename(os.path.join(p, f), dst)
                shard_files.append(dst)
    print(f"[stage_b] {len(shard_files)} shards, 开始聚类 ({time.time()-t0:.0f}s)", flush=True)

    # 3. 并行聚类
    clustered = []
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        for i, out in enumerate(ex.map(process_shard, shard_files), 1):
            clustered.append(out)
            if i % 16 == 0:
                print(f"[stage_b] clustered {i}/{len(shard_files)} ({time.time()-t0:.0f}s)", flush=True)

    # 4. 跨桶合并共享成员的簇
    con.execute(f"CREATE TEMP TABLE bm AS SELECT * FROM read_parquet('{tmp_dir}/clustered_shard_*.parquet')")
    sizes = dict(con.execute("SELECT bucket_cluster_id, count(DISTINCT id) FROM bm GROUP BY 1").fetchall())
    overlap = con.execute("""
      SELECT a.bucket_cluster_id, b.bucket_cluster_id, count(DISTINCT a.id)
      FROM bm a JOIN bm b ON a.id = b.id AND a.bucket_cluster_id < b.bucket_cluster_id
      GROUP BY 1, 2
    """).fetchall()
    cids = list(sizes)
    cidx = {c: i for i, c in enumerate(cids)}
    dsu = DSU(len(cids))
    for a, b, n_shared in overlap:
        if n_shared / min(sizes[a], sizes[b]) >= MERGE_OVERLAP:
            dsu.union(cidx[a], cidx[b])
    cid_to_event = {c: f"EVT_{dsu.find(cidx[c]):07d}" for c in cids}
    print(f"[stage_b] 桶内簇 {len(cids)} -> 合并后 {len(set(cid_to_event.values()))} 事件簇 ({time.time()-t0:.0f}s)", flush=True)

    # 5. members / clusters 落盘
    con.register("cmap", pa.table({
        "bucket_cluster_id": list(cid_to_event), "event_id": list(cid_to_event.values()),
    }))
    con.execute(f"""
    COPY (
      SELECT m.event_id, bm.id, any_value(bm.file) AS file, any_value(bm."offset") AS "offset",
             any_value(bm.nbytes) AS nbytes, any_value(bm.pub_date) AS pub_date,
             any_value(bm.published_at) AS published_at, any_value(bm.content_type) AS content_type,
             any_value(bm.source_name) AS source_name, any_value(bm.importance) AS importance,
             any_value(bm.title) AS title, any_value(bm.symbols) AS symbols,
             any_value(bm.body_len) AS body_len, any_value(bm.track) AS track
      FROM bm JOIN cmap m USING (bucket_cluster_id)
      GROUP BY m.event_id, bm.id
    ) TO '{config.EVENT_CANDIDATE_DIR}/members.parquet' (FORMAT PARQUET)
    """)

    con.execute(f"""
    CREATE TEMP TABLE stats AS
    SELECT event_id,
           count(*) AS n_articles,
           count(DISTINCT source_name) AS n_sources,
           count(DISTINCT content_type) AS n_content_types,
           min(pub_date) AS first_date, max(pub_date) AS last_date,
           mode(pub_date) AS peak_date,
           sum(CASE WHEN importance = 'high' THEN 1 ELSE 0 END) AS n_high,
           max(track) AS track,
           arg_max(title, body_len) AS rep_title
    FROM read_parquet('{config.EVENT_CANDIDATE_DIR}/members.parquet')
    GROUP BY 1
    """)
    con.execute(f"""
    CREATE TEMP TABLE esym AS
    SELECT DISTINCT event_id, u.sym
    FROM read_parquet('{config.EVENT_CANDIDATE_DIR}/members.parquet'),
         UNNEST(string_split(symbols, ',')) AS u(sym)
    WHERE u.sym <> ''
    """)
    # v2 佐证: 事件峰值日后 7 天内, 同 symbol 的研报/电话会数量(只算 n_articles>=2 的簇)
    con.execute(f"""
    CREATE TEMP TABLE v2r AS
    WITH v2 AS (
      SELECT doc_id, source_type, cast(pub_date AS DATE) AS d, u.sym
      FROM read_parquet('{config.EVENT_INDEX_DIR}/v2_*.parquet'),
           UNNEST(string_split(symbols, ',')) AS u(sym)
      WHERE pub_date <> '' AND u.sym <> ''
    )
    SELECT e.event_id, count(DISTINCT v2.doc_id) AS n_v2_reactions
    FROM esym e
    JOIN stats s USING (event_id)
    JOIN v2 ON v2.sym = e.sym
           AND v2.d BETWEEN cast(s.peak_date AS DATE) - 1 AND cast(s.peak_date AS DATE) + 7
    WHERE s.n_articles >= 2
    GROUP BY 1
    """)
    con.execute("""
    CREATE TEMP TABLE symagg AS
    SELECT event_id, string_agg(sym, ',') AS all_symbols
    FROM (SELECT event_id, sym,
                 row_number() OVER (PARTITION BY event_id ORDER BY sym) AS rn
          FROM esym)
    WHERE rn <= 30
    GROUP BY 1
    """)
    con.execute(f"""
    COPY (
      SELECT s.*, y.all_symbols, COALESCE(v.n_v2_reactions, 0) AS n_v2_reactions
      FROM stats s
      LEFT JOIN symagg y USING (event_id)
      LEFT JOIN v2r v USING (event_id)
    ) TO '{config.EVENT_CANDIDATE_DIR}/clusters.parquet' (FORMAT PARQUET)
    """)

    row = con.execute(f"""
      SELECT count(*), sum(CASE WHEN n_articles >= 3 THEN 1 ELSE 0 END),
             sum(CASE WHEN n_articles >= 10 THEN 1 ELSE 0 END)
      FROM read_parquet('{config.EVENT_CANDIDATE_DIR}/clusters.parquet')
    """).fetchone()
    out = {"pool_rows": n_pool, "clusters_total": row[0], "clusters_ge3": row[1],
           "clusters_ge10": row[2], "elapsed_sec": round(time.time() - t0, 1)}
    with open(f"{config.EVENT_REPORT_DIR}/stage_b_summary.json", "w") as fh:
        json.dump(out, fh, ensure_ascii=False, indent=2)
    print(json.dumps(out, ensure_ascii=False), flush=True)
