"""模板骨架挖掘 + 时间外推(泛化性)实验.

回答: 漏斗的 L3 噪声过滤能否泛化到别的时间窗(近几年数据)?

思路: 手写正则不泛化(按窗口打地鼠所得). 泛化替代 = 数据驱动挖模板:
  骨架 = 标题掩码(含数字 token -> '#', 首大写词连串 -> '@')
  模板 = 骨架满足 [高频 + 多主体 + 来源集中 + 有足够固定词]
挖掘按时间窗自适应, 无人工规则.

实验: 数据切 H1(2025-07..12) / H2(2026-01..06):
  1. H1 挖模板 T1, H2 挖模板 T2(oracle)
  2. 跨期保持率 = T1 在 H2 的杀伤 / T2 在 H2 的杀伤(oracle)
  3. 漂移 = T2 中 T1 没有的新模板及其 H2 命中量
  4. 与手写 L3 正则在 H2 上的杀伤对比(重叠/互补)

用法: python3 scripts/template_miner.py --v1-dir data/export_2025-07-08/v1 --out <json> [--dump-dir <dir>]
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import funnel_experiment as fe  # 复用 L1/L2 判定与 L3 手写规则

H1_END = "2026-01-01"  # H1: < 此日期; H2: >= 此日期

CAPWORD = re.compile(r"^[A-Z][A-Za-z.&'’\-]*$|^[A-Z0-9.\-]{2,8}$")  # 首大写词 或 ticker 样
MIN_COUNT = 20        # 模板骨架最小出现次数
MIN_SUBJECTS = 10     # 最少不同主体
MIN_SRC_SHARE = 0.6   # 头部来源占比下限(模板流来自单一自动化栏目)
MIN_FIXED = 3         # 骨架中至少几个未被掩码的固定词


def skeleton(title: str) -> tuple[str, tuple]:
    """掩码标题 -> (骨架, 主体元组). 连续大写词折叠为一个 '@'."""
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


def iter_records(v1_dir: str):
    """产出 (half, src_name, title). 仅 L1 有效记录(有标题有日期)."""
    for src in ("US_FLASH", "US_NEWS"):
        path = os.path.join(v1_dir, f"{src}.jsonl")
        with open(path) as fh:
            for line in fh:
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                title = (r.get("title") or "").strip()
                m = fe.DATE_RE.match(r.get("published_at") or "")
                if not title or not m:
                    continue
                half = "H1" if m.group(1) < H1_END else "H2"
                yield half, ((r.get("source") or {}).get("name") or ""), title


def mine(v1_dir: str, dump_dir: str | None):
    t0 = time.time()
    # pass 1: 骨架频次(分半)
    freq: dict = {"H1": Counter(), "H2": Counter()}
    for half, _, title in iter_records(v1_dir):
        sk, _ = skeleton(title)
        freq[half][sk] += 1
    print(f"[p1] skeletons H1={len(freq['H1'])} H2={len(freq['H2'])} ({time.time()-t0:.0f}s)", flush=True)

    # pass 2: 对候选骨架(任一半 >= MIN_COUNT)统计主体数/来源集中度
    cand = {sk for sk, c in freq["H1"].items() if c >= MIN_COUNT}
    cand |= {sk for sk, c in freq["H2"].items() if c >= MIN_COUNT}
    detail: dict = {h: defaultdict(lambda: [set(), Counter()]) for h in ("H1", "H2")}
    for half, src_name, title in iter_records(v1_dir):
        sk, subs = skeleton(title)
        if sk in cand:
            d = detail[half][sk]
            if len(d[0]) < 60:
                d[0].add(subs)
            d[1][src_name] += 1
    print(f"[p2] candidates={len(cand)} ({time.time()-t0:.0f}s)", flush=True)

    def decide(half: str) -> set:
        out = set()
        for sk, (subs, srcs) in detail[half].items():
            c = freq[half][sk]
            if c < MIN_COUNT or len(subs) < MIN_SUBJECTS:
                continue
            if sum(1 for t in sk.split() if t not in ("#", "@")) < MIN_FIXED:
                continue
            if max(srcs.values()) / c < MIN_SRC_SHARE:
                continue
            out.add(sk)
        return out

    T1, T2 = decide("H1"), decide("H2")
    print(f"[mine] templates H1={len(T1)} H2={len(T2)} ({time.time()-t0:.0f}s)", flush=True)

    # pass 3: H2 上评估 — T1 杀伤 vs T2(oracle) 杀伤 vs 手写 L3 正则
    n_h2 = kill_t1 = kill_t2 = kill_rx = kill_both = kill_rx_only = kill_t1_only = 0
    samples_t1, samples_drift = [], []
    drift_sks = T2 - T1
    drift_hits = Counter()
    for half, _, title in iter_records(v1_dir):
        if half != "H2":
            continue
        n_h2 += 1
        sk, _ = skeleton(title)
        in_t1, in_t2 = sk in T1, sk in T2
        in_rx = any(rx.search(title) for _, rx in fe.HARD_RULES)
        kill_t1 += in_t1
        kill_t2 += in_t2
        kill_rx += in_rx
        kill_both += in_t1 and in_rx
        kill_rx_only += in_rx and not in_t1
        kill_t1_only += in_t1 and not in_rx
        if in_t1 and len(samples_t1) < 40 and n_h2 % 97 == 0:
            samples_t1.append(title[:120])
        if sk in drift_sks:
            drift_hits[sk] += 1
            if len(samples_drift) < 40 and drift_hits[sk] == 3:
                samples_drift.append(title[:120])
    print(f"[p3] H2={n_h2} ({time.time()-t0:.0f}s)", flush=True)

    result = {
        "config": {"MIN_COUNT": MIN_COUNT, "MIN_SUBJECTS": MIN_SUBJECTS,
                   "MIN_SRC_SHARE": MIN_SRC_SHARE, "MIN_FIXED": MIN_FIXED, "H1_END": H1_END},
        "templates": {"H1": len(T1), "H2": len(T2),
                      "shared": len(T1 & T2), "drift_new_in_H2": len(drift_sks)},
        "eval_on_H2": {
            "n_records": n_h2,
            "killed_by_T1(跨期)": kill_t1,
            "killed_by_T2(oracle)": kill_t2,
            "跨期保持率": round(kill_t1 / max(1, kill_t2), 3),
            "killed_by_hand_regex": kill_rx,
            "T1∩regex": kill_both, "regex独有": kill_rx_only, "T1独有": kill_t1_only,
            "drift模板的H2命中": sum(drift_hits.values()),
        },
        "top_drift_templates": [{"skeleton": sk, "h2_hits": c}
                                for sk, c in drift_hits.most_common(15)],
        "samples_killed_by_T1_in_H2": samples_t1,
        "samples_drift": samples_drift,
    }
    if dump_dir:
        os.makedirs(dump_dir, exist_ok=True)
        for name, T in (("templates_H1.txt", T1), ("templates_H2.txt", T2)):
            with open(os.path.join(dump_dir, name), "w") as f:
                for sk in sorted(T, key=lambda s: -freq["H1" if name.endswith("H1.txt") else "H2"][s]):
                    f.write(sk + "\n")
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--v1-dir", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--dump-dir", default=None)
    args = ap.parse_args()
    result = mine(args.v1_dir, args.dump_dir)
    with open(args.out, "w") as f:
        json.dump(result, f, ensure_ascii=False, indent=1)
    print(json.dumps(result["eval_on_H2"], ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
