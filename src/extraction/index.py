"""Stage A: 全量扫描 v1(新闻) + v2(研报段落), 产出 parquet 轻量索引.

用法(服务器上):
    python -m src.main extract index          # 全量
    python -m src.main extract index --limit 2  # 每个目录只跑2个文件,冒烟测试

输出:
    index/v1_<batch>.parquet       v1 记录级索引(含文件字节偏移,后续可 seek 取正文)
    index/v2_<batch>.parquet       v2 doc 级索引(段落聚合;跨文件的 doc 由下游 group by 合并)
    index/sympairs_<batch>.parquet 观察到的 (symbol,name) 对计数,用于建 名称->代码 词典
    reports/stage_a_summary.json
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed

import orjson
import pyarrow as pa
import pyarrow.parquet as pq

from src import config

DATE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})")
ACCESSION_RE = re.compile(r"^notice:(\S+)$")
# v2 related_codes 形如 EXEL.O / WFC.N / UDI-1.N, 取交易所后缀前的主体
V2_CODE_RE = re.compile(r"^([A-Z][A-Z0-9\.\-]{0,9})\.(O|N|OQ|A|K|P)$")


def _as_dict(v) -> dict:
    """v2 的 source/context 可能是 dict 也可能是字符串化 dict."""
    if isinstance(v, dict):
        return v
    if isinstance(v, str) and v.startswith("{"):
        try:
            import ast
            d = ast.literal_eval(v)
            return d if isinstance(d, dict) else {}
        except Exception:
            return {}
    return {}


def norm_date(ts: str | None) -> str:
    if not ts:
        return ""
    m = DATE_RE.match(ts)
    d = m.group(1) if m else ""
    # v2 有 1970 和 2027 的脏时间戳, 超出业务合理区间的置空
    return d if "2000-01-01" <= d <= "2026-08-01" else ""


def v1_extract(rec: dict) -> dict:
    ents = rec.get("entities") or {}
    stocks = ents.get("stocks") or []
    symbols = [s.get("symbol", "") for s in stocks if s.get("symbol")]
    notice = rec.get("notice") or {}
    dedup_key = (rec.get("dedup") or {}).get("key", "")
    acc = ACCESSION_RE.match(dedup_key)
    pub = rec.get("published_at") or ""
    return {
        "id": rec.get("id", ""),
        "published_at": pub,
        "pub_date": norm_date(pub),
        "content_type": rec.get("content_type") or "",
        "title": (rec.get("title") or "")[: config.EVENT_TITLE_MAX_CHARS],
        "symbols": ",".join(symbols),
        "n_symbols": len(symbols),
        "has_crypto": bool(ents.get("crypto")),
        "importance": rec.get("importance") or "",
        "tags": ",".join(rec.get("tags") or []),
        "regions": ",".join(rec.get("regions") or []),
        "source_name": (rec.get("source") or {}).get("name", ""),
        "body_len": len(rec.get("body") or ""),
        "filing_type": notice.get("filing_type", ""),
        "declare_date": notice.get("declare_date", ""),
        "accession": acc.group(1) if acc else "",
    }


V1_SCHEMA = pa.schema([
    ("id", pa.string()), ("file", pa.string()), ("line_no", pa.int32()),
    ("offset", pa.int64()), ("nbytes", pa.int32()),
    ("published_at", pa.string()), ("pub_date", pa.string()),
    ("content_type", pa.string()), ("title", pa.string()),
    ("symbols", pa.string()), ("n_symbols", pa.int16()), ("has_crypto", pa.bool_()),
    ("importance", pa.string()), ("tags", pa.string()), ("regions", pa.string()),
    ("source_name", pa.string()), ("body_len", pa.int32()),
    ("filing_type", pa.string()), ("declare_date", pa.string()), ("accession", pa.string()),
])

V2_SCHEMA = pa.schema([
    ("doc_id", pa.string()), ("file", pa.string()),
    ("source_type", pa.string()), ("title", pa.string()),
    ("published_at", pa.string()), ("pub_date", pa.string()),
    ("symbols", pa.string()), ("n_paragraphs", pa.int32()),
    ("text_len", pa.int64()), ("offsets", pa.string()),
    ("source_name", pa.string()),
])


def index_v1_file(path: str) -> dict:
    base = os.path.basename(path)
    rows, sym_pairs = [], Counter()
    offset = 0
    with open(path, "rb", buffering=8 * 1024 * 1024) as fh:
        for line_no, raw in enumerate(fh, 1):
            nbytes = len(raw)
            try:
                rec = orjson.loads(raw)
                row = v1_extract(rec)
                row.update({"file": base, "line_no": line_no, "offset": offset, "nbytes": nbytes})
                rows.append(row)
                for s in (rec.get("entities") or {}).get("stocks") or []:
                    if s.get("symbol") and s.get("name"):
                        sym_pairs[(s["symbol"], s["name"])] += 1
            except Exception:
                pass
            offset += nbytes
    _write(rows, V1_SCHEMA, f"v1_{base.replace('.jsonl', '')}.parquet")
    _write_sympairs(sym_pairs, base)
    return {"file": base, "rows": len(rows)}


def index_v2_file(path: str) -> dict:
    base = os.path.basename(path)
    docs: dict[str, dict] = {}
    sym_pairs = Counter()
    offset = 0
    n = 0
    with open(path, "rb", buffering=8 * 1024 * 1024) as fh:
        for raw in fh:
            nbytes = len(raw)
            try:
                rec = orjson.loads(raw)
                n += 1
                did = rec.get("doc_id") or ""
                d = docs.get(did)
                if d is None:
                    src = _as_dict(rec.get("source"))
                    ctx = _as_dict(rec.get("context"))
                    codes = []
                    for c in ctx.get("related_codes") or []:
                        m = V2_CODE_RE.match(c)
                        codes.append(m.group(1) if m else c)
                    pub = rec.get("published_at") or ""
                    d = docs[did] = {
                        "doc_id": did, "file": base,
                        "source_type": rec.get("source_type") or "",
                        "title": (rec.get("title") or "")[: config.EVENT_TITLE_MAX_CHARS],
                        "published_at": pub, "pub_date": norm_date(pub),
                        "symbols": ",".join(dict.fromkeys(codes)),
                        "n_paragraphs": 0, "text_len": 0, "_offsets": [],
                        "source_name": src.get("name", "") if isinstance(src, dict) else "",
                    }
                d["n_paragraphs"] += 1
                d["text_len"] += len(rec.get("text") or "")
                d["_offsets"].append(offset)
            except Exception:
                pass
            offset += nbytes
    rows = []
    for d in docs.values():
        d["offsets"] = ",".join(map(str, d.pop("_offsets")))
        rows.append(d)
    _write(rows, V2_SCHEMA, f"v2_{base.replace('.jsonl', '')}.parquet")
    _write_sympairs(sym_pairs, base)
    return {"file": base, "rows": n, "docs": len(rows)}


def _write(rows: list[dict], schema: pa.Schema, name: str) -> None:
    cols = {f.name: [r.get(f.name) for r in rows] for f in schema}
    dst = os.path.join(config.EVENT_INDEX_DIR, name)
    pq.write_table(pa.table(cols, schema=schema), dst + ".tmp")
    os.replace(dst + ".tmp", dst)  # 原子落盘, 中途被杀不会留半截文件


def _write_sympairs(pairs: Counter, base: str) -> None:
    if not pairs:
        return
    tbl = pa.table({
        "symbol": [k[0] for k in pairs], "name": [k[1] for k in pairs],
        "count": list(pairs.values()),
    })
    dst = os.path.join(config.EVENT_INDEX_DIR, f"sympairs_{base.replace('.jsonl', '')}.parquet")
    pq.write_table(tbl, dst + ".tmp")
    os.replace(dst + ".tmp", dst)


def run(args: argparse.Namespace) -> None:
    os.makedirs(config.EVENT_INDEX_DIR, exist_ok=True)
    os.makedirs(config.EVENT_REPORT_DIR, exist_ok=True)

    v1 = sorted(glob.glob(f"{config.EVENT_V1_DIR}/cleaned_batch*.jsonl"))
    v2 = sorted(glob.glob(f"{config.EVENT_V2_DIR}/cleaned_batch*.jsonl"))
    if args.limit:
        v1, v2 = v1[: args.limit], v2[: args.limit]
    jobs = [("v1", p) for p in v1] + [("v2", p) for p in v2]
    if not args.fresh:
        def _done(kind: str, path: str) -> bool:
            stem = os.path.basename(path).replace(".jsonl", "")
            return os.path.exists(os.path.join(config.EVENT_INDEX_DIR, f"{kind}_{stem}.parquet"))
        before = len(jobs)
        jobs = [(k, p) for k, p in jobs if not _done(k, p)]
        print(f"[stage_a] 断点续跑: 跳过已完成 {before - len(jobs)} 个文件", flush=True)
    print(f"[stage_a] {len(v1)} v1 files + {len(v2)} v2 files, 待跑 {len(jobs)}, workers={args.workers}", flush=True)

    t0 = time.time()
    results, failed = [], []
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(index_v1_file if k == "v1" else index_v2_file, p): (k, p) for k, p in jobs}
        for i, fut in enumerate(as_completed(futs), 1):
            kind, path = futs[fut]
            try:
                r = fut.result()
                results.append({**r, "kind": kind})
                print(f"[{i}/{len(jobs)}] {kind} {r['file']} rows={r['rows']} "
                      f"({time.time()-t0:.0f}s)", flush=True)
            except Exception as e:
                failed.append({"file": path, "error": str(e)})
                print(f"[{i}/{len(jobs)}] FAILED {path}: {e}", flush=True)

    summary = {
        "elapsed_sec": round(time.time() - t0, 1),
        "v1_rows": sum(r["rows"] for r in results if r["kind"] == "v1"),
        "v2_paragraphs": sum(r["rows"] for r in results if r["kind"] == "v2"),
        "v2_docs": sum(r.get("docs", 0) for r in results if r["kind"] == "v2"),
        "files_ok": len(results), "failed": failed,
    }
    with open(f"{config.EVENT_REPORT_DIR}/stage_a_summary.json", "w") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)
    print(json.dumps(summary, ensure_ascii=False), flush=True)
