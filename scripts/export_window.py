"""按时间窗口从 v1/v2 抽取一个自包含子集, 供本地开发用.

原理: 复用服务器上已建好的 parquet 索引(含 file/offset), 只对窗口内的记录做
随机 seek 读取原字节, 逐字写出 cleaned_batch*.jsonl 子集 —— 只读 ~窗口体量,
不全扫 155GB.

按文件分派到 --workers 个进程并发抽取: v2 是段落级小随机读, 单进程受 ceph-fuse
往返延迟压到 ~1.7MB/s; 并发把延迟藏起来, 6 并发约 10MB/s(6 是 ceph-fuse 实测
安全线, 禁超 10, 12 拥塞 / 48 卡死). 输出一文件一入参文件, 无写冲突.

下载到本地后, 把 EVENT_V1_DIR/EVENT_V2_DIR 指到子集目录, 重跑
`python -m src.main extract index` 即可重建正确 offset 的本地索引.

用法(服务器上, 用带 duckdb 的解释器):
    .venv/bin/python scripts/export_window.py \
        --index /mnt/ainvest_content/v3/event_dataset/index \
        --v1-src /mnt/ainvest_content/v3/v1 \
        --v2-src /mnt/ainvest_content/v3/v2 \
        --out   /mnt/ainvest_content/v3/export_2025-07-08 \
        --start 2025-07-08 --v2-notice material --workers 6
"""
from __future__ import annotations

import argparse
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

import duckdb

# material: 携带事件的公告类型 —— 重大事件(8-K/6-K)、财报(10-Q/10-K/20-F)、并购(425);
# 丢弃招股书/基金/代理声明等噪声(424B*/485*/497*/DEF/S-*/13F/Form 3,4,144 等)
MATERIAL_FORMS = ["8-K", "10-Q", "10-K", "6-K", "20-F", "425"]


def _v1_worker(task):
    """v1: 整行记录, seek(offset)+read(nbytes) 逐字写出."""
    src_dir, out_dir, file, rows = task  # rows: [(offset, nbytes), ...] 已按 offset 升序
    written = 0
    with open(f"{src_dir}/{file}", "rb", buffering=8 * 1024 * 1024) as fin, \
         open(f"{out_dir}/v1/{file}", "wb", buffering=8 * 1024 * 1024) as fout:
        for offset, nbytes in rows:
            fin.seek(offset)
            buf = fin.read(nbytes)
            fout.write(buf)
            written += len(buf)
    return file, len(rows), written


def _v2_worker(task):
    """v2: 段落级记录, seek(off)+readline() 逐段写出."""
    src_dir, out_dir, file, offs = task  # offs: [offset, ...] 已升序
    written = 0
    with open(f"{src_dir}/{file}", "rb", buffering=8 * 1024 * 1024) as fin, \
         open(f"{out_dir}/v2/{file}", "wb", buffering=8 * 1024 * 1024) as fout:
        for off in offs:
            fin.seek(off)
            line = fin.readline()
            fout.write(line)
            written += len(line)
    return file, len(offs), written


def _run_pool(kind, tasks, workers, worker_fn, n_units):
    """按文件并发抽取, 汇总进度. tasks 每项末位是该文件的记录/段落列表."""
    n_files = len(tasks)
    print(f"[{kind}] {n_files} 文件 / {n_units:,} 条, workers={workers}, 开始…", flush=True)
    done_files = done_units = written = 0
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(worker_fn, t) for t in tasks]
        for fut in as_completed(futs):
            _, cnt, wb = fut.result()
            done_files += 1
            done_units += cnt
            written += wb
            if done_files % 20 == 0 or done_files == n_files:
                print(f"[{kind}] 文件 {done_files}/{n_files}  {done_units:,}/{n_units:,} 条  "
                      f"{written/1e9:.2f} GB  ({time.time()-t0:.0f}s)", flush=True)
    print(f"[{kind}] 完成: {done_units:,} 条, {written/1e9:.2f} GB, 用时 {time.time()-t0:.0f}s", flush=True)
    return written


def export_v1(con, index_dir, src_dir, out_dir, start, end, workers):
    os.makedirs(f"{out_dir}/v1", exist_ok=True)
    where = f"pub_date >= '{start}'" + (f" AND pub_date < '{end}'" if end else "")
    rows = con.execute(f"""
        SELECT "file", "offset", nbytes
        FROM read_parquet('{index_dir}/v1_*.parquet')
        WHERE {where}""").fetchall()
    per_file: dict[str, list] = {}
    for file, offset, nbytes in rows:
        per_file.setdefault(file, []).append((offset, nbytes))
    tasks = [(src_dir, out_dir, f, sorted(v)) for f, v in per_file.items()]
    return _run_pool("v1", tasks, workers, _v1_worker, len(rows))


def export_v2(con, index_dir, src_dir, out_dir, start, end, notice_mode, workers):
    os.makedirs(f"{out_dir}/v2", exist_ok=True)
    date = f"pub_date >= '{start}'" + (f" AND pub_date < '{end}'" if end else "")
    if notice_mode == "all":
        notice = "source_type='notice'"
    elif notice_mode == "material":
        likes = " OR ".join(f"title LIKE '%Form {f}%'" for f in MATERIAL_FORMS)
        notice = f"(source_type='notice' AND ({likes}))"
    elif notice_mode == "8k":
        notice = "(source_type='notice' AND title LIKE '%Form 8-K%')"
    else:  # none
        notice = "1=0"
    src = f"(source_type IN ('report','teleconference') OR {notice})"
    rows = con.execute(f"""
        SELECT "file", offsets
        FROM read_parquet('{index_dir}/v2_*.parquet')
        WHERE {date} AND {src}""").fetchall()
    per_file: dict[str, list[int]] = {}
    for file, offsets in rows:
        per_file.setdefault(file, []).extend(int(o) for o in offsets.split(",") if o)
    tasks = [(src_dir, out_dir, f, sorted(v)) for f, v in per_file.items()]
    n_para = sum(len(v) for _, _, _, v in tasks)
    print(f"[v2] 窗口内 {len(rows):,} docs / {n_para:,} 段落 (notice={notice_mode})", flush=True)
    return _run_pool("v2", tasks, workers, _v2_worker, n_para)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--index", required=True)
    ap.add_argument("--v1-src", required=True)
    ap.add_argument("--v2-src", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--start", required=True, help="含, ISO 日期, 如 2025-07-08")
    ap.add_argument("--end", default="", help="不含, 留空=到最新")
    ap.add_argument("--v2-notice", choices=["none", "8k", "material", "all"], default="material",
                    help="v2 notice 取舍: none 全丢 / 8k 仅8-K / "
                         "material 财报+重大事件+并购(8-K/10-Q/10-K/6-K/20-F/425, 推荐) / all 全要(含招股书13F噪声)")
    ap.add_argument("--workers", type=int, default=6, help="并发进程数, ceph-fuse 安全线 6, 禁超 10")
    ap.add_argument("--skip-v1", action="store_true")
    ap.add_argument("--skip-v2", action="store_true")
    a = ap.parse_args()
    if a.workers > 10:
        raise SystemExit("workers 超过 ceph-fuse 安全线(10), 会拥塞甚至卡死挂载")

    con = duckdb.connect()
    con.execute("SET threads TO 8")
    total = 0
    if not a.skip_v1:
        total += export_v1(con, a.index, a.v1_src, a.out, a.start, a.end, a.workers)
    if not a.skip_v2:
        total += export_v2(con, a.index, a.v2_src, a.out, a.start, a.end, a.v2_notice, a.workers)
    print(f"\n=== 全部完成, 落盘 {total/1e9:.2f} GB -> {a.out} ===", flush=True)
    print("下一步: tar 到本地, 改 config 的 EVENT_V1_DIR/V2_DIR 指向本地子集, 重跑 extract index", flush=True)


if __name__ == "__main__":
    main()
