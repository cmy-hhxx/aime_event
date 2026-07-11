"""从 US_NOTICE.jsonl 抽取 8-K 当期报告事件, 支持 --date / --day N / --month N 限定时间窗.

8-K 判定: notice.attachments 里存在 file_type 精确等于 "8-K" 的附件(排除 8-K/A 修正案).
时间窗锚定文件内 8-K 的最大自然日 D(按顶层 published_at):
    --date YYYY-MM-DD  只取该自然日(不扫全文件找 D, 与流水线 --date 口径一致)
    --day N    最近 N 个自然日 [D-(N-1)天, D];  默认(无参)等价 --day 1
    --month N  最近 N 个自然月(按 YYYY-MM);      month 1 = D 所在整月

每条输出是一条**精简事件行**(纯机械投影, 无 LLM), 字段见 project():
    trace_id / event_title / cik / accession / event_date / item_code /
    summary / body_source / source_url
其中 event_date 取 SEC 申报日 notice.declare_date(非 AInvest 发布时刻 published_at).

v2 关联(notice.jsonl, 段落级 SEC 原文): 按 accession 关联窗口内**全部** 8-K, 段落按
paragraph_index 升序拼接为 SEC 全文, 用于 (a) 给缺 body 的记录补 summary, (b) 从 Item
行抽 item_code / 给 v2 记录取 event_title. native 记录 summary 用自带 body, item_code
仍从 v2 全文抽(方案 B). --no-backfill 则完全不扫 v2(缺 body 记 none, native item_code 空).

输出:
    notice_8k/US_NOTICE.8k.<window>.jsonl   精简事件行(见 project())
    reports/notice_8k_<window>.json         窗口 / 计数 / 耗时

用法:
    python -m src.main extract notice8k                  # 最新一天
    python -m src.main extract notice8k --date 2026-05-29  # 指定自然日
    python -m src.main extract notice8k --day 7          # 最近 7 天
    python -m src.main extract notice8k --month 1        # 最新一个自然月
    python -m src.main extract notice8k --no-backfill --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Callable

import orjson

from src import config

# SEC accession 去横杠后为 18 位数字(10+2+6), 内嵌于 notice source.url 路径段
ACCESSION_RE = re.compile(r"(\d{18})")
# EDGAR 附件 url 路径段 edgar/data/<CIK>/ 中的 SEC 公司号
CIK_RE = re.compile(r"edgar/data/(\d+)/")
# 8-K 正文中的条目行 "Item X.YY <标题>"; 标题截到句号/换行/常见正文引导词前
ITEM_RE = re.compile(
    r"\bItem\s+(\d\.\d{2})\.?\s+"
    r"([A-Z][A-Za-z0-9 ,;’'\-/&()]+?)"
    r"(?=\s+(?:On\b|The\b|As\b|In\b|Pursuant\b|\d{4})|[.\n]|$)"
)


def is_8k(rec: dict) -> bool:
    """附件 file_type 精确等于 8-K(排除 8-K/A 等修正案)."""
    attachments = (rec.get("notice") or {}).get("attachments") or []
    return any(a.get("file_type") == "8-K" for a in attachments)


def day_of(rec: dict) -> str:
    """顶层 published_at 截到自然日(YYYY-MM-DD); 缺失/异常返回空串."""
    value = rec.get("published_at")
    return value[:10] if isinstance(value, str) else ""


def accession_of(rec: dict) -> str:
    """8-K 记录的去横杠 accession: 优先 dedup.debug.notice_accession, 退回 dedup.key."""
    dedup = rec.get("dedup") or {}
    acc = (dedup.get("debug") or {}).get("notice_accession")
    if acc:
        return acc
    key = dedup.get("key") or ""
    return key[len("notice:"):] if key.startswith("notice:") else ""


def accession_from_url(url: str | None) -> str:
    """从 notice 段落 source.url 抽第一个 18 位去横杠 accession; 无则空串."""
    if not isinstance(url, str):
        return ""
    m = ACCESSION_RE.search(url)
    return m.group(1) if m else ""


def eightk_url(rec: dict) -> str:
    """窗口内 8-K 附件的 EDGAR url(file_type 精确等于 8-K); 无则空串."""
    for a in (rec.get("notice") or {}).get("attachments") or []:
        if a.get("file_type") == "8-K":
            return a.get("url") or ""
    return ""


def cik_from_url(url: str) -> str:
    """从 EDGAR url 路径段 edgar/data/<CIK>/ 抽 SEC 公司号; 无则空串."""
    m = CIK_RE.search(url or "")
    return m.group(1) if m else ""


def declare_date_of(rec: dict) -> str:
    """SEC 申报日 notice.declare_date(YYYY-MM-DD); 缺失退回 published_at 截日."""
    d = (rec.get("notice") or {}).get("declare_date")
    return d if isinstance(d, str) and d else day_of(rec)


def first_sentence(text: str) -> str:
    """取正文首句(按 .!? + 空白切分); 空串返回空串."""
    if not text:
        return ""
    parts = re.split(r"(?<=[.!?])\s", text.strip(), maxsplit=1)
    return parts[0].strip()


def extract_item(text: str) -> tuple[str, str]:
    """从 SEC 8-K 全文抽首个 (item_code, item_title); 无则 ('', '')."""
    if not text:
        return "", ""
    m = ITEM_RE.search(text)
    if not m:
        return "", ""
    return m.group(1), re.sub(r"\s+", " ", m.group(2)).strip(" ;,.")


def project(rec: dict, *, summary: str, body_source: str, sec_text: str) -> dict:
    """把一条 8-K 原始记录投影为精简事件行.

    summary/body_source 由调用方按 native/v2_notice/none 判定; sec_text 是该 accession
    的 SEC 全文(native 也从 v2 取, 供 item_code 抽取), none 时为空串.
    event_title: native 取自带 body 首句; v2 取 Item 标题(无则回退 SEC 全文首句).
    """
    item_code, item_title = extract_item(sec_text)
    if body_source == "native":
        event_title = first_sentence(summary)
    elif body_source == "v2_notice":
        event_title = item_title or first_sentence(summary)
    else:
        event_title = ""
    return {
        "trace_id": rec.get("id"),
        "event_title": event_title or None,
        "cik": cik_from_url(eightk_url(rec)) or None,
        "accession": accession_of(rec) or None,
        "event_date": declare_date_of(rec) or None,
        "item_code": item_code or None,
        "summary": summary or None,
        "body_source": body_source,
        "source_url": eightk_url(rec) or None,
    }


def _source_url(rec: dict) -> str:
    """v2 段落 source 可能是 dict 或字符串化 dict, 取其中的 url."""
    src = rec.get("source")
    if isinstance(src, dict):
        return src.get("url") or ""
    if isinstance(src, str):
        m = re.search(r"https?://[^\s'\"}]+", src)
        return m.group(0) if m else ""
    return ""


def _iter_records(path: Path):
    """逐行流式解析 jsonl, 跳过空行与坏行."""
    with path.open("rb", buffering=8 * 1024 * 1024) as fh:
        for line in fh:
            if not line.strip():
                continue
            try:
                yield orjson.loads(line)
            except orjson.JSONDecodeError:
                continue


def find_max_8k_day(src: Path) -> str:
    """流式扫全文件, 返回 8-K 记录的最大自然日; 无 8-K 返回空串."""
    latest = ""
    for rec in _iter_records(src):
        if not is_8k(rec):
            continue
        day = day_of(rec)
        if day > latest:
            latest = day
    return latest


def _shift_month(year: int, month: int, back: int) -> tuple[int, int]:
    """把 (year, month) 往前推 back 个月."""
    idx = year * 12 + (month - 1) - back
    return idx // 12, idx % 12 + 1


def make_window(D: str, *, day: int | None, month: int | None) -> tuple[Callable[[str], bool], str]:
    """由锚点日 D 与 --day/--month 生成 (窗口判定函数, 文件名标签).

    day/month 互斥; 都为 None 时等价 day=1.
    """
    if day is not None and month is not None:
        raise ValueError("--day 与 --month 互斥")
    end = date.fromisoformat(D)

    if month is not None:
        months = {
            "%04d-%02d" % _shift_month(end.year, end.month, b) for b in range(month)
        }
        start_y, start_m = _shift_month(end.year, end.month, month - 1)
        start_label = "%04d-%02d" % (start_y, start_m)
        end_label = "%04d-%02d" % (end.year, end.month)
        label = end_label if month == 1 else f"{start_label}_{end_label}"
        return (lambda d: d[:7] in months), label

    n = 1 if day is None else day
    start = end - timedelta(days=n - 1)
    start_s, end_s = start.isoformat(), end.isoformat()
    label = end_s if n == 1 else f"{start_s}_{end_s}"
    return (lambda d: start_s <= d <= end_s), label


def collect_8k(src: Path, in_window: Callable[[str], bool]) -> tuple[list[dict], set[str]]:
    """流式扫 US_NOTICE, 收集窗口内 8-K 记录及其**全部** accession 集合.

    方案 B: 返回窗口内所有 8-K 的 accession(不止缺 body 的), 供后续一次性从 v2 取 SEC
    全文——既给缺 body 的补 summary, 也给 native 记录抽 item_code.
    """
    records: list[dict] = []
    accessions: set[str] = set()
    for rec in _iter_records(src):
        if not is_8k(rec) or not in_window(day_of(rec)):
            continue
        records.append(rec)
        acc = accession_of(rec)
        if acc:
            accessions.add(acc)
    return records, accessions


def backfill_bodies(v2: Path, needed: set[str]) -> dict[str, str]:
    """流式扫 notice.jsonl, 对命中 needed 的段落按 paragraph_index 升序拼接为原文."""
    if not needed:
        return {}
    paragraphs: dict[str, list[tuple[int, str]]] = {}
    for rec in _iter_records(v2):
        acc = accession_from_url(_source_url(rec))
        if acc not in needed:
            continue
        text = rec.get("text") or ""
        idx = rec.get("paragraph_index")
        idx = idx if isinstance(idx, int) else 0
        paragraphs.setdefault(acc, []).append((idx, text))
    bodies: dict[str, str] = {}
    for acc, parts in paragraphs.items():
        parts.sort(key=lambda p: p[0])
        bodies[acc] = "\n".join(t for _, t in parts if t)
    return bodies


def run(args: argparse.Namespace) -> None:
    src = Path(args.src or config.EVENT_NOTICE_SRC)
    if not src.is_file():
        raise SystemExit(f"源文件不存在: {src}")

    date = getattr(args, "date", None)
    if date:
        in_window, label, D = (lambda d: d == date), date, date
        print(f"[notice8k] 指定日窗口={label}", flush=True)
    else:
        D = find_max_8k_day(src)
        if not D:
            raise SystemExit(f"未在 {src} 中找到任何 8-K 记录")
        in_window, label = make_window(D, day=args.day, month=args.month)
        print(f"[notice8k] 最大 8-K 日 D={D}, 窗口={label}", flush=True)

    records, accessions = collect_8k(src, in_window)
    native_cnt = sum(1 for r in records if r.get("body"))
    print(f"[notice8k] 窗口内 8-K {len(records):,} 条, 自带 body {native_cnt:,} 条", flush=True)

    if args.dry_run:
        return

    # 方案 B: 对窗口内全部 accession 扫一遍 v2, 取 SEC 全文(补 summary + 抽 item_code)
    sec_text: dict[str, str] = {}
    if not args.no_backfill and accessions:
        v2 = Path(args.v2 or config.EVENT_NOTICE_V2)
        if not v2.is_file():
            raise SystemExit(f"补全需要 notice.jsonl, 但不存在: {v2}(可加 --no-backfill 跳过)")
        print(f"[notice8k] 扫 {v2} 取 SEC 全文…", flush=True)
        sec_text = backfill_bodies(v2, accessions)

    counts = {"native": 0, "v2_notice": 0, "none": 0}
    out = Path(args.out) if args.out else Path(config.EVENT_NOTICE_8K_DIR) / f"US_NOTICE.8k.{label}.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("wb") as fout:
        for rec in records:
            sec = sec_text.get(accession_of(rec)) or ""
            native_body = rec.get("body")
            if native_body:
                summary, src_tag = native_body, "native"
            elif sec:
                summary, src_tag = sec, "v2_notice"
            else:
                summary, src_tag = "", "none"
            counts[src_tag] += 1
            row = project(rec, summary=summary, body_source=src_tag, sec_text=sec)
            fout.write(orjson.dumps(row) + b"\n")

    report_dir = Path(config.EVENT_REPORT_DIR)
    report_dir.mkdir(parents=True, exist_ok=True)
    report = {
        "window": label, "max_8k_day": D, "src": str(src),
        "total": len(records), **counts, "out": str(out),
    }
    (report_dir / f"notice_8k_{label}.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2))
    print(f"完成: {len(records):,} 条 -> {out}", flush=True)
    print(json.dumps(report, ensure_ascii=False), flush=True)
