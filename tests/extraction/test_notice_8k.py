import argparse
import json

import orjson
import pytest

from src.extraction import notice_8k


# --- 8-K 判定 ---

def _rec(file_type, *, id="x", body=None, pub="2026-06-29T18:05:25Z",
         acc="000119312526286273", declare="2026-06-29", cik="1590750"):
    url = f"https://www.sec.gov:443/Archives/edgar/data/{cik}/{acc}/d8k.htm"
    return {
        "id": id,
        "title": f"Form {file_type}",
        "published_at": pub,
        "body": body,
        "notice": {"declare_date": declare,
                   "attachments": [{"url": url, "file_type": file_type}]},
        "dedup": {"key": f"notice:{acc}", "debug": {"notice_accession": acc}},
    }


def test_is_8k_exact_only():
    assert notice_8k.is_8k(_rec("8-K")) is True
    assert notice_8k.is_8k(_rec("8-K/A")) is False   # 修正案排除
    assert notice_8k.is_8k(_rec("424B2")) is False
    assert notice_8k.is_8k({"notice": {"attachments": []}}) is False
    assert notice_8k.is_8k({}) is False


def test_day_of():
    assert notice_8k.day_of(_rec("8-K", pub="2026-06-29T18:05:25Z")) == "2026-06-29"
    assert notice_8k.day_of({}) == ""


def test_accession_of():
    assert notice_8k.accession_of(_rec("8-K", acc="000119312526286273")) == "000119312526286273"
    # 无 debug.notice_accession 时退回 dedup.key
    rec = {"dedup": {"key": "notice:000073359025000034"}}
    assert notice_8k.accession_of(rec) == "000073359025000034"
    assert notice_8k.accession_of({}) == ""


# --- 精简行投影字段 ---

def test_eightk_url_and_cik():
    rec = _rec("8-K", acc="000119312526286273", cik="1590750")
    url = notice_8k.eightk_url(rec)
    assert "edgar/data/1590750/000119312526286273" in url
    assert notice_8k.cik_from_url(url) == "1590750"
    # 附件里没有精确 8-K 时返回空
    rec2 = _rec("8-K/A")
    assert notice_8k.eightk_url(rec2) == ""
    assert notice_8k.cik_from_url("http://x/no-cik") == ""


def test_declare_date_of():
    assert notice_8k.declare_date_of(_rec("8-K", declare="2026-06-28")) == "2026-06-28"
    # 缺 declare_date 退回 published_at 截日
    rec = _rec("8-K", pub="2026-06-29T18:00:00Z")
    del rec["notice"]["declare_date"]
    assert notice_8k.declare_date_of(rec) == "2026-06-29"


def test_first_sentence():
    assert notice_8k.first_sentence("Foo did X. Then Y.") == "Foo did X."
    assert notice_8k.first_sentence("No terminator here") == "No terminator here"
    assert notice_8k.first_sentence("") == ""


def test_extract_item():
    assert notice_8k.extract_item(
        "Item 5.02. Departure of Directors or Certain Officers On June 29, 2026 ..."
    ) == ("5.02", "Departure of Directors or Certain Officers")
    assert notice_8k.extract_item("Item 8.01 Other Events\nThe company...") == ("8.01", "Other Events")
    assert notice_8k.extract_item("no item line at all") == ("", "")


def test_project_native_vs_v2():
    # native: event_title 取自带 body 首句; item_code 由 sec_text(方案 B)抽
    rec = _rec("8-K", id="n", body="Acme announced a merger. Details follow.", cik="123")
    row = notice_8k.project(rec, summary=rec["body"], body_source="native",
                            sec_text="Item 1.01 Entry into a Material Definitive Agreement On ...")
    assert row["trace_id"] == "n"
    assert row["event_title"] == "Acme announced a merger."
    assert row["cik"] == "123"
    assert row["item_code"] == "1.01"
    assert row["summary"] == "Acme announced a merger. Details follow."
    assert row["body_source"] == "native"
    assert "edgar/data/123" in row["source_url"]
    # v2: event_title 取 Item 标题
    sec = "Item 3.02 Unregistered Sales of Equity Securities On June 29 ..."
    row2 = notice_8k.project(rec, summary=sec, body_source="v2_notice", sec_text=sec)
    assert row2["item_code"] == "3.02"
    assert row2["event_title"] == "Unregistered Sales of Equity Securities"
    # none: 无 summary / 无 sec_text
    row3 = notice_8k.project(rec, summary="", body_source="none", sec_text="")
    assert row3["summary"] is None and row3["item_code"] is None and row3["event_title"] is None


def test_accession_from_url():
    url = "https://www.sec.gov:443/Archives/edgar/data/2016072/000121390025088672/ea0257823-425.htm"
    assert notice_8k.accession_from_url(url) == "000121390025088672"
    assert notice_8k.accession_from_url("http://x/no-accession.htm") == ""
    assert notice_8k.accession_from_url(None) == ""


# --- 时间窗 ---

def test_window_day():
    pred, label = notice_8k.make_window("2026-06-29", day=1, month=None)
    assert pred("2026-06-29") and not pred("2026-06-28")
    assert label == "2026-06-29"

    pred, label = notice_8k.make_window("2026-06-29", day=7, month=None)
    assert pred("2026-06-29") and pred("2026-06-23") and not pred("2026-06-22")
    assert label == "2026-06-23_2026-06-29"


def test_window_month():
    pred, label = notice_8k.make_window("2026-06-29", day=None, month=1)
    assert pred("2026-06-01") and pred("2026-06-30") and not pred("2026-05-31")
    assert label == "2026-06"

    pred, label = notice_8k.make_window("2026-06-29", day=None, month=2)
    assert pred("2026-05-01") and pred("2026-06-30") and not pred("2026-04-30")
    assert label == "2026-05_2026-06"


def test_window_default_is_day_1():
    pred, label = notice_8k.make_window("2026-06-29", day=None, month=None)
    assert pred("2026-06-29") and not pred("2026-06-28")
    assert label == "2026-06-29"


# --- 流式扫描 ---

def _write_jsonl(path, records):
    with path.open("wb") as fh:
        for r in records:
            fh.write(orjson.dumps(r) + b"\n")


def test_find_max_8k_day(tmp_path):
    src = tmp_path / "US_NOTICE.jsonl"
    _write_jsonl(src, [
        _rec("8-K", pub="2026-06-27T00:00:00Z"),
        _rec("8-K", pub="2026-06-29T00:00:00Z"),
        _rec("424B2", pub="2026-06-30T00:00:00Z"),   # 非 8-K 不计入
        _rec("8-K/A", pub="2026-07-01T00:00:00Z"),   # 修正案不计入
    ])
    assert notice_8k.find_max_8k_day(src) == "2026-06-29"


def test_backfill_bodies_orders_by_paragraph_index(tmp_path):
    v2 = tmp_path / "notice.jsonl"
    acc = "000119312526286273"
    url = f"https://www.sec.gov:443/Archives/edgar/data/1/{acc}/d8k.htm"
    # 段落乱序写入, 期望按 paragraph_index 升序拼接
    _write_jsonl(v2, [
        {"source": {"url": url}, "paragraph_index": 2, "text": "world"},
        {"source": {"url": url}, "paragraph_index": 1, "text": "hello"},
        {"source": {"url": "http://x/000000000000000000/o.htm"}, "paragraph_index": 1, "text": "other"},
    ])
    bodies = notice_8k.backfill_bodies(v2, {acc})
    assert bodies[acc] == "hello\nworld"
    assert "000000000000000000" not in bodies


# --- 端到端 run ---

def _setup_run(tmp_path, monkeypatch):
    monkeypatch.setattr(notice_8k.config, "EVENT_NOTICE_8K_DIR", str(tmp_path / "notice_8k"))
    monkeypatch.setattr(notice_8k.config, "EVENT_REPORT_DIR", str(tmp_path / "reports"))


def _args(**kw):
    base = dict(day=None, month=None, src=None, v2=None, out=None,
               no_backfill=False, dry_run=False)
    base.update(kw)
    return argparse.Namespace(**base)


def test_run_backfills_and_counts(tmp_path, monkeypatch):
    _setup_run(tmp_path, monkeypatch)
    src = tmp_path / "US_NOTICE.jsonl"
    v2 = tmp_path / "notice.jsonl"
    acc_native, acc_backfill, acc_none = "111111111111111111", "222222222222222222", "333333333333333333"
    _write_jsonl(src, [
        _rec("8-K", id="n", body="Native summary sentence. More.", acc=acc_native, cik="10"),
        _rec("8-K", id="b", body=None, acc=acc_backfill, cik="20"),
        _rec("8-K", id="z", body=None, acc=acc_none, cik="30"),
        _rec("424B2", id="q", body=None, acc="999999999999999999"),   # 非 8-K, 应被过滤
    ])
    url = f"https://sec.gov/Archives/edgar/data/20/{acc_backfill}/d8k.htm"
    _write_jsonl(v2, [
        {"source": {"url": url}, "paragraph_index": 1,
         "text": "Item 5.02 Departure of Directors or Certain Officers On June 29, 2026 ..."},
    ])

    notice_8k.run(_args(src=str(src), v2=str(v2)))

    out = tmp_path / "notice_8k" / "US_NOTICE.8k.2026-06-29.jsonl"
    rows = [json.loads(l) for l in out.read_text().splitlines()]
    assert len(rows) == 3   # 3 条 8-K, 424B2 被过滤
    by_acc = {r["accession"]: r for r in rows}

    native = by_acc[acc_native]
    assert native["body_source"] == "native" and native["trace_id"] == "n"
    assert native["summary"] == "Native summary sentence. More."
    assert native["event_title"] == "Native summary sentence."
    assert native["cik"] == "10" and native["event_date"] == "2026-06-29"
    assert native["item_code"] is None   # native body 无 Item 且 v2 无此 accession

    bf = by_acc[acc_backfill]
    assert bf["body_source"] == "v2_notice"
    assert bf["item_code"] == "5.02"
    assert bf["event_title"] == "Departure of Directors or Certain Officers"
    assert bf["summary"].startswith("Item 5.02")

    none = by_acc[acc_none]
    assert none["body_source"] == "none"
    assert none["summary"] is None and none["item_code"] is None and none["event_title"] is None

    report = json.loads((tmp_path / "reports" / "notice_8k_2026-06-29.json").read_text())
    assert report["total"] == 3
    assert report["native"] == 1 and report["v2_notice"] == 1 and report["none"] == 1


def test_run_no_backfill_skips_v2(tmp_path, monkeypatch):
    _setup_run(tmp_path, monkeypatch)
    src = tmp_path / "US_NOTICE.jsonl"
    _write_jsonl(src, [_rec("8-K", body=None, acc="222222222222222222")])
    # 不提供 v2, --no-backfill 下也不应报错
    notice_8k.run(_args(src=str(src), v2=None, no_backfill=True))
    out = tmp_path / "notice_8k" / "US_NOTICE.8k.2026-06-29.jsonl"
    rows = [json.loads(l) for l in out.read_text().splitlines()]
    assert len(rows) == 1 and rows[0]["body_source"] == "none"
    assert rows[0]["summary"] is None and rows[0]["item_code"] is None


# --- --date 窗口 ---

def test_run_date_scoped(tmp_path, monkeypatch):
    _setup_run(tmp_path, monkeypatch)
    src = tmp_path / "US_NOTICE.jsonl"
    _write_jsonl(src, [
        _rec("8-K", id="a", body="Day one event.", acc="111111111111111111",
             pub="2026-05-29T10:00:00Z", declare="2026-05-29"),
        _rec("8-K", id="b", body="Other day.", acc="222222222222222222",
             pub="2026-06-29T10:00:00Z"),
    ])
    notice_8k.run(_args(src=str(src), no_backfill=True, date="2026-05-29"))
    out = tmp_path / "notice_8k" / "US_NOTICE.8k.2026-05-29.jsonl"
    rows = [json.loads(l) for l in out.read_text().splitlines()]
    assert [r["trace_id"] for r in rows] == ["a"]


# --- notice8k-select 桥接 ---

def _triage_ok(sig=4, syms=("AAA",), etype="ma_deal"):
    return {"is_valid_event": True, "significance": sig, "event_type": etype,
            "event_family": "fam", "event_subject": "subj",
            "primary_symbols": list(syms), "event_date": "2026-05-29", "title_cn": "标题"}


def test_notice8k_final_select_gates_and_dedup():
    from src.extraction import notice_8k_select
    accs = [f"{i:018d}" for i in (1, 2, 3, 4)]
    rows = [{"_event_id": f"EVT8K_{a}", "accession": a, "event_date": "2026-05-29",
             "trace_id": f"t{a[-1]}", "cik": "10", "item_code": "1.01",
             "source_url": "u", "body_source": "native", "event_title": "T",
             "summary": "S" * 20000} for a in accs]
    triage = {
        f"EVT8K_{accs[0]}": _triage_ok(sig=4, syms=("AAA",)),
        f"EVT8K_{accs[1]}": {"is_valid_event": False, "reject_reason": "例行"},
        f"EVT8K_{accs[2]}": _triage_ok(sig=2, syms=("CCC",)),        # 分不够
        f"EVT8K_{accs[3]}": _triage_ok(sig=3, syms=("AAA",)),        # 同(类型,日期,主体), 分低被去重
    }
    picked = notice_8k_select.final_select(rows, triage, "2026-05-29", 3)
    assert [p["event_id"] for p in picked] == [f"EVT8K_{accs[0]}"]
    p = picked[0]
    assert p["peak_date"] == "2026-05-29" and p["significance"] == 4
    assert p["n_articles"] == 1 and p["is_recent"] is True
    assert p["_8k"]["accession"] == accs[0]
    assert len(p["_8k"]["summary"]) == notice_8k_select.MAX_KEEP_CHARS  # 正文截断


def test_notice8k_final_select_bad_llm_date_falls_back():
    from src.extraction import notice_8k_select
    acc = "111111111111111111"
    rows = [{"_event_id": f"EVT8K_{acc}", "accession": acc, "event_date": "2026-05-28",
             "trace_id": "t", "cik": "1", "item_code": None, "source_url": None,
             "body_source": "native", "event_title": "T", "summary": "S"}]
    t = _triage_ok(); t["event_date"] = "2199-01-01"  # 越界日期回退申报日
    picked = notice_8k_select.final_select(rows, {f"EVT8K_{acc}": t}, "2026-05-29", 3)
    assert picked[0]["event_date"] == "2026-05-28"


# --- 新闻侧对 8-K 的跨源去重 (select.dedup_vs_8k) ---

def test_news_dedup_vs_8k(tmp_path, monkeypatch):
    from src.extraction import select
    monkeypatch.setattr(select.config, "EVENT_SELECTED_DIR", str(tmp_path))
    with open(tmp_path / "selected_8k.jsonl", "w") as fh:
        fh.write(json.dumps({"primary_symbols": ["BBY"], "event_date": "2026-05-28",
                             "event_type": "earnings_release"}) + "\n")
    picked = [
        # 同 symbol 同日 -> 去重(即使类型漂移)
        {"primary_symbols": ["BBY"], "event_date": "2026-05-28", "event_type": "other"},
        # 同 symbol 同类型 T+1 -> 去重(8-K 盘后发、新闻次日铺开)
        {"primary_symbols": ["BBY"], "event_date": "2026-05-29", "event_type": "earnings_release"},
        # 同 symbol 不同类型不同日 -> 保留(衍生新事件, 如次日分析师评级)
        {"primary_symbols": ["BBY"], "event_date": "2026-05-29", "event_type": "guidance_change"},
        # 无关 symbol -> 保留
        {"primary_symbols": ["DLTR"], "event_date": "2026-05-28", "event_type": "earnings_release"},
    ]
    kept, dropped = select.dedup_vs_8k(picked)
    assert dropped == 2
    assert [(k["primary_symbols"][0], k["event_type"]) for k in kept] == [
        ("BBY", "guidance_change"), ("DLTR", "earnings_release")]


def test_news_dedup_vs_8k_no_file(tmp_path, monkeypatch):
    from src.extraction import select
    monkeypatch.setattr(select.config, "EVENT_SELECTED_DIR", str(tmp_path / "nope"))
    picked = [{"primary_symbols": ["AAA"], "event_date": "2026-05-29", "event_type": "other"}]
    kept, dropped = select.dedup_vs_8k(picked)
    assert kept == picked and dropped == 0
