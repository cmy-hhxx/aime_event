import json

import pytest

from src.common import llm


def test_parse_json_text_plain():
    assert llm.parse_json_text('{"a": 1}') == {"a": 1}


def test_parse_json_text_wrapped():
    assert llm.parse_json_text('前缀```json\n{"a": {"b": 2}}\n```后缀') == {"a": {"b": 2}}


def test_parse_json_text_invalid():
    with pytest.raises(ValueError):
        llm.parse_json_text("not json at all")


def test_run_checkpointed_resume(tmp_path):
    out = str(tmp_path / "ck.jsonl")
    items = [{"k": "a"}, {"k": "b"}, {"k": "c"}]
    calls = []

    def work(it):
        calls.append(it["k"])
        if it["k"] == "b":
            raise RuntimeError("boom")
        return {"v": it["k"].upper()}

    r1 = llm.run_checkpointed(items, lambda it: it["k"], work, out, workers=2)
    assert r1["a"]["v"] == "A" and "_error" in r1["b"]
    # 重跑: 已成功和已失败的都不再调 work(失败记录也落盘, 重试需删文件)
    calls.clear()
    r2 = llm.run_checkpointed(items, lambda it: it["k"], work, out, workers=2)
    assert calls == [] and len(r2) == 3
    with open(out) as fh:
        assert len(fh.readlines()) == 3
