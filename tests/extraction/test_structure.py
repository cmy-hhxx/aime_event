from src.extraction import structure


def test_clean_body_strips_html():
    s = structure.clean_body("<p>Hello <a href='x'>world</a> &amp; more</p>")
    assert s == "Hello world & more"


def test_clean_body_truncates():
    assert len(structure.clean_body("x" * 99999)) == structure.MAX_BODY_CHARS


def test_source_rank_prefers_wires():
    assert structure.source_rank("Reuters News") < structure.source_rank("some blog")
    assert structure.source_rank(None) == structure.source_rank("unknown src")


def test_filter_by_peak():
    from src.extraction.structure import filter_by_peak
    events = [{"event_id": "A", "peak_date": "2026-05-29"},
              {"event_id": "B", "peak_date": "2026-05-28"}]
    assert filter_by_peak(events, None) == events
    assert [e["event_id"] for e in filter_by_peak(events, "2026-05-29")] == ["A"]


def test_eightk_member_builds_pseudo_article():
    ev = {"event_id": "EVT8K_1", "peak_date": "2026-05-29",
          "_8k": {"trace_id": "n1", "event_title": "Big deal announced.",
                  "item_code": "1.01", "summary": "<p>Body&nbsp;  text</p>"}}
    m = structure.eightk_member(ev)
    assert m["_body"] == "Body text"
    assert m["content_type"] == "US_NOTICE" and m["source_name"] == "SEC EDGAR 8-K"
    assert m["title"] == "Big deal announced." and m["id"] == "n1"
    assert m["pub_date"] == "2026-05-29"


def test_eightk_member_title_falls_back_to_item():
    ev = {"event_id": "EVT8K_1", "peak_date": "2026-05-29",
          "_8k": {"trace_id": "n1", "event_title": None, "item_code": "5.02", "summary": "x"}}
    assert structure.eightk_member(ev)["title"] == "Form 8-K Item 5.02"


def test_load_selected_merges_8k(tmp_path, monkeypatch):
    monkeypatch.setattr(structure.config, "EVENT_SELECTED_DIR", str(tmp_path))
    (tmp_path / "selected_events.jsonl").write_text('{"event_id": "EVT_1"}\n')
    (tmp_path / "selected_8k.jsonl").write_text('{"event_id": "EVT8K_1"}\n')
    assert {e["event_id"] for e in structure.load_selected()} == {"EVT_1", "EVT8K_1"}


def test_load_selected_without_8k_file(tmp_path, monkeypatch):
    monkeypatch.setattr(structure.config, "EVENT_SELECTED_DIR", str(tmp_path))
    (tmp_path / "selected_events.jsonl").write_text('{"event_id": "EVT_1"}\n')
    assert [e["event_id"] for e in structure.load_selected()] == ["EVT_1"]
