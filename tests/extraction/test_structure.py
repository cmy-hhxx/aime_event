from src.extraction import structure


def test_clean_body_strips_html():
    s = structure.clean_body("<p>Hello <a href='x'>world</a> &amp; more</p>")
    assert s == "Hello world & more"


def test_clean_body_truncates():
    assert len(structure.clean_body("x" * 99999)) == structure.MAX_BODY_CHARS


def test_source_rank_prefers_wires():
    assert structure.source_rank("Reuters News") < structure.source_rank("some blog")
    assert structure.source_rank(None) == structure.source_rank("unknown src")
