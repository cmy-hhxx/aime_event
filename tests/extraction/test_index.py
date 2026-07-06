from src.extraction import index


def test_norm_date_bounds():
    assert index.norm_date("2024-03-18T11:00:00Z") == "2024-03-18"
    assert index.norm_date("1970-01-01T00:00:00Z") == ""   # 脏时间戳
    assert index.norm_date("2027-06-29T00:00:00Z") == ""   # 未来脏时间戳
    assert index.norm_date(None) == ""


def test_as_dict_variants():
    assert index._as_dict({"a": 1}) == {"a": 1}
    assert index._as_dict("{'a': 1}") == {"a": 1}   # v2 字符串化 dict
    assert index._as_dict("broken{") == {}
    assert index._as_dict(None) == {}


def test_v1_extract_notice():
    rec = {"id": "x1", "content_type": "US_NOTICE", "title": "Form 6-K",
           "published_at": "2014-06-03T11:13:14Z",
           "notice": {"filing_type": "009", "declare_date": "2014-05-19"},
           "dedup": {"key": "notice:000119"}}
    row = index.v1_extract(rec)
    assert row["accession"] == "000119" and row["pub_date"] == "2014-06-03"
    assert row["symbols"] == "" and row["body_len"] == 0
