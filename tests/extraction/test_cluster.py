from src.extraction import cluster


def test_tokens_stopwords():
    t = cluster.tokens("NVIDIA stock surges after the new AI chip report")
    assert "nvidia" in t and "ai" in t and "the" not in t and "stock" not in t


def _item(title, date):
    return {"title": title, "pub_date": date}


def test_cluster_bucket_same_story():
    items = [
        _item("Frasers Group launches takeover bid for Accent Group", "2026-06-15"),
        _item("Accent Group jumps on Frasers takeover bid", "2026-06-16"),
        _item("Fed holds interest rates steady in June meeting", "2026-06-16"),
    ]
    groups = sorted(cluster.cluster_bucket(items), key=len, reverse=True)
    assert len(groups) == 2 and sorted(groups[0]) == [0, 1]


def test_cluster_bucket_window_split():
    items = [
        _item("Acme Corp announces quarterly dividend increase", "2026-01-05"),
        _item("Acme Corp announces quarterly dividend increase", "2026-03-01"),
    ]
    assert len(cluster.cluster_bucket(items)) == 2  # 超出 3 天窗不连边
