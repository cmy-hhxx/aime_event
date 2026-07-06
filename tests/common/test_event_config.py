from src import config


def test_eventpack_paths_exist():
    assert config.EVENT_V1_DIR == "/mnt/ainvest_content/v3/v1"
    assert config.EVENT_INDEX_DIR.startswith(config.EVENT_OUT_ROOT)


def test_eventpack_thresholds():
    assert config.EVENT_INDEX_WORKERS <= 10  # ceph-fuse 红线
    assert config.EVENT_MIN_SIGNIFICANCE == 3
    assert config.EVENT_EARLY_MIN_ARTICLES <= config.EVENT_RECENT_MIN_ARTICLES
