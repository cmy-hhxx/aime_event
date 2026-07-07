from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path

DEFAULT_PAYLOAD_PART_BYTES = 512 * 1024 * 1024

# ============================================================
# 用户配置区 — 日常运行只改这里
# ============================================================

# --- 路径 ---
INPUT_DIR = "/mnt/ainvest_content/v1"  # 原始 NDJSON 输入目录
CLEANED_DIR = "/mnt/ainvest_content/v3/v1"  # 审计格式 canonical 输出
DUPLICATES_DIR = "output/duplicates"  # 重复记录输出（默认不写）
REJECTS_DIR = "output/rejects"  # 被拒绝的原始行（默认不写）
STATE_DIR = "/tmp/aime_event/v1/state"  # 运行中 SQLite 状态库，放本地盘减少 Ceph 随机 IO
PAYLOAD_DIR = "/tmp/aime_event/v1/state/payloads"  # 运行中 payload 分片，放本地盘减少 Ceph 随机 IO
FINAL_STATE_DIR = "/mnt/ainvest_content/v3/v1/state"  # 运行结束后保存的最终 state
REPORTS_DIR = "/mnt/ainvest_content/v3/v1/reports"  # 统计报表输出

# --- 运行时 ---
WORKERS = 4  # 并行进程数，建议 ≤ CPU 核数
CHUNK_SIZE = 20_000  # 每批送入 transform 的行数，越大内存占用越高
PART_SIZE = 200_000  # 每个输出 JSONL 分片的最大行数
PAYLOAD_PART_BYTES = DEFAULT_PAYLOAD_PART_BYTES  # 每个 payload 分片最大字节数（默认 512MB）
TARGET_SCALE_ROWS = 20_000_000  # 用于 summary 中 2000 万行存储估算
WRITE_AUX_OUTPUTS = False  # 默认只写 cleaned 输出；测试/审计需要时可打开辅助输出
LOG_EVERY_ROWS = 50_000  # 长任务每处理多少行打印一次进度
LOG_EVERY_SECONDS = 15  # 长任务至少每隔多少秒打印一次进度

# --- 可选相似合并（当前项目不用，默认关闭） ---
NEAR_DEDUP_ENABLED = False
NEAR_DEDUP_METHODS = ()
NEAR_NUM_PERM = 128  # MinHash 排列数，越大越精确但越慢
NEAR_SEED = 1  # MinHash 随机种子，固定以保证可复现
NEAR_SHINGLE_SIZE = 5  # 正文 shingle 长度（词元数）
NEAR_MIN_BODY_CHARS = 160  # 正文短于此长度不参与近似去重
NEAR_THRESHOLD = 0.92  # MinHash Jaccard 阈值，越高越保守（0~1）
NEAR_FUZZY_THRESHOLD = 96.0  # RapidFuzz 正文相似度阈值（0~100）
NEAR_TITLE_THRESHOLD = 90.0  # 标题相似度阈值（0~100）
NEAR_LONG_GAP_TITLE_THRESHOLD = 96.0  # 发布时间间隔较大时要求的标题相似度
NEAR_MAX_DAYS_BETWEEN = 14  # 发布时间最大间隔天数（超出需更高标题相似度）
NEAR_MAX_BUCKET_SIZE = 250  # LSH 桶最大记录数，防止桶爆炸
NEAR_MAX_CANDIDATE_PAIRS = 1_000_000  # 近似去重候选对上限
NEAR_MAX_REPORT_PAIRS = 10_000  # near_duplicates.jsonl 最多写入对数

# --- eventpack: 事件训练包流水线 (extract/complete 阶段) ---
EVENT_V1_DIR = "/mnt/ainvest_content/v3/v1"  # 清洗后新闻语料(仅精确去重)
EVENT_V2_DIR = "/mnt/ainvest_content/v3/v2"  # 研报/电话会段落
EVENT_OUT_ROOT = "/mnt/ainvest_content/v3/event_dataset"
EVENT_INDEX_DIR = f"{EVENT_OUT_ROOT}/index"
EVENT_CANDIDATE_DIR = f"{EVENT_OUT_ROOT}/candidates"
EVENT_SELECTED_DIR = f"{EVENT_OUT_ROOT}/selected"
EVENT_STRUCTURED_DIR = f"{EVENT_OUT_ROOT}/structured"
EVENT_MARKET_DIR = f"{EVENT_OUT_ROOT}/market"
EVENT_FINAL_DIR = f"{EVENT_OUT_ROOT}/final"
EVENT_REPORT_DIR = f"{EVENT_OUT_ROOT}/reports"
# ceph-fuse 并发红线: 实测 48 卡死 / 12 拥塞 / 6 正常, 禁超 10
EVENT_INDEX_WORKERS = 6
EVENT_TITLE_MAX_CHARS = 400
# 阈值抽取(无数量配额): 规则送审门 + LLM significance 门
EVENT_ERA_SPLIT = "2023-07-01"
EVENT_RECENT_MIN_ARTICLES = 5      # 近三年送审: n_articles>=5
EVENT_RECENT_ALT_MIN_ARTICLES = 3  # 或 n_articles>=3 且有研报佐证
EVENT_EARLY_MIN_ARTICLES = 2       # 早年送审: n_articles>=2
EVENT_MIN_SIGNIFICANCE = 3         # LLM 入选门
EVENT_PER_SYMBOL_CAP = 12          # 单 symbol 事件上限, 0=关闭
EVENT_FETCH_START = "2013-06-01"   # yfinance 拉取窗
EVENT_FETCH_END = "2026-07-01"

# ============================================================
# 以下由程序组装，一般不需要修改
# ============================================================


@dataclass(frozen=True)
class PathsConfig:
    input_dir: Path = field(default_factory=lambda: Path(INPUT_DIR))
    cleaned_dir: Path = field(default_factory=lambda: Path(CLEANED_DIR))
    duplicates_dir: Path = field(default_factory=lambda: Path(DUPLICATES_DIR))
    rejects_dir: Path = field(default_factory=lambda: Path(REJECTS_DIR))
    state_dir: Path = field(default_factory=lambda: Path(STATE_DIR))
    payload_dir: Path = field(default_factory=lambda: Path(PAYLOAD_DIR))
    final_state_dir: Path = field(default_factory=lambda: Path(FINAL_STATE_DIR))
    reports_dir: Path = field(default_factory=lambda: Path(REPORTS_DIR))


@dataclass(frozen=True)
class RuntimeConfig:
    workers: int = WORKERS
    chunk_size: int = CHUNK_SIZE
    part_size: int = PART_SIZE
    payload_part_bytes: int = PAYLOAD_PART_BYTES
    target_scale_rows: int = TARGET_SCALE_ROWS
    write_aux_outputs: bool = WRITE_AUX_OUTPUTS
    log_every_rows: int = LOG_EVERY_ROWS
    log_every_seconds: int = LOG_EVERY_SECONDS


@dataclass(frozen=True)
class NearDuplicateConfig:
    enabled: bool = NEAR_DEDUP_ENABLED
    dedup_methods: tuple[str, ...] = NEAR_DEDUP_METHODS
    num_perm: int = NEAR_NUM_PERM
    seed: int = NEAR_SEED
    shingle_size: int = NEAR_SHINGLE_SIZE
    min_body_chars: int = NEAR_MIN_BODY_CHARS
    threshold: float = NEAR_THRESHOLD
    fuzzy_threshold: float = NEAR_FUZZY_THRESHOLD
    title_threshold: float = NEAR_TITLE_THRESHOLD
    long_gap_title_threshold: float = NEAR_LONG_GAP_TITLE_THRESHOLD
    max_days_between: int = NEAR_MAX_DAYS_BETWEEN
    max_bucket_size: int = NEAR_MAX_BUCKET_SIZE
    max_candidate_pairs: int = NEAR_MAX_CANDIDATE_PAIRS
    max_report_pairs: int = NEAR_MAX_REPORT_PAIRS

    @property
    def band_size(self) -> int:
        return 4


@dataclass(frozen=True)
class PipelineConfig:
    paths: PathsConfig = field(default_factory=PathsConfig)
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    near_duplicates: NearDuplicateConfig = field(default_factory=NearDuplicateConfig)

    def with_paths(self, **updates: Path) -> PipelineConfig:
        return replace(self, paths=replace(self.paths, **updates))

    def with_runtime(self, **updates: bool | int) -> PipelineConfig:
        return replace(self, runtime=replace(self.runtime, **updates))

    def with_near_duplicates(self, **updates: bool | int | float | tuple[str, ...]) -> PipelineConfig:
        return replace(self, near_duplicates=replace(self.near_duplicates, **updates))


DEFAULT_CONFIG = PipelineConfig()


def validate_config(config: PipelineConfig) -> None:
    runtime = config.runtime
    near = config.near_duplicates
    if runtime.workers < 1:
        raise ValueError("WORKERS 必须 >= 1")
    if runtime.chunk_size < 1:
        raise ValueError("CHUNK_SIZE 必须 >= 1")
    if runtime.part_size < 1:
        raise ValueError("PART_SIZE 必须 >= 1")
    if not 0 < near.threshold <= 1:
        raise ValueError("NEAR_THRESHOLD 必须在 (0, 1] 范围内")
    if not 0 < near.fuzzy_threshold <= 100:
        raise ValueError("NEAR_FUZZY_THRESHOLD 必须在 (0, 100] 范围内")
