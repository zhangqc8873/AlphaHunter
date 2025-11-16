from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    cache_dir: Path = Path(".cache")
    output_dir: Path = Path("outputs")

    # 筛选逻辑参数
    top_percent: float = 10.0  # 涨幅前10%
    volume_surge_ratio: float = 2.0  # 成交量相对5日均值的放大倍数

    # 技术指标窗口
    rsi_window: int = 14
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9

    # 数据获取控制
    max_symbols_for_hist: int | None = None  # None表示全部A股
    per_request_sleep_sec: float = 0.2

    # 指标过滤配置
    enable_indicator_filter: bool = True
    indicator_lookback_days: int = 60
    indicator_rsi_min: float = 50.0
    indicator_macd_hist_min: float = 0.0
    max_symbols_indicator_check: int = 50


DEFAULT_CONFIG = Config()