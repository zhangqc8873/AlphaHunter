from __future__ import annotations

import pandas as pd

from .data_fetch import get_symbol_hist_range


def top_percentile(df: pd.DataFrame, percentile: float = 90.0, column: str = "pct_chg") -> pd.DataFrame:
    if df.empty or column not in df.columns:
        return df
    threshold = df[column].quantile(percentile / 100.0)
    return df[df[column] >= threshold].copy().reset_index(drop=True)


def compute_volume_surge_ratio(code: str, date: str, lookback_days: int = 5) -> float | None:
    """计算成交量相对过去N日均值的放大倍数。"""
    # 拉取日期前后区间，保证包含目标日与之前若干日
    hist = get_symbol_hist_range(code, start_date=_offset_days(date, lookback_days + 1), end_date=date)
    if hist.empty or "volume" not in hist.columns:
        return None
    hist = hist.copy()
    hist = hist.sort_values(by="日期")
    # 目标日成交量
    today_row = hist[hist["日期"] == date]
    if today_row.empty:
        return None
    today_vol = float(today_row.iloc[0]["volume"]) if "volume" in today_row.columns else None
    if today_vol is None:
        return None
    # 过去N日均量（不含目标日）
    past = hist[hist["日期"] < date].tail(lookback_days)
    if past.empty:
        return None
    avg_vol = float(past["volume"].mean())
    if avg_vol == 0:
        return None
    return today_vol / avg_vol


def filter_volume_surge(df: pd.DataFrame, date: str, min_ratio: float = 2.0) -> pd.DataFrame:
    if df.empty or "代码" not in df.columns:
        return df
    ratios = []
    for code in df["代码"].astype(str).tolist():
        ratio = compute_volume_surge_ratio(code, date)
        ratios.append(ratio)
    out = df.copy()
    out["volume_surge_ratio"] = ratios
    return out[out["volume_surge_ratio"] >= min_ratio].copy().reset_index(drop=True)


def _offset_days(date: str, n: int) -> str:
    # 简单地向前偏移n天（自然日），不考虑节假日，足够覆盖数据
    import datetime as dt

    d = dt.datetime.strptime(date, "%Y%m%d")
    return (d - dt.timedelta(days=n)).strftime("%Y%m%d")


def compute_rsi(close: pd.Series, window: int = 14) -> pd.Series:
    delta = close.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    roll_up = up.rolling(window=window, min_periods=window).mean()
    roll_down = down.rolling(window=window, min_periods=window).mean()
    rs = roll_up / (roll_down + 1e-9)
    rsi = 100 - (100 / (1 + rs))
    return rsi


def compute_macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    signal_line = macd.ewm(span=signal, adjust=False).mean()
    hist = macd - signal_line
    return pd.DataFrame({"macd": macd, "signal": signal_line, "hist": hist})