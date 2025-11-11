from __future__ import annotations

import time
from typing import Optional, List

import pandas as pd

try:
    import akshare as ak
except Exception as e:  # pragma: no cover
    raise RuntimeError("akshare 未安装或导入失败，请先安装 akshare") from e

from .config import DEFAULT_CONFIG
from .cache import cacheable_df


def _prefix_for_code(code: str) -> str:
    code = code.strip()
    if code.startswith("6"):
        return "sh"
    if code[0] in {"0", "2", "3"}:
        return "sz"
    if code[0] in {"4", "8"}:
        return "bj"
    return "sz"


def to_em_symbol(code: str) -> str:
    return f"{_prefix_for_code(code)}{code}"


@cacheable_df(lambda provider="em": f"realtime_spot_{provider}")
def get_realtime_spot(provider: str = "em", use_cache: bool = True) -> pd.DataFrame:
    """获取A股实时行情快照。
    采用 Eastmoney 源：stock_zh_a_spot_em
    """
    if provider == "em":
        df = ak.stock_zh_a_spot_em()
    else:
        df = ak.stock_zh_a_spot()
    # 标准化列名
    df = df.copy()
    # 尝试统一涨跌幅列
    if "涨跌幅" in df.columns:
        df.rename(columns={"涨跌幅": "pct_chg"}, inplace=True)
    elif "涨幅" in df.columns:
        df.rename(columns={"涨幅": "pct_chg"}, inplace=True)
    # 统一代码列
    if "代码" not in df.columns and "code" in df.columns:
        df.rename(columns={"code": "代码"}, inplace=True)
    return df


def list_a_stock_codes() -> List[str]:
    spot = get_realtime_spot(use_cache=True)
    codes = spot["代码"].astype(str).tolist()
    return codes


@cacheable_df(lambda date, **_: f"market_hist_{date}")
def get_historical_market(
    date: str,
    use_cache: bool = True,
    max_symbols: Optional[int] = DEFAULT_CONFIG.max_symbols_for_hist,
    sleep_seconds: float = DEFAULT_CONFIG.per_request_sleep_sec,
) -> pd.DataFrame:
    """按指定日期聚合全市场历史日线数据。

    说明：akshare不提供“过去日期的全市场快照”接口，只能逐个代码拉取日线。
    使用 stock_zh_a_hist(symbol, period='daily', start_date=date, end_date=date, adjust='')。
    """
    codes = list_a_stock_codes()
    if max_symbols is not None:
        codes = codes[:max_symbols]

    records: list[pd.DataFrame] = []
    for code in codes:
        symbol = to_em_symbol(code)
        try:
            df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=date, end_date=date, adjust="")
            if df is None or df.empty:
                time.sleep(sleep_seconds)
                continue
            # 添加代码与symbol，便于后续匹配
            df["代码"] = code
            df["symbol"] = symbol
            records.append(df)
        except Exception:
            # 忽略单只股票错误
            pass
        finally:
            time.sleep(sleep_seconds)

    if not records:
        return pd.DataFrame()
    out = pd.concat(records, ignore_index=True)
    # 标准化核心列名
    rename_map = {
        "涨跌幅": "pct_chg",
        "成交量": "volume",
        "成交额": "amount",
        "收盘": "close",
        "开盘": "open",
        "最高": "high",
        "最低": "low",
        "换手率": "turnover",
    }
    for k, v in rename_map.items():
        if k in out.columns:
            out.rename(columns={k: v}, inplace=True)
    return out


@cacheable_df(lambda symbol_code, start_date, end_date: f"hist_range_{symbol_code}_{start_date}_{end_date}")
def get_symbol_hist_range(symbol_code: str, start_date: str, end_date: str, use_cache: bool = True) -> pd.DataFrame:
    """获取单只股票区间日线数据，便于技术指标与量能比较。"""
    symbol = to_em_symbol(symbol_code)
    try:
        df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="")
        if df is None or df.empty:
            return pd.DataFrame()
        df["代码"] = symbol_code
        rename_map = {
            "涨跌幅": "pct_chg",
            "成交量": "volume",
            "收盘": "close",
        }
        for k, v in rename_map.items():
            if k in df.columns:
                df.rename(columns={k: v}, inplace=True)
        return df
    except Exception:
        return pd.DataFrame()