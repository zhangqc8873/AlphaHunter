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
    优先使用东方财富；若单票失败或为空，自动回退到新浪与腾讯；统一输出必要列。
    """
    codes = list_a_stock_codes()
    if max_symbols is not None:
        codes = codes[:max_symbols]

    records: list[pd.DataFrame] = []
    for code in codes:
        def _unify_columns(df: pd.DataFrame) -> pd.DataFrame:
            if df is None or df.empty:
                return pd.DataFrame()
            out = df.copy()
            if "日期" not in out.columns and "date" in out.columns:
                out.rename(columns={"date": "日期"}, inplace=True)
            if "close" not in out.columns and "收盘" in out.columns:
                out.rename(columns={"收盘": "close"}, inplace=True)
            # 尽量统一常见列
            rename_map = {
                "涨跌幅": "pct_chg",
                "成交量": "volume",
                "开盘": "open",
                "最高": "high",
                "最低": "low",
                "成交额": "amount",
            }
            for src, dst in rename_map.items():
                if src in out.columns and dst not in out.columns:
                    out.rename(columns={src: dst}, inplace=True)
            if "日期" not in out.columns or "close" not in out.columns:
                return pd.DataFrame()
            return out

        symbol = to_em_symbol(code)
        got: pd.DataFrame | None = None
        # 1) 东方财富
        try:
            df_em = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=date, end_date=date, adjust="")
            df_em = _unify_columns(df_em)
            if df_em is not None and not df_em.empty:
                df_em["source"] = "em"
                got = df_em
        except Exception:
            pass

        # 2) 新浪
        if got is None or got.empty:
            try:
                prefix = _prefix_for_code(code)
                df_sina = ak.stock_zh_a_daily(symbol=f"{prefix}{code}", start_date=date, end_date=date, adjust="")
                df_sina = _unify_columns(df_sina)
                if df_sina is not None and not df_sina.empty:
                    df_sina["source"] = "sina"
                    got = df_sina
            except Exception:
                pass

        # 3) 腾讯
        if got is None or got.empty:
            try:
                prefix = _prefix_for_code(code)
                df_tx = ak.stock_zh_a_hist_tx(symbol=f"{prefix}{code}", start_date=date, end_date=date, adjust="")
                df_tx = _unify_columns(df_tx)
                if df_tx is not None and not df_tx.empty:
                    df_tx["source"] = "tx"
                    got = df_tx
            except Exception:
                pass

        if got is not None and not got.empty:
            got["代码"] = code
            got["symbol"] = symbol
            records.append(got)

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
    """获取单只股票区间日线数据，便于技术指标与量能比较。

    优先使用东方财富(日线)；若失败或为空，自动回退到新浪与腾讯数据源。
    所有数据源统一输出包含列：`日期`、`close`，并附加 `代码` 列。
    """

    def _unify_columns(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        out = df.copy()
        # 日期列统一
        if "日期" not in out.columns and "date" in out.columns:
            out.rename(columns={"date": "日期"}, inplace=True)
        # 收盘价统一
        if "close" not in out.columns and "收盘" in out.columns:
            out.rename(columns={"收盘": "close"}, inplace=True)
        # 其他常用列统一（非强制，仅尽量统一）
        rename_map = {
            "涨跌幅": "pct_chg",
            "成交量": "volume",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "成交额": "amount",
        }
        for src, dst in rename_map.items():
            if src in out.columns and dst not in out.columns:
                out.rename(columns={src: dst}, inplace=True)
        # 保证必要列存在
        if "日期" not in out.columns or "close" not in out.columns:
            return pd.DataFrame()
        return out

    code = symbol_code.strip()
    symbol_em = to_em_symbol(code)
    # 1) 东方财富
    try:
        df_em = ak.stock_zh_a_hist(symbol=symbol_em, period="daily", start_date=start_date, end_date=end_date, adjust="")
        df_em = _unify_columns(df_em)
        if df_em is not None and not df_em.empty:
            df_em["代码"] = code
            df_em["source"] = "em"
            return df_em
    except Exception:
        pass

    # 2) 新浪（日线，需带交易所前缀）
    try:
        prefix = _prefix_for_code(code)
        df_sina = ak.stock_zh_a_daily(symbol=f"{prefix}{code}", start_date=start_date, end_date=end_date, adjust="")
        df_sina = _unify_columns(df_sina)
        if df_sina is not None and not df_sina.empty:
            df_sina["代码"] = code
            df_sina["source"] = "sina"
            return df_sina
    except Exception:
        pass

    # 3) 腾讯（日线，需带交易所前缀）
    try:
        prefix = _prefix_for_code(code)
        df_tx = ak.stock_zh_a_hist_tx(symbol=f"{prefix}{code}", start_date=start_date, end_date=end_date, adjust="")
        df_tx = _unify_columns(df_tx)
        if df_tx is not None and not df_tx.empty:
            df_tx["代码"] = code
            df_tx["source"] = "tx"
            return df_tx
    except Exception:
        pass

    # 全部失败
    return pd.DataFrame()