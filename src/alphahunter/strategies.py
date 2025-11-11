from __future__ import annotations

import time
from typing import List

import pandas as pd

try:
    import akshare as ak
except Exception as e:  # pragma: no cover
    raise RuntimeError("akshare 未安装或导入失败，请先安装 akshare") from e

from .cache import cacheable_df
from .config import DEFAULT_CONFIG
from .filters import compute_rsi, compute_macd
from .data_fetch import get_symbol_hist_range


def _ensure_code_col(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "代码" not in out.columns:
        for col in ["code", "股票代码", "证券代码"]:
            if col in out.columns:
                out.rename(columns={col: "代码"}, inplace=True)
                break
    return out


@cacheable_df(lambda date: f"strong_direct_{date}")
def get_strong_stocks_direct(date: str, use_cache: bool = True) -> pd.DataFrame:
    """直接获取强势股榜单：使用涨停股池作为强势来源。"""
    try:
        df = ak.stock_zt_pool_em(date=date)
        df = _ensure_code_col(df)
        # 常见列：代码、名称、涨停原因类别、所属行业、连板数、成交额、涨跌幅等
        pick_cols = [c for c in ["代码", "名称", "涨跌幅", "连板数", "所属行业", "涨停时间"] if c in df.columns]
        return df[pick_cols] if pick_cols else df
    except Exception:
        return pd.DataFrame()


@cacheable_df(lambda date: f"strong_hot_{date}")
def get_strong_stocks_hot(date: str, use_cache: bool = True) -> pd.DataFrame:
    """人气榜作为冗余现成榜单来源。

    优先使用东方财富个股人气榜与飙升榜；部分接口为最新榜单，不严格按历史日期。
    """
    frames: List[pd.DataFrame] = []
    try:
        hot_rank = ak.stock_hot_rank_em()
        hot_rank = _ensure_code_col(hot_rank)
        frames.append(hot_rank)
    except Exception:
        pass
    try:
        hot_up = ak.stock_hot_up_em()
        hot_up = _ensure_code_col(hot_up)
        frames.append(hot_up)
    except Exception:
        pass
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df = _ensure_code_col(df)
    if "代码" in df.columns:
        df = df.drop_duplicates(subset=["代码"]).reset_index(drop=True)
    return df


@cacheable_df(lambda date: f"strong_lhb_{date}")
def get_strong_stocks_billboard(date: str, use_cache: bool = True) -> pd.DataFrame:
    """获取指定日期龙虎榜个股。"""
    try:
        df = ak.stock_lhb_detail_em(start_date=date, end_date=date)
        df = _ensure_code_col(df)
        pick_cols = [c for c in ["代码", "名称", "涨跌幅", "上榜原因", "买入额", "卖出额"] if c in df.columns]
        out = df[pick_cols] if pick_cols else df
        # 去重同一代码
        return out.drop_duplicates(subset=["代码"]).reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


@cacheable_df(lambda date, top_boards=5, **_: f"strong_sector_{date}_{top_boards}")
def get_strong_stocks_via_sector(date: str, top_boards: int = 5, candidates_per_board: int = 20, use_cache: bool = True) -> pd.DataFrame:
    """通过板块轮动补充强势股：选取当日涨幅居前的行业板块，并从其成分股中挑选候选。"""
    try:
        # 获取行业板块当日表现
        boards = ak.stock_board_industry_name_em()
        if boards is None or boards.empty:
            return pd.DataFrame()
        # 统一涨幅列名
        boards = boards.copy()
        if "涨跌幅" not in boards.columns and "涨幅" in boards.columns:
            boards.rename(columns={"涨幅": "涨跌幅"}, inplace=True)
        boards["涨跌幅"] = pd.to_numeric(boards["涨跌幅"], errors="coerce")
        boards = boards.sort_values(by="涨跌幅", ascending=False).head(top_boards)

        records: List[pd.DataFrame] = []
        for _, row in boards.iterrows():
            name_col = "板块名称" if "板块名称" in boards.columns else ("行业名称" if "行业名称" in boards.columns else None)
            if not name_col:
                continue
            board_name = str(row[name_col])
            try:
                cons = ak.stock_board_industry_cons_em(symbol=board_name)
                cons = _ensure_code_col(cons)
                # 取少量成分股作为候选，减少API调用和后续处理压力
                pick_cols = [c for c in ["代码", "名称", "板块", "最新价", "涨跌幅"] if c in cons.columns]
                cons = cons[pick_cols] if pick_cols else cons
                cons = cons.head(candidates_per_board)
                cons["来源板块"] = board_name
                records.append(cons)
            except Exception:
                # 某些板块拉取失败时跳过
                pass
            finally:
                time.sleep(DEFAULT_CONFIG.per_request_sleep_sec)

        if not records:
            return pd.DataFrame()
        out = pd.concat(records, ignore_index=True)
        # 去重
        if "代码" in out.columns:
            out = out.drop_duplicates(subset=["代码"]).reset_index(drop=True)
        return out
    except Exception:
        return pd.DataFrame()


def get_strong_stocks_comprehensive(target_date: str) -> pd.DataFrame:
    """综合多种高效方法获取强势股。包含API调用保护与缓存。

    步骤：
    1) 直接获取现成榜单（涨停股池）
    2) 获取龙虎榜股票
    3) 若不足，则通过板块轮动补充
    """
    all_strong: List[pd.DataFrame] = []

    print("=== 开始获取强势股 ===")

    # 1. 现成榜单
    print("1. 获取现成强势股榜单...")
    direct_df = get_strong_stocks_direct(target_date, use_cache=True)
    if direct_df is not None and not direct_df.empty:
        all_strong.append(direct_df)

    time.sleep(DEFAULT_CONFIG.per_request_sleep_sec)

    # 1b. 人气榜冗余来源
    print("1b. 获取人气榜股票...")
    hot_df = get_strong_stocks_hot(target_date, use_cache=True)
    if hot_df is not None and not hot_df.empty:
        all_strong.append(hot_df)

    # 2. 龙虎榜
    print("2. 获取龙虎榜股票...")
    lhb_df = get_strong_stocks_billboard(target_date, use_cache=True)
    if lhb_df is not None and not lhb_df.empty:
        all_strong.append(lhb_df)

    time.sleep(DEFAULT_CONFIG.per_request_sleep_sec)

    # 3. 板块轮动补充
    combined_count = sum(len(df) for df in all_strong)
    if combined_count < 20:
        print("3. 通过板块轮动补充...")
        sector_df = get_strong_stocks_via_sector(target_date, use_cache=True)
        if sector_df is not None and not sector_df.empty:
            all_strong.append(sector_df)

    # 合并与去重
    if all_strong:
        final = pd.concat(all_strong, ignore_index=True)
        final = _ensure_code_col(final)
        if "代码" in final.columns:
            final = final.drop_duplicates(subset=["代码"], keep="first")
        # 指标过滤辅助（可配置）
        if DEFAULT_CONFIG.enable_indicator_filter and "代码" in final.columns:
            print("4. 指标过滤辅助 (RSI / MACD)…")
            final = _apply_indicator_filter(final, target_date)
        print(f"=== 最终找到 {len(final)} 只强势股 ===")
        return final
    else:
        print("未找到强势股")
        return pd.DataFrame()


def _apply_indicator_filter(df: pd.DataFrame, date: str) -> pd.DataFrame:
    """在合并结果上应用轻量指标过滤：RSI>=阈值且MACD柱体>=阈值。

    为保护API调用：
    - 仅对前 max_symbols_indicator_check 只股票计算指标；
    - 使用缓存与节流；
    """
    limit = min(len(df), DEFAULT_CONFIG.max_symbols_indicator_check)
    subset = df.head(limit).copy()
    rsi_vals: List[float | None] = []
    macd_hist_vals: List[float | None] = []

    for code in subset["代码"].astype(str).tolist():
        start = _offset_days(date, DEFAULT_CONFIG.indicator_lookback_days)
        hist = get_symbol_hist_range(code, start_date=start, end_date=date, use_cache=True)
        if hist is None or hist.empty or "close" not in hist.columns:
            rsi_vals.append(None)
            macd_hist_vals.append(None)
            time.sleep(DEFAULT_CONFIG.per_request_sleep_sec)
            continue
        close = pd.to_numeric(hist["close"], errors="coerce")
        rsi_series = compute_rsi(close, window=DEFAULT_CONFIG.rsi_window)
        macd_df = compute_macd(close, fast=DEFAULT_CONFIG.macd_fast, slow=DEFAULT_CONFIG.macd_slow, signal=DEFAULT_CONFIG.macd_signal)
        rsi_vals.append(float(rsi_series.iloc[-1]) if not rsi_series.empty else None)
        macd_hist_vals.append(float(macd_df["hist"].iloc[-1]) if not macd_df.empty else None)
        time.sleep(DEFAULT_CONFIG.per_request_sleep_sec)

    subset["rsi"] = rsi_vals
    subset["macd_hist"] = macd_hist_vals
    # 过滤条件
    rsi_min = DEFAULT_CONFIG.indicator_rsi_min
    macd_min = DEFAULT_CONFIG.indicator_macd_hist_min
    filtered = subset[(subset["rsi"] >= rsi_min) & (subset["macd_hist"] >= macd_min)]
    # 将过滤结果与原始df按代码左连接合并，保留符合的股票
    if "代码" in df.columns:
        keep_codes = set(filtered["代码"].astype(str).tolist())
        df = df[df["代码"].astype(str).isin(keep_codes)].copy().reset_index(drop=True)
        # 附加指标列（可选展示）
        df = df.merge(filtered[["代码", "rsi", "macd_hist"]], on="代码", how="left")
    return df