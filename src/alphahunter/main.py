from __future__ import annotations

import argparse
from datetime import datetime

import pandas as pd

from .config import DEFAULT_CONFIG
from .data_fetch import get_realtime_spot, get_historical_market
from .processing import clean_spot_df, clean_hist_df, sort_by_column
from .filters import top_percentile, filter_volume_surge
from .output import save_results, plot_top_n_bar
from .strategies import get_strong_stocks_comprehensive


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="AlphaHunter A股强势股票筛选")
    p.add_argument("--date", type=str, default=None, help="目标日期，格式YYYYMMDD；不填则实时或取当天用于综合策略")
    p.add_argument("--top_percent", type=float, default=DEFAULT_CONFIG.top_percent, help="涨幅前X%阈值")
    p.add_argument("--vol_ratio", type=float, default=DEFAULT_CONFIG.volume_surge_ratio, help="量能放大倍数阈值")
    p.add_argument("--max_symbols", type=int, default=DEFAULT_CONFIG.max_symbols_for_hist or 0, help="历史模式最大股票数，0表示全部")
    p.add_argument("--output", type=str, default="csv,json", help="输出格式，逗号分隔：csv,json,xlsx")
    p.add_argument("--strategy", type=str, choices=["basic", "comprehensive"], default="basic", help="筛选策略：basic=原逻辑；comprehensive=现成榜单+龙虎榜+板块补充")
    p.add_argument("--no_indicators", action="store_true", help="综合策略下关闭RSI/MACD指标过滤")
    return p.parse_args()


def run_realtime(args: argparse.Namespace) -> pd.DataFrame:
    spot = get_realtime_spot(use_cache=True)
    spot = clean_spot_df(spot)
    # 涨幅前X%
    strong = top_percentile(spot, percentile=args.top_percent, column="pct_chg")
    strong = sort_by_column(strong, "pct_chg", ascending=False)
    base_name = f"realtime_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    save_results(strong, base_name=base_name, formats=args.output.split(","))
    plot_top_n_bar(strong, value_col="pct_chg", name_col="代码", top_n=20, title="Realtime Top涨幅")
    return strong


def run_history(args: argparse.Namespace) -> pd.DataFrame:
    date = args.date
    max_symbols = None if args.max_symbols == 0 else args.max_symbols
    hist = get_historical_market(date=date, use_cache=True, max_symbols=max_symbols)
    hist = clean_hist_df(hist)
    # 涨幅前X%
    strong = top_percentile(hist, percentile=100 - args.top_percent, column="pct_chg")
    # 量能放大过滤
    strong = filter_volume_surge(strong, date=date, min_ratio=args.vol_ratio)
    strong = sort_by_column(strong, "pct_chg", ascending=False)
    base_name = f"hist_{date}"
    save_results(strong, base_name=base_name, formats=args.output.split(","))
    plot_top_n_bar(strong, value_col="pct_chg", name_col="代码", top_n=20, title=f"{date} Top涨幅")
    return strong


def main() -> None:  # pragma: no cover
    args = parse_args()
    if args.strategy == "comprehensive":
        # 允许临时关闭指标过滤
        if args.no_indicators:
            from .config import DEFAULT_CONFIG as CFG
            CFG.enable_indicator_filter = False
        date = args.date or datetime.now().strftime("%Y%m%d")
        df = get_strong_stocks_comprehensive(date)
        base_name = f"strong_stocks_{date}"
        save_results(df, base_name=base_name, formats=args.output.split(","))
        if "pct_chg" in df.columns:
            plot_top_n_bar(df, value_col="pct_chg", name_col="代码", top_n=20, title=f"{date} 综合强势Top")
    else:
        if args.date:
            df = run_history(args)
        else:
            df = run_realtime(args)
    print(f"筛选结果：{len(df)} 条")
    print(df.head(20).to_string(index=False))


if __name__ == "__main__":  # pragma: no cover
    main()