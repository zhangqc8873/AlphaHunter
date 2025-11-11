from __future__ import annotations

import datetime as dt
from typing import List

import pandas as pd
import numpy as np
import streamlit as st

from pathlib import Path
import sys

# 将项目 src 目录加入 sys.path，便于绝对导入
ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT / "src"))

from alphahunter.config import DEFAULT_CONFIG
from alphahunter.strategies import get_strong_stocks_comprehensive, get_strong_stocks_comprehensive_with_stats
from alphahunter.data_fetch import get_symbol_hist_range
from alphahunter.realtime_service import (
    save_config as rt_save_config,
    load_config as rt_load_config,
    read_latest_snapshot,
    is_trading_time_now,
)
from alphahunter.filters import compute_rsi, compute_macd


st.set_page_config(page_title="AlphaHunter 强势股跟踪", layout="wide")

st.title("AlphaHunter 强势股筛选与价格跟踪")
st.caption("使用涨停池 / 人气榜 / 龙虎榜 / 板块轮动组合，并支持RSI/MACD过滤与价格跟踪")


def yyyyMMdd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")


# Sidebar 参数
today = dt.date.today()
date_input = st.sidebar.date_input("目标交易日", today)
target_date = yyyyMMdd(date_input)

st.sidebar.markdown("### 指标过滤设置")
enable_ind = st.sidebar.checkbox("启用RSI/MACD过滤", value=True)
rsi_min = st.sidebar.slider("RSI最小值", 0, 100, int(DEFAULT_CONFIG.indicator_rsi_min))
macd_hist_min = st.sidebar.number_input("MACD柱体最小值", value=float(DEFAULT_CONFIG.indicator_macd_hist_min), step=0.1)
lookback_days = st.sidebar.slider("指标回看天数", 20, 120, int(DEFAULT_CONFIG.indicator_lookback_days))
max_check = st.sidebar.slider("指标检查股票数上限", 10, 200, int(DEFAULT_CONFIG.max_symbols_indicator_check))

st.sidebar.markdown("### 价格跟踪设置")
track_days = st.sidebar.slider("跟踪区间天数", 30, 180, 90)
sleep_seconds = st.sidebar.number_input("每请求休眠秒数", value=float(DEFAULT_CONFIG.per_request_sleep_sec), min_value=0.0, step=0.1)

run_btn = st.sidebar.button("运行筛选")

# 会话状态：持久化筛选结果与选中代码，避免交互导致页面重载后丢失
if "result_df" not in st.session_state:
    st.session_state["result_df"] = pd.DataFrame()
if "stats" not in st.session_state:
    st.session_state["stats"] = None
if "selected_codes" not in st.session_state:
    st.session_state["selected_codes"] = []


def apply_indicator_config():
    DEFAULT_CONFIG.enable_indicator_filter = enable_ind
    DEFAULT_CONFIG.indicator_rsi_min = float(rsi_min)
    DEFAULT_CONFIG.indicator_macd_hist_min = float(macd_hist_min)
    DEFAULT_CONFIG.indicator_lookback_days = int(lookback_days)
    DEFAULT_CONFIG.max_symbols_indicator_check = int(max_check)
    DEFAULT_CONFIG.per_request_sleep_sec = float(sleep_seconds)


@st.cache_data(show_spinner=False)
def run_screening(date: str) -> pd.DataFrame:
    return get_strong_stocks_comprehensive(date)

def run_screening_with_progress(date: str):
    steps_total = 5
    prog = st.progress(0)
    step_text = st.empty()
    def _cb(step_idx: int, label: str):
        prog.progress(int(max(0, min(step_idx, steps_total)) / steps_total * 100))
        step_text.write(f"阶段 {step_idx}/{steps_total}: {label}")
    df, stats = get_strong_stocks_comprehensive_with_stats(date, progress_cb=_cb)
    prog.progress(100)
    return df, stats


if run_btn:
    apply_indicator_config()
    with st.spinner("正在获取并筛选强势股..."):
        result_df, stats = run_screening_with_progress(target_date)
    # 写入会话状态，避免后续交互导致数据丢失
    st.session_state["result_df"] = result_df
    st.session_state["stats"] = stats
    # 初始化默认选中
    if result_df is not None and not result_df.empty:
        codes_default = result_df["代码"].astype(str).tolist()[: min(10, len(result_df))] if "代码" in result_df.columns else []
        st.session_state["selected_codes"] = codes_default

# 渲染：无论是否点击过“运行筛选”，只要有结果就展示并可交互
result_df = st.session_state.get("result_df")
stats = st.session_state.get("stats")
if result_df is not None and not result_df.empty:
    st.success(f"找到 {len(result_df)} 只强势股")
    if stats:
        with st.expander("筛选阶段统计"):
            st.write(stats)
    st.dataframe(result_df, use_container_width=True)

    # 选择个股进行跟踪（持久化选中项）
    codes = result_df["代码"].astype(str).tolist() if "代码" in result_df.columns else []
    # 注意：避免同时使用 default 参数与 Session State 赋值，
    # 否则会出现“The widget with key ... was created with a default value but also had its value set via the Session State API.”告警。
    st.multiselect(
        "选择需要跟踪的股票代码",
        options=codes,
        key="selected_codes",
        help="选择后页面即会重载，但已选项会被保留。",
    )

    # 实时状态与提醒设置
    st.markdown("---")
    st.subheader("实时状态")
    cfg = rt_load_config()
    default_codes = st.session_state.get("selected_codes", cfg.get("tracked_codes", []))
    poll_min_default = int(cfg.get("poll_interval_sec", 300)) // 60
    alert_pct_default = float(cfg.get("alert_threshold_pct", 3.0))
    retention_days_default = int(cfg.get("retention_days", 7))

    with st.sidebar:
        st.markdown("### 实时跟踪设置")
        poll_minutes = st.number_input("实时轮询间隔（分钟）", min_value=1, max_value=60, value=poll_min_default)
        alert_thresh = st.number_input("预警阈值（%）", min_value=0.1, max_value=20.0, value=alert_pct_default, step=0.1)
        retention_days = st.number_input("保留天数", min_value=1, max_value=30, value=retention_days_default)
        if st.button("保存实时跟踪配置"):
            new_cfg = {
                "tracked_codes": default_codes,
                "poll_interval_sec": int(poll_minutes * 60),
                "alert_threshold_pct": float(alert_thresh),
                "retention_days": int(retention_days),
            }
            rt_save_config(new_cfg)
            st.success("配置已保存。后台服务将读取此配置。")

    trading = is_trading_time_now()
    st.caption(f"交易时段状态：{'在交易' if trading else '休市'}")

    latest_df = read_latest_snapshot()
    if latest_df is None or len(latest_df) == 0:
        st.info("尚无实时快照，请启动后台服务进程以采集数据。")
        st.code("python -m alphahunter.realtime_service", language="bash")
    else:
        # 按选择过滤显示
        if "代码" in latest_df.columns and default_codes:
            show_df = latest_df[latest_df["代码"].isin(default_codes)].copy()
        else:
            show_df = latest_df.copy()
        cols = [c for c in ["采集时间", "代码", "名称", "最新价", "close", "pct_chg", "alert", "状态"] if c in show_df.columns]
        st.dataframe(show_df[cols], use_container_width=True)

    # 价格跟踪可视化
    selected_codes = st.session_state.get("selected_codes", [])
    if selected_codes:
        end_date = yyyyMMdd(today)
        start_date = yyyyMMdd(today - dt.timedelta(days=track_days))
        tabs = st.tabs([f"{code}" for code in selected_codes])
        progress = st.progress(0)
        source_counts = {"em": 0, "sina": 0, "tx": 0}
        failed_codes: List[str] = []
        for idx, (tab, code) in enumerate(zip(tabs, selected_codes), start=1):
            with tab:
                hist = get_symbol_hist_range(code, start_date=start_date, end_date=end_date, use_cache=True)
                if hist is None or hist.empty:
                    failed_codes.append(code)
                    st.warning("该股票区间数据不可用（已自动重试多个数据源）")
                    progress.progress(int(idx / len(selected_codes) * 100))
                    continue
                hist = hist.copy()
                # 统计数据源
                if "source" in hist.columns:
                    src = str(hist["source"].iloc[0])
                    if src in source_counts:
                        source_counts[src] += 1
                # 日期与列检查与清理
                if "日期" in hist.columns:
                    hist["日期"] = pd.to_datetime(hist["日期"], errors="coerce")
                if "close" not in hist.columns:
                    st.warning("缺少收盘价，无法绘图")
                    progress.progress(int(idx / len(selected_codes) * 100))
                    continue
                # 指标计算
                close = pd.to_numeric(hist["close"], errors="coerce")
                hist["close"] = close
                hist["RSI"] = compute_rsi(close, window=DEFAULT_CONFIG.rsi_window)
                macd_df = compute_macd(close, fast=DEFAULT_CONFIG.macd_fast, slow=DEFAULT_CONFIG.macd_slow, signal=DEFAULT_CONFIG.macd_signal)
                hist["MACD_hist"] = macd_df["hist"].values

                # 简要数据源标注
                if "source" in hist.columns:
                    st.caption(f"数据源：{hist['source'].iloc[0]}")

                # 数据清理：去除无效日期与非有限值，避免 Vega-Lite Infinity 告警
                price_df = hist[["日期", "close"]].dropna().copy()
                price_df = price_df[np.isfinite(price_df["close"])]
                rsi_df = hist[["日期", "RSI"]].dropna().copy()
                rsi_df = rsi_df[np.isfinite(rsi_df["RSI"])]
                macd_plot_df = hist[["日期", "MACD_hist"]].dropna().copy()
                macd_plot_df = macd_plot_df[np.isfinite(macd_plot_df["MACD_hist"])]

                # 上方K线/收盘价折线，下方RSI与MACD柱体
                if not price_df.empty:
                    st.line_chart(price_df.set_index("日期")["close"], height=200)
                else:
                    st.warning("价格序列为空或全部为无效值，无法绘图")
                if not rsi_df.empty:
                    st.line_chart(rsi_df.set_index("日期")["RSI"], height=150)
                else:
                    st.info("RSI 序列为空或全部为无效值")
                if not macd_plot_df.empty:
                    st.bar_chart(macd_plot_df.set_index("日期")["MACD_hist"], height=150)
                else:
                    st.info("MACD 柱体序列为空或全部为无效值")

                # 简单评估：从目标日到最新日的收益率（若目标日在区间内）
                perf_col = st.columns(3)
                try:
                    if "日期" in hist.columns and "close" in hist.columns:
                        if pd.to_datetime(target_date) in hist["日期"].values:
                            start_close = float(hist.loc[hist["日期"] == pd.to_datetime(target_date), "close"].iloc[0])
                            last_close = float(hist["close"].iloc[-1])
                            ret = (last_close / start_close - 1.0) * 100.0
                            perf_col[0].metric("自目标日起收益率%", f"{ret:.2f}%")
                        else:
                            perf_col[0].write("目标日不在跟踪区间内")
                    perf_col[1].metric("最新收盘价", f"{hist['close'].iloc[-1]:.2f}")
                    perf_col[2].metric("RSI(末值)", f"{hist['RSI'].iloc[-1]:.1f}")
                except Exception:
                    pass

            progress.progress(int(idx / len(selected_codes) * 100))

        with st.expander("数据源与失败统计"):
            total = len(selected_codes)
            st.write({"总数": total, "失败数": len(failed_codes), "东财": source_counts["em"], "新浪": source_counts["sina"], "腾讯": source_counts["tx"]})
            if failed_codes:
                st.write("失败代码：", ", ".join(failed_codes))

    # 导出筛选结果
    st.download_button(
        label="下载筛选结果CSV",
        data=result_df.to_csv(index=False, encoding="utf-8-sig"),
        file_name=f"strong_stocks_{target_date}.csv",
        mime="text/csv",
    )
else:
    st.info("在左侧选择参数并点击“运行筛选”开始。")