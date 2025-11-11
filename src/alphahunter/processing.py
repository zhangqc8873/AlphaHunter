from __future__ import annotations

import pandas as pd


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace("%", "", regex=False), errors="coerce")


def clean_spot_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "pct_chg" in out.columns:
        out["pct_chg"] = _to_numeric(out["pct_chg"])  # 百分号转浮点
    # 成交量/成交额数值化
    for col in ["成交量", "成交额", "volume", "amount"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=["代码"]).reset_index(drop=True)
    return out


def clean_hist_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ["pct_chg", "volume", "amount", "close", "open", "high", "low", "turnover"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=["代码"]).reset_index(drop=True)
    return out


def sort_by_column(df: pd.DataFrame, column: str, ascending: bool = False) -> pd.DataFrame:
    if column in df.columns:
        return df.sort_values(by=column, ascending=ascending).reset_index(drop=True)
    return df