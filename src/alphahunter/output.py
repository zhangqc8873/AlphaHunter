from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from .config import DEFAULT_CONFIG


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def save_results(df: pd.DataFrame, base_name: str, formats: list[str] | None = None) -> list[Path]:
    formats = formats or ["csv", "json"]
    ensure_dir(DEFAULT_CONFIG.output_dir)
    saved: list[Path] = []
    for fmt in formats:
        out_path = DEFAULT_CONFIG.output_dir / f"{base_name}.{fmt}"
        if fmt == "csv":
            df.to_csv(out_path, index=False)
        elif fmt == "json":
            df.to_json(out_path, orient="records", force_ascii=False)
        elif fmt == "xlsx":
            df.to_excel(out_path, index=False)
        else:
            continue
        saved.append(out_path)
    return saved


def plot_top_n_bar(df: pd.DataFrame, value_col: str, name_col: str = "代码", top_n: int = 20, title: Optional[str] = None) -> Path | None:
    if df.empty or value_col not in df.columns:
        return None
    ensure_dir(DEFAULT_CONFIG.output_dir)
    plot_df = df.nlargest(top_n, value_col)
    plt.figure(figsize=(10, 6))
    sns.barplot(data=plot_df, x=name_col, y=value_col)
    plt.xticks(rotation=90)
    plt.title(title or f"Top {top_n} by {value_col}")
    plt.tight_layout()
    out_path = DEFAULT_CONFIG.output_dir / f"chart_{value_col}.png"
    plt.savefig(out_path)
    plt.close()
    return out_path