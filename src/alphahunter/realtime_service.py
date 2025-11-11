import json
import time
import gzip
from datetime import datetime, timedelta
import os
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from .config import DEFAULT_CONFIG
from .data_fetch import get_realtime_spot
from .processing import clean_spot_df


# Paths for config and data persistence
# DEFAULT_CONFIG 是 dataclass 对象，不支持 dict.get；直接使用属性
CACHE_DIR = DEFAULT_CONFIG.cache_dir
REALTIME_DIR = CACHE_DIR / "realtime"
REALTIME_DIR.mkdir(parents=True, exist_ok=True)

CONFIG_PATH = REALTIME_DIR / "realtime_config.json"
LATEST_PATH = REALTIME_DIR / "realtime_latest.csv"
LOG_DIR = REALTIME_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
CONTROL_PATH = REALTIME_DIR / "control.json"
STATUS_PATH = REALTIME_DIR / "service_status.json"
PID_PATH = REALTIME_DIR / "service_pid.txt"


DEFAULT_SERVICE_CONFIG = {
    "tracked_codes": [],  # list[str]
    "poll_interval_sec": 300,  # 5 minutes by default
    "alert_threshold_pct": 3.0,  # percent change threshold
    "retention_days": 7,  # keep logs for N days
}


def load_config() -> Dict:
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return DEFAULT_SERVICE_CONFIG.copy()


def save_config(cfg: Dict) -> None:
    # Only persist known keys to avoid junk
    clean = DEFAULT_SERVICE_CONFIG.copy()
    clean.update({k: v for k, v in cfg.items() if k in clean})
    CONFIG_PATH.write_text(json.dumps(clean, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_control() -> Dict:
    if CONTROL_PATH.exists():
        try:
            return json.loads(CONTROL_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"paused": False, "stop": False}


def _write_control(ctrl: Dict) -> None:
    clean = {"paused": bool(ctrl.get("paused", False)), "stop": bool(ctrl.get("stop", False))}
    tmp = CONTROL_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(clean, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(CONTROL_PATH)


def is_trading_time_now(ts: datetime | None = None) -> bool:
    dt = ts or datetime.now()
    # China A-share trading hours (local time):
    # Weekdays (Mon-Fri): 09:30-11:30, 13:00-15:00
    if dt.weekday() >= 5:
        return False
    hm = dt.hour * 100 + dt.minute
    morning = 930 <= hm <= 1130
    afternoon = 1300 <= hm <= 1500
    return morning or afternoon


def _extract_price_col(df: pd.DataFrame) -> str | None:
    # Try common columns for latest price
    candidates = [
        "最新价", "现价", "价格", "close", "收盘", "price"
    ]
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _atomic_write_csv(df: pd.DataFrame, path: Path) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp, index=False, encoding="utf-8")
    tmp.replace(path)


def _atomic_write_json(obj: Dict, path: Path) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _append_log(df: pd.DataFrame, now: datetime, retention_days: int) -> None:
    # Append daily log file and compress previous days if needed
    day = now.strftime("%Y%m%d")
    log_path = LOG_DIR / f"prices_{day}.csv"
    header = not log_path.exists()
    df.to_csv(log_path, mode="a", header=header, index=False, encoding="utf-8")

    # Housekeeping: compress older logs and delete beyond retention
    cutoff = now.date() - timedelta(days=retention_days)
    for p in LOG_DIR.glob("prices_*.csv"):
        # Compress logs not from today
        if p.name != log_path.name:
            gz_path = p.with_suffix(p.suffix + ".gz")
            if not gz_path.exists():
                with p.open("rb") as f_in, gzip.open(gz_path, "wb") as f_out:
                    f_out.write(f_in.read())
                try:
                    p.unlink()
                except Exception:
                    pass
    # Cleanup compressed logs older than retention
    for gz in LOG_DIR.glob("prices_*.csv.gz"):
        try:
            date_str = gz.stem.split("_")[1]  # prices_YYYYMMDD.csv.gz -> stem: prices_YYYYMMDD.csv
            date_str = date_str.replace(".csv", "")
            d = datetime.strptime(date_str, "%Y%m%d").date()
            if d < cutoff:
                gz.unlink()
        except Exception:
            continue


def one_poll(tracked_codes: List[str], alert_threshold_pct: float) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fetch realtime market snapshot, filter tracked codes, compute alerts.
    Returns (latest_snapshot_df, latest_tracked_df).
    """
    spot_df = get_realtime_spot()
    if spot_df is None or len(spot_df) == 0:
        return pd.DataFrame(), pd.DataFrame()

    spot_df = clean_spot_df(spot_df)
    # Ensure expected columns
    price_col = _extract_price_col(spot_df)
    if price_col is None:
        # Fallback: create synthetic price from close if possible
        if "close" in spot_df.columns:
            price_col = "close"
        else:
            # nothing usable
            return spot_df, pd.DataFrame()

    # Filter to tracked codes
    tracked_df = spot_df[spot_df["代码"].isin(tracked_codes)].copy()
    if len(tracked_df) == 0:
        return spot_df, tracked_df

    # Calculate alert flag based on pct change
    if "pct_chg" in tracked_df.columns:
        tracked_df["alert"] = tracked_df["pct_chg"].abs() >= alert_threshold_pct
    else:
        tracked_df["alert"] = False

    # Keep only relevant columns for snapshot
    keep_cols = [c for c in ["时间", "代码", price_col, "pct_chg", "名称", "alert"] if c in tracked_df.columns]
    tracked_df = tracked_df[keep_cols].copy()
    now = datetime.now()
    tracked_df["采集时间"] = now.strftime("%Y-%m-%d %H:%M:%S")
    return spot_df, tracked_df


def run_service(loop_once: bool = False) -> None:
    cfg = load_config()
    poll_interval = max(30, int(cfg.get("poll_interval_sec", 300)))
    alert_threshold = float(cfg.get("alert_threshold_pct", 3.0))
    tracked_codes: List[str] = list(cfg.get("tracked_codes", []))
    retention_days = int(cfg.get("retention_days", 7))

    # init status
    start_ts = datetime.now()
    try:
        PID_PATH.write_text(str(os.getpid()), encoding="utf-8")
    except Exception:
        pass
    error_count = 0
    _atomic_write_json({
        "running": True,
        "pid": os.getpid(),
        "start_time": start_ts.strftime("%Y-%m-%d %H:%M:%S"),
        "last_poll_time": None,
        "progress_pct": 0.0,
        "error_count": 0,
        "trading": is_trading_time_now(start_ts),
        "paused": False,
        "stop_requested": False,
    }, STATUS_PATH)

    while True:
        now = datetime.now()
        ctrl = _read_control()
        trading_flag = is_trading_time_now(now)
        if ctrl.get("stop", False):
            _atomic_write_json({
                "running": False,
                "pid": os.getpid(),
                "start_time": start_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "last_poll_time": None,
                "progress_pct": 0.0,
                "error_count": error_count,
                "trading": trading_flag,
                "paused": ctrl.get("paused", False),
                "stop_requested": True,
            }, STATUS_PATH)
            break

        if ctrl.get("paused", False):
            _atomic_write_json({
                "running": True,
                "pid": os.getpid(),
                "start_time": start_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "last_poll_time": None,
                "progress_pct": 0.0,
                "error_count": error_count,
                "trading": trading_flag,
                "paused": True,
                "stop_requested": False,
            }, STATUS_PATH)
            time.sleep(2)
            if loop_once:
                break
            continue

        if trading_flag and tracked_codes:
            try:
                spot_df, tracked_df = one_poll(tracked_codes, alert_threshold)
                if len(tracked_df) > 0:
                    _atomic_write_csv(tracked_df, LATEST_PATH)
                    _append_log(tracked_df, now, retention_days)
                _atomic_write_json({
                    "running": True,
                    "pid": os.getpid(),
                    "start_time": start_ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "last_poll_time": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "progress_pct": 0.0,
                    "error_count": error_count,
                    "trading": trading_flag,
                    "paused": False,
                    "stop_requested": False,
                }, STATUS_PATH)
            except Exception:
                # Avoid crashing service on transient errors
                error_count += 1
                time.sleep(5)
        else:
            # When not trading, still update LATEST with a heartbeat
            hb = pd.DataFrame({
                "采集时间": [now.strftime("%Y-%m-%d %H:%M:%S")],
                "状态": ["非交易时段"],
            })
            try:
                _atomic_write_csv(hb, LATEST_PATH)
            except Exception:
                pass
            _atomic_write_json({
                "running": True,
                "pid": os.getpid(),
                "start_time": start_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "last_poll_time": None,
                "progress_pct": 0.0,
                "error_count": error_count,
                "trading": trading_flag,
                "paused": False,
                "stop_requested": False,
            }, STATUS_PATH)

        if loop_once:
            break
        # update progress (elapsed over interval)
        elapsed = (datetime.now() - now).total_seconds()
        pct = max(0.0, min(100.0, (elapsed / poll_interval) * 100.0))
        try:
            cur = json.loads(STATUS_PATH.read_text(encoding="utf-8"))
        except Exception:
            cur = {}
        cur.update({
            "progress_pct": pct,
        })
        _atomic_write_json(cur, STATUS_PATH)
        time.sleep(poll_interval)


def read_latest_snapshot() -> pd.DataFrame:
    if LATEST_PATH.exists():
        try:
            return pd.read_csv(LATEST_PATH)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def read_service_status() -> Dict:
    if STATUS_PATH.exists():
        try:
            return json.loads(STATUS_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def set_service_control(paused: bool | None = None, stop: bool | None = None) -> None:
    ctrl = _read_control()
    if paused is not None:
        ctrl["paused"] = bool(paused)
    if stop is not None:
        ctrl["stop"] = bool(stop)
    _write_control(ctrl)


if __name__ == "__main__":
    # Allow running as a standalone background service
    run_service()