from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Callable, Any

import pandas as pd

from .config import DEFAULT_CONFIG


class CacheManager:
    def __init__(self, base_dir: Path | None = None):
        self.base_dir = base_dir or DEFAULT_CONFIG.cache_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _key_to_path(self, key: str) -> Path:
        safe = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self.base_dir / f"{safe}.pkl"

    def load_df(self, key: str) -> pd.DataFrame | None:
        path = self._key_to_path(key)
        if path.exists():
            try:
                return pd.read_pickle(path)
            except Exception:
                return None
        return None

    def save_df(self, key: str, df: pd.DataFrame) -> None:
        path = self._key_to_path(key)
        try:
            df.to_pickle(path)
        except Exception:
            pass


cache = CacheManager()


def cacheable_df(key_builder: Callable[..., str]):
    """简单的DataFrame磁盘缓存装饰器。

    key_builder: 根据函数入参构造缓存key的函数
    """

    def decorator(func: Callable[..., pd.DataFrame]):
        def wrapper(*args: Any, use_cache: bool = True, **kwargs: Any) -> pd.DataFrame:
            key = key_builder(*args, **kwargs)
            if use_cache:
                cached = cache.load_df(key)
                if cached is not None and not cached.empty:
                    return cached
            df = func(*args, **kwargs)
            if use_cache and df is not None and not df.empty:
                cache.save_df(key, df)
            return df

        return wrapper

    return decorator