from __future__ import annotations

import pandas as pd


FACTOR_KEY = "bias_close_ma"
FACTOR_NAME = "收盘/MA偏离"
PARAMS = [
    {"key": "window", "name": "MA周期", "type": "int", "default": 20, "min": 2, "max": 240},
]


def compute(df: pd.DataFrame, *, window: int = 20) -> pd.Series:
    close = pd.to_numeric(df["close"], errors="coerce")
    ma = close.rolling(window=window, min_periods=window).mean()
    return close / ma - 1

