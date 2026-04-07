from __future__ import annotations

from pathlib import Path

import pandas as pd

DEFAULT_PERIODS = ["1H", "2H", "3H", "4H", "6H", "8H", "12H", "16H", "24H"]

PANDAS_FREQ = {
    "1H": "1H",
    "2H": "2H",
    "3H": "3H",
    "4H": "4H",
    "6H": "6H",
    "8H": "8H",
    "12H": "12H",
    "16H": "16H",
    "24H": "1D",
}


def load_1h_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["ts"])
    df = df.set_index("ts").sort_index()
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    return df[["open", "high", "low", "close", "volume", "turnover"]]


def resample_bars(df: pd.DataFrame, period: str) -> pd.DataFrame:
    if period not in PANDAS_FREQ:
        raise ValueError(f"Unsupported period: {period}")

    source = df.sort_index()
    if period == "1H":
        return source.copy()

    agg = (
        source.resample(PANDAS_FREQ[period], label="right", closed="right")
        .agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
                "turnover": "sum",
            }
        )
        .dropna(subset=["open", "high", "low", "close"])
    )
    return agg
