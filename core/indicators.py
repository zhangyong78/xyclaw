from __future__ import annotations

import pandas as pd


def ema(series: pd.Series, span: int) -> pd.Series:
    return series.astype(float).ewm(span=span, adjust=False).mean()


def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    high_f = high.astype(float)
    low_f = low.astype(float)
    close_f = close.astype(float)
    prev_close = close_f.shift(1)
    true_range = pd.concat(
        [
            high_f - low_f,
            (high_f - prev_close).abs(),
            (low_f - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return true_range.ewm(alpha=1 / max(period, 1), adjust=False).mean()
