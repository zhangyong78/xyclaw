from __future__ import annotations

import math

import pandas as pd


def _max_underwater_duration_days(drawdown: pd.Series) -> float:
    underwater = drawdown < 0
    if underwater.empty or not underwater.any():
        return 0.0

    group_ids = (underwater != underwater.shift(fill_value=False)).cumsum()
    max_days = 0.0
    for _, segment in underwater.groupby(group_ids):
        if not bool(segment.iloc[0]):
            continue
        start = segment.index[0]
        end = segment.index[-1]
        days = (end - start).total_seconds() / 86_400
        max_days = max(max_days, days)
    return max_days


def compute_drawdown_metrics(equity_df: pd.DataFrame) -> dict[str, float]:
    equity = equity_df["equity"].astype(float)
    equity_low = equity_df["equity_low"].astype(float)

    peak_equity = equity.cummax()
    drawdown = equity / peak_equity - 1.0
    drawdown_amount = equity - peak_equity

    intrabar_drawdown = equity_low / peak_equity - 1.0
    rolling_peak_30 = equity.rolling("30D", min_periods=1).max()
    rolling_peak_90 = equity.rolling("90D", min_periods=1).max()
    rolling_dd_30 = equity / rolling_peak_30 - 1.0
    rolling_dd_90 = equity / rolling_peak_90 - 1.0

    negative_dd_pct = (-drawdown.clip(upper=0.0)) * 100
    cdar_threshold = negative_dd_pct.quantile(0.95) if not negative_dd_pct.empty else 0.0
    cdar_bucket = negative_dd_pct[negative_dd_pct >= cdar_threshold]
    cdar_95_pct = float(cdar_bucket.mean()) if not cdar_bucket.empty else 0.0

    ulcer_index = math.sqrt(float((negative_dd_pct.pow(2)).mean())) if not negative_dd_pct.empty else 0.0

    return {
        "max_drawdown_pct": float((-drawdown.min()) * 100),
        "max_drawdown_amount": float(-drawdown_amount.min()),
        "intrabar_drawdown_pct": float((-intrabar_drawdown.min()) * 100),
        "rolling_30d_drawdown_pct": float((-rolling_dd_30.min()) * 100),
        "rolling_90d_drawdown_pct": float((-rolling_dd_90.min()) * 100),
        "drawdown_duration_days": _max_underwater_duration_days(drawdown),
        "time_under_water_pct": float((drawdown < 0).mean() * 100),
        "ulcer_index": ulcer_index,
        "cdar_95_pct": cdar_95_pct,
    }

