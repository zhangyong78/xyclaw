from __future__ import annotations

import pandas as pd


DRAW_COLS = [
    "max_drawdown_pct",
    "max_drawdown_amount",
    "intrabar_drawdown_pct",
    "rolling_30d_drawdown_pct",
    "rolling_90d_drawdown_pct",
    "drawdown_duration_days",
    "time_under_water_pct",
    "ulcer_index",
    "cdar_95_pct",
]


def _scale(series: pd.Series, higher_is_better: bool) -> pd.Series:
    clean = series.astype(float)
    if clean.nunique(dropna=False) <= 1:
        return pd.Series([50.0] * len(clean), index=clean.index)

    low = clean.min()
    high = clean.max()
    scaled = (clean - low) / (high - low)
    if not higher_is_better:
        scaled = 1.0 - scaled
    return scaled * 100


def score_period_results(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)

    df["return_score"] = pd.concat(
        [
            _scale(df["net_return_pct"], higher_is_better=True),
            _scale(df["profit_factor"].clip(upper=10), higher_is_better=True),
            _scale(df["win_rate"], higher_is_better=True),
        ],
        axis=1,
    ).mean(axis=1)

    draw_scores = pd.concat([_scale(df[col], higher_is_better=False) for col in DRAW_COLS], axis=1)
    draw_scores.columns = DRAW_COLS
    df["drawdown_score"] = draw_scores.mean(axis=1)

    trade_count_score = _scale(df["trade_count"].clip(upper=200), higher_is_better=True)
    trade_freq_score = _scale(df["avg_trades_per_month"].clip(upper=20), higher_is_better=True)
    df["trade_score"] = (trade_count_score + trade_freq_score) / 2.0

    sample_penalty = df["trade_count"].map(
        lambda count: 0.05 if count == 0 else 0.35 if count < 10 else 0.60 if count < 30 else 1.0
    )
    base_score = (
        df["return_score"] * 0.30
        + df["drawdown_score"] * 0.50
        + df["trade_score"] * 0.20
    )
    df["final_score"] = base_score * sample_penalty
    df["rank"] = df["final_score"].rank(method="dense", ascending=False).astype(int)
    return df.sort_values(["final_score", "net_return_pct"], ascending=[False, False]).reset_index(drop=True)
