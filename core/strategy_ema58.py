from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from core.indicators import ema


@dataclass(slots=True)
class SignalSnapshot:
    ts: pd.Timestamp
    bar_index: int
    close: float
    ema_fast: float
    ema_slow: float
    cross_up: bool
    cross_down: bool


@dataclass(slots=True)
class ExitEvaluation:
    should_exit: bool
    reason: str | None
    held_bars: int


def add_strategy_columns(bars: pd.DataFrame, fast_ema: int, slow_ema: int) -> pd.DataFrame:
    df = bars.sort_index().copy()
    df["ema_fast"] = ema(df["close"], fast_ema)
    df["ema_slow"] = ema(df["close"], slow_ema)
    df["cross_up"] = (df["ema_fast"].shift(1) <= df["ema_slow"].shift(1)) & (df["ema_fast"] > df["ema_slow"])
    df["cross_down"] = (df["ema_fast"].shift(1) >= df["ema_slow"].shift(1)) & (df["ema_fast"] < df["ema_slow"])
    return df


def latest_signal_snapshot(signal_frame: pd.DataFrame) -> SignalSnapshot:
    if signal_frame.empty:
        raise ValueError("signal_frame is empty")

    ts = signal_frame.index[-1]
    row = signal_frame.iloc[-1]
    return SignalSnapshot(
        ts=ts,
        bar_index=len(signal_frame) - 1,
        close=float(row["close"]),
        ema_fast=float(row["ema_fast"]),
        ema_slow=float(row["ema_slow"]),
        cross_up=bool(row["cross_up"]),
        cross_down=bool(row["cross_down"]),
    )


def find_signal_index(signal_frame: pd.DataFrame, signal_ts: str | pd.Timestamp) -> int | None:
    if signal_ts is None:
        return None
    ts = pd.Timestamp(signal_ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    matches = signal_frame.index.get_indexer([ts])
    index = int(matches[0])
    return None if index < 0 else index


def find_latest_cross_up_ts(signal_frame: pd.DataFrame) -> pd.Timestamp | None:
    cross_ups = signal_frame.index[signal_frame["cross_up"]]
    if len(cross_ups) == 0:
        return None
    return cross_ups[-1]


def evaluate_long_exit(
    signal_frame: pd.DataFrame,
    entry_signal_ts: str | pd.Timestamp,
    hold_bars: int,
    *,
    stop_on_slow_ema: bool = True,
    exit_on_dead_cross: bool = True,
) -> ExitEvaluation:
    if signal_frame.empty:
        return ExitEvaluation(False, None, 0)

    latest = latest_signal_snapshot(signal_frame)
    entry_index = find_signal_index(signal_frame, entry_signal_ts)
    if entry_index is None:
        if exit_on_dead_cross and latest.cross_down:
            return ExitEvaluation(True, "dead_cross", 0)
        return ExitEvaluation(False, None, 0)

    held_bars = latest.bar_index - entry_index
    if exit_on_dead_cross and latest.cross_down:
        return ExitEvaluation(True, "dead_cross", held_bars)

    if stop_on_slow_ema and latest.close < latest.ema_slow:
        return ExitEvaluation(True, "slow_ema_stop", held_bars)

    if hold_bars > 0 and held_bars >= hold_bars:
        return ExitEvaluation(True, "time_exit", held_bars)

    return ExitEvaluation(False, None, held_bars)
