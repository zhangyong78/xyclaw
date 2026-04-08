from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class BacktestConfig:
    symbol: str
    initial_cash: float = 10_000.0
    risk_amount: float = 100.0
    fixed_position_qty: float | None = None
    fee_bps: float = 2.8
    slippage_bps: float = 2.0
    fast_ema: int = 21
    slow_ema: int = 55
    atr_period: int = 10
    hold_bars: int = 0
    max_allocation_pct: float = 0.95
    signal_side: str = "long_only"
    stop_on_slow_ema: bool = True
    exit_on_dead_cross: bool = True
    stop_loss_atr_multiplier: float | None = None
    take_profit_r_multiple: float | None = None
    stop_first_on_ambiguous_bar: bool = True


@dataclass(slots=True)
class PendingOrder:
    action: str
    signal_index: int
    signal_ts: object
    reason: str
    stop_price: float
    atr_value: float | None = None


@dataclass(slots=True)
class Position:
    side: str
    entry_ts: object
    entry_index: int
    signal_index: int
    entry_price: float
    qty: float
    entry_fee: float
    stop_price: float
    take_profit_price: float | None = None


@dataclass(slots=True)
class Trade:
    entry_ts: object
    exit_ts: object
    side: str
    qty: float
    entry_price: float
    exit_price: float
    pnl: float
    pnl_pct: float
    fees: float
    exit_reason: str
    hold_hours: float
