from __future__ import annotations

from dataclasses import asdict
from typing import Any

import pandas as pd

from backtest.costs import apply_slippage, calc_fee
from backtest.metrics_drawdown import compute_drawdown_metrics
from backtest.metrics_trade import compute_trade_metrics
from core.indicators import atr
from core.risk_engine import calculate_position_size
from core.strategy_ema58 import add_strategy_columns
from core.types import BacktestConfig, PendingOrder, Position, Trade


class BacktestEngine:
    def __init__(self, config: BacktestConfig) -> None:
        self.config = config

    def run(self, bars: pd.DataFrame, period: str) -> dict[str, Any]:
        df = add_strategy_columns(bars.copy(), self.config.fast_ema, self.config.slow_ema)
        df["atr"] = atr(df["high"], df["low"], df["close"], self.config.atr_period)

        cash = self.config.initial_cash
        position: Position | None = None
        pending: PendingOrder | None = None
        trades: list[Trade] = []
        equity_rows: list[dict[str, Any]] = []
        warmup = max(self.config.slow_ema, self.config.atr_period)

        for i, (ts, row) in enumerate(df.iterrows()):
            if pending is not None:
                if position is None and pending.action in {"buy", "sell"}:
                    entry_side = "long" if pending.action == "buy" else "short"
                    entry_price = apply_slippage(float(row["open"]), pending.action, self.config.slippage_bps)
                    stop_price, take_profit_price = self._resolve_entry_brackets(
                        entry_price=entry_price,
                        pending=pending,
                        position_side=entry_side,
                    )
                    configured_fixed_qty = self.config.fixed_position_qty
                    if configured_fixed_qty is not None and float(configured_fixed_qty) > 0.0:
                        qty = float(configured_fixed_qty)
                    else:
                        qty = calculate_position_size(
                            entry_price=entry_price,
                            stop_price=stop_price,
                            risk_amount=self.config.risk_amount,
                            available_cash=cash,
                            max_allocation_pct=self.config.max_allocation_pct,
                            side=entry_side,
                        )
                    if qty > 0:
                        entry_fee = calc_fee(entry_price * qty, self.config.fee_bps)
                        if entry_side == "long":
                            cash -= entry_price * qty + entry_fee
                        else:
                            cash += entry_price * qty - entry_fee
                        position = Position(
                            side=entry_side,
                            entry_ts=ts,
                            entry_index=i,
                            signal_index=pending.signal_index,
                            entry_price=entry_price,
                            qty=qty,
                            entry_fee=entry_fee,
                            stop_price=stop_price,
                            take_profit_price=take_profit_price,
                        )
                    pending = None
                elif position is not None and pending.action == self._exit_order_action(position.side):
                    exit_price = apply_slippage(float(row["open"]), pending.action, self.config.slippage_bps)
                    cash, trade = self._close_position(cash=cash, position=position, exit_ts=ts, exit_price=exit_price, exit_reason=pending.reason)
                    trades.append(trade)
                    position = None
                    pending = None

            if position is not None:
                intrabar_exit = self._resolve_intrabar_exit(row=row, position=position)
                if intrabar_exit is not None:
                    exit_reason, exit_price = intrabar_exit
                    cash, trade = self._close_position(cash=cash, position=position, exit_ts=ts, exit_price=exit_price, exit_reason=exit_reason)
                    trades.append(trade)
                    position = None
                    pending = None

            close_price = float(row["close"])
            low_price = float(row["low"])
            high_price = float(row["high"])
            if position is None:
                equity = cash
                equity_low = cash
            elif position.side == "short":
                equity = cash - position.qty * close_price
                equity_low = cash - position.qty * high_price
            else:
                equity = cash + position.qty * close_price
                equity_low = cash + position.qty * low_price
            equity_rows.append(
                {
                    "ts": ts,
                    "period": period,
                    "cash": cash,
                    "position_qty": position.qty if position else 0.0,
                    "close_price": close_price,
                    "equity": equity,
                    "equity_low": equity_low,
                }
            )

            if i < warmup or i == len(df) - 1 or pending is not None:
                continue

            cross_up = bool(row["cross_up"])
            cross_down = bool(row["cross_down"])

            if position is None:
                if self._allows_long() and cross_up:
                    pending = PendingOrder(
                        action="buy",
                        signal_index=i,
                        signal_ts=ts,
                        reason="golden_cross",
                        stop_price=float(row["ema_slow"]),
                        atr_value=self._safe_float(row.get("atr")),
                    )
                    continue
                if self._allows_short() and cross_down:
                    pending = PendingOrder(
                        action="sell",
                        signal_index=i,
                        signal_ts=ts,
                        reason="dead_cross",
                        stop_price=float(row["ema_slow"]),
                        atr_value=self._safe_float(row.get("atr")),
                    )
                    continue
                continue

            held_bars = i - position.signal_index
            slow_ema_value = float(row["ema_slow"])
            slow_ema_stop = self.config.stop_on_slow_ema and (
                close_price > slow_ema_value if position.side == "short" else close_price < slow_ema_value
            )
            time_exit = self.config.hold_bars > 0 and held_bars >= self.config.hold_bars

            if position.side == "short":
                if self.config.exit_on_dead_cross and cross_up:
                    pending = PendingOrder("buy", i, ts, "golden_cross_cover", 0.0)
                elif slow_ema_stop:
                    pending = PendingOrder("buy", i, ts, "slow_ema_stop", 0.0)
                elif time_exit:
                    pending = PendingOrder("buy", i, ts, "time_exit", 0.0)
            else:
                if self.config.exit_on_dead_cross and cross_down:
                    pending = PendingOrder("sell", i, ts, "dead_cross", 0.0)
                elif slow_ema_stop:
                    pending = PendingOrder("sell", i, ts, "slow_ema_stop", 0.0)
                elif time_exit:
                    pending = PendingOrder("sell", i, ts, "time_exit", 0.0)

        if position is not None:
            final_ts = df.index[-1]
            final_close = apply_slippage(float(df.iloc[-1]["close"]), self._exit_order_action(position.side), self.config.slippage_bps)
            cash, trade = self._close_position(cash=cash, position=position, exit_ts=final_ts, exit_price=final_close, exit_reason="end_of_data")
            trades.append(trade)
            equity_rows[-1]["cash"] = cash
            equity_rows[-1]["position_qty"] = 0.0
            equity_rows[-1]["equity"] = cash
            equity_rows[-1]["equity_low"] = cash

        trade_df = pd.DataFrame([asdict(trade) for trade in trades])
        equity_df = pd.DataFrame(equity_rows).set_index("ts")

        drawdown_metrics = compute_drawdown_metrics(equity_df)
        trade_metrics = compute_trade_metrics(
            trade_df=trade_df,
            initial_cash=self.config.initial_cash,
            final_equity=float(equity_df["equity"].iloc[-1]),
            start_ts=df.index[0],
            end_ts=df.index[-1],
        )

        summary = {
            "period": period,
            "bars": len(df),
            **trade_metrics,
            **drawdown_metrics,
        }

        return {
            "summary": summary,
            "trades": trade_df,
            "equity": equity_df,
            "signal_frame": df,
        }

    def _resolve_entry_brackets(
        self,
        *,
        entry_price: float,
        pending: PendingOrder,
        position_side: str,
    ) -> tuple[float, float | None]:
        stop_price = float(pending.stop_price)
        take_profit_price: float | None = None
        stop_mult = float(self.config.stop_loss_atr_multiplier or 0.0)
        atr_value = float(pending.atr_value or 0.0)

        if stop_mult > 0.0 and atr_value > 0.0:
            stop_distance = atr_value * stop_mult
            stop_price = entry_price + stop_distance if position_side == "short" else entry_price - stop_distance
            take_profit_multiple = float(self.config.take_profit_r_multiple or 0.0)
            if take_profit_multiple > 0.0:
                take_profit_price = (
                    entry_price - stop_distance * take_profit_multiple
                    if position_side == "short"
                    else entry_price + stop_distance * take_profit_multiple
                )

        return stop_price, take_profit_price

    def _resolve_intrabar_exit(self, *, row: pd.Series, position: Position) -> tuple[str, float] | None:
        stop_mult = float(self.config.stop_loss_atr_multiplier or 0.0)
        if stop_mult <= 0.0:
            return None

        bar_low = float(row["low"])
        bar_high = float(row["high"])
        stop_price = float(position.stop_price)
        take_profit_price = position.take_profit_price

        if position.side == "short":
            exit_side = "buy"
            stop_hit = stop_price > 0.0 and bar_high >= stop_price
            take_profit_hit = take_profit_price is not None and bar_low <= float(take_profit_price)
        else:
            exit_side = "sell"
            stop_hit = stop_price > 0.0 and bar_low <= stop_price
            take_profit_hit = take_profit_price is not None and bar_high >= float(take_profit_price)

        if not stop_hit and not take_profit_hit:
            return None

        if stop_hit and take_profit_hit:
            if self.config.stop_first_on_ambiguous_bar:
                return "atr_stop_loss", apply_slippage(stop_price, exit_side, self.config.slippage_bps)
            return "atr_take_profit", apply_slippage(float(take_profit_price), exit_side, self.config.slippage_bps)

        if stop_hit:
            return "atr_stop_loss", apply_slippage(stop_price, exit_side, self.config.slippage_bps)

        return "atr_take_profit", apply_slippage(float(take_profit_price), exit_side, self.config.slippage_bps)

    def _close_position(
        self,
        *,
        cash: float,
        position: Position,
        exit_ts,
        exit_price: float,
        exit_reason: str,
    ) -> tuple[float, Trade]:
        exit_fee = calc_fee(exit_price * position.qty, self.config.fee_bps)
        if position.side == "short":
            next_cash = cash - exit_price * position.qty - exit_fee
            pnl = (position.entry_price - exit_price) * position.qty - position.entry_fee - exit_fee
        else:
            next_cash = cash + exit_price * position.qty - exit_fee
            pnl = (exit_price - position.entry_price) * position.qty - position.entry_fee - exit_fee
        pnl_pct = pnl / (position.entry_price * position.qty) if position.entry_price > 0 else 0.0
        trade = Trade(
            entry_ts=position.entry_ts,
            exit_ts=exit_ts,
            side=position.side,
            qty=position.qty,
            entry_price=position.entry_price,
            exit_price=exit_price,
            pnl=pnl,
            pnl_pct=pnl_pct,
            fees=position.entry_fee + exit_fee,
            exit_reason=exit_reason,
            hold_hours=(exit_ts - position.entry_ts).total_seconds() / 3600,
        )
        return next_cash, trade

    def _allows_long(self) -> bool:
        return self.config.signal_side in {"long_only", "both"}

    def _allows_short(self) -> bool:
        return self.config.signal_side in {"short_only", "both"}

    @staticmethod
    def _exit_order_action(position_side: str) -> str:
        return "buy" if position_side == "short" else "sell"

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        if pd.isna(number):
            return None
        return number
