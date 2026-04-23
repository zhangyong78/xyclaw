from __future__ import annotations

import unittest

import pandas as pd

from backtest.engine import BacktestEngine
from backtest.costs import calc_fee
from core.types import BacktestConfig, Position
from live.config import LiveRunConfig
from live.state_store import LiveTradeRecord
from live.trader import LiveTrader


def _frame(rows: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(rows).set_index("ts")
    frame.index = pd.DatetimeIndex(frame.index)
    return frame


class TrendModeTests(unittest.TestCase):
    def test_backtest_trend_mode_moves_stop_after_one_r_with_fee_buffer(self) -> None:
        engine = BacktestEngine(
            BacktestConfig(
                symbol="BTC-USDT-SWAP",
                entry_mode="trend",
                stop_loss_atr_multiplier=1.0,
                take_profit_r_multiple=2.0,
                fee_bps=2.8,
                slippage_bps=2.0,
            )
        )
        position = Position(
            side="long",
            entry_ts=pd.Timestamp("2026-04-23T12:00:00Z"),
            entry_index=0,
            signal_index=0,
            entry_price=100.0,
            qty=1.0,
            entry_fee=calc_fee(100.0, 2.8),
            stop_price=90.0,
            initial_stop_price=90.0,
            take_profit_price=None,
        )

        first_bar = pd.Series({"high": 110.0, "low": 101.0})
        self.assertIsNone(engine._resolve_intrabar_exit(row=first_bar, position=position))
        self.assertGreater(position.stop_price, 100.0)

        second_bar = pd.Series({"high": 103.0, "low": 100.05})
        reason, exit_price = engine._resolve_intrabar_exit(row=second_bar, position=position) or ("", 0.0)
        self.assertEqual("trend_stop_loss", reason)
        self.assertGreater(exit_price, 100.0)

    def test_live_trend_mode_exits_on_trailing_stop_instead_of_fixed_take_profit(self) -> None:
        trader = LiveTrader(
            LiveRunConfig(
                symbol="BTC-USDT-SWAP",
                signal_symbol="BTC-USDT-SWAP",
                period="1H",
                signal_side="long_only",
                entry_mode="trend",
                stop_loss_atr_multiplier=1.0,
                take_profit_r_multiple=2.0,
                fee_bps=2.8,
            )
        )
        signal_frame = _frame(
            [
                {
                    "ts": pd.Timestamp("2026-04-23T12:00:00Z"),
                    "open": 99.0,
                    "high": 100.0,
                    "low": 98.5,
                    "close": 99.0,
                    "volume": 1.0,
                    "turnover": 99.0,
                    "ema_fast": 98.0,
                    "ema_slow": 99.0,
                    "cross_up": False,
                    "cross_down": False,
                    "atr": 10.0,
                },
                {
                    "ts": pd.Timestamp("2026-04-23T13:00:00Z"),
                    "open": 99.0,
                    "high": 100.0,
                    "low": 98.8,
                    "close": 100.0,
                    "volume": 1.0,
                    "turnover": 100.0,
                    "ema_fast": 100.5,
                    "ema_slow": 99.5,
                    "cross_up": True,
                    "cross_down": False,
                    "atr": 10.0,
                },
                {
                    "ts": pd.Timestamp("2026-04-23T14:00:00Z"),
                    "open": 100.0,
                    "high": 110.0,
                    "low": 101.0,
                    "close": 109.0,
                    "volume": 1.0,
                    "turnover": 109.0,
                    "ema_fast": 105.0,
                    "ema_slow": 100.0,
                    "cross_up": False,
                    "cross_down": False,
                    "atr": 10.0,
                },
                {
                    "ts": pd.Timestamp("2026-04-23T15:00:00Z"),
                    "open": 109.0,
                    "high": 109.5,
                    "low": 100.05,
                    "close": 101.0,
                    "volume": 1.0,
                    "turnover": 101.0,
                    "ema_fast": 103.0,
                    "ema_slow": 100.5,
                    "cross_up": False,
                    "cross_down": False,
                    "atr": 10.0,
                },
            ]
        )
        open_trade = LiveTradeRecord(
            trade_id="live-1",
            side="long",
            entry_ts="2026-04-23T13:00:00+00:00",
            entry_price=100.0,
            entry_size=1.0,
            entry_fee=calc_fee(100.0, 2.8),
            status="open",
        )

        exit_eval = trader._evaluate_long_trend_exit(
            signal_frame,
            entry_signal_ts="2026-04-23T13:00:00+00:00",
            open_trade=open_trade,
        )

        self.assertTrue(exit_eval.should_exit)
        self.assertEqual("trend_stop_loss", exit_eval.reason)
        self.assertEqual(2, exit_eval.held_bars)


if __name__ == "__main__":
    unittest.main()
