from __future__ import annotations

import unittest

import pandas as pd

from core.strategy_ema58 import latest_signal_snapshot
from live.config import LiveRunConfig
from live.state_store import LiveState, LiveTradeRecord
from live.trader import LiveTrader


class _DummyPublicClient:
    def __init__(self, recent_frame: pd.DataFrame) -> None:
        self.recent_frame = recent_frame

    def download_recent(
        self,
        inst_id: str,
        bar: str,
        limit: int = 2,
        *,
        include_unconfirmed: bool = True,
    ) -> pd.DataFrame:
        return self.recent_frame.copy()


def _build_signal_frame(rows: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(rows).set_index("ts")
    frame.index = pd.DatetimeIndex(frame.index)
    return frame


class LiveTraderTests(unittest.TestCase):
    def test_missed_cross_does_not_catch_up_on_later_bar(self) -> None:
        trader = LiveTrader(LiveRunConfig(symbol="BTC-USDT-SWAP", signal_symbol="BTC-USDT-SWAP", period="1H", signal_side="long_only"))
        signal_frame = _build_signal_frame(
            [
                {
                    "ts": pd.Timestamp("2026-04-21T12:00:00Z"),
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 1.0,
                    "turnover": 100.0,
                    "ema_fast": 99.0,
                    "ema_slow": 100.0,
                    "cross_up": False,
                    "cross_down": False,
                    "atr": 2.0,
                },
                {
                    "ts": pd.Timestamp("2026-04-21T13:00:00Z"),
                    "open": 100.0,
                    "high": 104.0,
                    "low": 99.0,
                    "close": 103.0,
                    "volume": 1.0,
                    "turnover": 103.0,
                    "ema_fast": 101.0,
                    "ema_slow": 100.0,
                    "cross_up": True,
                    "cross_down": False,
                    "atr": 2.5,
                },
                {
                    "ts": pd.Timestamp("2026-04-21T14:00:00Z"),
                    "open": 103.0,
                    "high": 104.5,
                    "low": 102.5,
                    "close": 104.0,
                    "volume": 1.0,
                    "turnover": 104.0,
                    "ema_fast": 101.8,
                    "ema_slow": 100.6,
                    "cross_up": False,
                    "cross_down": False,
                    "atr": 2.4,
                },
            ]
        )
        snapshot = latest_signal_snapshot(signal_frame)
        state = LiveState(last_sell_signal_ts="2026-04-21T10:00:00+00:00")

        pending = trader._select_pending_entry_signal(
            signal_frame,
            state=state,
            snapshot=snapshot,
            allow_resume_catchup=True,
            live_trades=[],
        )

        self.assertIsNone(pending)

    def test_does_not_reenter_same_cross_after_stop_loss(self) -> None:
        trader = LiveTrader(LiveRunConfig(symbol="BTC-USDT-SWAP", signal_symbol="BTC-USDT-SWAP", period="1H", signal_side="long_only"))
        signal_frame = _build_signal_frame(
            [
                {
                    "ts": pd.Timestamp("2026-04-21T12:00:00Z"),
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 1.0,
                    "turnover": 100.0,
                    "ema_fast": 99.0,
                    "ema_slow": 100.0,
                    "cross_up": False,
                    "cross_down": False,
                    "atr": 2.0,
                },
                {
                    "ts": pd.Timestamp("2026-04-21T13:00:00Z"),
                    "open": 100.0,
                    "high": 104.0,
                    "low": 99.0,
                    "close": 103.0,
                    "volume": 1.0,
                    "turnover": 103.0,
                    "ema_fast": 101.0,
                    "ema_slow": 100.0,
                    "cross_up": True,
                    "cross_down": False,
                    "atr": 2.5,
                },
                {
                    "ts": pd.Timestamp("2026-04-21T14:00:00Z"),
                    "open": 103.0,
                    "high": 103.5,
                    "low": 98.0,
                    "close": 99.0,
                    "volume": 1.0,
                    "turnover": 99.0,
                    "ema_fast": 100.5,
                    "ema_slow": 100.0,
                    "cross_up": False,
                    "cross_down": False,
                    "atr": 2.8,
                },
            ]
        )
        snapshot = latest_signal_snapshot(signal_frame)
        state = LiveState(last_buy_signal_ts="2026-04-21T13:00:00+00:00")
        trades = [
            LiveTradeRecord(
                trade_id="t-1",
                side="long",
                entry_ts="2026-04-21T13:00:00+00:00",
                entry_price=103.0,
                entry_size=1.0,
                exit_ts="2026-04-21T13:20:00+00:00",
                exit_price=100.0,
                exit_reason="atr_stop_loss",
                status="closed",
            )
        ]

        pending = trader._select_pending_entry_signal(
            signal_frame,
            state=state,
            snapshot=snapshot,
            allow_resume_catchup=True,
            live_trades=trades,
        )

        self.assertIsNone(pending)

    def test_still_catches_new_cross_after_processed_signal(self) -> None:
        trader = LiveTrader(LiveRunConfig(symbol="BTC-USDT-SWAP", signal_symbol="BTC-USDT-SWAP", period="1H", signal_side="both"))
        signal_frame = _build_signal_frame(
            [
                {
                    "ts": pd.Timestamp("2026-04-21T12:00:00Z"),
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 1.0,
                    "turnover": 100.0,
                    "ema_fast": 99.0,
                    "ema_slow": 100.0,
                    "cross_up": False,
                    "cross_down": False,
                    "atr": 2.0,
                },
                {
                    "ts": pd.Timestamp("2026-04-21T13:00:00Z"),
                    "open": 100.0,
                    "high": 104.0,
                    "low": 99.0,
                    "close": 103.0,
                    "volume": 1.0,
                    "turnover": 103.0,
                    "ema_fast": 101.0,
                    "ema_slow": 100.0,
                    "cross_up": True,
                    "cross_down": False,
                    "atr": 2.5,
                },
                {
                    "ts": pd.Timestamp("2026-04-21T14:00:00Z"),
                    "open": 103.0,
                    "high": 103.5,
                    "low": 98.0,
                    "close": 99.0,
                    "volume": 1.0,
                    "turnover": 99.0,
                    "ema_fast": 99.0,
                    "ema_slow": 100.0,
                    "cross_up": False,
                    "cross_down": True,
                    "atr": 2.8,
                },
                {
                    "ts": pd.Timestamp("2026-04-21T15:00:00Z"),
                    "open": 99.0,
                    "high": 105.0,
                    "low": 98.5,
                    "close": 104.0,
                    "volume": 1.0,
                    "turnover": 104.0,
                    "ema_fast": 101.5,
                    "ema_slow": 100.2,
                    "cross_up": True,
                    "cross_down": False,
                    "atr": 3.0,
                },
            ]
        )
        snapshot = latest_signal_snapshot(signal_frame)
        state = LiveState(
            last_buy_signal_ts="2026-04-21T13:00:00+00:00",
            last_sell_signal_ts="2026-04-21T14:00:00+00:00",
        )

        pending = trader._select_pending_entry_signal(
            signal_frame,
            state=state,
            snapshot=snapshot,
            allow_resume_catchup=True,
            live_trades=[],
        )

        self.assertIsNotNone(pending)
        self.assertEqual("buy", pending.action)
        self.assertEqual("long", pending.trade_side)
        self.assertEqual("2026-04-21T15:00:00+00:00", pending.signal_ts)

    def test_current_cross_still_allows_entry(self) -> None:
        trader = LiveTrader(LiveRunConfig(symbol="BTC-USDT-SWAP", signal_symbol="BTC-USDT-SWAP", period="1H", signal_side="both"))
        signal_frame = _build_signal_frame(
            [
                {
                    "ts": pd.Timestamp("2026-04-21T12:00:00Z"),
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 1.0,
                    "turnover": 100.0,
                    "ema_fast": 99.0,
                    "ema_slow": 100.0,
                    "cross_up": False,
                    "cross_down": False,
                    "atr": 2.0,
                },
                {
                    "ts": pd.Timestamp("2026-04-21T13:00:00Z"),
                    "open": 100.0,
                    "high": 104.0,
                    "low": 99.0,
                    "close": 103.0,
                    "volume": 1.0,
                    "turnover": 103.0,
                    "ema_fast": 101.0,
                    "ema_slow": 100.0,
                    "cross_up": True,
                    "cross_down": False,
                    "atr": 2.5,
                },
            ]
        )
        snapshot = latest_signal_snapshot(signal_frame)
        state = LiveState(last_sell_signal_ts="2026-04-21T10:00:00+00:00")

        pending = trader._select_pending_entry_signal(
            signal_frame,
            state=state,
            snapshot=snapshot,
            allow_resume_catchup=True,
            live_trades=[],
        )

        self.assertIsNotNone(pending)
        self.assertEqual("buy", pending.action)
        self.assertEqual("2026-04-21T13:00:00+00:00", pending.signal_ts)

    def test_preview_frame_appends_unconfirmed_candle_without_new_signal_marker(self) -> None:
        trader = LiveTrader(LiveRunConfig(symbol="BTC-USDT-SWAP", signal_symbol="BTC-USDT-SWAP", period="1H", signal_side="long_only"))
        signal_frame = _build_signal_frame(
            [
                {
                    "ts": pd.Timestamp("2026-04-21T12:00:00Z"),
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 1.0,
                    "turnover": 100.0,
                    "ema_fast": 99.0,
                    "ema_slow": 100.0,
                    "cross_up": False,
                    "cross_down": False,
                    "atr": 2.0,
                },
                {
                    "ts": pd.Timestamp("2026-04-21T13:00:00Z"),
                    "open": 100.0,
                    "high": 104.0,
                    "low": 99.0,
                    "close": 103.0,
                    "volume": 1.0,
                    "turnover": 103.0,
                    "ema_fast": 101.0,
                    "ema_slow": 100.0,
                    "cross_up": True,
                    "cross_down": False,
                    "atr": 2.5,
                },
                {
                    "ts": pd.Timestamp("2026-04-21T14:00:00Z"),
                    "open": 103.0,
                    "high": 104.0,
                    "low": 102.0,
                    "close": 103.5,
                    "volume": 1.0,
                    "turnover": 103.5,
                    "ema_fast": 102.0,
                    "ema_slow": 100.8,
                    "cross_up": False,
                    "cross_down": False,
                    "atr": 2.6,
                },
            ]
        )
        recent = pd.DataFrame(
            [
                {
                    "ts": pd.Timestamp("2026-04-21T14:00:00Z"),
                    "open": 103.0,
                    "high": 104.0,
                    "low": 102.0,
                    "close": 103.5,
                    "volume": 1.0,
                    "turnover": 103.5,
                },
                {
                    "ts": pd.Timestamp("2026-04-21T15:00:00Z"),
                    "open": 103.5,
                    "high": 106.0,
                    "low": 103.0,
                    "close": 105.5,
                    "volume": 1.2,
                    "turnover": 126.6,
                },
            ]
        ).set_index("ts")
        trader.public_client = _DummyPublicClient(recent)

        preview = trader._build_live_preview_signal_frame(signal_frame, signal_symbol="BTC-USDT-SWAP")

        self.assertEqual(4, len(preview))
        self.assertEqual(pd.Timestamp("2026-04-21T15:00:00Z"), preview.index[-1])
        self.assertFalse(bool(preview.iloc[-1]["cross_up"]))
        self.assertFalse(bool(preview.iloc[-1]["cross_down"]))


if __name__ == "__main__":
    unittest.main()
