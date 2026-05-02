from __future__ import annotations

import unittest

from app.desktop_app import TradingDesktopApp


class LiveStrategySlotTests(unittest.TestCase):
    def _build_app(self) -> TradingDesktopApp:
        app = TradingDesktopApp.__new__(TradingDesktopApp)
        app.live_strategy_records = {}
        return app

    def test_display_slots_compact_after_middle_strategy_removed(self) -> None:
        app = self._build_app()
        app.live_strategy_records = {
            "live-strategy-1": {"strategy": "策略1"},
            "live-strategy-3": {"strategy": "策略3"},
        }

        app._refresh_live_strategy_display_labels()

        self.assertEqual("策略1", app.live_strategy_records["live-strategy-1"]["strategy"])
        self.assertEqual("策略2", app.live_strategy_records["live-strategy-3"]["strategy"])
        self.assertEqual("2", app._display_slot_for_strategy_id("live-strategy-3"))
        self.assertEqual("live-strategy-3", app._strategy_id_for_display_slot("2"))

    def test_new_strategy_appends_after_compacted_slots(self) -> None:
        app = self._build_app()
        app.live_strategy_records = {
            "live-strategy-1": {"strategy": "策略1"},
            "live-strategy-3": {"strategy": "策略2"},
            "live-strategy-2": {"strategy": "策略3"},
        }

        app._refresh_live_strategy_display_labels()

        self.assertEqual("策略1", app.live_strategy_records["live-strategy-1"]["strategy"])
        self.assertEqual("策略2", app.live_strategy_records["live-strategy-3"]["strategy"])
        self.assertEqual("策略3", app.live_strategy_records["live-strategy-2"]["strategy"])
        self.assertEqual("live-strategy-2", app._strategy_id_for_display_slot(3))


if __name__ == "__main__":
    unittest.main()
