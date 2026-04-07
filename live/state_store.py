from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(slots=True)
class LiveState:
    last_buy_signal_ts: str | None = None
    last_sell_signal_ts: str | None = None
    entry_signal_ts: str | None = None
    entry_side: str | None = None
    last_order_cl_ord_id: str | None = None
    strategy_started_at: str | None = None
    strategy_start_equity: float | None = None
    day_anchor: str | None = None
    day_start_equity: float | None = None


@dataclass(slots=True)
class LiveTradeRecord:
    trade_id: str
    side: str = "long"
    entry_ts: str = ""
    entry_price: float = 0.0
    entry_size: float = 0.0
    entry_fee: float = 0.0
    entry_order_id: str | None = None
    exit_ts: str | None = None
    exit_price: float | None = None
    exit_size: float | None = None
    exit_fee: float = 0.0
    exit_order_id: str | None = None
    pnl: float | None = None
    fees: float = 0.0
    exit_reason: str | None = None
    status: str = "open"


class LiveStateStore:
    def __init__(self, state_dir: Path, symbol: str, period: str, account_tag: str = "default") -> None:
        safe_tag = account_tag.strip().replace(" ", "_").replace("/", "_") or "default"
        self.path = state_dir / f"{symbol.replace('-', '_')}_{period}_{safe_tag}.json"

    def load(self) -> LiveState:
        if not self.path.exists():
            return LiveState()
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        return LiveState(**payload)

    def save(self, state: LiveState) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(asdict(state), ensure_ascii=True, indent=2), encoding="utf-8")


class LiveTradeStore:
    def __init__(self, state_dir: Path, symbol: str, period: str, account_tag: str = "default") -> None:
        safe_tag = account_tag.strip().replace(" ", "_").replace("/", "_") or "default"
        self.path = state_dir / f"{symbol.replace('-', '_')}_{period}_{safe_tag}_trades.json"

    def load(self) -> list[LiveTradeRecord]:
        if not self.path.exists():
            return []
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            return []
        records: list[LiveTradeRecord] = []
        for item in payload:
            if isinstance(item, dict):
                records.append(LiveTradeRecord(**item))
        return records

    def save(self, records: list[LiveTradeRecord]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = [asdict(record) for record in records]
        self.path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
