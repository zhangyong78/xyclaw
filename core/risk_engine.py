from __future__ import annotations


def calculate_position_size(
    entry_price: float,
    stop_price: float,
    risk_amount: float,
    available_cash: float,
    max_allocation_pct: float,
    side: str = "long",
) -> float:
    risk_per_unit = stop_price - entry_price if side == "short" else entry_price - stop_price
    if risk_per_unit <= 0:
        return 0.0

    qty_by_risk = risk_amount / risk_per_unit
    max_notional = max(available_cash, 0.0) * max_allocation_pct
    qty_by_cash = max_notional / entry_price if entry_price > 0 else 0.0
    qty = min(qty_by_risk, qty_by_cash)
    return max(qty, 0.0)
