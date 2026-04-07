from __future__ import annotations


def apply_slippage(price: float, side: str, slippage_bps: float) -> float:
    slip = slippage_bps / 10_000
    if side == "buy":
        return price * (1 + slip)
    if side == "sell":
        return price * (1 - slip)
    raise ValueError(f"Unsupported side: {side}")


def calc_fee(notional: float, fee_bps: float) -> float:
    return notional * fee_bps / 10_000

