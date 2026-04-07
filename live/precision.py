from __future__ import annotations

from decimal import Decimal, ROUND_DOWN


def quantize_down(value: float, step: str | float) -> Decimal:
    value_dec = Decimal(str(value))
    step_dec = Decimal(str(step))
    if step_dec <= 0:
        raise ValueError("step must be positive")
    units = (value_dec / step_dec).to_integral_value(rounding=ROUND_DOWN)
    return units * step_dec


def format_decimal(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def format_size(value: float, lot_size: str | float, min_size: str | float) -> str | None:
    rounded = quantize_down(value, lot_size)
    if rounded < Decimal(str(min_size)):
        return None
    return format_decimal(rounded)

