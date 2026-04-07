from __future__ import annotations

import math

import pandas as pd


def compute_trade_metrics(
    trade_df: pd.DataFrame,
    initial_cash: float,
    final_equity: float,
    start_ts,
    end_ts,
) -> dict[str, float]:
    elapsed_days = max((end_ts - start_ts).total_seconds() / 86_400, 1 / 24)
    months = max(elapsed_days / 30.4375, 1 / 30.4375)

    if trade_df.empty:
        return {
            "trade_count": 0,
            "win_rate": 0.0,
            "gross_profit": 0.0,
            "gross_loss": 0.0,
            "profit_factor": 0.0,
            "avg_pnl": 0.0,
            "expectancy": 0.0,
            "avg_hold_hours": 0.0,
            "avg_trades_per_month": 0.0,
            "net_pnl": final_equity - initial_cash,
            "net_return_pct": (final_equity / initial_cash - 1.0) * 100,
        }

    pnl = trade_df["pnl"].astype(float)
    gross_profit = float(pnl[pnl > 0].sum())
    gross_loss_abs = float(-pnl[pnl < 0].sum())
    profit_factor = gross_profit / gross_loss_abs if gross_loss_abs > 0 else math.inf

    return {
        "trade_count": int(len(trade_df)),
        "win_rate": float((pnl > 0).mean() * 100),
        "gross_profit": gross_profit,
        "gross_loss": gross_loss_abs,
        "profit_factor": float(profit_factor if math.isfinite(profit_factor) else 99.0),
        "avg_pnl": float(pnl.mean()),
        "expectancy": float(pnl.mean()),
        "avg_hold_hours": float(trade_df["hold_hours"].mean()),
        "avg_trades_per_month": float(len(trade_df) / months),
        "net_pnl": final_equity - initial_cash,
        "net_return_pct": (final_equity / initial_cash - 1.0) * 100,
    }
