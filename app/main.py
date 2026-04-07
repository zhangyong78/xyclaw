from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from app.services import BacktestRequest, run_backtest
from data.resample import DEFAULT_PERIODS


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="OKX EMA5/EMA8 multi-period backtest")
    parser.add_argument("--symbol", default="BTC-USDT-SWAP", help="OKX 永续合约，例如 BTC-USDT-SWAP")
    parser.add_argument("--csv", help="Existing 1H candle CSV path")
    parser.add_argument("--history-bars-1h", type=int, default=2000, help="How many 1H candles to fetch from OKX")
    parser.add_argument("--start", help="Filter start time, e.g. 2024-01-01")
    parser.add_argument("--end", help="Filter end time, e.g. 2025-12-31")
    parser.add_argument("--initial-cash", type=float, default=10000.0)
    parser.add_argument("--risk-amount", type=float, default=100.0)
    parser.add_argument("--fee-bps", type=float, default=2.8)
    parser.add_argument("--slippage-bps", type=float, default=2.0)
    parser.add_argument("--fast-ema", type=int, default=21)
    parser.add_argument("--slow-ema", type=int, default=55)
    parser.add_argument("--hold-bars", type=int, default=0)
    parser.add_argument("--max-allocation-pct", type=float, default=0.95)
    parser.add_argument(
        "--periods",
        default="1H",
        help="Comma-separated periods, e.g. 1H,4H,12H",
    )
    parser.add_argument("--output-dir", default="reports")
    return parser


def print_summary(summary_df: pd.DataFrame) -> None:
    display_cols = [
        "period",
        "trade_count",
        "net_return_pct",
        "max_drawdown_pct",
        "cdar_95_pct",
        "ulcer_index",
        "final_score",
        "rank",
    ]
    pretty = summary_df[display_cols].copy()
    for col in ["net_return_pct", "max_drawdown_pct", "cdar_95_pct", "ulcer_index", "final_score"]:
        pretty[col] = pretty[col].map(lambda value: round(float(value), 2))
    print(pretty.to_string(index=False))


def main() -> None:
    args = build_parser().parse_args()
    result = run_backtest(
        BacktestRequest(
            symbol=args.symbol,
            csv=args.csv,
            history_bars=args.history_bars_1h,
            history_bars_1h=args.history_bars_1h,
            start=args.start,
            end=args.end,
            initial_cash=args.initial_cash,
            risk_amount=args.risk_amount,
            fee_bps=args.fee_bps,
            slippage_bps=args.slippage_bps,
            fast_ema=args.fast_ema,
            slow_ema=args.slow_ema,
            hold_bars=args.hold_bars,
            max_allocation_pct=args.max_allocation_pct,
            periods=[period.strip() for period in args.periods.split(",") if period.strip()],
            output_dir=args.output_dir,
        )
    )
    summary_df = pd.DataFrame(result["summary"])
    print_summary(summary_df)
    period_cache_csvs = result.get("period_cache_csvs", {})
    if isinstance(period_cache_csvs, dict):
        for period, path in period_cache_csvs.items():
            print(f"Saved {period} candles to {path}")
    elif result["cache_csv"] and not args.csv:
        print(f"Saved 1H candles to {result['cache_csv']}")
    print(f"\nSummary CSV: {result['summary_csv']}")
    print(f"Trades CSV:  {result['trades_csv']}")
    print(f"Trade SVG:   {result['trade_chart_path']}")
    print(f"Drawdown SVG:{result['drawdown_chart_path']}")


if __name__ == "__main__":
    main()
