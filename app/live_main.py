from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from live.config import LiveRunConfig, build_okx_credentials, load_okx_credentials
from live.trader import LiveTrader, format_cycle_report
from app.services import LiveRequest, run_live_check


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="OKX EMA5/EMA8 live trading runner")
    parser.add_argument("--symbol", default="BTC-USDT-SWAP")
    parser.add_argument("--period", default="1H")
    parser.add_argument("--bars", type=int, default=240, help="Closed candles to fetch for signal calculation")
    parser.add_argument("--risk-amount", type=float, default=100.0)
    parser.add_argument("--max-allocation-pct", type=float, default=0.95)
    parser.add_argument("--fast-ema", type=int, default=21)
    parser.add_argument("--slow-ema", type=int, default=55)
    parser.add_argument("--hold-bars", type=int, default=0)
    parser.add_argument("--poll-interval", type=int, default=60)
    parser.add_argument("--order-timeout", type=int, default=25)
    parser.add_argument("--simulate", action="store_true", help="Use OKX demo trading environment")
    parser.add_argument("--execute", action="store_true", help="Actually send orders to OKX")
    parser.add_argument("--loop", action="store_true", help="Run continuously instead of once")
    parser.add_argument("--quote-balance", type=float, help="Manual quote balance override for dry-run")
    parser.add_argument("--base-balance", type=float, help="Manual base balance override for dry-run")
    parser.add_argument("--base-url", help="Override OKX REST base URL, e.g. https://us.okx.com")
    parser.add_argument("--ws-private-url", help="Override OKX private WebSocket URL")
    parser.add_argument("--api-key")
    parser.add_argument("--api-secret")
    parser.add_argument("--api-passphrase")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.loop:
        config = LiveRunConfig(
            symbol=args.symbol,
            period=args.period,
            bars_to_fetch=args.bars,
            risk_amount=args.risk_amount,
            max_allocation_pct=args.max_allocation_pct,
            fast_ema=args.fast_ema,
            slow_ema=args.slow_ema,
            hold_bars=args.hold_bars,
            execute=args.execute,
            poll_interval_seconds=args.poll_interval,
            quote_balance_override=args.quote_balance,
            base_balance_override=args.base_balance,
            order_timeout_seconds=args.order_timeout,
        )
        credentials = None
        if args.execute or (args.quote_balance is None and args.base_balance is None):
            if args.api_key and args.api_secret and args.api_passphrase:
                credentials = build_okx_credentials(
                    api_key=args.api_key,
                    secret_key=args.api_secret,
                    passphrase=args.api_passphrase,
                    simulated=args.simulate,
                    base_url=args.base_url,
                    ws_private_url=args.ws_private_url,
                )
            else:
                credentials = load_okx_credentials(
                    simulated=args.simulate,
                    base_url=args.base_url,
                    ws_private_url=args.ws_private_url,
                )
        trader = LiveTrader(config, credentials=credentials, public_base_url=args.base_url)
        trader.run_loop()
        return

    result = run_live_check(
        LiveRequest(
            symbol=args.symbol,
            period=args.period,
            bars=args.bars,
            risk_amount=args.risk_amount,
            max_allocation_pct=args.max_allocation_pct,
            fast_ema=args.fast_ema,
            slow_ema=args.slow_ema,
            hold_bars=args.hold_bars,
            poll_interval=args.poll_interval,
            order_timeout=args.order_timeout,
            simulate=args.simulate,
            execute=args.execute,
            quote_balance=args.quote_balance,
            base_balance=args.base_balance,
            base_url=args.base_url,
            ws_private_url=args.ws_private_url,
            api_key=args.api_key,
            api_secret=args.api_secret,
            api_passphrase=args.api_passphrase,
        )
    )
    report = result["report"]
    execution = report["execution"]
    from live.order_router import OrderExecutionResult
    from live.trader import CycleReport

    cycle = CycleReport(
        action=report["action"],
        reason=report["reason"],
        signal_ts=report["signal_ts"],
        in_position=report["in_position"],
        base_balance=report["base_balance"],
        quote_balance=report["quote_balance"],
        suggested_size=report["suggested_size"],
        held_bars=report["held_bars"],
        latest_close=report["latest_close"],
        ema_fast=report["ema_fast"],
        ema_slow=report["ema_slow"],
        total_assets=report.get("total_assets", 0.0),
        today_pnl=report.get("today_pnl", 0.0),
        total_pnl=report.get("total_pnl", 0.0),
        signal_frame=result.get("signal_frame"),
        trades_df=result.get("trades"),
        execution=OrderExecutionResult(**execution) if execution else None,
    )
    print(format_cycle_report(cycle, execute=args.execute))


if __name__ == "__main__":
    main()
