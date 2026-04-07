from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class OKXCredentials:
    api_key: str
    secret_key: str
    passphrase: str
    base_url: str
    ws_private_url: str
    simulated: bool


@dataclass(slots=True)
class LiveRunConfig:
    symbol: str
    signal_symbol: str | None = None
    period: str = "1H"
    account_tag: str = "default"
    signal_side: str = "long_only"
    leverage: int = 1
    bars_to_fetch: int = 240
    risk_amount: float = 100.0
    max_allocation_pct: float = 0.95
    fast_ema: int = 21
    slow_ema: int = 55
    atr_period: int = 10
    hold_bars: int = 0
    stop_loss_atr_multiplier: float | None = None
    take_profit_r_multiple: float | None = None
    execute: bool = False
    poll_interval_seconds: int = 60
    fee_bps: float = 2.8
    slippage_bps: float = 2.0
    quote_balance_override: float | None = None
    base_balance_override: float | None = None
    state_dir: Path = Path("runtime")
    order_timeout_seconds: int = 25
    stop_on_slow_ema: bool = True
    exit_on_dead_cross: bool = True
    stop_first_on_ambiguous_bar: bool = True


def load_okx_credentials(
    simulated: bool,
    base_url: str | None = None,
    ws_private_url: str | None = None,
) -> OKXCredentials:
    api_key = os.getenv("OKX_API_KEY", "").strip()
    secret_key = os.getenv("OKX_API_SECRET", "").strip()
    passphrase = os.getenv("OKX_API_PASSPHRASE", "").strip()
    if not api_key or not secret_key or not passphrase:
        raise RuntimeError("Missing OKX credentials. Set OKX_API_KEY, OKX_API_SECRET, and OKX_API_PASSPHRASE.")

    return build_okx_credentials(
        api_key=api_key,
        secret_key=secret_key,
        passphrase=passphrase,
        simulated=simulated,
        base_url=base_url or os.getenv("OKX_BASE_URL"),
        ws_private_url=ws_private_url or os.getenv("OKX_WS_PRIVATE_URL"),
    )


def build_okx_credentials(
    *,
    api_key: str,
    secret_key: str,
    passphrase: str,
    simulated: bool,
    base_url: str | None = None,
    ws_private_url: str | None = None,
) -> OKXCredentials:
    resolved_base_url = (base_url or os.getenv("OKX_BASE_URL") or "https://www.okx.com").rstrip("/")
    resolved_ws_url = (ws_private_url or os.getenv("OKX_WS_PRIVATE_URL") or _default_ws_url(resolved_base_url, simulated)).strip()

    return OKXCredentials(
        api_key=api_key.strip(),
        secret_key=secret_key.strip(),
        passphrase=passphrase.strip(),
        base_url=resolved_base_url,
        ws_private_url=resolved_ws_url,
        simulated=simulated,
    )


def _default_ws_url(base_url: str, simulated: bool) -> str:
    host = base_url.lower()
    if "us.okx.com" in host:
        return "wss://wsuspap.okx.com:8443/ws/v5/private" if simulated else "wss://wsus.okx.com:8443/ws/v5/private"
    return "wss://wspap.okx.com:8443/ws/v5/private" if simulated else "wss://ws.okx.com:8443/ws/v5/private"
