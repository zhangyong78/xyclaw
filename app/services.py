from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from backtest.chart_report import build_chart_artifacts
from backtest.engine import BacktestEngine
from backtest.scorer import score_period_results
from backtest.sltp_matrix import run_sltp_matrix
from core.types import BacktestConfig
from data.fetch_history import download_history_bars, save_candles_csv
from data.resample import DEFAULT_PERIODS, load_1h_csv, resample_bars
from live.config import LiveRunConfig, build_okx_credentials, load_okx_credentials
from live.trader import LiveTrader

PERIOD_CACHE_LIMITS = {"5m": 10_000, "15m": 10_000, "1H": 10_000, "4H": 10_000, "24H": 10_000}


@dataclass(slots=True)
class BacktestRequest:
    symbol: str = "BTC-USDT-SWAP"
    csv: str | None = None
    history_bars: int = 2000
    history_bars_1h: int = 2000
    source_bar: str = "1H"
    start: str | None = None
    end: str | None = None
    initial_cash: float = 10_000.0
    risk_amount: float = 100.0
    fee_bps: float = 2.8
    slippage_bps: float = 2.0
    fast_ema: int = 21
    slow_ema: int = 55
    atr_period: int = 10
    hold_bars: int = 0
    max_allocation_pct: float = 0.95
    signal_side: str = "long_only"
    stop_loss_atr_multiplier: float | None = None
    take_profit_r_multiple: float | None = None
    periods: list[str] | None = None
    output_dir: str = "reports"


@dataclass(slots=True)
class LiveRequest:
    symbol: str = "BTC-USDT-SWAP"
    signal_symbol: str | None = None
    period: str = "1H"
    account_tag: str = "default"
    signal_side: str = "long_only"
    leverage: int = 1
    bars: int = 240
    risk_amount: float = 100.0
    max_allocation_pct: float = 0.95
    fast_ema: int = 21
    slow_ema: int = 55
    atr_period: int = 10
    hold_bars: int = 0
    stop_loss_atr_multiplier: float | None = None
    take_profit_r_multiple: float | None = None
    poll_interval: int = 60
    order_timeout: int = 25
    simulate: bool = False
    execute: bool = False
    quote_balance: float | None = None
    base_balance: float | None = None
    base_url: str | None = None
    ws_private_url: str | None = None
    api_key: str | None = None
    api_secret: str | None = None
    api_passphrase: str | None = None


def run_backtest(request: BacktestRequest) -> dict[str, Any]:
    output_dir = Path(request.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = output_dir / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    periods = request.periods or ["1H"]
    period_cache_csvs: dict[str, str] = {}
    period_data_sources: dict[str, str] = {}
    period_bars_map: dict[str, pd.DataFrame] = {}

    if request.csv:
        csv_path = Path(request.csv)
        source_frame = load_1h_csv(csv_path)
        source_frame = _filter_frame_by_time_range(source_frame, start=request.start, end=request.end)
        csv_period = _infer_period_from_csv_path(csv_path)
        csv_path_resolved = csv_path.resolve(strict=False)
        cache_path = None
        for period in periods:
            if csv_period == period:
                bars = source_frame.copy()
            elif csv_period in {None, request.source_bar}:
                bars = source_frame.copy() if period == request.source_bar else resample_bars(source_frame, period)
            elif _can_resample_csv_period(csv_period, period):
                bars = resample_bars(source_frame, period)
            else:
                bars = pd.DataFrame(columns=["open", "high", "low", "close", "volume", "turnover"])
            bars = _select_period_run_bars(
                bars,
                requested_bars=_resolve_requested_period_bars(request, period),
                preserve_range=bool(request.start or request.end),
            )
            period_bars_map[period] = bars
            period_data_sources[period] = f"手动CSV：{Path(request.csv).name}"
            target_cache_path = cache_dir / f"{request.symbol.replace('-', '_')}_{period}.csv"
            period_cache_path = None
            if target_cache_path.resolve(strict=False) != csv_path_resolved:
                period_cache_path = _save_period_cache_csv(
                    bars=bars,
                    cache_dir=cache_dir,
                    symbol=request.symbol,
                    period=period,
                )
            if target_cache_path.resolve(strict=False) == csv_path_resolved:
                period_cache_csvs[period] = str(csv_path)
            elif period_cache_path is not None:
                period_cache_csvs[period] = str(period_cache_path)
    else:
        cache_path = None
        for period in periods:
            bars, period_cache_path, source_label = _load_preferred_period_bars(
                request=request,
                cache_dir=cache_dir,
                period=period,
            )
            period_bars_map[period] = bars
            period_data_sources[period] = source_label
            if period_cache_path is not None:
                period_cache_csvs[period] = str(period_cache_path)
                if cache_path is None:
                    cache_path = period_cache_path

    if all(bars.empty for bars in period_bars_map.values()):
        raise RuntimeError(_build_backtest_no_data_message(request))

    config = BacktestConfig(
        symbol=request.symbol,
        initial_cash=request.initial_cash,
        risk_amount=request.risk_amount,
        fee_bps=request.fee_bps,
        slippage_bps=request.slippage_bps,
        fast_ema=request.fast_ema,
        slow_ema=request.slow_ema,
        atr_period=request.atr_period,
        hold_bars=request.hold_bars,
        max_allocation_pct=request.max_allocation_pct,
        signal_side=request.signal_side,
        stop_loss_atr_multiplier=request.stop_loss_atr_multiplier,
        take_profit_r_multiple=request.take_profit_r_multiple,
    )

    engine = BacktestEngine(config)
    period_summaries: list[dict[str, Any]] = []
    all_trades: list[pd.DataFrame] = []
    period_results: dict[str, dict[str, Any]] = {}
    insufficient_periods: list[tuple[str, int, int, str]] = []

    for period in periods:
        bars = period_bars_map.get(period, pd.DataFrame()).copy()
        minimum_required = max(request.slow_ema, request.atr_period, request.hold_bars) + 3
        if len(bars) < minimum_required:
            insufficient_periods.append(
                (
                    period,
                    int(len(bars)),
                    int(minimum_required),
                    str(period_data_sources.get(period) or "本地数据"),
                )
            )
            continue

        result = engine.run(bars, period=period)
        period_summaries.append(result["summary"])
        period_results[period] = result

        trade_df = result["trades"].copy()
        if not trade_df.empty:
            trade_df.insert(0, "period", period)
            all_trades.append(trade_df)

    if not period_summaries:
        raise RuntimeError(
            _build_backtest_insufficient_data_message(
                request,
                insufficient_periods=insufficient_periods,
            )
        )

    summary_df = score_period_results(period_summaries)
    trades_df = pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()

    summary_csv = output_dir / "summary.csv"
    trades_csv = output_dir / "trades.csv"
    summary_df.to_csv(summary_csv, index=False)
    trades_df.to_csv(trades_csv, index=False)
    top = summary_df.iloc[0].to_dict()
    top_period = str(top.get("period"))
    top_result = period_results[top_period]
    top_bars = period_bars_map[top_period].copy()
    sltp_matrix_df, sltp_chart_payloads = run_sltp_matrix(
        bars=top_bars,
        base_config=config,
        period=top_period,
        include_chart_payloads=True,
    )
    chart_artifacts = build_chart_artifacts(
        symbol=request.symbol,
        period=top_period,
        signal_frame=top_result["signal_frame"],
        trades_df=top_result["trades"],
        equity_df=top_result["equity"],
        output_dir=output_dir,
    )
    return {
        "symbol": request.symbol,
        "fast_ema": request.fast_ema,
        "slow_ema": request.slow_ema,
        "atr_period": request.atr_period,
        "signal_side": request.signal_side,
        "stop_loss_atr_multiplier": request.stop_loss_atr_multiplier,
        "take_profit_r_multiple": request.take_profit_r_multiple,
        "history_bars": int(_resolve_requested_period_bars(request, top_period)),
        "raw_bars_1h": int(len(top_bars)),
        "periods": periods,
        "cache_csv": str(cache_path) if cache_path else request.csv,
        "period_cache_csvs": period_cache_csvs,
        "period_data_sources": period_data_sources,
        "summary_csv": str(summary_csv),
        "trades_csv": str(trades_csv),
        "trade_chart_path": chart_artifacts["trade_chart_path"],
        "drawdown_chart_path": chart_artifacts["drawdown_chart_path"],
        "summary": _to_jsonable(summary_df.to_dict(orient="records")),
        "trades_preview": _to_jsonable(trades_df.head(50).to_dict(orient="records")) if not trades_df.empty else [],
        "top_period": top_period,
        "top_score": float(top.get("final_score", 0.0)),
        "sltp_matrix": _to_jsonable(sltp_matrix_df.to_dict(orient="records")) if not sltp_matrix_df.empty else [],
        "sltp_chart_payloads": sltp_chart_payloads,
        "top_chart": {
            "symbol": request.symbol,
            "period": top_period,
            "fast_ema": request.fast_ema,
            "slow_ema": request.slow_ema,
            "signal_frame": top_result["signal_frame"].copy(),
            "trades": top_result["trades"].copy(),
            "equity": top_result["equity"].copy(),
        },
    }


def sync_local_period_cache(
    *,
    symbol: str,
    period: str,
    output_dir: str = "desktop_reports",
) -> dict[str, Any]:
    output_path = Path(output_dir)
    cache_dir = output_path / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    cache_limit = int(PERIOD_CACHE_LIMITS.get(period, 10_000))
    cache_path = cache_dir / f"{symbol.replace('-', '_')}_{period}.csv"
    local_bars = _load_period_cache_csv(cache_path)
    fetch_bars = _estimate_period_cache_refresh_bars(local_bars, period, cache_limit)
    remote_bars = download_history_bars(inst_id=symbol, bar=period, bars=fetch_bars)
    merged_bars = _trim_period_cache_bars(_merge_period_frames(local_bars, remote_bars), period)

    added_bars = 0
    if not merged_bars.empty:
        existing_index = local_bars.index if not local_bars.empty else pd.Index([])
        added_bars = int(len(merged_bars.index.difference(existing_index)))
        save_candles_csv(merged_bars, cache_path)

    return {
        "symbol": symbol,
        "period": period,
        "cache_path": str(cache_path),
        "before_bars": int(len(local_bars)),
        "after_bars": int(len(merged_bars)),
        "added_bars": int(added_bars),
        "before_last_ts": _to_jsonable(local_bars.index.max()) if not local_bars.empty else None,
        "after_last_ts": _to_jsonable(merged_bars.index.max()) if not merged_bars.empty else None,
    }


def run_live_check(request: LiveRequest) -> dict[str, Any]:
    config = LiveRunConfig(
        symbol=request.symbol,
        signal_symbol=request.signal_symbol or request.symbol,
        period=request.period,
        account_tag=request.account_tag,
        signal_side=request.signal_side,
        leverage=request.leverage,
        bars_to_fetch=request.bars,
        risk_amount=request.risk_amount,
        max_allocation_pct=request.max_allocation_pct,
        fast_ema=request.fast_ema,
        slow_ema=request.slow_ema,
        atr_period=request.atr_period,
        hold_bars=request.hold_bars,
        stop_loss_atr_multiplier=request.stop_loss_atr_multiplier,
        take_profit_r_multiple=request.take_profit_r_multiple,
        execute=request.execute,
        poll_interval_seconds=request.poll_interval,
        quote_balance_override=request.quote_balance,
        base_balance_override=request.base_balance,
        order_timeout_seconds=request.order_timeout,
    )

    credentials = None
    if request.execute or (request.quote_balance is None and request.base_balance is None):
        if request.api_key and request.api_secret and request.api_passphrase:
            from live.config import build_okx_credentials

            credentials = build_okx_credentials(
                api_key=request.api_key,
                secret_key=request.api_secret,
                passphrase=request.api_passphrase,
                simulated=request.simulate,
                base_url=request.base_url,
                ws_private_url=request.ws_private_url,
            )
        else:
            try:
                credentials = load_okx_credentials(
                    simulated=request.simulate,
                    base_url=request.base_url,
                    ws_private_url=request.ws_private_url,
                )
            except RuntimeError as exc:
                if "Missing OKX credentials" in str(exc):
                    raise RuntimeError(
                        "当前真实仓位缺少可用的 OKX API 凭证。请先在“连接与余额”里填写 API Key / API Secret / Passphrase，"
                        "并点击“保存当前API”；或者设置 OKX_API_KEY / OKX_API_SECRET / OKX_API_PASSPHRASE 环境变量。"
                    ) from exc
                raise

    trader = LiveTrader(config, credentials=credentials, public_base_url=request.base_url)
    credential_mode = "simulated" if credentials and credentials.simulated else "real"
    try:
        report = trader.run_once()
    except Exception as exc:
        retry_with_simulated = (
            credentials is not None
            and not bool(request.execute)
            and not bool(credentials.simulated)
            and _looks_like_okx_unauthorized(exc)
        )
        if not retry_with_simulated:
            raise _rewrite_live_auth_error(exc) from exc

        demo_credentials = build_okx_credentials(
            api_key=credentials.api_key,
            secret_key=credentials.secret_key,
            passphrase=credentials.passphrase,
            simulated=True,
            base_url=credentials.base_url,
            ws_private_url=credentials.ws_private_url,
        )
        trader = LiveTrader(config, credentials=demo_credentials, public_base_url=request.base_url)
        try:
            report = trader.run_once()
            credential_mode = "simulated"
        except Exception as demo_exc:
            raise _rewrite_live_auth_error(demo_exc) from demo_exc

    return {
        "request": _to_jsonable(asdict(request)),
        "report": _to_jsonable({
            "action": report.action,
            "reason": report.reason,
            "signal_ts": report.signal_ts,
            "in_position": report.in_position,
            "base_balance": report.base_balance,
            "quote_balance": report.quote_balance,
            "suggested_size": report.suggested_size,
            "held_bars": report.held_bars,
            "latest_close": report.latest_close,
            "ema_fast": report.ema_fast,
            "ema_slow": report.ema_slow,
            "total_assets": report.total_assets,
            "today_pnl": report.today_pnl,
            "total_pnl": report.total_pnl,
            "account_name": trader.detected_account_name,
            "account_uid": trader.detected_account_uid,
            "credential_mode": credential_mode,
            "execution": asdict(report.execution) if report.execution is not None else None,
        }),
        "signal_frame": report.signal_frame.copy(),
        "trades": report.trades_df.copy(),
        "executed": bool(request.execute),
        "simulated": bool(request.simulate),
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }


def _looks_like_okx_unauthorized(exc: Exception) -> bool:
    message = str(exc)
    lowered = message.lower()
    return "401" in lowered or "unauthorized" in lowered


def _rewrite_live_auth_error(exc: Exception) -> RuntimeError:
    message = str(exc)
    lowered = message.lower()
    if "401" in lowered or "unauthorized" in lowered:
        return RuntimeError(
            "OKX 账户认证失败（401）。请检查 API Key / API Secret / Passphrase 是否正确，"
            "并确认该 API 已开启交易/读取权限、IP 白名单匹配；"
            "如果你填的是 OKX 模拟盘 API，请切到“模拟并下单”，或继续用“仅信号检查”让程序自动按模拟盘重试。"
        )
    return RuntimeError(message)


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _to_jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return value


def _filter_frame_by_time_range(df: pd.DataFrame, *, start: str | None, end: str | None) -> pd.DataFrame:
    if df.empty:
        return df

    filtered = df
    if start:
        start_ts = pd.Timestamp(start)
        filtered = filtered[filtered.index >= start_ts]
    if end:
        end_ts = pd.Timestamp(end)
        filtered = filtered[filtered.index <= end_ts]
    return filtered.sort_index()


def _load_preferred_period_bars(
    *,
    request: BacktestRequest,
    cache_dir: Path,
    period: str,
) -> tuple[pd.DataFrame, Path | None, str]:
    requested_bars = _resolve_requested_period_bars(request, period)
    required_bars = _estimate_required_period_bars(request, period)
    minimum_required = max(request.slow_ema, request.atr_period, request.hold_bars) + 3
    preserve_range = bool(request.start or request.end)
    local_bars, local_label = _load_local_period_bars(request=request, cache_dir=cache_dir, period=period)
    local_filtered = _filter_frame_by_time_range(local_bars, start=request.start, end=request.end)
    local_run_bars = _select_period_run_bars(
        local_filtered,
        requested_bars=requested_bars,
        preserve_range=preserve_range,
    )

    if _period_data_sufficient(
        local_bars,
        filtered_bars=local_filtered,
        required_bars=required_bars,
        minimum_required=minimum_required,
        start=request.start,
        end=request.end,
    ):
        cache_path = _save_period_cache_csv(
            bars=local_bars,
            cache_dir=cache_dir,
            symbol=request.symbol,
            period=period,
        )
        return local_run_bars, cache_path, local_label or "本地缓存"

    try:
        remote_bars = download_history_bars(
            inst_id=request.symbol,
            bar=period,
            bars=required_bars,
            start=request.start,
            end=request.end,
        )
    except Exception:
        if len(local_run_bars) >= minimum_required:
            cache_path = _save_period_cache_csv(
                bars=local_bars,
                cache_dir=cache_dir,
                symbol=request.symbol,
                period=period,
            )
            fallback_label = local_label or "本地缓存"
            return local_run_bars, cache_path, f"{fallback_label}（补齐失败，已使用现有本地数据）"
        raise

    merged_bars = _merge_period_frames(local_bars, remote_bars)
    filtered_merged = _filter_frame_by_time_range(merged_bars, start=request.start, end=request.end)
    run_bars = _select_period_run_bars(
        filtered_merged,
        requested_bars=requested_bars,
        preserve_range=preserve_range,
    )
    cache_path = _save_period_cache_csv(
        bars=merged_bars,
        cache_dir=cache_dir,
        symbol=request.symbol,
        period=period,
    )

    if local_bars.empty:
        source_label = "OKX自动下载并写入本地缓存"
    else:
        source_label = f"{local_label} + OKX自动补齐"
    return run_bars, cache_path, source_label


def _load_local_period_bars(
    *,
    request: BacktestRequest,
    cache_dir: Path,
    period: str,
) -> tuple[pd.DataFrame, str]:
    symbol = request.symbol.replace("-", "_")
    labels: list[str] = []
    frames: list[pd.DataFrame] = []

    period_cache_path = cache_dir / f"{symbol}_{period}.csv"
    direct_bars = _load_period_cache_csv(period_cache_path)
    if not direct_bars.empty:
        frames.append(direct_bars)
        labels.append(f"本地{period}缓存")

    if request.source_bar and request.source_bar != period:
        source_cache_path = cache_dir / f"{symbol}_{request.source_bar}.csv"
        source_bars = _load_period_cache_csv(source_cache_path)
        if not source_bars.empty:
            try:
                frames.append(resample_bars(source_bars, period))
                labels.append(f"本地{request.source_bar}缓存聚合")
            except Exception:
                pass

    return _merge_period_frames(*frames), " + ".join(labels)


def _load_period_cache_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume", "turnover"])
    try:
        return load_1h_csv(path)
    except Exception:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume", "turnover"])


def _merge_period_frames(*frames: pd.DataFrame) -> pd.DataFrame:
    usable = [frame.copy() for frame in frames if frame is not None and not frame.empty]
    if not usable:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume", "turnover"])
    merged = pd.concat(usable).sort_index()
    merged = merged[~merged.index.duplicated(keep="last")]
    return merged


def _infer_period_from_csv_path(path: Path) -> str | None:
    stem = path.stem.upper()
    for period in ("24H", "16H", "12H", "8H", "6H", "4H", "3H", "2H", "1H", "15M", "5M"):
        suffix = f"_{period}"
        if stem.endswith(suffix):
            return period.replace("M", "m")
    return None


def _can_resample_csv_period(source_period: str, target_period: str) -> bool:
    source_minutes = _period_to_minutes(source_period)
    target_minutes = _period_to_minutes(target_period)
    if source_minutes is None or target_minutes is None:
        return False
    return source_minutes <= target_minutes and target_minutes % source_minutes == 0


def _resolve_requested_period_bars(request: BacktestRequest, period: str) -> int:
    requested = int(request.history_bars or 0)
    if requested > 0:
        return requested

    source_bars = max(1, int(request.history_bars_1h or 0))
    source_minutes = _period_to_minutes(request.source_bar)
    period_minutes = _period_to_minutes(period)
    if source_minutes is None or period_minutes is None:
        return source_bars
    return max(1, int(source_bars * source_minutes / period_minutes))


def _estimate_required_period_bars(request: BacktestRequest, period: str) -> int:
    minimum_required = max(request.slow_ema, request.atr_period, request.hold_bars) + 3
    requested_bars = _resolve_requested_period_bars(request, period)
    required_bars = max(minimum_required, requested_bars)

    if request.start and request.end:
        try:
            start_ts = pd.Timestamp(request.start)
            end_ts = pd.Timestamp(request.end)
            period_step = _period_to_timedelta(period)
            if period_step is not None and end_ts >= start_ts:
                span_bars = int((end_ts - start_ts) / period_step) + 2
                required_bars = max(required_bars, span_bars)
        except Exception:
            pass

    return max(1, required_bars)


def _estimate_period_cache_refresh_bars(local_bars: pd.DataFrame, period: str, cache_limit: int) -> int:
    if local_bars.empty:
        return cache_limit

    period_step = _period_to_timedelta(period)
    if period_step is None:
        return cache_limit

    last_ts = pd.Timestamp(local_bars.index.max())
    if last_ts.tzinfo is None:
        last_ts = last_ts.tz_localize("UTC")
    else:
        last_ts = last_ts.tz_convert("UTC")

    now_ts = pd.Timestamp.now(tz="UTC")
    gap = max(now_ts - last_ts, pd.Timedelta(0))
    needed = int(gap / period_step) + 8
    return max(8, min(cache_limit, needed))


def _period_data_sufficient(
    all_bars: pd.DataFrame,
    *,
    filtered_bars: pd.DataFrame,
    required_bars: int,
    minimum_required: int,
    start: str | None,
    end: str | None,
) -> bool:
    if filtered_bars.empty or len(filtered_bars) < minimum_required:
        return False
    if start or end:
        return _covers_requested_time_range(all_bars, start=start, end=end)
    return len(filtered_bars) >= required_bars


def _covers_requested_time_range(df: pd.DataFrame, *, start: str | None, end: str | None) -> bool:
    if df.empty:
        return False
    first_ts = df.index.min()
    last_ts = df.index.max()
    if start:
        try:
            if first_ts > pd.Timestamp(start):
                return False
        except Exception:
            return False
    if end:
        try:
            if last_ts < pd.Timestamp(end):
                return False
        except Exception:
            return False
    return True


def _select_period_run_bars(df: pd.DataFrame, *, requested_bars: int, preserve_range: bool) -> pd.DataFrame:
    if df.empty:
        return df
    if preserve_range:
        return df.sort_index().copy()
    if requested_bars <= 0 or len(df) <= requested_bars:
        return df.sort_index().copy()
    return df.sort_index().tail(requested_bars).copy()


def _period_to_minutes(period: str | None) -> int | None:
    if not period:
        return None
    value = str(period).strip()
    if value.endswith("m"):
        return int(value[:-1])
    if value.endswith("H"):
        return int(value[:-1]) * 60
    return None


def _period_to_timedelta(period: str) -> pd.Timedelta | None:
    minutes = _period_to_minutes(period)
    if minutes is None:
        return None
    return pd.Timedelta(minutes=minutes)


def _build_backtest_no_data_message(request: BacktestRequest) -> str:
    time_hint = _format_backtest_time_hint(request.start, request.end)
    if request.csv:
        return f"回测失败：当前自定义时间段{time_hint}内没有可用K线，请检查本地CSV是否包含这段时间。"
    return f"回测失败：本地缓存和 OKX 补齐后，在当前自定义时间段{time_hint}内仍没有可用K线，请放大时间范围后重试。"


def _build_backtest_insufficient_data_message(
    request: BacktestRequest,
    *,
    insufficient_periods: list[tuple[str, int, int, str]],
) -> str:
    period, available, required, source_label = insufficient_periods[0] if insufficient_periods else (
        (request.periods or ["1H"])[0],
        0,
        int(max(request.slow_ema, request.atr_period, request.hold_bars) + 3),
        "本地缓存",
    )
    time_hint = _format_backtest_time_hint(request.start, request.end)
    return (
        f"回测失败：当前自定义时间段{time_hint}内K线数量不足。"
        f"当前数据来源为 {source_label}，只有 {available} 根 {period} K线，"
        f"至少需要 {required} 根 {period} K线才能计算 EMA/ATR 并开始回测。"
    )


def _format_backtest_time_hint(start: str | None, end: str | None) -> str:
    if not start and not end:
        return "（未限定时间段）"
    start_text = _format_backtest_bound(start, fallback="最早")
    end_text = _format_backtest_bound(end, fallback="最新")
    return f"（{start_text} 到 {end_text}）"


def _format_backtest_bound(value: str | None, *, fallback: str) -> str:
    if not value:
        return fallback
    try:
        stamp = pd.Timestamp(value)
    except Exception:
        return str(value)
    if stamp.tzinfo is not None:
        stamp = stamp.tz_convert("Asia/Shanghai").tz_localize(None)
    return stamp.strftime("%Y-%m-%d %H:%M")


def _save_period_cache_csv(
    *,
    bars: pd.DataFrame,
    cache_dir: Path,
    symbol: str,
    period: str,
) -> Path | None:
    trimmed = _trim_period_cache_bars(bars, period)
    if trimmed.empty:
        return None

    path = cache_dir / f"{symbol.replace('-', '_')}_{period}.csv"
    save_candles_csv(trimmed, path)
    return path


def _trim_period_cache_bars(bars: pd.DataFrame, period: str) -> pd.DataFrame:
    if bars.empty:
        return bars.copy()

    limit = PERIOD_CACHE_LIMITS.get(period)
    if limit is None or len(bars) <= limit:
        return bars.copy()
    return bars.tail(limit).copy()
