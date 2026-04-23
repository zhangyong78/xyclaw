from __future__ import annotations

import time
from dataclasses import dataclass

import pandas as pd

from core.indicators import atr
from core.risk_engine import calculate_position_size
from core.strategy_ema58 import (
    ExitEvaluation,
    add_strategy_columns,
    evaluate_long_exit,
    find_latest_cross_up_ts,
    latest_signal_snapshot,
)
from data.okx_rest import OKXPublicClient
from live.config import LiveRunConfig, OKXCredentials
from live.order_router import OrderExecutionResult, OrderRouter
from live.precision import format_size
from live.state_store import LiveState, LiveStateStore, LiveTradeRecord, LiveTradeStore


@dataclass(slots=True)
class CycleReport:
    action: str
    reason: str
    signal_ts: str
    in_position: bool
    base_balance: float
    quote_balance: float
    suggested_size: str | None
    held_bars: int
    latest_close: float
    ema_fast: float
    ema_slow: float
    total_assets: float
    today_pnl: float
    total_pnl: float
    signal_frame: pd.DataFrame
    trades_df: pd.DataFrame
    execution: OrderExecutionResult | None = None


@dataclass(slots=True)
class PendingEntrySignal:
    action: str
    trade_side: str
    reason: str
    signal_ts: str


class LiveTrader:
    def __init__(
        self,
        config: LiveRunConfig,
        *,
        credentials: OKXCredentials | None = None,
        public_base_url: str | None = None,
    ) -> None:
        self.config = config
        self.credentials = credentials
        self.public_client = OKXPublicClient(base_url=public_base_url or (credentials.base_url if credentials else None))
        self.state_store = LiveStateStore(config.state_dir, config.symbol, config.period, config.account_tag)
        self.trade_store = LiveTradeStore(config.state_dir, config.symbol, config.period, config.account_tag)
        self.order_router = OrderRouter(credentials, execute=config.execute, order_timeout_seconds=config.order_timeout_seconds) if credentials else None
        self.detected_account_name = ""
        self.detected_account_uid = ""

    def run_once(self) -> CycleReport:
        self._refresh_account_identity()
        instrument = self.public_client.get_instrument(self.config.symbol, inst_type="SWAP")
        signal_symbol = self.config.signal_symbol or self.config.symbol
        bars = self.public_client.download_history(signal_symbol, self.config.period, self.config.bars_to_fetch)
        if len(bars) <= max(self.config.slow_ema, self.config.hold_bars) + 2:
            raise RuntimeError("Not enough closed candles for live evaluation.")

        signal_frame = add_strategy_columns(bars, self.config.fast_ema, self.config.slow_ema)
        signal_frame["atr"] = atr(
            signal_frame["high"],
            signal_frame["low"],
            signal_frame["close"],
            max(int(self.config.atr_period), 1),
        )
        snapshot = latest_signal_snapshot(signal_frame)
        current_ts = self._utc_now_iso()
        live_price = self._get_live_reference_price(self.config.symbol, fallback=snapshot.close)
        contract_info = self._extract_contract_info(instrument, latest_price=live_price)
        available_margin, total_equity = self._get_margin_snapshot(margin_ccy=contract_info["margin_ccy"])
        position = self._get_position_snapshot(contract_info=contract_info)

        state = self.state_store.load()
        live_trades = self.trade_store.load()
        has_processed_signals = bool(state.last_buy_signal_ts or state.last_sell_signal_ts)
        state, today_pnl, total_pnl = self._refresh_equity_baselines(state, total_assets=total_equity)
        if position["side"] is None and (state.entry_signal_ts is not None or state.entry_side is not None):
            state.entry_signal_ts = None
            state.entry_side = None
            self.state_store.save(state)
        if position["side"] is None and self._reconcile_flat_position_trades(
            live_trades,
            signal_frame=signal_frame,
            fallback_price=live_price,
            signal_ts=current_ts,
        ):
            self.trade_store.save(live_trades)
        if self._hydrate_open_trade(
            live_trades,
            signal_frame=signal_frame,
            state=state,
            position_side=position["side"],
            position_base_size=position["base_qty"],
            fallback_price=float(position.get("avg_price") or live_price),
        ):
            self.trade_store.save(live_trades)

        if position["side"] is None:
            pending_entry = self._select_pending_entry_signal(
                signal_frame,
                state=state,
                snapshot=snapshot,
                allow_resume_catchup=has_processed_signals,
                live_trades=live_trades,
            )
            if pending_entry is not None:
                return self._with_live_preview_frame(
                    self._handle_entry(
                        trade_side=pending_entry.trade_side,
                        action=pending_entry.action,
                        reason=pending_entry.reason,
                        signal_frame=signal_frame,
                        snapshot=snapshot,
                        state=state,
                        live_trades=live_trades,
                        available_margin=available_margin,
                        total_assets=total_equity,
                        today_pnl=today_pnl,
                        total_pnl=total_pnl,
                        contract_info=contract_info,
                        signal_ts=pending_entry.signal_ts,
                    ),
                    signal_symbol=signal_symbol,
                )
            return self._with_live_preview_frame(
                self._build_report(
                    action="hold",
                    reason="no_entry_signal",
                    signal_ts=snapshot.ts.isoformat(),
                    in_position=False,
                    base_balance=0.0,
                    quote_balance=available_margin,
                    suggested_size=None,
                    held_bars=0,
                    latest_close=live_price,
                    ema_fast=snapshot.ema_fast,
                    ema_slow=snapshot.ema_slow,
                    total_assets=total_equity,
                    today_pnl=today_pnl,
                    total_pnl=total_pnl,
                    signal_frame=signal_frame,
                    trades_df=self._build_trades_frame(live_trades, latest_close=live_price),
                ),
                signal_symbol=signal_symbol,
            )

        open_trade = self._find_open_trade(live_trades)
        entry_signal_ts = state.entry_signal_ts or self._infer_entry_signal_ts(signal_frame, position["side"])
        exit_eval = self._evaluate_exit(
            signal_frame,
            entry_signal_ts=entry_signal_ts,
            open_trade=open_trade,
            position_side=position["side"],
        )

        if position["side"] == "long" and exit_eval.should_exit and state.last_sell_signal_ts != snapshot.ts.isoformat():
            return self._with_live_preview_frame(
                self._handle_exit(
                    order_side="sell",
                    position_side="long",
                    exit_reason=exit_eval.reason or "exit_signal",
                    signal_frame=signal_frame,
                    snapshot=snapshot,
                    state=state,
                    live_trades=live_trades,
                    position=position,
                    total_assets=total_equity,
                    today_pnl=today_pnl,
                    total_pnl=total_pnl,
                    held_bars=exit_eval.held_bars,
                    contract_info=contract_info,
                    quote_balance=available_margin,
                ),
                signal_symbol=signal_symbol,
            )
        if position["side"] == "short" and exit_eval.should_exit and state.last_buy_signal_ts != snapshot.ts.isoformat():
            return self._with_live_preview_frame(
                self._handle_exit(
                    order_side="buy",
                    position_side="short",
                    exit_reason=exit_eval.reason or "exit_signal",
                    signal_frame=signal_frame,
                    snapshot=snapshot,
                    state=state,
                    live_trades=live_trades,
                    position=position,
                    total_assets=total_equity,
                    today_pnl=today_pnl,
                    total_pnl=total_pnl,
                    held_bars=exit_eval.held_bars,
                    contract_info=contract_info,
                    quote_balance=available_margin,
                ),
                signal_symbol=signal_symbol,
            )

        return self._with_live_preview_frame(
            self._build_report(
                action="hold",
                reason="position_open_no_exit",
                signal_ts=snapshot.ts.isoformat(),
                in_position=True,
                base_balance=position["signed_base_qty"],
                quote_balance=available_margin,
                suggested_size=None,
                held_bars=exit_eval.held_bars,
                latest_close=live_price,
                ema_fast=snapshot.ema_fast,
                ema_slow=snapshot.ema_slow,
                total_assets=total_equity,
                today_pnl=today_pnl,
                total_pnl=total_pnl,
                signal_frame=signal_frame,
                trades_df=self._build_trades_frame(live_trades, latest_close=live_price),
            ),
            signal_symbol=signal_symbol,
        )

    def _handle_entry(
        self,
        *,
        trade_side: str,
        action: str,
        reason: str,
        signal_frame: pd.DataFrame,
        snapshot,
        state: LiveState,
        live_trades: list[LiveTradeRecord],
        available_margin: float,
        total_assets: float,
        today_pnl: float,
        total_pnl: float,
        contract_info: dict[str, float | str],
        signal_ts: str | None = None,
    ) -> CycleReport:
        effective_signal_ts = str(signal_ts or snapshot.ts.isoformat())
        execution_ts = self._utc_now_iso()
        live_price = self._get_live_reference_price(self.config.symbol, fallback=snapshot.close)
        entry_reference_price = float(live_price if live_price > 0 else snapshot.close)
        stop_price = self._resolve_entry_stop_price(
            signal_frame,
            entry_price=entry_reference_price,
            fallback_stop=snapshot.ema_slow,
            trade_side=trade_side,
        )
        risk_cash = max(float(available_margin), 0.0) * max(float(self.config.leverage), 1.0)
        desired_base_qty = calculate_position_size(
            entry_price=entry_reference_price,
            stop_price=stop_price,
            risk_amount=self.config.risk_amount,
            available_cash=risk_cash,
            max_allocation_pct=self.config.max_allocation_pct,
            side=trade_side,
        )
        desired_contracts = self._base_qty_to_contracts(desired_base_qty, contract_info)
        size = format_size(desired_contracts, contract_info["lot_size"], contract_info["min_size"])
        base_size = self._contracts_to_base_qty(float(size or 0.0), contract_info)
        execution = self._execute_order_if_needed(order_side=action, size=size, reduce_only=False)

        if execution is not None and execution.status not in {"dry_run"}:
            if not self._execution_is_rejected(execution):
                self.state_store.save(
                    LiveState(
                        last_buy_signal_ts=effective_signal_ts if action == "buy" else state.last_buy_signal_ts,
                        last_sell_signal_ts=effective_signal_ts if action == "sell" else state.last_sell_signal_ts,
                        entry_signal_ts=effective_signal_ts,
                        entry_side=trade_side,
                        last_order_cl_ord_id=execution.cl_ord_id,
                        strategy_started_at=state.strategy_started_at,
                        strategy_start_equity=state.strategy_start_equity,
                        day_anchor=state.day_anchor,
                        day_start_equity=state.day_start_equity,
                    )
                )
            if self._execution_has_fill(execution):
                self._record_open_trade(
                    live_trades,
                    trade_side=trade_side,
                    signal_frame=signal_frame,
                    signal_ts=effective_signal_ts,
                    fallback_ts=execution_ts,
                    fallback_price=live_price,
                    fallback_contract_size=size,
                    fallback_base_size=base_size,
                    contract_info=contract_info,
                    execution=execution,
                )
                self.trade_store.save(live_trades)

        return self._build_report(
            action=action if size else "skip",
            reason=(self._resolve_execution_reason(execution, default=reason) if size else "size_below_minimum"),
            signal_ts=effective_signal_ts,
            in_position=False,
            base_balance=0.0,
            quote_balance=available_margin,
            suggested_size=size,
            held_bars=0,
            latest_close=live_price,
            ema_fast=snapshot.ema_fast,
            ema_slow=snapshot.ema_slow,
            total_assets=total_assets,
            today_pnl=today_pnl,
            total_pnl=total_pnl,
            signal_frame=signal_frame,
            trades_df=self._build_trades_frame(live_trades, latest_close=live_price),
            execution=execution,
        )

    def _handle_exit(
        self,
        *,
        order_side: str,
        position_side: str,
        exit_reason: str,
        signal_frame: pd.DataFrame,
        snapshot,
        state: LiveState,
        live_trades: list[LiveTradeRecord],
        position: dict[str, float | str | None],
        total_assets: float,
        today_pnl: float,
        total_pnl: float,
        held_bars: int,
        contract_info: dict[str, float | str],
        quote_balance: float,
    ) -> CycleReport:
        size = format_size(abs(float(position["contracts"] or 0.0)), contract_info["lot_size"], contract_info["min_size"])
        execution = self._execute_order_if_needed(order_side=order_side, size=size, reduce_only=True)
        execution_ts = self._utc_now_iso()
        live_price = self._get_live_reference_price(self.config.symbol, fallback=snapshot.close)

        if execution is not None and execution.status not in {"dry_run"}:
            if not self._execution_is_rejected(execution):
                self.state_store.save(
                    LiveState(
                        last_buy_signal_ts=snapshot.ts.isoformat() if order_side == "buy" else state.last_buy_signal_ts,
                        last_sell_signal_ts=snapshot.ts.isoformat() if order_side == "sell" else state.last_sell_signal_ts,
                        entry_signal_ts=None,
                        entry_side=None,
                        last_order_cl_ord_id=execution.cl_ord_id,
                        strategy_started_at=state.strategy_started_at,
                        strategy_start_equity=state.strategy_start_equity,
                        day_anchor=state.day_anchor,
                        day_start_equity=state.day_start_equity,
                    )
                )
            if self._execution_has_fill(execution):
                self._record_close_trade(
                    live_trades,
                    signal_frame=signal_frame,
                    state=state,
                    position_side=position_side,
                    signal_ts=snapshot.ts.isoformat(),
                    fallback_ts=execution_ts,
                    fallback_price=live_price,
                    fallback_entry_price=float(position.get("avg_price") or 0.0),
                    fallback_contract_size=size,
                    fallback_base_size=float(position["base_qty"] or 0.0),
                    contract_info=contract_info,
                    execution=execution,
                    exit_reason=exit_reason,
                )
                self.trade_store.save(live_trades)

        return self._build_report(
            action=order_side if size else "skip",
            reason=(self._resolve_execution_reason(execution, default=exit_reason) if size else "size_below_minimum"),
            signal_ts=snapshot.ts.isoformat(),
            in_position=True,
            base_balance=float(position["signed_base_qty"] or 0.0),
            quote_balance=quote_balance,
            suggested_size=size,
            held_bars=held_bars,
            latest_close=live_price,
            ema_fast=snapshot.ema_fast,
            ema_slow=snapshot.ema_slow,
            total_assets=total_assets,
            today_pnl=today_pnl,
            total_pnl=total_pnl,
            signal_frame=signal_frame,
            trades_df=self._build_trades_frame(live_trades, latest_close=live_price),
            execution=execution,
        )

    def _build_report(
        self,
        *,
        action: str,
        reason: str,
        signal_ts: str,
        in_position: bool,
        base_balance: float,
        quote_balance: float,
        suggested_size: str | None,
        held_bars: int,
        latest_close: float,
        ema_fast: float,
        ema_slow: float,
        total_assets: float,
        today_pnl: float,
        total_pnl: float,
        signal_frame: pd.DataFrame,
        trades_df: pd.DataFrame,
        execution: OrderExecutionResult | None = None,
    ) -> CycleReport:
        return CycleReport(
            action=action,
            reason=reason,
            signal_ts=signal_ts,
            in_position=in_position,
            base_balance=float(base_balance),
            quote_balance=float(quote_balance),
            suggested_size=suggested_size,
            held_bars=int(held_bars),
            latest_close=float(latest_close),
            ema_fast=float(ema_fast),
            ema_slow=float(ema_slow),
            total_assets=float(total_assets),
            today_pnl=float(today_pnl),
            total_pnl=float(total_pnl),
            signal_frame=signal_frame.copy(),
            trades_df=trades_df.copy(),
            execution=execution,
        )

    def run_loop(self) -> None:
        while True:
            report = self.run_once()
            print(format_cycle_report(report, execute=self.config.execute))
            time.sleep(self.config.poll_interval_seconds)

    def _get_margin_snapshot(self, *, margin_ccy: str) -> tuple[float, float]:
        if self.config.quote_balance_override is not None:
            total_equity = float(self.config.quote_balance_override or 0.0)
            return total_equity, total_equity

        if self.credentials is None or self.order_router is None:
            raise RuntimeError("No credentials available. Provide overrides for dry-run or set OKX credentials.")

        balances = self.order_router.trade_client.get_balances([margin_ccy])
        row = balances.get(margin_ccy, {})
        available_margin = self._extract_float(row, "availEq", "availBal", "cashBal", "eq") or 0.0
        total_equity = self._extract_float(row, "eq", "cashBal", "availEq", "availBal") or available_margin
        return float(available_margin), float(total_equity)

    def _get_position_snapshot(self, *, contract_info: dict[str, float | str]) -> dict[str, float | str | None]:
        if self.config.base_balance_override is not None:
            signed_base_qty = float(self.config.base_balance_override or 0.0)
            contracts = self._base_qty_to_contracts(abs(signed_base_qty), contract_info)
            side = "long" if signed_base_qty > 0 else "short" if signed_base_qty < 0 else None
            return {
                "side": side,
                "contracts": abs(contracts),
                "signed_contracts": contracts if signed_base_qty >= 0 else -contracts,
                "base_qty": abs(signed_base_qty),
                "signed_base_qty": signed_base_qty,
            }

        if self.credentials is None or self.order_router is None:
            return {"side": None, "contracts": 0.0, "signed_contracts": 0.0, "base_qty": 0.0, "signed_base_qty": 0.0}

        positions = self.order_router.trade_client.get_positions(inst_id=self.config.symbol, inst_type="SWAP")
        signed_contracts = 0.0
        weighted_avg_price = 0.0
        total_abs_contracts = 0.0
        for row in positions:
            pos_value = self._extract_float(row, "pos", "availPos") or 0.0
            if pos_value == 0.0:
                continue
            pos_side = str(row.get("posSide") or "").strip().lower()
            avg_px = self._extract_float(row, "avgPx") or 0.0
            if pos_side == "long":
                signed_contracts += abs(pos_value)
            elif pos_side == "short":
                signed_contracts -= abs(pos_value)
            else:
                signed_contracts += pos_value
            if avg_px > 0.0:
                weighted_avg_price += abs(pos_value) * avg_px
                total_abs_contracts += abs(pos_value)

        side = "long" if signed_contracts > 0 else "short" if signed_contracts < 0 else None
        base_qty = self._contracts_to_base_qty(abs(signed_contracts), contract_info)
        signed_base_qty = base_qty if signed_contracts >= 0 else -base_qty
        avg_price = (weighted_avg_price / total_abs_contracts) if total_abs_contracts > 0 else 0.0
        return {
            "side": side,
            "contracts": abs(signed_contracts),
            "signed_contracts": signed_contracts,
            "base_qty": base_qty,
            "signed_base_qty": signed_base_qty,
            "avg_price": avg_price,
        }

    def _extract_contract_info(self, instrument: dict, *, latest_price: float) -> dict[str, float | str]:
        base_ccy = str(instrument.get("baseCcy") or "")
        quote_ccy = str(instrument.get("quoteCcy") or "")
        margin_ccy = str(instrument.get("settleCcy") or quote_ccy or "USDT")
        lot_size = str(instrument.get("lotSz") or "1")
        min_size = str(instrument.get("minSz") or lot_size)
        ct_val = float(instrument.get("ctVal") or 0.0)
        ct_val_ccy = str(instrument.get("ctValCcy") or base_ccy)

        if ct_val > 0.0 and ct_val_ccy.upper() == quote_ccy.upper() and latest_price > 0:
            base_per_contract = ct_val / latest_price
        else:
            base_per_contract = ct_val if ct_val > 0.0 else 1.0

        return {
            "base_ccy": base_ccy,
            "quote_ccy": quote_ccy,
            "margin_ccy": margin_ccy,
            "lot_size": lot_size,
            "min_size": min_size,
            "base_per_contract": float(base_per_contract),
        }

    def _allows_long(self) -> bool:
        return self.config.signal_side in {"long_only", "both"}

    def _allows_short(self) -> bool:
        return self.config.signal_side in {"short_only", "both"}

    def _infer_entry_signal_ts(self, signal_frame: pd.DataFrame, side: str | None) -> str | None:
        if side == "short":
            inferred = self._find_latest_cross_down_ts(signal_frame)
        else:
            inferred = find_latest_cross_up_ts(signal_frame)
        return inferred.isoformat() if inferred is not None else None

    @staticmethod
    def _find_latest_cross_down_ts(signal_frame: pd.DataFrame) -> pd.Timestamp | None:
        cross_downs = signal_frame.index[signal_frame["cross_down"]]
        if len(cross_downs) == 0:
            return None
        return cross_downs[-1]

    def _select_pending_entry_signal(
        self,
        signal_frame: pd.DataFrame,
        *,
        state: LiveState,
        snapshot,
        allow_resume_catchup: bool,
        live_trades: list[LiveTradeRecord],
    ) -> PendingEntrySignal | None:
        latest_ts = snapshot.ts.isoformat()
        # Only allow entry during the bar immediately after a confirmed cross.
        # Once the latest closed bar is no longer the cross bar, stale signals expire.
        if self._allows_long() and snapshot.cross_up and state.last_buy_signal_ts != latest_ts:
            return PendingEntrySignal(action="buy", trade_side="long", reason="golden_cross", signal_ts=latest_ts)
        if self._allows_short() and snapshot.cross_down and state.last_sell_signal_ts != latest_ts:
            return PendingEntrySignal(action="sell", trade_side="short", reason="dead_cross_short", signal_ts=latest_ts)
        return None

    def _with_live_preview_frame(self, report: CycleReport, *, signal_symbol: str) -> CycleReport:
        report.signal_frame = self._build_live_preview_signal_frame(report.signal_frame, signal_symbol=signal_symbol)
        return report

    def _build_live_preview_signal_frame(self, signal_frame: pd.DataFrame, *, signal_symbol: str) -> pd.DataFrame:
        if signal_frame.empty:
            return signal_frame.copy()
        try:
            recent = self.public_client.download_recent(
                signal_symbol,
                self.config.period,
                limit=2,
                include_unconfirmed=True,
            )
        except Exception:
            return signal_frame.copy()
        if recent.empty:
            return signal_frame.copy()
        try:
            latest_closed_ts = pd.Timestamp(signal_frame.index.max())
            latest_recent_ts = pd.Timestamp(recent.index.max())
        except Exception:
            return signal_frame.copy()
        if latest_recent_ts <= latest_closed_ts:
            return signal_frame.copy()

        base_columns = [name for name in ("open", "high", "low", "close", "volume", "turnover") if name in signal_frame.columns]
        if not base_columns:
            return signal_frame.copy()

        merged_bars = pd.concat([signal_frame[base_columns], recent[base_columns]], axis=0)
        merged_bars = merged_bars[~merged_bars.index.duplicated(keep="last")].sort_index().tail(len(signal_frame) + 1)

        preview_frame = add_strategy_columns(merged_bars, self.config.fast_ema, self.config.slow_ema)
        preview_frame["atr"] = atr(
            preview_frame["high"],
            preview_frame["low"],
            preview_frame["close"],
            max(int(self.config.atr_period), 1),
        )
        last_index = preview_frame.index[-1]
        preview_frame.at[last_index, "cross_up"] = False
        preview_frame.at[last_index, "cross_down"] = False
        return preview_frame

    def _latest_processed_signal_ts(self, state: LiveState) -> pd.Timestamp | None:
        candidates: list[pd.Timestamp] = []
        for raw in (state.last_buy_signal_ts, state.last_sell_signal_ts):
            normalized = self._normalize_timestamp_for_index(raw, None)
            if normalized is not None:
                candidates.append(normalized)
        if not candidates:
            return None
        return max(candidates)

    def _find_latest_cross_after(
        self,
        signal_frame: pd.DataFrame,
        column: str,
        *,
        after_ts: pd.Timestamp | None,
    ) -> pd.Timestamp | None:
        if signal_frame.empty or column not in signal_frame.columns:
            return None
        try:
            candidates = signal_frame.index[signal_frame[column].fillna(False)]
        except Exception:
            return None
        if len(candidates) == 0:
            return None
        if after_ts is None:
            return candidates[-1]
        cutoff = self._normalize_timestamp_for_index(after_ts, signal_frame.index)
        if cutoff is None:
            return candidates[-1]
        valid = candidates[candidates > cutoff]
        if len(valid) == 0:
            return None
        return valid[-1]

    def _normalize_timestamp_for_index(
        self,
        ts_value: str | pd.Timestamp | None,
        index: pd.Index | None,
    ) -> pd.Timestamp | None:
        if ts_value in (None, ""):
            return None
        try:
            stamp = pd.Timestamp(ts_value)
        except Exception:
            return None
        if index is None or not hasattr(index, "tz"):
            return stamp
        index_tz = getattr(index, "tz", None)
        if index_tz is not None and stamp.tzinfo is None:
            stamp = stamp.tz_localize(index_tz)
        elif index_tz is None and stamp.tzinfo is not None:
            stamp = stamp.tz_localize(None)
        elif index_tz is not None and stamp.tzinfo is not None:
            stamp = stamp.tz_convert(index_tz)
        return stamp

    def _locate_bar_index(self, signal_frame: pd.DataFrame, ts_value: str | None) -> int | None:
        if signal_frame.empty or not ts_value:
            return None
        try:
            stamp = pd.Timestamp(ts_value)
        except Exception:
            return None

        index_tz = getattr(signal_frame.index, "tz", None)
        if index_tz is not None and stamp.tzinfo is None:
            stamp = stamp.tz_localize(index_tz)
        elif index_tz is None and stamp.tzinfo is not None:
            stamp = stamp.tz_localize(None)

        located = int(signal_frame.index.get_indexer([stamp])[0])
        if located < 0:
            located = int(signal_frame.index.searchsorted(stamp))
        if 0 <= located < len(signal_frame):
            return located
        return None

    def _resolve_entry_stop_price(
        self,
        signal_frame: pd.DataFrame,
        *,
        entry_price: float,
        fallback_stop: float,
        trade_side: str,
    ) -> float:
        stop_mult = float(self.config.stop_loss_atr_multiplier or 0.0)
        if stop_mult <= 0.0 or signal_frame.empty or "atr" not in signal_frame.columns:
            return float(fallback_stop)
        try:
            atr_value = float(signal_frame["atr"].iloc[-1])
        except Exception:
            return float(fallback_stop)
        if not pd.notna(atr_value) or atr_value <= 0.0:
            return float(fallback_stop)
        stop_distance = atr_value * stop_mult
        return float(entry_price) + stop_distance if trade_side == "short" else float(entry_price) - stop_distance

    def _evaluate_exit(
        self,
        signal_frame: pd.DataFrame,
        *,
        entry_signal_ts: str | None,
        open_trade: LiveTradeRecord | None,
        position_side: str | None,
    ) -> ExitEvaluation:
        if self._is_trend_mode():
            if position_side == "short":
                return self._evaluate_short_trend_exit(signal_frame, entry_signal_ts=entry_signal_ts, open_trade=open_trade)
            return self._evaluate_long_trend_exit(signal_frame, entry_signal_ts=entry_signal_ts, open_trade=open_trade)

        if position_side == "short":
            ema_eval = self._evaluate_short_exit(signal_frame, entry_signal_ts)
            atr_eval = self._evaluate_short_atr_exit(signal_frame, entry_signal_ts=entry_signal_ts, open_trade=open_trade)
        else:
            ema_eval = (
                evaluate_long_exit(
                    signal_frame,
                    entry_signal_ts,
                    self.config.hold_bars,
                    stop_on_slow_ema=self.config.stop_on_slow_ema,
                    exit_on_dead_cross=self.config.exit_on_dead_cross,
                )
                if entry_signal_ts
                else ExitEvaluation(False, None, 0)
            )
            atr_eval = self._evaluate_long_atr_exit(signal_frame, entry_signal_ts=entry_signal_ts, open_trade=open_trade)

        if atr_eval.should_exit:
            return atr_eval
        if ema_eval.should_exit:
            return ema_eval
        return ExitEvaluation(False, None, max(ema_eval.held_bars, atr_eval.held_bars))

    def _evaluate_short_exit(self, signal_frame: pd.DataFrame, entry_signal_ts: str | None) -> ExitEvaluation:
        if signal_frame.empty:
            return ExitEvaluation(False, None, 0)

        latest = latest_signal_snapshot(signal_frame)
        entry_index = self._locate_bar_index(signal_frame, entry_signal_ts)
        if entry_index is None:
            if self.config.exit_on_dead_cross and latest.cross_up:
                return ExitEvaluation(True, "golden_cross_cover", 0)
            return ExitEvaluation(False, None, 0)

        held_bars = latest.bar_index - entry_index
        if self.config.exit_on_dead_cross and latest.cross_up:
            return ExitEvaluation(True, "golden_cross_cover", held_bars)
        if self.config.stop_on_slow_ema and latest.close > latest.ema_slow:
            return ExitEvaluation(True, "slow_ema_stop", held_bars)
        if self.config.hold_bars > 0 and held_bars >= self.config.hold_bars:
            return ExitEvaluation(True, "time_exit", held_bars)
        return ExitEvaluation(False, None, held_bars)

    def _evaluate_long_trend_exit(
        self,
        signal_frame: pd.DataFrame,
        *,
        entry_signal_ts: str | None,
        open_trade: LiveTradeRecord | None,
    ) -> ExitEvaluation:
        return self._evaluate_trend_exit(signal_frame, entry_signal_ts=entry_signal_ts, open_trade=open_trade, position_side="long")

    def _evaluate_short_trend_exit(
        self,
        signal_frame: pd.DataFrame,
        *,
        entry_signal_ts: str | None,
        open_trade: LiveTradeRecord | None,
    ) -> ExitEvaluation:
        return self._evaluate_trend_exit(signal_frame, entry_signal_ts=entry_signal_ts, open_trade=open_trade, position_side="short")

    def _evaluate_trend_exit(
        self,
        signal_frame: pd.DataFrame,
        *,
        entry_signal_ts: str | None,
        open_trade: LiveTradeRecord | None,
        position_side: str,
    ) -> ExitEvaluation:
        stop_mult = float(self.config.stop_loss_atr_multiplier or 0.0)
        if stop_mult <= 0.0 or signal_frame.empty or open_trade is None or "atr" not in signal_frame.columns:
            return ExitEvaluation(False, None, 0)

        entry_ref_ts = entry_signal_ts or open_trade.entry_ts
        entry_index = self._locate_bar_index(signal_frame, entry_ref_ts)
        if entry_index is None:
            return ExitEvaluation(False, None, 0)

        try:
            entry_atr = float(signal_frame["atr"].iloc[entry_index])
        except Exception:
            return ExitEvaluation(False, None, 0)
        if not pd.notna(entry_atr) or entry_atr <= 0.0:
            return ExitEvaluation(False, None, 0)

        entry_price = float(open_trade.entry_price or 0.0)
        if entry_price <= 0.0:
            fallback_index = min(entry_index + 1, len(signal_frame) - 1)
            entry_price = float(signal_frame["close"].iloc[fallback_index])

        stop_distance = entry_atr * stop_mult
        if stop_distance <= 0.0:
            return ExitEvaluation(False, None, 0)

        fee_buffer = self._trend_fee_buffer(entry_price=entry_price, position_side=position_side)
        current_stop = entry_price + stop_distance if position_side == "short" else entry_price - stop_distance

        for idx in range(entry_index + 1, len(signal_frame)):
            row = signal_frame.iloc[idx]
            held_bars = idx - entry_index
            if position_side == "short":
                if float(row["high"]) >= current_stop:
                    return ExitEvaluation(True, "trend_stop_loss", held_bars)
                achieved_r = int(max((entry_price - float(row["low"])) / stop_distance, 0.0))
                if achieved_r >= 1:
                    candidate_stop = entry_price - max(achieved_r - 1, 0) * stop_distance - fee_buffer
                    current_stop = min(current_stop, candidate_stop)
            else:
                if float(row["low"]) <= current_stop:
                    return ExitEvaluation(True, "trend_stop_loss", held_bars)
                achieved_r = int(max((float(row["high"]) - entry_price) / stop_distance, 0.0))
                if achieved_r >= 1:
                    candidate_stop = entry_price + max(achieved_r - 1, 0) * stop_distance + fee_buffer
                    current_stop = max(current_stop, candidate_stop)

        return ExitEvaluation(False, None, max(len(signal_frame) - 1 - entry_index, 0))

    def _evaluate_long_atr_exit(
        self,
        signal_frame: pd.DataFrame,
        *,
        entry_signal_ts: str | None,
        open_trade: LiveTradeRecord | None,
    ) -> ExitEvaluation:
        stop_mult = float(self.config.stop_loss_atr_multiplier or 0.0)
        if stop_mult <= 0.0 or signal_frame.empty or open_trade is None or "atr" not in signal_frame.columns:
            return ExitEvaluation(False, None, 0)

        entry_ref_ts = entry_signal_ts or open_trade.entry_ts
        entry_index = self._locate_bar_index(signal_frame, entry_ref_ts)
        if entry_index is None:
            return ExitEvaluation(False, None, 0)

        try:
            entry_atr = float(signal_frame["atr"].iloc[entry_index])
        except Exception:
            return ExitEvaluation(False, None, 0)
        if not pd.notna(entry_atr) or entry_atr <= 0.0:
            return ExitEvaluation(False, None, 0)

        entry_price = float(open_trade.entry_price or 0.0)
        if entry_price <= 0.0:
            fallback_index = min(entry_index + 1, len(signal_frame) - 1)
            entry_price = float(signal_frame["close"].iloc[fallback_index])

        stop_distance = entry_atr * stop_mult
        stop_price = entry_price - stop_distance
        take_profit_price = None
        take_mult = float(self.config.take_profit_r_multiple or 0.0)
        if take_mult > 0.0:
            take_profit_price = entry_price + stop_distance * take_mult

        for idx in range(entry_index + 1, len(signal_frame)):
            row = signal_frame.iloc[idx]
            stop_hit = float(row["low"]) <= stop_price
            take_profit_hit = take_profit_price is not None and float(row["high"]) >= take_profit_price
            held_bars = idx - entry_index

            if stop_hit and take_profit_hit:
                reason = "atr_stop_loss" if self.config.stop_first_on_ambiguous_bar else "atr_take_profit"
                return ExitEvaluation(True, reason, held_bars)
            if stop_hit:
                return ExitEvaluation(True, "atr_stop_loss", held_bars)
            if take_profit_hit:
                return ExitEvaluation(True, "atr_take_profit", held_bars)

        return ExitEvaluation(False, None, max(len(signal_frame) - 1 - entry_index, 0))

    def _evaluate_short_atr_exit(
        self,
        signal_frame: pd.DataFrame,
        *,
        entry_signal_ts: str | None,
        open_trade: LiveTradeRecord | None,
    ) -> ExitEvaluation:
        stop_mult = float(self.config.stop_loss_atr_multiplier or 0.0)
        if stop_mult <= 0.0 or signal_frame.empty or open_trade is None or "atr" not in signal_frame.columns:
            return ExitEvaluation(False, None, 0)

        entry_ref_ts = entry_signal_ts or open_trade.entry_ts
        entry_index = self._locate_bar_index(signal_frame, entry_ref_ts)
        if entry_index is None:
            return ExitEvaluation(False, None, 0)

        try:
            entry_atr = float(signal_frame["atr"].iloc[entry_index])
        except Exception:
            return ExitEvaluation(False, None, 0)
        if not pd.notna(entry_atr) or entry_atr <= 0.0:
            return ExitEvaluation(False, None, 0)

        entry_price = float(open_trade.entry_price or 0.0)
        if entry_price <= 0.0:
            fallback_index = min(entry_index + 1, len(signal_frame) - 1)
            entry_price = float(signal_frame["close"].iloc[fallback_index])

        stop_distance = entry_atr * stop_mult
        stop_price = entry_price + stop_distance
        take_profit_price = None
        take_mult = float(self.config.take_profit_r_multiple or 0.0)
        if take_mult > 0.0:
            take_profit_price = entry_price - stop_distance * take_mult

        for idx in range(entry_index + 1, len(signal_frame)):
            row = signal_frame.iloc[idx]
            stop_hit = float(row["high"]) >= stop_price
            take_profit_hit = take_profit_price is not None and float(row["low"]) <= take_profit_price
            held_bars = idx - entry_index

            if stop_hit and take_profit_hit:
                reason = "atr_stop_loss" if self.config.stop_first_on_ambiguous_bar else "atr_take_profit"
                return ExitEvaluation(True, reason, held_bars)
            if stop_hit:
                return ExitEvaluation(True, "atr_stop_loss", held_bars)
            if take_profit_hit:
                return ExitEvaluation(True, "atr_take_profit", held_bars)

        return ExitEvaluation(False, None, max(len(signal_frame) - 1 - entry_index, 0))

    def _execute_order_if_needed(self, *, order_side: str, size: str | None, reduce_only: bool) -> OrderExecutionResult | None:
        if size is None:
            return None
        if not self.config.execute:
            return OrderExecutionResult(
                cl_ord_id=f"dryrun-{order_side}",
                side=order_side,
                size=size,
                status="dry_run",
                response={},
            )
        if self.order_router is None:
            return None
        return self.order_router.place_swap_market_order(
            self.config.symbol,
            side=order_side,
            size=size,
            leverage=self.config.leverage,
            reduce_only=reduce_only,
        )

    def _hydrate_open_trade(
        self,
        trades: list[LiveTradeRecord],
        *,
        signal_frame: pd.DataFrame,
        state: LiveState,
        position_side: str | None,
        position_base_size: float,
        fallback_price: float,
    ) -> bool:
        if position_side is None or position_base_size <= 0 or self._find_open_trade(trades) is not None:
            return False

        entry_ts = state.entry_signal_ts or self._infer_entry_signal_ts(signal_frame, position_side)
        if not entry_ts:
            return False

        entry_price = float(fallback_price)
        trades.append(
            LiveTradeRecord(
                trade_id=f"recover-{position_side}-{entry_ts}",
                side=position_side,
                entry_ts=entry_ts,
                entry_price=entry_price,
                entry_size=max(float(position_base_size), 0.0),
                entry_fee=0.0,
                fees=0.0,
                exit_reason="position_open",
                status="open",
            )
        )
        return True

    def _record_open_trade(
        self,
        trades: list[LiveTradeRecord],
        *,
        trade_side: str,
        signal_frame: pd.DataFrame,
        signal_ts: str,
        fallback_ts: str,
        fallback_price: float,
        fallback_contract_size: str | None,
        fallback_base_size: float,
        contract_info: dict[str, float | str],
        execution: OrderExecutionResult,
    ) -> None:
        if self._find_open_trade(trades) is not None:
            return

        entry_contracts = self._execution_contract_size(execution, fallback=fallback_contract_size)
        entry_base_size = self._contracts_to_base_qty(entry_contracts, contract_info) if entry_contracts > 0 else fallback_base_size
        if entry_base_size <= 0:
            entry_base_size = fallback_base_size
        if entry_base_size <= 0:
            return

        entry_ts = self._execution_timestamp(execution) or fallback_ts
        entry_price = self._execution_price(execution) or float(fallback_price)
        entry_fee = self._execution_fee(execution)
        if entry_fee <= 0:
            entry_fee = self._estimate_fee(entry_price, entry_base_size)

        prefix = "buy" if trade_side == "long" else "sell"
        trades.append(
            LiveTradeRecord(
                trade_id=execution.cl_ord_id or f"{prefix}-{entry_ts}",
                side=trade_side,
                entry_ts=entry_ts,
                entry_price=entry_price,
                entry_size=entry_base_size,
                entry_fee=entry_fee,
                entry_order_id=execution.cl_ord_id,
                fees=entry_fee,
                exit_reason="position_open",
                status="open",
            )
        )

    def _record_close_trade(
        self,
        trades: list[LiveTradeRecord],
        *,
        signal_frame: pd.DataFrame,
        state: LiveState,
        position_side: str,
        signal_ts: str,
        fallback_ts: str,
        fallback_price: float,
        fallback_entry_price: float,
        fallback_contract_size: str | None,
        fallback_base_size: float,
        contract_info: dict[str, float | str],
        execution: OrderExecutionResult,
        exit_reason: str,
    ) -> None:
        open_trade = self._find_open_trade(trades)
        if open_trade is None:
            self._hydrate_open_trade(
                trades,
                signal_frame=signal_frame,
                state=state,
                position_side=position_side,
                position_base_size=fallback_base_size,
                fallback_price=float(fallback_entry_price or fallback_price),
            )
            open_trade = self._find_open_trade(trades)
        if open_trade is None:
            return

        exit_contracts = self._execution_contract_size(execution, fallback=fallback_contract_size)
        exit_base_size = self._contracts_to_base_qty(exit_contracts, contract_info) if exit_contracts > 0 else fallback_base_size
        if exit_base_size <= 0:
            exit_base_size = max(float(open_trade.entry_size), 0.0)

        exit_ts = self._execution_timestamp(execution) or fallback_ts
        exit_price = self._execution_price(execution) or float(fallback_price)
        exit_fee = self._execution_fee(execution)
        if exit_fee <= 0:
            exit_fee = self._estimate_fee(exit_price, exit_base_size)

        open_trade.exit_ts = exit_ts
        open_trade.exit_price = exit_price
        open_trade.exit_size = exit_base_size
        open_trade.exit_fee = exit_fee
        open_trade.exit_order_id = execution.cl_ord_id
        open_trade.exit_reason = exit_reason
        open_trade.fees = float(open_trade.entry_fee) + float(exit_fee)
        open_trade.pnl = self._calculate_trade_pnl(
            side=open_trade.side,
            entry_price=float(open_trade.entry_price),
            exit_price=float(exit_price),
            size=exit_base_size,
            fees=open_trade.fees,
        )
        open_trade.status = "closed"

    def _reconcile_flat_position_trades(
        self,
        trades: list[LiveTradeRecord],
        *,
        signal_frame: pd.DataFrame,
        fallback_price: float,
        signal_ts: str,
    ) -> bool:
        open_trade = self._find_open_trade(trades)
        if open_trade is None:
            return False
        open_trade.exit_ts = signal_ts
        open_trade.exit_price = None
        open_trade.exit_size = max(float(open_trade.entry_size or 0.0), 0.0)
        open_trade.exit_fee = 0.0
        open_trade.exit_reason = "flat_position_sync"
        open_trade.fees = float(open_trade.entry_fee or 0.0) + float(open_trade.exit_fee or 0.0)
        open_trade.pnl = None
        open_trade.status = "closed"
        return True

    @staticmethod
    def _find_open_trade(trades: list[LiveTradeRecord]) -> LiveTradeRecord | None:
        for trade in reversed(trades):
            if str(trade.status).lower() == "open" and not trade.exit_ts:
                return trade
        return None

    @staticmethod
    def _find_latest_closed_trade(trades: list[LiveTradeRecord]) -> LiveTradeRecord | None:
        for trade in reversed(trades):
            if str(trade.status).lower() != "open" or trade.exit_ts:
                return trade
        return None

    def _can_resume_regime_entry(
        self,
        signal_frame: pd.DataFrame,
        *,
        regime_side: str,
        latest_same_side_cross: pd.Timestamp | None,
        latest_opposite_cross: pd.Timestamp | None,
        processed_same_side_signal_ts: str | None,
        last_closed_trade: LiveTradeRecord | None,
    ) -> bool:
        if signal_frame.empty or latest_same_side_cross is None:
            return False

        latest_row = signal_frame.iloc[-1]
        ema_fast = float(latest_row.get("ema_fast", 0.0))
        ema_slow = float(latest_row.get("ema_slow", 0.0))
        if regime_side == "long":
            if not self._allows_long() or ema_fast <= ema_slow:
                return False
        else:
            if not self._allows_short() or ema_fast >= ema_slow:
                return False

        if latest_opposite_cross is not None and latest_same_side_cross <= latest_opposite_cross:
            return False

        processed_same = self._normalize_timestamp_for_index(processed_same_side_signal_ts, signal_frame.index)
        if processed_same is None or processed_same != latest_same_side_cross:
            return False

        if last_closed_trade is None or last_closed_trade.side != regime_side or not last_closed_trade.exit_ts:
            return False

        blocked_reasons = {"dead_cross", "golden_cross_cover", "flat_position_sync"}
        if str(last_closed_trade.exit_reason or "").strip() in blocked_reasons:
            return False

        last_exit_ts = self._normalize_timestamp_for_index(last_closed_trade.exit_ts, signal_frame.index)
        if last_exit_ts is None or last_exit_ts < latest_same_side_cross:
            return False

        return True

    def _build_trades_frame(self, trades: list[LiveTradeRecord], *, latest_close: float) -> pd.DataFrame:
        rows: list[dict] = []
        for trade in sorted(trades, key=lambda item: item.entry_ts or ""):
            fees = float(trade.fees or 0.0)
            if fees <= 0:
                fees = float(trade.entry_fee) + float(trade.exit_fee)

            pnl = trade.pnl
            exit_reason = trade.exit_reason or ""
            if str(trade.status).lower() == "open":
                if trade.entry_size > 0:
                    pnl = self._calculate_trade_pnl(
                        side=trade.side,
                        entry_price=float(trade.entry_price),
                        exit_price=float(latest_close),
                        size=float(trade.entry_size),
                        fees=fees,
                    )
                exit_reason = exit_reason or "position_open"

            rows.append(
                {
                    "side": trade.side,
                    "qty": float(trade.entry_size or 0.0),
                    "entry_ts": trade.entry_ts,
                    "entry_price": float(trade.entry_price),
                    "exit_ts": trade.exit_ts or "",
                    "exit_price": trade.exit_price,
                    "pnl": pnl,
                    "fees": fees,
                    "exit_reason": exit_reason,
                    "status": trade.status,
                }
            )

        return pd.DataFrame(rows)

    @staticmethod
    def _calculate_trade_pnl(*, side: str, entry_price: float, exit_price: float, size: float, fees: float) -> float:
        if side == "short":
            gross = entry_price * size - exit_price * size
        else:
            gross = exit_price * size - entry_price * size
        return float(gross) - float(fees)

    @staticmethod
    def _base_qty_to_contracts(base_qty: float, contract_info: dict[str, float | str]) -> float:
        base_per_contract = float(contract_info.get("base_per_contract") or 0.0)
        if base_qty <= 0 or base_per_contract <= 0:
            return 0.0
        return float(base_qty) / base_per_contract

    @staticmethod
    def _contracts_to_base_qty(contracts: float, contract_info: dict[str, float | str]) -> float:
        base_per_contract = float(contract_info.get("base_per_contract") or 0.0)
        if contracts <= 0 or base_per_contract <= 0:
            return 0.0
        return float(contracts) * base_per_contract

    @staticmethod
    def _extract_float(payload: dict | None, *keys: str) -> float | None:
        if not isinstance(payload, dict):
            return None
        for key in keys:
            raw = payload.get(key)
            if raw in (None, "", "0", "0.0"):
                continue
            try:
                return float(raw)
            except (TypeError, ValueError):
                continue
        return None

    def _execution_order_payload(self, execution: OrderExecutionResult | None) -> dict:
        if execution is None or not isinstance(execution.response, dict):
            return {}
        order = execution.response.get("order")
        return order if isinstance(order, dict) else execution.response

    def _execution_has_fill(self, execution: OrderExecutionResult | None) -> bool:
        if execution is None:
            return False
        order = self._execution_order_payload(execution)
        filled_size = self._extract_float(order, "accFillSz", "fillSz")
        if filled_size is not None and filled_size > 0:
            return True
        state = str(order.get("state") or execution.status or "").strip().lower()
        return state in {"filled", "partially_filled"}

    def _execution_is_rejected(self, execution: OrderExecutionResult | None) -> bool:
        if execution is None:
            return False
        order = self._execution_order_payload(execution)
        state = str(order.get("state") or execution.status or "").strip().lower()
        return state in {"canceled", "cancelled", "failed", "rejected"}

    def _resolve_execution_reason(self, execution: OrderExecutionResult | None, *, default: str) -> str:
        if execution is None or execution.status == "dry_run":
            return default
        if self._execution_is_rejected(execution):
            return "order_not_filled"
        if not self._execution_has_fill(execution):
            return "order_wait_fill"
        return default

    def _execution_price(self, execution: OrderExecutionResult | None) -> float | None:
        order = self._execution_order_payload(execution)
        return self._extract_float(order, "avgPx", "fillPx", "px")

    def _execution_contract_size(self, execution: OrderExecutionResult | None, *, fallback: str | None = None) -> float:
        order = self._execution_order_payload(execution)
        size = self._extract_float(order, "accFillSz", "fillSz", "sz")
        if size is not None and size > 0:
            return size
        try:
            return float((fallback or "").strip() or 0.0)
        except ValueError:
            return 0.0

    def _execution_fee(self, execution: OrderExecutionResult | None) -> float:
        order = self._execution_order_payload(execution)
        fee = self._extract_float(order, "fillFee", "fee")
        return abs(float(fee or 0.0))

    def _execution_timestamp(self, execution: OrderExecutionResult | None) -> str | None:
        order = self._execution_order_payload(execution)
        for key in ("fillTime", "uTime", "cTime"):
            raw = order.get(key) if isinstance(order, dict) else None
            if raw in (None, ""):
                continue
            text = str(raw).strip()
            if text.isdigit():
                try:
                    return pd.to_datetime(int(text), unit="ms", utc=True).isoformat()
                except Exception:
                    continue
            try:
                return pd.Timestamp(text).isoformat()
            except Exception:
                continue
        return None

    def _lookup_price(self, signal_frame: pd.DataFrame, ts_value: str, *, fallback: float) -> float:
        if signal_frame.empty:
            return float(fallback)
        try:
            stamp = pd.Timestamp(ts_value)
        except Exception:
            return float(fallback)

        index_tz = getattr(signal_frame.index, "tz", None)
        if index_tz is not None and stamp.tzinfo is None:
            stamp = stamp.tz_localize(index_tz)
        elif index_tz is None and stamp.tzinfo is not None:
            stamp = stamp.tz_localize(None)

        located = int(signal_frame.index.get_indexer([stamp])[0])
        if located < 0:
            located = int(signal_frame.index.searchsorted(stamp))
        if 0 <= located < len(signal_frame):
            try:
                return float(signal_frame["close"].iloc[located])
            except Exception:
                return float(fallback)
        return float(fallback)

    def _get_live_reference_price(self, symbol: str, *, fallback: float) -> float:
        try:
            return float(self.public_client.get_ticker_last(symbol))
        except Exception:
            return float(fallback)

    @staticmethod
    def _utc_now_iso() -> str:
        return pd.Timestamp.now(tz="UTC").isoformat()

    def _estimate_fee(self, price: float, size: float) -> float:
        if price <= 0 or size <= 0:
            return 0.0
        return abs(price * size) * float(self.config.fee_bps) / 10_000.0

    def _trend_fee_buffer(self, *, entry_price: float, position_side: str) -> float:
        fee_rate = float(self.config.fee_bps or 0.0) / 10_000.0
        if fee_rate <= 0.0 or entry_price <= 0.0:
            return 0.0
        if position_side == "short":
            breakeven = entry_price * (1.0 - fee_rate) / max(1.0 + fee_rate, 1e-12)
            return max(entry_price - breakeven, 0.0)
        breakeven = entry_price * (1.0 + fee_rate) / max(1.0 - fee_rate, 1e-12)
        return max(breakeven - entry_price, 0.0)

    def _is_range_mode(self) -> bool:
        return str(self.config.entry_mode or "range").strip().lower() != "trend"

    def _is_trend_mode(self) -> bool:
        return not self._is_range_mode()

    def _refresh_equity_baselines(self, state: LiveState, *, total_assets: float) -> tuple[LiveState, float, float]:
        local_today = pd.Timestamp.now(tz="Asia/Shanghai").date().isoformat()
        changed = False

        if state.strategy_start_equity is None:
            state.strategy_started_at = pd.Timestamp.now(tz="Asia/Shanghai").isoformat()
            state.strategy_start_equity = float(total_assets)
            changed = True

        if state.day_start_equity is None or state.day_anchor != local_today:
            state.day_anchor = local_today
            state.day_start_equity = float(total_assets)
            changed = True

        if changed:
            self.state_store.save(state)

        today_pnl = float(total_assets) - float(state.day_start_equity or total_assets)
        total_pnl = float(total_assets) - float(state.strategy_start_equity or total_assets)
        return state, today_pnl, total_pnl

    def _refresh_account_identity(self) -> None:
        self.detected_account_name = ""
        self.detected_account_uid = ""
        if self.credentials is None or self.order_router is None:
            return
        try:
            identity = self.order_router.trade_client.get_account_identity()
        except Exception:
            return
        self.detected_account_name = str(identity.get("account_name") or "").strip()
        self.detected_account_uid = str(identity.get("account_uid") or "").strip()


def format_cycle_report(report: CycleReport, *, execute: bool) -> str:
    lines = [
        f"[{report.signal_ts}] 动作={_translate_action(report.action)} 原因={_translate_reason(report.reason)}",
        f"持仓={report.in_position} 合约口径持仓={report.base_balance:.8f} 可用保证金={report.quote_balance:.2f} 持有K线={report.held_bars}",
        f"收盘价={report.latest_close:.4f} 快线EMA={report.ema_fast:.4f} 慢线EMA={report.ema_slow:.4f} 下单数量={report.suggested_size or '-'}",
    ]
    if report.execution is not None:
        lines.append(f"模式={'实盘执行' if execute else '模拟预演'} 订单号={report.execution.cl_ord_id} 状态={report.execution.status}")
    else:
        lines.append(f"模式={'实盘执行' if execute else '模拟预演'} 未发送订单")
    return "\n".join(lines)


def _translate_action(action: str) -> str:
    mapping = {
        "buy": "准备买入",
        "sell": "准备卖出",
        "hold": "继续观察",
        "skip": "跳过执行",
    }
    return mapping.get(action, action)


def _translate_reason(reason: str) -> str:
    mapping = {
        "golden_cross": "EMA21 上穿 EMA55",
        "dead_cross": "EMA21 下穿 EMA55",
        "dead_cross_short": "EMA21 下穿 EMA55，准备开空",
        "golden_cross_cover": "EMA21 上穿 EMA55，准备平空",
        "bullish_regime_resume": "多头趋势仍在，空仓后允许再次开多",
        "bearish_regime_resume": "空头趋势仍在，空仓后允许再次开空",
        "slow_ema_stop": "价格反穿慢线，触发止损",
        "atr_stop_loss": "触发 ATR 止损",
        "atr_take_profit": "触发 ATR 止盈",
        "trend_stop_loss": "触发趋势移动止损",
        "flat_position_sync": "检测到空仓，已自动纠正（盈亏待确认）",
        "position_open_no_exit": "当前持仓中，暂未触发平仓",
        "no_entry_signal": "当前没有新的入场信号",
        "order_wait_fill": "订单已提交，等待实际成交",
        "order_not_filled": "订单未成交或已被撤销",
        "size_below_minimum": "下单数量低于最小合约张数",
        "time_exit": "达到时间退出条件",
        "exit_signal": "触发平仓条件",
    }
    return mapping.get(reason, reason)
