from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from live.config import OKXCredentials
from live.okx_rest import OKXTradeClient
from live.okx_ws import OKXPrivateWebSocket


@dataclass(slots=True)
class OrderExecutionResult:
    cl_ord_id: str
    side: str
    size: str
    status: str
    response: dict


class OrderRouter:
    def __init__(self, credentials: OKXCredentials, execute: bool, order_timeout_seconds: int = 25) -> None:
        self.credentials = credentials
        self.execute = execute
        self.order_timeout_seconds = order_timeout_seconds
        self.trade_client = OKXTradeClient(credentials)

    def place_swap_market_order(
        self,
        symbol: str,
        *,
        side: str,
        size: str,
        leverage: int,
        margin_mode: str = "cross",
        reduce_only: bool = False,
    ) -> OrderExecutionResult:
        payload = {
            "instId": symbol,
            "tdMode": margin_mode,
            "side": side,
            "ordType": "market",
            "sz": size,
            "reduceOnly": reduce_only,
            "clOrdId": self._build_cl_ord_id("B" if side == "buy" else "S"),
        }
        if self.execute:
            self.trade_client.set_leverage(inst_id=symbol, leverage=leverage, mgn_mode=margin_mode)
        return self._submit(payload, inst_type="SWAP")

    def place_spot_market_buy(self, symbol: str, size: str) -> OrderExecutionResult:
        payload = {
            "instId": symbol,
            "tdMode": "cash",
            "side": "buy",
            "ordType": "market",
            "tgtCcy": "base_ccy",
            "sz": size,
            "clOrdId": self._build_cl_ord_id("B"),
        }
        return self._submit(payload, inst_type="SPOT")

    def place_spot_market_sell(self, symbol: str, size: str) -> OrderExecutionResult:
        payload = {
            "instId": symbol,
            "tdMode": "cash",
            "side": "sell",
            "ordType": "market",
            "sz": size,
            "clOrdId": self._build_cl_ord_id("S"),
        }
        return self._submit(payload, inst_type="SPOT")

    def _submit(self, payload: dict, *, inst_type: str = "SPOT") -> OrderExecutionResult:
        if not self.execute:
            return OrderExecutionResult(
                cl_ord_id=payload["clOrdId"],
                side=payload["side"],
                size=payload["sz"],
                status="dry_run",
                response=payload,
            )

        ws_client = OKXPrivateWebSocket(self.credentials, timeout=self.order_timeout_seconds)
        try:
            ws_client.connect()
            ws_client.subscribe_orders(inst_type=inst_type, inst_id=payload["instId"])

            order_ack = self.trade_client.place_order(payload)
            order_update = ws_client.wait_for_order(payload["clOrdId"], timeout=self.order_timeout_seconds)
            if order_update is None:
                order_update = self.trade_client.get_order(payload["instId"], cl_ord_id=payload["clOrdId"])

            return OrderExecutionResult(
                cl_ord_id=payload["clOrdId"],
                side=payload["side"],
                size=payload["sz"],
                status=order_update.get("state") or "submitted",
                response={"ack": order_ack, "order": order_update},
            )
        finally:
            ws_client.close()

    @staticmethod
    def _build_cl_ord_id(prefix: str) -> str:
        stamp = datetime.now(timezone.utc).strftime("%y%m%d%H%M%S%f")[:-3]
        return f"ema58{prefix}{stamp}"[:32]
