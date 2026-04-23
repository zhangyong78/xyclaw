from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re
import time

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
        margin_mode: str = "isolated",
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
        leverage_notice = None
        if self.execute:
            leverage_notice = self._prepare_swap_leverage(symbol=symbol, leverage=leverage, margin_mode=margin_mode)
        result = self._submit(payload, inst_type="SWAP")
        if leverage_notice:
            result.response["leverage_notice"] = leverage_notice
        return result

    def place_swap_limit_order(
        self,
        symbol: str,
        *,
        side: str,
        size: str,
        price: str,
        leverage: int,
        margin_mode: str = "isolated",
        reduce_only: bool = False,
    ) -> OrderExecutionResult:
        payload = {
            "instId": symbol,
            "tdMode": margin_mode,
            "side": side,
            "ordType": "limit",
            "px": price,
            "sz": size,
            "reduceOnly": reduce_only,
            "clOrdId": self._build_cl_ord_id("B" if side == "buy" else "S"),
        }
        leverage_notice = None
        if self.execute:
            leverage_notice = self._prepare_swap_leverage(symbol=symbol, leverage=leverage, margin_mode=margin_mode)
        result = self._submit(payload, inst_type="SWAP")
        if leverage_notice:
            result.response["leverage_notice"] = leverage_notice
        return result

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

        ws_client: OKXPrivateWebSocket | None = OKXPrivateWebSocket(self.credentials, timeout=self.order_timeout_seconds)
        transport_notice: str | None = None
        try:
            try:
                ws_client.connect()
                ws_client.subscribe_orders(inst_type=inst_type, inst_id=payload["instId"])
            except Exception as exc:
                transport_notice = self._transport_notice_from_exception(exc)
                if ws_client is not None:
                    try:
                        ws_client.close()
                    except Exception:
                        pass
                ws_client = None

            order_ack = self.trade_client.place_order(payload)
            order_update = None
            if ws_client is not None:
                try:
                    order_update = ws_client.wait_for_order(payload["clOrdId"], timeout=self.order_timeout_seconds)
                except Exception as exc:
                    transport_notice = self._merge_notices(transport_notice, self._transport_notice_from_exception(exc))

            if order_update is None:
                order_update = self._poll_order_via_rest(
                    inst_id=payload["instId"],
                    cl_ord_id=payload["clOrdId"],
                    attempts=max(3, min(self.order_timeout_seconds, 8)),
                )

            response_payload = {"ack": order_ack, "order": order_update}
            if transport_notice:
                response_payload["transport_notice"] = transport_notice
            return OrderExecutionResult(
                cl_ord_id=payload["clOrdId"],
                side=payload["side"],
                size=payload["sz"],
                status=order_update.get("state") or "submitted",
                response=response_payload,
            )
        finally:
            if ws_client is not None:
                try:
                    ws_client.close()
                except Exception:
                    pass

    def _prepare_swap_leverage(self, *, symbol: str, leverage: int, margin_mode: str) -> str | None:
        try:
            self.trade_client.set_leverage(inst_id=symbol, leverage=leverage, mgn_mode=margin_mode)
        except Exception as exc:
            if self._can_skip_swap_leverage_error(exc, margin_mode=margin_mode):
                return "当前账户为 PM 账户，OKX 不支持在全仓永续上单独调整杠杆；程序已跳过设杠杆，继续按账户现有设置发单。"
            raise self._rewrite_swap_leverage_error(exc, symbol=symbol, leverage=leverage, margin_mode=margin_mode) from exc
        return None

    @staticmethod
    def _can_skip_swap_leverage_error(exc: Exception, *, margin_mode: str) -> bool:
        message = str(exc)
        lowered = message.lower()
        if str(margin_mode).strip().lower() != "cross":
            return False
        return "51039" in lowered or ("pm account" in lowered and "leverage cannot be adjusted" in lowered)

    @staticmethod
    def _rewrite_swap_leverage_error(exc: Exception, *, symbol: str, leverage: int, margin_mode: str) -> RuntimeError:
        message = str(exc).strip()
        lowered = message.lower()
        code = OrderRouter._extract_okx_code(message)
        normalized_mode = str(margin_mode).strip().lower()
        if normalized_mode == "cross":
            mode_text = "全仓"
        elif normalized_mode == "isolated":
            mode_text = "逐仓"
        else:
            mode_text = str(margin_mode).strip() or "当前"

        if code == "59102" or "leverage exceeds the maximum limit" in lowered:
            return RuntimeError(
                f"{symbol} {mode_text}杠杆设置为 {int(leverage)}x 失败："
                f"OKX 提示当前账户该合约允许的最大杠杆低于 {int(leverage)}x。"
                "请把“永续杠杆倍数”调低后再试，"
                "或先到 OKX App / 网页端确认该合约在当前账户模式下允许的最大杠杆。"
                f"原始错误：{message}"
            )

        return RuntimeError(
            f"{symbol} {mode_text}杠杆设置失败，当前请求为 {int(leverage)}x。原始错误：{message}"
        )

    @staticmethod
    def _extract_okx_code(message: str) -> str:
        patterns = [
            r"'code'\s*:\s*'([^']+)'",
            r'"code"\s*:\s*"([^"]+)"',
        ]
        for pattern in patterns:
            matched = re.search(pattern, message)
            if matched:
                return str(matched.group(1) or "").strip()
        return ""

    def _poll_order_via_rest(self, *, inst_id: str, cl_ord_id: str, attempts: int) -> dict:
        last_order: dict = {}
        for attempt in range(max(int(attempts), 1)):
            try:
                last_order = self.trade_client.get_order(inst_id, cl_ord_id=cl_ord_id)
            except Exception:
                last_order = {}
            if last_order:
                return last_order
            if attempt < max(int(attempts), 1) - 1:
                time.sleep(0.8)
        return last_order

    @staticmethod
    def _transport_notice_from_exception(exc: Exception) -> str:
        message = str(exc)
        lowered = message.lower()
        if "10054" in lowered or "forcibly closed" in lowered or "远程主机强迫关闭了一个现有的连接" in message:
            return "OKX 私有回报连接被远端中断，程序已自动改用 REST 查询订单状态。"
        return f"OKX 私有回报通道异常，程序已自动改用 REST 查询订单状态。原始原因：{message}"

    @staticmethod
    def _merge_notices(current: str | None, extra: str | None) -> str | None:
        left = (current or "").strip()
        right = (extra or "").strip()
        if left and right:
            return left if right in left else f"{left}\n{right}"
        return left or right or None

    @staticmethod
    def _build_cl_ord_id(prefix: str) -> str:
        stamp = datetime.now(timezone.utc).strftime("%y%m%d%H%M%S%f")[:-3]
        return f"ema58{prefix}{stamp}"[:32]
