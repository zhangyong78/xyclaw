from __future__ import annotations

import json
import time

import websocket

from live.auth import sign_ws_login, ws_timestamp
from live.config import OKXCredentials


class OKXPrivateWebSocket:
    def __init__(self, credentials: OKXCredentials, timeout: int = 20) -> None:
        self.credentials = credentials
        self.timeout = timeout
        self.ws: websocket.WebSocket | None = None

    def connect(self) -> None:
        self.ws = websocket.create_connection(self.credentials.ws_private_url, timeout=self.timeout)
        self._login()

    def subscribe_orders(self, inst_type: str = "SPOT", inst_id: str | None = None) -> None:
        args = {"channel": "orders", "instType": inst_type}
        if inst_id:
            args["instId"] = inst_id
        self._send({"op": "subscribe", "args": [args]})
        message = self._receive_until(lambda payload: payload.get("event") == "subscribe", timeout=self.timeout)
        if message.get("code", "0") != "0":
            raise RuntimeError(f"Orders subscribe failed: {message}")

    def wait_for_order(self, cl_ord_id: str, timeout: int = 25) -> dict | None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            remaining = max(deadline - time.time(), 1)
            payload = self._receive(timeout=remaining)
            if payload is None:
                continue
            if "data" not in payload:
                continue
            for row in payload["data"]:
                if row.get("clOrdId") == cl_ord_id:
                    return row
        return None

    def close(self) -> None:
        if self.ws is not None:
            try:
                self.ws.close()
            finally:
                self.ws = None

    def _login(self) -> None:
        timestamp = ws_timestamp()
        self._send(
            {
                "op": "login",
                "args": [
                    {
                        "apiKey": self.credentials.api_key,
                        "passphrase": self.credentials.passphrase,
                        "timestamp": timestamp,
                        "sign": sign_ws_login(self.credentials.secret_key, timestamp),
                    }
                ],
            }
        )
        message = self._receive_until(lambda payload: payload.get("event") == "login", timeout=self.timeout)
        if message.get("code", "0") != "0":
            raise RuntimeError(f"WebSocket login failed: {message}")

    def _receive_until(self, predicate, timeout: int) -> dict:
        deadline = time.time() + timeout
        while time.time() < deadline:
            remaining = max(deadline - time.time(), 1)
            payload = self._receive(timeout=remaining)
            if payload is None:
                continue
            if predicate(payload):
                return payload
        raise TimeoutError("Timed out waiting for WebSocket event")

    def _receive(self, timeout: float) -> dict | None:
        if self.ws is None:
            raise RuntimeError("WebSocket is not connected")
        self.ws.settimeout(timeout)
        try:
            raw = self.ws.recv()
        except websocket.WebSocketTimeoutException:
            return None
        if not raw:
            return None
        payload = json.loads(raw)
        if payload.get("event") == "error":
            raise RuntimeError(f"WebSocket error: {payload}")
        return payload

    def _send(self, payload: dict) -> None:
        if self.ws is None:
            raise RuntimeError("WebSocket is not connected")
        self.ws.send(json.dumps(payload, separators=(",", ":"), ensure_ascii=True))
