from __future__ import annotations

import json
import time
from urllib.parse import urlencode

import requests

from live.auth import rest_timestamp, sign_rest
from live.config import OKXCredentials


def _describe_http_error_response(response: requests.Response | None) -> str:
    if response is None:
        return "unknown http error"
    try:
        payload = response.json()
    except Exception:
        payload = None
    if payload is not None:
        return json.dumps(payload, ensure_ascii=False)
    text = (response.text or "").strip()
    if text:
        return text
    reason = (response.reason or "").strip()
    if reason:
        return reason
    return f"url={response.url}"


class OKXTradeClient:
    def __init__(
        self,
        credentials: OKXCredentials,
        timeout: int = 45,
        connect_timeout: int = 15,
        max_retries: int = 5,
        retry_delay: float = 1.2,
    ) -> None:
        self.credentials = credentials
        self.timeout = timeout
        self.connect_timeout = connect_timeout
        self.max_retries = max(1, int(max_retries))
        self.retry_delay = max(0.0, float(retry_delay))
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "okx-ema-live/0.1",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    def get_balances(self, currencies: list[str]) -> dict[str, dict]:
        params = {"ccy": ",".join(currencies)}
        payload = self._request("GET", "/api/v5/account/balance", params=params)
        details = payload["data"][0].get("details", [])
        return {row["ccy"]: row for row in details}

    def get_account_config(self) -> dict:
        payload = self._request("GET", "/api/v5/account/config")
        data = payload.get("data", [])
        first = data[0] if data else {}
        return first if isinstance(first, dict) else {}

    def get_account_identity(self) -> dict[str, str]:
        config = self.get_account_config()
        uid = str(config.get("uid") or config.get("mainUid") or "").strip()
        raw_name = ""
        for key in ("label", "subAcct", "subAcctName", "name", "alias", "acctName"):
            value = str(config.get(key) or "").strip()
            if value:
                raw_name = value
                break
        account_name = raw_name or (f"UID {uid}" if uid else "")
        return {"account_name": account_name, "account_uid": uid}

    def get_positions(self, *, inst_id: str | None = None, inst_type: str = "SWAP") -> list[dict]:
        params = {"instType": inst_type}
        if inst_id:
            params["instId"] = inst_id
        payload = self._request("GET", "/api/v5/account/positions", params=params)
        data = payload.get("data", [])
        return data if isinstance(data, list) else []

    def set_leverage(self, *, inst_id: str, leverage: int | float, mgn_mode: str = "isolated") -> dict:
        payload = self._request(
            "POST",
            "/api/v5/account/set-leverage",
            payload={"instId": inst_id, "lever": str(leverage), "mgnMode": mgn_mode},
        )
        data = payload.get("data", [])
        first = data[0] if data else {}
        if isinstance(first, dict) and first.get("sCode", "0") not in {"0", 0}:
            raise RuntimeError(f"Set leverage failed: {first}")
        return first if isinstance(first, dict) else {}

    def place_order(self, order: dict) -> dict:
        payload = self._request("POST", "/api/v5/trade/order", payload=order)
        if not payload.get("data"):
            raise RuntimeError(f"Unexpected place_order response: {payload}")
        first = payload["data"][0]
        if first.get("sCode") != "0":
            raise RuntimeError(f"Order rejected: {first}")
        return first

    def get_order(self, inst_id: str, *, ord_id: str | None = None, cl_ord_id: str | None = None) -> dict:
        if not ord_id and not cl_ord_id:
            raise ValueError("ord_id or cl_ord_id is required")
        params = {"instId": inst_id}
        if ord_id:
            params["ordId"] = ord_id
        if cl_ord_id:
            params["clOrdId"] = cl_ord_id
        payload = self._request("GET", "/api/v5/trade/order", params=params)
        data = payload.get("data", [])
        return data[0] if data else {}

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        payload: dict | None = None,
    ) -> dict:
        params = params or {}
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True) if payload is not None else ""
        query_string = f"?{urlencode(params)}" if params else ""
        request_path = f"{path}{query_string}"
        last_error: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            timestamp = rest_timestamp()
            headers = {
                "OK-ACCESS-KEY": self.credentials.api_key,
                "OK-ACCESS-SIGN": sign_rest(self.credentials.secret_key, timestamp, method, request_path, body),
                "OK-ACCESS-TIMESTAMP": timestamp,
                "OK-ACCESS-PASSPHRASE": self.credentials.passphrase,
            }
            if self.credentials.simulated:
                headers["x-simulated-trading"] = "1"

            try:
                response = self.session.request(
                    method=method.upper(),
                    url=f"{self.credentials.base_url}{path}",
                    params=params or None,
                    data=body or None,
                    headers=headers,
                    timeout=(self.connect_timeout, self.timeout),
                )
                response.raise_for_status()
                result = response.json()
                if result.get("code") != "0":
                    raise RuntimeError(f"OKX request failed: {result}")
                return result
            except requests.exceptions.HTTPError as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    status_code = exc.response.status_code if exc.response is not None else "?"
                    detail = _describe_http_error_response(exc.response)
                    raise RuntimeError(f"OKX HTTP {status_code}: {detail}") from exc
            except requests.exceptions.Timeout as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    raise RuntimeError(
                        f"连接 OKX 超时，已重试 {self.max_retries} 次仍未成功。请稍后重试。原始错误: {exc}"
                    ) from exc
            except requests.exceptions.RequestException as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    raise RuntimeError(
                        f"连接 OKX 失败，已重试 {self.max_retries} 次仍未成功。请检查网络或稍后重试。原始错误: {exc}"
                    ) from exc
            if self.retry_delay > 0:
                time.sleep(self.retry_delay)

        if last_error is not None:
            raise RuntimeError(f"OKX 请求失败：{last_error}") from last_error
        raise RuntimeError("OKX 请求失败：未知错误。")
