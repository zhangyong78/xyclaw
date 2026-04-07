from __future__ import annotations

import time
from datetime import timezone

import pandas as pd
import requests


class OKXPublicClient:
    DEFAULT_BASE_URL = "https://www.okx.com"

    def __init__(
        self,
        base_url: str | None = None,
        timeout: int = 45,
        connect_timeout: int = 15,
        max_retries: int = 5,
        retry_delay: float = 1.2,
    ) -> None:
        self.base_url = (base_url or self.DEFAULT_BASE_URL).rstrip("/")
        self.timeout = timeout
        self.connect_timeout = connect_timeout
        self.max_retries = max(1, int(max_retries))
        self.retry_delay = max(0.0, float(retry_delay))
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "okx-ema-backtest/0.1",
                "Accept": "application/json",
            }
        )

    def history_candles(
        self,
        inst_id: str,
        bar: str = "1H",
        limit: int = 100,
        after: str | None = None,
    ) -> list[list[str]]:
        params = {
            "instId": inst_id,
            "bar": _normalize_okx_bar(bar),
            "limit": str(limit),
        }
        if after:
            params["after"] = after

        payload = self._get_json("/api/v5/market/history-candles", params=params)
        if payload.get("code") != "0":
            raise RuntimeError(f"OKX error: {payload}")
        return payload.get("data", [])

    def get_instrument(self, inst_id: str, inst_type: str = "SPOT") -> dict:
        params = {"instType": inst_type, "instId": inst_id}
        payload = self._get_json("/api/v5/public/instruments", params=params)
        if payload.get("code") != "0":
            raise RuntimeError(f"OKX error: {payload}")
        data = payload.get("data", [])
        if not data:
            raise RuntimeError(f"Instrument not found: {inst_id}")
        return data[0]

    def _get_json(self, path: str, *, params: dict | None = None) -> dict:
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(
                    f"{self.base_url}{path}",
                    params=params,
                    timeout=(self.connect_timeout, self.timeout),
                )
                response.raise_for_status()
                return response.json()
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
                        f"连接 OKX 失败，已重试 {self.max_retries} 次仍未成功。请检查网络后重试。原始错误: {exc}"
                    ) from exc
            if self.retry_delay > 0:
                time.sleep(self.retry_delay)

        if last_error is not None:
            raise RuntimeError(f"OKX 请求失败：{last_error}") from last_error
        raise RuntimeError("OKX 请求失败：未知错误。")

    def download_history(
        self,
        inst_id: str,
        bar: str,
        bars: int,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        start_ts = _to_millis(start) if start else None
        end_ts = _to_millis(end, end_of_day=True) if end else None

        collected: list[list[str]] = []
        cursor_after: str | None = None

        while len(collected) < bars:
            batch = self.history_candles(
                inst_id=inst_id,
                bar=bar,
                limit=min(100, bars - len(collected)),
                after=cursor_after,
            )
            if not batch:
                break

            collected.extend(batch)
            oldest_ts = min(int(row[0]) for row in batch)
            cursor_after = str(oldest_ts)

            if start_ts is not None and oldest_ts <= start_ts:
                break

        df = _rows_to_frame(collected)
        if df.empty:
            return df

        if start_ts is not None:
            df = df[df.index >= pd.to_datetime(start_ts, unit="ms", utc=True)]
        if end_ts is not None:
            df = df[df.index <= pd.to_datetime(end_ts, unit="ms", utc=True)]
        return df.sort_index().tail(bars)


def _rows_to_frame(rows: list[list[str]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume", "turnover"])

    normalized = []
    for row in rows:
        if len(row) < 6:
            continue
        normalized.append(
            {
                "ts": pd.to_datetime(int(row[0]), unit="ms", utc=True),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
                "turnover": float(row[7]) if len(row) > 7 else 0.0,
                "confirm": row[8] if len(row) > 8 else "1",
            }
        )

    frame = pd.DataFrame(normalized).drop_duplicates(subset=["ts"]).set_index("ts").sort_index()
    if "confirm" in frame.columns:
        frame = frame[frame["confirm"] == "1"]
        frame = frame.drop(columns=["confirm"])
    return frame


def _normalize_okx_bar(bar: str) -> str:
    raw = str(bar or "").strip()
    mapping = {
        "24H": "1D",
        "24h": "1D",
        "1d": "1D",
    }
    return mapping.get(raw, raw)


def _to_millis(value: str, end_of_day: bool = False) -> int:
    timestamp = pd.Timestamp(value, tz=timezone.utc)
    if end_of_day:
        timestamp = timestamp + pd.Timedelta(days=1) - pd.Timedelta(milliseconds=1)
    return int(timestamp.timestamp() * 1000)
