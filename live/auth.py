from __future__ import annotations

import base64
import hashlib
import hmac
import time
from datetime import datetime, timezone


def rest_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def ws_timestamp() -> str:
    return str(int(time.time()))


def sign(secret_key: str, payload: str) -> str:
    digest = hmac.new(secret_key.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def sign_rest(secret_key: str, timestamp: str, method: str, request_path: str, body: str = "") -> str:
    prehash = f"{timestamp}{method.upper()}{request_path}{body}"
    return sign(secret_key, prehash)


def sign_ws_login(secret_key: str, timestamp: str) -> str:
    return sign(secret_key, f"{timestamp}GET/users/self/verify")

