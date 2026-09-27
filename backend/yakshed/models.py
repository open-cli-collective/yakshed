from __future__ import annotations

from datetime import datetime, timezone
import json
import uuid
from typing import Any


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def new_id(prefix: str = "") -> str:
    value = uuid.uuid4().hex
    return f"{prefix}{value}" if prefix else value


def dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def loads(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


def scrub(value: Any, secrets: tuple[str, ...] = ()) -> Any:
    """Remove likely secret values before provider payloads cross the store seam."""
    if isinstance(value, str):
        result = value
        for secret in secrets:
            if secret:
                result = result.replace(secret, "[REDACTED]")
        return result
    if isinstance(value, list):
        return [scrub(item, secrets) for item in value]
    if isinstance(value, dict):
        hidden = {
            "apikey",
            "accesstoken",
            "refreshtoken",
            "authorization",
            "password",
            "clientsecret",
            "privatekey",
            "secret",
            "bearer",
        }
        return {
            key: "[REDACTED]"
            if "".join(character for character in str(key).lower() if character.isalnum()) in hidden
            else scrub(item, secrets)
            for key, item in value.items()
        }
    return value
