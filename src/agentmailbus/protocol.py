"""JSON frames shared by the broker, the relay, and the local socket."""

from __future__ import annotations

import json
import re
import uuid
from typing import Any

MAX_TEXT = 32_000
MAX_FRAME = 256 * 1024
PRIORITIES = ("next", "now", "later")
HARNESSES = ("claude", "dsh", "grok", "orca")
NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


class ProtocolError(ValueError):
    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        super().__init__(detail or code)


def dumps(obj: dict[str, Any]) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def loads(raw: str | bytes) -> dict[str, Any]:
    if isinstance(raw, bytes):
        if len(raw) > MAX_FRAME:
            raise ProtocolError("too-large")
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ProtocolError("bad-json") from exc
    else:
        text = raw
        if len(text.encode("utf-8")) > MAX_FRAME:
            raise ProtocolError("too-large")
    try:
        obj = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ProtocolError("bad-json") from exc
    if not isinstance(obj, dict):
        raise ProtocolError("bad-json")
    return obj


def check_name(value: object, field: str = "name") -> str:
    if not isinstance(value, str) or NAME.fullmatch(value) is None:
        raise ProtocolError("bad-name", field)
    return value


def check_id(value: object) -> str:
    """A message id is a UUID in its canonical text form."""
    if not isinstance(value, str):
        raise ProtocolError("bad-id")
    try:
        parsed = uuid.UUID(value)
    except ValueError as exc:
        raise ProtocolError("bad-id") from exc
    if str(parsed) != value:
        raise ProtocolError("bad-id")
    return value


def check_text(value: object) -> str:
    if not isinstance(value, str) or value.strip() == "":
        raise ProtocolError("empty-text")
    if len(value) > MAX_TEXT:
        raise ProtocolError("too-large")
    return value


def check_priority(value: object) -> str:
    if value is None or value == "":
        return "next"
    if not isinstance(value, str) or value not in PRIORITIES:
        raise ProtocolError("bad-priority")
    return value


def check_harness(value: object) -> str:
    if not isinstance(value, str) or value not in HARNESSES:
        raise ProtocolError("bad-harness")
    return value
