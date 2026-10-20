"""LAN allow-list and the shared token."""

from __future__ import annotations

import hashlib
import hmac
import ipaddress
import subprocess
from pathlib import Path

DEFAULT_ALLOW = (
    "127.0.0.0/8",
    "::1/128",
    "10.0.0.0/8",
    "172.16.0.0/12",
    "192.168.0.0/16",
    "100.64.0.0/10",
)


def parse_allow(items: list[str] | tuple[str, ...]) -> tuple[ipaddress._BaseNetwork, ...]:
    return tuple(ipaddress.ip_network(item) for item in items)


def ip_allowed(host: str, nets: tuple[ipaddress._BaseNetwork, ...]) -> bool:
    if host.startswith("::ffff:"):
        host = host[7:]
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        return False
    return any(addr in net for net in nets)


def tokens_equal(got: object, expected: str) -> bool:
    if not isinstance(got, str) or expected == "":
        return False
    left = hashlib.sha256(got.encode()).digest()
    right = hashlib.sha256(expected.encode()).digest()
    return hmac.compare_digest(left, right)


def load_secret(spec: str) -> str:
    """Read a token from `pass:entry` or `file:/path`. Never accepts a raw token."""
    if spec.startswith("pass:"):
        entry = spec[5:]
        if entry == "" or entry.startswith("-"):
            raise SystemExit("pass entry name is empty")
        proc = subprocess.run(
            ["pass", "show", entry],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            raise SystemExit(f"pass show {entry} failed")
        lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
        if not lines:
            raise SystemExit(f"pass entry {entry} is empty")
        return lines[0]
    if spec.startswith("file:"):
        path = Path(spec[5:]).expanduser()
        try:
            lines = [line.strip() for line in path.read_text().splitlines() if line.strip()]
        except OSError as exc:
            raise SystemExit(f"cannot read token file {path}") from exc
        if not lines:
            raise SystemExit(f"token file {path} is empty")
        return lines[0]
    raise SystemExit("token must be pass:NAME or file:PATH")
