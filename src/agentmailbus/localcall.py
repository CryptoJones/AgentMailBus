"""One-shot client for the relay's Unix socket."""

from __future__ import annotations

import socket
from pathlib import Path

from agentmailbus.protocol import dumps, loads


def local_rpc(path: Path, payload: dict, timeout: float = 20) -> dict:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        sock.connect(str(path))
        sock.sendall((dumps(payload) + "\n").encode())
        buf = b""
        while b"\n" not in buf:
            chunk = sock.recv(65536)
            if not chunk:
                break
            buf += chunk
    if not buf:
        return {"ok": False, "error": "no-reply"}
    return loads(buf.split(b"\n", 1)[0])
