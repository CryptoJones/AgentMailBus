"""Local delivery. Receiving never goes through an MCP tool."""

from __future__ import annotations

import hashlib
import json
import os
import socket
from pathlib import Path

# Claude Code listens on this socket. The frame matches the protocol verified
# 2026-10-03: an auth line, then a user line. The peer token file is
# ~/.claude/sessions/<pid>.<sha256 of the socket path>.key


def claude_socket(pid: int) -> str:
    uid = os.getuid()
    name = f"{pid}.sock"
    candidates: list[str] = []
    xdg = os.environ.get("XDG_RUNTIME_DIR")
    if xdg:
        candidates.append(os.path.join(xdg, "cc-socks", name))
    tmp = os.path.join("/tmp", "cc-socks", name)
    if len(tmp.encode()) > 103:
        tmp = os.path.join(f"/tmp/cc-socks-{uid}", name)
    candidates.append(tmp)
    short = os.path.join(f"/tmp/cc-socks-{uid}", name)
    if short not in candidates:
        candidates.append(short)
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    return candidates[0]


def peer_token(sock_path: str, pid: int) -> str | None:
    digest = hashlib.sha256(sock_path.encode()).hexdigest()
    path = Path.home() / ".claude" / "sessions" / f"{pid}.{digest}.key"
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    token = data.get("peerToken")
    if isinstance(token, str) and token:
        return token
    return None


def claude_frames(binding: dict, message: dict) -> list[bytes]:
    sock_path = binding.get("socket") or claude_socket(int(binding["pid"]))
    body = (
        '<cross-session-message from="{host}" from-name="{name}" from-mode="agentmailbus">\n'
        "{text}\n"
        "</cross-session-message>"
    ).format(
        host=message.get("from_host") or "agentmailbus",
        name=message["from"],
        text=message["text"],
    )
    user = {
        "type": "user",
        "message": {"content": body},
        "from": f"uds:{binding.get('relay_socket') or sock_path}",
        "priority": message.get("priority") or "next",
        "msg_id": message["id"],
    }
    frames = []
    token = peer_token(sock_path, int(binding["pid"]))
    if token:
        frames.append(_line({"type": "auth", "token": token}))
    frames.append(_line(user))
    return frames


def deliver_claude(binding: dict, message: dict) -> bool:
    sock_path = binding.get("socket") or claude_socket(int(binding["pid"]))
    frames = claude_frames(binding, message)
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(5)
            sock.connect(sock_path)
            for frame in frames:
                sock.sendall(frame)
            sock.shutdown(socket.SHUT_WR)
    except OSError:
        return False
    return True


def deliver_wake(binding: dict, message: dict) -> bool:
    path = Path(binding["wake"]).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    line = json.dumps(
        {
            "id": message["id"],
            "from": message["from"],
            "from_host": message.get("from_host") or "",
            "to": message["to"],
            "ts": message.get("created_at") or message.get("ts") or "",
            "text": message["text"],
        },
        ensure_ascii=False,
    )
    fd = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.write(fd, (line + "\n").encode())
        os.fsync(fd)
    finally:
        os.close(fd)
    os.chmod(path, 0o600)
    return True


def deliver_dsh(binding: dict, message: dict) -> bool:
    """One JSON line on the session inbox socket.

    The frame is AgentMailBus's contract. Confirm it against a live dsh before
    treating it as that program's native protocol.
    """
    payload = _line(
        {
            "type": "user",
            "id": message["id"],
            "from": message["from"],
            "from_host": message.get("from_host") or "",
            "text": message["text"],
        }
    )
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(5)
            sock.connect(binding["socket"])
            sock.sendall(payload)
            sock.shutdown(socket.SHUT_WR)
    except OSError:
        return False
    return True


def default_adapters() -> dict:
    return {
        "claude": deliver_claude,
        "dsh": deliver_dsh,
        "grok": deliver_wake,
        "orca": deliver_wake,
    }


def _line(obj: dict) -> bytes:
    return (json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
