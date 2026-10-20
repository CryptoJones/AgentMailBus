"""Local delivery. Receiving never goes through an MCP tool.

None of these sockets answer. cc-socks sends no reply, and the dsh inbox is this
project's own contract. "Delivered" therefore means the bytes were written, not that
the session accepted them. PROTOCOL.md says the same.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import socket
import stat
import tempfile
import threading
import uuid
from pathlib import Path

from agentmailbus.paths import private_dir

# Claude Code listens on this socket. The frame matches the protocol verified
# 2026-10-03: an auth line, then a user line. The peer token file is
# ~/.claude/sessions/<pid>.<sha256 of the socket path>.key

log = logging.getLogger("agentmailbus.adapters")

_TAG = re.compile(r"<(/?)cross-session-message", re.IGNORECASE)


def claude_socket(pid: int) -> str:
    """Where Claude Code puts its socket: $XDG_RUNTIME_DIR, then the temp dir.

    Claude Code moves to `cc-socks-<uid>` when the temp-dir path would exceed the
    Unix socket path limit. The same rule is applied to the same temp dir here.
    """
    uid = os.getuid()
    name = f"{pid}.sock"
    candidates: list[str] = []
    xdg = os.environ.get("XDG_RUNTIME_DIR")
    if xdg:
        candidates.append(os.path.join(xdg, "cc-socks", name))
    tmpdir = tempfile.gettempdir()
    tmp = os.path.join(tmpdir, "cc-socks", name)
    if len(tmp.encode()) > 103:
        tmp = os.path.join(tmpdir, f"cc-socks-{uid}", name)
    for candidate in (tmp, os.path.join("/tmp", "cc-socks", name), os.path.join("/tmp", f"cc-socks-{uid}", name)):
        if candidate not in candidates:
            candidates.append(candidate)
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


def escape_text(text: str) -> str:
    """Stop a message from closing or opening the wrapper tag around it."""
    return _TAG.sub(r"&lt;\1cross-session-message", text)


def claude_frames(binding: dict, message: dict) -> list[bytes]:
    sock_path = binding.get("socket") or claude_socket(int(binding["pid"]))
    body = (
        '<cross-session-message from="{host}" from-name="{name}" from-mode="agentmailbus">\n'
        "{text}\n"
        "</cross-session-message>"
    ).format(
        host=message.get("from_host") or "agentmailbus",
        name=message["from"],
        text=escape_text(message["text"]),
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


_wake_locks: dict[str, threading.Lock] = {}
_wake_locks_guard = threading.Lock()
_WAKE_LOCKS_CAP = 256


def _wake_lock(path: Path) -> threading.Lock:
    """One lock per wake path. Two names may share a file; their writes never overlap.

    The table is pruned of locks nobody holds once it passes the cap, so a wrapper
    that mints a new path per session does not grow it without bound.
    """
    key = os.path.realpath(path)
    with _wake_locks_guard:
        lock = _wake_locks.get(key)
        if lock is None:
            if len(_wake_locks) >= _WAKE_LOCKS_CAP:
                for other, held in list(_wake_locks.items()):
                    if not held.locked():
                        del _wake_locks[other]
            lock = _wake_locks[key] = threading.Lock()
        return lock


def deliver_wake(binding: dict, message: dict) -> bool:
    path = Path(binding["wake"]).expanduser()
    private_dir(path.parent)
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
    with _wake_lock(path):
        return _append_line(path, (line + "\n").encode())


def _append_line(path: Path, payload: bytes) -> bool:
    data = memoryview(payload)
    fd = _open_wake(path)
    if fd is None:
        return False
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            return False
        start = _terminated(fd)
        done = 0
        try:
            while data:
                written = os.write(fd, data)
                if written <= 0:
                    raise OSError("short write")
                data = data[written:]
                done += written
        except OSError:
            # Part of a line is in the file. Take it back out, but only if nothing
            # else landed after it; otherwise end what is there with a newline so a
            # line reader moves past it. Either way this delivery is not complete.
            if done:
                _unwrite(fd, path, start, done)
            return False
        try:
            os.fsync(fd)
        except OSError as exc:
            # The line is written and a reader may already have it. A retry would
            # append it twice, so this is a delivered message with a logged sync error.
            log.warning("fsync failed for %s: %s", path, type(exc).__name__)
    finally:
        try:
            os.close(fd)
        except OSError as exc:
            log.warning("close failed for %s: %s", path, type(exc).__name__)
    return True


def _terminated(fd: int) -> int:
    """Make sure the file ends on a newline (a crash can leave a torn tail) and
    return the offset this line will start at."""
    size = os.fstat(fd).st_size
    if size and os.pread(fd, 1, size - 1) != b"\n":
        os.write(fd, b"\n")
        size += 1
    return size


def _unwrite(fd: int, path: Path, start: int, done: int) -> None:
    try:
        if os.fstat(fd).st_size == start + done:
            os.ftruncate(fd, start)
            return
    except OSError:
        pass
    try:
        os.write(fd, b"\n")
    except OSError:
        pass
    log.warning("partial wake line left in %s", path)


_WAKE_FLAGS = os.O_RDWR | os.O_APPEND | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0)


def _open_wake(path: Path) -> int | None:
    """Open the wake file for append; create it if missing.

    A new file is created under a private temporary name, made 0600 through its
    descriptor whatever the umask, and then linked into place, so the wake path
    never names a half-made file. An existing file keeps its mode. O_NONBLOCK keeps
    a FIFO without a reader from hanging the open; O_NOFOLLOW refuses a symlink.
    """
    try:
        return os.open(path, _WAKE_FLAGS)
    except FileNotFoundError:
        pass
    except OSError:
        return None
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp")
    try:
        fd = os.open(tmp, os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
    except OSError:
        return None
    try:
        os.fchmod(fd, 0o600)
        try:
            os.link(tmp, path)
        except FileExistsError:
            pass  # another writer made it first; open theirs below
        except OSError:
            return None
    finally:
        os.close(fd)
        try:
            os.unlink(tmp)
        except OSError:
            pass
    try:
        return os.open(path, _WAKE_FLAGS)
    except OSError:
        return None


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
