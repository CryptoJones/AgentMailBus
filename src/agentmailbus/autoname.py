"""A bus name for a Claude Code session that was started without `--from`.

Registered once at user scope, `agentmailbus mcp --harness claude` runs in every
session on the host, so the name has to come from the session itself:

1. `AGENTMAILBUS_NAME`, when it is set and not empty, for an interactive session.
2. The `name` Claude Code gives the session, a dash, the first eight characters of
   its session id, a dot, and the relay's host: `rea-4b-54e558bf.ronin28`. Claude
   Code's own names repeat (a folder name and two hex digits), and the broker keeps
   mail for a name it has seen, so the session id keeps a new session from taking
   an old one's mail. A resumed session keeps its id, and so its name.

Only an interactive session whose record Claude Code has written joins. A session
started by a script (`claude -p`, the Agent SDK), or one with no usable record,
stays off the bus; a script that should join passes `--from` in its own config.
`AGENTMAILBUS_NAME` is inherited by every process a session starts, which is why a
script does not honour it, and why a name another live session holds is not taken.
"""

from __future__ import annotations

import json
import os
import re
import socket
import time
import tomllib
from pathlib import Path

from agentmailbus.protocol import ProtocolError, check_name

ENV = "AGENTMAILBUS_NAME"
_BAD = re.compile(r"[^A-Za-z0-9._-]")


def relay_host(cfg: dict) -> str:
    """The host a relay with this config announces. The relay uses this too."""
    return str(cfg.get("host") or socket.gethostname().split(".")[0])


def config_host(path: Path) -> str:
    try:
        cfg = tomllib.loads(path.expanduser().read_text())
    except (OSError, ValueError):
        cfg = {}
    return relay_host(cfg)


def sessions_dir() -> Path:
    """Claude Code keeps its state under CLAUDE_CONFIG_DIR when that is set. The MCP
    server inherits the session's environment, so it looks where the session wrote."""
    base = os.environ.get("CLAUDE_CONFIG_DIR")
    return ((Path(base).expanduser() if base else Path.home() / ".claude") / "sessions").absolute()


def read_session(pid: int, wait: float = 3.0, directory: Path | None = None) -> dict:
    """Claude Code's record of session `pid`, or {} when there is none.

    Claude Code writes it around the time it starts MCP servers, so wait a moment.
    A record that names another pid is ignored.
    """
    path = (directory or sessions_dir()) / f"{pid}.json"
    deadline = time.monotonic() + wait
    while True:
        try:
            data = json.loads(path.read_text())
            # Claude Code rewrites the record as the session starts; wait for one
            # that says what kind of session this is.
            if isinstance(data, dict) and data.get("pid") == pid and "entrypoint" in data and not _stale(data, pid):
                return data
        except (OSError, ValueError):
            pass
        if time.monotonic() >= deadline:
            return {}
        time.sleep(0.1)


def _stale(session: dict, pid: int) -> bool:
    """A record left by an earlier process that had the same pid. Linux only: the
    record's `procStart` (a string of digits) is the process's start time."""
    from agentmailbus.registry import started_elsewhen

    return started_elsewhen(pid, session.get("procStart"))


def interactive(session: dict) -> bool:
    """A record Claude Code wrote for a session a person is using. `claude -p` and
    the Agent SDK write an `sdk-…` entrypoint; no record at all also counts as no."""
    entry = session.get("entrypoint")
    return isinstance(entry, str) and bool(entry) and not entry.startswith("sdk")


def pick(pid: int, session: dict, env_name: str, live: dict[str, dict], relay_config: Path) -> str | None:
    """The name for Claude session `pid`, or None to stay off the bus."""
    if not interactive(session):
        return None
    if env_name:
        name = check_name(env_name, ENV)
        holder = live.get(name)
        if holder is None or holder.get("pid") == pid:
            return name
        # Inherited from a session that still holds it: do not take its mail.
    return auto_name(pid, config_host(relay_config), live, session)


def clean(text: str) -> str:
    """Only characters a name allows, starting with a letter or digit."""
    return _BAD.sub("-", text).lstrip("._-")


def _tagged(base: str, tag: str, host: str) -> str:
    """`base-tag.host` in at most 64 characters. The tag always survives; the base
    is cut first, then a very long host, so at least one base character stays."""
    tag = clean(tag)[:40]
    host = clean(host)[: 64 - len(tag) - 4].rstrip("._-")
    room = 64 - len(tag) - 1 - (len(host) + 1 if host else 0)
    head = clean(base)[:room].rstrip("._-") or "c"
    return f"{head}-{tag}" + (f".{host}" if host else "")


def auto_name(
    pid: int,
    host: str,
    live: dict[str, dict],
    session: dict,
) -> str:
    """The derived name for session `pid`: its Claude name tagged with its session
    id. If another live session on this host still holds that, the pid is added."""
    raw = session.get("name")
    base = clean(raw) if isinstance(raw, str) else ""
    base = base or "claude"
    sid = session.get("sessionId")
    tag = (clean(sid.replace("-", ""))[:8] if isinstance(sid, str) else "") or str(pid)
    tags = [tag, f"{tag}-{pid}"] + [f"{tag}-{pid}-{n}" for n in range(2, 100)]
    for candidate in tags:
        name = _tagged(base, candidate, host)
        holder = live.get(name)
        if holder is None or holder.get("pid") == pid:
            return check_name(name, "from")
    raise ProtocolError("name-taken", "every candidate name is held by another session")


def folder_key(folder: str) -> str:
    """Twelve hex digits of SHA-256 of the folder's real path. The plugin computes
    the same from the `directory` opencode gives it."""
    import hashlib

    # The plugin sees the folder as text; bytes that are not UTF-8 reach it as U+FFFD.
    seen = os.fsencode(os.path.realpath(folder)).decode("utf-8", "replace")
    return hashlib.sha256(seen.encode("utf-8")).hexdigest()[:12]


MAX_SOCKET_PATH = 103  # bytes: macOS allows 104 with the NUL, Linux 108


def opencode_socket(pid: int, folder: str) -> str:
    """Where the opencode plugin for process `pid` and project `folder` listens:
    <data dir>/opencode/<pid>-<folder key>.sock. One opencode process can serve
    several folders, and each gets its own socket and name."""
    from agentmailbus.paths import data_dir

    return str((data_dir() / "opencode" / f"{pid}-{folder_key(folder)}.sock").absolute())


def process_started(pid: int) -> str:
    """When `pid` started, as text: clock ticks on Linux, `ps -o lstart=` elsewhere,
    "" when neither can be read."""
    from agentmailbus.registry import start_time

    ticks = start_time(pid)
    if ticks is not None:
        return str(ticks)
    import subprocess

    try:
        # The same locale and zone wherever it runs, so the text compares.
        out = subprocess.run(
            ["ps", "-o", "lstart=", "-p", str(pid)],
            capture_output=True, text=True, timeout=5, check=False,
            env={**os.environ, "LC_ALL": "C", "TZ": "UTC"},
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    return out.stdout.strip() if out.returncode == 0 else ""


def opencode_tag(pid: int, folder: str) -> str:
    """`oc` and eight hex digits from the process (pid and start time) and folder.
    The broker keeps mail for a name it has seen, so a later process that reuses
    the pid must not get the same name. With no start time to read, the tag is
    random, and the name lasts only as long as this server."""
    import hashlib
    import uuid

    started = process_started(pid) or uuid.uuid4().hex
    key = f"{pid}:{started}:".encode() + os.fsencode(os.path.realpath(folder))
    return "oc" + hashlib.sha256(key).hexdigest()[:8]


def sweep_opencode_sockets(directory: Path | None = None) -> int:
    """Remove plugin sockets (`<pid>-<folder key>.sock`) whose opencode process is
    gone. opencode does not run the plugin's exit handler when a signal kills it."""
    import stat

    from agentmailbus.paths import data_dir
    from agentmailbus.registry import pid_alive

    gone = 0
    for path in (directory or data_dir() / "opencode").glob("*.sock"):
        try:
            owner = int(path.stem.split("-", 1)[0])
            if owner > 0 and not pid_alive(owner) and stat.S_ISSOCK(path.lstat().st_mode):
                path.unlink()
                gone += 1
        except (ValueError, OSError):
            continue
    return gone


def opencode_name(
    pid: int, env_name: str, held: dict[str, dict], relay_config: Path, folder: str, socket_path: str = ""
) -> str:
    """The name for opencode process `pid` in `folder`: AGENTMAILBUS_NAME unless
    another session holds it, else the folder's name and the process tag,
    `rea-oc1a2b3c4d.ronin28`. `held` is every name a live process holds, live
    or still waiting for its plugin. A holder is this session only when it has the
    same process and the same socket: one opencode process serves several folders."""
    socket_path = socket_path or opencode_socket(pid, folder)

    def mine(holder: dict | None) -> bool:
        return holder is None or (holder.get("pid") == pid and holder.get("socket") == socket_path)

    if env_name:
        name = check_name(env_name, ENV)
        if mine(held.get(name)):
            return name
    base = clean(os.path.basename(os.path.realpath(folder))) or "opencode"
    host = config_host(relay_config)
    tag = opencode_tag(pid, folder)
    for candidate in [tag] + [f"{tag}-{n}" for n in range(2, 100)]:
        name = _tagged(base, candidate, host)
        if mine(held.get(name)):
            return check_name(name, "from")
    raise ProtocolError("name-taken", "every candidate name is held by another session")


def reused(pid: int, start: object, harness: str = "opencode") -> bool:
    """True when `start` names another process than the one now at `pid`. On Linux
    it is clock ticks; off Linux an opencode binding holds the `ps -o lstart=` text,
    which costs a `ps` and so is checked only here, not on every liveness scan. A
    Claude binding's start time is Claude Code's own and is judged as before."""
    from agentmailbus.registry import started_elsewhen

    if harness == "opencode" and isinstance(start, str) and start and not start.isdigit():
        now = process_started(pid)
        return bool(now) and now != start
    return started_elsewhen(pid, start)


def claude_socket_from(session: dict) -> str:
    """The socket path Claude Code reports for the session, when it reports one."""
    path = session.get("messagingSocketPath")
    if isinstance(path, str) and os.path.isabs(path):
        return path
    return ""


class registry_lock:
    """An exclusive lock on the registry directory, held while a name is chosen and
    saved or removed. Waits at most `timeout` seconds, then raises TimeoutError. On a
    filesystem that cannot lock at all (some NFS and FUSE mounts), it goes ahead
    unlocked and sets `locked` to False, so the caller skips the sweep."""

    def __init__(self, directory: Path, timeout: float = 10.0) -> None:
        self.directory = directory
        self.timeout = timeout
        self.fd = -1
        self.locked = False

    def __enter__(self) -> "registry_lock":
        import fcntl

        from agentmailbus.paths import private_dir

        private_dir(self.directory)
        try:
            fd = os.open(self.directory / ".lock", os.O_RDWR | os.O_CREAT, 0o600)
        except OSError:
            return self  # no lock file to be had: go ahead unlocked
        deadline = time.monotonic() + self.timeout
        try:
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self.locked = True
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("registry lock") from None
                    time.sleep(0.05)
                except OSError:
                    break  # this filesystem cannot lock
        except BaseException:
            os.close(fd)
            raise
        self.fd = fd
        return self

    def __exit__(self, *exc) -> None:
        import fcntl

        if self.fd < 0:
            return
        if self.locked:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
        os.close(self.fd)


def sweep_dead(directory: Path) -> int:
    """Remove Claude and opencode bindings whose session process is gone, and
    temporary files of writers that died. Run under the registry lock. Returns how
    many went."""
    from agentmailbus.registry import pid_alive

    gone = 0
    for path in directory.glob("*.json"):
        try:
            data = json.loads(path.read_text())
            if not isinstance(data, dict) or data.get("harness") not in {"claude", "opencode"}:
                continue
            owner = int(data.get("pid") or 0)
        except (OSError, ValueError, TypeError, OverflowError):
            continue
        if owner > 0 and (not pid_alive(owner) or reused(owner, data.get("proc_start"), data["harness"])):
            path.unlink(missing_ok=True)
            gone += 1
    for path in directory.glob(".*.json.*.tmp"):
        try:
            writer = int(path.name.rsplit(".", 2)[-2])
        except (ValueError, IndexError):
            continue
        if not pid_alive(writer):
            path.unlink(missing_ok=True)
            gone += 1
    return gone
