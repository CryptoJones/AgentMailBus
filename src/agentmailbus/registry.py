"""Session bindings. A live binding is the only thing that may send or receive."""

from __future__ import annotations

import json
import logging
import os
import stat
from pathlib import Path

from agentmailbus.adapters import claude_socket, expand_wake, wake_problem
from agentmailbus.paths import private_dir
from agentmailbus.protocol import HARNESSES, ProtocolError, check_harness, check_name

BINDING_FIELDS = ("harness", "pid", "wake", "socket")

log = logging.getLogger("agentmailbus.registry")
_said: dict[str, None] = {}  # insertion-ordered: the oldest entry goes first
_SAID_CAP = 1024


def _say_once(key: str, message: str, *args: object) -> None:
    """The liveness scan runs every second; a dead binding is reported once per
    name and path. The table is bounded so a host with a thousand dead session
    files cannot grow it without end; past the bound the oldest is forgotten and
    would be reported again."""
    if key in _said:
        return
    while len(_said) >= _SAID_CAP:
        del _said[next(iter(_said))]
    _said[key] = None
    log.warning(message, *args)


def pid_alive(pid: int) -> bool:
    """True only for a running process this user may signal. Another user's pid is not ours."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError):
        return False
    except (OverflowError, ValueError, OSError):
        # A pid the platform cannot represent is not a process of ours.
        return False
    return True


def start_time(pid: int) -> int | None:
    """When process `pid` started, in clock ticks since boot (field 22 of
    /proc/<pid>/stat). Linux only; None elsewhere or when it cannot be read."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except (OSError, ValueError):
        return None
    fields = stat[stat.rfind(")") + 2 :].split()
    # fields[0] is field 3 (state), so field 22, the start time, is fields[19].
    if len(fields) <= 19:
        return None
    try:
        return int(fields[19])
    except ValueError:
        return None


def started_elsewhen(pid: int, start: object) -> bool:
    """True when `start`, a process start time in clock ticks, is not when process
    `pid` started: the pid now belongs to another process. Linux only; elsewhere,
    or with no readable start time to compare, it is False."""
    if start is None:
        return False
    now = start_time(pid)
    if now is None:
        return False
    try:
        return now != int(str(start))
    except ValueError:
        return False  # a start time that cannot be read says nothing either way


def save_binding(directory: Path, binding: dict) -> Path:
    """Write the session file. `binding` is updated in place (the pid as an int, a
    Grok or Orca wake path expanded), so keep that same dict for `remove_binding`."""
    name = check_name(binding["name"])
    harness = check_harness(binding["harness"])
    if harness in {"grok", "orca"}:
        wake = binding.get("wake")
        if wake is None or wake == "":
            raise ProtocolError("bad-wake", "a Grok or Orca binding needs a wake path")
        problem = wake_problem(wake)
        if problem:
            raise ProtocolError("bad-wake", f"the wake path {problem}")
        binding["wake"] = expand_wake(wake)  # the file holds what the scan can judge
    binding["pid"] = int(binding.get("pid") or 0)
    private_dir(directory)
    path = directory / f"{name}.json"
    # Per process, so two sessions saving at once never share a temporary file.
    tmp = path.with_name(f".{name}.json.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(binding, indent=2) + "\n")
    os.chmod(tmp, 0o600)
    tmp.replace(path)
    return path


def remove_binding(path: Path, binding: dict) -> bool:
    """Unlink `path` only while it still holds `binding`. A newer session's file stays."""
    try:
        current = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return False
    if not isinstance(current, dict) or not same_binding(current, binding):
        return False
    if current.get("server") != binding.get("server"):
        # Same session, but a newer server process wrote it.
        return False
    path.unlink(missing_ok=True)
    return True


def same_binding(a: dict, b: dict) -> bool:
    return all(a.get(field) == b.get(field) for field in BINDING_FIELDS)


def is_live(binding: dict) -> bool:
    try:
        harness = binding["harness"]
        if harness not in HARNESSES:
            return False
        pid = int(binding.get("pid") or 0)
    except (KeyError, TypeError, ValueError, OverflowError):
        return False
    if harness in {"grok", "orca"}:
        wake = binding.get("wake")
        if not pid_alive(pid):
            return False
        if not wake:
            _say_once(f"{binding.get('name')}:", "session %s is not live: it has no wake path", binding.get("name"))
            return False
        # A wake path that can never be delivered to is refused at bind time; a
        # file written by hand or by an older build that names one is not live.
        # This runs on the relay's loop, so it judges the string and touches nothing.
        problem = wake_problem(wake, live=True)
        if problem:
            _say_once(f"{binding.get('name')}:{wake}", "session %s is not live: its wake path %s %s", binding.get("name"), wake, problem)
            return False
        return True
    if harness in {"claude", "opencode"}:
        if not pid_alive(pid) or started_elsewhen(pid, binding.get("proc_start")):
            return False
        sock = binding.get("socket") or (claude_socket(pid) if harness == "claude" else "")
        if not isinstance(sock, str) or not sock:
            return False
        if harness == "claude":
            return os.path.exists(sock)
        try:  # the plugin's socket, in a directory only this user can enter, as the plugin requires
            st = os.lstat(sock)
            parent = os.lstat(os.path.dirname(sock))
        except OSError:
            return False
        me = os.geteuid()
        return (
            stat.S_ISSOCK(st.st_mode)
            and st.st_uid == me
            and stat.S_ISDIR(parent.st_mode)
            and parent.st_uid == me
            and parent.st_mode & 0o077 == 0
        )
    if harness == "dsh":
        sock = binding.get("socket") or ""
        if not isinstance(sock, str) or not sock or not os.path.exists(sock):
            return False
        if pid and not pid_alive(pid):
            return False
        return True
    return False


def load_held(directory: Path) -> dict[str, dict]:
    """Every name a running process holds: the live ones, and opencode bindings whose
    process is running but whose plugin is not listening yet. A new name must not
    take one of those."""
    found = load_live(directory)
    if not directory.is_dir():
        return found
    for path in sorted(directory.glob("*.json")):
        try:
            data = json.loads(path.read_text())
            name = check_name(data["name"])
            if name != path.stem or name in found or data.get("harness") != "opencode":
                continue
            pid = int(data.get("pid") or 0)
        except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError, AttributeError, OverflowError):
            continue
        from agentmailbus.autoname import reused

        if pid_alive(pid) and not reused(pid, data.get("proc_start")):
            found[name] = data
    return found


def load_live(directory: Path) -> dict[str, dict]:
    """One bad file skips that file. It never takes the other names down with it."""
    found: dict[str, dict] = {}
    if not directory.is_dir():
        return found
    for path in sorted(directory.glob("*.json")):
        try:
            data = json.loads(path.read_text())
            check_name(data["name"])
            if data["name"] != path.stem or not is_live(data):
                continue
        except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError, AttributeError):
            continue
        found[data["name"]] = data
    return found
