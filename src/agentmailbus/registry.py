"""Session bindings. A live binding is the only thing that may send or receive."""

from __future__ import annotations

import json
import os
from pathlib import Path

from agentmailbus.adapters import claude_socket
from agentmailbus.paths import private_dir
from agentmailbus.protocol import HARNESSES, check_harness, check_name

BINDING_FIELDS = ("harness", "pid", "wake", "socket")


def pid_alive(pid: int) -> bool:
    """True only for a running process this user may signal. Another user's pid is not ours."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError):
        return False
    return True


def save_binding(directory: Path, binding: dict) -> Path:
    name = check_name(binding["name"])
    check_harness(binding["harness"])
    binding["pid"] = int(binding.get("pid") or 0)
    private_dir(directory)
    path = directory / f"{name}.json"
    tmp = path.with_suffix(".json.tmp")
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
    except (KeyError, TypeError, ValueError):
        return False
    if harness in {"grok", "orca"}:
        return pid_alive(pid) and bool(binding.get("wake"))
    if harness == "claude":
        if not pid_alive(pid):
            return False
        sock = binding.get("socket") or claude_socket(pid)
        return bool(sock) and os.path.exists(sock)
    if harness == "dsh":
        sock = binding.get("socket") or ""
        if not sock or not os.path.exists(sock):
            return False
        if pid and not pid_alive(pid):
            return False
        return True
    return False


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
