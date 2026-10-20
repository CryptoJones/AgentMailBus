"""Session bindings. A live binding is the only thing that may send or receive."""

from __future__ import annotations

import json
import os
from pathlib import Path

from agentmailbus.adapters import claude_socket
from agentmailbus.protocol import HARNESSES, check_harness, check_name


def pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def save_binding(directory: Path, binding: dict) -> Path:
    name = check_name(binding["name"])
    check_harness(binding["harness"])
    directory.mkdir(parents=True, exist_ok=True)
    os.chmod(directory, 0o700)
    path = directory / f"{name}.json"
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(binding, indent=2) + "\n")
    os.chmod(tmp, 0o600)
    tmp.replace(path)
    return path


def is_live(binding: dict) -> bool:
    try:
        harness = binding["harness"]
        if harness not in HARNESSES:
            return False
    except (KeyError, TypeError):
        return False
    pid = binding.get("pid")
    if harness in {"grok", "orca"}:
        return bool(pid) and pid_alive(int(pid)) and bool(binding.get("wake"))
    if harness == "claude":
        if not pid or not pid_alive(int(pid)):
            return False
        sock = binding.get("socket") or claude_socket(int(pid))
        return bool(sock) and os.path.exists(sock)
    if harness == "dsh":
        sock = binding.get("socket") or ""
        if not sock or not os.path.exists(sock):
            return False
        if pid and not pid_alive(int(pid)):
            return False
        return True
    return False


def load_live(directory: Path) -> dict[str, dict]:
    found: dict[str, dict] = {}
    if not directory.is_dir():
        return found
    for path in sorted(directory.glob("*.json")):
        try:
            data = json.loads(path.read_text())
            check_name(data["name"])
        except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError):
            continue
        if data["name"] != path.stem or not is_live(data):
            continue
        found[data["name"]] = data
    return found
