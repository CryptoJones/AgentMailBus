"""Private directories. Only a directory this program creates gets its mode changed."""

from __future__ import annotations

import os
from pathlib import Path


def private_dir(path: Path) -> Path:
    """Create `path` and any missing parents, each 0700. Existing directories keep their mode.

    Each component is created on its own, without `exist_ok`, so a directory another
    process made first is never chmodded here.
    """
    path = Path(path)
    if path.is_dir():
        return path
    missing: list[Path] = []
    cursor = path
    while not cursor.is_dir():
        missing.append(cursor)
        if cursor.parent == cursor:
            break
        cursor = cursor.parent
    for component in reversed(missing):
        try:
            component.mkdir()
        except FileExistsError:
            continue
        os.chmod(component, 0o700)
    return path


def data_dir() -> Path:
    base = os.environ.get("XDG_DATA_HOME", "~/.local/share")
    return Path(base).expanduser() / "agentmailbus"
