"""Private directories. Only a directory this program creates gets its mode changed."""

from __future__ import annotations

import os
from pathlib import Path


def private_dir(path: Path) -> Path:
    """Create `path` and any missing parents, each 0700. Existing directories keep their mode.

    Each component is created on its own, without `exist_ok`, so a directory another
    process made first is never chmodded here. The mode is applied through a
    descriptor opened with O_NOFOLLOW on the directory just created, not by name.
    A component that exists but is not a directory is an error, not a success.
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
            component.mkdir(mode=0o700)
        except FileExistsError:
            if not component.is_dir():
                raise NotADirectoryError(str(component)) from None
            continue
        try:
            fd = os.open(component, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0))
        except PermissionError:
            # An umask that strips the owner's bits left a directory nobody can open.
            # It was created a moment ago by this call, so set it by name.
            os.chmod(component, 0o700)
            continue
        try:
            os.fchmod(fd, 0o700)
        finally:
            os.close(fd)
    return path


def data_dir() -> Path:
    base = os.environ.get("XDG_DATA_HOME", "~/.local/share")
    return Path(base).expanduser() / "agentmailbus"
