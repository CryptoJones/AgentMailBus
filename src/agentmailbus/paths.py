"""Private directories. Only a directory this program creates gets its mode changed."""

from __future__ import annotations

import os
from pathlib import Path


def private_dir(path: Path) -> Path:
    """Create `path` as a 0700 directory. An existing directory is left as it is."""
    if path.is_dir():
        return path
    path.mkdir(parents=True, exist_ok=True)
    os.chmod(path, 0o700)
    return path


def data_dir() -> Path:
    base = os.environ.get("XDG_DATA_HOME", "~/.local/share")
    return Path(base).expanduser() / "agentmailbus"
