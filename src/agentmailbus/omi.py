"""Mirror a delivered message into OMI. Optional; off unless the relay config turns it on."""

from __future__ import annotations

import logging
import subprocess

log = logging.getLogger("agentmailbus.omi")


class OmiMirror:
    def __init__(self, enabled: bool = False, argv: tuple[str, ...] = ("omind", "note")) -> None:
        self.enabled = enabled
        self.argv = argv

    def write(self, message: dict) -> bool:
        if not self.enabled:
            return False
        title = f"AgentMailBus {message['id']}"
        summary = f"{message['from']} at {message.get('from_host') or ''} to {message['to']}"
        try:
            proc = subprocess.run(
                [
                    *self.argv,
                    "--title",
                    title,
                    "--summary",
                    summary[:300],
                    "--tags",
                    "agentmailbus,mesh",
                    "--confidence",
                    "high",
                ],
                input=message["text"],
                text=True,
                timeout=30,
                check=False,
                capture_output=True,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            log.warning("omi mirror failed for %s: %s", message.get("id"), type(exc).__name__)
            return False
        if proc.returncode != 0:
            log.warning(
                "omi mirror exited %d for %s: %s",
                proc.returncode,
                message.get("id"),
                (proc.stderr or "").strip()[:200],
            )
            return False
        return True
