"""Mirror a delivered message into OMI. The note is the record."""

from __future__ import annotations

import logging
import subprocess

log = logging.getLogger("agentmailbus.omi")


class OmiMirror:
    def __init__(self, enabled: bool = True, argv: tuple[str, ...] = ("omind", "note")) -> None:
        self.enabled = enabled
        self.argv = argv

    def write(self, message: dict) -> None:
        if not self.enabled:
            return
        title = f"AgentMailBus {message['id']}"
        summary = f"{message['from']} at {message.get('from_host') or ''} to {message['to']}"
        try:
            subprocess.run(
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
            )
        except (OSError, subprocess.TimeoutExpired):
            log.warning("omi mirror failed for %s", message.get("id"))
