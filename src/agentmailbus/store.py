"""SQLite queue for the broker, and the relay's delivered-id set."""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from agentmailbus.paths import private_dir

MAX_ATTEMPTS = 20
MAX_BACKOFF = 60.0
RETENTION_DAYS = 7
# The relay remembers ids longer than the broker keeps finished rows, so a row the
# broker requeues after a long outage is still recognised. See PROTOCOL.md.
SEEN_RETENTION_DAYS = 30


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _later(seconds: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat(timespec="microseconds")


def _before(days: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat(timespec="microseconds")


def backoff(attempts: int) -> float:
    return min(float(2**attempts), MAX_BACKOFF)


def _open(path: Path) -> sqlite3.Connection:
    private_dir(path.parent)
    if not path.exists():
        path.touch(mode=0o600)
    os.chmod(path, 0o600)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=3000")
    return conn


class BrokerStore:
    def __init__(self, path: Path) -> None:
        self.conn = _open(path)
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS agents (
                name TEXT PRIMARY KEY,
                host TEXT NOT NULL,
                harness TEXT NOT NULL,
                online INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                sender TEXT NOT NULL,
                recipient TEXT NOT NULL,
                from_host TEXT NOT NULL,
                text TEXT NOT NULL,
                priority TEXT NOT NULL,
                created_at TEXT NOT NULL,
                state TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                not_before TEXT NOT NULL DEFAULT '',
                finished_at TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS messages_q ON messages(recipient, state);
            """
        )
        self._migrate()
        self.startup()

    def _migrate(self) -> None:
        cols = {row["name"] for row in self.conn.execute("PRAGMA table_info(messages)")}
        if "attempts" not in cols:
            self.conn.execute("ALTER TABLE messages ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0")
        if "not_before" not in cols:
            self.conn.execute("ALTER TABLE messages ADD COLUMN not_before TEXT NOT NULL DEFAULT ''")
        if "finished_at" not in cols:
            self.conn.execute("ALTER TABLE messages ADD COLUMN finished_at TEXT NOT NULL DEFAULT ''")
        # Rows from before the broker stamped time: date them now so they age normally
        # instead of being pruned on the first pass.
        now = _now()
        self.conn.execute("UPDATE messages SET created_at=? WHERE created_at=''", (now,))
        self.conn.execute(
            "UPDATE messages SET finished_at=? WHERE finished_at='' AND state IN ('acked', 'failed')",
            (now,),
        )
        self.conn.commit()

    def startup(self) -> None:
        self.conn.execute("UPDATE messages SET state='queued' WHERE state='delivered'")
        self.conn.execute("UPDATE agents SET online=0")
        self.conn.commit()

    def upsert_agent(self, name: str, host: str, harness: str, online: bool) -> None:
        self.conn.execute(
            """
            INSERT INTO agents(name, host, harness, online, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                host=excluded.host,
                harness=excluded.harness,
                online=excluded.online,
                updated_at=excluded.updated_at
            """,
            (name, host, harness, 1 if online else 0, _now()),
        )
        self.conn.commit()

    def set_online(self, name: str, online: bool) -> None:
        self.conn.execute(
            "UPDATE agents SET online=?, updated_at=? WHERE name=?",
            (1 if online else 0, _now(), name),
        )
        self.conn.commit()

    def offline_host(self, host: str) -> None:
        self.conn.execute(
            "UPDATE agents SET online=0, updated_at=? WHERE host=?",
            (_now(), host),
        )
        self.conn.commit()

    def agent_known(self, name: str) -> bool:
        row = self.conn.execute("SELECT 1 FROM agents WHERE name=?", (name,)).fetchone()
        return row is not None

    def host_of(self, name: str) -> str:
        row = self.conn.execute("SELECT host FROM agents WHERE name=?", (name,)).fetchone()
        return "" if row is None else str(row["host"])

    def insert_message(
        self,
        mid: str,
        sender: str,
        recipient: str,
        text: str,
        priority: str,
    ) -> None:
        """The broker stamps the time. Nothing the sender supplies decides the order."""
        self.conn.execute(
            """
            INSERT INTO messages(id, sender, recipient, from_host, text, priority, created_at, state)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'queued')
            """,
            (mid, sender, recipient, self.host_of(sender), text, priority, _now()),
        )
        self.conn.commit()

    def get_message(self, mid: str) -> dict | None:
        row = self.conn.execute("SELECT rowid, * FROM messages WHERE id=?", (mid,)).fetchone()
        return None if row is None else dict(row)

    def head(self, name: str) -> dict | None:
        """The oldest unfinished row for `name`: the only one that may go out next.

        One message is in flight per name at a time, so order is acceptance order
        even across failures. A `delivered` head means wait for its ack or nack.
        """
        row = self.conn.execute(
            """
            SELECT rowid, * FROM messages
            WHERE recipient=? AND state IN ('queued', 'delivered')
            ORDER BY rowid LIMIT 1
            """,
            (name,),
        ).fetchone()
        return None if row is None else dict(row)

    def due(self, row: dict) -> bool:
        return row["state"] == "queued" and row["not_before"] <= _now()

    def claim(self, mid: str) -> bool:
        """Move one queued row to delivered. False when another path already took it."""
        cur = self.conn.execute(
            "UPDATE messages SET state='delivered' WHERE id=? AND state='queued'",
            (mid,),
        )
        self.conn.commit()
        return cur.rowcount == 1

    def ack(self, mid: str) -> None:
        if mid:
            self.conn.execute(
                "UPDATE messages SET state='acked', finished_at=? WHERE id=? AND state='delivered'",
                (_now(), mid),
            )
            self.conn.commit()

    def nack(self, mid: str) -> str:
        """Count the attempt and requeue with a delay, or give up. Returns the new state."""
        row = self.get_message(mid) if mid else None
        if row is None or row["state"] != "delivered":
            return ""
        attempts = int(row["attempts"]) + 1
        if attempts >= MAX_ATTEMPTS:
            state, not_before, finished = "failed", "", _now()
        else:
            state, not_before, finished = "queued", _later(backoff(attempts)), ""
        self.conn.execute(
            """
            UPDATE messages SET state=?, attempts=?, not_before=?, finished_at=?
            WHERE id=? AND state='delivered'
            """,
            (state, attempts, not_before, finished, mid),
        )
        self.conn.commit()
        return state

    def queued(self, name: str) -> list[dict]:
        rows = self.conn.execute(
            """
            SELECT rowid, * FROM messages
            WHERE recipient=? AND state='queued' AND not_before<=?
            ORDER BY rowid
            """,
            (name, _now()),
        ).fetchall()
        return [dict(row) for row in rows]

    def requeue_host(self, host: str) -> None:
        self.conn.execute(
            """
            UPDATE messages SET state='queued'
            WHERE state='delivered' AND recipient IN (SELECT name FROM agents WHERE host=?)
            """,
            (host,),
        )
        self.conn.commit()

    def requeue_name(self, name: str) -> None:
        self.conn.execute(
            "UPDATE messages SET state='queued' WHERE state='delivered' AND recipient=?",
            (name,),
        )
        self.conn.commit()

    def requeue_id(self, mid: str) -> None:
        if not mid:
            return
        self.conn.execute(
            "UPDATE messages SET state='queued' WHERE state='delivered' AND id=?",
            (mid,),
        )
        self.conn.commit()

    def prune(self, days: float = RETENTION_DAYS) -> int:
        """Drop rows that finished more than `days` ago. That is also the id de-duplication window."""
        cur = self.conn.execute(
            "DELETE FROM messages WHERE state IN ('acked', 'failed') AND finished_at < ?",
            (_before(days),),
        )
        self.conn.commit()
        return cur.rowcount

    def directory(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT name, host, harness, online FROM agents ORDER BY name"
        ).fetchall()
        out = []
        for row in rows:
            counts = self.conn.execute(
                """
                SELECT
                    SUM(CASE WHEN state IN ('queued', 'delivered') THEN 1 ELSE 0 END) AS pending,
                    SUM(CASE WHEN state='failed' THEN 1 ELSE 0 END) AS failed
                FROM messages WHERE recipient=?
                """,
                (row["name"],),
            ).fetchone()
            out.append(
                {
                    "name": row["name"],
                    "host": row["host"],
                    "harness": row["harness"],
                    "online": bool(row["online"]),
                    "queued": int(counts["pending"] or 0),
                    "failed": int(counts["failed"] or 0),
                }
            )
        return out


class SeenStore:
    def __init__(self, path: Path) -> None:
        self.conn = _open(path)
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS seen (id TEXT PRIMARY KEY, at TEXT NOT NULL)"
        )
        self.conn.commit()

    def done(self, mid: str) -> bool:
        row = self.conn.execute("SELECT 1 FROM seen WHERE id=?", (mid,)).fetchone()
        return row is not None

    def mark(self, mid: str) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO seen(id, at) VALUES (?, ?)",
            (mid, _now()),
        )
        self.conn.commit()

    def prune(self, days: float = SEEN_RETENTION_DAYS) -> int:
        cur = self.conn.execute("DELETE FROM seen WHERE at < ?", (_before(days),))
        self.conn.commit()
        return cur.rowcount
