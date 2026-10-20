"""SQLite queue for the broker, and the relay's delivered-id set."""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _open(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=3000")
    os.chmod(path, 0o600)
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
                state TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS messages_q ON messages(recipient, state);
            """
        )
        self.startup()

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
        created_at: str,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO messages(id, sender, recipient, from_host, text, priority, created_at, state)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'queued')
            """,
            (mid, sender, recipient, self.host_of(sender), text, priority, created_at),
        )
        self.conn.commit()

    def get_message(self, mid: str) -> dict | None:
        row = self.conn.execute("SELECT * FROM messages WHERE id=?", (mid,)).fetchone()
        return None if row is None else dict(row)

    def mark(self, mid: str, state: str) -> None:
        self.conn.execute("UPDATE messages SET state=? WHERE id=?", (state, mid))
        self.conn.commit()

    def ack(self, mid: str) -> None:
        if mid:
            self.mark(mid, "acked")

    def queued(self, name: str) -> list[dict]:
        rows = self.conn.execute(
            """
            SELECT * FROM messages
            WHERE recipient=? AND state='queued'
            ORDER BY created_at, id
            """,
            (name,),
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

    def directory(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT name, host, harness, online FROM agents ORDER BY name"
        ).fetchall()
        out = []
        for row in rows:
            queued = self.conn.execute(
                "SELECT COUNT(*) AS n FROM messages WHERE recipient=? AND state!='acked'",
                (row["name"],),
            ).fetchone()
            out.append(
                {
                    "name": row["name"],
                    "host": row["host"],
                    "harness": row["harness"],
                    "online": bool(row["online"]),
                    "queued": int(queued["n"]),
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
