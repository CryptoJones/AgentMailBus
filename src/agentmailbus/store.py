"""SQLite queue for the broker, and the relay's delivered-id set."""

from __future__ import annotations

import os
import hashlib
import hmac
import secrets
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from agentmailbus.paths import private_dir

MAX_ATTEMPTS = 20
MAX_BACKOFF = 60.0
# A delivered row with no ack or nack after this long counts as a failed attempt.
# The relay runs one delivery worker per name, so the clock covers one delivery for
# that name: at most two 5 s socket timeouts, or a wake-file write, plus a 30 s OMI
# note left over from the previous message. A live relay answers well inside it.
DELIVERY_TIMEOUT = 300.0
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


def _receipt_row(row) -> dict:
    out = dict(row)
    # delivered_at carries a claim counter after "/" for requeue matching; a receipt
    # shows only the time.
    out["delivered_at"] = str(out.get("delivered_at") or "").split("/", 1)[0]
    out.pop("from_host", None)
    out.pop("broadcast", None)
    return out


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
    def __init__(self, path: Path, delivery_timeout: float = DELIVERY_TIMEOUT) -> None:
        self.delivery_timeout = delivery_timeout
        self.claims = 0
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
                finished_at TEXT NOT NULL DEFAULT '',
                delivered_at TEXT NOT NULL DEFAULT '',
                broadcast TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS messages_q ON messages(recipient, state);
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS broadcasts (
                id TEXT PRIMARY KEY,
                sender TEXT NOT NULL,
                text TEXT NOT NULL,
                priority TEXT NOT NULL,
                salt TEXT NOT NULL,
                created_at TEXT NOT NULL,
                from_host TEXT NOT NULL DEFAULT ''
            );
            """
        )
        self._migrate()
        # Made now, so a backup of the database holds it before the first broadcast.
        self.broadcast_salt("00000000-0000-0000-0000-000000000000", "")
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
        if "delivered_at" not in cols:
            self.conn.execute("ALTER TABLE messages ADD COLUMN delivered_at TEXT NOT NULL DEFAULT ''")
        # Rows an older broker had handed out without stamping them get a stamp, so a
        # late ack from the relay that wrote them still finishes them after the
        # restart requeue. On a database this broker wrote, no row matches.
        self.conn.execute(
            "UPDATE messages SET delivered_at=created_at WHERE delivered_at='' AND (state='delivered' OR attempts>0)"
        )
        if "broadcast" not in cols:
            self.conn.execute("ALTER TABLE messages ADD COLUMN broadcast TEXT NOT NULL DEFAULT ''")
        bcols = {row["name"] for row in self.conn.execute("PRAGMA table_info(broadcasts)")}
        if "from_host" not in bcols:
            # The sending relay, so receipts can tell it from the recipients' relays
            # even for a broadcast with no copies. Older rows take their first copy's.
            self.conn.execute("ALTER TABLE broadcasts ADD COLUMN from_host TEXT NOT NULL DEFAULT ''")
            self.conn.execute(
                """
                UPDATE broadcasts SET from_host=COALESCE(
                    (SELECT from_host FROM messages WHERE broadcast=broadcasts.id ORDER BY rowid LIMIT 1), '')
                """
            )
        # After the column exists, so an older database can take it.
        self.conn.execute("CREATE INDEX IF NOT EXISTS messages_b ON messages(broadcast)")
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
        broadcast: str = "",
    ) -> None:
        """The broker stamps the time. Nothing the sender supplies decides the order.
        `broadcast` is the broadcast id when this row is one recipient's copy."""
        self.conn.execute(
            """
            INSERT INTO messages(id, sender, recipient, from_host, text, priority, created_at, state, broadcast)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?)
            """,
            (mid, sender, recipient, self.host_of(sender), text, priority, _now(), broadcast),
        )
        self.conn.commit()

    def get_broadcast(self, broadcast_id: str) -> dict | None:
        row = self.conn.execute("SELECT * FROM broadcasts WHERE id=?", (broadcast_id,)).fetchone()
        return None if row is None else dict(row)

    def insert_broadcast(self, broadcast_id: str, sender: str, text: str, priority: str) -> dict:
        """Record a broadcast before any copy is made, so its id and sender are
        reserved even when it reaches nobody, and a retry reuses its text."""
        self.conn.execute(
            "INSERT INTO broadcasts(id, sender, text, priority, salt, created_at, from_host) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (broadcast_id, sender, text, priority, self.broadcast_salt(broadcast_id, sender), _now(), self.host_of(sender)),
        )
        self.conn.commit()
        row = self.get_broadcast(broadcast_id)
        assert row is not None
        return row

    def broadcast_salt(self, broadcast_id: str, sender: str) -> str:
        """The salt for a broadcast's copy ids: keyed by a secret this database keeps,
        so it is the same for the same sender and id however long ago the record was
        pruned (a relay that injected a copy still recognises it), different for
        another sender reusing the id, and nobody outside the broker can work it out."""
        row = self.conn.execute("SELECT value FROM meta WHERE key='broadcast_secret'").fetchone()
        if row is None:
            self.conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES ('broadcast_secret', ?)",
                (secrets.token_hex(32),),
            )
            self.conn.commit()
            row = self.conn.execute("SELECT value FROM meta WHERE key='broadcast_secret'").fetchone()
        digest = hmac.new(
            bytes.fromhex(row["value"]), f"{sender}\0{broadcast_id}".encode(), hashlib.sha256
        ).digest()
        return str(uuid.UUID(bytes=digest[:16]))

    def broadcast_copies(self, broadcast_id: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT recipient, state FROM messages WHERE broadcast=? ORDER BY recipient",
            (broadcast_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def receipt(self, mid: str, limit: int = 1000, only: set[str] | None = None) -> dict | None:
        """Where a message stands, per recipient, without its text: one row for a
        direct message or a broadcast copy's own id, one per copy for a broadcast id.
        `only` keeps the rows for those recipients. None once the broker has no
        record of it (never sent, or pruned after retention_days)."""
        cols = "recipient, sender, from_host, state, attempts, created_at, delivered_at, finished_at"
        row = self.conn.execute(f"SELECT {cols}, broadcast FROM messages WHERE id=?", (mid,)).fetchone()
        if row is not None:
            rows = [_receipt_row(row)] if only is None or row["recipient"] in only else []
            return {
                "kind": "copy" if row["broadcast"] else "direct",
                "broadcast": row["broadcast"],
                "sender": row["sender"],
                "from_host": row["from_host"],
                "created_at": row["created_at"],
                "recipients": rows,
                "more": 0,
            }
        record = self.get_broadcast(mid)
        where, args = "broadcast=?", [mid]
        if only is not None:
            names = sorted(only)
            where += f" AND recipient IN ({','.join('?' * len(names))})" if names else " AND 0"
            args += names
        rows = self.conn.execute(
            f"SELECT {cols} FROM messages WHERE {where} ORDER BY recipient LIMIT ?", (*args, limit)
        ).fetchall()
        first = self.conn.execute(
            f"SELECT {cols} FROM messages WHERE broadcast=? ORDER BY rowid LIMIT 1", (mid,)
        ).fetchone()
        if record is None and first is None:
            return None
        total = self.conn.execute(f"SELECT COUNT(*) FROM messages WHERE {where}", args).fetchone()[0]
        return {
            "kind": "broadcast",
            "broadcast": mid,
            "sender": record["sender"] if record else first["sender"],
            "from_host": (record["from_host"] if record else "") or (first["from_host"] if first else ""),
            "created_at": record["created_at"] if record else first["created_at"],
            "recipients": [_receipt_row(r) for r in rows],
            "more": max(0, total - len(rows)),
        }

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

    def stale(self, row: dict) -> bool:
        """A delivered row whose ack or nack is overdue."""
        return row["state"] == "delivered" and row["delivered_at"] < _before(self.delivery_timeout / 86400.0)

    def claim(self, mid: str) -> str | None:
        """Move one queued row to delivered. Returns the claim stamp, or None when
        another path already took it. The stamp lets a failed send roll back only
        its own claim and not a newer one; the counter makes it unique even if the
        clock repeats."""
        self.claims += 1
        stamp = f"{_now()}/{self.claims}"
        cur = self.conn.execute(
            "UPDATE messages SET state='delivered', delivered_at=? WHERE id=? AND state='queued'",
            (stamp, mid),
        )
        self.conn.commit()
        return stamp if cur.rowcount == 1 else None

    def ack(self, mid: str) -> None:
        """An ack always means the relay wrote the message. It finishes the row even
        if the broker has meanwhile handed it back (a deadline, a drop, a restart) or
        given up on it. A row that was never handed out, which `delivered_at` records,
        is not touched: no relay can have written it."""
        if mid:
            self.conn.execute(
                """
                UPDATE messages SET state='acked', finished_at=?
                WHERE id=? AND (state='delivered' OR (state IN ('queued', 'failed') AND delivered_at != ''))
                """,
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

    def requeue_id(self, mid: str, stamp: str | None = None) -> None:
        """Put one delivered row back. With `stamp`, only the claim that made it."""
        if not mid:
            return
        if stamp is None:
            self.conn.execute(
                "UPDATE messages SET state='queued' WHERE state='delivered' AND id=?",
                (mid,),
            )
        else:
            self.conn.execute(
                "UPDATE messages SET state='queued' WHERE state='delivered' AND id=? AND delivered_at=?",
                (mid, stamp),
            )
        self.conn.commit()

    def prune(self, days: float = RETENTION_DAYS) -> int:
        """Drop rows that finished more than `days` ago. That is also the id de-duplication window."""
        cutoff = _before(days)
        cur = self.conn.execute(
            "DELETE FROM messages WHERE state IN ('acked', 'failed') AND finished_at < ?",
            (cutoff,),
        )
        dropped = cur.rowcount
        # A broadcast record goes once it is old and none of its copies is left. Its
        # salt does not depend on the record, so a later retry makes the same copy ids.
        cur = self.conn.execute(
            """
            DELETE FROM broadcasts WHERE created_at < ?
            AND NOT EXISTS (SELECT 1 FROM messages WHERE messages.broadcast = broadcasts.id)
            """,
            (cutoff,),
        )
        self.conn.commit()
        return dropped + cur.rowcount

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
