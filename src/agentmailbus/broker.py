"""The always-on queue. One process, persistent WebSockets from each relay."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from agentmailbus.net import ip_allowed, tokens_equal
from agentmailbus.protocol import (
    MAX_FRAME,
    ProtocolError,
    check_harness,
    check_id,
    check_name,
    check_priority,
    check_text,
    dumps,
    loads,
)
from agentmailbus.store import RETENTION_DAYS, BrokerStore
from agentmailbus.wsutil import ws_serve

log = logging.getLogger("agentmailbus.broker")

MAX_PENDING = 64  # connections that may sit in the hello step at once
PRUNE_INTERVAL = 3600.0


class Broker:
    def __init__(
        self,
        store: BrokerStore,
        token: str,
        allow,
        host: str,
        port: int,
        retention_days: float = RETENTION_DAYS,
    ) -> None:
        self.store = store
        self.token = token
        self.allow = allow
        self.host = host
        self.port = port
        self.retention_days = retention_days
        self.conns: dict[str, Any] = {}
        self.owner: dict[str, str] = {}
        self.server = None
        self.pending = asyncio.Semaphore(MAX_PENDING)
        self.pruner: asyncio.Task | None = None

    async def start(self) -> None:
        self.server = await ws_serve(
            self.handler, self.host, self.port, max_size=MAX_FRAME
        )
        sock = next(iter(self.server.sockets))
        self.port = sock.getsockname()[1]
        self.pruner = asyncio.create_task(self._prune_loop())

    async def stop(self) -> None:
        if self.pruner is not None:
            self.pruner.cancel()
            await asyncio.gather(self.pruner, return_exceptions=True)
            self.pruner = None
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

    async def _prune_loop(self) -> None:
        while True:
            try:
                dropped = self.store.prune(self.retention_days)
                if dropped:
                    log.info("pruned %d finished messages", dropped)
            except Exception as exc:
                log.warning("prune failed: %s", type(exc).__name__)
            await asyncio.sleep(PRUNE_INTERVAL)

    async def handler(self, ws) -> None:
        peer = ""
        if ws.remote_address:
            peer = ws.remote_address[0]
        if not ip_allowed(peer, self.allow):
            await ws.close(code=1008, reason="lan-only")
            return
        if self.pending.locked():
            await ws.close(code=1013, reason="busy")
            return
        async with self.pending:
            try:
                raw = await asyncio.wait_for(ws.recv(), 5)
                hello = loads(raw)
            except Exception:
                await ws.close(code=1008, reason="hello-timeout")
                return
            if hello.get("type") != "hello" or hello.get("v") != 1 or not tokens_equal(
                hello.get("token"), self.token
            ):
                await self._refuse(ws, "unauthorized")
                return
            try:
                relay_id = check_name(hello.get("host"), "host")
            except ProtocolError as exc:
                await self._refuse(ws, exc.code)
                return
        old = self.conns.get(relay_id)
        self.conns[relay_id] = ws
        if old is not None and old is not ws:
            log.warning("relay %s connected again; replacing its link", relay_id)
            self._drop(relay_id)
            await old.close()
        log.info("relay %s connected from %s", relay_id, peer)
        await ws.send(dumps({"type": "welcome", "host": relay_id}))
        try:
            async for raw in ws:
                await self._on(ws, relay_id, raw)
        finally:
            if self.conns.get(relay_id) is ws:
                self._drop(relay_id)
                self.conns.pop(relay_id, None)
                log.info("relay %s disconnected", relay_id)

    async def _refuse(self, ws, code: str) -> None:
        try:
            await ws.send(dumps({"type": "error", "error": code}))
            await ws.close(code=1008, reason=code)
        except Exception:
            pass

    def _drop(self, relay_id: str) -> None:
        self.store.requeue_host(relay_id)
        self.store.offline_host(relay_id)
        for name, owner in list(self.owner.items()):
            if owner == relay_id:
                del self.owner[name]

    def _owns_message(self, relay_id: str, mid: str) -> bool:
        """Only the relay that holds the recipient may ack or nack a message."""
        row = self.store.get_message(mid) if mid else None
        return row is not None and self.owner.get(row["recipient"]) == relay_id

    async def _on(self, ws, relay_id: str, raw) -> None:
        try:
            msg = loads(raw)
        except ProtocolError as exc:
            await ws.send(dumps({"type": "error", "error": exc.code}))
            return
        kind = msg.get("type")
        if kind == "register":
            await self._register(ws, relay_id, msg)
        elif kind == "unregister":
            await self._unregister(relay_id, msg)
        elif kind == "send":
            await self._send(ws, relay_id, msg)
        elif kind == "ack":
            mid = str(msg.get("id") or "")
            if self._owns_message(relay_id, mid):
                self.store.ack(mid)
        elif kind == "nack":
            mid = str(msg.get("id") or "")
            if self._owns_message(relay_id, mid):
                state = self.store.nack(mid)
                if state == "failed":
                    log.warning("message %s gave up after repeated delivery failures", mid)
        elif kind == "pull":
            for name, owner in list(self.owner.items()):
                if owner == relay_id:
                    await self._flush(ws, name)
        elif kind == "directory":
            await ws.send(
                dumps(
                    {
                        "type": "agents",
                        "id": msg.get("id"),
                        "agents": self.store.directory(),
                    }
                )
            )
        else:
            await ws.send(dumps({"type": "error", "id": msg.get("id"), "error": "bad-type"}))

    async def _register(self, ws, relay_id: str, msg: dict) -> None:
        try:
            name = check_name(msg.get("name"))
            harness = check_harness(msg.get("harness"))
        except ProtocolError as exc:
            await ws.send(dumps({"type": "error", "name": msg.get("name"), "error": exc.code}))
            return
        current = self.owner.get(name)
        if current and current != relay_id and current in self.conns:
            await ws.send(dumps({"type": "error", "name": name, "error": "name-taken"}))
            return
        self.owner[name] = relay_id
        self.store.upsert_agent(name, relay_id, harness, True)
        await ws.send(dumps({"type": "registered", "name": name}))
        await self._flush(ws, name)

    async def _unregister(self, relay_id: str, msg: dict) -> None:
        try:
            name = check_name(msg.get("name"))
        except ProtocolError:
            return
        if self.owner.get(name) != relay_id:
            return
        del self.owner[name]
        self.store.requeue_name(name)
        self.store.set_online(name, False)

    async def _flush(self, ws, name: str) -> None:
        """Claim each queued row before it goes out, so no row is sent twice."""
        for row in self.store.queued(name):
            if not self.store.claim(row["id"]):
                continue
            try:
                await ws.send(dumps(_deliver_frame(row)))
            except Exception:
                self.store.requeue_id(row["id"])
                raise

    async def _push(self, row: dict) -> bool:
        """Try to hand a queued row to its recipient now. True when it is in flight."""
        dest = self.conns.get(self.owner.get(row["recipient"], ""))
        if dest is None:
            return False
        if not self.store.claim(row["id"]):
            return True
        try:
            await dest.send(dumps(_deliver_frame(row)))
        except Exception:
            log.warning("push to %s failed", row["recipient"])
            self.store.requeue_id(row["id"])
            return False
        return True

    async def _send(self, ws, relay_id: str, msg: dict) -> None:
        raw_id = msg.get("id")
        mid = raw_id if isinstance(raw_id, str) else ""
        try:
            mid = check_id(raw_id)
            sender = check_name(msg.get("from"), "from")
            recipient = check_name(msg.get("to"), "to")
            text = check_text(msg.get("text"))
            priority = check_priority(msg.get("priority"))
        except ProtocolError as exc:
            await ws.send(dumps({"type": "error", "id": mid, "error": exc.code}))
            return
        if self.owner.get(sender) != relay_id:
            await ws.send(dumps({"type": "error", "id": mid, "error": "not-your-agent"}))
            return
        if not self.store.agent_known(recipient):
            await ws.send(dumps({"type": "error", "id": mid, "error": "unknown-agent"}))
            return
        existing = self.store.get_message(mid)
        if existing is None:
            self.store.insert_message(mid, sender, recipient, text, priority)
            existing = self.store.get_message(mid)
        elif existing["sender"] != sender or existing["recipient"] != recipient:
            # A known id addressed differently is a different message. Refuse it.
            await ws.send(dumps({"type": "error", "id": mid, "error": "id-conflict"}))
            return
        assert existing is not None
        waiting = existing["state"] == "queued"
        if waiting:
            waiting = not await self._push(existing)
        await ws.send(
            dumps({"type": "accepted", "id": mid, "to": recipient, "waiting": waiting})
        )


def _deliver_frame(row: dict) -> dict:
    return {
        "type": "deliver",
        "id": row["id"],
        "from": row["sender"],
        "from_host": row["from_host"],
        "to": row["recipient"],
        "text": row["text"],
        "priority": row["priority"],
        "ts": row["created_at"],
    }
