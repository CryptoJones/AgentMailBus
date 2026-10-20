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
    check_name,
    check_priority,
    check_text,
    dumps,
    loads,
)
from agentmailbus.store import BrokerStore
from agentmailbus.wsutil import ws_serve

log = logging.getLogger("agentmailbus.broker")


class Broker:
    def __init__(self, store: BrokerStore, token: str, allow, host: str, port: int) -> None:
        self.store = store
        self.token = token
        self.allow = allow
        self.host = host
        self.port = port
        self.conns: dict[str, Any] = {}
        self.owner: dict[str, str] = {}
        self.server = None

    async def start(self) -> None:
        self.server = await ws_serve(
            self.handler, self.host, self.port, max_size=MAX_FRAME
        )
        sock = next(iter(self.server.sockets))
        self.port = sock.getsockname()[1]

    async def stop(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

    async def handler(self, ws) -> None:
        peer = ""
        if ws.remote_address:
            peer = ws.remote_address[0]
        if not ip_allowed(peer, self.allow):
            await ws.close(code=1008, reason="lan-only")
            return
        try:
            raw = await asyncio.wait_for(ws.recv(), 5)
            hello = loads(raw)
        except (asyncio.TimeoutError, ProtocolError, Exception):
            await ws.close(code=1008, reason="hello-timeout")
            return
        if hello.get("type") != "hello" or hello.get("v") != 1 or not tokens_equal(
            hello.get("token"), self.token
        ):
            await ws.send(dumps({"type": "error", "error": "unauthorized"}))
            await ws.close(code=1008, reason="unauthorized")
            return
        try:
            relay_id = check_name(hello.get("host"), "host")
        except ProtocolError as exc:
            await ws.send(dumps({"type": "error", "error": exc.code}))
            await ws.close(code=1008, reason=exc.code)
            return
        old = self.conns.get(relay_id)
        self.conns[relay_id] = ws
        if old is not None and old is not ws:
            self._drop(relay_id)
            self.conns[relay_id] = ws
            await old.close()
        await ws.send(dumps({"type": "welcome", "host": relay_id}))
        try:
            async for raw in ws:
                await self._on(ws, relay_id, raw)
        finally:
            if self.conns.get(relay_id) is ws:
                self._drop(relay_id)
                self.conns.pop(relay_id, None)

    def _drop(self, relay_id: str) -> None:
        self.store.requeue_host(relay_id)
        self.store.offline_host(relay_id)
        for name, owner in list(self.owner.items()):
            if owner == relay_id:
                del self.owner[name]

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
            self.store.ack(str(msg.get("id") or ""))
        elif kind == "nack":
            self.store.requeue_id(str(msg.get("id") or ""))
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
        if self.owner.get(name) == relay_id:
            del self.owner[name]
        self.store.requeue_name(name)
        self.store.set_online(name, False)

    async def _flush(self, ws, name: str) -> None:
        for row in self.store.queued(name):
            await ws.send(dumps(_deliver_frame(row)))
            self.store.mark(row["id"], "delivered")

    async def _send(self, ws, relay_id: str, msg: dict) -> None:
        mid = str(msg.get("id") or "")
        try:
            sender = check_name(msg.get("from"), "from")
            recipient = check_name(msg.get("to"), "to")
            text = check_text(msg.get("text"))
            priority = check_priority(msg.get("priority"))
            if mid == "":
                raise ProtocolError("bad-id")
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
            created = str(msg.get("ts") or "")
            self.store.insert_message(mid, sender, recipient, text, priority, created)
            existing = self.store.get_message(mid)
        assert existing is not None
        waiting = existing["state"] != "acked"
        dest = self.conns.get(self.owner.get(recipient, ""))
        if existing["state"] == "queued" and dest is not None:
            try:
                await dest.send(dumps(_deliver_frame(existing)))
                self.store.mark(mid, "delivered")
                waiting = False
            except Exception:
                log.warning("push to %s failed", recipient)
                waiting = True
        elif existing["state"] == "delivered":
            waiting = False
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
