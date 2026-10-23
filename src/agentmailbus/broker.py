"""The always-on queue. One process, persistent WebSockets from each relay."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from agentmailbus.net import ip_allowed, normalize_ip, tokens_equal
from agentmailbus.protocol import (
    BROADCAST,
    MAX_FRAME,
    MAX_RECIPIENTS,
    ProtocolError,
    check_harness,
    check_id,
    check_name,
    check_priority,
    check_recipient,
    check_text,
    copy_id,
    dumps,
    loads,
)
from agentmailbus.store import RETENTION_DAYS, BrokerStore
from agentmailbus.wsutil import ws_serve

log = logging.getLogger("agentmailbus.broker")

MAX_PENDING_PER_IP = 8  # connections from one address that may sit in the hello step
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
        self.pending: dict[str, int] = {}
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
                    log.info("pruned %d finished messages and broadcast records", dropped)
            except Exception as exc:
                log.warning("prune failed: %s", type(exc).__name__)
            await asyncio.sleep(PRUNE_INTERVAL)

    async def handler(self, ws) -> None:
        peer = ""
        if ws.remote_address:
            peer = normalize_ip(ws.remote_address[0])
        if not ip_allowed(peer, self.allow):
            await ws.close(code=1008, reason="lan-only")
            return
        if self.pending.get(peer, 0) >= MAX_PENDING_PER_IP:
            await ws.close(code=1013, reason="busy")
            return
        # The slot is held until the hello is accepted or the refusal has finished,
        # so a stream of bad hellos is capped the same as a stream of silent ones.
        self.pending[peer] = self.pending.get(peer, 0) + 1
        try:
            relay_id = await self._hello(ws)
        finally:
            self.pending[peer] -= 1
            if self.pending[peer] <= 0:
                self.pending.pop(peer, None)
        if relay_id is None:
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

    async def _hello(self, ws) -> str | None:
        """Read and check the hello. Returns the relay id, or None after closing the socket."""
        try:
            raw = await asyncio.wait_for(ws.recv(), 5)
            hello = loads(raw)
        except Exception:
            await ws.close(code=1008, reason="hello-timeout")
            return None
        if hello.get("type") != "hello" or hello.get("v") != 1 or not tokens_equal(
            hello.get("token"), self.token
        ):
            await self._refuse(ws, "unauthorized")
            return None
        try:
            return check_name(hello.get("host"), "host")
        except ProtocolError as exc:
            await self._refuse(ws, exc.code)
            return None

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

    async def _status(self, ws, relay_id: str, msg: dict) -> None:
        """Where one message stands, per recipient, never with its text. The
        sender's relay sees every recipient; a recipient's relay sees only the rows
        for names it holds now, so one broadcast recipient cannot watch the others."""
        try:
            mid = check_id(msg.get("message"))
        except ProtocolError as exc:
            await ws.send(dumps({"type": "error", "id": msg.get("id"), "error": exc.code}))
            return
        receipt = self.store.receipt(mid)
        if receipt is not None and relay_id not in {receipt["from_host"], self.owner.get(receipt["sender"])}:
            mine = {name for name, owner in self.owner.items() if owner == relay_id}
            receipt = self.store.receipt(mid, only=mine)
            if receipt is not None and not receipt["recipients"]:
                receipt = None
        if receipt is None:
            await ws.send(dumps({"type": "error", "id": msg.get("id"), "error": "unknown-message"}))
            return
        recipients = [
            {
                "name": r["recipient"],
                "state": r["state"],
                "attempts": r["attempts"],
                "delivered_at": r["delivered_at"],
                "finished_at": r["finished_at"],
            }
            for r in receipt["recipients"]
        ]
        await ws.send(
            dumps(
                {
                    "type": "status",
                    "id": msg.get("id"),
                    "message": mid,
                    "kind": receipt["kind"],
                    "broadcast": receipt["broadcast"],
                    "from": receipt["sender"],
                    "created_at": receipt["created_at"],
                    "recipients": recipients,
                    "more": receipt["more"],
                }
            )
        )

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
            row = self.store.get_message(mid) if mid else None
            if row is not None and self.owner.get(row["recipient"]) == relay_id:
                self.store.ack(mid)
                # The next message for that name goes out now, not on the next pull.
                await self._flush(ws, row["recipient"])
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
        elif kind == "status":
            await self._status(ws, relay_id, msg)
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

    async def _flush(self, ws, name: str) -> bool:
        """Send the head of `name`'s queue if nothing is in flight and it is due.

        One message per name is out at a time; `claim` makes it a single winner. The
        next one goes when the ack for this one arrives. Returns True when a frame
        went out.
        """
        head = self.store.head(name)
        if head is not None and self.store.stale(head):
            # The relay never answered for this one. Count it as a failed attempt so
            # the name cannot stay blocked behind a lost ack; the relay's seen set
            # absorbs a late duplicate.
            log.warning("no ack for %s to %s; handing it back", head["id"], name)
            self.store.nack(head["id"])
            head = self.store.head(name)
        if head is None or not self.store.due(head):
            return False
        stamp = self.store.claim(head["id"])
        if stamp is None:
            return False
        try:
            await ws.send(dumps(_deliver_frame(head)))
        except Exception:
            self.store.requeue_id(head["id"], stamp)
            raise
        return True

    async def _send(self, ws, relay_id: str, msg: dict) -> None:
        raw_id = msg.get("id")
        mid = raw_id if isinstance(raw_id, str) else ""
        try:
            mid = check_id(raw_id)
            sender = check_name(msg.get("from"), "from")
            recipient = check_recipient(msg.get("to"))
            text = check_text(msg.get("text"))
            priority = check_priority(msg.get("priority"))
        except ProtocolError as exc:
            await ws.send(dumps({"type": "error", "id": mid, "error": exc.code}))
            return
        if self.owner.get(sender) != relay_id:
            await ws.send(dumps({"type": "error", "id": mid, "error": "not-your-agent"}))
            return
        if recipient == BROADCAST:
            await self._broadcast(ws, mid, sender, text, priority)
            return
        if not self.store.agent_known(recipient):
            await ws.send(dumps({"type": "error", "id": mid, "error": "unknown-agent"}))
            return
        if self.store.get_broadcast(mid) is not None:
            # The id already names a broadcast.
            await ws.send(dumps({"type": "error", "id": mid, "error": "id-conflict"}))
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
        if existing["state"] == "failed":
            await ws.send(dumps({"type": "error", "id": mid, "error": "delivery-failed"}))
            return
        dest = self.conns.get(self.owner.get(recipient, ""))
        if dest is not None and existing["state"] == "queued":
            try:
                await self._flush(dest, recipient)
            except Exception:
                log.warning("push to %s failed", recipient)
        current = self.store.get_message(mid)
        waiting = current is not None and current["state"] == "queued"
        await ws.send(
            dumps({"type": "accepted", "id": mid, "to": recipient, "waiting": waiting})
        )

    async def _broadcast(self, ws, mid: str, sender: str, text: str, priority: str) -> None:
        """One ordinary message for every name online now, the sender excepted.

        The broadcast is recorded first, so its id and sender are reserved even when
        it reaches nobody, and a retry sends the text first given. Each copy has its
        own id, made from the name and a salt only the broker knows, and from then on
        is a direct message: per-name order, acks, retries and give-up all apply
        unchanged. A retry reaches the names online at the retry, none of them twice.
        A name that is offline when it is sent gets nothing from that send.
        """
        if self.store.get_message(mid) is not None:
            # The id already names a direct message.
            await ws.send(dumps({"type": "error", "id": mid, "error": "id-conflict"}))
            return
        cast = self.store.get_broadcast(mid)
        if cast is not None and cast["sender"] != sender:
            await ws.send(dumps({"type": "error", "id": mid, "error": "id-conflict"}))
            return
        names = sorted(
            name for name, owner in self.owner.items() if name != sender and owner in self.conns
        )
        # The cap bounds the new copies one send makes; names that already have a copy
        # do not count.
        have = {c["recipient"] for c in self.store.broadcast_copies(mid)}
        # Only these names may get a new copy in this send, whatever happens to the
        # others' rows while it runs (a prune between pushes cannot raise the count).
        missing = {name for name in names if name not in have}
        if len(missing) > MAX_RECIPIENTS:
            await ws.send(dumps({"type": "error", "id": mid, "error": "too-many-recipients"}))
            return
        if cast is None:
            cast = self.store.insert_broadcast(mid, sender, text, priority)
        for name in names:
            dest = self.conns.get(self.owner.get(name, ""))
            if dest is None:
                # Went offline while an earlier copy was being pushed.
                continue
            cid = copy_id(cast["salt"], name)
            existing = self.store.get_message(cid)
            if existing is None:
                if name not in missing:
                    continue  # its copy was there when this send began
                if self.store.get_broadcast(cid) is not None:
                    log.warning("copy id %s of broadcast %s names a broadcast; skipping %s", cid, mid, name)
                    continue
                self.store.insert_message(
                    cid, sender, name, cast["text"], cast["priority"], broadcast=mid
                )
                existing = self.store.get_message(cid)
            elif existing["broadcast"] != mid:
                log.warning("copy id %s of broadcast %s is taken; skipping %s", cid, mid, name)
                continue
            if existing is not None and existing["state"] == "queued":
                try:
                    await self._flush(dest, name)
                except Exception:
                    log.warning("push to %s failed", name)
        # Every copy not given up on, from this call or an earlier one with this id.
        copies = [c for c in self.store.broadcast_copies(mid) if c["state"] != "failed"]
        recipients = [c["recipient"] for c in copies]
        frame = {
            "type": "accepted",
            "id": mid,
            "to": BROADCAST,
            "recipients": recipients[:MAX_RECIPIENTS],
            "waiting": any(c["state"] == "queued" for c in copies),
        }
        if len(recipients) > MAX_RECIPIENTS:
            frame["more"] = len(recipients) - MAX_RECIPIENTS
        await ws.send(dumps(frame))


def _deliver_frame(row: dict) -> dict:
    frame = {
        "type": "deliver",
        "id": row["id"],
        "from": row["sender"],
        "from_host": row["from_host"],
        "to": row["recipient"],
        "text": row["text"],
        "priority": row["priority"],
        "ts": row["created_at"],
    }
    if row.get("broadcast"):
        frame["broadcast"] = row["broadcast"]
    return frame
