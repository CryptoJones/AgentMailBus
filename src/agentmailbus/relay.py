"""One relay per host. It holds the WebSocket and delivers on the machine."""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from pathlib import Path

from agentmailbus.adapters import default_adapters
from agentmailbus.omi import OmiMirror
from agentmailbus.protocol import MAX_FRAME, ProtocolError, check_name, check_priority, check_text, dumps, loads
from agentmailbus.registry import load_live
from agentmailbus.store import SeenStore
from agentmailbus.wsutil import ws_connect

log = logging.getLogger("agentmailbus.relay")


class LocalError(Exception):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class Relay:
    def __init__(
        self,
        url: str,
        token: str,
        host_id: str,
        socket_path: Path,
        registry: Path,
        seen: SeenStore,
        adapters: dict | None = None,
        omi: OmiMirror | None = None,
    ) -> None:
        self.url = url
        self.token = token
        self.host_id = host_id
        self.socket_path = socket_path
        self.registry = registry
        self.seen = seen
        self.adapters = adapters if adapters is not None else default_adapters()
        self.omi = omi if omi is not None else OmiMirror(enabled=False)
        self.stop = asyncio.Event()
        self.connected = asyncio.Event()
        self.backoff_start = 1.0
        self.ws = None
        self.unix = None
        self.send_lock = asyncio.Lock()
        self.waiters: dict[str, asyncio.Future] = {}
        self.registered: dict[str, dict] = {}
        self.reg_lock = asyncio.Lock()

    async def run(self) -> None:
        await self._listen()
        delay = self.backoff_start
        try:
            while not self.stop.is_set():
                try:
                    async with ws_connect(self.url, max_size=MAX_FRAME) as ws:
                        delay = self.backoff_start
                        await self._session(ws)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.warning(
                        "broker link dropped for %s: %s", self.host_id, type(exc).__name__
                    )
                self.connected.clear()
                self.ws = None
                self.registered.clear()
                self._fail_waiters("broker-down")
                if self.stop.is_set():
                    return
                try:
                    await asyncio.wait_for(self.stop.wait(), delay)
                    return
                except asyncio.TimeoutError:
                    delay = min(delay * 2, 30)
        finally:
            if self.unix is not None:
                self.unix.close()
                await self.unix.wait_closed()

    async def close(self) -> None:
        self.stop.set()
        ws = self.ws
        if ws is not None:
            await ws.close()
        if self.unix is not None:
            self.unix.close()

    async def _listen(self) -> None:
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)
        os.chmod(self.socket_path.parent, 0o700)
        if self.socket_path.exists():
            if await asyncio.to_thread(_socket_open, self.socket_path):
                raise SystemExit(f"relay already running at {self.socket_path}")
            self.socket_path.unlink()
        self.unix = await asyncio.start_unix_server(self._client, path=str(self.socket_path))
        os.chmod(self.socket_path, 0o600)

    async def _session(self, ws) -> None:
        self.ws = ws
        self.registered.clear()
        async with self.send_lock:
            await ws.send(
                dumps({"type": "hello", "v": 1, "host": self.host_id, "token": self.token})
            )
        raw = await asyncio.wait_for(ws.recv(), 5)
        welcome = loads(raw)
        if welcome.get("type") != "welcome":
            raise RuntimeError(welcome.get("error", "no welcome"))
        self.connected.set()
        reader = asyncio.create_task(self._read(ws))
        poller = asyncio.create_task(self._poll())
        try:
            await self.sync()
            await reader
        finally:
            reader.cancel()
            poller.cancel()

    async def _poll(self) -> None:
        while not self.stop.is_set():
            try:
                await self.sync()
            except Exception as exc:
                log.warning("registry sync failed: %s", type(exc).__name__)
            try:
                await asyncio.wait_for(self.stop.wait(), 1)
                return
            except asyncio.TimeoutError:
                pass
            if self.ws is not None:
                try:
                    async with self.send_lock:
                        await self.ws.send(dumps({"type": "pull"}))
                except Exception:
                    log.warning("queue pull failed for %s", self.host_id)

    async def sync(self) -> None:
        if self.ws is None:
            return
        live = load_live(self.registry)
        async with self.reg_lock:
            stale = [name for name in self.registered if name not in live]
            for name in stale:
                async with self.send_lock:
                    await self.ws.send(dumps({"type": "unregister", "name": name}))
                self.registered.pop(name, None)
            pending = [binding for name, binding in live.items() if name not in self.registered]
        for binding in pending:
            await self._register(binding)

    async def _register(self, binding: dict) -> None:
        name = binding["name"]
        async with self.reg_lock:
            if name in self.registered or self.ws is None:
                return
            loop = asyncio.get_running_loop()
            fut: asyncio.Future = loop.create_future()
            self.waiters[f"reg:{name}"] = fut
            async with self.send_lock:
                await self.ws.send(
                    dumps({"type": "register", "name": name, "harness": binding["harness"]})
                )
            try:
                msg = await asyncio.wait_for(fut, 5)
            except asyncio.TimeoutError:
                self.waiters.pop(f"reg:{name}", None)
                return
            if msg.get("type") == "registered":
                self.registered[name] = binding

    async def _read(self, ws) -> None:
        async for raw in ws:
            try:
                msg = loads(raw)
            except ProtocolError:
                continue
            kind = msg.get("type")
            if kind == "deliver":
                await self._deliver(msg)
                continue
            key = None
            if kind in {"registered", "error"} and msg.get("name"):
                key = f"reg:{msg['name']}"
            elif msg.get("id"):
                key = str(msg["id"])
            if key and key in self.waiters:
                fut = self.waiters.pop(key)
                if not fut.done():
                    fut.set_result(msg)

    async def _deliver(self, msg: dict) -> None:
        mid = str(msg.get("id") or "")
        if mid and self.seen.done(mid):
            await self._ack(mid)
            return
        binding = self.registered.get(msg.get("to")) or load_live(self.registry).get(msg.get("to"))
        if binding is None:
            return
        binding = dict(binding)
        binding["relay_socket"] = str(self.socket_path)
        fn = self.adapters.get(binding["harness"])
        if fn is None:
            await self._nack(mid)
            return
        try:
            ok = await asyncio.to_thread(fn, binding, msg)
        except Exception as exc:
            log.warning("delivery failed for %s: %s", mid, type(exc).__name__)
            ok = False
        if not ok or not mid:
            await self._nack(mid)
            return
        self.seen.mark(mid)
        await self._ack(mid)
        if self.omi.enabled:
            await asyncio.to_thread(self.omi.write, msg)

    async def _ack(self, mid: str) -> None:
        await self._signal("ack", mid)

    async def _nack(self, mid: str) -> None:
        await self._signal("nack", mid)

    async def _signal(self, kind: str, mid: str) -> None:
        if self.ws is None or not mid:
            return
        try:
            async with self.send_lock:
                await self.ws.send(dumps({"type": kind, "id": mid}))
        except Exception:
            log.warning("%s failed for %s", kind, mid)

    async def local_send(self, sender: str, recipient: str, text: str, priority: str = "next") -> dict:
        sender = check_name(sender, "from")
        recipient = check_name(recipient, "to")
        text = check_text(text)
        priority = check_priority(priority)
        if sender not in load_live(self.registry):
            raise LocalError("sender-offline")
        if self.ws is None:
            raise LocalError("broker-down")
        if sender not in self.registered:
            await self._register(load_live(self.registry)[sender])
        if sender not in self.registered:
            raise LocalError("not-registered")
        mid = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self.waiters[mid] = fut
        async with self.send_lock:
            await self.ws.send(
                dumps(
                    {
                        "type": "send",
                        "id": mid,
                        "from": sender,
                        "to": recipient,
                        "text": text,
                        "priority": priority,
                    }
                )
            )
        try:
            msg = await asyncio.wait_for(fut, 15)
        except asyncio.TimeoutError:
            self.waiters.pop(mid, None)
            raise LocalError("timeout") from None
        if msg.get("type") != "accepted":
            return {"ok": False, "id": mid, "error": msg.get("error", "rejected")}
        return {"ok": True, "id": mid, "waiting": bool(msg.get("waiting"))}

    async def local_who(self) -> dict:
        if self.ws is None:
            return {
                "ok": False,
                "error": "broker-down",
                "local": sorted(load_live(self.registry)),
            }
        mid = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self.waiters[mid] = fut
        async with self.send_lock:
            await self.ws.send(dumps({"type": "directory", "id": mid}))
        try:
            msg = await asyncio.wait_for(fut, 5)
        except asyncio.TimeoutError:
            self.waiters.pop(mid, None)
            raise LocalError("timeout") from None
        return {"ok": True, "agents": msg.get("agents") or []}

    async def _client(self, reader, writer) -> None:
        try:
            line = await asyncio.wait_for(reader.readline(), 10)
            msg = loads(line)
            op = msg.get("op")
            if op == "ping":
                out = {"ok": True, "op": "ping"}
            elif op == "send":
                out = await self.local_send(
                    str(msg.get("from") or ""),
                    str(msg.get("to") or ""),
                    msg.get("text"),
                    msg.get("priority"),
                )
                out["op"] = "send"
            elif op == "who":
                out = await self.local_who()
                out["op"] = "who"
            else:
                out = {"ok": False, "error": "bad-op"}
        except LocalError as exc:
            out = {"ok": False, "error": exc.code}
        except ProtocolError as exc:
            out = {"ok": False, "error": exc.code}
        except Exception:
            log.warning("local client failed", exc_info=True)
            out = {"ok": False, "error": "local-error"}
        writer.write((dumps(out) + "\n").encode())
        await writer.drain()
        writer.close()

    def _fail_waiters(self, code: str) -> None:
        for fut in self.waiters.values():
            if not fut.done():
                fut.set_result({"type": "error", "error": code})
        self.waiters.clear()


def _socket_open(path: Path) -> bool:
    import socket

    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.2)
            sock.connect(str(path))
        return True
    except OSError:
        return False
