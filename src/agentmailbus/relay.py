"""One relay per host. It holds the WebSocket and delivers on the machine."""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from pathlib import Path

from agentmailbus.adapters import default_adapters
from agentmailbus.omi import OmiMirror
from agentmailbus.paths import private_dir
from agentmailbus.protocol import (
    MAX_FRAME,
    ProtocolError,
    check_id,
    check_name,
    check_priority,
    check_text,
    dumps,
    loads,
)
from agentmailbus.registry import load_live, same_binding
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
        self.rejected: set[str] = set()
        self.reg_lock = asyncio.Lock()
        self.inbox: asyncio.Queue | None = None

    async def run(self) -> None:
        await self._listen()
        self.seen.prune()
        delay = self.backoff_start
        try:
            while not self.stop.is_set():
                try:
                    async with ws_connect(self.url, max_size=MAX_FRAME) as ws:
                        delay = self.backoff_start
                        await self._session(ws)
                except asyncio.CancelledError:
                    raise
                except RuntimeError as exc:
                    log.warning("broker refused %s: %s", self.host_id, exc)
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
        private_dir(self.socket_path.parent)
        if self.socket_path.exists():
            if await asyncio.to_thread(_socket_open, self.socket_path):
                raise SystemExit(f"relay already running at {self.socket_path}")
            self.socket_path.unlink()
        self.unix = await asyncio.start_unix_server(
            self._client, path=str(self.socket_path), limit=MAX_FRAME
        )
        os.chmod(self.socket_path, 0o600)

    async def _session(self, ws) -> None:
        self.ws = ws
        self.registered.clear()
        self.rejected.clear()
        async with self.send_lock:
            await ws.send(
                dumps({"type": "hello", "v": 1, "host": self.host_id, "token": self.token})
            )
        raw = await asyncio.wait_for(ws.recv(), 5)
        welcome = loads(raw)
        if welcome.get("type") != "welcome":
            raise RuntimeError(welcome.get("error", "no welcome"))
        log.info("connected to %s as %s", self.url, self.host_id)
        self.connected.set()
        self.inbox = asyncio.Queue()
        reader = asyncio.create_task(self._read(ws))
        poller = asyncio.create_task(self._poll())
        worker = asyncio.create_task(self._deliver_loop(self.inbox))
        try:
            await self.sync()
            await reader
        finally:
            for task in (reader, poller, worker):
                task.cancel()
            await asyncio.gather(reader, poller, worker, return_exceptions=True)
            self.inbox = None

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
        """Match the broker's view to the session files: new, gone, and changed bindings."""
        if self.ws is None:
            return
        live = load_live(self.registry)
        async with self.reg_lock:
            gone = [name for name in self.registered if name not in live]
            changed = [
                name
                for name, binding in self.registered.items()
                if name in live and not same_binding(binding, live[name])
            ]
            for name in changed:
                if live[name]["harness"] != self.registered[name]["harness"]:
                    gone.append(name)
                else:
                    self.registered[name] = live[name]
            for name in gone:
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
                log.warning("no answer from the broker registering %s", name)
                return
            if msg.get("type") == "registered":
                self.registered[name] = binding
                if name in self.rejected:
                    self.rejected.discard(name)
                log.info("registered %s (%s)", name, binding["harness"])
            elif name not in self.rejected:
                self.rejected.add(name)
                log.warning("broker rejected %s: %s", name, msg.get("error", "rejected"))

    async def _read(self, ws) -> None:
        async for raw in ws:
            try:
                msg = loads(raw)
            except ProtocolError:
                continue
            kind = msg.get("type")
            if kind == "deliver":
                if self.inbox is not None:
                    self.inbox.put_nowait(msg)
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

    async def _deliver_loop(self, inbox: asyncio.Queue) -> None:
        """Deliver one message at a time, off the read loop.

        A delivery in progress is allowed to finish when the session ends, so the
        seen-mark that stops a second injection is never skipped.
        """
        while True:
            msg = await inbox.get()
            job = asyncio.ensure_future(self._deliver(msg))
            try:
                await asyncio.shield(job)
            except asyncio.CancelledError:
                await asyncio.gather(job, return_exceptions=True)
                raise

    async def _deliver(self, msg: dict) -> None:
        mid = str(msg.get("id") or "")
        if not mid:
            return
        if self.seen.done(mid):
            await self._ack(mid)
            return
        to = msg.get("to")
        binding = load_live(self.registry).get(to) if isinstance(to, str) else None
        if binding is None:
            log.warning("no live session for %s; handing %s back", to, mid)
            await self._nack(mid)
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
        if not ok:
            await self._nack(mid)
            return
        self.seen.mark(mid)
        if self.omi.enabled:
            await asyncio.to_thread(self.omi.write, msg)
        await self._ack(mid)

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

    async def local_send(
        self,
        sender: str,
        recipient: str,
        text: str,
        priority: str = "next",
        mid: str | None = None,
    ) -> dict:
        """Send one message. A caller that retries passes the same `mid` again."""
        sender = check_name(sender, "from")
        recipient = check_name(recipient, "to")
        text = check_text(text)
        priority = check_priority(priority)
        mid = check_id(mid) if mid else str(uuid.uuid4())
        live = load_live(self.registry)
        if sender not in live:
            raise LocalError("sender-offline")
        if self.ws is None:
            raise LocalError("broker-down")
        if sender not in self.registered:
            await self._register(live[sender])
        if sender not in self.registered:
            raise LocalError("not-registered")
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
                    msg.get("id") or None,
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
        except asyncio.TimeoutError:
            out = {"ok": False, "error": "timeout"}
        except Exception:
            log.warning("local client failed", exc_info=True)
            out = {"ok": False, "error": "local-error"}
        try:
            writer.write((dumps(out) + "\n").encode())
            await writer.drain()
        except (OSError, asyncio.CancelledError):
            pass
        finally:
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
