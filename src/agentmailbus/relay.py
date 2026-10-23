"""One relay per host. It holds the WebSocket and delivers on the machine."""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
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
    check_recipient,
    check_text,
    dumps,
    loads,
)
from agentmailbus.registry import load_live, same_binding
from agentmailbus.store import SeenStore
from agentmailbus.wsutil import ws_connect

log = logging.getLogger("agentmailbus.relay")

SEEN_PRUNE_INTERVAL = 3600.0
WORKER_IDLE = 60.0  # a name's delivery worker exits after this long with nothing to do
DELIVERY_THREADS = 128  # adapters and OMI notes run here, not on the default pool


class BrokerRefused(RuntimeError):
    """The broker answered the hello with an error instead of a welcome."""


class LocalError(Exception):
    def __init__(self, code: str, reason: str = "") -> None:
        self.code = code
        self.reason = reason
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
        # Register replies still owed to attempts that timed out, per name. Replies
        # come back in order, so the next one for that name belongs to the dead attempt.
        self.reg_late: dict[str, int] = {}
        self.reg_timeout = 5.0
        self.reg_lock = asyncio.Lock()
        # One delivery queue and worker per name, so a slow delivery for one name
        # never holds another name's message past the broker's deadline.
        self.workers: dict[str, tuple[asyncio.Queue, asyncio.Task]] = {}
        self.pool = ThreadPoolExecutor(max_workers=DELIVERY_THREADS, thread_name_prefix="deliver")
        self.next_prune = 0.0
        # Ids that were written but could not be recorded in the seen store. Kept in
        # memory so a reconnect inside this process does not inject them again.
        self.seen_fallback: set[str] = set()

    async def run(self) -> None:
        await self._listen()
        self.prune_seen()
        delay = self.backoff_start
        try:
            while not self.stop.is_set():
                try:
                    async with ws_connect(self.url, max_size=MAX_FRAME) as ws:
                        delay = self.backoff_start
                        await self._session(ws)
                except asyncio.CancelledError:
                    raise
                except BrokerRefused as exc:
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
            self.pool.shutdown(wait=False)

    async def _in_thread(self, fn, *args):
        return await asyncio.get_running_loop().run_in_executor(self.pool, fn, *args)

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
        self.reg_late.clear()
        async with self.send_lock:
            await ws.send(
                dumps({"type": "hello", "v": 1, "host": self.host_id, "token": self.token})
            )
        raw = await asyncio.wait_for(ws.recv(), 5)
        welcome = loads(raw)
        if welcome.get("type") != "welcome":
            raise BrokerRefused(welcome.get("error", "no welcome"))
        log.info("connected to %s as %s", self.url, self.host_id)
        self.connected.set()
        self.workers = {}
        reader = asyncio.create_task(self._read(ws))
        poller = asyncio.create_task(self._poll())
        try:
            await self.sync()
            await reader
        finally:
            tasks = [reader, poller, *(task for _, task in list(self.workers.values()))]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.workers = {}

    def _enqueue(self, msg: dict) -> None:
        """Hand a deliver frame to its name's worker, starting one if needed. The
        broker keeps one message in flight per name, so each queue holds about one
        frame (a re-register can add a stale copy, which the seen set absorbs)."""
        name = msg.get("to")
        if not isinstance(name, str):
            return
        entry = self.workers.get(name)
        if entry is None or entry[1].done():
            queue: asyncio.Queue = asyncio.Queue()
            entry = (queue, asyncio.create_task(self._deliver_loop(name, queue)))
            self.workers[name] = entry
        entry[0].put_nowait(msg)

    async def _poll(self) -> None:
        while not self.stop.is_set():
            try:
                await self.sync()
            except Exception as exc:
                log.warning("registry sync failed: %s", type(exc).__name__)
            self.prune_seen()
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

    def prune_seen(self) -> None:
        """Drop old seen ids about once an hour. Runs on the loop thread: the sqlite
        connection was opened there and will not serve another thread. The deadline
        lives on the relay, so reconnects do not push it back."""
        now = time.monotonic()
        if now < self.next_prune:
            return
        self.next_prune = now + SEEN_PRUNE_INTERVAL
        try:
            dropped = self.seen.prune()
            if dropped:
                log.info("pruned %d seen ids", dropped)
        except Exception as exc:
            log.warning("seen prune failed: %s", type(exc).__name__)

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

    async def _register(self, binding: dict) -> str:
        """Register one name. Returns "" once it is registered, else why not:
        the broker's error code, `no-answer`, or `broker-down`."""
        name = binding["name"]
        async with self.reg_lock:
            if name in self.registered:
                return ""
            if self.ws is None:
                return "broker-down"
            loop = asyncio.get_running_loop()
            fut: asyncio.Future = loop.create_future()
            self.waiters[f"reg:{name}"] = fut
            try:
                async with self.send_lock:
                    await self.ws.send(
                        dumps({"type": "register", "name": name, "harness": binding["harness"]})
                    )
            except Exception:
                self.waiters.pop(f"reg:{name}", None)
                return "broker-down"
            # asyncio.wait leaves the future alone on timeout, so a reply that lands
            # as the timer fires is still read here instead of being thrown away.
            await asyncio.wait({fut}, timeout=self.reg_timeout)
            if not fut.done():
                # Nothing runs between these lines, so the reply is truly still owed.
                self.waiters.pop(f"reg:{name}", None)
                self.reg_late[name] = self.reg_late.get(name, 0) + 1
                log.warning("no answer from the broker registering %s", name)
                return "no-answer"
            msg = fut.result()
            if msg.get("type") == "registered":
                self.registered[name] = binding
                if name in self.rejected:
                    self.rejected.discard(name)
                log.info("registered %s (%s)", name, binding["harness"])
                return ""
            why = str(msg.get("error") or "rejected")
            if why == "broker-down":
                # The link dropped while waiting (_fail_waiters); not a refusal.
                log.warning("broker link dropped while registering %s", name)
                return why
            if name not in self.rejected:
                self.rejected.add(name)
                log.warning("broker rejected %s: %s", name, why)
            return why

    async def _read(self, ws) -> None:
        async for raw in ws:
            try:
                msg = loads(raw)
            except ProtocolError:
                continue
            kind = msg.get("type")
            if kind == "deliver":
                self._enqueue(msg)
                continue
            key = None
            if kind in {"registered", "error"} and msg.get("name"):
                name = str(msg["name"])
                if self.reg_late.get(name):
                    # The answer to an attempt that already gave up: not this one's.
                    self.reg_late[name] -= 1
                    continue
                key = f"reg:{name}"
            elif msg.get("id"):
                key = str(msg["id"])
            if key and key in self.waiters:
                fut = self.waiters.pop(key)
                if not fut.done():
                    fut.set_result(msg)

    async def _deliver_loop(self, name: str, inbox: asyncio.Queue) -> None:
        """Deliver one name's messages, one at a time, off the read loop.

        A delivery in progress is allowed to finish when the session ends, so the
        seen-mark that stops a second injection is never skipped. A worker with
        nothing to do for WORKER_IDLE seconds removes itself; `_enqueue` starts a
        fresh one when the next frame arrives.
        """
        while True:
            try:
                msg = await asyncio.wait_for(inbox.get(), WORKER_IDLE)
            except asyncio.TimeoutError:
                if inbox.empty():
                    if self.workers.get(name, (None, None))[0] is inbox:
                        del self.workers[name]
                    return
                continue
            job = asyncio.ensure_future(self._deliver(msg))
            try:
                await asyncio.shield(job)
            except asyncio.CancelledError:
                await asyncio.gather(job, return_exceptions=True)
                raise
            except Exception as exc:
                # One bad delivery (a relay.db error, say) hands its message back and
                # the worker carries on. It never dies quietly.
                mid = str(msg.get("id") or "")
                log.warning("delivery of %s raised %s; handing it back", mid, type(exc).__name__)
                await self._nack(mid)
            finally:
                msg = job = None  # keep no payload alive while idle

    async def _deliver(self, msg: dict) -> None:
        mid = str(msg.get("id") or "")
        if not mid:
            return
        if mid in self.seen_fallback or self.seen.done(mid):
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
            ok = await self._in_thread(fn, binding, msg)
        except Exception as exc:
            log.warning("delivery of %s to %s raised %s; handing it back", mid, to, type(exc).__name__)
            await self._nack(mid)
            return
        if not ok:
            log.warning("delivery of %s to %s failed; handing it back", mid, to)
            await self._nack(mid)
            return
        # The bytes are in the session. From here on nothing may turn into a nack,
        # or the broker would hand the same message back for a second injection.
        try:
            self.seen.mark(mid)
        except Exception as exc:
            log.warning("could not record %s as seen: %s", mid, type(exc).__name__)
            self.seen_fallback.add(mid)
        else:
            self._drain_fallback()
        await self._ack(mid)
        # The note is bookkeeping; the next message for this name waits on the ack,
        # so it goes out before the note is written.
        if self.omi.enabled:
            try:
                await self._in_thread(self.omi.write, msg)
            except Exception as exc:
                log.warning("omi mirror raised for %s: %s", mid, type(exc).__name__)

    def _drain_fallback(self) -> None:
        """The seen store works again: move the ids held in memory into it."""
        for held in list(self.seen_fallback):
            try:
                self.seen.mark(held)
            except Exception:
                return
            self.seen_fallback.discard(held)

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
        recipient = check_recipient(recipient)
        text = check_text(text)
        priority = check_priority(priority)
        mid = check_id(mid) if mid else str(uuid.uuid4())
        live = load_live(self.registry)
        if sender not in live:
            raise LocalError("sender-offline")
        if self.ws is None:
            raise LocalError("broker-down")
        why = ""
        if sender not in self.registered:
            why = await self._register(live[sender])
        if sender not in self.registered:
            # Say why, so a refusal that waiting cannot fix (name-taken) is told apart.
            raise LocalError("not-registered", why)
        if mid in self.waiters:
            # The same id is already on the wire; a second caller would steal its reply.
            raise LocalError("in-flight")
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self.waiters[mid] = fut
        try:
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
        except Exception:
            # Nothing reached the broker; the id must not stay marked in flight.
            self.waiters.pop(mid, None)
            raise LocalError("broker-down") from None
        try:
            msg = await asyncio.wait_for(fut, 15)
        except asyncio.TimeoutError:
            self.waiters.pop(mid, None)
            raise LocalError("timeout") from None
        if msg.get("type") != "accepted":
            if msg.get("error") == "not-your-agent":
                # The broker does not count this relay as the owner (the link or the
                # binding changed during the send): register before the next try.
                self.registered.pop(sender, None)
            return {"ok": False, "id": mid, "error": msg.get("error", "rejected")}
        out = {"ok": True, "id": mid, "waiting": bool(msg.get("waiting"))}
        if "recipients" in msg:
            names = msg.get("recipients")
            out["recipients"] = [n for n in names if isinstance(n, str)] if isinstance(names, list) else []
            if type(msg.get("more")) is int:
                out["more"] = msg["more"]
        return out

    async def local_status(self, message_id) -> dict:
        """Ask the broker where one message stands, per recipient."""
        message_id = check_id(message_id)
        if self.ws is None:
            raise LocalError("broker-down")
        rid = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self.waiters[rid] = fut
        try:
            async with self.send_lock:
                await self.ws.send(dumps({"type": "status", "id": rid, "message": message_id}))
        except Exception:
            self.waiters.pop(rid, None)
            raise LocalError("broker-down") from None
        try:
            msg = await asyncio.wait_for(fut, 5)
        except asyncio.TimeoutError:
            self.waiters.pop(rid, None)
            raise LocalError("timeout") from None
        if msg.get("type") != "status":
            # An older broker answers bad-type: it cannot give receipts.
            return {"ok": False, "id": message_id, "error": str(msg.get("error") or "status-failed")}
        return {
            "ok": True,
            "id": message_id,
            "kind": msg.get("kind"),
            "broadcast": msg.get("broadcast") or "",
            "from": msg.get("from"),
            "created_at": msg.get("created_at"),
            "recipients": [r for r in msg.get("recipients") or [] if isinstance(r, dict)]
            if isinstance(msg.get("recipients"), list)
            else [],
            "more": msg.get("more") if type(msg.get("more")) is int else 0,
        }

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
            elif op == "status":
                out = await self.local_status(msg.get("id"))
                out["op"] = "status"
            else:
                out = {"ok": False, "error": "bad-op"}
        except LocalError as exc:
            out = {"ok": False, "error": exc.code}
            if exc.reason:
                out["reason"] = exc.reason
        except ProtocolError as exc:
            out = {"ok": False, "error": exc.code}
        except ValueError:
            # asyncio's readline raises ValueError for a line over the limit.
            out = {"ok": False, "error": "too-large"}
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
