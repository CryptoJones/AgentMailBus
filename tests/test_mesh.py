import asyncio
import os
from pathlib import Path

import pytest

from agentmailbus.broker import Broker
from agentmailbus.net import parse_allow
from agentmailbus.registry import save_binding
from agentmailbus.relay import Relay
from agentmailbus.store import BrokerStore, SeenStore


class Recording:
    def __init__(self) -> None:
        self.messages = []

    def deliver(self, binding, message) -> bool:
        self.messages.append(message)
        return True


class Mirror:
    enabled = True

    def __init__(self) -> None:
        self.messages = []

    def write(self, message) -> None:
        self.messages.append(message)


def test_offline_agent_waits_then_receives(tmp_path: Path) -> None:
    asyncio.run(_offline(tmp_path))


def test_unknown_agent_and_lan_rejection(tmp_path: Path) -> None:
    asyncio.run(_reject(tmp_path))


async def _mesh(tmp_path: Path, name: str, host: str, allow: str, token: str = "test-token"):
    store = BrokerStore(tmp_path / name / "broker.db")
    broker = Broker(store, token, parse_allow([allow]), "127.0.0.1", 0)
    await broker.start()
    return broker


def _bind(directory: Path, agent: str) -> None:
    save_binding(
        directory,
        {
            "name": agent,
            "harness": "grok",
            "pid": os.getpid(),
            "wake": str(directory / f"{agent}.wake"),
            "socket": "",
        },
    )


async def _relay(tmp_path: Path, host: str, port: int, token: str, adapter, omi=None):
    base = tmp_path / host
    deliver = adapter.deliver
    relay = Relay(
        url=f"ws://127.0.0.1:{port}",
        token=token,
        host_id=host,
        socket_path=base / "relay.sock",
        registry=base / "sessions",
        seen=SeenStore(base / "seen.db"),
        adapters={"grok": deliver, "orca": deliver, "claude": deliver, "dsh": deliver},
        omi=omi,
    )
    relay.backoff_start = 0.05
    _bind(relay.registry, "alpha" if host == "host-a" else "beta")
    task = asyncio.create_task(relay.run())
    await asyncio.wait_for(relay.connected.wait(), 5)
    return relay, task


async def _offline(tmp_path: Path) -> None:
    broker = await _mesh(tmp_path, "q", "host-a", "127.0.0.0/8")
    rec_a, rec_b = Recording(), Recording()
    mirror = Mirror()
    try:
        relay_a, task_a = await _relay(tmp_path, "host-a", broker.port, "test-token", rec_a)
        relay_b, task_b = await _relay(
            tmp_path, "host-b", broker.port, "test-token", rec_b, mirror
        )
        # host-a is bound as alpha and host-b as beta. Cross-bind so each can send as itself.
        first = await relay_a.local_send("alpha", "beta", "hello")
        assert first["ok"] is True
        assert first["waiting"] is False
        await _until(lambda: rec_b.messages and mirror.messages)
        assert rec_b.messages[0]["text"] == "hello"
        assert rec_b.messages[0]["from_host"] == "host-a"
        assert mirror.messages[0]["id"] == rec_b.messages[0]["id"]

        await relay_b.close()
        await task_b
        await asyncio.sleep(0.2)
        second = await relay_a.local_send("alpha", "beta", "later")
        assert second["ok"] is True
        assert second["waiting"] is True
        assert [m["text"] for m in rec_b.messages] == ["hello"]

        relay_b2, task_b2 = await _relay(
            tmp_path, "host-b", broker.port, "test-token", rec_b, mirror
        )
        await _until(lambda: any(m["text"] == "later" for m in rec_b.messages))
        await relay_b2.close()
        await task_b2
        await relay_a.close()
        await task_a
    finally:
        await broker.stop()


async def _reject(tmp_path: Path) -> None:
    broker = await _mesh(tmp_path, "r", "host-a", "127.0.0.0/8")
    rec = Recording()
    try:
        relay, task = await _relay(tmp_path, "host-a", broker.port, "test-token", rec)
        missing = await relay.local_send("alpha", "nobody", "hi")
        assert missing == {"ok": False, "id": missing["id"], "error": "unknown-agent"}
        await relay.close()
        await task
    finally:
        await broker.stop()

    closed = Broker(
        BrokerStore(tmp_path / "lan.db"),
        "test-token",
        parse_allow(["10.0.0.0/8"]),
        "127.0.0.1",
        0,
    )
    await closed.start()
    try:
        from agentmailbus.wsutil import ws_connect

        with pytest.raises(Exception):
            async with ws_connect(f"ws://127.0.0.1:{closed.port}") as ws:
                await ws.send('{"type":"hello","v":1,"host":"host-a","token":"test-token"}')
                await ws.recv()
    finally:
        await closed.stop()


async def _until(pred) -> None:
    for _ in range(50):
        if pred():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("timed out")
