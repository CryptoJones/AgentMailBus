"""A refused send says what happened and what to do, and never claims it was sent."""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from pathlib import Path

import pytest

from agentmailbus import cli
from agentmailbus.broker import Broker
from agentmailbus.mcp import handle_request, named_error
from agentmailbus.net import parse_allow
from agentmailbus.registry import save_binding
from agentmailbus.relay import LocalError
from agentmailbus.store import BrokerStore

from test_broadcast import Recording, _relay, _stop


def _cli_send(sock: Path, sender: str, mid: str, to: str = "alpha") -> None:
    cli.main(["send", "--from", sender, "--to", to, "--text", "hi", "--id", mid, "--relay-socket", str(sock)])


def _spy_sends(relay) -> list[dict]:
    """Record every send frame the relay puts on the wire to the broker."""
    frames: list[dict] = []
    real = relay.ws.send

    async def send(raw):
        msg = json.loads(raw)
        if msg.get("type") == "send":
            frames.append(msg)
        return await real(raw)

    relay.ws.send = send
    return frames


def _bind(base: Path, name: str) -> None:
    save_binding(
        base / "sessions",
        {"name": name, "harness": "grok", "pid": os.getpid(), "wake": str(base / f"{name}.wake"), "socket": ""},
    )


def test_offline_sender_is_refused_with_a_fix_and_the_retry_is_the_same_message(tmp_path: Path, capsys) -> None:
    asyncio.run(_offline_then_bound(tmp_path, capsys))


async def _offline_then_bound(tmp_path: Path, capsys) -> None:
    store = BrokerStore(tmp_path / "broker.db")
    broker = Broker(store, "test-token", parse_allow(["127.0.0.0/8"]), "127.0.0.1", 0)
    await broker.start()
    rec = Recording()
    running = None
    try:
        running = await _relay(tmp_path, "host-a", "alpha", broker.port, rec)
        frames = _spy_sends(running[0])
        sock = tmp_path / "host-a" / "relay.sock"
        mid = str(uuid.uuid4())
        capsys.readouterr()
        with pytest.raises(SystemExit) as info:
            await asyncio.to_thread(_cli_send, sock, "ghost", mid)
        text = str(info.value)
        assert text.startswith(f"sender-offline (message id {mid}; this attempt sent no message, retry with this id): ")
        assert "agentmailbus bind --harness HARNESS ghost" in text
        assert "same message, not a copy" in text
        # Nothing claimed success, and no send frame left the relay.
        assert capsys.readouterr().out == ""
        assert frames == []
        assert rec.messages == []
        assert store.get_message(mid) is None

        # Bind the name, retry with the same id: one frame, one delivery, and no second.
        _bind(tmp_path / "host-a", "ghost")
        await asyncio.to_thread(_cli_send, sock, "ghost", mid)
        assert capsys.readouterr().out.strip() == f"{mid} accepted"
        for _ in range(100):
            if rec.messages:
                break
            await asyncio.sleep(0.05)
        await asyncio.sleep(0.5)
        assert [f["id"] for f in frames] == [mid]
        assert [m["id"] for m in rec.messages] == [mid]
    finally:
        if running:
            await _stop(*running)
        await broker.stop()


def test_a_name_held_by_another_host_says_so(tmp_path: Path) -> None:
    asyncio.run(_name_taken(tmp_path))


async def _name_taken(tmp_path: Path) -> None:
    store = BrokerStore(tmp_path / "broker.db")
    broker = Broker(store, "test-token", parse_allow(["127.0.0.0/8"]), "127.0.0.1", 0)
    await broker.start()
    a = b = None
    try:
        a = await _relay(tmp_path, "host-a", "alpha", broker.port, Recording())
        b = await _relay(tmp_path, "host-b", "beta", broker.port, Recording())
        frames = _spy_sends(b[0])
        _bind(tmp_path / "host-b", "alpha")  # host-a holds this name and is connected
        with pytest.raises(LocalError) as info:
            await b[0].local_send("alpha", "beta", "x")
        assert (info.value.code, info.value.reason) == ("not-registered", "name-taken")
        assert frames == []
        text = named_error("not-registered", "m", reason="name-taken")
        assert text.startswith("not-registered (message id m; this attempt sent no message, retry with this id): ")
        assert "another host" in text
        # The reason survives the relay's local socket to the command line.
        with pytest.raises(SystemExit) as info:
            await asyncio.to_thread(_cli_send, tmp_path / "host-b" / "relay.sock", "alpha", str(uuid.uuid4()), "beta")
        assert str(info.value).startswith("not-registered (message id ")
        assert "name-taken" in str(info.value) and "another host" in str(info.value)
        assert frames == []
    finally:
        for r in (b, a):
            if r:
                await _stop(*r)
        await broker.stop()


def test_mcp_and_cli_carry_the_refusal_reason(monkeypatch) -> None:
    mid = str(uuid.uuid4())
    reply = {"ok": False, "id": mid, "error": "not-registered", "reason": "name-taken"}
    out = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "send", "arguments": {"to": "b", "text": "t", "id": mid}}},
        "alpha",
        lambda payload: reply,
    )
    assert "name-taken" in out["result"]["content"][0]["text"]
    monkeypatch.setattr(cli, "local_rpc", lambda sock, payload: reply)
    with pytest.raises(SystemExit) as info:
        cli.main(["send", "--from", "a", "--to", "b", "--text", "x", "--id", mid])
    assert "name-taken" in str(info.value) and "another host" in str(info.value)


def test_in_flight_does_not_claim_nothing_was_sent() -> None:
    text = named_error("in-flight", "m")
    assert "sent no" not in text
    assert text.startswith("in-flight (message id m; an earlier send with this id is still in progress")


def test_mcp_sender_offline_says_this_attempt_sent_nothing() -> None:
    mid = str(uuid.uuid4())
    out = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "send", "arguments": {"to": "b", "text": "t", "id": mid}}},
        "alpha",
        lambda payload: {"ok": False, "id": mid, "error": "sender-offline"},
    )
    assert out["result"]["isError"] is True
    assert out["result"]["content"][0]["text"].startswith(
        f"sender-offline (message id {mid}; this attempt sent no message, retry with this id): this session has no live binding"
    )


def test_cli_unknown_recipient_points_at_who(monkeypatch) -> None:
    mid = str(uuid.uuid4())
    monkeypatch.setattr(cli, "local_rpc", lambda sock, payload: {"ok": False, "id": mid, "error": "unknown-agent"})
    with pytest.raises(SystemExit) as info:
        cli.main(["send", "--from", "a", "--to", "b", "--text", "x", "--id", mid])
    assert str(info.value).startswith(f"unknown-agent (message id {mid}; not retryable as is, send a new message with a new id): ")
    assert "agentmailbus who" in str(info.value)


def test_relay_down_hint_matches_the_failure(monkeypatch) -> None:
    for exc, hint in ((ConnectionRefusedError(), "and is that the right socket?)"), (TimeoutError(), "did not answer cleanly")):
        monkeypatch.setattr(cli, "local_rpc", lambda sock, payload, e=exc: (_ for _ in ()).throw(e))
        with pytest.raises(SystemExit) as info:
            cli.main(["send", "--from", "a", "--to", "b", "--text", "x"])
        assert str(info.value).startswith("relay-down: ") and str(info.value).endswith(hint)


def test_a_malformed_id_is_not_retried_as_is(tmp_path: Path) -> None:
    text = named_error("bad-id", "nope")
    assert text.startswith("bad-id (message id nope; this attempt sent no message, retry with a UUID")
    assert "retry with this id" not in text


def test_relay_socket_permission_hint(monkeypatch) -> None:
    monkeypatch.setattr(cli, "local_rpc", lambda sock, payload: (_ for _ in ()).throw(PermissionError()))
    with pytest.raises(SystemExit) as info:
        cli.main(["send", "--from", "a", "--to", "b", "--text", "x"])
    assert "this attempt sent no message" in str(info.value) and "run as the relay's user" in str(info.value)


def _bare_relay(tmp_path: Path):
    from agentmailbus.relay import Relay
    from agentmailbus.store import SeenStore

    return Relay(
        url="ws://127.0.0.1:1",
        token="t",
        host_id="h",
        socket_path=tmp_path / "relay.sock",
        registry=tmp_path / "sessions",
        seen=SeenStore(tmp_path / "seen.db"),
        adapters={},
    )


class _Wire:
    """A broker link: records what the relay sends, and replays frames into _read."""

    def __init__(self):
        self.sent: list[dict] = []
        self.frames: list[str] = []

    async def send(self, raw):
        self.sent.append(json.loads(raw))

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.frames:
            raise StopAsyncIteration
        return self.frames.pop(0)

    async def reply(self, relay, *frames):
        self.frames = [json.dumps(f) for f in frames]
        await relay._read(self)


def test_a_register_that_times_out_drops_its_late_reply_and_the_next_one_works(tmp_path: Path) -> None:
    asyncio.run(_timeout_then_late(tmp_path))


async def _timeout_then_late(tmp_path: Path) -> None:
    relay = _bare_relay(tmp_path)
    wire = _Wire()
    relay.ws = wire
    relay.reg_timeout = 0.05
    binding = {"name": "x", "harness": "grok"}
    assert await relay._register(binding) == "no-answer"
    assert relay.reg_late == {"x": 1}
    # The next attempt is waiting when the first attempt's late refusal arrives,
    # then its own success.
    relay.reg_timeout = 5.0
    second = asyncio.create_task(relay._register(binding))
    for _ in range(100):
        if "reg:x" in relay.waiters:
            break
        await asyncio.sleep(0.01)
    assert "reg:x" in relay.waiters
    await wire.reply(relay, {"type": "error", "name": "x", "error": "name-taken"}, {"type": "registered", "name": "x"})
    assert await second == ""
    assert "x" in relay.registered and relay.reg_late == {"x": 0}


def test_a_reply_that_lands_as_the_timeout_fires_is_used(tmp_path: Path, monkeypatch) -> None:
    asyncio.run(_reply_at_the_deadline(tmp_path, monkeypatch))


async def _reply_at_the_deadline(tmp_path: Path, monkeypatch) -> None:
    from agentmailbus import relay as relay_mod

    relay = _bare_relay(tmp_path)
    wire = _Wire()
    relay.ws = wire
    relay.reg_timeout = 0.01
    real_wait = asyncio.wait

    async def wait_then_reply(fs, timeout=None):
        out = await real_wait(fs, timeout=timeout)
        # The timer has fired; the reply is read before _register resumes.
        await wire.reply(relay, {"type": "registered", "name": "x"})
        return out

    monkeypatch.setattr(relay_mod.asyncio, "wait", wait_then_reply)
    assert await relay._register({"name": "x", "harness": "grok"}) == ""
    assert "x" in relay.registered and relay.reg_late == {}


def test_a_relay_socket_nobody_listens_on_sent_nothing(tmp_path: Path) -> None:
    mid = str(uuid.uuid4())
    sock = tmp_path / "nothing.sock"
    with pytest.raises(SystemExit) as info:
        cli.main(["send", "--from", "a", "--to", "b", "--text", "x", "--id", mid, "--relay-socket", str(sock)])
    assert str(info.value) == (
        f"relay-down: FileNotFoundError (message id {mid}; this attempt sent no message, retry with --id {mid}): "
        f"no relay is listening at {sock} (is the relay running, and is that the right socket?)"
    )


def test_a_dropped_link_while_registering_is_not_logged_as_a_rejection(tmp_path: Path, caplog) -> None:
    asyncio.run(_dropped_while_registering(tmp_path))
    assert "broker rejected" not in caplog.text


async def _dropped_while_registering(tmp_path: Path) -> None:
    relay = _bare_relay(tmp_path)
    relay.ws = _Wire()
    task = asyncio.create_task(relay._register({"name": "x", "harness": "grok"}))
    for _ in range(100):
        if "reg:x" in relay.waiters:
            break
        await asyncio.sleep(0.01)
    relay._fail_waiters("broker-down")
    assert await task == "broker-down"
    assert "x" not in relay.rejected


def test_an_undecodable_id_is_refused_without_a_traceback(monkeypatch) -> None:
    monkeypatch.setattr(cli, "local_rpc", lambda *a: (_ for _ in ()).throw(AssertionError("sent")))
    with pytest.raises(SystemExit) as info:
        cli.main(["send", "--from", "a", "--to", "b", "--text", "x", "--id", "a\udcff"])
    assert str(info.value) == (
        "bad-id (this attempt sent no message): --id holds bytes that are not valid text; "
        "retry with a UUID as the id or with no id"
    )


def test_not_your_agent_says_nothing_was_sent_and_why(monkeypatch) -> None:
    mid = str(uuid.uuid4())
    monkeypatch.setattr(cli, "local_rpc", lambda sock, payload: {"ok": False, "id": mid, "error": "not-your-agent"})
    with pytest.raises(SystemExit) as info:
        cli.main(["send", "--from", "a", "--to", "b", "--text", "x", "--id", mid])
    assert str(info.value).startswith(f"not-your-agent (message id {mid}; this attempt sent no message, retry with this id): ")
    assert "registers the name again" in str(info.value)


def test_not_your_agent_makes_the_relay_register_again(tmp_path: Path) -> None:
    asyncio.run(_not_yours(tmp_path))


async def _not_yours(tmp_path: Path) -> None:
    relay = _bare_relay(tmp_path)
    wire = _Wire()
    relay.ws = wire
    _bind(tmp_path, "x")
    relay.registered["x"] = {"name": "x", "harness": "grok"}
    task = asyncio.create_task(relay.local_send("x", "y", "hi"))
    for _ in range(100):
        if wire.sent:
            break
        await asyncio.sleep(0.01)
    mid = wire.sent[-1]["id"]
    await wire.reply(relay, {"type": "error", "id": mid, "error": "not-your-agent"})
    assert (await task)["error"] == "not-your-agent"
    assert "x" not in relay.registered
