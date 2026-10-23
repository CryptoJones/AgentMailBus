"""Broadcast: one message to every agent online now, delivered as ordinary copies."""

import asyncio
import json
import os
import sqlite3
import uuid
from pathlib import Path

from agentmailbus.adapters import claude_frames, deliver_wake
from agentmailbus.broker import Broker
from agentmailbus.mcp import handle_request
from agentmailbus.net import parse_allow
from agentmailbus.protocol import copy_id
from agentmailbus.registry import save_binding
from agentmailbus.relay import Relay
from agentmailbus.store import BrokerStore, SeenStore


class Recording:
    def __init__(self) -> None:
        self.messages = []

    def deliver(self, binding, message) -> bool:
        self.messages.append(message)
        return True


async def _relay(tmp_path: Path, host: str, agent: str, port: int, rec: Recording):
    base = tmp_path / host
    save_binding(
        base / "sessions",
        {"name": agent, "harness": "grok", "pid": os.getpid(), "wake": str(base / f"{agent}.wake"), "socket": ""},
    )
    relay = Relay(
        url=f"ws://127.0.0.1:{port}",
        token="test-token",
        host_id=host,
        socket_path=base / "relay.sock",
        registry=base / "sessions",
        seen=SeenStore(base / "seen.db"),
        adapters={h: rec.deliver for h in ("grok", "orca", "claude", "dsh")},
    )
    relay.backoff_start = 0.05
    task = asyncio.create_task(relay.run())
    await asyncio.wait_for(relay.connected.wait(), 5)
    await _until(lambda: agent in relay.registered)
    return relay, task


async def _stop(relay, task) -> None:
    await relay.close()
    await task


async def _until(pred) -> None:
    for _ in range(100):
        if pred():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("timed out")


def test_broadcast_reaches_everyone_online_once(tmp_path: Path) -> None:
    asyncio.run(_reaches(tmp_path))


async def _reaches(tmp_path: Path) -> None:
    store = BrokerStore(tmp_path / "broker.db")
    broker = Broker(store, "test-token", parse_allow(["127.0.0.0/8"]), "127.0.0.1", 0)
    await broker.start()
    rec_a, rec_b, rec_c = Recording(), Recording(), Recording()
    try:
        a = await _relay(tmp_path, "host-a", "alpha", broker.port, rec_a)
        b = await _relay(tmp_path, "host-b", "beta", broker.port, rec_b)
        c = await _relay(tmp_path, "host-c", "gamma", broker.port, rec_c)
        relay_a = a[0]

        first = await relay_a.local_send("alpha", "*", "all hands")
        assert first["ok"] is True
        assert first["recipients"] == ["beta", "gamma"]
        await _until(lambda: rec_b.messages and rec_c.messages)
        for rec, name in ((rec_b, "beta"), (rec_c, "gamma")):
            got = rec.messages[0]
            assert got["text"] == "all hands"
            assert got["from"] == "alpha"
            assert got["to"] == name
            assert got["broadcast"] == first["id"]
            salt = store.get_broadcast(first["id"])["salt"]
            assert got["id"] == copy_id(salt, name)
            assert got["id"] != copy_id(first["id"], name)
        assert rec_a.messages == []

        # A retry with the same id reaches nobody twice, and keeps the first text.
        again = await relay_a.local_send("alpha", "*", "changed text", mid=first["id"])
        assert again["ok"] is True
        assert again["recipients"] == ["beta", "gamma"]
        await asyncio.sleep(0.3)
        assert {r["text"] for r in store.conn.execute("SELECT text FROM messages WHERE broadcast=?", (first["id"],))} == {"all hands"}
        assert len(rec_b.messages) == 1 and len(rec_c.messages) == 1
        count = store.conn.execute(
            "SELECT COUNT(*) FROM messages WHERE broadcast=?", (first["id"],)
        ).fetchone()[0]
        assert count == 2

        # A direct message is not marked.
        direct = await relay_a.local_send("alpha", "beta", "just you")
        assert direct["ok"] is True and "recipients" not in direct
        await _until(lambda: len(rec_b.messages) == 2)
        assert "broadcast" not in rec_b.messages[1]

        # An offline name gets nothing, now or when it comes back.
        await _stop(*c)
        await _until(lambda: not any(r["name"] == "gamma" and r["online"] for r in store.directory()))
        second = await relay_a.local_send("alpha", "*", "while gamma is away")
        assert second["recipients"] == ["beta"]
        c = await _relay(tmp_path, "host-c", "gamma", broker.port, rec_c)
        await asyncio.sleep(0.5)
        assert [m["text"] for m in rec_c.messages] == ["all hands"]

        # Nobody else online: accepted, reaches no one.
        await _stop(*b)
        await _stop(*c)
        await _until(lambda: [r["name"] for r in store.directory() if r["online"]] == ["alpha"])
        alone = await relay_a.local_send("alpha", "*", "anyone?")
        assert alone == {"ok": True, "id": alone["id"], "waiting": False, "recipients": []}
        await _stop(*a)
    finally:
        await broker.stop()


def test_broadcast_id_conflicts(tmp_path: Path) -> None:
    asyncio.run(_conflicts(tmp_path))


async def _conflicts(tmp_path: Path) -> None:
    store = BrokerStore(tmp_path / "broker.db")
    broker = Broker(store, "test-token", parse_allow(["127.0.0.0/8"]), "127.0.0.1", 0)
    await broker.start()
    rec_a, rec_b = Recording(), Recording()
    try:
        a = await _relay(tmp_path, "host-a", "alpha", broker.port, rec_a)
        b = await _relay(tmp_path, "host-b", "beta", broker.port, rec_b)
        # An id already used by a direct message cannot become a broadcast.
        direct = await a[0].local_send("alpha", "beta", "direct")
        clash = await a[0].local_send("alpha", "*", "cast", mid=direct["id"])
        assert clash == {"ok": False, "id": direct["id"], "error": "id-conflict"}
        # A broadcast id belongs to its sender.
        cast = await a[0].local_send("alpha", "*", "cast")
        assert cast["recipients"] == ["beta"]
        stolen = await b[0].local_send("beta", "*", "cast", mid=cast["id"])
        assert stolen == {"ok": False, "id": cast["id"], "error": "id-conflict"}
        await _stop(*a)
        await _stop(*b)
    finally:
        await broker.stop()


def test_copy_id_is_stable_and_canonical() -> None:
    mid = str(uuid.uuid4())
    assert copy_id(mid, "beta") == copy_id(mid, "beta")
    assert copy_id(mid, "beta") != copy_id(mid, "gamma")
    assert copy_id(mid, "beta") == str(uuid.UUID(copy_id(mid, "beta")))


def test_frames_mark_a_broadcast(tmp_path: Path) -> None:
    binding = {"pid": os.getpid(), "socket": str(tmp_path / "s.sock"), "relay_socket": "r"}
    message = {"id": str(uuid.uuid4()), "from": "alpha", "from_host": "h", "to": "beta", "text": "hi"}
    plain = json.loads(claude_frames(binding, message)[-1])["message"]["content"]
    assert "broadcast=" not in plain
    cast = str(uuid.uuid4())
    marked = json.loads(claude_frames(binding, {**message, "broadcast": cast})[-1])["message"]["content"]
    assert marked.startswith(f'<cross-session-message from="h" from-name="alpha" from-mode="agentmailbus" broadcast="{cast}">')
    odd = json.loads(claude_frames(binding, {**message, "broadcast": 'x" evil="1'})[-1])["message"]["content"]
    assert 'evil=' not in odd.split(">", 1)[0]

    wake = tmp_path / "inbox" / "beta.jsonl"
    assert deliver_wake({"wake": str(wake)}, message)
    assert deliver_wake({"wake": str(wake)}, {**message, "id": str(uuid.uuid4()), "broadcast": cast})
    lines = [json.loads(line) for line in wake.read_text().splitlines()]
    assert "broadcast" not in lines[0]
    assert lines[1]["broadcast"] == cast


def test_mcp_broadcast_tool() -> None:
    calls = []

    def rpc(payload):
        calls.append(payload)
        return {"ok": True, "id": payload["id"], "waiting": False, "recipients": ["beta", "gamma"]}

    tools = handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, "alpha", rpc)
    assert [t["name"] for t in tools["result"]["tools"]] == ["send", "broadcast"]
    out = handle_request(
        {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "broadcast", "arguments": {"text": "hi"}}},
        "alpha",
        rpc,
    )
    assert calls[0]["to"] == "*" and calls[0]["from"] == "alpha"
    assert out["result"]["content"][0]["text"] == f"{calls[0]['id']} broadcast to 2: beta, gamma"
    # `send` will not broadcast; that has to be asked for by name.
    star = handle_request(
        {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "send", "arguments": {"to": "*", "text": "hi"}}},
        "alpha",
        rpc,
    )
    assert star["result"]["isError"] is True and star["result"]["content"][0]["text"].startswith("bad-name")
    assert len(calls) == 1


def test_old_database_gains_the_broadcast_column(tmp_path: Path) -> None:
    path = tmp_path / "old.db"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE agents (name TEXT PRIMARY KEY, host TEXT NOT NULL, harness TEXT NOT NULL,
                             online INTEGER NOT NULL, updated_at TEXT NOT NULL);
        CREATE TABLE messages (id TEXT PRIMARY KEY, sender TEXT NOT NULL, recipient TEXT NOT NULL,
                               from_host TEXT NOT NULL, text TEXT NOT NULL, priority TEXT NOT NULL,
                               created_at TEXT NOT NULL, state TEXT NOT NULL);
        INSERT INTO messages VALUES ('m1', 'a', 'b', 'h', 'old', 'next', '2026-10-01T00:00:00+00:00', 'queued');
        """
    )
    conn.commit()
    conn.close()
    store = BrokerStore(path)
    row = store.get_message("m1")
    assert row["broadcast"] == ""


def test_broadcast_ids_are_reserved_both_ways(tmp_path: Path) -> None:
    asyncio.run(_reserved(tmp_path))


async def _reserved(tmp_path: Path) -> None:
    store = BrokerStore(tmp_path / "broker.db")
    broker = Broker(store, "test-token", parse_allow(["127.0.0.0/8"]), "127.0.0.1", 0)
    await broker.start()
    rec_a, rec_b = Recording(), Recording()
    try:
        a = await _relay(tmp_path, "host-a", "alpha", broker.port, rec_a)
        # Alone on the bus: accepted, reaches nobody, and still reserves the id.
        empty = await a[0].local_send("alpha", "*", "anyone?")
        assert empty["recipients"] == []
        b = await _relay(tmp_path, "host-b", "beta", broker.port, rec_b)
        stolen = await b[0].local_send("beta", "*", "mine", mid=empty["id"])
        assert stolen == {"ok": False, "id": empty["id"], "error": "id-conflict"}
        # A direct send cannot take a broadcast id, from the sender or anyone else.
        for relay, sender, to in ((a[0], "alpha", "beta"), (b[0], "beta", "alpha")):
            clash = await relay.local_send(sender, to, "direct", mid=empty["id"])
            assert clash == {"ok": False, "id": empty["id"], "error": "id-conflict"}
        # The sender's own retry still works, and now reaches beta with the first text.
        retry = await a[0].local_send("alpha", "*", "other", mid=empty["id"])
        assert retry["recipients"] == ["beta"]
        await _until(lambda: rec_b.messages)
        assert rec_b.messages[0]["text"] == "anyone?"
        await _stop(*a)
        await _stop(*b)
    finally:
        await broker.stop()


def test_retry_reports_every_pending_copy(tmp_path: Path) -> None:
    asyncio.run(_pending(tmp_path))


async def _pending(tmp_path: Path) -> None:
    store = BrokerStore(tmp_path / "broker.db")
    broker = Broker(store, "test-token", parse_allow(["127.0.0.0/8"]), "127.0.0.1", 0)
    await broker.start()
    rec_a, rec_b, rec_c = Recording(), Recording(), Recording()
    try:
        a = await _relay(tmp_path, "host-a", "alpha", broker.port, rec_a)
        b = await _relay(tmp_path, "host-b", "beta", broker.port, rec_b)
        c = await _relay(tmp_path, "host-c", "gamma", broker.port, rec_c)
        await _stop(*c)
        await _until(lambda: not any(r["name"] == "gamma" and r["online"] for r in store.directory()))
        # gamma's copy exists but is queued: put one in place as if gamma had just left.
        first = await a[0].local_send("alpha", "*", "hi")
        assert first["recipients"] == ["beta"]
        salt = store.get_broadcast(first["id"])["salt"]
        store.insert_message(copy_id(salt, "gamma"), "alpha", "gamma", "hi", "next", broadcast=first["id"])
        again = await a[0].local_send("alpha", "*", "hi", mid=first["id"])
        assert again["recipients"] == ["beta", "gamma"]
        assert again["waiting"] is True
        await _stop(*a)
        await _stop(*b)
    finally:
        await broker.stop()


def test_too_many_recipients_is_refused_before_any_copy(tmp_path: Path, monkeypatch) -> None:
    import agentmailbus.broker as broker_mod

    monkeypatch.setattr(broker_mod, "MAX_RECIPIENTS", 1)
    asyncio.run(_cap(tmp_path))


async def _cap(tmp_path: Path) -> None:
    store = BrokerStore(tmp_path / "broker.db")
    broker = Broker(store, "test-token", parse_allow(["127.0.0.0/8"]), "127.0.0.1", 0)
    await broker.start()
    recs = [Recording() for _ in range(3)]
    try:
        relays = [
            await _relay(tmp_path, f"host-{n}", n, broker.port, rec)
            for n, rec in zip(("alpha", "beta", "gamma"), recs)
        ]
        out = await relays[0][0].local_send("alpha", "*", "hi")
        assert out == {"ok": False, "id": out["id"], "error": "too-many-recipients"}
        assert store.get_broadcast(out["id"]) is None
        assert store.conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 0
        # Once fewer are online, the same id goes out.
        await _stop(*relays[2])
        await _until(lambda: not any(r["name"] == "gamma" and r["online"] for r in store.directory()))
        sent = await relays[0][0].local_send("alpha", "*", "hi", mid=out["id"])
        assert sent["recipients"] == ["beta"]
        # A retry of a broadcast already out counts only names without a copy.
        relays[2] = await _relay(tmp_path, "host-gamma", "gamma", broker.port, recs[2])
        again = await relays[0][0].local_send("alpha", "*", "hi", mid=out["id"])
        # The answer lists at most the cap and counts the rest.
        assert again["recipients"] == ["beta"] and again["more"] == 1
        assert [c["recipient"] for c in store.broadcast_copies(out["id"])] == ["beta", "gamma"]
        for r in relays:
            await _stop(*r)
    finally:
        await broker.stop()


def test_records_are_pruned_but_copy_ids_survive(tmp_path: Path) -> None:
    store = BrokerStore(tmp_path / "broker.db")
    old = store.insert_broadcast(str(uuid.uuid4()), "alpha", "hi", "next")
    recent = store.insert_broadcast(str(uuid.uuid4()), "alpha", "hi", "next")
    kept = store.insert_broadcast(str(uuid.uuid4()), "alpha", "hi", "next")
    store.conn.execute("UPDATE broadcasts SET created_at='2000-01-01T00:00:00+00:00' WHERE id IN (?, ?)", (old["id"], kept["id"]))
    store.conn.commit()
    store.insert_message(str(uuid.uuid4()), "alpha", "beta", "hi", "next", broadcast=kept["id"])
    assert store.prune(days=7) == 1
    assert store.get_broadcast(old["id"]) is None
    assert store.get_broadcast(recent["id"]) is not None
    assert store.get_broadcast(kept["id"]) is not None
    # A retry after the record went makes the same salt, so the same copy ids.
    again = store.insert_broadcast(old["id"], "alpha", "hi", "next")
    assert again["salt"] == old["salt"]
    # The secret is kept with the database, and differs between databases.
    assert BrokerStore(tmp_path / "broker.db").broadcast_salt(old["id"], "alpha") == old["salt"]
    assert BrokerStore(tmp_path / "other.db").broadcast_salt(old["id"], "alpha") != old["salt"]
    assert store.broadcast_salt(old["id"], "alpha") == old["salt"]
    # Another sender reusing the id gets other copy ids, so a relay does not swallow them.
    assert store.broadcast_salt(old["id"], "gamma") != old["salt"]


def test_dsh_frame_marks_a_broadcast(tmp_path: Path) -> None:
    import socket as so

    from agentmailbus.adapters import deliver_dsh

    path = tmp_path / "in.sock"
    server = so.socket(so.AF_UNIX, so.SOCK_STREAM)
    server.bind(str(path))
    server.listen(2)
    cast = str(uuid.uuid4())
    got = []
    for extra in ({}, {"broadcast": cast}):
        message = {"id": str(uuid.uuid4()), "from": "alpha", "from_host": "h", "to": "d", "text": "hi", **extra}
        assert deliver_dsh({"socket": str(path)}, message)
        conn, _ = server.accept()
        got.append(json.loads(conn.makefile().readline()))
        conn.close()
    server.close()
    assert "broadcast" not in got[0]
    assert got[1]["broadcast"] == cast


def test_cli_send_refuses_a_star(tmp_path: Path) -> None:
    import subprocess
    import sys

    out = subprocess.run(
        [sys.executable, "-m", "agentmailbus", "send", "--from", "a", "--to", "*", "--text", "x",
         "--relay-socket", str(tmp_path / "none.sock")],
        capture_output=True, timeout=30, check=False,
    )
    assert out.returncode != 0 and b"bad-name (message id" in out.stderr and b"this attempt sent nothing): send takes one name" in out.stderr


def test_a_prune_during_a_retry_does_not_raise_the_copy_count(tmp_path: Path) -> None:
    asyncio.run(_prune_mid_retry(tmp_path))


async def _prune_mid_retry(tmp_path: Path) -> None:
    store = BrokerStore(tmp_path / "broker.db")
    broker = Broker(store, "test-token", parse_allow(["127.0.0.0/8"]), "127.0.0.1", 0)
    await broker.start()
    recs = {n: Recording() for n in ("alpha", "zeta", "gamma")}
    try:
        a = await _relay(tmp_path, "host-a", "alpha", broker.port, recs["alpha"])
        z = await _relay(tmp_path, "host-z", "zeta", broker.port, recs["zeta"])
        first = await a[0].local_send("alpha", "*", "hi")
        assert first["recipients"] == ["zeta"]
        await _until(lambda: recs["zeta"].messages)
        await _until(lambda: store.broadcast_copies(first["id"])[0]["state"] == "acked")
        store.conn.execute("UPDATE messages SET finished_at='2000-01-01T00:00:00+00:00'")
        store.conn.commit()
        g = await _relay(tmp_path, "host-g", "gamma", broker.port, recs["gamma"])
        real_flush = broker._flush

        async def flush_then_prune(ws, name):
            sent = await real_flush(ws, name)
            store.prune(days=7)  # the scheduled prune lands between two pushes
            return sent

        broker._flush = flush_then_prune
        again = await a[0].local_send("alpha", "*", "hi", mid=first["id"])
        broker._flush = real_flush
        # gamma (sorted first) got the one new copy; zeta's pruned copy was not remade.
        assert [c["recipient"] for c in store.broadcast_copies(first["id"])] == ["gamma"]
        assert again["recipients"] == ["gamma"]
        for r in (a, z, g):
            await _stop(*r)
    finally:
        await broker.stop()


def test_mcp_send_to_star_points_at_the_broadcast_tool() -> None:
    out = handle_request(
        {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "send", "arguments": {"to": "*", "text": "hi"}}},
        "alpha",
        lambda payload: {"ok": True},
    )
    assert out["result"]["isError"] is True
    assert "use the broadcast tool" in out["result"]["content"][0]["text"]


def test_cli_failure_names_the_id_to_retry_with(monkeypatch) -> None:
    from agentmailbus import cli

    mid = str(uuid.uuid4())
    monkeypatch.setattr(cli, "local_rpc", lambda sock, payload: {"ok": False, "id": mid, "error": "too-many-recipients"})
    try:
        cli.main(["broadcast", "--from", "a", "--text", "x"])
    except SystemExit as exc:
        assert str(exc) == f"too-many-recipients (message id {mid}; this send made no new copies, retry with this id once fewer agents are online)"
    else:
        raise AssertionError("no failure")


def test_cli_makes_the_id_so_a_relay_failure_still_names_it(monkeypatch) -> None:
    from agentmailbus import cli

    seen = {}

    def fake(sock, payload):
        seen.update(payload)
        return {"ok": False, "error": "timeout"}

    monkeypatch.setattr(cli, "local_rpc", fake)
    try:
        cli.main(["broadcast", "--from", "a", "--text", "x"])
    except SystemExit as exc:
        assert str(exc) == f"timeout (message id {seen['id']}; outcome unknown, retry with this id)"
        assert str(uuid.UUID(seen["id"])) == seen["id"]
    else:
        raise AssertionError("no failure")


def test_mcp_send_to_star_names_the_id() -> None:
    mid = str(uuid.uuid4())
    out = handle_request(
        {"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {"name": "send", "arguments": {"to": "*", "text": "hi", "id": mid}}},
        "alpha",
        lambda payload: {"ok": True},
    )
    assert mid in out["result"]["content"][0]["text"]


def test_cli_names_the_id_when_the_relay_socket_fails(monkeypatch) -> None:
    from agentmailbus import cli

    seen = {}

    def broken(sock, payload):
        seen.update(payload)
        raise TimeoutError("timed out")

    monkeypatch.setattr(cli, "local_rpc", broken)
    try:
        cli.main(["broadcast", "--from", "a", "--text", "x"])
    except SystemExit as exc:
        assert str(exc).startswith(f"relay-down: TimeoutError (message id {seen['id']}; outcome unknown, retry with --id {seen['id']})")
    else:
        raise AssertionError("no failure")


def test_a_copy_id_that_names_a_broadcast_is_skipped(tmp_path: Path) -> None:
    asyncio.run(_copy_names_broadcast(tmp_path))


async def _copy_names_broadcast(tmp_path: Path) -> None:
    store = BrokerStore(tmp_path / "broker.db")
    broker = Broker(store, "test-token", parse_allow(["127.0.0.0/8"]), "127.0.0.1", 0)
    await broker.start()
    rec_a, rec_b = Recording(), Recording()
    try:
        a = await _relay(tmp_path, "host-a", "alpha", broker.port, rec_a)
        b = await _relay(tmp_path, "host-b", "beta", broker.port, rec_b)
        mid = str(uuid.uuid4())
        cid = copy_id(store.broadcast_salt(mid, "alpha"), "beta")
        # beta already holds a broadcast under the id alpha's copy for beta would get.
        mine = await b[0].local_send("beta", "*", "beta's own", mid=cid)
        assert mine["recipients"] == ["alpha"]
        out = await a[0].local_send("alpha", "*", "hi", mid=mid)
        assert out["recipients"] == []
        again = await b[0].local_send("beta", "*", "beta's own", mid=cid)
        assert again["ok"] is True
        await _stop(*a)
        await _stop(*b)
    finally:
        await broker.stop()


def test_cli_names_the_id_for_text_that_cannot_be_encoded(monkeypatch) -> None:
    from agentmailbus import cli
    from agentmailbus.localcall import local_rpc

    mid = str(uuid.uuid4())
    monkeypatch.setattr(cli, "local_rpc", local_rpc)
    try:
        cli.main(["broadcast", "--from", "a", "--text", "\udcff", "--id", mid, "--relay-socket", "/nonexistent.sock"])
    except SystemExit as exc:
        assert str(exc) == f"bad-text (message id {mid}; this attempt sent nothing): --text holds bytes that are not valid text"
    else:
        raise AssertionError("no failure")


def test_mcp_broadcast_refuses_a_recipient() -> None:
    calls = []
    out = handle_request(
        {"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": {"name": "broadcast", "arguments": {"to": "beta", "text": "hi"}}},
        "alpha",
        lambda payload: calls.append(payload) or {"ok": True},
    )
    assert out["result"]["isError"] is True
    assert "broadcast takes no `to`" in out["result"]["content"][0]["text"]
    assert calls == []


def test_cli_names_the_id_for_any_undecodable_argument(monkeypatch) -> None:
    from agentmailbus import cli

    monkeypatch.setattr(cli, "local_rpc", lambda *a: (_ for _ in ()).throw(AssertionError("sent")))
    mid = str(uuid.uuid4())
    for argv, code in (
        (["broadcast", "--from", "a\udcff", "--text", "x"], "bad-from"),
        (["send", "--from", "a", "--to", "b\udcff", "--text", "x"], "bad-to"),
        (["broadcast", "--from", "a", "--text", "x", "--priority", "n\udcff"], "bad-priority"),
    ):
        try:
            cli.main(argv + ["--id", mid])
        except SystemExit as exc:
            assert str(exc).startswith(f"{code} (message id {mid}; this attempt sent nothing): --")
        else:
            raise AssertionError("no failure")
