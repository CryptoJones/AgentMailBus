"""Receipts: `agentmailbus status --id` says where a message stands, per recipient."""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime
from pathlib import Path

import pytest

from agentmailbus import cli
from agentmailbus.broker import Broker
from agentmailbus.net import parse_allow
from agentmailbus.store import BrokerStore

from test_broadcast import Recording, _relay, _stop, _until


def _status(sock: Path, mid: str, *extra: str) -> None:
    cli.main(["status", "--id", mid, "--relay-socket", str(sock), *extra])


def test_receipts_for_direct_and_broadcast_messages(tmp_path: Path, capsys) -> None:
    asyncio.run(_receipts(tmp_path, capsys))


async def _receipts(tmp_path: Path, capsys) -> None:
    store = BrokerStore(tmp_path / "broker.db")
    broker = Broker(store, "test-token", parse_allow(["127.0.0.0/8"]), "127.0.0.1", 0)
    await broker.start()
    rec_b = Recording()
    a = b = None
    try:
        a = await _relay(tmp_path, "host-a", "alpha", broker.port, Recording())
        b = await _relay(tmp_path, "host-b", "beta", broker.port, rec_b)
        sock = tmp_path / "host-a" / "relay.sock"

        sent = await a[0].local_send("alpha", "beta", "secret words")
        await _until(lambda: store.get_message(sent["id"])["state"] == "acked")
        capsys.readouterr()
        await asyncio.to_thread(_status, sock, sent["id"], "--json")
        out = json.loads(capsys.readouterr().out)
        assert out["kind"] == "direct" and out["from"] == "alpha"
        assert [(r["name"], r["state"]) for r in out["recipients"]] == [("beta", "acked")]
        assert out["recipients"][0]["finished_at"]
        datetime.fromisoformat(out["recipients"][0]["delivered_at"])  # a plain time, no claim counter
        assert "secret words" not in json.dumps(out)

        await asyncio.to_thread(_status, sock, sent["id"])
        text = capsys.readouterr().out
        assert text.startswith(f"{sent['id']} direct from alpha at ")
        assert "beta\tacked\twritten by the recipient's relay (not necessarily read); ack recorded at " in text

        cast = await a[0].local_send("alpha", "*", "all hands")
        await _until(lambda: all(c["state"] == "acked" for c in store.broadcast_copies(cast["id"])))
        await asyncio.to_thread(_status, sock, cast["id"], "--json")
        out = json.loads(capsys.readouterr().out)
        assert out["kind"] == "broadcast" and [r["name"] for r in out["recipients"]] == ["beta"]
        assert out["broadcast"] == cast["id"]
        copy_id = rec_b.messages[-1]["id"]
        await asyncio.to_thread(_status, sock, copy_id)
        text = capsys.readouterr().out
        assert text.startswith(f"{copy_id} copy from alpha at ") and f"(a copy of broadcast {cast['id']})" in text
        assert "beta\tacked" in text
    finally:
        for r in (b, a):
            if r:
                await _stop(*r)
        await broker.stop()


def test_a_message_waiting_for_an_offline_recipient_says_waiting(tmp_path: Path, capsys) -> None:
    asyncio.run(_waiting(tmp_path, capsys))


async def _waiting(tmp_path: Path, capsys) -> None:
    store = BrokerStore(tmp_path / "broker.db")
    broker = Broker(store, "test-token", parse_allow(["127.0.0.0/8"]), "127.0.0.1", 0)
    await broker.start()
    a = b = None
    try:
        a = await _relay(tmp_path, "host-a", "alpha", broker.port, Recording())
        b = await _relay(tmp_path, "host-b", "beta", broker.port, Recording())
        await _stop(*b)
        b = None
        await _until(lambda: not any(x["online"] for x in store.directory() if x["name"] == "beta"))
        sent = await a[0].local_send("alpha", "beta", "later")
        capsys.readouterr()
        await asyncio.to_thread(_status, tmp_path / "host-a" / "relay.sock", sent["id"])
        assert "beta\tqueued\twaiting" in capsys.readouterr().out
    finally:
        for r in (b, a):
            if r:
                await _stop(*r)
        await broker.stop()


def test_an_unknown_id_says_why(tmp_path: Path) -> None:
    asyncio.run(_unknown(tmp_path))


async def _unknown(tmp_path: Path) -> None:
    store = BrokerStore(tmp_path / "broker.db")
    broker = Broker(store, "test-token", parse_allow(["127.0.0.0/8"]), "127.0.0.1", 0)
    await broker.start()
    a = None
    try:
        a = await _relay(tmp_path, "host-a", "alpha", broker.port, Recording())
        mid = str(uuid.uuid4())
        with pytest.raises(SystemExit) as info:
            await asyncio.to_thread(_status, tmp_path / "host-a" / "relay.sock", mid)
        assert str(info.value).startswith(f"unknown-message (message id {mid}): the broker has no record")
        with pytest.raises(SystemExit) as info:
            await asyncio.to_thread(_status, tmp_path / "host-a" / "relay.sock", "nope")
        assert str(info.value).startswith("bad-id (message id nope): the id must be a UUID")
    finally:
        if a:
            await _stop(*a)
        await broker.stop()


def test_store_receipt_counts_copies_past_the_limit(tmp_path: Path) -> None:
    store = BrokerStore(tmp_path / "broker.db")
    bid = str(uuid.uuid4())
    store.insert_broadcast(bid, "alpha", "t", "next")
    for i in range(5):
        store.insert_message(str(uuid.uuid4()), "alpha", f"n{i}", "t", "next", broadcast=bid)
    got = store.receipt(bid, limit=3)
    assert [r["recipient"] for r in got["recipients"]] == ["n0", "n1", "n2"] and got["more"] == 2
    assert store.receipt(str(uuid.uuid4())) is None


def test_a_recipient_sees_only_its_own_copy_and_a_stranger_sees_nothing(tmp_path: Path, capsys) -> None:
    asyncio.run(_privacy(tmp_path, capsys))


async def _privacy(tmp_path: Path, capsys) -> None:
    store = BrokerStore(tmp_path / "broker.db")
    broker = Broker(store, "test-token", parse_allow(["127.0.0.0/8"]), "127.0.0.1", 0)
    await broker.start()
    a = b = c = None
    try:
        a = await _relay(tmp_path, "host-a", "alpha", broker.port, Recording())
        b = await _relay(tmp_path, "host-b", "beta", broker.port, Recording())
        c = await _relay(tmp_path, "host-c", "gamma", broker.port, Recording())
        cast = await a[0].local_send("alpha", "*", "all hands")
        direct = await a[0].local_send("alpha", "beta", "just beta")
        await _until(lambda: all(x["state"] == "acked" for x in store.broadcast_copies(cast["id"])))
        capsys.readouterr()
        # The sender's host sees both copies.
        await asyncio.to_thread(_status, tmp_path / "host-a" / "relay.sock", cast["id"], "--json")
        assert [r["name"] for r in json.loads(capsys.readouterr().out)["recipients"]] == ["beta", "gamma"]
        # A recipient's host sees only its own copy of the broadcast.
        await asyncio.to_thread(_status, tmp_path / "host-b" / "relay.sock", cast["id"], "--json")
        assert [r["name"] for r in json.loads(capsys.readouterr().out)["recipients"]] == ["beta"]
        # A host that neither sent nor received the direct message learns nothing.
        with pytest.raises(SystemExit) as info:
            await asyncio.to_thread(_status, tmp_path / "host-c" / "relay.sock", direct["id"])
        assert str(info.value).startswith("unknown-message ")
    finally:
        for r in (c, b, a):
            if r:
                await _stop(*r)
        await broker.stop()


def test_receipt_lines_say_only_what_the_broker_knows() -> None:
    line = cli._receipt_line
    assert line({"name": "b", "state": "queued", "attempts": 0, "delivered_at": "T1"}).startswith(
        "b\tqueued\twaiting to be sent again (last hand-out at T1; the recipient's relay may already have written it)"
    )
    assert line({"name": "b", "state": "queued", "attempts": 2, "delivered_at": ""}) == "b\tqueued\twaiting (2 attempts so far)"
    assert line({"name": "b", "state": "failed", "attempts": 20, "finished_at": "T2"}) == (
        "b\tfailed\tgiven up at T2 after 20 attempts (a late ack can still mark it written)"
    )


def test_store_strips_the_claim_counter_and_filters_rows(tmp_path: Path) -> None:
    store = BrokerStore(tmp_path / "broker.db")
    store.upsert_agent("b", "h", "grok", True)
    mid = str(uuid.uuid4())
    store.insert_message(mid, "a", "b", "t", "next")
    store.claim(mid)
    got = store.receipt(mid)
    assert "/" not in got["recipients"][0]["delivered_at"]
    assert store.receipt(mid, only={"x"})["recipients"] == []


def test_cli_status_edges(monkeypatch, capsys) -> None:
    mid = str(uuid.uuid4())
    base = {"ok": True, "id": mid, "kind": "broadcast", "broadcast": mid, "from": "a", "created_at": "T"}
    monkeypatch.setattr(cli, "local_rpc", lambda sock, p: {**base, "recipients": [], "more": 0})
    cli.main(["status", "--id", mid])
    assert "no copies: nobody else was online when it was sent" in capsys.readouterr().out
    monkeypatch.setattr(
        cli, "local_rpc", lambda sock, p: {**base, "recipients": [{"name": "b", "state": "acked"}], "more": 3}
    )
    cli.main(["status", "--id", mid])
    assert capsys.readouterr().out.rstrip().endswith("... and 3 more")
    for code, hint in (("bad-type", "older than this relay"), ("timeout", "did not answer in time")):
        monkeypatch.setattr(cli, "local_rpc", lambda sock, p, c=code: {"ok": False, "error": c})
        with pytest.raises(SystemExit) as info:
            cli.main(["status", "--id", mid])
        assert str(info.value).startswith(f"{code} (message id {mid}): ") and hint in str(info.value)


def test_a_broadcast_that_reached_nobody_stays_visible_to_its_sending_host(tmp_path: Path) -> None:
    store = BrokerStore(tmp_path / "broker.db")
    store.upsert_agent("alpha", "host-a", "grok", True)
    bid = str(uuid.uuid4())
    store.insert_broadcast(bid, "alpha", "t", "next")
    store.set_online("alpha", False)
    got = store.receipt(bid)
    assert got["from_host"] == "host-a" and got["recipients"] == []


@pytest.mark.skipif(
    __import__("sqlite3").sqlite_version_info < (3, 35, 0), reason="DROP COLUMN needs SQLite 3.35"
)
def test_older_broadcast_rows_take_their_first_copy_host(tmp_path: Path) -> None:
    import sqlite3

    path = tmp_path / "broker.db"
    store = BrokerStore(path)
    bid = str(uuid.uuid4())
    store.upsert_agent("alpha", "host-a", "grok", True)
    store.insert_broadcast(bid, "alpha", "t", "next")
    store.insert_message(str(uuid.uuid4()), "alpha", "b", "t", "next", broadcast=bid)
    store.conn.close()
    conn = sqlite3.connect(path)
    conn.execute("ALTER TABLE broadcasts DROP COLUMN from_host")
    conn.commit()
    conn.close()
    assert BrokerStore(path).receipt(bid)["from_host"] == "host-a"
