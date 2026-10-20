import json
import os
import socket
import threading
import time
from pathlib import Path

import pytest

from agentmailbus.adapters import claude_frames, deliver_claude, deliver_dsh, deliver_wake, peer_token
from agentmailbus.mcp import handle_request
from agentmailbus.net import ip_allowed, load_secret, parse_allow


def test_lan_allowlist() -> None:
    nets = parse_allow(
        ["127.0.0.0/8", "::1/128", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "100.64.0.0/10"]
    )
    assert ip_allowed("127.0.0.1", nets)
    assert ip_allowed("::1", nets)
    assert ip_allowed("172.16.28.254", nets)
    assert ip_allowed("100.64.1.2", nets)
    assert ip_allowed("::ffff:192.168.1.9", nets)
    assert not ip_allowed("8.8.8.8", nets)
    assert not ip_allowed("not-an-ip", nets)


def test_token_must_come_from_pass_or_file() -> None:
    with pytest.raises(SystemExit):
        load_secret("literal-token")


def test_wake_file_is_private(tmp_path: Path) -> None:
    binding = {"wake": str(tmp_path / "inbox" / "grok.jsonl")}
    message = {
        "id": "m1",
        "from": "alpha",
        "from_host": "host-a",
        "to": "grok-aima",
        "text": "hello",
        "created_at": "2026-10-20T00:00:00+00:00",
    }
    assert deliver_wake(binding, message) is True
    path = Path(binding["wake"])
    assert (path.stat().st_mode & 0o777) == 0o600
    line = json.loads(path.read_text().splitlines()[0])
    assert line["text"] == "hello"
    assert line["from"] == "alpha"


def test_claude_frame_carries_peer_token(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    sock = "/tmp/cc-socks/example.sock"
    digest_name = __import__("hashlib").sha256(sock.encode()).hexdigest()
    key = tmp_path / ".claude" / "sessions" / f"42.{digest_name}.key"
    key.parent.mkdir(parents=True)
    key.write_text(json.dumps({"peerToken": "abc123"}))
    assert peer_token(sock, 42) == "abc123"
    frames = claude_frames(
        {"pid": 42, "socket": sock, "relay_socket": "/tmp/relay.sock"},
        {"id": "m", "from": "rea-63", "from_host": "ronin28", "text": "ping", "priority": "next"},
    )
    auth, user = (json.loads(frame) for frame in frames)
    assert auth == {"type": "auth", "token": "abc123"}
    assert user["type"] == "user"
    assert user["priority"] == "next"
    assert "rea-63" in user["message"]["content"]
    assert user["from"] == "uds:/tmp/relay.sock"


def _serve(path: Path, got: list) -> threading.Event:
    ready = threading.Event()

    def run() -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(str(path))
        sock.listen(1)
        ready.set()
        conn, _ = sock.accept()
        data = b""
        while b"\n" not in data:
            chunk = conn.recv(4096)
            if not chunk:
                break
            data += chunk
        got.append(json.loads(data.split(b"\n", 1)[0]))
        conn.close()
        sock.close()

    threading.Thread(target=run).start()
    assert ready.wait(2)
    return ready


def test_claude_and_dsh_sockets(tmp_path: Path) -> None:
    got: list = []
    dsh = tmp_path / "dsh.sock"
    _serve(dsh, got)
    assert deliver_dsh(
        {"socket": str(dsh)},
        {"id": "d1", "from": "alpha", "from_host": "h", "text": "review this"},
    )
    for _ in range(20):
        if got:
            break
        time.sleep(0.05)
    assert got[0]["text"] == "review this"

    cc = tmp_path / "cc.sock"
    got.clear()
    _serve(cc, got)
    assert deliver_claude(
        {"pid": 7, "socket": str(cc)},
        {"id": "c1", "from": "rea-63", "from_host": "ronin28", "text": "status", "priority": "now"},
    )
    for _ in range(20):
        if got:
            break
        time.sleep(0.05)
    assert got[0]["type"] == "user"
    assert got[0]["priority"] == "now"


def test_mcp_send_cannot_spoof_sender() -> None:
    seen = {}

    def rpc(payload):
        seen.update(payload)
        return {"ok": True, "id": "abc", "waiting": True}

    reply = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": {
                "name": "send",
                "arguments": {"to": "rea-63", "text": "hi", "from": "someone-else"},
            },
        },
        "grok-aima",
        rpc,
    )
    assert seen["from"] == "grok-aima"
    assert seen["to"] == "rea-63"
    assert "queued" in reply["result"]["content"][0]["text"]
    assert "isError" not in reply["result"]
