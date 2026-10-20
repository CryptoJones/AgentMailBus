"""Command line for the broker, the relay, and one session."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import socket
import sys
import tomllib
from pathlib import Path

from agentmailbus import __version__
from agentmailbus.broker import Broker
from agentmailbus.localcall import local_rpc
from agentmailbus.mcp import serve_stdio
from agentmailbus.net import DEFAULT_ALLOW, load_secret, parse_allow
from agentmailbus.omi import OmiMirror
from agentmailbus.paths import data_dir
from agentmailbus.protocol import ProtocolError, check_harness, check_name
from agentmailbus.registry import remove_binding, save_binding
from agentmailbus.relay import Relay
from agentmailbus.store import BrokerStore, SeenStore


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="agentmailbus")
    parser.add_argument("--version", action="version", version=f"agentmailbus {__version__}")
    sub = parser.add_subparsers(dest="cmd", required=True)

    broker = sub.add_parser("broker", help="run the queue")
    broker.add_argument("--config", required=True, type=Path)
    broker.set_defaults(func=_broker)

    relay = sub.add_parser("relay", help="run this host's relay")
    relay.add_argument("--config", required=True, type=Path)
    relay.set_defaults(func=_relay)

    mcp = sub.add_parser("mcp", help="stdio MCP server with the send tool")
    mcp.add_argument("--from", dest="sender", required=True)
    mcp.add_argument("--harness", required=True)
    mcp.add_argument("--pid", type=int)
    mcp.add_argument("--wake", default="")
    mcp.add_argument("--socket", default="")
    mcp.add_argument("--registry", type=Path)
    mcp.add_argument("--relay-socket", type=Path)
    mcp.set_defaults(func=_mcp)

    send = sub.add_parser("send", help="send one message through the local relay")
    send.add_argument("--from", dest="sender", required=True)
    send.add_argument("--to", required=True)
    send.add_argument("--text", required=True)
    send.add_argument("--priority", default="next")
    send.add_argument("--id", default="", help="reuse a UUID so a retry is the same message")
    send.add_argument("--relay-socket", type=Path)
    send.set_defaults(func=_send)

    bind = sub.add_parser("bind", help="publish a live session name")
    bind.add_argument("name")
    bind.add_argument("--harness", required=True)
    bind.add_argument("--pid", type=int, help="defaults to the calling process for grok, orca, claude")
    bind.add_argument("--wake", default="")
    bind.add_argument("--socket", default="")
    bind.add_argument("--registry", type=Path)
    bind.set_defaults(func=_bind)

    unbind = sub.add_parser("unbind", help="remove a session name")
    unbind.add_argument("name")
    unbind.add_argument("--registry", type=Path)
    unbind.set_defaults(func=_unbind)

    who = sub.add_parser("who", help="list agents the broker knows")
    who.add_argument("--relay-socket", type=Path)
    who.set_defaults(func=_who)

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(name)s: %(message)s")
    try:
        args.func(args)
    except ProtocolError as exc:
        raise SystemExit(f"{exc.code}: {exc.detail}" if exc.detail else exc.code) from None


def _broker(args) -> None:
    cfg = _toml(args.config)
    token = load_secret(str(cfg["token"]))
    allow = parse_allow(list(cfg.get("allow") or DEFAULT_ALLOW))
    store = BrokerStore(Path(cfg.get("db") or data_dir() / "broker.db").expanduser())
    broker = Broker(
        store,
        token,
        allow,
        str(cfg.get("bind") or "0.0.0.0"),
        int(cfg.get("port") or 8770),
        retention_days=float(cfg.get("retention_days") or 7),
    )
    asyncio.run(_until_signal(broker.start(), broker.stop()))


def _relay(args) -> None:
    cfg = _toml(args.config)
    token = load_secret(str(cfg["token"]))
    host = str(cfg.get("host") or socket.gethostname().split(".")[0])
    check_name(host, "host")
    relay = Relay(
        url=str(cfg["broker"]),
        token=token,
        host_id=host,
        socket_path=Path(cfg.get("socket") or data_dir() / "relay.sock").expanduser(),
        registry=Path(cfg.get("registry") or data_dir() / "sessions").expanduser(),
        seen=SeenStore(Path(cfg.get("state") or data_dir() / "relay.db").expanduser()),
        omi=OmiMirror(enabled=bool(cfg.get("omi", False))),
    )
    asyncio.run(_run_relay(relay))


def _default_pid(harness: str, pid: int | None) -> int:
    if pid is not None:
        return pid
    if harness in {"grok", "orca"}:
        return os.getpid()
    if harness == "claude":
        return os.getppid()
    return 0


def _mcp(args) -> None:
    sender = check_name(args.sender, "from")
    harness = check_harness(args.harness)
    binding = {
        "name": sender,
        "harness": harness,
        "pid": _default_pid(harness, args.pid),
        "wake": args.wake,
        "socket": args.socket,
    }
    path = save_binding(_registry(args.registry), binding)
    sock = _relay_socket(args.relay_socket)

    def rpc(payload: dict) -> dict:
        return local_rpc(sock, payload)

    try:
        serve_stdio(sender, rpc)
    finally:
        # Another process may have bound the same name since. Leave its file alone.
        remove_binding(path, binding)


def _send(args) -> None:
    payload = {
        "op": "send",
        "from": args.sender,
        "to": args.to,
        "text": args.text,
        "priority": args.priority,
    }
    if args.id:
        payload["id"] = args.id
    result = local_rpc(_relay_socket(args.relay_socket), payload)
    if not result.get("ok"):
        raise SystemExit(str(result.get("error") or "send-failed"))
    state = "queued" if result.get("waiting") else "accepted"
    print(f"{result.get('id', '')} {state}")


def _bind(args) -> None:
    harness = check_harness(args.harness)
    pid = args.pid
    if pid is None and harness != "dsh":
        pid = os.getppid()
    binding = {
        "name": check_name(args.name),
        "harness": harness,
        "pid": pid or 0,
        "wake": args.wake,
        "socket": args.socket,
    }
    path = save_binding(_registry(args.registry), binding)
    print(path)


def _unbind(args) -> None:
    path = _registry(args.registry) / f"{check_name(args.name)}.json"
    path.unlink(missing_ok=True)


def _who(args) -> None:
    result = local_rpc(_relay_socket(args.relay_socket), {"op": "who"})
    if not result.get("ok"):
        raise SystemExit(str(result.get("error") or "who-failed"))
    for agent in result.get("agents") or []:
        flag = "online" if agent.get("online") else "offline"
        line = f"{agent['name']}\t{agent['host']}\t{agent['harness']}\t{flag}\t{agent.get('queued', 0)}"
        if agent.get("failed"):
            line += f"\tfailed={agent['failed']}"
        print(line)


def _toml(path: Path) -> dict:
    return tomllib.loads(path.expanduser().read_text())


def _registry(path: Path | None) -> Path:
    return (path or (data_dir() / "sessions")).expanduser()


def _relay_socket(path: Path | None) -> Path:
    env = os.environ.get("AGENTMAILBUS_SOCKET")
    if path is not None:
        return path.expanduser()
    if env:
        return Path(env).expanduser()
    return data_dir() / "relay.sock"


def _signal_event() -> asyncio.Event:
    done = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, done.set)
        except NotImplementedError:
            pass
    return done


async def _until_signal(started, stop) -> None:
    await started
    await _signal_event().wait()
    await stop()


async def _run_relay(relay: Relay) -> None:
    """Exit on a signal, or when the relay itself fails, so a dead relay is never left looking alive."""
    task = asyncio.create_task(relay.run())
    waiter = asyncio.create_task(_signal_event().wait())
    await asyncio.wait({task, waiter}, return_when=asyncio.FIRST_COMPLETED)
    if task.done():
        waiter.cancel()
        task.result()
        return
    await relay.close()
    await task


if __name__ == "__main__":
    main(sys.argv[1:])
