"""Command line for the broker, the relay, and one session."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
import tomllib
from pathlib import Path

from agentmailbus import __version__, autoname
from agentmailbus.broker import Broker
from agentmailbus.localcall import local_rpc
from agentmailbus.mcp import serve_stdio
from agentmailbus.net import DEFAULT_ALLOW, load_secret, parse_allow
from agentmailbus.omi import OmiMirror
from agentmailbus.paths import data_dir
from agentmailbus.adapters import wake_problem
from agentmailbus.protocol import ProtocolError, check_harness, check_name
from agentmailbus.registry import load_live, remove_binding, save_binding
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
    mcp.add_argument(
        "--from",
        dest="sender",
        help="the session's name; for claude it may be left out, see README",
    )
    mcp.add_argument("--harness", required=True)
    mcp.add_argument(
        "--relay-config",
        type=Path,
        default=Path("~/.config/agentmailbus/relay.toml"),
        help="read for the host part of an automatic name",
    )
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
    host = autoname.relay_host(cfg)
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


def _wake_arg(text: str, harness: str) -> str:
    """The wake path as the session means it: expanded and absolute in the caller's
    directory, since the relay runs somewhere else. Only Grok and Orca read it."""
    if harness not in {"grok", "orca"} or not text:
        return text
    problem = wake_problem(text) if text.startswith("~") else ""
    if problem:  # judged as typed, before `~` is expanded into an ordinary path
        raise ProtocolError("bad-wake", f"the wake path {problem}")
    try:
        path = Path(text).expanduser() if text.startswith("~") else Path(text)
        return str(path.absolute())
    except (RuntimeError, OSError) as exc:
        raise ProtocolError("bad-wake", f"the wake path cannot be made absolute: {exc}") from None


def _mcp(args) -> None:
    # SIGTERM and SIGHUP end the server through its cleanup, so the binding file goes.
    # The first one also ignores any that follow, so nothing cuts the cleanup short.
    def _stop(*_):
        for signum in (signal.SIGTERM, signal.SIGHUP):
            signal.signal(signum, signal.SIG_IGN)
        sys.exit(0)

    for signum in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, _stop)
    harness = check_harness(args.harness)
    pid = _default_pid(harness, args.pid)
    sock = _relay_socket(args.relay_socket)
    log = logging.getLogger("agentmailbus.mcp")

    def rpc(payload: dict) -> dict:
        return local_rpc(sock, payload)

    registry = _registry(args.registry)
    binding: dict | None = None
    path: Path | None = None
    sender: str | None = None
    try:
        if args.sender is not None:
            sender = check_name(args.sender, "from")
            binding = {
                "name": sender,
                "harness": harness,
                "pid": pid,
                "wake": _wake_arg(args.wake, harness),
                "socket": args.socket,
                "server": os.getpid(),
            }
            if harness == "claude":
                binding["sessions_dir"] = str(autoname.sessions_dir())
            path = registry / f"{sender}.json"
            _save_locked(registry, binding)
        elif harness == "claude":
            session = autoname.read_session(pid)
            if not autoname.interactive(session):
                log.info("not joining the bus: pid %d is a scripted session or has no session record", pid)
            else:
                try:
                    # Choosing a name and saving it is one step for every session on this host.
                    with autoname.registry_lock(registry) as lock:
                        if lock.locked:
                            autoname.sweep_dead(registry)
                        sender = autoname.pick(
                            pid, session, os.environ.get(autoname.ENV, ""), load_live(registry), args.relay_config
                        )
                        if sender is not None:
                            binding = {
                                "name": sender,
                                "harness": harness,
                                "pid": pid,
                                "wake": "",
                                "socket": args.socket or autoname.claude_socket_from(session),
                                "server": os.getpid(),
                                # So a recycled pid does not revive this binding, and the
                                # relay finds the peer key where this session keeps it.
                                "proc_start": session.get("procStart"),
                                "sessions_dir": str(autoname.sessions_dir()),
                            }
                            path = registry / f"{sender}.json"
                            save_binding(registry, binding)
                except TimeoutError:
                    log.warning("not joining the bus: the session registry stayed locked")
                    sender = None
        else:
            raise SystemExit("--from is required for this harness")
        if sender is not None:
            log.info("bound as %s", sender)
        serve_stdio(sender, rpc)
    finally:
        # A second signal must not cut the cleanup short.
        for signum in (signal.SIGTERM, signal.SIGHUP):
            signal.signal(signum, signal.SIG_IGN)
        # Only this server's own file. Another process may have bound the name since.
        if path is not None and binding is not None:
            _remove_locked(registry, path, binding)


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


def _save_locked(registry: Path, binding: dict) -> Path:
    """Save under the registry lock, so a startup sweep never removes a file just
    written. A lock that stays busy stops the save rather than racing the sweep."""
    try:
        with autoname.registry_lock(registry):
            return save_binding(registry, binding)
    except TimeoutError:
        raise SystemExit("the session registry stayed locked; nothing was bound") from None


def _remove_locked(registry: Path, path: Path, binding: dict) -> None:
    """Remove this server's own binding under the lock, so a replacement written in
    the meantime is never the file that goes. If the lock stays busy for a second the
    file is left: once the session is gone, the next automatic start sweeps it."""
    try:
        with autoname.registry_lock(registry, timeout=1.0):
            remove_binding(path, binding)
    except TimeoutError:
        pass
    except OSError:
        remove_binding(path, binding)  # no lock to be had here at all


def _bind(args) -> None:
    harness = check_harness(args.harness)
    pid = args.pid
    if pid is None and harness != "dsh":
        pid = os.getppid()
    binding = {
        "name": check_name(args.name),
        "harness": harness,
        "pid": pid or 0,
        "wake": _wake_arg(args.wake, harness),
        "socket": args.socket,
    }
    if harness == "claude":
        binding["sessions_dir"] = str(autoname.sessions_dir())
    path = _save_locked(_registry(args.registry), binding)
    print(path)


def _unbind(args) -> None:
    registry = _registry(args.registry)
    path = registry / f"{check_name(args.name)}.json"
    try:
        with autoname.registry_lock(registry):
            path.unlink(missing_ok=True)
    except TimeoutError:
        raise SystemExit("the session registry stayed locked; nothing was removed") from None


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
