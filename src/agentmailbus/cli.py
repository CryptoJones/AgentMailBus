"""Command line for the broker, the relay, and one session."""

from __future__ import annotations

import argparse
import json
import asyncio
import logging
import os
import signal
import sys
import tomllib
import uuid
from pathlib import Path

from agentmailbus import __version__, autoname
from agentmailbus.broker import Broker
from agentmailbus.localcall import local_rpc
from agentmailbus.mcp import named_error, serve_stdio
from agentmailbus.net import DEFAULT_ALLOW, load_secret, parse_allow
from agentmailbus.omi import OmiMirror
from agentmailbus.paths import data_dir
from agentmailbus.adapters import wake_problem
from agentmailbus.protocol import BROADCAST, ProtocolError, check_harness, check_name
from agentmailbus.registry import load_held, load_live, remove_binding, save_binding, start_time
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

    mcp = sub.add_parser("mcp", help="stdio MCP server with the send and broadcast tools")
    mcp.add_argument(
        "--from",
        dest="sender",
        help="the session's name; for claude and opencode it may be left out, see README",
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

    cast = sub.add_parser("broadcast", help="send one message to every agent online now")
    cast.add_argument("--from", dest="sender", required=True)
    cast.add_argument("--text", required=True)
    cast.add_argument("--priority", default="next")
    cast.add_argument("--id", default="", help="reuse a UUID so a retry reaches nobody twice")
    cast.add_argument("--relay-socket", type=Path)
    cast.set_defaults(func=_send, to=BROADCAST)

    bind = sub.add_parser("bind", help="publish a live session name")
    bind.add_argument("name")
    bind.add_argument("--harness", required=True)
    bind.add_argument(
        "--pid", type=int, help="defaults to the calling process for grok, orca, claude; required for opencode"
    )
    bind.add_argument("--wake", default="")
    bind.add_argument("--socket", default="")
    bind.add_argument("--registry", type=Path)
    bind.set_defaults(func=_bind)

    unbind = sub.add_parser("unbind", help="remove a session name")
    unbind.add_argument("name")
    unbind.add_argument("--registry", type=Path)
    unbind.set_defaults(func=_unbind)

    status = sub.add_parser("status", help="show where one message stands, per recipient")
    status.add_argument("--id", required=True, help="the message id a send or broadcast printed")
    status.add_argument("--json", action="store_true", help="print the receipt as JSON")
    status.add_argument("--relay-socket", type=Path)
    status.set_defaults(func=_status)

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
    if harness in {"claude", "opencode"}:
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


def _no_socket_for_opencode(args) -> None:
    """The plugin listens only where it computes; a socket given here would never be live."""
    if args.harness == "opencode" and args.socket:
        raise SystemExit("--socket is not used with --harness opencode: the plugin's socket is derived")


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
    _no_socket_for_opencode(args)
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
            if harness == "opencode":
                try:
                    _opencode_defaults(binding, pid)
                except (ProtocolError, OSError) as exc:
                    log.warning("not joining the bus: %s", getattr(exc, "detail", "") or exc)
                    binding, sender = None, None
            if binding is not None:
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
                            pid, session, os.environ.get(autoname.ENV, ""), load_held(registry), args.relay_config
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
        elif harness == "opencode":
            sender, binding, path = _opencode_auto(args, pid, registry, log)
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


def _opencode_auto(args, pid: int, registry: Path, log) -> tuple[str | None, dict | None, Path | None]:
    """Name opencode process `pid` and bind it to its plugin's socket. The binding is
    live once the plugin listens there; the server does not wait for it."""
    try:
        with autoname.registry_lock(registry) as lock:
            if lock.locked:
                autoname.sweep_dead(registry)
                autoname.sweep_opencode_sockets()
            sender = autoname.opencode_name(
                pid,
                os.environ.get(autoname.ENV, ""),
                load_held(registry),
                args.relay_config,
                os.getcwd(),
            )
            binding = {
                "name": sender,
                "harness": "opencode",
                "pid": pid,
                "wake": "",
                "socket": "",
                "server": os.getpid(),
            }
            _opencode_defaults(binding, pid)
            path = registry / f"{sender}.json"
            save_binding(registry, binding)
    except TimeoutError:
        log.warning("not joining the bus: the session registry stayed locked")
        return None, None, None
    except ProtocolError as exc:
        log.warning("not joining the bus: %s", exc.detail or exc.code)
        return None, None, None
    except OSError as exc:  # the working folder is gone, or similar
        log.warning("not joining the bus: %s", exc)
        return None, None, None
    if not os.path.exists(binding["socket"]):
        log.info("waiting for the opencode plugin to listen on %s", binding["socket"])
    return sender, binding, path


def _opencode_defaults(binding: dict, pid: int) -> None:
    """The plugin's socket for this process and folder (opencode starts the server in
    the project's folder), and the start time, so a recycled pid is not this session."""
    if not binding.get("socket"):
        binding["socket"] = autoname.opencode_socket(pid, os.getcwd())
    if len(os.fsencode(binding["socket"])) > autoname.MAX_SOCKET_PATH:
        # The plugin will not listen there either; set a shorter XDG_DATA_HOME.
        raise ProtocolError("bad-socket", f"the socket path {binding['socket']} is too long for a Unix socket")
    # Clock ticks on Linux; elsewhere the `ps` start text, checked when names are chosen.
    ticks = start_time(pid)
    binding["proc_start"] = ticks if ticks is not None else (autoname.process_started(pid) or None)


def _send(args) -> None:
    # The id is made first, so every failure below names the id to retry with.
    mid = args.id or str(uuid.uuid4())
    if args.cmd == "send" and args.to == BROADCAST:
        raise SystemExit(
            f"bad-name (message id {mid}; this attempt sent nothing): send takes one name; use `agentmailbus broadcast`"
        )
    payload = {
        "op": "send",
        "from": args.sender,
        "to": args.to,
        "text": args.text,
        "priority": args.priority,
        "id": mid,
    }
    for field in ("from", "to", "text", "priority", "id"):
        try:
            payload[field].encode("utf-8")
        except UnicodeEncodeError:
            # Bytes the shell could not decode; nothing is sent.
            if field == "id":
                raise SystemExit(
                    "bad-id (this attempt sent no message): --id holds bytes that are not valid text; "
                    "retry with a UUID as the id or with no id"
                ) from None
            code = "bad-text" if field == "text" else f"bad-{field}"
            raise SystemExit(
                f"{code} (message id {mid}; this attempt sent nothing): --{field} "
                "holds bytes that are not valid text"
            ) from None
    sock = _relay_socket(args.relay_socket)
    try:
        result = local_rpc(sock, payload)
    except (OSError, ProtocolError) as exc:
        why = exc.code if isinstance(exc, ProtocolError) else type(exc).__name__
        # These three fail at connect, before anything is written.
        if isinstance(exc, (FileNotFoundError, ConnectionRefusedError)):
            sent, hint = "this attempt sent no message", f"no relay is listening at {sock} (is the relay running, and is that the right socket?)"
        elif isinstance(exc, PermissionError):
            sent, hint = "this attempt sent no message", f"this user may not open {sock}; run as the relay's user"
        else:
            # The send may have reached the broker; the same id is the safe retry.
            sent, hint = "outcome unknown", "this host's relay did not answer cleanly"
        raise SystemExit(f"relay-down: {why} (message id {mid}; {sent}, retry with --id {mid}): {hint}") from None
    if not result.get("ok"):
        # The id is the one to retry with, so it is part of the message.
        code = str(result.get("error") or "send-failed")
        reason = str(result.get("reason") or "")
        raise SystemExit(named_error(code, str(result.get("id") or mid), _cli_hints(args.sender), reason))
    if "recipients" in result:
        names = result.get("recipients") or []
        more = result.get("more") or 0
        tail = f" and {more} more" if more else ""
        who = " ".join(names) + tail if names else "reached nobody"
        print(f"{result.get('id', '')} broadcast to {len(names) + more}: {who}")
        return
    state = "queued" if result.get("waiting") else "accepted"
    print(f"{result.get('id', '')} {state}")


def _cli_hints(sender: str) -> dict[str, str]:
    """What to do about a refused send, said for someone at a shell."""
    return {
        "sender-offline": (
            f"'{sender}' is not a live name on this host (never bound, or its process or socket is gone, "
            f"or its wake path is unusable); bind it with `agentmailbus bind --harness HARNESS {sender} ...` "
            "(grok and orca also need --wake, dsh --socket, opencode --pid; see `agentmailbus bind --help`), "
            "then retry with --id; a retry with that id is the same message, not a copy"
        ),
        "not-registered": "the broker has not accepted this name; retry with --id in a moment",
        "broker-down": "this host's relay has no broker connection; retry with --id once `agentmailbus who` succeeds",
        "unknown-agent": "no agent by that name has ever registered; `agentmailbus who` lists the names the broker knows",
    }


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
    _no_socket_for_opencode(args)
    if harness == "opencode" and (args.pid is None or args.pid <= 0):
        # The caller is a shell, not opencode: its pid is not the session's.
        raise SystemExit("bind --harness opencode needs --pid <opencode pid>")
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
    if harness == "opencode":
        _opencode_defaults(binding, binding["pid"])
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


STATUS_HINTS = {
    "unknown-message": "the broker has no record of this id that this host may see: it was never accepted, it "
    "finished more than retention_days ago and was pruned, or this host neither sent it nor holds a recipient",
    "bad-type": "the broker is older than this relay and cannot give receipts; update it",
    "bad-id": "the id must be a UUID, as printed by send or broadcast",
    "broker-down": "this host's relay has no broker connection; try again once `agentmailbus who` succeeds",
    "timeout": "the broker did not answer in time; try again",
}


def _receipt_line(r: dict) -> str:
    """One recipient's line. Each state says only what the broker knows."""
    state = str(r.get("state"))
    name = r.get("name")
    attempts = r.get("attempts") or 0
    handed = r.get("delivered_at") or ""
    if state == "acked":
        what = "written by the recipient's relay (not necessarily read)"
        if r.get("finished_at"):
            what += f"; ack recorded at {r['finished_at']}"
    elif state == "delivered":
        what = "handed to the recipient's relay, no ack yet"
        if handed:
            what += f" (handed out at {handed})"
    elif state == "queued" and handed:
        what = (
            f"waiting to be sent again (last hand-out at {handed}; the recipient's relay may already have written it"
            + (f"; {attempts} attempts so far)" if attempts else ")")
        )
    elif state == "queued":
        what = "waiting"
        if attempts:
            what += f" ({attempts} attempts so far)"
    elif state == "failed":
        what = f"given up at {r.get('finished_at')} after {attempts} attempts (a late ack can still mark it written)"
    else:
        what = state
    return f"{name}\t{state}\t{what}"


def _status(args) -> None:
    sock = _relay_socket(args.relay_socket)
    try:
        result = local_rpc(sock, {"op": "status", "id": args.id})
    except (OSError, ProtocolError) as exc:
        why = exc.code if isinstance(exc, ProtocolError) else type(exc).__name__
        raise SystemExit(f"relay-down: {why}: could not ask the relay at {sock}") from None
    if not result.get("ok"):
        code = str(result.get("error") or "status-failed")
        hint = STATUS_HINTS.get(code)
        raise SystemExit(f"{code} (message id {args.id[:64]})" + (f": {hint}" if hint else ""))
    if args.json:
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return
    head = f"{result.get('id')} {result.get('kind')} from {result.get('from')} at {result.get('created_at')}"
    if result.get("kind") == "copy":
        head += f" (a copy of broadcast {result.get('broadcast')})"
    print(head)
    recipients = result.get("recipients") or []
    for r in recipients:
        print(_receipt_line(r))
    if result.get("more"):
        print(f"... and {result['more']} more")
    if not recipients and not result.get("more"):
        print("no copies: nobody else was online when it was sent")


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
