"""Stdio MCP server. The tools are send and broadcast. Receiving is the relay's job.

MCP stdio framing is one JSON-RPC object per line, no headers. A line may also hold a
JSON array (a batch); the reply is then an array.
"""

from __future__ import annotations

import json
import logging
import sys
import uuid

from agentmailbus import __version__
from agentmailbus.protocol import (
    BROADCAST,
    ProtocolError,
    check_id,
    check_name,
    check_priority,
    check_text,
)

log = logging.getLogger("agentmailbus.mcp")

SUPPORTED_VERSIONS = ("2025-06-18", "2025-03-26", "2024-11-05")
# A 32,000-character text escaped as \uXXXX pairs is under 400 KB; this leaves room.
MAX_LINE = 1024 * 1024

SEND_TOOL = {
    "name": "send",
    "description": (
        "Send a message to a named agent. The relay delivers it. "
        "You do not receive messages with a tool. "
        "A failure names the message id it used and says whether to retry with that id "
        "(outcome unknown) or to send a new message (that one can never be delivered)."
    ),
    "inputSchema": {
        "type": "object",
        "properties": {
            "to": {"type": "string", "description": "Recipient name, such as rea-63."},
            "text": {"type": "string"},
            "priority": {
                "type": "string",
                "enum": ["next", "now", "later"],
                "description": "Claude delivery priority. Other harnesses ignore it.",
            },
            "id": {
                "type": "string",
                "description": "Optional UUID. Reuse the id from a failed call to retry it as the same message.",
            },
        },
        "required": ["to", "text"],
        "additionalProperties": False,
    },
}


BROADCAST_TOOL = {
    "name": "broadcast",
    "description": (
        "Send one message to every agent that is online right now, except you. "
        "Agents offline when you send it do not get it. Each recipient sees it "
        "marked as a broadcast. The result lists up to 1,000 agents a copy is going to "
        "and counts the rest. "
        "A failure names the message id it used and says whether to retry with that id "
        "or to send a new message; a retry with the same id is the same broadcast "
        "and is not delivered twice while the broker keeps it."
    ),
    "inputSchema": {
        "type": "object",
        "properties": {
            "text": {"type": "string"},
            "priority": SEND_TOOL["inputSchema"]["properties"]["priority"],
            "id": SEND_TOOL["inputSchema"]["properties"]["id"],
        },
        "required": ["text"],
        "additionalProperties": False,
    },
}


class _Eof:
    pass


EOF = _Eof()


def handle_request(msg: dict, sender: str | None, rpc) -> dict | None:
    mid = msg.get("id")
    if mid is None:
        return None
    method = msg.get("method")
    params = msg.get("params")
    if not isinstance(params, dict):
        params = {}
    if method == "initialize":
        asked = params.get("protocolVersion")
        version = asked if asked in SUPPORTED_VERSIONS else SUPPORTED_VERSIONS[0]
        return {
            "jsonrpc": "2.0",
            "id": mid,
            "result": {
                "protocolVersion": version,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "agentmailbus", "version": __version__},
            },
        }
    if method == "ping":
        return {"jsonrpc": "2.0", "id": mid, "result": {}}
    if method == "tools/list":
        tools = (
            [_named_tool(SEND_TOOL, sender), _named_tool(BROADCAST_TOOL, sender)]
            if sender is not None
            else []
        )
        return {"jsonrpc": "2.0", "id": mid, "result": {"tools": tools}}
    if method == "tools/call":
        return _call(mid, params, sender, rpc)
    return {
        "jsonrpc": "2.0",
        "id": mid,
        "error": {"code": -32601, "message": "method not found"},
    }


def _named_tool(tool: dict, sender: str) -> dict:
    """The tool as this session sees it: it says the session's own name, so the
    model can tell another agent where to reply."""
    return {**tool, "description": f"Your name on the bus is {sender}. " + tool["description"]}


def _call(mid, params: dict, sender: str | None, rpc) -> dict:
    tool = params.get("name")
    if sender is None or tool not in {"send", "broadcast"}:
        return _tool_error(mid, "unknown-tool")
    args = params.get("arguments")
    if not isinstance(args, dict):
        return _tool_error(mid, "bad-arguments")
    try:
        msg_id = check_id(args["id"]) if args.get("id") else str(uuid.uuid4())
    except ProtocolError as exc:
        return _tool_error(mid, named_error(exc.code, str(args.get("id"))[:64]))
    # From here on the id exists, and every failure names it.
    try:
        # `send` takes a name only, so a broadcast is always asked for by name.
        if tool == "broadcast" and "to" in args:
            # A model that names a recipient did not mean everyone.
            return _tool_error(
                mid,
                f"bad-arguments (message id {msg_id}; this attempt sent nothing): broadcast takes no `to`, "
                "use send to reach one agent",
            )
        if tool == "send" and args.get("to") == BROADCAST:
            return _tool_error(
                mid,
                f"bad-name (message id {msg_id}; this attempt sent nothing): send takes one name, "
                "use the broadcast tool to reach every agent",
            )
        recipient = BROADCAST if tool == "broadcast" else check_name(args.get("to"), "to")
        text = check_text(args.get("text"))
        priority = check_priority(args.get("priority"))
    except ProtocolError as exc:
        return _tool_error(mid, named_error(exc.code, msg_id))
    payload = {
        "op": "send",
        "id": msg_id,
        "from": sender,
        "to": recipient,
        "text": text,
        "priority": priority,
    }
    try:
        result = rpc(payload)
    except (OSError, ProtocolError, ValueError) as exc:
        # The relay is down or restarting. The tool fails; the server stays up.
        log.warning("relay call failed: %s", type(exc).__name__)
        return _tool_error(mid, named_error("relay-down", msg_id))
    if not isinstance(result, dict) or not result.get("ok"):
        code = str(result.get("error") or "send-failed") if isinstance(result, dict) else "send-failed"
        reason = str(result.get("reason") or "") if isinstance(result, dict) else ""
        return _tool_error(mid, named_error(code, msg_id, reason=reason))
    if tool == "broadcast":
        names = result.get("recipients") or []
        who = ", ".join(names) if names else "reached nobody"
        more = result.get("more")
        if isinstance(more, int) and more > 0:
            who += f" and {more} more"
        line = f"{result.get('id', msg_id)} broadcast to {len(names) + (more or 0)}: {who}"
    else:
        state = "queued" if result.get("waiting") else "accepted"
        line = f"{result.get('id', msg_id)} {state}"
    return {
        "jsonrpc": "2.0",
        "id": mid,
        "result": {"content": [{"type": "text", "text": line}]},
    }


# Errors that the same message can never get past: the broker gave up on it, the id
# belongs to another message, the recipient does not exist, or the message itself is
# invalid. Everything else describes the moment (relay down, session not registered
# yet, answer lost) and the same id is the right retry.
TERMINAL = {
    "delivery-failed", "id-conflict", "unknown-agent",
    "bad-name", "bad-text", "empty-text", "too-large", "bad-priority",
}


# This attempt left nothing stored or delivered: refused by this host's relay before
# the message went to the broker (a not-registered attempt may have sent a register
# frame), or, for not-your-agent, refused by the broker before storing it. The same id is still the right
# retry. An earlier attempt with the same id may have got through, so the wording
# speaks of this attempt only.
NOT_SENT = {"sender-offline", "not-registered", "not-your-agent"}

# Said whoever is asking, since the code alone does not say what happened.
COMMON_HINTS = {
    "not-your-agent": "the broker does not count this host's relay as the name's owner right now (the broker link "
    "or the name's binding changed during the send); the relay registers the name again on the next try",
}

# What to do about a refusal, for the codes where the code alone does not say.
MCP_HINTS = {
    "sender-offline": (
        "this session has no live binding on this host (its binding, process, or socket is gone); "
        "restarting the session binds it again, perhaps under a new name"
    ),
    "not-registered": "the broker has not accepted this session's name",
}

# Why the broker would not take a name, as the relay reports it with not-registered.
REFUSALS = {
    "name-taken": "the broker refused the name (name-taken): a relay on another host that is still connected holds it; "
    "stop that session or use a different name; until then a retry is refused the same way",
    "no-answer": "the broker did not answer the registration; retry with this id in a moment",
    "broker-down": "this host's relay lost its broker connection while registering the name",
}


def named_error(code: str, msg_id: str, hints: dict[str, str] | None = None, reason: str = "") -> str:
    """The error text for a failed send: the code, the id, whether to retry, and a hint."""
    hint = (hints if hints is not None else MCP_HINTS).get(code) or COMMON_HINTS.get(code)
    if code == "not-registered" and reason:
        hint = REFUSALS.get(reason, f"the broker refused the name ({reason}); the relay log has details")
    tail = f": {hint}" if hint else ""
    if code == "in-flight":
        return (
            f"{code} (message id {msg_id}; an earlier send with this id is still in progress and its outcome "
            f"is unknown, retry with this id shortly){tail}"
        )
    if code == "bad-id":
        return f"{code} (message id {msg_id}; this attempt sent no message, retry with a UUID as the id or with no id){tail}"
    if code in NOT_SENT:
        return f"{code} (message id {msg_id}; this attempt sent no message, retry with this id){tail}"
    if code == "too-many-recipients":
        return f"{code} (message id {msg_id}; this send made no new copies, retry with this id once fewer agents are online){tail}"
    if code in TERMINAL:
        return f"{code} (message id {msg_id}; not retryable as is, send a new message with a new id){tail}"
    return f"{code} (message id {msg_id}; outcome unknown, retry with this id){tail}"


def _tool_error(mid, code: str) -> dict:
    return {
        "jsonrpc": "2.0",
        "id": mid,
        "result": {"content": [{"type": "text", "text": code}], "isError": True},
    }


def _rpc_error(mid, code: int, message: str) -> dict:
    return {"jsonrpc": "2.0", "id": mid, "error": {"code": code, "message": message}}


def read_message(stream):
    """One JSON value per line, at most MAX_LINE bytes. EOF at end of input.

    Raises ValueError (including JSONDecodeError) for a bad line; the over-long
    remainder of such a line is discarded first so the stream stays in sync. A
    line holding JSON `null` is returned as None, which is a value, not the end.
    """
    while True:
        line = stream.readline(MAX_LINE + 2)
        if line == b"":
            return EOF
        if len(line) > MAX_LINE + 1 or (len(line) == MAX_LINE + 1 and not line.endswith(b"\n")):
            while line and not line.endswith(b"\n"):
                line = stream.readline(MAX_LINE + 2)
            raise ValueError("too-large")
        line = line.strip()
        if not line:
            continue
        try:
            return json.loads(line)
        except RecursionError as exc:
            raise ValueError("too-deep") from exc


def write_message(stream, obj) -> None:
    text = json.dumps(obj, ensure_ascii=False)
    try:
        data = text.encode()
    except UnicodeEncodeError:
        # A lone surrogate in a client-supplied id cannot be UTF-8; escape it instead.
        data = json.dumps(obj, ensure_ascii=True).encode()
    stream.write(data + b"\n")
    stream.flush()


def serve_stdio(sender: str | None, rpc, stdin=None, stdout=None) -> None:
    stdin = sys.stdin.buffer if stdin is None else stdin
    stdout = sys.stdout.buffer if stdout is None else stdout
    while True:
        try:
            msg = read_message(stdin)
        except ValueError as exc:
            # The id cannot be known without parsing the line, so this reply carries null.
            code, text = (-32600, "request too large") if str(exc) == "too-large" else (-32700, "parse error")
            write_message(stdout, _rpc_error(None, code, text))
            continue
        if msg is EOF:
            break
        if isinstance(msg, list):
            if not msg:
                write_message(stdout, _rpc_error(None, -32600, "empty batch"))
                continue
            replies = [r for r in (_handle(m, sender, rpc) for m in msg) if r is not None]
            if replies:
                write_message(stdout, replies)
            continue
        reply = _handle(msg, sender, rpc)
        if reply is not None:
            write_message(stdout, reply)


def _handle(msg, sender: str | None, rpc) -> dict | None:
    if not isinstance(msg, dict):
        return _rpc_error(None, -32600, "invalid request")
    try:
        return handle_request(msg, sender, rpc)
    except Exception as exc:
        # A malformed request must not end the server and drop the session binding.
        log.warning("request failed: %s", type(exc).__name__)
        mid = msg.get("id")
        return None if mid is None else _rpc_error(mid, -32603, "internal error")
