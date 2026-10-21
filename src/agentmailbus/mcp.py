"""Stdio MCP server. The only tool is send. Receiving is the relay's job.

MCP stdio framing is one JSON-RPC object per line, no headers. A line may also hold a
JSON array (a batch); the reply is then an array.
"""

from __future__ import annotations

import json
import logging
import sys
import uuid

from agentmailbus import __version__
from agentmailbus.protocol import ProtocolError, check_id, check_name, check_priority, check_text

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
        tools = [_named_tool(SEND_TOOL, sender)] if sender is not None else []
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
    if sender is None or params.get("name") != "send":
        return _tool_error(mid, "unknown-tool")
    args = params.get("arguments")
    if not isinstance(args, dict):
        return _tool_error(mid, "bad-arguments")
    try:
        msg_id = check_id(args["id"]) if args.get("id") else str(uuid.uuid4())
    except ProtocolError as exc:
        return _tool_error(mid, exc.code)
    # From here on the id exists, and every failure names it.
    try:
        recipient = check_name(args.get("to"), "to")
        text = check_text(args.get("text"))
        priority = check_priority(args.get("priority"))
    except ProtocolError as exc:
        return _tool_error(mid, _named(exc.code, msg_id))
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
        return _tool_error(mid, _named("relay-down", msg_id))
    if not isinstance(result, dict) or not result.get("ok"):
        code = str(result.get("error") or "send-failed") if isinstance(result, dict) else "send-failed"
        return _tool_error(mid, _named(code, msg_id))
    state = "queued" if result.get("waiting") else "accepted"
    return {
        "jsonrpc": "2.0",
        "id": mid,
        "result": {
            "content": [{"type": "text", "text": f"{result.get('id', msg_id)} {state}"}],
        },
    }


# Errors that the same message can never get past: the broker gave up on it, the id
# belongs to another message, the recipient does not exist, or the message itself is
# invalid. Everything else describes the moment (relay down, session not registered
# yet, answer lost) and the same id is the right retry.
TERMINAL = {
    "delivery-failed", "id-conflict", "unknown-agent",
    "bad-name", "bad-text", "empty-text", "too-large", "bad-priority",
}


def _named(code: str, msg_id: str) -> str:
    if code in TERMINAL:
        return f"{code} (message id {msg_id}; not retryable as is, send a new message with a new id)"
    return f"{code} (message id {msg_id}; outcome unknown, retry with this id)"


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
