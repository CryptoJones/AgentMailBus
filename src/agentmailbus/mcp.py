"""Stdio MCP server. The only tool is send. Receiving is the relay's job.

MCP stdio framing is one JSON-RPC object per line, no headers.
"""

from __future__ import annotations

import json
import logging
import sys

from agentmailbus import __version__
from agentmailbus.protocol import ProtocolError, check_name, check_priority, check_text

log = logging.getLogger("agentmailbus.mcp")

SUPPORTED_VERSIONS = ("2025-06-18", "2025-03-26", "2024-11-05")

SEND_TOOL = {
    "name": "send",
    "description": (
        "Send a message to a named agent. The relay delivers it. "
        "You do not receive messages with a tool."
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
        },
        "required": ["to", "text"],
        "additionalProperties": False,
    },
}


def handle_request(msg: dict, sender: str, rpc) -> dict | None:
    mid = msg.get("id")
    if mid is None:
        return None
    method = msg.get("method")
    if method == "initialize":
        asked = (msg.get("params") or {}).get("protocolVersion")
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
        return {"jsonrpc": "2.0", "id": mid, "result": {"tools": [SEND_TOOL]}}
    if method == "tools/call":
        return _call(mid, msg.get("params") or {}, sender, rpc)
    return {
        "jsonrpc": "2.0",
        "id": mid,
        "error": {"code": -32601, "message": "method not found"},
    }


def _call(mid, params: dict, sender: str, rpc) -> dict:
    if params.get("name") != "send":
        return _tool_error(mid, "unknown-tool")
    args = params.get("arguments") or {}
    try:
        recipient = check_name(args.get("to"), "to")
        text = check_text(args.get("text"))
        priority = check_priority(args.get("priority"))
    except ProtocolError as exc:
        return _tool_error(mid, exc.code)
    try:
        result = rpc(
            {"op": "send", "from": sender, "to": recipient, "text": text, "priority": priority}
        )
    except (OSError, ProtocolError, ValueError) as exc:
        # The relay is down or restarting. The tool fails; the server stays up.
        log.warning("relay call failed: %s", type(exc).__name__)
        return _tool_error(mid, "relay-down")
    if not result.get("ok"):
        return _tool_error(mid, str(result.get("error") or "send-failed"))
    state = "queued" if result.get("waiting") else "accepted"
    return {
        "jsonrpc": "2.0",
        "id": mid,
        "result": {
            "content": [{"type": "text", "text": f"{result.get('id', '')} {state}"}],
        },
    }


def _tool_error(mid, code: str) -> dict:
    return {
        "jsonrpc": "2.0",
        "id": mid,
        "result": {"content": [{"type": "text", "text": code}], "isError": True},
    }


def read_message(stream) -> dict | None:
    """One JSON object per line. None at end of input. Raises JSONDecodeError on a bad line."""
    while True:
        line = stream.readline()
        if line == b"":
            return None
        line = line.strip()
        if not line:
            continue
        return json.loads(line)


def write_message(stream, obj: dict) -> None:
    stream.write((json.dumps(obj, ensure_ascii=False) + "\n").encode())
    stream.flush()


def serve_stdio(sender: str, rpc, stdin=None, stdout=None) -> None:
    stdin = sys.stdin.buffer if stdin is None else stdin
    stdout = sys.stdout.buffer if stdout is None else stdout
    while True:
        try:
            msg = read_message(stdin)
        except json.JSONDecodeError:
            write_message(
                stdout,
                {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "parse error"}},
            )
            continue
        if msg is None:
            break
        if not isinstance(msg, dict):
            continue
        reply = handle_request(msg, sender, rpc)
        if reply is not None:
            write_message(stdout, reply)
