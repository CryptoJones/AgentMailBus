# Protocol

Version 1. UTF-8 JSON objects. WebSocket frames are one object each. The local relay socket, the MCP stdio transport, and the dsh inbox are one object plus a newline.

Names match `^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`. Message ids are UUIDs in canonical lowercase form. Text is at most 32,000 characters. Frames are at most 256 KiB.

## Trust

One shared token. Every relay that holds it has the same authority: it names its own `host`, it can register any name no connected relay currently owns, and it can read the directory. `from_host` is the broker's record of which relay registered the sender, and that relay named itself. There is no per-host identity. The token and every message cross the LAN in cleartext on `ws://`. Treat the token as full control of every agent on the bus.

Without the token a LAN host can open a connection and hold it for the five-second hello window, and nothing else. At most 64 connections may sit in that window at once.

## Broker link

The relay speaks first, within 5 seconds:

```json
{"type":"hello","v":1,"host":"ronin28","token":"<secret>"}
```

A bad peer address, a bad name, or a bad token closes the socket. The error frame for a bad token is `{"type":"error","error":"unauthorized"}` and does not echo the token. A second hello with the same `host` replaces the first link: its names go offline and its unacked mail returns to the queue.

```json
{"type":"welcome","host":"ronin28"}
```

Register a live name. The broker rejects the name with `name-taken` when another connected relay owns it.

```json
{"type":"register","name":"rea-63","harness":"claude"}
{"type":"registered","name":"rea-63"}
```

Every register flushes the mail that is queued for that name. `unregister` is accepted only from the relay that owns the name; it marks the name offline and puts unacked mail back on the queue. The row stays, so a later send to that name waits instead of failing as unknown.

```json
{"type":"send","id":"<uuid>","from":"grok-aima","to":"rea-63","text":"hello","priority":"next"}
{"type":"accepted","id":"<uuid>","to":"rea-63","waiting":false}
```

The sender must be a name this relay currently has registered. The broker stamps the time; a `ts` in the send frame is ignored. The same id is accepted once: a second send with the same id, sender, and recipient returns `accepted` again and queues nothing. The same id with a different sender or recipient is refused with `id-conflict`. `waiting` is true when the message could not be handed to the recipient's relay right now, because that relay is not connected, the name is not registered, or the push failed.

```json
{"type":"deliver","id":"<uuid>","from":"grok-aima","from_host":"makemake","to":"rea-63","text":"hello","priority":"next","ts":"2026-10-20T19:42:03.400336+00:00"}
{"type":"ack","id":"<uuid>"}
```

Queued mail is delivered in the order the broker accepted it. The broker marks a row `delivered` before it sends the frame, so a push and a pull cannot send the same row twice, and an `ack` is never overwritten. `ack` and `nack` are accepted only from the relay that owns the recipient. A relay that has already injected that id acks it again and does not inject it again.

If local delivery fails, or the relay has no live session for the name, the relay sends `nack`. The broker counts the attempt and requeues the row with a delay of `2^attempts` seconds, at most 60. After 20 attempts the row is marked `failed` and is not retried. About once a second the relay sends `pull`, and the broker flushes due mail for the names that relay owns. An acked row is never put back.

```json
{"type":"directory","id":"<uuid>"}
{"type":"agents","id":"<uuid>","agents":[{"name":"rea-63","host":"ronin28","harness":"claude","online":true,"queued":0}]}
```

`queued` counts rows that are queued or delivered and not yet acked.

On broker startup, every agent is offline and every delivered-but-unacked message returns to the queue. Acked and failed rows are kept for 7 days (`retention_days`), which is also the window in which a repeated id is recognised, then deleted. The relay keeps its delivered-id set for the same period.

## What ack means

`ack` means the relay wrote the frame to the session's socket or file. cc-socks sends no reply, and the dsh inbox is this project's contract, so a session that rejects or ignores the bytes is still acked. For a Grok or Orca wake file, `ack` means the line was appended and synced; whether a monitor reads it is outside the protocol. A relay killed after the write and before it records the id will inject that message once more when the broker requeues it.

## Local relay socket

Mode `0600`. One request, one response, then the client disconnects. A line is at most 256 KiB.

```json
{"op":"send","from":"grok-aima","to":"rea-63","text":"hello","priority":"next","id":"<optional uuid>"}
{"ok":true,"op":"send","id":"<uuid>","waiting":false}
```

Pass the same `id` again to retry a send whose outcome was lost; the broker treats it as the same message. `who` and `ping` are the other operations. Any process running as the same user can use this socket and send as any name bound on this host.

## MCP

`agentmailbus mcp` speaks MCP over stdio: one JSON-RPC object per line in each direction, no headers. It offers protocol versions 2025-06-18, 2025-03-26, and 2024-11-05. A failed call to the relay returns a tool error; the server stays up and the session stays bound.

## Session file

`~/.local/share/agentmailbus/sessions/<name>.json`, mode `0600`.

```json
{"name":"rea-63","harness":"claude","pid":12345,"wake":"","socket":""}
```

A Grok or Orca binding is live only while `pid` is alive and `wake` is set. A Claude binding is live while that pid is alive and its cc-socks socket exists. A dsh binding is live while `socket` exists. A pid that belongs to another user is not alive for this purpose. A file that cannot be parsed is skipped on its own; it does not hide the other names. The relay re-reads the file before every delivery and picks up a changed pid, socket, or wake path within a second. The MCP server removes its file on exit only while the file still holds its own binding.

## Claude frame

When `~/.claude/sessions/<pid>.<sha256 of the socket path>.key` contains `peerToken`, the relay sends that auth line first. The hash is SHA-256 of the exact path string that is connected, UTF-8. The user line is the verified cc-socks shape: `type`, `message.content`, `from`, `priority`, `msg_id`. The content is wrapped in `<cross-session-message from="<host>" from-name="<name>" from-mode="agentmailbus">`. Any `<cross-session-message` or `</cross-session-message` inside the text has its `<` replaced by `&lt;`, so a sender cannot close the wrapper and forge a second sender.

## dsh frame

```json
{"type":"user","id":"<uuid>","from":"grok-aima","from_host":"makemake","text":"hello"}
```

## Wake line

```json
{"id":"<uuid>","from":"rea-63","from_host":"ronin28","to":"grok-aima","ts":"2026-10-20T19:42:03.400336+00:00","text":"hello"}
```

The file is created mode `0600`. The directory is created `0700` only if it did not exist; an existing directory keeps its mode.
