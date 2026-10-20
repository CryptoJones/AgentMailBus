# Protocol

Version 1. UTF-8 JSON objects. WebSocket frames are one object each. The local relay socket and the dsh inbox are one object plus a newline.

Names match `^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`. Text is at most 32,000 characters. Frames are at most 256 KiB.

## Broker link

The relay speaks first, within 5 seconds:

```json
{"type":"hello","v":1,"host":"ronin28","token":"<secret>"}
```

A bad peer address, a bad name, or a bad token closes the socket. The error frame for a bad token is `{"type":"error","error":"unauthorized"}` and does not echo the token.

```json
{"type":"welcome","host":"ronin28"}
```

Register a live name. The broker rejects the name when another connected relay already owns it.

```json
{"type":"register","name":"rea-63","harness":"claude"}
{"type":"registered","name":"rea-63"}
```

The first register on a connection replays every queued message for that name. A later register only flushes mail that is still queued. `unregister` marks the name offline and puts unacked mail back on the queue. The row stays, so a later send to that name waits instead of failing as unknown.

```json
{"type":"send","id":"<uuid>","from":"grok-aima","to":"rea-63","text":"hello","priority":"next"}
{"type":"accepted","id":"<uuid>","to":"rea-63","waiting":false}
```

`waiting` is true when the recipient's relay is not connected. The sender must be a name this relay currently has registered. The same id is accepted once and is not queued twice.

```json
{"type":"deliver","id":"<uuid>","from":"grok-aima","from_host":"makemake","to":"rea-63","text":"hello","priority":"next","ts":""}
{"type":"ack","id":"<uuid>"}
```

`from_host` is the broker's record of the sender's relay, not a field the sender gets to choose. A relay that has already injected that id acks it again and does not inject it again. If local delivery fails, the relay sends `nack` and the broker puts that delivered row back on the queue. About once a second the relay sends `pull`, and the broker flushes queued mail for the names that relay owns. An acked row is never put back.

```json
{"type":"directory","id":"<uuid>"}
{"type":"agents","id":"<uuid>","agents":[{"name":"rea-63","host":"ronin28","harness":"claude","online":true,"queued":0}]}
```

On broker startup, every agent is offline and every delivered-but-unacked message returns to the queue.

## Local relay socket

Mode `0600`. One request, one response, then the client disconnects.

```json
{"op":"send","from":"grok-aima","to":"rea-63","text":"hello","priority":"next"}
{"ok":true,"op":"send","id":"<uuid>","waiting":false}
```

`who` and `ping` are the other operations.

## Session file

`~/.local/share/agentmailbus/sessions/<name>.json`, mode `0600`.

```json
{"name":"rea-63","harness":"claude","pid":12345,"wake":"","socket":""}
```

A Grok or Orca binding is live only while `pid` is alive and `wake` is set. A Claude binding is live while that pid is alive and its cc-socks socket exists. A dsh binding is live while `socket` exists.

## Claude frame

When `~/.claude/sessions/<pid>.<sha256 of the socket path>.key` contains `peerToken`, the relay sends that auth line first. The hash is SHA-256 of the exact path string that is connected, UTF-8. The user line is the verified cc-socks shape: `type`, `message.content`, `from`, `priority`, `msg_id`.

## dsh frame

```json
{"type":"user","id":"<uuid>","from":"grok-aima","from_host":"makemake","text":"hello"}
```

## Wake line

```json
{"id":"<uuid>","from":"rea-63","from_host":"ronin28","to":"grok-aima","ts":"","text":"hello"}
```

The file is created mode `0600`.
