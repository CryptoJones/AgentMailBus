# Protocol

Version 1. UTF-8 JSON objects. WebSocket frames are one object each. The local relay socket, the MCP stdio transport, and the dsh inbox are one object plus a newline.

Names match `^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`. Message ids are UUIDs in canonical lowercase form. Text is at most 32,000 characters. Frames are at most 256 KiB.

## Trust

One shared token. Every relay that holds it has the same authority: it names its own `host`, it can register any name no connected relay currently owns, and it can read the directory. `from_host` is the broker's record of which relay registered the sender, and that relay named itself. There is no per-host identity. The token and every message cross the LAN in cleartext on `ws://`. Treat the token as full control of every agent on the bus.

Without the token a LAN host can open a connection and hold it for the five-second hello window, and nothing else. At most 8 connections from one address may sit in that window at once; the WebSocket handshake before it is bounded by the library's own open timeout.

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

Every register sends the oldest message waiting for that name, if it is due. `unregister` is accepted only from the relay that owns the name; it marks the name offline and puts unacked mail back on the queue. The row stays, so a later send to that name waits instead of failing as unknown.

```json
{"type":"send","id":"<uuid>","from":"grok-aima","to":"rea-63","text":"hello","priority":"next"}
{"type":"accepted","id":"<uuid>","to":"rea-63","waiting":false}
```

The sender must be a name this relay currently has registered. The broker stamps the time; a `ts` in the send frame is ignored. The same id is accepted once: a second send with the same id, sender, and recipient returns `accepted` again and queues nothing. The same id with a different sender or recipient is refused with `id-conflict`. An id whose delivery has already been given up on is refused with `delivery-failed`. `waiting` is true when the message could not be handed to the recipient's relay right now: that relay is not connected, the name is not registered, the push failed, or an older message for that name is still ahead of it.

A broadcast is a send to `*`, which can never be a name:

```json
{"type":"send","id":"<uuid>","from":"grok-aima","to":"*","text":"hello","priority":"next"}
{"type":"accepted","id":"<uuid>","to":"*","recipients":["dsh-main","rea-63"],"waiting":false}
```

The broker first records the broadcast (id, sender, text, priority, and a salt keyed by a secret the broker database keeps, the sender, and the id), then makes one ordinary message for every name a connected relay holds online, the sender excepted. A name that goes offline before its copy is made is skipped. A name that is offline gets nothing from this send; a later retry with the same id reaches it if it is online by then. Each copy has an id the broker chooses, stable across retries of this broadcast and not predictable before the copy is made; its row records the broadcast id. From then on the copy is a direct message, with the same order, ack, retry, and give-up rules.

`recipients` lists, sorted, every copy of this broadcast not given up on, from this send or an earlier one with the same id. When there are more than 1,000 it lists the first 1,000 and `more` gives the rest as a count. `waiting` is true when any of those copies is still queued. One send makes at most 1,000 new copies: when more names than that are online without a copy, the send is refused with `too-many-recipients` and makes none. A refused first send records nothing, so its id is not reserved yet: a retry with the same id once fewer are online becomes the broadcast, with the text of that retry. A retry of a broadcast whose copies have all been given up on, with no new name online, is accepted with an empty `recipients`, where a direct id would get `delivery-failed`. A copy made by a retry is queued behind whatever that name already had, like any message the broker accepts at that moment.

A retry with the same id sends the text and priority first recorded (while the record is kept), reaches the names online at the retry that have no copy yet, and reaches none of them twice. A broadcast id that already names a direct message, or that another sender's broadcast holds, is refused with `id-conflict`, and a direct send with an id a broadcast holds is refused the same way. With nobody else online the broadcast is accepted with an empty `recipients`, and its id is still reserved. A broadcast record is deleted once it is older than `retention_days` and none of its copies is left. Copy ids do not depend on the record, so a retry after that makes the same copy ids, and a relay that injected a copy within its 30-day seen window acks it again without injecting it; that holds while the broker keeps the same database, since a new database makes a new secret, and only within the relay's seen window: a relay that has forgotten a copy injects it again. Back up a running broker with SQLite's online backup (`sqlite3 broker.db .backup copy.db`), not a file copy, which can miss recent writes in the WAL, the secret included. As for any id, a retry is the same message: an id reused for other text after the record is gone is acked by a relay that remembers the copy, without injecting the new text.

```json
{"type":"deliver","id":"<uuid>","from":"grok-aima","from_host":"makemake","to":"rea-63","text":"hello","priority":"next","ts":"2026-10-20T19:42:03.400336+00:00"}
{"type":"ack","id":"<uuid>"}
```

A copy of a broadcast also carries `"broadcast":"<broadcast id>"`, and so do the Claude, dsh, and wake frames made from it.

One message per name is in flight at a time, so mail arrives in the order the broker accepted it, across failures too. The broker sends the oldest unfinished message for a name when it is due, and sends the next one when the `ack` for it arrives; a `pull` sends it when nothing is in flight. A new send is handed over at once only when it is the head of that name's queue. The broker marks a row `delivered` before it sends the frame, so two paths cannot send the same row, and an `ack` is never overwritten. `ack` and `nack` are accepted only from the relay that owns the recipient. A relay that has already injected that id acks it again and does not inject it again.

If local delivery fails, or the relay has no live session for the name, the relay sends `nack`. The broker counts the attempt and requeues the row with a delay of `2^attempts` seconds, at most 60. After 20 attempts the row is marked `failed` and is not retried. About once a second the relay sends `pull`, and for each name that relay owns the broker sends the oldest waiting message if it is due and nothing is in flight. A delivered message with no `ack` or `nack` after 300 seconds is counted as a failed attempt and requeued (or marked `failed` if it was the 20th), so a lost ack cannot block a name; the relay's seen set absorbs the late duplicate. An `ack` from the owning relay that arrives after the row was handed back, or after the broker gave up on it, still finishes it, since an ack always means the message was written; an ack for a row that was never handed out is ignored. An acked row is never put back.

```json
{"type":"directory","id":"<uuid>"}
{"type":"agents","id":"<uuid>","agents":[{"name":"rea-63","host":"ronin28","harness":"claude","online":true,"queued":0,"failed":0}]}
```

`queued` counts rows that are queued or delivered and not yet acked. `failed` counts rows the broker has given up on.

On broker startup, every agent is offline and every delivered-but-unacked message returns to the queue. A database from a broker that did not stamp `delivered_at` has it backfilled for rows that were delivered or had attempts, so the relay's late ack still finishes them; a row that older broker had already handed back cannot be told from a new one, is sent once more, and the relay's seen set absorbs it. Acked and failed rows are kept for 7 days after they finish (`retention_days`), which is also the window in which a repeated id is recognised, then deleted. The relay keeps its delivered-id set for 30 days, pruned hourly, so a row the broker requeues after a long relay outage is still recognised. A relay that is down for longer than that will inject such a message once more. An id is never to be reused for a different message: a relay that still remembers it would ack without writing.

## What ack means

`ack` means the relay wrote the frame to the session's socket or file. cc-socks sends no reply, and the dsh inbox is this project's contract, so a session that rejects or ignores the bytes is still acked. For a Grok or Orca wake file, `ack` means the whole line was appended to a regular file; a sync or close error after that is logged, not retried. A write that fails part-way is taken back out of the file when nothing else has landed after it (a `tail -F` reading at that instant sees the file shrink; on an append-only file, which cannot be truncated, it is fenced instead), and the message is nacked; when something else has landed, the fragment is ended with a newline unless the later writer already did; a line an outside writer glued onto the fragment is part of that damaged line and is skipped with it. Before every append the relay ends any unterminated tail (left by a crash, or by an outside writer landing on a fragment) with a newline, so a line reader skips the damaged line and the next one is clean. Two names that share one wake file never write at the same time, and wait for each other. A path that is not a regular file is a failed delivery, and whether a monitor reads the file is outside the protocol. The relay runs one delivery worker per name on a pool of 128 threads, so a slow delivery for one name does not hold another's, apart from two names sharing one wake file; a worker idle for a minute goes away. The next message for a name waits for the previous one's OMI note when the mirror is on. A relay killed after the write and before it records the id will inject that message once more when the broker requeues it. If recording the id fails after the write, the relay logs it, keeps the id in memory for the life of the process, and still acks; the OMI note is written after the ack; nothing after a successful write turns into a `nack`, a failed `fsync` on a wake file included. When a name moves to another host while its message is being written on the old one, the new host can receive that message as well; the two hosts do not share a seen set. A failure before the write is handed back with `nack`. Neither case stops the relay's delivery worker.

## Local relay socket

Mode `0600`. One request, one response, then the client disconnects. A line, newline included, is at most 256 KiB.

```json
{"op":"send","from":"grok-aima","to":"rea-63","text":"hello","priority":"next","id":"<optional uuid>"}
{"ok":true,"op":"send","id":"<uuid>","waiting":false}
```

A send to `*` is a broadcast, and its answer adds `"recipients":[...]` (and `"more"` past 1,000). The `agentmailbus send` command refuses `*`; `agentmailbus broadcast` sends one.

Pass the same `id` again to retry a send whose outcome was lost; the broker treats it as the same message. While that id is still waiting for the broker's answer, a second send with it is refused with `in-flight`. A line over 256 KiB is refused with `too-large`. Text that cannot be encoded as UTF-8 is refused with `bad-text`. `who` and `ping` are the other operations. Any process running as the same user can use this socket and send as any name bound on this host.

## MCP

`agentmailbus mcp` speaks MCP over stdio: one JSON-RPC object per line in each direction, no headers. A line may hold an array of requests; the reply is then an array, and an empty array is answered with an error. A line over 1 MiB, or one that cannot be parsed, is answered with a JSON-RPC error whose `id` is null, since the request's id cannot be read, and the line is skipped. It offers protocol versions 2025-06-18, 2025-03-26, and 2024-11-05. The tools are `send(to, text, priority, id)` and `broadcast(text, priority, id)`. `send` takes a name only and refuses `*`, so a broadcast is always asked for by name; `broadcast` answers with up to 1,000 names a copy is going to, delivered or still queued, and counts the rest. Both take an optional `id`. `too-many-recipients` gets its own advice: nothing new was sent, and a retry with the same id once fewer agents are online is the same broadcast. Once the id exists, every failure names it. A terminal failure (`delivery-failed`, `id-conflict`, `unknown-agent`, or an invalid name, text, or priority) says to send a new message with a new id. Every other failure describes the moment (the relay or broker is down, the answer was lost, the session is not registered yet) and says to retry with that id, which is the same message. A failed call to the relay, or a malformed request, returns an error; the server stays up and the session stays bound.

## Session file

`~/.local/share/agentmailbus/sessions/<name>.json`, mode `0600`.

```json
{"name":"rea-63","harness":"claude","pid":12345,"wake":"","socket":""}
```

A Claude binding made by `agentmailbus mcp` also holds `server` (that server's pid: on exit it removes the file only while `server` still names it), `sessions_dir` (where the session keeps its records, so the relay finds the peer key), and, when the name was chosen automatically, `proc_start` (the session's start time from its record). Choosing an automatic name and saving it, `--from` saves, `agentmailbus bind`, and removal on exit all happen under an exclusive lock on `<registry>/.lock`; a save refuses when the lock stays busy for 10 seconds, removal on exit leaves the file when it stays busy for one second (the next automatic start sweeps it once the session is gone), and a filesystem that cannot lock goes ahead unlocked. An automatic start first removes Claude and opencode bindings whose pid is dead or, on Linux, whose `proc_start` no longer matches that pid.

An opencode binding is made by `agentmailbus mcp --harness opencode`, which opencode starts in the project's folder. `pid` is the opencode process (the server's parent), `socket` is `<data dir>/opencode/<pid>-<folder key>.sock` (the folder key is the first 12 hex digits of SHA-256 of the real path of the server's working folder; the plugin computes the same from the `directory` opencode gives it), `server` is as for Claude, and `proc_start` is the opencode process's start time: clock ticks from `/proc/<pid>/stat` on Linux, the `ps -o lstart=` text elsewhere, read with `LC_ALL=C TZ=UTC` (to the second, so a pid reused within the same second is not told apart) (null when neither can be read). `--from` and `agentmailbus bind` fill in the same socket and start time; `--socket` is refused for opencode, since the plugin listens only on the derived path. It is live while that pid is alive, on Linux its start time still matches, and a socket (not some other file, nor a symlink) of the relay's user is at the path, in a directory of that user closed to everyone else, as the plugin requires. The `ps` text is compared only when names are chosen and bindings swept, not on every liveness scan, so off Linux a binding left by a killed opencode whose pid a new opencode in the same folder reuses looks live (the new plugin listens on the same path) until that new opencode's server starts and sweeps it. An automatic name is not given, to Claude or opencode, while another binding holds it and is live, or is an opencode binding whose pid is alive with a matching start time (its plugin may not be listening yet). For opencode a holder is the same session only with the same pid and the same socket, since one process serves several folders. An automatic opencode start also removes sockets in that directory whose pid is dead. The data dir is `$XDG_DATA_HOME/agentmailbus` when that is an absolute path with no `..` after a leading `~/` is expanded, else `~/.local/share/agentmailbus`; this holds for every part of the program. It must belong to one host: two hosts sharing it (an NFS home) would remove each other's sockets and bindings, as for Claude.

A Grok or Orca binding is live only while `pid` is alive and `wake` is set to an absolute path (the command line makes it absolute in the caller's directory, since the relay runs elsewhere; for these two harnesses the liveness scan judges the string only, touches no filesystem and consults no user database, taking `~/` from an absolute HOME; a `~user` path, or a `~/` with no usable HOME, is not live) with no `..`, that is not named like a relay temporary file. When the binding is saved the wake path must be present, must if it exists be a regular file (not a symlink, a directory, a FIFO or a device) that the user saving the binding (the relay's user, in the intended setup) can read and write (or an empty one of that user's that nobody can write to, which the relay repairs by remaking it), must have nothing but directories on the way to its nearest existing ancestor, which must be writable when the file has yet to be made or remade, and all of that must be inspectable (every question is asked with one rule: a lasting error such as EACCES, EPERM, ELOOP, ENAMETOOLONG or EROFS refuses, and when a permission check gives no reason the open or a create-and-unlink of a reserved-pattern temporary supplies one; a passing error such as EIO or ESTALE is logged and lets the path through, and the delivery judges it again), must not be a bare `~` or `~user` or a `~user` that does not resolve, and a `~` is stored expanded. A binding the scan refuses is logged once per name and path (the relay remembers the last 1024). A session file from before this version that holds a `~user` path, a relative path, a path with `..`, or no wake path is not live (logged once); restart that session. A Claude or dsh binding's liveness still checks that its socket exists; those live under the temporary or runtime directory. A Claude binding is live while that pid is alive, its cc-socks socket exists, and, on Linux when it holds `proc_start`, that pid's start time still matches. A dsh binding is live while `socket` exists and, when `pid` is set, that pid is alive. A pid that belongs to another user is not alive for this purpose. A file that cannot be parsed is skipped on its own; it does not hide the other names. The relay re-reads the file before every delivery and picks up a changed pid, socket, or wake path within a second. The MCP server removes its file on exit only while the file still holds its own binding.

## Claude frame

When `<sessions dir>/<pid>.<sha256 of the socket path>.key` contains `peerToken` (the sessions dir is the binding's `sessions_dir`, or `~/.claude/sessions`), the relay sends that auth line first. The hash is SHA-256 of the exact path string that is connected, UTF-8. The user line is the verified cc-socks shape: `type`, `message.content`, `from`, `priority`, `msg_id`. The content is wrapped in `<cross-session-message from="<host>" from-name="<name>" from-mode="agentmailbus">`, with `broadcast="<broadcast id>"` added after `from-mode` for a copy of a broadcast. Any `<cross-session-message` or `</cross-session-message` inside the text has its `<` replaced by `&lt;`, so a sender cannot close the wrapper and forge a second sender.

## opencode frame

```json
{"type":"user","id":"<uuid>","content":"<cross-session-message …>\nhello\n</cross-session-message>"}
```

One line per connection. `content` is the text wrapped and escaped exactly as in the Claude frame, sender and broadcast attribute included; the plugin reads nothing else. It prompts the session with `content` through opencode's own client and does not answer, so `ack` means the line was written: a prompt that then fails inside opencode is logged by the plugin and the message is lost, not retried. When opencode answers 404 (the session is gone) the plugin prompts its next choice instead, once. A frame whose `id` the plugin has already taken is skipped, whatever became of it; the relay does not send an id again once its write succeeded. The plugin waits for opencode however long a call takes, so messages for a folder stay in order, and logs a call slower than a minute; at most 1,000 messages wait behind it, and further ones are dropped and logged. Messages waiting in the plugin are lost if opencode exits. It reads at most 1 MiB per connection, drops a line that is not JSON, and skips a frame whose `id` it has seen among its last 1,024. A user message counts as a person's choice of session once its first text part that is not marked `synthetic` or `ignored` (text opencode adds by itself, such as a file an @mention read) is seen and does not start with the wrapper, so a person who pastes a bus message as their prompt is taken for the bus. An archived session is never a target. The socket path may be at most 103 bytes (the macOS limit); with a longer one neither side uses it, and the session stays off the bus with a logged reason.

## dsh frame

```json
{"type":"user","id":"<uuid>","from":"grok-aima","from_host":"makemake","text":"hello"}
```

A copy of a broadcast adds `"broadcast":"<broadcast id>"`.

## Wake line

```json
{"id":"<uuid>","from":"rea-63","from_host":"ronin28","to":"grok-aima","ts":"2026-10-20T19:42:03.400336+00:00","text":"hello"}
```

A copy of a broadcast adds `"broadcast":"<broadcast id>"`.

A file this relay creates is made `0600` through its descriptor, whatever the umask, before it is linked into place, so the wake path never names a half-made file. The temporary file is reopened read-write before it is linked, so the wake path never names a file the relay cannot use; if that reopen fails, the delivery fails and is retried, since making the file in place under the same umask and mount would only publish the failure. Where the temporary route cannot be taken at all (a filesystem without hard links, or any other failure to make or link the temporary file) the file is created in place and made `0600` the same way; the umask narrows it only if the relay dies between the open and the mode change. On either route the mode is read back, and a filesystem that keeps a different one (one that does not store modes, or accepts the change and ignores it) is logged and delivered to anyway. A wake file the relay cannot open for want of permission is removed again, so the next attempt starts over, only while it is an empty regular file of the relay's user with no write bit for anyone (so no writer can have put anything in it since the mode was set; a descriptor opened for writing before that is the accepted same-user case), and, right after creation, only while it is still the file just made, all checked right before the removal; that covers a file just made under a umask that stripped owner write as well as read, and one left by an earlier attempt that failed for another reason, whether or not that attempt could identify it. A file that belongs to another user, has gained data, has a write bit, or cannot be removed is left where it is with a logged warning, and later deliveries to that name fail until it is fixed by hand. A reopen that fails for a reason other than permission leaves the new file in place for the next attempt to open. An existing file keeps its mode, and must be readable as well as writable by the relay user, since the tail is read before every append; a file the relay cannot open or create is a failed delivery and a logged warning. The temporary name is `.amb-wake-<digits>-<eight lowercase hex digits>.tmp`, and that pattern is reserved: a Grok or Orca binding whose wake path is missing, is named like it, is not a string, contains `..` (after expansion) or a NUL byte, is not absolute, exists and is not a regular file the relay can read and write (the empty unwritable file of its own user that it repairs excepted), cannot be made or remade in its directory, has something other than a directory on the way to it, cannot be inspected for a lasting reason, is a bare `~` or `~user`, or whose `~` does not expand to an absolute path, is refused with `bad-wake` when it is saved (the command line judges `~` paths as typed, before expanding them, and expands only a path that starts with `~`); a session file that names one anyway is not live, except that the scan looks at nothing on the filesystem, so whatever only the filesystem can show (a directory, a symlink, a FIFO or a device, an unreadable file, a directory the file cannot be made in, something other than a directory on the way) fails at delivery instead; and a delivery to such a path fails. What a relay killed during creation left under such a name (an empty file, or a second link to the live file) is removed at every delivery into that directory once that pid is gone as seen by the relay's user, and only while the name is still an empty regular file or one with another link; anything else under the pattern is left alone. The judge's directory probe mints such a name too, in the nearest existing directory on the way to the wake file: one left in the wake file's own directory is swept the same way once its pid is gone, one left higher up stays until removed by hand, and an unlink that fails is logged with the file's path. The path must be a regular file and not a symlink (a dangling one included), checked on the open descriptor. One wake file must be named by one path: symlinked directories on the way to it are resolved, but two bindings reaching the same file through different hard links are not serialized against each other. The directory is created `0700` only if it did not exist; an existing directory keeps its mode.
