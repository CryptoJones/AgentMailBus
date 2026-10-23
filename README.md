# AgentMailBus

A named agent on one machine can message a named agent on another. One broker holds the queue. Each host runs one relay. MCP is only the send path.

```
claude rea-63                         grok grok-aima
   cc-socks                                wake file
      |                                       |
 relay on ronin28                      relay on makemake
      \                                     /
       \____________ WebSocket ____________/
                        |
                 broker on pluto
                 sqlite queue
```

The broker belongs on pluto, the machine that stays up. A relay keeps one WebSocket open. Addressing is a name such as `rea-63`, not a pid. A message for an offline name waits in the queue until that name registers again, and comes out in the order it was sent.

## What each part does

The broker accepts relays from loopback, RFC1918, and Tailscale (`100.64.0.0/10`). Every other address is dropped. The shared secret is `pass:agentmailbus/token` or `file:/path`. A raw token in the config is rejected.

One token, one level of trust. Whoever holds it can register any name that is not currently online, send as that name, and read the directory. The token and every message cross the LAN in cleartext on `ws://`. That is the same posture as dsh-web on HTTP, with one difference worth saying out loud: what arrives here is injected into agent sessions as a prompt. Keep the token off hosts you do not trust, and keep the bus on a network you do.

The relay registers each live session and delivers on the machine:

| Harness | Delivery |
| --- | --- |
| claude | cc-socks. Auth line, then a user line. The peer token is read from the session's sessions directory (`~/.claude/sessions/`, or under `CLAUDE_CONFIG_DIR`). |
| dsh | One JSON line on the inbox socket named in the binding. Confirm that frame on a live dsh before relying on it. |
| opencode | One JSON line on the socket of the opencode plugin (`deploy/opencode/agentmailbus.js`), which puts the message into the session as a prompt. |
| grok, orca | One JSON line appended to the wake file. A Grok session watches that file. There is no receive tool. |

None of these sockets answer, so "delivered" means the bytes were written. PROTOCOL.md spells out what an ack does and does not promise. A delivery that fails is retried with backoff and given up after 20 attempts; `agentmailbus who` shows what is still pending and how many messages were given up on. One message is in flight per name at a time and the next goes out on its ack, so a fresh message never overtakes an older one for the same recipient; one that keeps failing holds the ones behind it until it succeeds or is given up on.

The broker database is the queue, not an archive: finished rows are deleted after 7 days. It also holds the secret that keeps a broadcast's copy ids the same across retries; a new database makes a new one, so a retry of an older broadcast would be delivered again. With `omi = true` in the relay config, each delivered message is also filed with `omind note`. That is off by default, because it writes whatever any token holder sent into the vault.

## MCP

`agentmailbus mcp` speaks stdio MCP and exposes two tools, `send(to, text, priority, id)` and `broadcast(text, priority, id)`. The sender name comes from `--from`, or for Claude and opencode from the session itself (see below); a Claude session that stays off the bus gets no tools. The model cannot supply a different sender through the tool. Any process running as the same user can, through the relay socket or `agentmailbus send`.

Receiving is not a tool. Claude is injected through cc-socks. dsh is injected through its inbox socket. opencode is injected by its plugin. Grok is injected by a new line on the wake file.

Grok does not start a turn because a file grew. Start a `monitor` on the wake file before the session's MCP server registers, or the backlog that is flushed at registration is already in the file when the monitor starts:

```bash
tail -n 0 -F ~/.local/share/agentmailbus/inbox/grok-aima.jsonl
```

## Run

```bash
install -d -m 700 ~/.local/share/agentmailbus
python3 -m venv ~/.local/share/agentmailbus/venv
~/.local/share/agentmailbus/venv/bin/pip install -e .
```

The data directory is made first so it is private; a directory that already exists keeps whatever mode it has.

Create the token once, then put the same value on every host. Do not commit it.

```bash
pass generate agentmailbus/token 32          # on the first host
pass show agentmailbus/token | pass insert -e agentmailbus/token   # on each other host, pasted
```

Broker, on pluto:

```bash
mkdir -p ~/.config/agentmailbus
cp examples/broker.toml ~/.config/agentmailbus/broker.toml
agentmailbus broker --config ~/.config/agentmailbus/broker.toml
```

Relay, on every host including pluto:

```bash
cp examples/relay.toml ~/.config/agentmailbus/relay.toml
agentmailbus relay --config ~/.config/agentmailbus/relay.toml
```

User units for both are in `deploy/`. Two things about running them under systemd:

- `pass show` needs an unlocked gpg-agent, which a user service started at boot does not have. Under systemd use `token = "file:~/.config/agentmailbus/token"` (mode 0600) instead, or make sure the agent is unlocked before the unit starts.
- User units start at login and stop at logout unless you run `loginctl enable-linger $USER` once on that host.

A Claude session:

```bash
agentmailbus mcp --from rea-63 --harness claude
```

`--pid` defaults to the parent process. Pass it when the parent is not the `claude` process that owns the session's cc-socks socket (`$XDG_RUNTIME_DIR/cc-socks/<pid>.sock`, or under the temporary directory).

The name is per session, not per project. Two Claude sessions started from the same MCP config with `--from rea-63` would both claim `rea-63`; the later one takes the name and the earlier one's sends go out under it.

For Claude, leave `--from` out and register the server once per host at user scope. Every new interactive Claude session on the host then joins the bus under a name of its own:

```bash
claude mcp add -s user agentmailbus -- ~/.local/share/agentmailbus/venv/bin/agentmailbus mcp --harness claude
```

The name is picked in this order:

1. `AGENTMAILBUS_NAME`, when it is set and not empty: `AGENTMAILBUS_NAME=rea-63 claude`. It must already be a valid name; a bad one stops the server with `bad-name`. Every process the session starts inherits the variable, so when another live session on this host already holds that name, it is not taken and the next rule applies.
2. The name Claude Code gives the session, a dash, the first eight characters of its session id, a dot, and the relay's host, for example `rea-4b-54e558bf.ronin28`. Claude Code's own names repeat (a folder name and two hex digits), and the broker keeps mail for any name it has seen, so the session id keeps a new session from receiving an old one's mail. Eight hex digits make that very unlikely, not impossible. A resumed session has the same id, and so the same name unless the session was renamed.

The host is the one the relay announces: `host` in `~/.config/agentmailbus/relay.toml` (`--relay-config` to read another file), or the short host name when that is unset or the file cannot be read. So two hosts with a session of the same name do not collide (unless their host names agree in the first 50 or so characters); the broker refuses a name another relay holds online. On one host, the name is chosen and saved under a lock (on a filesystem that cannot lock, without one). Characters a name cannot hold become `-`, leading `.`, `_` and `-` are dropped, and the result is cut to 64 characters: the Claude name is cut first, then a long host (over about 50 characters), and the session id is always kept. A name another live session on this host already holds gets the pid added, then a counter.

Each session's tools start with "Your name on the bus is …", so the agent can tell others where to reply. The name is taken when the session starts and does not follow a later rename. Since an automatic name is new for every session id, give an agent that others write to regularly a fixed name with `AGENTMAILBUS_NAME`.

Only an interactive session joins. Claude Code writes a record for each session in `~/.claude/sessions/<pid>.json` (under `CLAUDE_CONFIG_DIR` when that is set); the server waits up to three seconds for it, and when there is none, or it says the session was started by a script (`claude -p` and the Agent SDK record an `sdk-…` entrypoint), the server answers with no tools and binds no name, and logs why. That holds even when `AGENTMAILBUS_NAME` is set, since a script inherits it from the session that started it. Claude Code records `sdk-cli` for a `claude -p` started from a terminal session; one started from inside an IDE or desktop session keeps that session's entrypoint, and so joins. A script that should be on the bus passes `--from` in its own MCP config. `--from` always wins and skips all of these checks.

`AGENTMAILBUS_NAME` applies only to Claude and opencode; the other harnesses take `--from`.

The server removes its binding when it exits, on SIGTERM and SIGHUP too. A binding left by a session that was killed is removed the next time an automatically named Claude or opencode session on the host starts. On Linux, an automatic binding (and a session record) whose pid now belongs to another process counts as dead. If an older config also starts `agentmailbus mcp --from …` for the same sessions, remove it, or each session joins twice under two names; one registered at user scope under the same name has to be removed first (`claude mcp remove -s user agentmailbus`).

Claude Code starts MCP servers as its direct children, and the server takes its parent pid as the session. Register the command itself, not a shell or launcher that stays in between.

An opencode session needs two pieces, both once per host. The plugin receives; the MCP server names the session and sends. Copy the plugin into opencode's plugin directory:

```bash
cp deploy/opencode/agentmailbus.js ~/.config/opencode/plugins/
```

and add the server to `~/.config/opencode/opencode.json` (or `.jsonc`), with the absolute path to `agentmailbus`:

```json
{
  "mcp": {
    "agentmailbus": {
      "type": "local",
      "command": ["/home/you/.local/share/agentmailbus/venv/bin/agentmailbus", "mcp", "--harness", "opencode"]
    }
  }
}
```

Restart opencode. Each project folder an opencode process has open then joins under `AGENTMAILBUS_NAME` when that is set and no other running session or folder holds it, and otherwise under the folder's name, `oc` and eight hex digits made from the process (its pid and start time) and the folder, and the relay's host: `rea-oc1a2b3c4d.ronin28`. A later opencode that happens to reuse the pid gets another name, so it does not receive mail queued for the old one; the same holds for every restart, so give a folder that others write to regularly a fixed name with `AGENTMAILBUS_NAME`, set in the `environment` of the server's entry in the project's opencode config. A second opencode on the same project then takes an automatic name instead; a `--from` there would bind both windows to one name, and the first to close would take it off the bus. The broker must run this version before an opencode name can come online; an older one refuses it with `bad-harness`.

A message goes to the session a person last sent a prompt to (messages from the bus do not count); before that, to the most recently updated session for the folder, which may be yesterday's and not the one on screen; with none, the plugin makes one called "AgentMailBus". A subagent's session is never the target. A message that arrives while the session is working waits for it, as a typed prompt does; `priority` has no effect. The model then runs a turn in that session with that session's permissions, whether or not anyone is looking at it.

The plugin listens on `<data dir>/opencode/<pid>-<folder key>.sock`, where the data dir is `$XDG_DATA_HOME/agentmailbus` (`~/.local/share/agentmailbus` when that is unset, empty, or not an absolute path without `..` after a leading `~/` is expanded, on both sides; this rule now holds for the broker and relay too, so an install that set such a value finds its state under `~/.local/share/agentmailbus`) and the folder key is the first 12 hex digits of SHA-256 of the folder's real path. The directory must be its user's own and closed to everyone else, or the plugin does not listen. opencode starts the MCP server in the project's folder, as its child, so the server takes its parent pid and its working folder and binds the same path (register the command itself, not a shell or launcher that stays in between); the name is online once both are running. A name stays taken while its opencode process runs, even before the plugin listens. A scripted `opencode run` joins too, for as long as it runs. A socket left by an opencode that was killed is removed the next time an automatically named opencode session on the host starts. `--from` takes the same socket path for the folder it runs in; if you use it at all, put it in a project's opencode config, never the global one, where every folder would join under one name; `agentmailbus bind --harness opencode` takes it too, run in the project's folder and given the opencode process with `--pid`. The socket path must fit a Unix socket (103 bytes), which holds for a home directory up to about 40 characters; with a longer one, set a shorter `XDG_DATA_HOME`. Tested with opencode 1.19.3. A folder whose name is not valid UTF-8 is handled as the plugin sees it under Node; under opencode's Bun runtime that is untested. A message the plugin cannot put into a session (opencode refuses it) is logged in opencode's output and lost: the relay counted it delivered once the line was written.

A Grok session:

```bash
agentmailbus mcp --from grok-aima --harness grok \
  --wake ~/.local/share/agentmailbus/inbox/grok-aima.jsonl
```

The MCP process pid is the liveness check. When the session exits, the name goes offline and further mail waits on the broker.

A dsh session:

```bash
agentmailbus mcp --from dsh-main --harness dsh --socket /path/to/inbox.sock
```

`agentmailbus who` lists names. `agentmailbus send --from grok-aima --to rea-63 --text "hello"` is the same call the tool makes; add `--id <uuid>` to retry a send as the same message. The MCP tool does the same: a failed call names the id it used, and a retry with that id is not a second message. A refused send exits non-zero, prints nothing on stdout, and says on stderr what to do: the error code comes first, then, where there is one, the id and whether to retry with it, and, where the code alone does not say, what happened and a hint. A send from a name that is not live on this host fails with `sender-offline` before that attempt sends the message; bind the name (`agentmailbus bind`) and retry with the same id. A name the broker refuses, for instance because a connected relay on another host holds it, fails with `not-registered` and the broker's reason. `accepted`, or `queued` when the recipient cannot take it yet, is printed only when the broker took the message.

`agentmailbus broadcast --from grok-aima --text "hello"` sends one message to every agent online right now except the sender, and prints the names a copy is going to. An agent that is offline does not get it. Each recipient gets an ordinary copy marked with the broadcast id (`broadcast="<id>"` on the Claude wrapper, `"broadcast": "<id>"` in a wake line or dsh frame), so it can tell the message went to everyone and not only to it. Copies are delivered, retried, and given up on one by one, like any other message. One send makes at most 1,000 new copies; with more names than that online and without a copy it is refused and nothing new is sent. Retry with the same id once fewer are online; a first send that was refused reserved nothing, so its retry goes out with the retry's text. `agentmailbus send` takes a name only and refuses `*`.

A name the broker has never seen is `unknown-agent`. The recipient has to have registered at least once before mail can wait for it.

## Development

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/python -m pytest
```

Python 3.12 or newer. The only runtime dependency is `websockets`.

## Not claimed yet

The dsh line format is this project's contract, not a captured dsh trace. Orca does not yet have a confirmed push socket, so Grok and Orca share the wake file. There is no `wss://` and no per-host identity; see PROTOCOL.md.
