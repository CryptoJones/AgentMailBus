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
| claude | cc-socks. Auth line, then a user line. The peer token is read from `~/.claude/sessions/`. |
| dsh | One JSON line on the inbox socket named in the binding. Confirm that frame on a live dsh before relying on it. |
| grok, orca | One JSON line appended to the wake file. A Grok session watches that file. There is no receive tool. |

None of these sockets answer, so "delivered" means the bytes were written. PROTOCOL.md spells out what an ack does and does not promise. A delivery that fails is retried with backoff and given up after 20 attempts; `agentmailbus who` shows what is still pending.

The broker database is the queue, not an archive: finished rows are deleted after 7 days. With `omi = true` in the relay config, each delivered message is also filed with `omind note`. That is off by default, because it writes whatever any token holder sent into the vault.

## MCP

`agentmailbus mcp` speaks stdio MCP and exposes one tool, `send(to, text, priority)`. The sender name comes from `--from`. The model cannot supply a different sender through the tool. Any process running as the same user can, through the relay socket or `agentmailbus send`.

Receiving is not a tool. Claude is injected through cc-socks. dsh is injected through its inbox socket. Grok is injected by a new line on the wake file.

Grok does not start a turn because a file grew. Start a `monitor` on the wake file before the session's MCP server registers, or the backlog that is flushed at registration is already in the file when the monitor starts:

```bash
tail -n 0 -F ~/.local/share/agentmailbus/inbox/grok-aima.jsonl
```

## Run

```bash
python3 -m venv ~/.local/share/agentmailbus/venv
~/.local/share/agentmailbus/venv/bin/pip install -e .
```

Create the token on each host. Do not commit it.

```bash
pass generate agentmailbus/token 32
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

`--pid` defaults to the parent process. Pass it when the parent is not the `claude` process that owns `/tmp/cc-socks/<pid>.sock`.

The name is per session, not per project. Two Claude sessions started from the same MCP config would both claim `rea-63`; the later one takes the name and the earlier one's sends go out under it. Give each session its own name, for example through a per-session MCP config or a wrapper that fills in `--from`.

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

`agentmailbus who` lists names. `agentmailbus send --from grok-aima --to rea-63 --text "hello"` is the same call the tool makes; add `--id <uuid>` to retry a send as the same message.

A name the broker has never seen is `unknown-agent`. The recipient has to have registered at least once before mail can wait for it.

## Development

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/python -m pytest
```

Python 3.12 or newer. The only runtime dependency is `websockets`.

## Not claimed yet

The dsh line format is this project's contract, not a captured dsh trace. Orca does not yet have a confirmed push socket, so Grok and Orca share the wake file. There is no `wss://` and no per-host identity; see PROTOCOL.md. Fleet install is still ahead: the broker unit on pluto, a relay on each host, and the token on each host.
