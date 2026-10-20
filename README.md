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

The broker belongs on pluto, the machine that stays up. A relay keeps one WebSocket open. Addressing is a name such as `rea-63`, not a pid. A message for an offline name waits in the queue until that name registers again.

## What each part does

The broker accepts relays from loopback, RFC1918, and Tailscale (`100.64.0.0/10`). Every other address is dropped. The shared secret is `pass:agentmailbus/token` or `file:/path`. A raw token in the config is rejected. The token crosses the LAN in cleartext, the same posture as dsh-web on HTTP.

The relay registers each live session and delivers on the machine:

| Harness | Delivery |
| --- | --- |
| claude | cc-socks. Auth line, then a user line. The peer token is read from `~/.claude/sessions/`. |
| dsh | One JSON line on the inbox socket named in the binding. Confirm that frame on a live dsh before relying on it. |
| grok, orca | One JSON line appended to the wake file. A Grok session watches that file. There is no receive tool. |

After a message is delivered on the machine, the relay files it with `omind note`. That note is the record. The broker database is the queue, not the audit trail. Acked rows stay so a retried id is not injected twice.

## MCP

`agentmailbus mcp` speaks stdio MCP and exposes one tool, `send(to, text, priority)`. The sender name comes from `--from`. The model cannot supply a different sender.

Receiving is not a tool. Claude is injected through cc-socks. dsh is injected through its inbox socket. Grok is injected by a new line on the wake file.

Grok does not start a turn because a file grew. Point a `monitor` at the wake file:

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
pass insert agentmailbus/token
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

User units for both are in `deploy/`.

A Claude session:

```bash
agentmailbus mcp --from rea-63 --harness claude
```

`--pid` defaults to the parent process. Pass it when the parent is not the `claude` process that owns `/tmp/cc-socks/<pid>.sock`.

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

`agentmailbus who` lists names. `agentmailbus send --from grok-aima --to rea-63 --text "hello"` is the same call the tool makes.

## Development

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/python -m pytest
```

Python 3.12 or newer. The only runtime dependency is `websockets`.

## Not claimed yet

The dsh line format is this project's contract, not a captured dsh trace. Orca does not yet have a confirmed push socket, so Grok and Orca share the wake file. Fleet install is still ahead: the broker unit on pluto, a relay on each host, and the pass entry on each host.
