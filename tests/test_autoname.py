"""Automatic names for Claude sessions started without --from."""

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

from agentmailbus import autoname
from agentmailbus.protocol import NAME


SID = "54e558bf-06a8-4dda-8c16-0c5cdb54028b"
LIVE_CLI = {"entrypoint": "cli", "sessionId": SID}


def test_session_name_is_tagged_with_the_session_id() -> None:
    assert autoname.auto_name(10, "ronin28", {}, {**LIVE_CLI, "name": "rea-4b"}) == "rea-4b-54e558bf.ronin28"


def test_without_a_session_id_the_pid_tags_it() -> None:
    assert autoname.auto_name(10, "ronin28", {}, {"name": "rea-4b"}) == "rea-4b-10.ronin28"


def test_fallback_without_a_usable_name() -> None:
    for session in ({}, {"name": ""}, {"name": "///"}, {"name": True}, {"name": None}):
        assert autoname.auto_name(10, "ronin28", {}, session) == "claude-10.ronin28"


def test_odd_characters_become_dashes() -> None:
    assert autoname.auto_name(10, "h", {}, {"name": "-my session/ü x"}) == "my-session---x-10.h"


def test_long_names_keep_the_tag_and_the_host() -> None:
    name = autoname.auto_name(10, "ronin28", {}, {**LIVE_CLI, "name": "a" * 90})
    assert len(name) == 64 and name.endswith("-54e558bf.ronin28")
    long_host = "h" * 64
    assert NAME.fullmatch(autoname.auto_name(10, long_host, {}, {"name": "rea"}))


def test_the_tag_survives_any_host_length() -> None:
    for n in range(0, 71):
        host = "h" * n
        a = autoname.auto_name(10, host, {}, {"name": "x" * 70, "sessionId": "aaaaaaaa-0"})
        b = autoname.auto_name(10, host, {}, {"name": "x" * 70, "sessionId": "aaaabbbb-0"})
        assert a != b and "-aaaaaaaa" in a and NAME.fullmatch(a) and NAME.fullmatch(b)
        held = {a: {"name": a, "pid": 99}}
        c = autoname.auto_name(10, host, held, {"name": "x" * 70, "sessionId": "aaaaaaaa-0"})
        assert c != a and NAME.fullmatch(c)


def test_every_result_is_a_valid_name() -> None:
    for raw in ["", ".", "..x", "é" * 70, "a.", "x" * 64, "-_."]:
        for host in ["", "h", "H.Q", "x" * 70, "."]:
            for sid in (None, SID, "", "!!!"):
                session = {"name": raw, "sessionId": sid}
                assert NAME.fullmatch(autoname.auto_name(7, host, {}, session))


def test_a_name_held_by_another_live_session_gets_the_pid_even_when_long() -> None:
    session = {**LIVE_CLI, "name": "a" * 62}
    first = autoname.auto_name(10, "h", {}, session)
    live = {first: {"name": first, "pid": 99}}
    second = autoname.auto_name(10, "h", live, session)
    assert second != first and second.endswith("-54e558bf-10.h") and len(second) <= 64
    mine = {first: {"name": first, "pid": 10}}
    assert autoname.auto_name(10, "h", mine, session) == first


def test_only_interactive_records_join() -> None:
    assert autoname.interactive({"entrypoint": "cli"})
    assert autoname.interactive({"entrypoint": "claude-vscode"})
    assert not autoname.interactive({"entrypoint": "sdk-cli"})
    assert not autoname.interactive({"entrypoint": "sdk-ts"})
    assert not autoname.interactive({})
    assert not autoname.interactive({"entrypoint": ""})


def test_read_session_checks_the_pid_and_waits(tmp_path: Path) -> None:
    (tmp_path / "5.json").write_text(json.dumps({"pid": 6, "name": "stale", "entrypoint": "cli"}))
    started = time.monotonic()
    assert autoname.read_session(5, wait=0.2, directory=tmp_path) == {}
    assert time.monotonic() - started >= 0.2
    (tmp_path / "5.json").write_text(json.dumps({"pid": 5, "name": "ok", "entrypoint": "cli"}))
    assert autoname.read_session(5, wait=0, directory=tmp_path)["name"] == "ok"
    (tmp_path / "7.json").write_text("{not json")
    assert autoname.read_session(7, wait=0, directory=tmp_path) == {}


def test_host_matches_the_relay(tmp_path: Path) -> None:
    cfg = tmp_path / "relay.toml"
    cfg.write_text("host = 'dl380'\n[extra]\nhost = \"other\"\n")
    assert autoname.config_host(cfg) == "dl380"
    cfg.write_text('broker = "ws://x"\n')
    assert autoname.config_host(cfg) == socket.gethostname().split(".")[0]
    assert autoname.config_host(tmp_path / "missing.toml") == socket.gethostname().split(".")[0]


def test_socket_from_session() -> None:
    assert autoname.claude_socket_from({"messagingSocketPath": "/run/user/1/cc-socks/5.sock"}) == "/run/user/1/cc-socks/5.sock"
    assert autoname.claude_socket_from({"messagingSocketPath": "rel.sock"}) == ""
    assert autoname.claude_socket_from({}) == ""


def _run_mcp(tmp_path: Path, session: dict | None, env_name: str = "", stdin: bytes = b""):
    home = tmp_path / "home"
    sessions = home / ".claude" / "sessions"
    sessions.mkdir(parents=True)
    pid = os.getpid()  # the child's --pid; the session record is keyed by it
    if session is not None:
        (sessions / f"{pid}.json").write_text(json.dumps({"pid": pid, **session}))
    cfg = tmp_path / "relay.toml"
    cfg.write_text('host = "testhost"\n')
    env = {**os.environ, "HOME": str(home)}
    env.pop("AGENTMAILBUS_NAME", None)
    if env_name:
        env["AGENTMAILBUS_NAME"] = env_name
    return subprocess.run(
        [
            sys.executable, "-m", "agentmailbus", "mcp", "--harness", "claude",
            "--pid", str(pid), "--relay-config", str(cfg),
            "--registry", str(tmp_path / "reg"), "--relay-socket", str(tmp_path / "none.sock"),
        ],
        input=stdin, capture_output=True, env=env, timeout=30, check=False,
    )


LIST = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}).encode() + b"\n"


def test_mcp_binds_the_automatic_name_and_says_it(tmp_path: Path) -> None:
    out = _run_mcp(tmp_path, {"name": "rea-4b", **LIVE_CLI}, stdin=LIST)
    assert out.returncode == 0, out.stderr
    assert b"bound as rea-4b-54e558bf.testhost" in out.stderr
    tools = json.loads(out.stdout.splitlines()[0])["result"]["tools"]
    assert tools[0]["description"].startswith("Your name on the bus is rea-4b-54e558bf.testhost.")


def test_mcp_env_name_wins_for_an_interactive_session(tmp_path: Path) -> None:
    out = _run_mcp(tmp_path, {"name": "rea-4b", **LIVE_CLI}, env_name="fixed-1")
    assert out.returncode == 0, out.stderr
    assert b"bound as fixed-1" in out.stderr


def test_mcp_bad_env_name_fails_loudly(tmp_path: Path) -> None:
    out = _run_mcp(tmp_path, {"name": "rea-4b", **LIVE_CLI}, env_name="bad name")
    assert out.returncode != 0
    assert b"bad-name" in out.stderr


def test_mcp_scripted_or_unknown_sessions_stay_off_the_bus(tmp_path: Path) -> None:
    call = json.dumps({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "send"}}).encode() + b"\n"
    cases = (
        ({"name": "rea-4b", "entrypoint": "sdk-cli"}, ""),
        ({"name": "rea-4b", "entrypoint": "sdk-cli"}, "inherited-name"),
        ({"name": "rea-4b"}, ""),
    )
    for n, (session, env_name) in enumerate(cases):
        base = tmp_path / str(n)
        base.mkdir()
        out = _run_mcp(base, session, env_name=env_name, stdin=LIST + call)
        assert out.returncode == 0, out.stderr
        assert b"bound as" not in out.stderr and b"not joining the bus" in out.stderr
        replies = [json.loads(line) for line in out.stdout.splitlines()]
        assert replies[0]["result"]["tools"] == []
        assert replies[1]["result"]["isError"] is True
        assert replies[1]["result"]["content"][0]["text"] == "unknown-tool"
        assert not list((base / "reg").glob("*.json"))


def test_a_scripted_session_never_touches_the_registry(tmp_path: Path) -> None:
    blocked = tmp_path / "ro"
    blocked.mkdir()
    blocked.chmod(0o500)
    home = tmp_path / "home"
    (home / ".claude" / "sessions").mkdir(parents=True)
    pid = os.getpid()
    (home / ".claude" / "sessions" / f"{pid}.json").write_text(json.dumps({"pid": pid, "entrypoint": "sdk-cli"}))
    out = subprocess.run(
        [sys.executable, "-m", "agentmailbus", "mcp", "--harness", "claude", "--pid", str(pid),
         "--registry", str(blocked / "reg"), "--relay-socket", str(tmp_path / "none.sock")],
        input=LIST, capture_output=True, env={**os.environ, "HOME": str(home)}, timeout=30, check=False,
    )
    assert out.returncode == 0, out.stderr
    assert json.loads(out.stdout.splitlines()[0])["result"]["tools"] == []


def test_sigterm_removes_the_binding(tmp_path: Path) -> None:
    import signal as sig

    home = tmp_path / "home"
    (home / ".claude" / "sessions").mkdir(parents=True)
    pid = os.getpid()
    (home / ".claude" / "sessions" / f"{pid}.json").write_text(json.dumps({"pid": pid, "name": "t", **LIVE_CLI}))
    reg = tmp_path / "reg"
    proc = subprocess.Popen(
        [sys.executable, "-m", "agentmailbus", "mcp", "--harness", "claude", "--pid", str(pid),
         "--registry", str(reg), "--relay-socket", str(tmp_path / "none.sock"),
         "--relay-config", str(tmp_path / "none.toml")],
        stdin=subprocess.PIPE, stderr=subprocess.PIPE, env={**os.environ, "HOME": str(home)},
    )
    for _ in range(100):
        if list(reg.glob("*.json")):
            break
        time.sleep(0.05)
    assert list(reg.glob("*.json"))
    proc.send_signal(sig.SIGTERM)
    proc.wait(timeout=10)
    assert not list(reg.glob("*.json"))


def test_mcp_without_a_record_waits_then_stays_off(tmp_path: Path) -> None:
    out = _run_mcp(tmp_path, None)
    assert out.returncode == 0, out.stderr
    assert b"not joining the bus" in out.stderr


def test_a_malformed_registry_file_does_not_stop_startup(tmp_path: Path) -> None:
    reg = tmp_path / "reg"
    reg.mkdir()
    (reg / "x.json").write_text('{"name":"x","harness":"claude","pid":1e999}')
    out = _run_mcp(tmp_path, {"name": "rea-4b", **LIVE_CLI})
    assert out.returncode == 0, out.stderr
    assert b"bound as rea-4b-54e558bf.testhost" in out.stderr


def test_inherited_env_name_is_not_taken_from_a_live_holder(tmp_path: Path) -> None:
    cfg = tmp_path / "relay.toml"
    cfg.write_text('host = "h"\n')
    live = {"rea-63": {"name": "rea-63", "pid": 500}}
    # The holder itself keeps it.
    assert autoname.pick(500, LIVE_CLI, "rea-63", live, cfg) == "rea-63"
    # A nested interactive session falls through to its own name.
    assert autoname.pick(501, {**LIVE_CLI, "name": "kid"}, "rea-63", live, cfg) == "kid-54e558bf.h"
    # A scripted child stays off the bus, held or not.
    assert autoname.pick(502, {"entrypoint": "sdk-cli"}, "rea-63", live, cfg) is None
    assert autoname.pick(502, {"entrypoint": "sdk-cli"}, "rea-63", {}, cfg) is None


def test_record_from_an_earlier_process_with_the_same_pid_is_ignored(tmp_path: Path) -> None:
    pid = os.getpid()
    stat = Path(f"/proc/{pid}/stat")
    if not stat.exists():
        return  # the check is Linux only
    s = stat.read_text()
    start = s[s.rfind(")") + 2 :].split()[19]
    (tmp_path / f"{pid}.json").write_text(json.dumps({"pid": pid, "name": "old", "entrypoint": "cli", "procStart": str(int(start) - 1)}))
    assert autoname.read_session(pid, wait=0, directory=tmp_path) == {}
    for value in (start, int(start)):  # Claude Code writes a string; an int is fine too
        (tmp_path / f"{pid}.json").write_text(json.dumps({"pid": pid, "name": "now", "entrypoint": "cli", "procStart": value}))
        assert autoname.read_session(pid, wait=0, directory=tmp_path)["name"] == "now"


def test_two_sessions_saving_at_once_do_not_collide(tmp_path: Path) -> None:
    from concurrent.futures import ThreadPoolExecutor

    from agentmailbus.registry import save_binding

    reg = tmp_path / "reg"

    def save(pid: int) -> None:
        with autoname.registry_lock(reg):
            save_binding(reg, {"name": "same", "harness": "claude", "pid": pid, "wake": "", "socket": ""})

    with ThreadPoolExecutor(8) as pool:
        list(pool.map(save, range(1, 50)))
    assert sorted(p.name for p in reg.iterdir()) == [".lock", "same.json"]


def test_every_collision_candidate_is_checked() -> None:
    session = {"name": "rea", "sessionId": SID}
    live = {
        "rea-54e558bf.h": {"pid": 99},
        "rea-54e558bf-10.h": {"pid": 88},
    }
    assert autoname.auto_name(10, "h", live, session) == "rea-54e558bf-10-2.h"


def test_a_registry_file_with_a_non_string_socket_is_skipped(tmp_path: Path) -> None:
    from agentmailbus.registry import load_live

    (tmp_path / "x.json").write_text(json.dumps({"name": "x", "harness": "claude", "pid": os.getpid(), "socket": 10**100}))
    (tmp_path / "y.json").write_text(json.dumps({"name": "y", "harness": "dsh", "pid": 0, "socket": 7}))
    assert load_live(tmp_path) == {}


def test_an_old_server_does_not_remove_its_replacements_file(tmp_path: Path) -> None:
    from agentmailbus.registry import remove_binding, save_binding

    old = {"name": "s", "harness": "claude", "pid": 5, "wake": "", "socket": "", "server": 100}
    new = {**old, "server": 200}
    path = save_binding(tmp_path, dict(new))
    assert remove_binding(path, old) is False and path.exists()
    assert remove_binding(path, new) is True and not path.exists()


def test_dead_claude_bindings_and_temp_files_are_swept(tmp_path: Path) -> None:
    dead = 2**22 + 12345  # above the default pid_max, so never a live process
    (tmp_path / "gone.json").write_text(json.dumps({"name": "gone", "harness": "claude", "pid": dead}))
    (tmp_path / "here.json").write_text(json.dumps({"name": "here", "harness": "claude", "pid": os.getpid()}))
    (tmp_path / "grok.json").write_text(json.dumps({"name": "grok", "harness": "grok", "pid": dead}))
    (tmp_path / f".here.json.{dead}.tmp").write_text("{}")
    (tmp_path / f".here.json.{os.getpid()}.tmp").write_text("{}")
    assert autoname.sweep_dead(tmp_path) == 2
    assert sorted(p.name for p in tmp_path.iterdir()) == sorted(
        ["here.json", "grok.json", f".here.json.{os.getpid()}.tmp"]
    )


def test_the_lock_gives_up_after_its_timeout(tmp_path: Path) -> None:
    with autoname.registry_lock(tmp_path / "reg"):
        held = subprocess.run(
            [sys.executable, "-c",
             "import sys; from pathlib import Path; from agentmailbus import autoname\n"
             "try:\n"
             f"    autoname.registry_lock(Path({str(tmp_path / 'reg')!r}), timeout=0.3).__enter__()\n"
             "except TimeoutError:\n"
             "    sys.exit(7)\n"],
            capture_output=True, timeout=30, check=False,
        )
    assert held.returncode == 7, held.stderr
    with autoname.registry_lock(tmp_path / "reg", timeout=0.3):
        pass


def test_claude_config_dir_is_honoured(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "cfg"))
    assert autoname.sessions_dir() == tmp_path / "cfg" / "sessions"
    monkeypatch.delenv("CLAUDE_CONFIG_DIR")
    assert autoname.sessions_dir() == Path.home() / ".claude" / "sessions"


def test_sighup_and_sigterm_remove_the_binding_on_the_from_path_too(tmp_path: Path) -> None:
    import signal as sig

    for n, signum in enumerate((sig.SIGTERM, sig.SIGHUP)):
        reg = tmp_path / f"reg{n}"
        proc = subprocess.Popen(
            [sys.executable, "-m", "agentmailbus", "mcp", "--from", f"fixed-{n}", "--harness", "claude",
             "--pid", str(os.getpid()), "--registry", str(reg), "--relay-socket", str(tmp_path / "none.sock")],
            stdin=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        for _ in range(100):
            if list(reg.glob("*.json")):
                break
            time.sleep(0.05)
        assert list(reg.glob("*.json"))
        proc.send_signal(signum)
        proc.wait(timeout=10)
        assert not list(reg.glob("*.json")), signum


def test_a_binding_whose_pid_was_reused_is_not_live(tmp_path: Path) -> None:
    from agentmailbus.registry import is_live

    pid = os.getpid()
    stat = Path(f"/proc/{pid}/stat")
    if not stat.exists():
        return  # Linux only
    s = stat.read_text()
    start = s[s.rfind(")") + 2 :].split()[19]
    sock = tmp_path / "s.sock"
    sock.write_text("")
    base = {"name": "x", "harness": "claude", "pid": pid, "socket": str(sock)}
    assert is_live({**base, "proc_start": start})
    assert is_live(base)
    assert not is_live({**base, "proc_start": str(int(start) + 1)})


def test_peer_token_is_read_from_the_sessions_dir_in_the_binding(tmp_path: Path) -> None:
    import hashlib

    from agentmailbus.adapters import peer_token

    sock = "/run/user/1/cc-socks/5.sock"
    digest = hashlib.sha256(sock.encode()).hexdigest()
    (tmp_path / f"5.{digest}.key").write_text(json.dumps({"peerToken": "abc"}))
    assert peer_token(sock, 5, str(tmp_path)) == "abc"
    assert peer_token(sock, 5, "relative/dir") != "abc"


def test_empty_from_is_refused(tmp_path: Path) -> None:
    out = subprocess.run(
        [sys.executable, "-m", "agentmailbus", "mcp", "--from", "", "--harness", "claude",
         "--registry", str(tmp_path / "reg"), "--relay-socket", str(tmp_path / "none.sock")],
        capture_output=True, timeout=30, check=False,
    )
    assert out.returncode != 0 and b"bad-name" in out.stderr


def test_a_cut_host_does_not_end_in_punctuation() -> None:
    host = "a" * 51 + ".bbbb"
    name = autoname.auto_name(10, host, {}, {"name": "x", "sessionId": SID})
    assert not name.endswith(".") and NAME.fullmatch(name)


def test_sweep_removes_a_binding_whose_pid_was_reused(tmp_path: Path) -> None:
    pid = os.getpid()
    stat = Path(f"/proc/{pid}/stat")
    if not stat.exists():
        return  # Linux only
    s = stat.read_text()
    start = s[s.rfind(")") + 2 :].split()[19]
    (tmp_path / "old.json").write_text(json.dumps({"name": "old", "harness": "claude", "pid": pid, "proc_start": str(int(start) + 1)}))
    (tmp_path / "now.json").write_text(json.dumps({"name": "now", "harness": "claude", "pid": pid, "proc_start": start}))
    assert autoname.sweep_dead(tmp_path) == 1
    assert [p.name for p in tmp_path.glob("*.json")] == ["now.json"]


def test_a_relative_claude_config_dir_is_made_absolute(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", "cfg")
    assert autoname.sessions_dir() == tmp_path / "cfg" / "sessions"


def test_a_filesystem_that_cannot_lock_goes_ahead_unlocked(tmp_path: Path, monkeypatch) -> None:
    import errno
    import fcntl

    def refuse(fd, op):
        raise OSError(errno.ENOLCK, "no locks")

    monkeypatch.setattr(fcntl, "flock", refuse)
    with autoname.registry_lock(tmp_path / "reg") as lock:
        assert lock.locked is False


def test_an_explicit_save_stops_when_the_lock_stays_busy(tmp_path: Path, monkeypatch) -> None:
    from agentmailbus import cli

    class Busy:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            raise TimeoutError("registry lock")

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(autoname, "registry_lock", Busy)
    binding = {"name": "x", "harness": "claude", "pid": 1, "wake": "", "socket": ""}
    try:
        cli._save_locked(tmp_path / "reg", binding)
    except SystemExit as exc:
        assert "locked" in str(exc)
    else:
        raise AssertionError("saved without the lock")
    assert not (tmp_path / "reg" / "x.json").exists()


def test_cleanup_finishes_even_when_the_lock_file_cannot_be_opened(tmp_path: Path) -> None:
    from agentmailbus import cli
    from agentmailbus.registry import save_binding

    reg = tmp_path / "reg"
    binding = {"name": "x", "harness": "claude", "pid": 1, "wake": "", "socket": "", "server": 9}
    path = save_binding(reg, dict(binding))
    (reg / ".lock").mkdir()
    cli._remove_locked(reg, path, binding)
    assert not path.exists()


def test_a_record_without_an_entrypoint_is_not_yet_finished(tmp_path: Path) -> None:
    (tmp_path / "5.json").write_text(json.dumps({"pid": 5, "name": "skeleton"}))
    assert autoname.read_session(5, wait=0, directory=tmp_path) == {}


def test_an_unreadable_start_time_is_not_taken_as_a_reused_pid() -> None:
    from agentmailbus.registry import started_elsewhen

    if not Path(f"/proc/{os.getpid()}/stat").exists():
        return
    for odd in ("", "abc", "1234567.0"):
        assert started_elsewhen(os.getpid(), odd) is False
