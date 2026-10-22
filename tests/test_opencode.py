"""The opencode harness: naming, liveness, delivery, and the MCP server's binding."""

import json
import os
import socket
import subprocess
import sys
import threading
from pathlib import Path

from agentmailbus import autoname
from agentmailbus.adapters import default_adapters, deliver_opencode, opencode_frame
from agentmailbus.protocol import NAME, check_harness
from agentmailbus.registry import is_live, load_live, save_binding, start_time


def _expected_start():
    """What a binding of this process holds: clock ticks on Linux, `ps` text elsewhere."""
    ticks = start_time(os.getpid())
    return ticks if ticks is not None else (autoname.process_started(os.getpid()) or None)


_DATA: dict[Path, Path] = {}


def _data(tmp_path: Path) -> Path:
    """A data dir short enough for a Unix socket path; pytest's own are too long."""
    import atexit
    import shutil
    import tempfile

    if tmp_path not in _DATA:
        _DATA[tmp_path] = Path(tempfile.mkdtemp(prefix="amb", dir="/tmp"))
        atexit.register(shutil.rmtree, _DATA[tmp_path], True)
    return _DATA[tmp_path]


def _listen(path: Path) -> tuple[socket.socket, list[bytes]]:
    """A one-shot Unix listener that records what it reads."""
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(str(path))
    srv.listen(1)
    got: list[bytes] = []

    def run() -> None:
        conn, _ = srv.accept()
        with conn:
            data = b""
            while chunk := conn.recv(65536):
                data += chunk
            got.append(data)

    threading.Thread(target=run, daemon=True).start()
    return srv, got


def test_opencode_is_a_harness() -> None:
    assert check_harness("opencode") == "opencode"
    assert default_adapters()["opencode"] is deliver_opencode


def test_name_is_the_folder_and_a_tag_of_the_process(tmp_path: Path) -> None:
    import re

    cfg = tmp_path / "relay.toml"
    cfg.write_text('host = "ronin28"\n')
    rea = tmp_path / "rea"
    other = tmp_path / "other" / "rea"
    rea.mkdir()
    other.mkdir(parents=True)
    me = os.getpid()
    name = autoname.opencode_name(me, "", {}, cfg, str(rea))
    assert re.fullmatch(r"rea-oc[0-9a-f]{8}\.ronin28", name)
    assert autoname.opencode_name(me, "", {}, cfg, str(rea)) == name  # stable for this process
    assert autoname.opencode_name(me, "", {}, cfg, str(other)) != name  # another folder
    assert autoname.opencode_name(me + 1, "", {}, cfg, str(rea)) != name  # another process
    odd = tmp_path / "my repo ü"
    odd.mkdir()
    assert autoname.opencode_name(me, "", {}, cfg, str(odd)).startswith("my-repo-")
    assert NAME.fullmatch(autoname.opencode_name(me, "", {}, cfg, "/"))
    assert autoname.opencode_name(me, "", {}, cfg, "/").startswith("opencode-oc")
    long_dir = tmp_path / ("x" * 200)
    long_dir.mkdir()
    assert NAME.fullmatch(autoname.opencode_name(me, "", {}, cfg, str(long_dir)))


def test_env_name_wins_unless_another_session_holds_it(tmp_path: Path) -> None:
    cfg = tmp_path / "none.toml"
    a = tmp_path / "a"
    b = tmp_path / "b"
    a.mkdir()
    b.mkdir()
    sock_a = autoname.opencode_socket(5, str(a))
    assert autoname.opencode_name(5, "glm", {}, cfg, str(a)) == "glm"
    assert autoname.opencode_name(5, "glm", {"glm": {"pid": 5, "socket": sock_a}}, cfg, str(a)) == "glm"
    assert autoname.opencode_name(5, "glm", {"glm": {"pid": 6, "socket": sock_a}}, cfg, str(a)) != "glm"
    # One opencode process, two folders: the second folder does not take the first's name.
    assert autoname.opencode_name(5, "glm", {"glm": {"pid": 5, "socket": sock_a}}, cfg, str(b)) != "glm"


def test_a_claude_session_does_not_take_a_name_an_opencode_process_holds(tmp_path: Path) -> None:
    from agentmailbus.registry import load_held

    reg = tmp_path / "reg"
    me = os.getpid()
    save_binding(reg, {"name": "glm", "harness": "opencode", "pid": me, "wake": "", "socket": "/nowhere",
                       "proc_start": start_time(me)})
    session = {"entrypoint": "cli", "name": "x", "sessionId": "54e558bf-06a8"}
    assert autoname.pick(me + 1, session, "glm", load_held(reg), tmp_path / "none.toml") != "glm"


def test_a_claude_binding_with_a_text_start_time_survives_the_sweep(tmp_path: Path) -> None:
    # Claude Code on macOS records its start time as `ps` text in UTC.
    reg = tmp_path / "reg"
    save_binding(reg, {"name": "c", "harness": "claude", "pid": os.getpid(), "wake": "", "socket": "/x",
                       "proc_start": "Tue Oct 20 21:59:43 2026"})
    assert autoname.sweep_dead(reg) == 0


def test_ps_runs_in_one_locale_and_zone(monkeypatch) -> None:
    import subprocess as sp

    seen = {}

    def fake(cmd, **kw):
        seen.update(kw.get("env") or {})
        return sp.CompletedProcess(cmd, 0, "Wed Oct 22 13:00:00 2026\n", "")

    monkeypatch.setattr("agentmailbus.registry.start_time", lambda pid: None)
    monkeypatch.setattr(sp, "run", fake)
    assert autoname.process_started(5) == "Wed Oct 22 13:00:00 2026"
    assert seen.get("LC_ALL") == "C" and seen.get("TZ") == "UTC"


def test_a_start_time_from_ps_is_compared_when_names_are_chosen(monkeypatch) -> None:
    monkeypatch.setattr(autoname, "process_started", lambda pid: "Wed Oct 22 08:00:00 2026")
    assert autoname.reused(5, "Tue Oct 21 08:00:00 2026")
    assert not autoname.reused(5, "Wed Oct 22 08:00:00 2026")
    monkeypatch.setattr(autoname, "process_started", lambda pid: "")
    assert not autoname.reused(5, "Tue Oct 21 08:00:00 2026")  # unreadable says nothing


def test_only_a_socket_at_the_path_is_live(tmp_path: Path) -> None:
    path = _data(tmp_path) / "p.sock"
    path.write_text("")
    assert not is_live({"name": "x", "harness": "opencode", "pid": os.getpid(), "socket": str(path)})


def test_a_folder_name_that_is_not_utf8_is_hashed(tmp_path: Path) -> None:
    odd = os.path.join(os.fsencode(str(tmp_path)), b"a\xff")
    os.mkdir(odd)
    folder = os.fsdecode(odd)
    assert len(autoname.folder_key(folder)) == 12
    assert NAME.fullmatch(autoname.opencode_name(5, "", {}, tmp_path / "none.toml", folder))


def test_a_tilde_xdg_data_home_is_expanded(monkeypatch) -> None:
    from agentmailbus.paths import data_dir

    monkeypatch.setenv("XDG_DATA_HOME", "~/x")
    assert data_dir() == Path.home() / "x" / "agentmailbus"
    for bad in ("", "rel", "/var/run/../tmp"):
        monkeypatch.setenv("XDG_DATA_HOME", bad)
        assert data_dir() == Path.home() / ".local" / "share" / "agentmailbus"


def test_a_held_name_gets_a_counter(tmp_path: Path) -> None:
    cfg = tmp_path / "relay.toml"
    cfg.write_text('host = "h"\n')
    me = os.getpid()  # a running process, so its tag is stable on every platform
    first = autoname.opencode_name(me, "", {}, cfg, str(tmp_path))
    assert autoname.opencode_name(me, "", {first: {"pid": 9}}, cfg, str(tmp_path)) == first.replace(".h", "-2.h")
    mine = {first: {"pid": me, "socket": autoname.opencode_socket(me, str(tmp_path))}}
    assert autoname.opencode_name(me, "", mine, cfg, str(tmp_path)) == first


def test_a_name_waiting_for_its_plugin_is_held(tmp_path: Path) -> None:
    from agentmailbus.registry import load_held

    reg = tmp_path / "reg"
    me = os.getpid()
    save_binding(reg, {"name": "glm", "harness": "opencode", "pid": me, "wake": "", "socket": str(tmp_path / "no.sock"),
                       "proc_start": start_time(me)})
    save_binding(reg, {"name": "gone", "harness": "opencode", "pid": 2**22 + 12345, "wake": "", "socket": "/x"})
    assert load_live(reg) == {}
    held = load_held(reg)
    assert list(held) == ["glm"]
    # So a second process with the same AGENTMAILBUS_NAME does not take it.
    assert autoname.opencode_name(me + 1, "glm", held, tmp_path / "none.toml", str(tmp_path)) != "glm"
    if start_time(me) is not None:
        save_binding(reg, {"name": "glm", "harness": "opencode", "pid": me, "wake": "", "socket": "/x",
                           "proc_start": start_time(me) + 1})
        assert load_held(reg) == {}  # the pid was reused: not held


def test_python_and_the_plugin_agree_on_the_socket_path(tmp_path: Path) -> None:
    import shutil

    import pytest

    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed")
    plugin = Path(__file__).resolve().parent.parent / "deploy" / "opencode" / "agentmailbus.js"
    folder = tmp_path / "proj"
    folder.mkdir()
    link = tmp_path / "link"
    link.symlink_to(folder)
    script = (
        f"const {{AgentMailBus}} = await import({json.dumps(plugin.as_uri())});"
        "const i = AgentMailBus.internals;"
        f"console.log(JSON.stringify([i.dataDir(), i.folderKey({json.dumps(str(link))})]));"
    )
    odd = os.path.join(os.fsencode(str(tmp_path)), b"dir\xff")
    os.mkdir(odd)
    lossy = os.fsdecode(odd).encode("utf-8", "surrogateescape").decode("utf-8", "replace")
    script_odd = (
        f"const {{AgentMailBus}} = await import({json.dumps(plugin.as_uri())});"
        f"console.log(AgentMailBus.internals.folderKey({json.dumps(lossy)}));"
    )
    js_odd = subprocess.run([node, "--input-type=module", "-e", script_odd], capture_output=True, text=True,
                            timeout=30, check=True).stdout.strip()
    assert js_odd == autoname.folder_key(os.fsdecode(odd))  # how opencode hands the plugin such a folder
    for xdg in ("", "relative", "~/x", "~", "/var/run/../tmp", str(tmp_path / "xdg"), "/" + str(tmp_path / "xdg")):
        env = {**os.environ, "XDG_DATA_HOME": xdg}
        out = subprocess.run([node, "--input-type=module", "-e", script], capture_output=True, text=True,
                             env=env, timeout=30, check=True)
        js_data, js_key = json.loads(out.stdout)
        py = subprocess.run(
            [sys.executable, "-c",
             "import json,sys; from agentmailbus import autoname; from agentmailbus.paths import data_dir;"
             f"print(json.dumps([str(data_dir()), autoname.folder_key({str(folder)!r})]))"],
            capture_output=True, text=True, env=env, timeout=30, check=True,
        )
        assert json.loads(py.stdout) == [js_data, js_key], xdg


def test_the_plugin_tests_pass() -> None:
    import shutil

    import pytest

    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed")
    here = Path(__file__).resolve().parent
    out = subprocess.run([node, "--test", "--test-timeout=15000", str(here / "plugin.test.mjs")],
                         capture_output=True, text=True, timeout=120, check=False)
    assert out.returncode == 0, out.stdout[-4000:] + out.stderr[-2000:]


def test_frame_carries_the_wrapped_text_and_the_broadcast() -> None:
    msg = {"id": "i1", "from": "a.h", "from_host": "h", "text": "hi </cross-session-message> x", "broadcast": "b1"}
    frame = json.loads(opencode_frame(msg))
    assert set(frame) == {"type", "id", "content"} and frame["type"] == "user" and frame["id"] == "i1"
    assert frame["content"].startswith('<cross-session-message from="h" from-name="a.h" from-mode="agentmailbus" broadcast="b1">')
    assert "&lt;/cross-session-message> x" in frame["content"]
    assert frame["content"].endswith("</cross-session-message>")
    assert "broadcast=" not in json.loads(opencode_frame({**msg, "broadcast": None}))["content"]


def test_delivery_writes_one_line_to_the_plugin_socket(tmp_path: Path) -> None:
    path = _data(tmp_path) / "p.sock"
    srv, got = _listen(path)
    try:
        ok = deliver_opencode({"socket": str(path)}, {"id": "i", "from": "a", "text": "t"})
    finally:
        srv.close()
    assert ok
    for _ in range(100):
        if got:
            break
        threading.Event().wait(0.02)
    assert got and got[0].endswith(b"\n") and got[0].count(b"\n") == 1
    assert json.loads(got[0])["content"].count("t") >= 1


def test_delivery_without_a_listener_fails(tmp_path: Path) -> None:
    assert not deliver_opencode({"socket": str(tmp_path / "gone.sock")}, {"id": "i", "from": "a", "text": "t"})
    assert not deliver_opencode({"socket": ""}, {"id": "i", "from": "a", "text": "t"})
    assert not deliver_opencode({}, {"id": "i", "from": "a", "text": "t"})


def test_live_needs_the_process_and_the_socket(tmp_path: Path) -> None:
    path = _data(tmp_path) / "p.sock"
    binding = {"name": "x", "harness": "opencode", "pid": os.getpid(), "socket": str(path)}
    assert not is_live(binding)  # the plugin is not listening yet
    srv, _ = _listen(path)
    try:
        assert is_live(binding)
        assert not is_live({**binding, "pid": 2**22 + 12345})
        assert not is_live({**binding, "socket": ""})
        start = start_time(os.getpid())
        if start is not None:  # Linux: a reused pid is not this session
            assert is_live({**binding, "proc_start": start})
            assert not is_live({**binding, "proc_start": start + 1})
    finally:
        srv.close()


def test_dead_opencode_bindings_and_sockets_are_swept(tmp_path: Path) -> None:
    reg = tmp_path / "reg"
    save_binding(reg, {"name": "dead", "harness": "opencode", "pid": 2**22 + 12345, "wake": "", "socket": "/x"})
    save_binding(reg, {"name": "alive", "harness": "opencode", "pid": os.getpid(), "wake": "", "socket": "/x"})
    assert autoname.sweep_dead(reg) == 1
    assert sorted(p.stem for p in reg.glob("*.json")) == ["alive"]

    socks = _data(tmp_path) / "s"
    socks.mkdir()
    for pid in (2**22 + 12345, os.getpid()):
        s = socket.socket(socket.AF_UNIX)
        s.bind(str(socks / f"{pid}-0123456789ab.sock"))
        s.close()
    (socks / "99999999-0123456789ab.sock").write_text("not a socket")
    (socks / "junk.sock").write_text("")
    assert autoname.sweep_opencode_sockets(socks) == 1
    assert sorted(p.name for p in socks.iterdir()) == sorted(
        [f"{os.getpid()}-0123456789ab.sock", "99999999-0123456789ab.sock", "junk.sock"]
    )


def _mcp(tmp_path: Path, cwd: Path, stdin: bytes, env_name: str = "") -> subprocess.CompletedProcess:
    cfg = tmp_path / "relay.toml"
    cfg.write_text('host = "testhost"\n')
    env = {**os.environ, "XDG_DATA_HOME": str(_data(tmp_path))}
    env.pop("AGENTMAILBUS_NAME", None)
    if env_name:
        env["AGENTMAILBUS_NAME"] = env_name
    return subprocess.run(
        [
            sys.executable, "-m", "agentmailbus", "mcp", "--harness", "opencode",
            "--pid", str(os.getpid()), "--relay-config", str(cfg),
            "--registry", str(tmp_path / "reg"), "--relay-socket", str(tmp_path / "none.sock"),
        ],
        input=stdin, capture_output=True, env=env, cwd=cwd, timeout=30, check=False,
    )


LIST = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}).encode() + b"\n"


def test_mcp_binds_the_plugin_socket_and_says_the_name(tmp_path: Path) -> None:
    folder = tmp_path / "rea"
    folder.mkdir()
    reg = tmp_path / "reg"
    cfg = tmp_path / "relay.toml"
    cfg.write_text('host = "testhost"\n')
    proc = subprocess.Popen(
        [sys.executable, "-m", "agentmailbus", "mcp", "--harness", "opencode", "--pid", str(os.getpid()),
         "--registry", str(reg), "--relay-socket", str(tmp_path / "none.sock"), "--relay-config", str(cfg)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=folder,
        env={**{k: v for k, v in os.environ.items() if k != "AGENTMAILBUS_NAME"}, "XDG_DATA_HOME": str(_data(tmp_path))},
    )
    files: list[Path] = []
    for _ in range(200):
        files = list(reg.glob("*.json")) if reg.is_dir() else []
        if files:
            break
        threading.Event().wait(0.05)
    binding = json.loads(files[0].read_text()) if files else {}
    out, err = proc.communicate(LIST, timeout=30)
    assert proc.returncode == 0, err
    name = binding["name"]
    assert name.startswith("rea-oc") and name.endswith(".testhost")
    assert f"bound as {name}".encode() in err
    tools = json.loads(out.splitlines()[0])["result"]["tools"]
    assert [t["name"] for t in tools] == ["send", "broadcast"]
    assert all(t["description"].startswith(f"Your name on the bus is {name}.") for t in tools)
    assert binding["harness"] == "opencode" and binding["pid"] == os.getpid()
    key = autoname.folder_key(str(folder))
    assert binding["socket"] == str(_data(tmp_path) / "agentmailbus" / "opencode" / f"{os.getpid()}-{key}.sock")
    assert binding["proc_start"] == _expected_start()
    assert not list(reg.glob("*.json"))  # removed when the server ends


def test_mcp_env_name(tmp_path: Path) -> None:
    out = _mcp(tmp_path, tmp_path, LIST, env_name="glm-review")
    assert out.returncode == 0, out.stderr
    assert b"bound as glm-review" in out.stderr


def test_the_binding_is_live_once_the_plugin_listens(tmp_path: Path) -> None:
    import shutil
    import tempfile

    short = Path(tempfile.mkdtemp(dir="/tmp"))  # a Unix socket path must stay short
    try:
        _live_once_listening(short)
    finally:
        shutil.rmtree(short, ignore_errors=True)


def _live_once_listening(tmp_path: Path) -> None:
    reg = tmp_path / "reg"
    sock = _data(tmp_path) / "agentmailbus" / "opencode" / f"{os.getpid()}-{autoname.folder_key(str(tmp_path))}.sock"
    sock.parent.mkdir(parents=True, mode=0o700)
    os.chmod(sock.parent, 0o700)
    srv, _ = _listen(sock)
    try:
        proc = subprocess.Popen(
            [sys.executable, "-m", "agentmailbus", "mcp", "--harness", "opencode", "--pid", str(os.getpid()),
             "--registry", str(reg), "--relay-socket", str(tmp_path / "none.sock"),
             "--relay-config", str(tmp_path / "none.toml")],
            stdin=subprocess.PIPE, stderr=subprocess.PIPE, cwd=tmp_path,
            env={**os.environ, "XDG_DATA_HOME": str(_data(tmp_path))},
        )
        live: dict = {}
        for _ in range(200):
            live = load_live(reg)
            if live:
                break
            threading.Event().wait(0.05)
        proc.stdin.close()
        proc.wait(timeout=10)
        assert list(live.values())[0]["harness"] == "opencode"
    finally:
        srv.close()


def test_from_and_bind_point_an_opencode_binding_at_the_plugin(tmp_path: Path) -> None:
    reg = tmp_path / "reg"
    env = {**os.environ, "XDG_DATA_HOME": str(_data(tmp_path))}
    key = autoname.folder_key(str(tmp_path))
    refused = subprocess.run(
        [sys.executable, "-m", "agentmailbus", "bind", "x", "--harness", "opencode", "--registry", str(reg)],
        capture_output=True, text=True, env=env, cwd=tmp_path, timeout=30, check=False,
    )
    assert refused.returncode != 0 and "--pid" in refused.stderr
    zero = subprocess.run(
        [sys.executable, "-m", "agentmailbus", "bind", "x", "--harness", "opencode", "--pid", "0", "--registry", str(reg)],
        capture_output=True, text=True, env=env, cwd=tmp_path, timeout=30, check=False,
    )
    assert zero.returncode != 0 and "--pid" in zero.stderr
    out = subprocess.run(
        [sys.executable, "-m", "agentmailbus", "bind", "fixed", "--harness", "opencode", "--pid", str(os.getpid()),
         "--registry", str(reg)],
        capture_output=True, text=True, env=env, cwd=tmp_path, timeout=30, check=False,
    )
    assert out.returncode == 0, out.stderr
    saved = json.loads((reg / "fixed.json").read_text())
    assert saved["socket"] == str(_data(tmp_path) / "agentmailbus" / "opencode" / f"{os.getpid()}-{key}.sock")
    assert saved["proc_start"] == _expected_start()

    proc = subprocess.Popen(
        [sys.executable, "-m", "agentmailbus", "mcp", "--from", "named", "--harness", "opencode",
         "--pid", str(os.getpid()), "--registry", str(reg), "--relay-socket", str(tmp_path / "none.sock")],
        stdin=subprocess.PIPE, stderr=subprocess.PIPE, cwd=tmp_path, env=env,
    )
    for _ in range(200):
        if (reg / "named.json").exists():
            break
        threading.Event().wait(0.05)
    saved = json.loads((reg / "named.json").read_text())
    proc.stdin.close()
    proc.wait(timeout=10)
    assert saved["socket"].endswith(f"/{os.getpid()}-{key}.sock")


def test_a_socket_path_too_long_keeps_the_session_off_the_bus(tmp_path: Path) -> None:
    long_data = tmp_path / ("d" * 90)
    out = subprocess.run(
        [sys.executable, "-m", "agentmailbus", "mcp", "--harness", "opencode", "--pid", str(os.getpid()),
         "--registry", str(tmp_path / "reg"), "--relay-socket", str(tmp_path / "none.sock"),
         "--relay-config", str(tmp_path / "none.toml")],
        input=LIST, capture_output=True, env={**os.environ, "XDG_DATA_HOME": str(long_data)},
        cwd=tmp_path, timeout=30, check=False,
    )
    assert out.returncode == 0, out.stderr
    assert b"too long for a Unix socket" in out.stderr
    assert json.loads(out.stdout.splitlines()[0])["result"]["tools"] == []
    assert not list((tmp_path / "reg").glob("*.json")) if (tmp_path / "reg").is_dir() else True


def test_from_with_a_socket_path_too_long_serves_without_tools(tmp_path: Path) -> None:
    out = subprocess.run(
        [sys.executable, "-m", "agentmailbus", "mcp", "--from", "named", "--harness", "opencode",
         "--pid", str(os.getpid()), "--registry", str(tmp_path / "reg"), "--relay-socket", str(tmp_path / "none.sock")],
        input=LIST, capture_output=True, env={**os.environ, "XDG_DATA_HOME": str(tmp_path / ("d" * 90))},
        cwd=tmp_path, timeout=30, check=False,
    )
    assert out.returncode == 0, out.stderr
    assert b"too long for a Unix socket" in out.stderr
    assert json.loads(out.stdout.splitlines()[0])["result"]["tools"] == []


def test_socket_is_refused_for_opencode(tmp_path: Path) -> None:
    for cmd in (["mcp", "--harness", "opencode"], ["bind", "x", "--harness", "opencode", "--pid", "1"]):
        out = subprocess.run(
            [sys.executable, "-m", "agentmailbus", *cmd, "--socket", "/x.sock", "--registry", str(tmp_path / "reg")],
            input=b"", capture_output=True, cwd=tmp_path, timeout=30, check=False,
        )
        assert out.returncode != 0 and b"--socket is not used" in out.stderr


def test_a_socket_in_a_directory_open_to_others_is_not_live(tmp_path: Path) -> None:
    import shutil
    import tempfile

    short = Path(tempfile.mkdtemp(dir="/tmp"))
    try:
        sock_dir = short / "s"
        sock_dir.mkdir(mode=0o700)
        path = sock_dir / "p.sock"
        srv, _ = _listen(path)
        try:
            binding = {"name": "x", "harness": "opencode", "pid": os.getpid(), "socket": str(path)}
            assert is_live(binding)
            os.chmod(sock_dir, 0o755)
            assert not is_live(binding)
            os.chmod(sock_dir, 0o700)
            link = sock_dir / "l.sock"
            link.symlink_to(path)
            assert not is_live({**binding, "socket": str(link)})
        finally:
            srv.close()
    finally:
        shutil.rmtree(short, ignore_errors=True)


def test_a_deleted_working_folder_serves_without_tools(tmp_path: Path) -> None:
    gone = tmp_path / "gone"
    gone.mkdir()
    proc = subprocess.Popen(
        [sys.executable, "-c", "import os,sys; os.rmdir(sys.argv[1]); from agentmailbus.cli import main; "
         "main(['mcp','--harness','opencode','--pid',sys.argv[2],'--registry',sys.argv[3],'--relay-socket','/none'])",
         str(gone), str(os.getpid()), str(tmp_path / "reg")],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=gone,
        env={**os.environ, "XDG_DATA_HOME": str(_data(tmp_path))},
    )
    out, err = proc.communicate(LIST, timeout=30)
    assert proc.returncode == 0, err
    assert b"not joining the bus" in err
    assert json.loads(out.splitlines()[0])["result"]["tools"] == []
