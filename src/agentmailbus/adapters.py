"""Local delivery. Receiving never goes through an MCP tool.

None of these sockets answer. cc-socks sends no reply, and the dsh inbox is this
project's own contract. "Delivered" therefore means the bytes were written, not that
the session accepted them. PROTOCOL.md says the same.
"""

from __future__ import annotations

import contextlib
import errno
import hashlib
import json
import logging
import os
import re
import socket
import stat
import tempfile
import threading
import uuid
from collections.abc import Iterator
from pathlib import Path

from agentmailbus.paths import private_dir

# Claude Code listens on this socket. The frame matches the protocol verified
# 2026-10-03: an auth line, then a user line. The peer token file is
# <sessions dir>/<pid>.<sha256 of the socket path>.key, where the sessions dir is
# the binding's `sessions_dir`, or ~/.claude/sessions.

log = logging.getLogger("agentmailbus.adapters")

_TAG = re.compile(r"<(/?)cross-session-message", re.IGNORECASE)


def claude_socket(pid: int) -> str:
    """Where Claude Code puts its socket: $XDG_RUNTIME_DIR, then the temp dir.

    Claude Code moves to `cc-socks-<uid>` when the temp-dir path would exceed the
    Unix socket path limit. The same rule is applied to the same temp dir here.
    """
    uid = os.getuid()
    name = f"{pid}.sock"
    candidates: list[str] = []
    xdg = os.environ.get("XDG_RUNTIME_DIR")
    if xdg:
        candidates.append(os.path.join(xdg, "cc-socks", name))
    tmpdir = tempfile.gettempdir()
    tmp = os.path.join(tmpdir, "cc-socks", name)
    if len(tmp.encode()) > 103:
        tmp = os.path.join(tmpdir, f"cc-socks-{uid}", name)
    for candidate in (tmp, os.path.join("/tmp", "cc-socks", name), os.path.join("/tmp", f"cc-socks-{uid}", name)):
        if candidate not in candidates:
            candidates.append(candidate)
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    return candidates[0]


def peer_token(sock_path: str, pid: int, sessions_dir: object = None) -> str | None:
    """The session's peer token. `sessions_dir` is where the session keeps its
    records, when its binding says (CLAUDE_CONFIG_DIR moves it); else ~/.claude."""
    digest = hashlib.sha256(sock_path.encode()).hexdigest()
    base = Path(sessions_dir) if isinstance(sessions_dir, str) and os.path.isabs(sessions_dir) else Path.home() / ".claude" / "sessions"
    path = base / f"{pid}.{digest}.key"
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    token = data.get("peerToken")
    if isinstance(token, str) and token:
        return token
    return None


def escape_text(text: str) -> str:
    """Stop a message from closing or opening the wrapper tag around it."""
    return _TAG.sub(r"&lt;\1cross-session-message", text)


def _attr(value: object) -> str:
    """A value fit for an attribute of the wrapper tag."""
    return re.sub(r'[^A-Za-z0-9-]', "", str(value))


def wrapped(message: dict) -> str:
    """The message as a session reads it: the text inside a tag that names the
    sender, which the text itself cannot close or forge."""
    return (
        '<cross-session-message from="{host}" from-name="{name}" from-mode="agentmailbus"{extra}>\n'
        "{text}\n"
        "</cross-session-message>"
    ).format(
        host=message.get("from_host") or "agentmailbus",
        name=message["from"],
        extra=' broadcast="{}"'.format(_attr(message["broadcast"])) if message.get("broadcast") else "",
        text=escape_text(message["text"]),
    )


def claude_frames(binding: dict, message: dict) -> list[bytes]:
    sock_path = binding.get("socket") or claude_socket(int(binding["pid"]))
    body = wrapped(message)
    user = {
        "type": "user",
        "message": {"content": body},
        "from": f"uds:{binding.get('relay_socket') or sock_path}",
        "priority": message.get("priority") or "next",
        "msg_id": message["id"],
    }
    frames = []
    token = peer_token(sock_path, int(binding["pid"]), binding.get("sessions_dir"))
    if token:
        frames.append(_line({"type": "auth", "token": token}))
    frames.append(_line(user))
    return frames


def deliver_claude(binding: dict, message: dict) -> bool:
    sock_path = binding.get("socket") or claude_socket(int(binding["pid"]))
    frames = claude_frames(binding, message)
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(5)
            sock.connect(sock_path)
            for frame in frames:
                sock.sendall(frame)
            sock.shutdown(socket.SHUT_WR)
    except OSError:
        return False
    return True


class _PathLock:
    """A lock for one wake path and the number of callers holding a reference to it."""

    __slots__ = ("lock", "refs")

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.refs = 0


_wake_locks: dict[str, _PathLock] = {}
_wake_locks_guard = threading.Lock()
_WAKE_LOCKS_CAP = 256
# Temporary names this module mints while creating a wake file. The pattern is
# reserved: deliver_wake refuses a wake path named like this, see PROTOCOL.md.
_TEMP = re.compile(r"\.amb-wake-([0-9]+)-[0-9a-f]{8}\.tmp")


def _home() -> str | None:
    """HOME when it is set to an absolute path; None otherwise."""
    home = os.environ.get("HOME")
    return home if home and os.path.isabs(home) else None


def _expand(wake: str, live: bool) -> Path | str:
    """Expand a leading `~`, or say why that cannot be done."""
    path = Path(wake)
    if not wake.startswith("~"):
        return path
    if len(path.parts) == 1:
        return "names a home directory"
    if wake.startswith("~/"):
        home = _home()
        if home is not None:
            return Path(home.rstrip("/") + "/" + wake[2:])  # one string, so a doubled slash cannot discard HOME
        if live or "HOME" in os.environ:
            return "cannot be expanded"  # HOME unusable, or no lookup allowed on the loop
    elif live:
        return "cannot be expanded"  # `~user` needs the user database
    try:
        return path.expanduser()
    except RuntimeError:
        return "cannot be expanded"


def wake_problem(wake: object, *, live: bool = False) -> str:
    """Why a wake path can never be delivered to. Empty when it is fine.

    The path must be a string with no NUL byte, absolute once a leading `~` is
    expanded, with no `..` after that expansion either, not a bare `~`/`~user`, not
    named like one of the relay's temporary files, and if it exists a regular file
    (not a symlink, a directory, a FIFO or a device) this relay can read and write,
    with nothing but directories on the way to its nearest existing ancestor, which
    must be writable when the file has to be made; what cannot be inspected for a
    lasting reason is refused too. Only a string that starts with `~` is
    expanded; `~/` comes from HOME, which must be absolute. With `live=True`, for
    the relay's liveness scan on its event loop, nothing on the filesystem is touched
    and no user database is consulted, so a `~user` path is simply not live.
    """
    if not isinstance(wake, str):
        return "is not a string"
    if "\0" in wake:
        return "contains a NUL byte"
    path = _expand(wake, live)
    if isinstance(path, str):
        return path
    if ".." in path.parts:
        return "contains .."
    if not path.is_absolute():
        return "is not absolute"
    if _TEMP.fullmatch(path.name):
        return "named like a relay temporary file"
    if not live:
        return _leaf_problem(path)
    return ""


# Errors that say something lasting about a path. Anything else (EIO, ESTALE, a
# timeout) is a moment's trouble: the judge lets the path through with a warning
# and the delivery, which runs the judge again and then the open, retries.
_LASTING = {errno.EACCES, errno.EPERM, errno.ELOOP, errno.ENAMETOOLONG, errno.EROFS}
_UNSEEN = "unseen"


def _sort(exc: OSError, path: Path, what: str) -> str:
    """A lasting error is a reason; a passing one is logged and lets the path through."""
    if exc.errno in _LASTING:
        return f"{what} ({_errname(exc)})"
    log.warning("cannot inspect %s right now: %s", path, _errname(exc))
    return _UNSEEN


def _stat(path: Path, follow: bool) -> os.stat_result | None | str:
    """The entry, None when there is none, `_UNSEEN` when a passing error hid it,
    or why it cannot be looked at."""
    try:
        return os.stat(path, follow_symlinks=follow)
    except (FileNotFoundError, NotADirectoryError):
        return None
    except OSError as exc:
        return _sort(exc, path, "cannot be inspected")


def _access(path: Path, mode: int) -> bool:
    """Permission as the open will see it: by effective ids where the platform can."""
    if os.access in os.supports_effective_ids:
        return os.access(path, mode, effective_ids=True)
    return os.access(path, mode)


def _probe_open(path: Path) -> str:
    """`access` said no without an errno: ask the open itself. Empty when it works
    or failed for a passing reason."""
    try:
        _close(os.open(path, _WAKE_FLAGS))  # the delivery's own flags, O_APPEND included
    except OSError as exc:
        found = _sort(exc, path, "cannot be read and written")
        return "" if found == _UNSEEN else found
    return ""


def _probe_create(directory: Path) -> str:
    """`access` said the directory is not writable: try making a reserved-pattern
    temporary there and taking it away again, which is what creation will do."""
    tmp = directory / f".amb-wake-{os.getpid()}-{uuid.uuid4().hex[:8]}.tmp"
    try:
        _close(os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600))
    except OSError as exc:
        found = _sort(exc, directory, f"cannot be made in {directory}")
        return "" if found == _UNSEEN else found
    try:
        os.unlink(tmp)
    except OSError as exc:
        # The sweep lists only the wake file's own directory, and this may be an
        # ancestor of it: say where the empty file is, so a hand can take it.
        log.warning("probe file %s left behind: %s", tmp, _errname(exc))
    return ""


def _leaf_problem(path: Path) -> str:
    """What the filesystem says against the path, at save time and before a
    delivery: the leaf, if it exists, must be a regular file this relay can read
    and write (or an empty one of this user's that nobody can write to, which the
    relay repairs); the way down to it must be directories; and the nearest
    existing directory must be writable when the leaf has to be made or remade.
    Every question is asked with the same errno policy: a lasting answer refuses,
    a passing one lets the path through."""
    st = _stat(path, follow=False)
    if isinstance(st, str):
        return "" if st == _UNSEEN else st
    must_write_dir = st is None
    if st is not None:
        mode = st.st_mode
        if stat.S_ISLNK(mode):
            return "is a symlink"
        if stat.S_ISDIR(mode):
            return "is a directory"
        if not stat.S_ISREG(mode):
            return "is not a regular file"
        if not _access(path, os.R_OK | os.W_OK):
            repairable = st.st_uid == os.geteuid() and st.st_size == 0 and not stat.S_IMODE(mode) & 0o222
            if repairable:
                must_write_dir = True  # the repair is an unlink and a new file
            else:
                found = _probe_open(path)
                if found:
                    return found
    for ancestor in path.parents:
        st = _stat(ancestor, follow=False)
        if isinstance(st, str):
            return "" if st == _UNSEEN else st
        if st is None:
            continue
        target = _stat(ancestor, follow=True)  # through a symlink: what is really there
        if isinstance(target, str):
            return "" if target == _UNSEEN else target
        if target is None or not stat.S_ISDIR(target.st_mode):  # a file, or a symlink to nowhere
            return "has something other than a directory on the way"
        if must_write_dir and not _access(ancestor, os.W_OK | os.X_OK):
            found = _probe_create(ancestor)
            if found:
                return found
        break
    return ""


def expand_wake(wake: str) -> str:
    """The wake path as a session file should hold it: `~` expanded, so the
    liveness scan never has to. Call after `wake_problem` has passed it."""
    path = _expand(wake, False)
    return str(path) if isinstance(path, Path) else wake


def _wake_entry(path: Path) -> _PathLock:
    """The entry for a path, with this caller counted on it. Two names may share a
    file; their writes never overlap.

    The table is pruned of entries nobody references once it passes the cap, so a
    wrapper that mints a new path per session does not grow it without bound. The
    reference count, not the lock state, decides what may go: an entry handed to a
    caller that has not yet acquired the lock is referenced and stays.
    """
    key = os.path.realpath(path)
    with _wake_locks_guard:
        entry = _wake_locks.get(key)
        if entry is None:
            if len(_wake_locks) >= _WAKE_LOCKS_CAP:
                for other, held in list(_wake_locks.items()):
                    if held.refs == 0:
                        del _wake_locks[other]
            entry = _wake_locks[key] = _PathLock()
        entry.refs += 1
    return entry


def _wake_release(entry: _PathLock) -> None:
    with _wake_locks_guard:
        entry.refs -= 1


@contextlib.contextmanager
def _wake_lock(path: Path) -> Iterator[None]:
    entry = _wake_entry(path)
    try:
        with entry.lock:
            yield
    finally:
        _wake_release(entry)


def _sweep_temps(directory: Path) -> None:
    """Remove what a relay killed during creation left behind: an empty file under
    a temporary name, or a second link to the live wake file. Only names this
    module mints are touched, only while they look like that (a regular file that
    is empty or has another link), and only once the pid in the name is gone, as
    seen by this user. Runs at every delivery; the directory is small."""
    from agentmailbus.registry import pid_alive  # registry imports this module

    try:
        names = os.listdir(directory)
    except OSError:
        return
    for name in names:
        found = _TEMP.fullmatch(name)
        if not found or pid_alive(int(found.group(1))):
            continue
        leftover = directory / name
        try:
            st = os.lstat(leftover)
            if stat.S_ISREG(st.st_mode) and (st.st_size == 0 or st.st_nlink > 1):
                os.unlink(leftover)
                log.info("removed %s left by a dead relay", name)
        except OSError:
            pass


def deliver_wake(binding: dict, message: dict) -> bool:
    problem = wake_problem(binding["wake"])
    if problem:
        log.warning("wake path %s %s; refused", binding["wake"], problem)
        return False
    path = Path(binding["wake"]).expanduser()
    private_dir(path.parent)
    record = {
        "id": message["id"],
        "from": message["from"],
        "from_host": message.get("from_host") or "",
        "to": message["to"],
        "ts": message.get("created_at") or message.get("ts") or "",
        "text": message["text"],
    }
    if message.get("broadcast"):
        record["broadcast"] = message["broadcast"]
    line = json.dumps(record, ensure_ascii=False)
    with _wake_lock(path):
        _sweep_temps(path.parent)
        return _append_line(path, (line + "\n").encode())


def _append_line(path: Path, payload: bytes) -> bool:
    data = memoryview(payload)
    fd = _open_wake(path)
    if fd is None:
        return False
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            log.warning("wake path %s is not a regular file", path)
            return False
        start = _terminated(fd)
        done = 0
        try:
            while data:
                written = os.write(fd, data)
                if written <= 0:
                    raise OSError("short write")
                data = data[written:]
                done += written
        except OSError:
            # Part of a line is in the file. Take it back out, but only if nothing
            # else landed after it; otherwise end what is there with a newline so a
            # line reader moves past it. Either way this delivery is not complete.
            if done:
                _unwrite(fd, path, start, done)
            return False
        try:
            os.fsync(fd)
        except OSError as exc:
            # The line is written and a reader may already have it. A retry would
            # append it twice, so this is a delivered message with a logged sync error.
            log.warning("fsync failed for %s: %s", path, type(exc).__name__)
    finally:
        try:
            os.close(fd)
        except OSError as exc:
            log.warning("close failed for %s: %s", path, type(exc).__name__)
    return True


def _terminated(fd: int) -> int:
    """Make sure the file ends on a newline (a crash can leave a torn tail) and
    return the offset this line will start at."""
    size = os.fstat(fd).st_size
    if size and os.pread(fd, 1, size - 1) != b"\n":
        os.write(fd, b"\n")
        size += 1
    return size


def _unwrite(fd: int, path: Path, start: int, done: int) -> None:
    try:
        size = os.fstat(fd).st_size
        if size == start + done:
            os.ftruncate(fd, start)
            return
        ended = bool(size) and os.pread(fd, 1, size - 1) == b"\n"
    except OSError:
        ended = False
    if not ended:
        try:
            os.write(fd, b"\n")
        except OSError:
            pass
    log.warning("partial wake line left in %s", path)


_WAKE_FLAGS = os.O_RDWR | os.O_APPEND | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0)


def _open_wake(path: Path) -> int | None:
    """Open the wake file for append; create it if missing.

    A new file is created under a private temporary name, made 0600 through its
    descriptor whatever the umask, and then linked into place, so the wake path
    never names a half-made file. Where that cannot be done (a filesystem without
    hard links, or any other failure to make or link the temporary file) the file
    is created in place and made 0600 the same way. Either way the mode is read
    back, and a filesystem that keeps a different one is logged. An existing file
    keeps its mode. The descriptor is read-write, since the tail is read before
    every append. O_NONBLOCK keeps a FIFO from blocking the open; a FIFO or a device
    is then refused by the regular-file check, and O_NOFOLLOW refuses a symlink.

    A wake file this user owns that nobody can write to (no write bit at all) and
    that is empty is a leftover of an attempt that could not set the mode: it is
    removed again, whether found at the first open or made just now, so the next
    attempt starts over. See `_remove_dead` for the rule.
    """
    try:
        return os.open(path, _WAKE_FLAGS)
    except FileNotFoundError:
        pass
    except OSError as exc:
        log.warning("cannot open wake file %s: %s", path, _errname(exc))
        if exc.errno in (errno.EACCES, errno.EPERM) and _remove_dead(path, None):
            pass  # an empty file nobody could write, left by an earlier attempt: start over
        else:
            return None
    made = _link_in(path)
    if made is _UNUSABLE:
        # The temporary file could not be reopened for use. Making the file in
        # place under the same umask and mount would only publish that failure
        # at the wake path, so this delivery fails here and is retried.
        return None
    if made is None:
        made = _create_in_place(path)
    if made is None:
        return None
    try:
        return os.open(path, _WAKE_FLAGS)
    except OSError as exc:
        log.warning("cannot open wake file %s: %s", path, _errname(exc))
        if made is not _THEIRS and exc.errno in (errno.EACCES, errno.EPERM):
            _remove_dead(path, made)
        return None


# What creating a wake file reports: the (device, inode) of the file this relay
# made, _THEIRS when another writer's file was there first, _UNKNOWN when the
# file was made but could not be identified, _UNUSABLE when the temporary file
# could not be reopened for use, None when nothing was made.
_Ident = tuple[int, int]
_THEIRS: _Ident = (-1, -1)
_UNKNOWN: _Ident = (0, 0)
_UNUSABLE: _Ident = (-2, -2)


def _link_in(path: Path) -> _Ident | None:
    """Make the file under a temporary name and link it into place."""
    tmp = path.with_name(f".amb-wake-{os.getpid()}-{uuid.uuid4().hex[:8]}.tmp")
    try:
        fd = os.open(tmp, os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
    except OSError:
        return None
    try:
        ident = _set_mode(fd, path)
        _close(fd)  # nothing needs it now, and the probe should not need a second slot
        # Prove the file can be opened for use before it gets its real name, so the
        # wake path never names a file this relay cannot use.
        try:
            _close(os.open(tmp, _WAKE_FLAGS))
        except OSError as exc:
            log.warning("cannot reopen the new wake file for %s: %s", path, _errname(exc))
            return _UNUSABLE
        os.link(tmp, path)
    except FileExistsError:
        return _THEIRS  # another writer made it first; open theirs
    except OSError as exc:
        log.warning("cannot make wake file %s through a temporary name: %s", path, _errname(exc))
        return None
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass
    return ident


def _create_in_place(path: Path) -> _Ident | None:
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
    except FileExistsError:
        return _THEIRS
    except OSError as exc:
        log.warning("cannot create wake file %s: %s", path, _errname(exc))
        return None
    try:
        return _set_mode(fd, path)
    finally:
        _close(fd)


def _set_mode(fd: int, path: Path) -> _Ident:
    """0600 through the descriptor, then read back: a filesystem that does not
    store modes, or ignores the change, keeps its own and is logged. Returns the
    file's identity for a later removal."""
    try:
        os.fchmod(fd, 0o600)
    except OSError as exc:
        log.warning("cannot set the mode of %s: %s", path, _errname(exc))
    try:
        st = os.fstat(fd)
    except OSError as exc:
        log.warning("cannot read back %s: %s", path, _errname(exc))
        return _UNKNOWN
    mode = stat.S_IMODE(st.st_mode)
    if mode != 0o600:
        log.warning("wake file %s stays mode %o: the filesystem keeps its own", path, mode)
    return (st.st_dev, st.st_ino)


def _remove_dead(path: Path, ident: _Ident | None) -> bool:
    """Remove the file at `path` only while it is an empty regular file of this
    user's that nobody could write to (no write bit for anyone), so no writer can
    have put anything in it since the mode was set; with `ident`, also only while
    it is the file this relay just made. Checked right before the removal. True
    when it is gone. A descriptor somebody opened for writing before the mode was
    narrowed is the accepted same-user case."""
    if ident == _UNKNOWN:
        log.warning("cannot tell whether %s is the file just made; left alone", path)
        return False
    try:
        there = os.lstat(path)
    except OSError as exc:
        log.warning("cannot check %s: %s", path, _errname(exc))
        return False
    if not stat.S_ISREG(there.st_mode) or (ident is not None and (there.st_dev, there.st_ino) != ident):
        log.warning("%s was replaced while it was being made; left alone", path)
        return False
    if there.st_uid != os.geteuid():
        log.warning("%s belongs to another user; left alone", path)
        return False
    if there.st_size:
        log.warning("%s has data this relay did not write; left alone", path)
        return False
    if stat.S_IMODE(there.st_mode) & 0o222:
        log.warning("%s could have been written to; left alone", path)
        return False
    try:
        os.unlink(path)
        log.warning("removed %s: an empty wake file nobody could write to", path)
        return True
    except OSError as exc:
        log.warning("cannot remove %s: %s; later deliveries will fail to open it", path, _errname(exc))
        return False


def _close(fd: int) -> None:
    try:
        os.close(fd)
    except OSError as exc:
        log.warning("close failed: %s", _errname(exc))


def _errname(exc: OSError) -> str:
    return errno.errorcode.get(exc.errno or 0, type(exc).__name__)


def deliver_dsh(binding: dict, message: dict) -> bool:
    """One JSON line on the session inbox socket.

    The frame is AgentMailBus's contract. Confirm it against a live dsh before
    treating it as that program's native protocol.
    """
    frame = {
        "type": "user",
        "id": message["id"],
        "from": message["from"],
        "from_host": message.get("from_host") or "",
        "text": message["text"],
    }
    if message.get("broadcast"):
        frame["broadcast"] = message["broadcast"]
    payload = _line(frame)
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(5)
            sock.connect(binding["socket"])
            sock.sendall(payload)
            sock.shutdown(socket.SHUT_WR)
    except OSError:
        return False
    return True


def opencode_frame(message: dict) -> bytes:
    """One JSON line for the opencode plugin (deploy/opencode/agentmailbus.js).
    `content` is what the plugin puts in the session, wrapped as for Claude; the
    sender and a broadcast id are in the wrapper, and the plugin reads nothing else."""
    return _line({"type": "user", "id": message["id"], "content": wrapped(message)})


def deliver_opencode(binding: dict, message: dict) -> bool:
    sock_path = binding.get("socket")
    if not isinstance(sock_path, str) or not sock_path:
        return False
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(5)
            sock.connect(sock_path)
            sock.sendall(opencode_frame(message))
            sock.shutdown(socket.SHUT_WR)
    except OSError:
        return False
    return True


def default_adapters() -> dict:
    return {
        "claude": deliver_claude,
        "dsh": deliver_dsh,
        "grok": deliver_wake,
        "opencode": deliver_opencode,
        "orca": deliver_wake,
    }


def _line(obj: dict) -> bytes:
    return (json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
