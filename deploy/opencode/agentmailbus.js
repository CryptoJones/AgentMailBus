// AgentMailBus delivery for opencode.
//
// The relay writes one JSON line per message to a Unix socket; this plugin owns
// that socket and puts the message into an opencode session as a prompt. The name
// on the bus comes from `agentmailbus mcp --harness opencode`, which opencode
// starts in the project's folder and which binds the same path:
//   <data dir>/agentmailbus/opencode/<pid>-<folder key>.sock
// where the data dir is $XDG_DATA_HOME when that is an absolute path, else
// ~/.local/share, and the folder key is the first 12 hex digits of SHA-256 of
// the folder's real path. One opencode process can serve several folders; each
// gets its own socket, name and state.
//
// Install: copy this file to ~/.config/opencode/plugins/. See README, "opencode".

import crypto from "node:crypto";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";

const MAX_FRAME = 1 << 20; // the relay sends one message per connection
const SEEN_CAP = 1024;
const QUEUE_CAP = 1000; // messages waiting behind a slow opencode call
const SLOW_CALL = 60000; // an opencode API call this slow is logged
const WRAPPER = "<cross-session-message ";
const MAX_SOCKET_PATH = 103; // bytes: macOS allows 104 with the NUL, Linux 108
const KEY = Symbol.for("agentmailbus.opencode");
const log = (...args) => console.error("[agentmailbus]", ...args);

function dataDir() {
  let env = process.env.XDG_DATA_HOME || "";
  if (env === "~" || env.startsWith("~/")) env = os.homedir() + env.slice(1);
  if (env.startsWith("//")) env = "/" + env.replace(/^\/+/, "");
  // `..` would be resolved here as text and by Python through symlinks; neither uses it.
  const usable = path.isAbsolute(env) && !env.split("/").includes("..");
  const base = usable ? env : path.join(os.homedir(), ".local", "share");
  return path.join(base, "agentmailbus");
}

function folderKey(directory) {
  let real = directory;
  try {
    real = fs.realpathSync(directory);
  } catch {}
  return crypto.createHash("sha256").update(real).digest("hex").slice(0, 12);
}

function socketPath(directory) {
  return path.join(dataDir(), "opencode", `${process.pid}-${folderKey(directory)}.sock`);
}

// Only a directory of this user that nobody else can enter may hold the socket.
function privateDir(dir) {
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  const st = fs.lstatSync(dir);
  if (!st.isDirectory()) throw new Error(`${dir} is not a directory`);
  if (typeof process.getuid === "function" && st.uid !== process.getuid()) {
    throw new Error(`${dir} belongs to another user`);
  }
  if ((st.mode & 0o077) !== 0) throw new Error(`${dir} is open to other users`);
}

// State per folder, kept across rebuilds of the plugin in this process. opencode
// gives each folder its own event stream, and `directory` is already a real path.
function folderState(directory) {
  const all = (globalThis[KEY] ||= new Map());
  const key = folderKey(directory); // the same key as the socket's
  let s = all.get(key);
  if (!s) {
    s = {
      directory,
      client: null,
      personAt: new Map(), // session id -> creation time of the newest prompt a person wrote there
      pending: new Map(), // user message id -> {sid, created}, until its text says whose it is
      decided: new Set(), // user message ids already judged, so a repeated update is ignored
      children: new Set(), // subagent sessions, never a target
      roots: new Set(), // sessions opencode said are roots
      gone: new Set(), // sessions archived or deleted; never a target again
      seen: [], // recent frame ids, so a relay retry is not a second prompt
      queue: Promise.resolve(),
      waiting: 0,
      server: null,
      listening: false,
    };
    all.set(key, s);
  }
  return s;
}

function capSet(set, cap) {
  while (set.size > cap) set.delete(set.values().next().value);
}

function remember(list, value, cap) {
  list.push(value);
  if (list.length > cap) list.splice(0, list.length - cap);
}

function status(res) {
  return res?.response?.status;
}

// Waits for opencode however long it takes, so messages stay in order and an
// outcome is never guessed; a call that takes over a minute is logged.
function call(promise) {
  const timer = setTimeout(() => log("opencode has not answered for a minute; later messages wait"), SLOW_CALL);
  return Promise.resolve(promise).finally(() => clearTimeout(timer));
}

// "root", "child", "gone" (deleted or archived), or "unknown". An answer that
// says nothing (an error other than 404) keeps a session already known to be a
// root, so a passing fault does not send the message elsewhere.
async function kind(s, id) {
  if (s.children.has(id)) return "child";
  if (s.gone.has(id)) return "gone";
  const res = await call(s.client.session.get({ path: { id }, query: { directory: s.directory } }));
  // An archive or delete seen while this was asked outranks the answer.
  if (s.gone.has(id) || status(res) === 404) return "gone";
  if (res.error || !res.data) return s.roots.has(id) ? "root" : "unknown";
  if (res.data.parentID) {
    s.children.add(id);
    capSet(s.children, SEEN_CAP);
    return "child";
  }
  if (res.data.time?.archived) return "gone";
  knownRoot(s, id);
  return "root";
}

function knownRoot(s, id) {
  if (s.gone.has(id) || s.children.has(id)) return;
  s.roots.add(id);
  capSet(s.roots, SEEN_CAP);
}

function gone(s, id) {
  if (!id) return;
  s.gone.add(id); // kept for the life of the process: a stale answer must never revive it
  forget(s, id);
}

function forget(s, sid) {
  s.personAt.delete(sid);
  s.roots.delete(sid);
}

function capMap(map, cap, oldestFirst) {
  while (map.size > cap) map.delete(oldestFirst());
}

// The root session a person last prompted (by the time opencode gives the
// message); else the folder's newest root session; else a new one. `avoid` is a
// session that just answered 404.
async function target(s, avoid = null) {
  const recent = [...s.personAt.entries()]
    .filter(([sid]) => sid !== avoid && !s.children.has(sid))
    .sort((a, b) => b[1] - a[1]);
  for (const [sid] of recent) {
    const k = await kind(s, sid);
    if (k === "root") return sid;
    if (k === "gone") forget(s, sid);
  }
  const listed = await call(s.client.session.list({ query: { directory: s.directory } }));
  if (listed.error || !Array.isArray(listed.data)) throw new Error(`listing sessions failed: HTTP ${status(listed) ?? "?"}`);
  const roots = listed.data.filter(
    (x) => !x.parentID && !x.time?.archived && x.id !== avoid && !s.gone.has(x.id) && (!x.directory || x.directory === s.directory),
  );
  roots.sort((a, b) => (b.time?.updated || 0) - (a.time?.updated || 0));
  if (roots.length) {
    knownRoot(s, roots[0].id);
    return roots[0].id;
  }
  const made = await call(
    s.client.session.create({ body: { title: "AgentMailBus" }, query: { directory: s.directory } }),
  );
  if (!made.data?.id) throw new Error("could not create a session");
  if (s.gone.has(made.data.id)) throw new Error("the new session was archived at once");
  knownRoot(s, made.data.id);
  return made.data.id;
}

function prompt(s, id, content) {
  return call(
    s.client.session.promptAsync({
      path: { id },
      query: { directory: s.directory },
      body: { parts: [{ type: "text", text: content }] },
    }),
  );
}

function deliver(s, frame) {
  // In arrival order, one at a time. Behind a stuck opencode call the wait is
  // bounded: past QUEUE_CAP messages, new ones are dropped and logged.
  if (s.waiting >= QUEUE_CAP) {
    log(`message ${frame.id} was dropped: ${QUEUE_CAP} messages already wait for opencode`);
    return s.queue;
  }
  s.waiting += 1;
  s.queue = s.queue
    .then(async () => {
      const id = await target(s);
      let res = await prompt(s, id, frame.content);
      if (res.error && status(res) === 404) {
        // The session is gone: the next choice gets it instead, once.
        gone(s, id);
        const next = await target(s, id);
        res = await prompt(s, next, frame.content);
        if (res.error && status(res) === 404) gone(s, next);
      }
      if (res.error) throw new Error(`HTTP ${status(res) ?? "?"}`);
    })
    .catch((err) => log(`message ${frame.id} was not delivered:`, err?.message || err))
    .finally(() => {
      s.waiting -= 1;
    });
  return s.queue;
}

function handleLine(s, line) {
  if (!line.trim()) return;
  let frame;
  try {
    frame = JSON.parse(line);
  } catch {
    log("a line that is not JSON was dropped");
    return;
  }
  if (frame?.type !== "user" || typeof frame.content !== "string" || !frame.content) return;
  if (typeof frame.id === "string" && frame.id) {
    if (s.seen.includes(frame.id)) return; // the relay retried a write that had landed
    remember(s.seen, frame.id, SEEN_CAP);
  }
  deliver(s, frame);
}

function onConnection(s, conn) {
  const chunks = [];
  let size = 0;
  let tooBig = false;
  conn.setTimeout(10000, () => conn.destroy());
  conn.on("data", (chunk) => {
    if (tooBig) return;
    size += chunk.length;
    if (size > MAX_FRAME) {
      tooBig = true;
      log("frame over 1 MiB dropped");
      conn.destroy();
      return;
    }
    chunks.push(chunk);
  });
  conn.on("error", () => {});
  conn.on("end", () => {
    if (tooBig) return;
    for (const line of Buffer.concat(chunks).toString("utf8").split("\n")) handleLine(s, line);
  });
}

function listen(s) {
  const file = socketPath(s.directory);
  if (Buffer.byteLength(file) > MAX_SOCKET_PATH) {
    throw new Error(`the socket path ${file} is too long for a Unix socket; set a shorter XDG_DATA_HOME`);
  }
  privateDir(path.dirname(file));
  try {
    // Left by an earlier process with this pid; nothing of ours listens on it.
    if (fs.lstatSync(file).isSocket()) fs.unlinkSync(file);
  } catch {}
  const server = net.createServer((conn) => onConnection(s, conn));
  s.server = server;
  server.on("error", (err) => {
    log("socket:", err?.message || err);
    // A listen that failed is tried again on the next build. After listening, an
    // error (a passing EMFILE on accept) leaves the listener as it is.
    if (!s.listening && s.server === server) s.server = null;
  });
  server.listen(file, () => {
    s.listening = true;
    try {
      fs.chmodSync(file, 0o600);
      const ino = fs.lstatSync(file).ino;
      process.once("exit", () => {
        try {
          if (fs.lstatSync(file).ino === ino) fs.unlinkSync(file);
        } catch {}
      });
    } catch (err) {
      log("socket:", err?.message || err);
    }
  });
}

function socketThere(directory) {
  try {
    return fs.lstatSync(socketPath(directory)).isSocket();
  } catch {
    return false;
  }
}

function onEvent(s, event) {
  const p = event?.properties || {};
  if (event?.type === "session.created" || event?.type === "session.updated") {
    if (p.info?.parentID) {
      s.children.add(p.info.id);
      capSet(s.children, SEEN_CAP);
    }
    if (p.info?.time?.archived) gone(s, p.info.id);
    else if (p.info?.id && !p.info.parentID) knownRoot(s, p.info.id);
  } else if (event?.type === "session.deleted") {
    s.children.delete(p.info?.id);
    gone(s, p.info?.id);
  } else if (event?.type === "message.updated") {
    // opencode sends this again for the same message after each step, so the
    // message's own creation time, not the event, says when it was prompted.
    const info = p.info;
    if (info?.role !== "user" || !info.id || !info.sessionID) return;
    if (s.pending.has(info.id) || s.decided.has(info.id)) return;
    s.pending.set(info.id, { sid: info.sessionID, created: info.time?.created || Date.now() });
    capMap(s.pending, SEEN_CAP, () => s.pending.keys().next().value);
  } else if (event?.type === "message.part.updated") {
    const part = p.part;
    if (part?.type !== "text" || typeof part.text !== "string" || !part.messageID) return;
    // Text opencode adds by itself (a file an @mention read, a reminder, the turn
    // after compaction) says nothing about who wrote the message; skip the part.
    if (part.synthetic || part.ignored) return;
    const msg = s.pending.get(part.messageID);
    if (!msg) return;
    s.pending.delete(part.messageID);
    s.decided.add(part.messageID);
    capSet(s.decided, SEEN_CAP);
    // A prompt from the bus is no one's choice of session; a person's text is.
    if (part.text.startsWith(WRAPPER)) return;
    if ((s.personAt.get(msg.sid) || 0) < msg.created) s.personAt.set(msg.sid, msg.created);
    capMap(s.personAt, SEEN_CAP, () => [...s.personAt.entries()].sort((x, y) => x[1] - y[1])[0][0]);
  }
}

export const AgentMailBus = async ({ client, directory }) => {
  const s = folderState(directory);
  s.client = client; // a rebuild for this folder brings a fresh client
  if (s.server && s.listening && !socketThere(s.directory)) {
    // The socket file was removed under a live listener: listen again.
    try {
      s.server.close();
    } catch {}
    s.server = null;
    s.listening = false;
  }
  if (!s.server) {
    try {
      listen(s);
    } catch (err) {
      log("not receiving:", err?.message || err);
      return {};
    }
  }
  return {
    event: async ({ event }) => onEvent(s, event),
  };
};

// For the tests only. opencode runs every function a plugin file exports, so the
// helpers hang off the plugin instead of being exported.
AgentMailBus.internals = { dataDir, folderKey, socketPath, folderState, deliver, handleLine, onEvent };
