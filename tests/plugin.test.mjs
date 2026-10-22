// The opencode plugin, run under node with a fake opencode client.
// Run: node --test tests/plugin.test.mjs (tests/test_opencode.py runs it too).
import assert from "node:assert/strict";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";

const tmp = fs.mkdtempSync(path.join("/tmp", "amb-plugin-")); // short: a Unix socket path is limited
process.env.XDG_DATA_HOME = path.join(tmp, "data");
const { AgentMailBus } = await import("../deploy/opencode/agentmailbus.js");
const { folderState, deliver, handleLine, onEvent, socketPath, folderKey, dataDir } = AgentMailBus.internals;

function fakeClient(sessions, { promptStatus = () => 200 } = {}) {
  const calls = { prompts: [], created: 0 };
  const res = (data, code = 200) => (code >= 400 ? { error: { code }, response: { status: code } } : { data, response: { status: code } });
  return {
    calls,
    session: {
      get: async ({ path: { id } }) => {
        const s = sessions.find((x) => x.id === id);
        return s ? res(s) : res(null, 404);
      },
      list: async () => res(sessions),
      create: async () => {
        calls.created += 1;
        const s = { id: `new${calls.created}`, time: { updated: Date.now() } };
        sessions.push(s);
        return res(s);
      },
      promptAsync: async ({ path: { id }, body }) => {
        const code = promptStatus(id);
        calls.prompts.push({ id, text: body.parts[0].text, code });
        return res(undefined, code);
      },
    },
  };
}

let n = 0;
function fresh(sessions, opts) {
  const dir = fs.mkdtempSync(path.join(tmp, `f${n++}-`));
  const s = folderState(dir);
  s.client = fakeClient(sessions, opts);
  return s;
}

const frame = (id, content = `msg ${id}`) => ({ type: "user", id, content });

test("the newest root session gets a message when nobody has prompted yet", async () => {
  const s = fresh([
    { id: "old", time: { updated: 1 } },
    { id: "kid", parentID: "old", time: { updated: 9 } },
    { id: "new", time: { updated: 5 } },
  ]);
  await deliver(s, frame("a"));
  assert.deepEqual(s.client.calls.prompts.map((p) => p.id), ["new"]);
});

test("with no session at all one is made", async () => {
  const s = fresh([]);
  await deliver(s, frame("a"));
  assert.equal(s.client.calls.created, 1);
  assert.equal(s.client.calls.prompts[0].id, "new1");
});

const userMsg = (id, sid, created) => ({ type: "message.updated", properties: { info: { id, role: "user", sessionID: sid, time: { created } } } });
const personPart = (mid, sid, text = "hello") => ({
  type: "message.part.updated",
  properties: { part: { id: `p${mid}`, type: "text", messageID: mid, sessionID: sid, text } },
});
const typed = (s, mid, sid, created) => {
  onEvent(s, userMsg(mid, sid, created));
  onEvent(s, personPart(mid, sid));
};
const busPart = (mid, sid) => ({
  type: "message.part.updated",
  properties: { part: { id: `p${mid}`, type: "text", messageID: mid, sessionID: sid, text: '<cross-session-message from="h" from-name="x">\nhi\n</cross-session-message>' } },
});

test("the session a person last prompted is the target; the plugin's own prompts do not count", async () => {
  const s = fresh([{ id: "A", time: { updated: 1 } }, { id: "B", time: { updated: 9 } }]);
  typed(s, "m1", "A", 100);
  await deliver(s, frame("x"));
  assert.equal(s.client.calls.prompts.at(-1).id, "A");
  // The person moves to B; the injected message in A arrives afterwards, with a later time.
  typed(s, "m2", "B", 200);
  onEvent(s, userMsg("inj", "A", 300));
  onEvent(s, busPart("inj", "A"));
  await deliver(s, frame("y"));
  assert.equal(s.client.calls.prompts.at(-1).id, "B");
});

test("a user message whose text is not seen yet does not count", async () => {
  const s = fresh([{ id: "A", time: { updated: 1 } }, { id: "B", time: { updated: 2 } }]);
  typed(s, "m1", "A", 100);
  onEvent(s, userMsg("inj", "B", 300)); // the plugin's own prompt, its text part still to come
  await deliver(s, frame("x"));
  assert.equal(s.client.calls.prompts.at(-1).id, "A");
});

test("a failed session list makes no session and delivers nothing", async () => {
  const s = fresh([]);
  s.client.session.list = async () => ({ error: { code: 500 }, response: { status: 500 } });
  await deliver(s, frame("x"));
  assert.equal(s.client.calls.created, 0);
  assert.equal(s.client.calls.prompts.length, 0);
});

test("an archived session is not the fallback", async () => {
  const s = fresh([{ id: "live", time: { updated: 1 } }, { id: "old", time: { updated: 9, archived: 10 } }]);
  await deliver(s, frame("x"));
  assert.equal(s.client.calls.prompts.at(-1).id, "live");
});

test("a repeated update of an older prompt does not move the target", async () => {
  const s = fresh([{ id: "A", time: { updated: 1 } }, { id: "B", time: { updated: 2 } }]);
  typed(s, "m1", "A", 100);
  typed(s, "m2", "B", 200);
  onEvent(s, userMsg("m1", "A", 100)); // opencode updates m1 again after a step
  await deliver(s, frame("x"));
  assert.equal(s.client.calls.prompts.at(-1).id, "B");
});

test("a lookup that fails for another reason than 404 keeps the target", async () => {
  const s = fresh([{ id: "A", time: { updated: 1 } }, { id: "B", time: { updated: 2 } }]);
  typed(s, "m1", "A", 100);
  await deliver(s, frame("w")); // A is confirmed a root here
  s.client.session.get = async () => ({ error: { code: 500 }, response: { status: 500 } });
  await deliver(s, frame("x"));
  assert.equal(s.client.calls.prompts.at(-1).id, "A");
});

test("a frame id already taken is skipped, whatever became of it", async () => {
  const s = fresh([{ id: "A", time: { updated: 1 } }], { promptStatus: () => 400 });
  handleLine(s, JSON.stringify(frame("refused")));
  await s.queue;
  handleLine(s, JSON.stringify(frame("refused")));
  await s.queue;
  assert.equal(s.client.calls.prompts.length, 1);
});

test("text opencode adds by itself is not a person's choice", async () => {
  const s = fresh([{ id: "A", time: { updated: 1 } }, { id: "B", time: { updated: 2 } }]);
  typed(s, "m1", "A", 100);
  onEvent(s, userMsg("auto", "B", 200));
  onEvent(s, {
    type: "message.part.updated",
    properties: { part: { id: "pa", type: "text", messageID: "auto", sessionID: "B", text: "Continue if you have next steps", synthetic: true } },
  });
  await deliver(s, frame("x"));
  assert.equal(s.client.calls.prompts.at(-1).id, "A");
});

test("an archived session is not a target even after a person prompted it", async () => {
  const s = fresh([{ id: "A", time: { updated: 1, archived: 5 } }, { id: "B", time: { updated: 2 } }]);
  typed(s, "m1", "A", 100);
  await deliver(s, frame("x"));
  assert.equal(s.client.calls.prompts.at(-1).id, "B");
});

test("a failed lookup keeps a known root but not a session never checked", async () => {
  const s = fresh([{ id: "A", time: { updated: 1 } }, { id: "B", time: { updated: 2 } }]);
  typed(s, "m1", "A", 100);
  s.client.session.get = async () => ({ error: { code: 500 }, response: { status: 500 } });
  await deliver(s, frame("x"));
  assert.equal(s.client.calls.prompts.at(-1).id, "B"); // A was never confirmed a root
});

test("a slow call holds later messages, so they stay in order", async () => {
  const s = fresh([{ id: "A", time: { updated: 1 } }]);
  const real = s.client.session.promptAsync;
  let release;
  const gate = new Promise((r) => (release = r));
  let first = true;
  s.client.session.promptAsync = async (args) => {
    if (first) {
      first = false;
      await gate;
    }
    return real(args);
  };
  handleLine(s, JSON.stringify(frame("1")));
  handleLine(s, JSON.stringify(frame("2")));
  await new Promise((r) => setTimeout(r, 20));
  assert.equal(s.client.calls.prompts.length, 0);
  release();
  await s.queue;
  assert.deepEqual(s.client.calls.prompts.map((p) => p.text), ["msg 1", "msg 2"]);
});

test("a session that is gone (404) passes the message to the newest one; other errors do not", async () => {
  const s = fresh([{ id: "A", time: { updated: 1 } }, { id: "B", time: { updated: 2 } }], {
    promptStatus: (id) => (id === "A" ? 404 : 200),
  });
  typed(s, "m1", "A", 100);
  await deliver(s, frame("a"));
  assert.deepEqual(s.client.calls.prompts.map((p) => p.id), ["A", "B"]);

  const t = fresh([{ id: "A", time: { updated: 1 } }, { id: "B", time: { updated: 2 } }], { promptStatus: () => 400 });
  typed(t, "m1", "A", 100);
  await deliver(t, frame("b"));
  assert.deepEqual(t.client.calls.prompts.map((p) => p.id), ["A"]);
});

test("a repeated frame id is one prompt; bad lines are dropped", async () => {
  const s = fresh([{ id: "A", time: { updated: 1 } }]);
  handleLine(s, JSON.stringify(frame("same")));
  handleLine(s, JSON.stringify(frame("same")));
  handleLine(s, "not json");
  handleLine(s, JSON.stringify({ type: "other", id: "z", content: "x" }));
  handleLine(s, JSON.stringify({ type: "user", id: "y", content: "" }));
  await s.queue;
  assert.equal(s.client.calls.prompts.length, 1);
});

test("messages are prompted in arrival order", async () => {
  const s = fresh([{ id: "A", time: { updated: 1 } }]);
  for (const id of ["1", "2", "3"]) handleLine(s, JSON.stringify(frame(id)));
  await s.queue;
  assert.deepEqual(s.client.calls.prompts.map((p) => p.text), ["msg 1", "msg 2", "msg 3"]);
});

test("the data dir expands ~/ and ignores an XDG_DATA_HOME that is empty or not absolute", () => {
  const saved = process.env.XDG_DATA_HOME;
  try {
    for (const v of ["", "rel/dir", "~x"]) {
      process.env.XDG_DATA_HOME = v;
      assert.equal(dataDir(), path.join(os.homedir(), ".local", "share", "agentmailbus"));
    }
    process.env.XDG_DATA_HOME = "~/x";
    assert.equal(dataDir(), path.join(os.homedir(), "x", "agentmailbus"));
    process.env.XDG_DATA_HOME = "/var/run/../tmp";
    assert.equal(dataDir(), path.join(os.homedir(), ".local", "share", "agentmailbus"));
    process.env.XDG_DATA_HOME = "/abs";
    assert.equal(dataDir(), "/abs/agentmailbus");
  } finally {
    process.env.XDG_DATA_HOME = saved;
  }
});

function send(file, text) {
  return new Promise((resolve, reject) => {
    const c = net.createConnection(file, () => c.end(text));
    c.on("close", resolve);
    c.on("error", reject);
  });
}

async function until(check) {
  for (let i = 0; i < 200 && !check(); i++) await new Promise((r) => setTimeout(r, 10));
  assert.ok(check());
}

test("end to end: each folder listens on its own private socket and prompts its own session", async (t) => {
  const dirA = fs.mkdtempSync(path.join(tmp, "projA-"));
  const dirB = fs.mkdtempSync(path.join(tmp, "projB-"));
  const a = fakeClient([{ id: "A1", time: { updated: 1 } }]);
  const b = fakeClient([{ id: "B1", time: { updated: 1 } }]);
  const hooks = await AgentMailBus({ client: a, directory: dirA });
  await AgentMailBus({ client: b, directory: dirB });
  t.after(() => {
    for (const s of [folderState(dirA), folderState(dirB)]) s.server?.close();
  });
  assert.equal(typeof hooks.event, "function");
  const fa = socketPath(dirA);
  const fb = socketPath(dirB);
  assert.notEqual(fa, fb);
  await until(() => folderState(dirA).listening && folderState(dirB).listening);
  assert.equal(fs.statSync(path.dirname(fa)).mode & 0o777, 0o700);
  assert.equal(fs.lstatSync(fa).mode & 0o777, 0o600);
  await send(fa, JSON.stringify(frame("e1", "to A")) + "\n");
  await send(fb, JSON.stringify(frame("e2", "to B")) + "\n");
  await until(() => a.calls.prompts.length === 1 && b.calls.prompts.length === 1);
  assert.deepEqual(a.calls.prompts[0], { id: "A1", text: "to A", code: 200 });
  assert.deepEqual(b.calls.prompts[0], { id: "B1", text: "to B", code: 200 });
  // A rebuild for the same folder keeps the listener and takes the new client.
  const a2 = fakeClient([{ id: "A1", time: { updated: 1 } }]);
  await AgentMailBus({ client: a2, directory: dirA });
  await send(fa, JSON.stringify(frame("e3", "again")) + "\n");
  await until(() => a2.calls.prompts.length === 1);
});

test("a folder whose listen failed listens on the next build", async () => {
  const dir = fs.mkdtempSync(path.join(tmp, "blocked-"));
  const file = socketPath(dir);
  fs.mkdirSync(file, { recursive: true }); // a directory in the way: listen fails
  await AgentMailBus({ client: fakeClient([]), directory: dir });
  await until(() => folderState(dir).server === null);
  fs.rmdirSync(file);
  await AgentMailBus({ client: fakeClient([]), directory: dir });
  await until(() => folderState(dir).listening);
  folderState(dir).server.close();
});

test("a socket directory open to other users is refused", async () => {
  const saved = process.env.XDG_DATA_HOME;
  const loose = path.join(tmp, "loose");
  fs.mkdirSync(path.join(loose, "agentmailbus", "opencode"), { recursive: true });
  fs.chmodSync(path.join(loose, "agentmailbus", "opencode"), 0o755);
  process.env.XDG_DATA_HOME = loose;
  try {
    const dir = fs.mkdtempSync(path.join(tmp, "loose-"));
    const hooks = await AgentMailBus({ client: fakeClient([]), directory: dir });
    assert.deepEqual(hooks, {});
    assert.equal(fs.existsSync(socketPath(dir)), false);
  } finally {
    process.env.XDG_DATA_HOME = saved;
  }
});

test("the folder key is the real path's", () => {
  const dir = fs.mkdtempSync(path.join(tmp, "real-"));
  const link = path.join(tmp, `link-${n++}`);
  fs.symlinkSync(dir, link);
  assert.equal(folderKey(link), folderKey(dir));
});

test("a socket path too long for a Unix socket is refused", async () => {
  const saved = process.env.XDG_DATA_HOME;
  process.env.XDG_DATA_HOME = "/" + "x".repeat(90);
  try {
    const dir = fs.mkdtempSync(path.join(tmp, "long-"));
    const hooks = await AgentMailBus({ client: fakeClient([]), directory: dir });
    assert.deepEqual(hooks, {});
  } finally {
    process.env.XDG_DATA_HOME = saved;
  }
});

test("a thousand bus messages do not push out the person's last prompt", async () => {
  const s = fresh([{ id: "A", time: { updated: 1 } }, { id: "B", time: { updated: 5000 } }]);
  typed(s, "pA", "A", 100);
  for (let i = 0; i < 1100; i++) {
    onEvent(s, userMsg(`bus${i}`, "A", 200 + i));
    onEvent(s, busPart(`bus${i}`, "A"));
    onEvent(s, userMsg(`bus${i}`, "A", 200 + i)); // opencode's later update of the same message
  }
  await deliver(s, frame("x"));
  assert.equal(s.client.calls.prompts.at(-1).id, "A");
});

test("a session archived while known is not prompted even when a lookup fails", async () => {
  const s = fresh([{ id: "A", time: { updated: 1 } }, { id: "B", time: { updated: 2 } }]);
  typed(s, "m1", "A", 100);
  await deliver(s, frame("w")); // A is confirmed a root
  onEvent(s, { type: "session.updated", properties: { info: { id: "A", time: { updated: 3, archived: 3 } } } });
  s.client.session.get = async () => ({ error: { code: 500 }, response: { status: 500 } });
  s.client.session.list = async () => ({ data: [{ id: "B", time: { updated: 2 } }], response: { status: 200 } });
  await deliver(s, frame("x"));
  assert.equal(s.client.calls.prompts.at(-1).id, "B");
});

test("a person's prompt with an @mention still counts, whatever order its parts come in", async () => {
  const synth = (mid, sid) => ({
    type: "message.part.updated",
    properties: { part: { id: `s${mid}`, type: "text", messageID: mid, sessionID: sid, text: "Called the Read tool with the following input", synthetic: true } },
  });
  const s = fresh([{ id: "A", time: { updated: 1 } }, { id: "B", time: { updated: 2 } }, { id: "C", time: { updated: 3 } }]);
  typed(s, "m1", "A", 100);
  onEvent(s, userMsg("m2", "B", 200));
  onEvent(s, personPart("m2", "B"));
  onEvent(s, synth("m2", "B"));
  await deliver(s, frame("x"));
  assert.equal(s.client.calls.prompts.at(-1).id, "B");
  onEvent(s, userMsg("m3", "A", 300));
  onEvent(s, synth("m3", "A"));
  onEvent(s, personPart("m3", "A"));
  await deliver(s, frame("y"));
  assert.equal(s.client.calls.prompts.at(-1).id, "A");
});

test("after a thousand prompts a new one still moves the target", async () => {
  const s = fresh([{ id: "A", time: { updated: 1 } }, { id: "B", time: { updated: 2 } }]);
  for (let i = 0; i < 1100; i++) typed(s, `a${i}`, "A", 100 + i);
  typed(s, "b", "B", 5000);
  await deliver(s, frame("x"));
  assert.equal(s.client.calls.prompts.at(-1).id, "B");
});

test("a deleted session is forgotten as a root", async () => {
  const s = fresh([{ id: "A", time: { updated: 1 } }, { id: "B", time: { updated: 2 } }]);
  typed(s, "m1", "A", 100);
  await deliver(s, frame("w")); // A confirmed a root
  onEvent(s, { type: "session.deleted", properties: { info: { id: "A" } } });
  assert.equal(folderState(s.directory).roots.has("A"), false);
});

test("a 404 never retries into the same session", async () => {
  const s = fresh([{ id: "A", time: { updated: 2 } }, { id: "B", time: { updated: 1 } }], {
    promptStatus: (id) => (id === "A" ? 404 : 200),
  });
  await deliver(s, frame("x")); // the list still shows A first
  assert.deepEqual(s.client.calls.prompts.map((p) => p.id), ["A", "B"]);
});

test("a root seen through the list keeps the target when a later lookup fails", async () => {
  const s = fresh([{ id: "X", time: { updated: 1 } }]);
  await deliver(s, frame("w")); // X picked through the list
  typed(s, "m1", "X", 100);
  s.client.session.get = async () => ({ error: { code: 500 }, response: { status: 500 } });
  s.client.session.list = async () => ({ data: [{ id: "Y", time: { updated: 9 } }, { id: "X", time: { updated: 1 } }], response: { status: 200 } });
  await deliver(s, frame("x"));
  assert.equal(s.client.calls.prompts.at(-1).id, "X");
});

test("an archive seen while a lookup is in flight wins over the lookup", async () => {
  const s = fresh([{ id: "X", time: { updated: 1 } }, { id: "Y", time: { updated: 2 } }]);
  typed(s, "m1", "X", 100);
  let release;
  const gate = new Promise((r) => (release = r));
  s.client.session.get = async ({ path: { id } }) => {
    await gate;
    return { data: { id, time: { updated: 1 } }, response: { status: 200 } };
  };
  const done = deliver(s, frame("x"));
  await new Promise((r) => setTimeout(r, 10));
  onEvent(s, { type: "session.updated", properties: { info: { id: "X", time: { updated: 3, archived: 3 } } } });
  release();
  await done;
  assert.equal(s.client.calls.prompts.at(-1).id, "Y");
  assert.equal(s.roots.has("X"), false);
});

test("a session archived while it is being made is not prompted", async () => {
  const s = fresh([]);
  const create = s.client.session.create;
  s.client.session.create = async (args) => {
    const res = await create(args);
    onEvent(s, { type: "session.updated", properties: { info: { id: res.data.id, time: { updated: 1, archived: 1 } } } });
    return res;
  };
  await deliver(s, frame("x"));
  assert.equal(s.client.calls.prompts.length, 0);
});

test("messages waiting behind a stuck call are bounded", async () => {
  const s = fresh([{ id: "A", time: { updated: 1 } }]);
  let release;
  const gate = new Promise((r) => (release = r));
  const real = s.client.session.promptAsync;
  s.client.session.promptAsync = async (args) => {
    await gate;
    return real(args);
  };
  for (let i = 0; i < 1100; i++) handleLine(s, JSON.stringify(frame(`q${i}`)));
  assert.equal(s.waiting, 1000);
  release();
  await s.queue;
  assert.equal(s.client.calls.prompts.length, 1000);
  assert.equal(s.waiting, 0);
});

test("a prompt 404 marks the session gone for good", async () => {
  const s = fresh([{ id: "A", time: { updated: 2 } }, { id: "B", time: { updated: 1 } }], {
    promptStatus: (id) => (id === "A" ? 404 : 200),
  });
  await deliver(s, frame("x"));
  assert.equal(s.gone.has("A"), true);
});

test("an error after listening keeps the listener; a removed socket is rebuilt on the next build", async () => {
  const dir = fs.mkdtempSync(path.join(tmp, "relisten-"));
  await AgentMailBus({ client: fakeClient([]), directory: dir });
  const s = folderState(dir);
  await until(() => s.listening);
  const first = s.server;
  s.server.emit("error", new Error("EMFILE"));
  assert.equal(s.server, first);
  fs.unlinkSync(socketPath(dir));
  await AgentMailBus({ client: fakeClient([]), directory: dir });
  await until(() => folderState(dir).listening && fs.existsSync(socketPath(dir)));
  assert.notEqual(folderState(dir).server, first);
  folderState(dir).server.close();
});

test("a 404 on the retry marks that session gone too", async () => {
  const s = fresh([{ id: "A", time: { updated: 3 } }, { id: "B", time: { updated: 2 } }, { id: "C", time: { updated: 1 } }], {
    promptStatus: (id) => (id === "C" ? 200 : 404),
  });
  await deliver(s, frame("x"));
  assert.equal(s.gone.has("A") && s.gone.has("B"), true);
});
