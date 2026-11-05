#!/usr/bin/env node
// Last Christmas: Frostbite - online relay.
// Zero dependencies (Node 18+). Lets two players on different networks/countries play together WITHOUT any
// port forwarding: both connect OUT to this server with a room code, the server forwards their game packets.
// It does not run the game; the host's game does. If the host leaves it helps the other player take over.
//
//   node relay_server.js            (PORT env var, default 8787)
//
// Control messages are JSON text frames, game packets are binary frames:
//   client->server  [int32 LE target id (0 = everyone else)][u8 mode][u8 channel][payload...]
//   server->client  [int32 LE sender id][u8 mode][u8 channel][payload...]
"use strict";
const http = require("http");
const crypto = require("crypto");

const PORT = parseInt(process.env.PORT || "8787", 10);
const GRACE_MS = parseInt(process.env.RECONNECT_GRACE_MS || "45000", 10); // time a dropped player keeps their slot
const ORPHAN_MS = 90000;           // time an owner-less room waits for someone to take over as host
const MAX_ROOMS = 2000;
const MAX_PEERS = 4;
const MAX_FRAME = 64 * 1024;
const MAX_MSGS_PER_SEC = 600;
const CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

const rooms = new Map(); // code -> room

function makeCode() {
  for (let tries = 0; tries < 50; tries++) {
    let c = "";
    for (let i = 0; i < 5; i++) c += CODE_CHARS[crypto.randomInt(CODE_CHARS.length)];
    if (!rooms.has(c)) return c;
  }
  return null;
}
const token = () => crypto.randomBytes(12).toString("hex");
const clean = (s) => String(s || "Player").replace(/[^\p{L}\p{N} _.-]/gu, "").slice(0, 16) || "Player";
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);

// ------------------------------------------------------------------ minimal WebSocket framing
class Conn {
  constructor(socket) {
    this.socket = socket; this.buf = Buffer.alloc(0); this.frag = null; this.fragOp = 0;
    this.alive = true; this.slot = null; this.room = null;
    this.msgCount = 0; this.msgWindow = Date.now();
    socket.setNoDelay(true);
    socket.on("data", (d) => this.onData(d));
    socket.on("close", () => this.onClose());
    socket.on("error", () => this.onClose());
  }
  onData(d) {
    this.buf = Buffer.concat([this.buf, d]);
    for (;;) {
      if (this.buf.length < 2) return;
      const b0 = this.buf[0], b1 = this.buf[1];
      const fin = (b0 & 0x80) !== 0, op = b0 & 0x0f, masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f, off = 2;
      if (len === 126) { if (this.buf.length < 4) return; len = this.buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (this.buf.length < 10) return; len = Number(this.buf.readBigUInt64BE(2)); off = 10; }
      if (!masked || len > MAX_FRAME) return this.kill();
      if (this.buf.length < off + 4 + len) return;
      const mask = this.buf.subarray(off, off + 4);
      const payload = Buffer.from(this.buf.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < len; i++) payload[i] ^= mask[i & 3];
      this.buf = this.buf.subarray(off + 4 + len);
      if (op >= 8) { this.control(op, payload); continue; }
      if (op === 0) {
        if (!this.frag) return this.kill();
        this.frag.push(payload);
        if (fin) { const m = Buffer.concat(this.frag); const o = this.fragOp; this.frag = null; this.message(o, m); }
      } else if (!fin) { this.frag = [payload]; this.fragOp = op; }
      else this.message(op, payload);
    }
  }
  control(op, payload) {
    if (op === 8) { this.sendFrame(8, payload.subarray(0, 2)); this.kill(); }
    else if (op === 9) this.sendFrame(10, payload);
  }
  sendFrame(op, payload) {
    if (!this.alive) return;
    const n = payload.length;
    let head;
    if (n < 126) head = Buffer.from([0x80 | op, n]);
    else if (n < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(n, 2); }
    else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeBigUInt64BE(BigInt(n), 2); }
    this.socket.write(Buffer.concat([head, payload]));
  }
  sendJson(obj) { this.sendFrame(1, Buffer.from(JSON.stringify(obj))); }
  sendBinary(buf) {
    if (this.socket.writableLength > 512 * 1024 && buf[4] < 2) return; // backlog: drop unreliable game packets (mode 0/1)
    this.sendFrame(2, buf);
  }
  message(op, payload) {
    const now = Date.now();
    if (now - this.msgWindow > 1000) { this.msgWindow = now; this.msgCount = 0; }
    if (++this.msgCount > MAX_MSGS_PER_SEC) return;
    if (op === 2) return relayPacket(this, payload);
    if (op !== 1) return;
    let m; try { m = JSON.parse(payload.toString("utf8")); } catch { return; }
    handleControl(this, m);
  }
  kill() { if (this.alive) { this.alive = false; try { this.socket.destroy(); } catch {} this.onClose(); } }
  onClose() {
    if (this._closed) return; this._closed = true; this.alive = false;
    dropConn(this);
  }
}

// ------------------------------------------------------------------ rooms
// slot: {id, name, token, conn|null, graceTimer, isHost}
function sendTo(slot, obj) { if (slot && slot.conn) slot.conn.sendJson(obj); }
function broadcast(room, obj, exceptId) { for (const s of allSlots(room)) if (s.id !== exceptId) sendTo(s, obj); }
function allSlots(room) { return [room.host, ...room.peers.values()].filter(Boolean); }
function bind(conn, room, slot) { conn.room = room; conn.slot = slot; slot.conn = conn; clearTimeout(slot.graceTimer); slot.graceTimer = null; }

function deleteRoomLater(room) {
  clearTimeout(room.orphanTimer);
  room.orphanTimer = setTimeout(() => { rooms.delete(room.code); log("room", room.code, "expired"); }, ORPHAN_MS);
}

function removeSlot(room, slot, reason) {
  if (slot.isHost) {
    room.host = null;
    room.state = "orphan";
    for (const p of room.peers.values()) sendTo(p, { t: "host_lost", reason });
    deleteRoomLater(room);
    log("room", room.code, "host lost:", reason);
  } else {
    room.peers.delete(slot.id);
    sendTo(room.host, { t: "peer_left", id: slot.id, reason });
    if (!room.host && room.peers.size === 0) deleteRoomLater(room); // keep it briefly so a leaving player can come back as host
  }
}

function dropConn(conn) {
  const room = conn.room, slot = conn.slot;
  conn.room = null; conn.slot = null;
  if (!room || !slot || slot.conn !== conn) return;
  slot.conn = null;
  if (slot.leaving) { removeSlot(room, slot, "left"); return; }
  // unexpected drop: keep the slot for a while so the player can reconnect
  broadcast(room, { t: "peer_unstable", id: slot.id }, slot.id);
  slot.graceTimer = setTimeout(() => {
    if (slot.conn) return;
    removeSlot(room, slot, "timeout");
  }, GRACE_MS);
  log("room", room.code, "slot", slot.id, "dropped (grace)");
}

function handleControl(conn, m) {
  switch (m.t) {
    case "ping": return conn.sendJson({ t: "pong", n: m.n });
    case "host": {
      if (conn.room) return;
      if (rooms.size >= MAX_ROOMS) return conn.sendJson({ t: "error", reason: "Server is busy, try again later." });
      const code = makeCode();
      const slot = { id: 1, name: clean(m.name), token: token(), conn: null, graceTimer: null, isHost: true };
      const room = { code, host: slot, peers: new Map(), nextId: 2, state: "open", orphanTimer: null };
      rooms.set(code, room);
      bind(conn, room, slot);
      log("room", code, "created by", slot.name);
      return conn.sendJson({ t: "hosted", code, id: 1, token: slot.token });
    }
    case "join": {
      if (conn.room) return;
      const room = rooms.get(String(m.code || "").toUpperCase().trim());
      if (!room) return conn.sendJson({ t: "error", reason: "Room not found. Check the code." });
      if (!room.host) return conn.sendJson({ t: "error", reason: "That room has no host right now.", retry: true });
      if (room.peers.size >= MAX_PEERS - 1) return conn.sendJson({ t: "error", reason: "Room is full." });
      const slot = { id: room.nextId++, name: clean(m.name), token: token(), conn: null, graceTimer: null, isHost: false };
      room.peers.set(slot.id, slot);
      bind(conn, room, slot);
      conn.sendJson({ t: "joined", id: slot.id, token: slot.token, code: room.code, host_name: room.host.name });
      sendTo(room.host, { t: "peer_joined", id: slot.id, name: slot.name });
      return log("room", room.code, slot.name, "joined as", slot.id);
    }
    case "resume": {
      if (conn.room) return;
      const room = rooms.get(String(m.code || "").toUpperCase());
      if (!room) return conn.sendJson({ t: "resume_failed", reason: "Room no longer exists." });
      const slot = Number(m.id) === 1 ? room.host : room.peers.get(Number(m.id));
      if (!slot || slot.token !== m.token) return conn.sendJson({ t: "resume_failed", reason: "Slot expired." });
      if (slot.conn) { slot.conn.slot = null; slot.conn.room = null; try { slot.conn.socket.destroy(); } catch {} }
      bind(conn, room, slot);
      conn.sendJson({ t: "resumed", id: slot.id, code: room.code });
      broadcast(room, { t: "peer_back", id: slot.id }, slot.id);
      return log("room", room.code, "slot", slot.id, "resumed");
    }
    case "become_host": { // host migration: the first remaining player to ask becomes the new host (Godot id 1)
      if (conn.room) return;
      const room = rooms.get(String(m.code || "").toUpperCase());
      if (!room) return conn.sendJson({ t: "error", reason: "Room no longer exists." });
      if (room.host) return conn.sendJson({ t: "error", reason: "host_taken", retry: true });
      const slot = { id: 1, name: clean(m.name), token: token(), conn: null, graceTimer: null, isHost: true };
      // everyone else must re-join (fresh Godot peer ids against the new host)
      for (const p of room.peers.values()) { clearTimeout(p.graceTimer); sendTo(p, { t: "host_changed", name: slot.name }); }
      room.peers.clear(); room.nextId = 2; room.host = slot; room.state = "open";
      clearTimeout(room.orphanTimer);
      bind(conn, room, slot);
      log("room", room.code, "new host", slot.name);
      return conn.sendJson({ t: "hosted", code: room.code, id: 1, token: slot.token, migrated: true });
    }
    case "leave": { if (conn.slot) conn.slot.leaving = true; return conn.kill(); }
  }
}

function relayPacket(conn, buf) {
  const room = conn.room, slot = conn.slot;
  if (!room || !slot || buf.length < 6) return;
  const target = buf.readInt32LE(0);
  const out = Buffer.from(buf);
  out.writeInt32LE(slot.id, 0);
  if (target === 0) {
    for (const s of allSlots(room)) if (s.id !== slot.id && s.conn) s.conn.sendBinary(out);
  } else {
    const t = target === 1 ? room.host : room.peers.get(target);
    if (t && t.conn && t.id !== slot.id) t.conn.sendBinary(out);
  }
}

// ------------------------------------------------------------------ http + upgrade
const server = http.createServer((req, res) => {
  res.writeHead(200, { "content-type": "application/json" });
  res.end(JSON.stringify({ service: "frostbite-relay", rooms: rooms.size, ok: true }));
});
server.on("upgrade", (req, socket) => {
  const key = req.headers["sec-websocket-key"];
  if (!key || (req.headers.upgrade || "").toLowerCase() !== "websocket") return socket.destroy();
  const accept = crypto.createHash("sha1").update(key + GUID).digest("base64");
  socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n");
  const conn = new Conn(socket);
  conn.pingTimer = setInterval(() => { if (!conn.alive) return clearInterval(conn.pingTimer); conn.sendFrame(9, Buffer.alloc(0)); }, 20000);
  socket.on("close", () => clearInterval(conn.pingTimer));
});
server.listen(PORT, () => log("frostbite relay listening on", PORT));
