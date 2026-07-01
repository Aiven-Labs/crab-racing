/**
 * Crab Racing — multiplayer server
 *
 * Architecture:
 *   Browser  ──WebSocket──►  server.js  ──produce──►  Kafka topic "race-moves"
 *                                              ◄──consume──
 *                         server applies move, broadcasts updated state to all WS clients
 *
 * Start: node server.js
 * Env:   see .env.example
 */
require('dotenv').config();

const http      = require('http');
const fs        = require('fs');
const path      = require('path');
const WebSocket = require('ws');
const { Kafka } = require('kafkajs');

const MIME = {
  '.html': 'text/html',
  '.js':   'text/javascript',
  '.css':  'text/css',
  '.png':  'image/png',
  '.ico':  'image/x-icon',
};

function serveStatic(req, res) {
  // "/" → game.html, anything else → filename in project dir
  const urlPath = req.url === '/' ? '/game.html' : req.url.split('?')[0];
  const filePath = path.join(__dirname, urlPath);
  const ext = path.extname(filePath);

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(err.code === 'ENOENT' ? 404 : 500);
      res.end(err.code === 'ENOENT' ? '404 Not Found' : '500 Internal Error');
      return;
    }
    res.writeHead(200, {
      'Content-Type': MIME[ext] || 'application/octet-stream',
      'Access-Control-Allow-Origin': '*',
    });
    res.end(data);
  });
}

const PORT     = process.env.PORT         || 3000;
const BROKER   = process.env.KAFKA_BROKER || 'localhost:9092';
const TOPIC    = 'race-moves';

// Aiven Kafka uses SSL client-certificate (mTLS) auth. The certs are provided
// as PEM strings in env vars (KAFKA_ACCESS_KEY / KAFKA_ACCESS_CERT / KAFKA_CA_CERT).
// KAFKA_SECURITY_PROTOCOL is "SSL" for Aiven, "PLAINTEXT" for local docker Kafka.
const SECURITY_PROTOCOL = process.env.KAFKA_SECURITY_PROTOCOL || 'PLAINTEXT';
const KAFKA_ACCESS_KEY  = process.env.KAFKA_ACCESS_KEY;
const KAFKA_ACCESS_CERT = process.env.KAFKA_ACCESS_CERT;
const KAFKA_CA_CERT     = process.env.KAFKA_CA_CERT;
const USE_SSL           = SECURITY_PROTOCOL.toUpperCase() === 'SSL';

/**
 * Aiven exposes cert values either inline (PEM string in the env var) or as a
 * path to a .pem file. Accept both so it works locally and on the platform.
 */
function readPem(value) {
  if (!value) return undefined;
  const looksLikePem = value.includes('-----BEGIN');
  if (looksLikePem) return value.replace(/\\n/g, '\n'); // un-escape newlines if flattened
  return fs.readFileSync(value, 'utf8');                // otherwise treat as a file path
}

// ── game constants (mirrors game.html) ──────────────────────────────────────────
const LANES = 3;
const ZONES = [
  { len: 5 }, { len: 8 }, { len: 12 }, { len: 10 }, { len: 8 }, { len: 3 },
];
const TOTAL_CELLS = ZONES.reduce((s, z) => s + z.len, 0); // 46
const FINISH_CELL = TOTAL_CELLS - 3;                        // 43

const OBSTACLES = [
  { cell: 7,  lane: 0, effect: 'stun',  turns: 2, msg: 'FELL IN A HOLE!'     },
  { cell: 9,  lane: 2, effect: 'stun',  turns: 1, msg: 'STUNG!'               },
  { cell: 11, lane: 1, effect: 'stun',  turns: 2, msg: 'FELL IN A HOLE!'     },
  { cell: 15, lane: 1, effect: 'block',            msg: 'BLOCKED BY ROCK!'    },
  { cell: 17, lane: 0, effect: 'push',             msg: 'TOURIST KICKED YOU!' },
  { cell: 19, lane: 2, effect: 'block',            msg: 'BLOCKED BY ROCK!'    },
  { cell: 22, lane: 1, effect: 'push',             msg: 'TOURIST KICKED YOU!' },
  { cell: 26, lane: 0, effect: 'stun',  turns: 1, msg: 'TANGLED IN A NET!'   },
  { cell: 28, lane: 2, effect: 'stun',  turns: 1, msg: 'TANGLED IN A NET!'   },
  { cell: 30, lane: 1, effect: 'stun',  turns: 1, msg: 'STUNG!'               },
  { cell: 33, lane: 0, effect: 'block',            msg: 'BLOCKED BY ROCK!'    },
  { cell: 36, lane: 2, effect: 'block',            msg: 'BLOCKED BY ROCK!'    },
  { cell: 39, lane: 1, effect: 'stun',  turns: 1, msg: 'TANGLED IN A NET!'   },
  { cell: 41, lane: 0, effect: 'stun',  turns: 1, msg: 'STUNG!'               },
];
const OBS_MAP = new Map(OBSTACLES.map(o => [`${o.cell},${o.lane}`, o]));

// ── server state ────────────────────────────────────────────────────────────────
const rooms   = new Map(); // roomId → Room
const clients = new Map(); // ws     → { playerId, roomId, name }

function makeRoom(id) {
  return { id, players: new Map(), started: false, finished: false };
}

function makePlayer(id, name, colorIdx) {
  return {
    id, name, colorIdx,
    cell: 0, lane: colorIdx % LANES,
    stunTurns: 0, won: false,
  };
}

/** Serialisable snapshot of all players in a room */
function snapshot(room) {
  return [...room.players.values()].map(
    ({ id, name, colorIdx, cell, lane, stunTurns, won }) =>
      ({ id, name, colorIdx, cell, lane, stunTurns, won })
  );
}

function broadcast(roomId, msg) {
  const data = JSON.stringify(msg);
  for (const [ws, info] of clients)
    if (info.roomId === roomId && ws.readyState === WebSocket.OPEN)
      ws.send(data);
}

function sendTo(ws, msg) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}

/** Server-authoritative move application. Returns a result object. */
function applyMove(room, playerId, dx, dlane) {
  const p = room.players.get(playerId);
  if (!p || p.won || !room.started) return { ok: false };

  // Consume a stun turn instead of moving
  if (p.stunTurns > 0) {
    p.stunTurns--;
    return { ok: true, effect: 'stunned', stunLeft: p.stunTurns };
  }

  const nc = p.cell + dx;
  const nl = p.lane + dlane;
  if (nc < 0 || nc >= TOTAL_CELLS || nl < 0 || nl >= LANES) return { ok: false };

  const obs = OBS_MAP.get(`${nc},${nl}`);
  if (obs?.effect === 'block') {
    return { ok: true, effect: 'block', msg: obs.msg };
  }

  // Commit move
  p.cell = nc;
  p.lane = nl;

  let effect = null;
  if (obs?.effect === 'stun') {
    p.stunTurns = obs.turns;
    effect = { type: 'stun', msg: obs.msg, turns: obs.turns };
  } else if (obs?.effect === 'push') {
    p.cell = Math.max(0, p.cell - 1);
    effect = { type: 'push', msg: obs.msg };
  }

  if (p.cell >= FINISH_CELL) { p.won = true; room.finished = true; }

  return { ok: true, effect, won: p.won };
}

/** 3-2-1 countdown, then broadcast 'go' */
function startCountdown(room, roomId) {
  room.started = true;
  let count = 3;
  const tick = setInterval(() => {
    broadcast(roomId, { type: 'countdown', count });
    if (count-- <= 0) {
      clearInterval(tick);
      broadcast(roomId, { type: 'go', players: snapshot(room) });
    }
  }, 1000);
}

// ── main ────────────────────────────────────────────────────────────────────────
async function main() {
  // Kafka config — supports both local (PLAINTEXT) and Aiven (SSL client certs / mTLS)
  const kafkaCfg = {
    clientId: 'crab-racing',
    brokers:  [BROKER],
    logLevel: 1, // WARN
  };
  if (USE_SSL) {
    kafkaCfg.ssl = {
      ca:   [readPem(KAFKA_CA_CERT)],   // Aiven CA that signed the broker cert
      key:  readPem(KAFKA_ACCESS_KEY),  // this client's private key
      cert: readPem(KAFKA_ACCESS_CERT), // this client's public cert
    };
  }
  const kafka = new Kafka(kafkaCfg);

  // Ensure topic exists
  const admin = kafka.admin();
  await admin.connect();
  const existingTopics = await admin.listTopics();
  if (!existingTopics.includes(TOPIC)) {
    await admin.createTopics({
      topics: [{ topic: TOPIC, numPartitions: 4, replicationFactor: 1 }],
    });
    console.log(`✓ Created Kafka topic "${TOPIC}"`);
  }
  await admin.disconnect();

  // Producer: publishes move events
  const producer = kafka.producer();
  await producer.connect();
  console.log('✓ Kafka producer connected');

  // Consumer: unique group ID so this server instance gets every message.
  // fromBeginning:false means we only process events produced after startup
  // (no replaying finished races on restart).
  const consumer = kafka.consumer({ groupId: `crab-srv-${Date.now()}` });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      let event;
      try { event = JSON.parse(message.value.toString()); }
      catch { return; }

      if (event.action !== 'move') return;

      const room = rooms.get(event.roomId);
      if (!room) return;

      const result = applyMove(room, event.playerId, event.dx, event.dlane);
      if (!result.ok && result.effect !== 'block') return; // truly invalid — skip

      // Broadcast updated state + result to everyone in the room
      broadcast(event.roomId, {
        type:     'state',
        players:  snapshot(room),
        result,
        movedId:  event.playerId,
      });

      // Announce winner
      if (result.won) {
        const p = room.players.get(event.playerId);
        broadcast(event.roomId, {
          type:     'win',
          playerId: event.playerId,
          name:     p?.name ?? '?',
        });
      }
    },
  });
  console.log('✓ Kafka consumer listening');

  // ── HTTP + WebSocket server ────────────────────────────────────────────────────
  const server = http.createServer(serveStatic);

  const wss = new WebSocket.Server({ server });

  wss.on('connection', (ws) => {
    console.log('[ws] client connected');

    ws.on('message', async (raw) => {
      let msg;
      try { msg = JSON.parse(raw); } catch { return; }

      // ── join ────────────────────────────────────────────────────────────────
      if (msg.type === 'join') {
        const name   = (msg.name   || 'Crab').slice(0, 20);
        const roomId = (msg.roomId || 'room1').slice(0, 32);

        let room = rooms.get(roomId);
        if (!room) { room = makeRoom(roomId); rooms.set(roomId, room); }

        if (room.started) {
          sendTo(ws, { type: 'error', msg: 'Race already started — try another room' });
          return;
        }

        const playerId = `p${Date.now().toString(36)}${Math.random().toString(36).slice(2, 5)}`;
        const colorIdx = room.players.size % 4;
        room.players.set(playerId, makePlayer(playerId, name, colorIdx));
        clients.set(ws, { playerId, roomId, name });

        sendTo(ws, { type: 'joined', playerId, colorIdx, players: snapshot(room) });
        broadcast(roomId, { type: 'player-joined', name, players: snapshot(room) });
        console.log(`[room:${roomId}] ${name} joined (${room.players.size} players)`);

        // Auto-start when 2+ players are in
        if (room.players.size >= 2 && !room.started) {
          startCountdown(room, roomId);
        }
        return;
      }

      // ── move ────────────────────────────────────────────────────────────────
      if (msg.type === 'move') {
        const info = clients.get(ws);
        if (!info) return;
        const { dx = 0, dlane = 0 } = msg;

        // Publish to Kafka — the consumer will apply the move and broadcast state
        await producer.send({
          topic: TOPIC,
          messages: [{
            key:   info.roomId,
            value: JSON.stringify({
              action:   'move',
              roomId:   info.roomId,
              playerId: info.playerId,
              dx, dlane,
              ts: Date.now(),
            }),
          }],
        }).catch(err => console.error('[kafka] produce error:', err.message));
        return;
      }

      // ── solo-multi: 1-player room, start immediately (for testing) ──────────
      if (msg.type === 'solo-start') {
        const info = clients.get(ws);
        if (!info) return;
        const room = rooms.get(info.roomId);
        if (!room || room.started) return;
        room.started = true;
        broadcast(info.roomId, { type: 'go', players: snapshot(room) });
      }
    });

    ws.on('close', () => {
      const info = clients.get(ws);
      if (info) {
        const room = rooms.get(info.roomId);
        if (room) {
          room.players.delete(info.playerId);
          broadcast(info.roomId, {
            type:     'player-left',
            playerId: info.playerId,
            players:  snapshot(room),
          });
          if (room.players.size === 0) {
            rooms.delete(info.roomId);
            console.log(`[room:${info.roomId}] closed (empty)`);
          }
        }
        clients.delete(ws);
        console.log(`[ws] ${info.name} disconnected`);
      }
    });

    ws.on('error', err => console.error('[ws] error:', err.message));
  });

  server.listen(PORT, () => {
    console.log(`\n🦀  Crab Racing\n    http://localhost:${PORT}  ← open in browser\n    ws://localhost:${PORT}   ← WebSocket\n`);
    if (!USE_SSL) {
      console.log('    (using local Kafka — set KAFKA_SECURITY_PROTOCOL=SSL + certs for Aiven)\n');
    }
  });
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
