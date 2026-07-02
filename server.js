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
const Valkey    = require('iovalkey');

const MIME = {
  '.html': 'text/html',
  '.js':   'text/javascript',
  '.css':  'text/css',
  '.png':  'image/png',
  '.ico':  'image/x-icon',
};

function sendJson(res, code, body) {
  res.writeHead(code, {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
  });
  res.end(JSON.stringify(body));
}

/** POST /leaderboard body: { name, timeCs } → records score, returns board. */
function postLeaderboard(req, res) {
  let raw = '';
  req.on('data', chunk => {
    raw += chunk;
    if (raw.length > 1024) req.destroy(); // tiny payload — reject anything bigger
  });
  req.on('end', async () => {
    let body;
    try { body = JSON.parse(raw); } catch { return sendJson(res, 400, { error: 'bad json' }); }
    const name     = String(body.name || 'Crab').slice(0, 20);
    const timeCs   = Number(body.timeCs);
    const colorIdx = Number(body.colorIdx) || 0;
    // The obstacle seed the race was run on — kept so a board entry can be
    // replayed on the identical track. May be absent from older clients.
    const seed     = body.seed != null ? String(body.seed).slice(0, 32) : '';
    await recordScore(name, timeCs, colorIdx, seed);
    sendJson(res, 200, await getLeaderboard());
  });
}

function serveStatic(req, res) {
  const route = req.url.split('?')[0];

  // Client config — the browser fetches this on load to learn the WebSocket URL.
  // Empty unless PUBLIC_WS_URL is set; when empty the client defaults to the
  // origin that served the page, so no config is needed for same-host deploys.
  if (route === '/config') {
    sendJson(res, 200, { wsUrl: PUBLIC_WS_URL });
    return;
  }

  // Leaderboard API — GET reads the board, POST records a finish.
  // Used by both the lobby (home page) and the end screen, in solo and multi.
  if (route === '/leaderboard') {
    if (req.method === 'POST') { postLeaderboard(req, res); return; }
    getLeaderboard().then(board => sendJson(res, 200, board));
    return;
  }

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

// Optional public WebSocket URL handed to the browser via GET /config. Leave
// unset for same-host deploys (the client falls back to the page's own origin);
// set it only when the WS server is reached at a different host than the static
// files, e.g. PUBLIC_WS_URL=wss://your-service-3000.aiven.app
const PUBLIC_WS_URL = process.env.PUBLIC_WS_URL || '';

// Valkey (Redis-compatible) — ephemeral leaderboard store. Aiven for Valkey
// gives you a single connection string; locally it's redis://valkey:6379.
// Any of redis:// rediss:// valkey:// valkeys:// is accepted; the "s" variants
// (rediss / valkeys) use TLS.
//
// Aiven's TLS cert is signed by a per-project CA that isn't in the system trust
// store, so we hand iovalkey that CA explicitly. VALKEY_CA_CERT accepts either
// the PEM text inline or a path to a .pem file (same as the Kafka certs below).
const VALKEY_URI     = process.env.VALKEY_URI || 'redis://localhost:6379';
const VALKEY_CA_CERT = process.env.VALKEY_CA_CERT;

// Aiven hands out valkey:// / valkeys:// URIs; iovalkey (ioredis under the hood)
// only speaks redis:// / rediss://. Normalise the scheme and report whether it's
// a TLS URI (rediss / valkeys), so one connection string covers every form.
function parseValkeyUri(uri) {
  const m = /^(rediss?|valkeys?):\/\//i.exec(uri);
  const scheme = (m?.[1] || 'redis').toLowerCase();
  const tls = scheme === 'rediss' || scheme === 'valkeys';
  // Rewrite valkey → redis so the client accepts it.
  const url = uri.replace(/^valkeys?:\/\//i, tls ? 'rediss://' : 'redis://');
  return { url, tls };
}
const LB_KEY      = 'leaderboard';        // sorted set: member = name, score = time (centiseconds)
const LB_COLOR_KEY = 'leaderboard:colors'; // hash: name → crab colorIdx
const LB_SEED_KEY  = 'leaderboard:seeds';  // hash: name → obstacle seed for that run
const LB_MAX      = 10;                    // keep only the top N
const LB_TTL_SECS = 60 * 60;               // ephemeral: whole board expires after 1h idle

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

// Obstacles are placed procedurally from a per-room seed so every race gets a
// fresh but fair layout. This server is authoritative: it picks the seed (or
// honours a custom one from the joining player) and ships it to every client so
// they render/simulate the identical track.
//
// IMPORTANT: keep this generator byte-for-byte identical to the one in game.html
// — the same seed must produce the same obstacles on both sides.
const OBSTACLE_KINDS = [
  { type: 'hole',  effect: 'stun',  turns: 2, msg: 'FELL IN A HOLE!'     },
  { type: 'jelly', effect: 'stun',  turns: 1, msg: 'STUNG!'              },
  { type: 'rock',  effect: 'block',            msg: 'BLOCKED BY ROCK!'    },
  { type: 'beach', effect: 'push',             msg: 'TOURIST KICKED YOU!' },
  { type: 'net',   effect: 'stun',  turns: 1, msg: 'TANGLED IN A NET!'   },
];

// xmur3 string hash → 32-bit int (lets custom seeds be arbitrary strings)
function hashSeed(str) {
  let h = 1779033703 ^ str.length;
  for (let i = 0; i < str.length; i++) {
    h = Math.imul(h ^ str.charCodeAt(i), 3432918353);
    h = (h << 13) | (h >>> 19);
  }
  h = Math.imul(h ^ (h >>> 16), 2246822507);
  h = Math.imul(h ^ (h >>> 13), 3266489909);
  return (h ^ (h >>> 16)) >>> 0;
}

// mulberry32 — small deterministic PRNG seeded by a 32-bit int
function mulberry32(a) {
  return function () {
    a |= 0; a = (a + 0x6D2B79F5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// Deterministically place obstacles. At most one per cell column, so at least
// two lanes are always open — the track is never fully blocked.
function generateObstacles(seed, opts) {
  const { lanes, startCell, finishCell, density = 0.38 } = opts;
  const seedNum = typeof seed === 'number' ? (seed >>> 0) : hashSeed(String(seed));
  const rng = mulberry32(seedNum);
  const list = [];
  for (let cell = startCell; cell < finishCell - 1; cell++) {
    if (rng() >= density) continue;
    const lane = Math.floor(rng() * lanes);
    const kind = OBSTACLE_KINDS[Math.floor(rng() * OBSTACLE_KINDS.length)];
    list.push({ cell, lane, ...kind });
  }
  return list;
}

/** Build the obstacle list + lookup map for a given seed. */
function buildObstacles(seed) {
  const obstacles = generateObstacles(seed, {
    lanes: LANES, startCell: 6, finishCell: FINISH_CELL,
  });
  const map = new Map(obstacles.map(o => [`${o.cell},${o.lane}`, o]));
  return { obstacles, map };
}

// ── Valkey leaderboard ────────────────────────────────────────────────────────
// One redis client, connected in main(). If Valkey is unreachable the game still
// works — the leaderboard just silently no-ops rather than taking the server down.
let valkey = null;

/** Record a finish. `timeCs` = race time in centiseconds (lower is better);
 *  `colorIdx` = which crab palette (0–3) the player raced as;
 *  `seed` = the obstacle seed the run was raced on (for replay). */
async function recordScore(name, timeCs, colorIdx, seed = '') {
  if (valkey?.status !== 'ready' || !name || !Number.isFinite(timeCs)) return;
  try {
    // Keep the player's *best* time only: GT means "update only if greater",
    // so we store the negated time and a fastest run wins the highest score.
    await valkey.zadd(LB_KEY, 'GT', -timeCs, name);
    // Remember the crab colour for this name so the board can render it.
    await valkey.hset(LB_COLOR_KEY, name, String(colorIdx | 0));
    // Remember the seed the run was raced on. Skip when unknown so we don't
    // overwrite a real seed with an empty one from an older client.
    if (seed) await valkey.hset(LB_SEED_KEY, name, seed);
    // Trim to the top LB_MAX (drop everything below rank LB_MAX).
    await valkey.zremrangebyrank(LB_KEY, 0, -(LB_MAX + 1));
    await valkey.expire(LB_KEY, LB_TTL_SECS);
    await valkey.expire(LB_COLOR_KEY, LB_TTL_SECS);
    await valkey.expire(LB_SEED_KEY, LB_TTL_SECS);
  } catch (err) {
    console.error('[valkey] recordScore error:', err.message);
  }
}

/** Top scores, fastest first: [{ name, timeCs, colorIdx, seed }, …]. */
async function getLeaderboard() {
  if (valkey?.status !== 'ready') return [];
  try {
    // zrevrange … WITHSCORES returns a flat [member, score, member, score, …].
    const flat   = await valkey.zrevrange(LB_KEY, 0, LB_MAX - 1, 'WITHSCORES');
    const colors = await valkey.hgetall(LB_COLOR_KEY); // { name: "colorIdx" }
    const seeds  = await valkey.hgetall(LB_SEED_KEY);  // { name: "seed" }
    const board = [];
    for (let i = 0; i < flat.length; i += 2) {
      const name = flat[i];
      board.push({
        name,
        timeCs:   -Number(flat[i + 1]),
        colorIdx: Number(colors[name]) || 0,
        seed:     seeds[name] || '',
      });
    }
    return board;
  } catch (err) {
    console.error('[valkey] getLeaderboard error:', err.message);
    return [];
  }
}

// ── server state ────────────────────────────────────────────────────────────────
const rooms   = new Map(); // roomId → Room
const clients = new Map(); // ws     → { playerId, roomId, name }

function makeRoom(id, seed) {
  const { obstacles, map } = buildObstacles(seed);
  return { id, players: new Map(), started: false, finished: false, seed, obsMap: map, obstacles };
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

  const obs = room.obsMap.get(`${nc},${nl}`);
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
      broadcast(roomId, { type: 'go', seed: room.seed, players: snapshot(room) });
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

  // Valkey — connect via the single connection string. rediss:// URIs turn on
  // TLS automatically (that's what Aiven for Valkey hands you). lazyConnect lets
  // us await the first connection and treat failure as "leaderboard disabled"
  // rather than crashing the server.
  const { url: valkeyUrl, tls } = parseValkeyUri(VALKEY_URI);
  const valkeyOpts = { lazyConnect: true, maxRetriesPerRequest: 1 };
  if (tls) {
    // Supply Aiven's project CA so the broker cert verifies. Without it Node
    // rejects the connection with a self-signed-cert / unknown-CA error.
    valkeyOpts.tls = VALKEY_CA_CERT ? { ca: [readPem(VALKEY_CA_CERT)] } : {};
  }
  valkey = new Valkey(valkeyUrl, valkeyOpts);
  valkey.on('error', err => console.error('[valkey] client error:', err.message));
  try {
    await valkey.connect();
    console.log(`✓ Valkey connected (${valkeyUrl.split('@').pop()}${tls ? ', TLS' : ''})`);
  } catch (err) {
    console.error('[valkey] connect failed — leaderboard disabled:', err.message);
  }

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
        // First player to create the room fixes its seed: a custom seed if they
        // supplied one, otherwise a random one. Later joiners inherit it.
        const seed = (msg.seed != null && String(msg.seed).trim())
          ? String(msg.seed).slice(0, 32)
          : (Math.floor(Math.random() * 0xffffffff)).toString();

        let room = rooms.get(roomId);
        if (!room) { room = makeRoom(roomId, seed); rooms.set(roomId, room); }

        if (room.started) {
          sendTo(ws, { type: 'error', msg: 'Race already started — try another room' });
          return;
        }

        const playerId = `p${Date.now().toString(36)}${Math.random().toString(36).slice(2, 5)}`;
        const colorIdx = room.players.size % 4;
        room.players.set(playerId, makePlayer(playerId, name, colorIdx));
        clients.set(ws, { playerId, roomId, name });

        sendTo(ws, { type: 'joined', playerId, colorIdx, seed: room.seed, players: snapshot(room) });
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
        broadcast(info.roomId, { type: 'go', seed: room.seed, players: snapshot(room) });
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
