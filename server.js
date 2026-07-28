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
const crypto    = require('crypto');
const WebSocket = require('ws');
const { Kafka } = require('kafkajs');
const Valkey    = require('iovalkey');

const MIME = {
  '.html': 'text/html',
  '.js':   'text/javascript',
  '.json': 'application/json',
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

/** GET /solo-token → mints a one-time race token for solo score submission.
 *  The token is stored in Valkey with a 10-minute TTL. The client must include
 *  it in the POST /leaderboard body. This ties a score submission to a real
 *  race session that the server approved — without it, anyone could POST times
 *  without having played.
 *  Fails open (returns token:null) when Valkey is unavailable so a Valkey
 *  outage never blocks solo play. postLeaderboard skips validation when the
 *  token is null, matching the existing degrade-don't-crash pattern. */
async function getSoloToken(req, res) {
  if (valkey?.status !== 'ready') {
    // Valkey down — signal the client to send token:null; postLeaderboard
    // will skip validation in that case so scores still record.
    return sendJson(res, 200, { token: null });
  }
  const token = crypto.randomUUID();
  try {
    await valkey.set(`solo:token:${token}`, '1', 'EX', 10 * 60); // 10-minute TTL
    sendJson(res, 200, { token });
  } catch (err) {
    console.error('[valkey] getSoloToken error:', err.message);
    sendJson(res, 200, { token: null }); // degrade rather than block the player
  }
}

/** POST /leaderboard body: { name, timeCs, token } → validates solo race token,
 *  records score, returns board. The token field is required for solo races and
 *  must be a value minted by GET /solo-token for this session (single-use).
 *  Multiplayer scores are recorded server-side via the Kafka consumer; this
 *  endpoint is solo-only. */
function postLeaderboard(req, res) {
  let raw = '';
  req.on('data', chunk => {
    raw += chunk;
    if (raw.length > 1024) req.destroy(); // tiny payload — reject anything bigger
  });
  req.on('end', async () => {
    // Flood guard: cap leaderboard writes per client IP. Behind a proxy, honour
    // X-Forwarded-For's first hop; otherwise the socket address.
    const ip = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim()
               || req.socket.remoteAddress || 'unknown';
    if (!await rateLimit(`rl:lb:${ip}`, RL_LB_LIMIT, RL_LB_WINDOW_SECS)) {
      return sendJson(res, 429, { error: 'rate limited' });
    }

    let body;
    try { body = JSON.parse(raw); } catch { return sendJson(res, 400, { error: 'bad json' }); }

    // Validate and consume the solo race token. The token must have been issued
    // by GET /solo-token for this session — it is single-use and expires after
    // 10 minutes. When Valkey is ready a missing or already-consumed token is
    // rejected outright. When Valkey is unavailable we fail open (degrade-don't-
    // crash) so a Valkey blip never blocks a solo player's finish from recording.
    const token = body.token != null ? String(body.token).slice(0, 64) : null;
    if (valkey?.status === 'ready') {
      if (!token) {
        return sendJson(res, 403, { error: 'solo token required' });
      }
      try {
        const deleted = await valkey.del(`solo:token:${token}`);
        if (deleted !== 1) {
          return sendJson(res, 403, { error: 'invalid or already used token' });
        }
      } catch (err) {
        console.error('[valkey] token validation error:', err.message);
        // Fail open on Valkey error — a blip must not block a solo finish.
      }
    }
    // If Valkey is not ready, skip token validation (fail open — degraded mode).

    const name     = String(body.name || 'Crab').slice(0, 20);
    const timeCs   = Number(body.timeCs);
    // Reject implausible times up front so a bad client gets a clear error
    // rather than a silent no-op (recordScore also guards defensively).
    if (!isValidTimeCs(timeCs)) return sendJson(res, 400, { error: 'invalid timeCs' });
    const colorIdx = Number(body.colorIdx) || 0;
    // The obstacle seed the race was run on — kept so a board entry can be
    // replayed on the identical track. May be absent from older clients.
    const seed     = body.seed != null ? String(body.seed).slice(0, 32) : '';
    // The raceId of the run, so a board row links to its replay. UUID-shaped;
    // absent from older clients. Bounded like the other fields.
    const raceId   = body.raceId != null ? String(body.raceId).slice(0, 64) : '';
    // The client's playerId makes the sorted-set member unique so two players
    // with the same name (e.g. the default "Crab") don't share one slot.
    // Absent for older clients or solo runs — recordScore generates a random
    // suffix in that case so every entry is still distinct.
    const playerId = body.playerId != null ? String(body.playerId).slice(0, 64) : '';
    await recordScore(name, timeCs, colorIdx, seed, raceId, playerId);
    // Return the board for this same seed so the client can display it immediately.
    sendJson(res, 200, await getLeaderboard(seed));
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

  // Solo race token — issued before a solo race begins; must be included in
  // POST /leaderboard to prove the submission belongs to a real race session.
  if (route === '/solo-token') {
    if (req.method === 'GET') { getSoloToken(req, res); return; }
    res.writeHead(405); res.end('405 Method Not Allowed'); return;
  }

  // Room browser — GET /rooms returns a JSON array of joinable (unlocked, not yet
  // started) rooms so players can discover active lobbies without knowing the exact
  // room code. Only public state is exposed: no seeds, no internal timers, no configs.
  // A room is "locked" here when it has started or the join window has elapsed.
  if (route === '/rooms') {
    if (req.method !== 'GET') { res.writeHead(405); res.end('405 Method Not Allowed'); return; }
    const now = Date.now();
    const list = [];
    for (const [roomId, room] of rooms) {
      // Only surface rooms that are still joinable.
      if (room.started || room.locked) continue;
      // Derive the host's display name from the player map.
      const host = room.hostId ? (room.players.get(room.hostId)?.name ?? 'Unknown') : 'Unknown';
      list.push({
        roomId,
        playerCount: room.players.size,
        hostName:    host,
        locked:      false,                          // always false here (filtered above)
        ageSecs:     Math.floor((now - room.createdAt) / 1000),
      });
    }
    sendJson(res, 200, list);
    return;
  }

  // Leaderboard API — GET reads the board, POST records a finish (solo only;
  // multiplayer finishes are recorded server-side via the Kafka consumer).
  // The board is per-seed: GET requires ?seed= and returns only scores from
  // that obstacle layout. Omitting seed returns 400 — mixing seeds would make
  // times meaningless (different tracks = different difficulty).
  if (route === '/leaderboard') {
    if (req.method === 'POST') { postLeaderboard(req, res); return; }
    const seed = new URLSearchParams(req.url.split('?')[1] || '').get('seed') || '';
    if (!seed) { sendJson(res, 400, { error: 'seed required' }); return; }
    getLeaderboard(seed).then(board => sendJson(res, 200, board));
    return;
  }

  // "/" → game.html, anything else → filename in project dir.
  // Decode percent-escapes so a request like /%2e%2e/.env can't smuggle a
  // traversal past the containment check below.
  let urlPath;
  try {
    urlPath = req.url === '/' ? '/game.html' : decodeURIComponent(req.url.split('?')[0]);
  } catch {
    res.writeHead(400); res.end('400 Bad Request'); return; // malformed %-escape
  }

  // Containment: resolve the requested path and confirm it stays inside
  // __dirname. path.join collapses `..`, so without this a path that escapes
  // the project dir (…/../.env, /etc/passwd) would be served from anywhere on
  // disk. Reject anything that resolves outside the served root.
  const filePath = path.join(__dirname, urlPath);
  const rel = path.relative(__dirname, filePath);
  if (rel.startsWith('..') || path.isAbsolute(rel)) {
    res.writeHead(403); res.end('403 Forbidden'); return;
  }
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
// Per-seed Valkey key helpers — namespace every board entry by the obstacle seed
// so scores raced on seed "abc" never compete with scores from seed "xyz".
// The same data structure (sorted set + side-hashes) is kept; only the key names
// change.  An empty seed is treated as "no board" — recordScore / getLeaderboard
// both short-circuit when seed is absent, which can only happen with very old
// clients that pre-date seed support.
const lbKey      = (seed) => `lb:scores:${seed}`;  // sorted set: member="name:id", score=-timeCs
const lbColorKey = (seed) => `lb:colors:${seed}`;  // hash: "name:id" → colorIdx
const lbSeedKey  = (seed) => `lb:seeds:${seed}`;   // hash: "name:id" → seed (== seed, kept for compat)
const lbRaceKey  = (seed) => `lb:races:${seed}`;   // hash: "name:id" → raceId of best run
const LB_MAX      = 10;                    // keep only the top N
const LB_TTL_SECS = 60 * 60;               // ephemeral: whole board expires after 1h idle
// Replay records: race:<raceId> holds the full recorded race (seed, config,
// roster, move stream). Kept longer than the board so a board link stays
// watchable for a while after the run.
const RACE_KEY_PREFIX = 'race:';
const RACE_TTL_SECS   = 24 * 60 * 60;      // replays live 24h
// A finish time must be a positive centisecond count within a sane range. This
// bounds untrusted input: a non-positive time would (via the negated score +
// GT) pin an unbeatable top entry, and an absurd time is clearly bogus. Ceiling
// is 1h — far longer than any real race on the largest track.
const LB_MIN_TIME_CS = 1;
const LB_MAX_TIME_CS = 60 * 60 * 100;

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

// ── game config (single source of truth: game-config.json) ─────────────────────
// The track — lane count, zones, obstacle density and the obstacle kinds table —
// lives in game-config.json so a custom game can be tuned without touching code.
// The server loads it at startup and derives its constants from it; the browser
// fetches the same file (it's served as a static asset), so the client and server
// always simulate the byte-for-byte identical track.
//
// These defaults are the fallback if game-config.json is missing or unreadable;
// they must stay in sync with the values shipped in game-config.json / game.html.
const DEFAULT_GAME_CONFIG = {
  lanes: 3,
  obstacleDensity: 0.38,
  obstacleStartCell: 6,
  // Once the first crab finishes, everyone else has this many seconds to reach
  // the line before the race ends and the stragglers are recorded as DNF.
  dnfTimeoutSecs: 15,
  zones: [
    { tile: 'shore',       len: 5  },
    { tile: 'wetSand',     len: 8  },
    { tile: 'drySand',     len: 12 },
    { tile: 'undergrowth', len: 10 },
    { tile: 'treeline',    len: 8  },
    { tile: 'finish',      len: 3  },
  ],
  obstacleKinds: [
    { type: 'hole',  effect: 'stun',  turns: 2, msg: 'FELL IN A HOLE!'     },
    { type: 'jelly', effect: 'stun',  turns: 1, msg: 'STUNG!'              },
    { type: 'rock',  effect: 'block',            msg: 'BLOCKED BY ROCK!'    },
    { type: 'beach', effect: 'push',             msg: 'TOURIST KICKED YOU!' },
    { type: 'net',   effect: 'stun',  turns: 1, msg: 'TANGLED IN A NET!'   },
  ],
};

/** Load game-config.json, falling back to the built-in defaults on any problem.
 *  Missing keys fall back individually, so a partial config file is fine. */
function loadGameConfig() {
  try {
    const raw = fs.readFileSync(path.join(__dirname, 'game-config.json'), 'utf8');
    const cfg = JSON.parse(raw);
    return { ...DEFAULT_GAME_CONFIG, ...cfg };
  } catch (err) {
    console.warn('[config] using defaults — could not load game-config.json:', err.message);
    return DEFAULT_GAME_CONFIG;
  }
}

// The loaded config is the DEFAULT track. Each room gets its own config (built
// by buildRoomConfig from a joiner's overrides), so GAME_CONFIG is only the
// baseline a room inherits when the creator doesn't customise a field.
const GAME_CONFIG = loadGameConfig();

// Bounds for per-lobby overrides — clamped server-side so a malicious/buggy
// client can't hang the server with e.g. lanes: 1e9 or a 1e9-cell track.
const LANE_MIN = 1,  LANE_MAX = 12;
const LEN_MIN  = 10, LEN_MAX  = 200;
const DNF_MIN  = 3,  DNF_MAX  = 120; // seconds the field gets after the first finish

// Scale a base zone list's lengths proportionally to hit `targetLen` total cells,
// preserving the tile sequence. Each zone keeps at least 1 cell; rounding drift
// is absorbed by the largest zone so the total lands exactly on targetLen.
function scaleZones(baseZones, targetLen) {
  const baseTotal = baseZones.reduce((s, z) => s + z.len, 0);
  const scaled = baseZones.map(z => ({
    ...z, len: Math.max(1, Math.round(z.len * targetLen / baseTotal)),
  }));
  const drift = targetLen - scaled.reduce((s, z) => s + z.len, 0);
  if (drift !== 0) {
    let bi = 0;
    for (let i = 1; i < scaled.length; i++) if (scaled[i].len > scaled[bi].len) bi = i;
    scaled[bi].len = Math.max(1, scaled[bi].len + drift);
  }
  return scaled;
}

/** Build a room's track config from optional client overrides, starting from the
 *  server default (GAME_CONFIG) and clamping every value to a safe range. */
function buildRoomConfig(overrides = {}) {
  const o = overrides || {};

  // lanes: integer within [LANE_MIN, LANE_MAX]; bad/missing → default.
  let lanes = Math.round(Number(o.lanes));
  if (!Number.isFinite(lanes)) lanes = GAME_CONFIG.lanes;
  lanes = Math.max(LANE_MIN, Math.min(LANE_MAX, lanes));

  // trackLength: integer within [LEN_MIN, LEN_MAX] → scaled zones; else default zones.
  let zones = GAME_CONFIG.zones;
  let len = Math.round(Number(o.trackLength));
  if (Number.isFinite(len)) {
    len = Math.max(LEN_MIN, Math.min(LEN_MAX, len));
    zones = scaleZones(GAME_CONFIG.zones, len);
  }

  // obstacleKinds: allowlist of type names → filter the server's kinds table, so
  // only known obstacles (with their fixed effects/turns/msgs) can appear. This
  // ignores unknown types, preventing injection of arbitrary effects. Empty or
  // missing → keep all default kinds.
  let obstacleKinds = GAME_CONFIG.obstacleKinds;
  if (Array.isArray(o.obstacleKinds) && o.obstacleKinds.length) {
    const allow = new Set(o.obstacleKinds.map(String));
    const filtered = GAME_CONFIG.obstacleKinds.filter(k => allow.has(k.type));
    if (filtered.length) obstacleKinds = filtered;
  }

  // dnfTimeoutSecs: seconds the field gets to finish after the first crab wins,
  // clamped to a sane range; bad/missing → default.
  let dnfTimeoutSecs = Number(o.dnfTimeoutSecs);
  if (!Number.isFinite(dnfTimeoutSecs)) dnfTimeoutSecs = GAME_CONFIG.dnfTimeoutSecs ?? 15;
  dnfTimeoutSecs = Math.max(DNF_MIN, Math.min(DNF_MAX, dnfTimeoutSecs));

  const totalCells = zones.reduce((s, z) => s + z.len, 0);
  const finishCell = totalCells - zones[zones.length - 1].len;
  return {
    lanes, zones, obstacleKinds, totalCells, finishCell, dnfTimeoutSecs,
    obstacleStartCell: GAME_CONFIG.obstacleStartCell,
    obstacleDensity:   GAME_CONFIG.obstacleDensity,
  };
}

// Serialisable subset of a room config to ship to clients (drives applyGameConfig
// there). Mirrors the shape game-config.json / applyGameConfig expects.
function configForClient(config) {
  return {
    lanes: config.lanes,
    zones: config.zones,
    obstacleKinds: config.obstacleKinds,
    obstacleStartCell: config.obstacleStartCell,
    obstacleDensity: config.obstacleDensity,
    dnfTimeoutSecs: config.dnfTimeoutSecs,
  };
}

// Inverse of configForClient: rebuild a full server-side room config (with the
// derived totalCells / finishCell that applyMove and makeRoom need) from the
// client-shaped config stored in a replay record. Missing fields fall back to
// the server default so an old/partial record still reconstructs a track.
function configFromStored(stored = {}) {
  const zones = Array.isArray(stored.zones) && stored.zones.length ? stored.zones : GAME_CONFIG.zones;
  const totalCells = zones.reduce((s, z) => s + z.len, 0);
  const finishCell = totalCells - zones[zones.length - 1].len;
  return {
    lanes:             stored.lanes             ?? GAME_CONFIG.lanes,
    zones,
    obstacleKinds:     stored.obstacleKinds     ?? GAME_CONFIG.obstacleKinds,
    obstacleStartCell: stored.obstacleStartCell ?? GAME_CONFIG.obstacleStartCell,
    obstacleDensity:   stored.obstacleDensity   ?? GAME_CONFIG.obstacleDensity,
    dnfTimeoutSecs:    stored.dnfTimeoutSecs    ?? GAME_CONFIG.dnfTimeoutSecs,
    totalCells, finishCell,
  };
}

// Obstacles are placed procedurally from a per-room seed so every race gets a
// fresh but fair layout. This server is authoritative: it picks the seed (or
// honours a custom one from the joining player) and ships it to every client so
// they render/simulate the identical track.
//
// IMPORTANT: keep this generator byte-for-byte identical to the one in game.html
// — the same seed + config must produce the same obstacles on both sides.

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

// Guarantee the track is walkable: carve out the fewest wall obstacles needed so
// an orthogonal path (crabs move one cell OR one lane per turn — never diagonally)
// exists from the start to the finish line.
//
// A "wall" is any cell a crab can never sit on going forward: `block` (rock —
// entry refused) AND `push` (tourist — kicks you back a cell, so with no free lane
// to sidestep into you loop forever one cell short). Stun cells (hole/jelly/net)
// are NOT walls: they only cost turns, so any wall-free path is always traversable.
// "One obstacle per column" is NOT enough on its own — staggered walls in adjacent
// columns can seal the track, and at 1 lane a single push is an inescapable trap.
// Fully deterministic (no rng), so client and server stay in sync — keep
// byte-for-byte identical to game.html's ensurePassable.
function ensurePassable(list, lanes, finishCell) {
  const isWall = (o) => o.effect === 'block' || o.effect === 'push';
  const wallAt = new Map();           // packed cell*lanes+lane → obstacle index
  list.forEach((o, i) => { if (isWall(o)) wallAt.set(o.cell * lanes + o.lane, i); });
  const removed = new Set();          // packed keys of walls we've carved out
  for (;;) {
    // Flood-fill the free cells reachable from the start; a crossing exists once
    // we touch a cell at/after the finish line. All start lanes at cell 0 are
    // seeded (the run-up before startCell is always obstacle-free anyway).
    const seen = new Set(), queue = [];
    for (let l = 0; l < lanes; l++) { const k = 0 * lanes + l; seen.add(k); queue.push([0, l]); }
    const frontier = new Set();       // walls touching the reachable region
    let reached = false;
    for (let h = 0; h < queue.length; h++) {
      const [c, l] = queue[h];
      if (c >= finishCell) { reached = true; break; }
      for (const [nc, nl] of [[c + 1, l], [c - 1, l], [c, l + 1], [c, l - 1]]) {
        if (nc < 0 || nc > finishCell || nl < 0 || nl >= lanes) continue;
        const k = nc * lanes + nl;
        if (seen.has(k)) continue;
        if (wallAt.has(k) && !removed.has(k)) { frontier.add(k); continue; }
        seen.add(k); queue.push([nc, nl]);
      }
    }
    if (reached || frontier.size === 0) break;
    // Carve out the lowest-keyed blocking wall on the frontier (deterministic)
    // and re-flood; repeat until the finish is reachable.
    let victim = Infinity;
    for (const k of frontier) if (k < victim) victim = k;
    removed.add(victim);
  }
  return removed.size
    ? list.filter(o => !(isWall(o) && removed.has(o.cell * lanes + o.lane)))
    : list;
}

// Deterministically place obstacles. At most one per cell column; ensurePassable
// then guarantees a walkable path so no seed can ever hard-block the track.
function generateObstacles(seed, opts) {
  const { lanes, startCell, finishCell, density = 0.38, obstacleKinds } = opts;
  const seedNum = typeof seed === 'number' ? (seed >>> 0) : hashSeed(String(seed));
  const rng = mulberry32(seedNum);
  const list = [];
  for (let cell = startCell; cell < finishCell - 1; cell++) {
    if (rng() >= density) continue;
    const lane = Math.floor(rng() * lanes);
    const kind = obstacleKinds[Math.floor(rng() * obstacleKinds.length)];
    list.push({ cell, lane, ...kind });
  }
  return ensurePassable(list, lanes, finishCell);
}

/** Build the obstacle list + lookup map for a given seed and room config. */
function buildObstacles(seed, config) {
  const obstacles = generateObstacles(seed, {
    lanes: config.lanes, startCell: config.obstacleStartCell, finishCell: config.finishCell,
    density: config.obstacleDensity, obstacleKinds: config.obstacleKinds,
  });
  const map = new Map(obstacles.map(o => [`${o.cell},${o.lane}`, o]));
  return { obstacles, map };
}

// ── Valkey leaderboard ────────────────────────────────────────────────────────
// One redis client, connected in main(). If Valkey is unreachable the game still
// works — the leaderboard just silently no-ops rather than taking the server down.
let valkey = null;

// The Kafka client, set in main(). Exposed at module scope so the replay
// fallback (loadRaceFromKafka) can spin up a throwaway consumer to reconstruct
// a race from the durable log when Valkey doesn't have it.
let kafka = null;

// ── Valkey-backed rate limiting ─────────────────────────────────────────────────
// Rate limits live in Valkey (not in-process) so they hold ACROSS instances: room
// and client state here is per-process, so an in-memory limiter would be per-pod
// and trivially bypassed by reconnecting to another instance behind a load
// balancer. See docs/blog/rate-limiting-with-valkey.md.
//
// Per-caller limits (fixed window). Tuned generously — these are flood ceilings,
// not gameplay pacing.
const RL_MOVE_LIMIT = 20, RL_MOVE_WINDOW_SECS = 1;   // moves per player per second
const RL_LB_LIMIT   = 5,  RL_LB_WINDOW_SECS   = 60;  // leaderboard POSTs per IP per minute

/** Fixed-window rate limit backed by Valkey. Returns true if the action is
 *  allowed, false if the caller is over `limit` within `windowSecs`.
 *  Fails OPEN when Valkey isn't ready — a Valkey blip must never stop gameplay
 *  (matches the leaderboard's degrade-don't-crash behaviour). Keys are prefixed
 *  `rl:` and always carry a TTL so they expire on their own. */
async function rateLimit(key, limit, windowSecs) {
  if (valkey?.status !== 'ready') return true; // fail open
  try {
    const n = await valkey.incr(key);
    if (n === 1) await valkey.expire(key, windowSecs); // first hit starts the window
    return n <= limit;
  } catch (err) {
    console.error('[valkey] rateLimit error:', err.message);
    return true; // fail open on error too
  }
}

/** True when `timeCs` is a plausible finish time (positive, in range). Rejects
 *  the values that would corrupt the board — non-positive (unbeatable via the
 *  negated GT score) or absurdly large. */
function isValidTimeCs(timeCs) {
  return Number.isFinite(timeCs) && timeCs >= LB_MIN_TIME_CS && timeCs <= LB_MAX_TIME_CS;
}

/** Record a finish. `timeCs` = race time in centiseconds (lower is better);
 *  `colorIdx` = which crab palette (0–3) the player raced as;
 *  `seed` = the obstacle seed the run was raced on (for replay);
 *  `raceId` = the recorded race this run belongs to (for the watch link);
 *  `memberId` = the player's unique id (playerId from room state). When absent
 *  (e.g. old clients or solo HTTP POST without a playerId) a random 8-hex-char
 *  suffix is generated so two players named "Crab" never share one slot. */
async function recordScore(name, timeCs, colorIdx, seed = '', raceId = '', memberId = '') {
  // seed is required — without it we can't build the per-seed key and would
  // write to a meaningless global bucket. Old clients (pre-seed) just get no entry.
  if (valkey?.status !== 'ready' || !name || !isValidTimeCs(timeCs) || !seed) return;
  // Build the unique composite member key: "name:id".  The display name is
  // everything before the last colon, so the colon-separated suffix is always
  // stripped before JSON is returned to callers.
  const suffix = memberId ? String(memberId).slice(0, 64) : crypto.randomBytes(4).toString('hex');
  const member = `${name}:${suffix}`;
  try {
    // Build the per-seed Valkey keys for this run's obstacle layout.
    const k = lbKey(seed), kC = lbColorKey(seed), kS = lbSeedKey(seed), kR = lbRaceKey(seed);

    // Pre-read (outside the pipeline) to identify which side-hash entry to
    // prune.  After the new entry is added the set can hold at most LB_MAX+1
    // members; zremrangebyrank then drops the single rank-0 member (current
    // worst) when the set was already at capacity.  Fetching both the size and
    // the current worst in parallel keeps this to one round-trip.
    const [currentSize, currentWorst] = await Promise.all([
      valkey.zcard(k),
      valkey.zrange(k, 0, 0),
    ]);
    // The member evicted from the side-hashes is the current rank-0 entry,
    // but only when the set is full AND it belongs to a different composite key.
    // When member === currentWorst[0], GT either improves the score in-place
    // (no eviction, count unchanged) or is a no-op — either way nothing
    // is removed.  Note: if a brand-new player submits a time worse than
    // everything on a full board they are immediately evicted; their hset
    // below is undone by the hdel that follows it in the pipeline.
    const candidateEvict =
      (currentSize >= LB_MAX && currentWorst.length > 0 && currentWorst[0] !== member)
        ? currentWorst[0]
        : null;

    // Wrap all writes in a single pipeline (MULTI/EXEC) so concurrent
    // finishers cannot interleave their zadd/hset/trim sequences.
    const tx = valkey.multi();

    // Keep the player's *best* time only: GT means "update only if greater",
    // so we store the negated time and a fastest run wins the highest score.
    tx.zadd(k, 'GT', -timeCs, member);
    // Remember the crab colour for this member so the board can render it.
    tx.hset(kC, member, String(colorIdx | 0));
    // Store the seed — since the key is already namespaced by seed, this is
    // redundant, but kept so the returned board entries carry a seed field
    // (consumers may use it for replay links).
    tx.hset(kS, member, seed);
    // Remember which recorded race this run was, so the board can link to a
    // replay.
    if (raceId) tx.hset(kR, member, raceId);
    // Trim to the top LB_MAX (drop everything below rank LB_MAX).
    tx.zremrangebyrank(k, 0, -(LB_MAX + 1));
    // Prune the side-hashes for the evicted member so they don't grow
    // unbounded (one distinct composite key per submission until the 1h TTL).
    if (candidateEvict) {
      tx.hdel(kC, candidateEvict);
      tx.hdel(kS, candidateEvict);
      tx.hdel(kR, candidateEvict);
    }
    tx.expire(k,  LB_TTL_SECS);
    tx.expire(kC, LB_TTL_SECS);
    tx.expire(kS, LB_TTL_SECS);
    tx.expire(kR, LB_TTL_SECS);
    await tx.exec();
  } catch (err) {
    console.error('[valkey] recordScore error:', err.message);
  }
}

/** Top scores for `seed`, fastest first: [{ name, timeCs, colorIdx, seed, raceId }, …].
 *  Returns an empty array when seed is absent or Valkey is unreachable.
 *  Members in the sorted set are "name:id" composites; the id suffix is stripped
 *  before returning so callers always receive the display name. */
async function getLeaderboard(seed) {
  if (valkey?.status !== 'ready' || !seed) return [];
  try {
    const k = lbKey(seed), kC = lbColorKey(seed), kS = lbSeedKey(seed), kR = lbRaceKey(seed);
    // zrevrange … WITHSCORES returns a flat [member, score, member, score, …].
    const flat   = await valkey.zrevrange(k, 0, LB_MAX - 1, 'WITHSCORES');
    const colors = await valkey.hgetall(kC); // { "name:id": "colorIdx" }
    const seeds  = await valkey.hgetall(kS); // { "name:id": "seed" }
    const races  = await valkey.hgetall(kR); // { "name:id": "raceId" }
    const board = [];
    for (let i = 0; i < flat.length; i += 2) {
      const member = flat[i]; // "name:id" composite key
      // Strip the ":id" suffix: everything before the last colon is the display name.
      // Falls back to the full member string if (for some reason) no colon exists,
      // so old entries written before this fix still render gracefully.
      const lastColon = member.lastIndexOf(':');
      const name = lastColon >= 0 ? member.slice(0, lastColon) : member;
      board.push({
        name,
        timeCs:   -Number(flat[i + 1]),
        colorIdx: Number((colors || {})[member]) || 0,
        seed:     (seeds  || {})[member] || seed,
        // raceId links a board row to its watchable replay (absent if the run
        // predates replay support or the record has since expired).
        raceId:   (races  || {})[member] || '',
      });
    }
    return board;
  } catch (err) {
    console.error('[valkey] getLeaderboard error:', err.message);
    return [];
  }
}

// ── replay records ─────────────────────────────────────────────────────────────
// A recorded race is everything needed to reconstruct it deterministically:
// seed + config rebuild the identical track, the starting roster rebuilds the
// field, and the move stream is re-applied through applyMove. Stored in Valkey
// (primary, survives Kafka retention); a Kafka scan is the fallback (see
// loadRaceFromKafka). Degrade-don't-crash: any Valkey problem just means "no
// replay", never a thrown error into gameplay.

/** Persist a finished race so it can be replayed later. No-op if Valkey is down
 *  or the race recorded no moves (nothing to watch). */
async function saveRace(room) {
  if (valkey?.status !== 'ready' || !room.raceId || !room.moves?.length) return;
  const record = {
    raceId:  room.raceId,
    roomId:  room.id,
    seed:    room.seed,
    config:  configForClient(room.config),
    players: room.startRoster || [],
    moves:   room.moves,
  };
  try {
    await valkey.set(RACE_KEY_PREFIX + room.raceId, JSON.stringify(record), 'EX', RACE_TTL_SECS);
  } catch (err) {
    console.error('[valkey] saveRace error:', err.message);
  }
}

/** Fetch a recorded race by id, or null if not found / Valkey unavailable. */
async function loadRace(raceId) {
  if (valkey?.status !== 'ready' || !raceId) return null;
  try {
    const raw = await valkey.get(RACE_KEY_PREFIX + raceId);
    return raw ? JSON.parse(raw) : null;
  } catch (err) {
    console.error('[valkey] loadRace error:', err.message);
    return null;
  }
}

// ── server state ────────────────────────────────────────────────────────────────
const rooms   = new Map(); // roomId → Room
const clients = new Map(); // ws     → { playerId, roomId, name }

// Reconnect tokens: token (UUID) → { playerId, roomId, timer }
// Issued to each player at join time; a client that reconnects within
// GRACE_PERIOD_MS can resume their slot without a full rejoin.
const reconnectTokens = new Map();
const GRACE_PERIOD_MS = 15 * 1000; // 15 seconds

// How long a freshly-created lobby stays open for others to join. After this the
// lobby locks automatically; the creator can also lock it early by starting the
// match. Locked lobbies reject new joiners (see the join handler).
const LOBBY_OPEN_MS = 60 * 1000;

// If the host hasn't started within this window AND only one player is present,
// auto-close the room (nobody joined — no reason to keep it open).
const SOLO_AUTOCLOSE_MS = 30 * 1000;

function makeRoom(id, seed, config) {
  const { obstacles, map } = buildObstacles(seed, config);
  return {
    id, players: new Map(), started: false, finished: false, locked: false,
    hostId: null, lockTimer: null, seed, config, obsMap: map, obstacles,
    // Timestamp when the room was created — exposed via GET /rooms as age.
    createdAt: Date.now(),
    // Race timing / finish tracking. startedAt is stamped when the 'go' fires,
    // so finish times are measured server-side from a single authoritative clock.
    startedAt: null, dnfTimer: null, countdownInterval: null, finishCount: 0,
    // raceId is a per-race UUID stamped when the race actually begins (go/solo).
    // It's the Kafka message key for this race's moves and the id a replay is
    // fetched by. `moves` buffers the applied move stream so we can persist the
    // whole race for replay when it ends. Both reset for each new race in a room.
    raceId: null, moves: [],
    // Timer that auto-closes a solo lobby if nobody else joins within
    // SOLO_AUTOCLOSE_MS. Cleared as soon as a second player arrives.
    soloAutocloseTimer: null,
    // Track which colorIdx values (0–3) and which starting lanes are currently
    // assigned so player churn (join/leave/rejoin) never produces duplicates.
    // Released in the 'close' handler when a player leaves.
    usedColors: new Set(),
    usedLanes:  new Set(),
  };
}

/** Stop accepting new joiners (idempotent). Clears the auto-lock timer. */
function lockLobby(room) {
  room.locked = true;
  if (room.lockTimer) { clearTimeout(room.lockTimer); room.lockTimer = null; }
}

function makePlayer(id, name, colorIdx, lanes) {
  return {
    id, name, colorIdx,
    cell: 0, lane: colorIdx % lanes,
    stunTurns: 0, won: false,
    // Filled in when the crab crosses the line: finishTimeCs = race time in
    // centiseconds, place = 1-based finishing order. DNF crabs keep both null.
    finishTimeCs: null, place: null,
    // Starting lane — stored so resetRoom can restore each player's position
    // without re-running the original usedLanes assignment.
    startLane: colorIdx % lanes,
  };
}

/** Serialisable snapshot of all players in a room */
function snapshot(room) {
  return [...room.players.values()].map(
    ({ id, name, colorIdx, cell, lane, stunTurns, won, finishTimeCs, place }) =>
      ({ id, name, colorIdx, cell, lane, stunTurns, won, finishTimeCs, place })
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
  // Guard on startedAt, not started. room.started flips true at the top of
  // startCountdown (before the 'go'), but startedAt is only stamped by
  // beginRace when the countdown actually completes. A move processed before
  // startedAt is set would have raceTimeCs return 0 → unbeatable finish time.
  if (!p || p.won || !room.startedAt || room.finished) return { ok: false };

  // Consume a stun turn instead of moving
  if (p.stunTurns > 0) {
    p.stunTurns--;
    return { ok: true, effect: 'stunned', stunLeft: p.stunTurns };
  }

  const nc = p.cell + dx;
  const nl = p.lane + dlane;
  if (nc < 0 || nc >= room.config.totalCells || nl < 0 || nl >= room.config.lanes) return { ok: false };

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

  if (p.cell >= room.config.finishCell && !p.won) {
    p.won = true;
    p.place = ++room.finishCount;
    p.finishTimeCs = raceTimeCs(room);
  }

  return { ok: true, effect, won: p.won, place: p.place, finishTimeCs: p.finishTimeCs };
}

/** Race time in centiseconds since the 'go', from the server's authoritative
 *  clock. applyMove guards on startedAt before calling this, so the fallback
 *  should never be reached in normal flow — it's a last-resort safety net. */
function raceTimeCs(room) {
  if (!room.startedAt) return 0;
  return Math.round((Date.now() - room.startedAt) / 10);
}

/** Everyone who's still racing is marked DNF; the race is closed out and a final
 *  results message is broadcast. Idempotent — a second call is a no-op. */
function endRace(room, roomId) {
  if (room.finished) return;
  room.finished = true;
  if (room.dnfTimer) { clearTimeout(room.dnfTimer); room.dnfTimer = null; }

  // Anyone who hasn't crossed the line is a DNF: leave finishTimeCs/place null.
  const results = [...room.players.values()]
    .map(({ id, name, colorIdx, finishTimeCs, place }) =>
      ({ id, name, colorIdx, finishTimeCs, place, dnf: place == null }))
    // Finishers first (by place), then DNF crabs.
    .sort((a, b) => (a.place ?? Infinity) - (b.place ?? Infinity));

  broadcast(roomId, { type: 'race-over', results });

  // Persist the recorded move stream so this race can be replayed. Fire-and-
  // forget: replay is a nice-to-have, never a reason to hold up race teardown.
  saveRace(room);

  // Reset the room back to lobby state so the same players can race again
  // without disconnecting. The host-changed and player-list state are kept.
  resetRoom(room, roomId);
}

/** Reset a room to lobby state after a race ends, keeping all connected players.
 *  Generates a fresh seed for the next race and broadcasts `room-reset` so
 *  clients can update their UI (show the lobby again, enable Start Match, etc.).
 *  The existing disconnect-based cleanup (empty-room delete) is unchanged. */
function resetRoom(room, roomId) {
  // Fresh seed + obstacles for the next race.
  const newSeed = (Math.floor(Math.random() * 0xffffffff)).toString();
  room.seed = newSeed;
  const { obstacles, map } = buildObstacles(newSeed, room.config);
  room.obsMap    = map;
  room.obstacles = obstacles;

  // Clear all race-specific state.
  room.started     = false;
  room.finished    = false;
  room.startedAt   = null;
  room.raceId      = null;
  room.moves       = [];
  room.startRoster = null;
  room.finishCount = 0;

  // Re-open the lobby join window so late arrivals can join the next race.
  if (room.lockTimer) clearTimeout(room.lockTimer);
  room.locked = false;
  room.lockTimer = setTimeout(() => {
    lockLobby(room);
    broadcast(roomId, { type: 'lobby-locked', players: snapshot(room) });
    console.log(`[room:${roomId}] lobby locked (timeout after reset)`);
  }, LOBBY_OPEN_MS);

  // Restore every player to their starting position.
  for (const p of room.players.values()) {
    p.cell = 0;
    p.lane = p.startLane ?? (p.colorIdx % room.config.lanes);
    p.stunTurns  = 0;
    p.won        = false;
    p.finishTimeCs = null;
    p.place        = null;
  }

  broadcast(roomId, {
    type:      'room-reset',
    seed:      newSeed,
    config:    configForClient(room.config),
    players:   snapshot(room),
    hostId:    room.hostId,
    opensInMs: LOBBY_OPEN_MS,
  });
  console.log(`[room:${roomId}] reset after race — ready for next race`);
}

/** Called when a crab finishes. Starts the DNF countdown on the first finish,
 *  and ends the race immediately once every player has finished. */
function onFinish(room, roomId) {
  // First finisher opens the DNF window for the rest of the field.
  if (!room.dnfTimer && !room.finished) {
    const ms = (room.config.dnfTimeoutSecs ?? 15) * 1000;
    room.dnfTimer = setTimeout(() => endRace(room, roomId), ms);
    broadcast(roomId, { type: 'dnf-window', ms });
  }
  // Everyone home → end now rather than waiting out the timer.
  if (allFinished(room)) endRace(room, roomId);
}

/** True when every player currently in the room has crossed the line. False for
 *  an empty room (nobody to finish). */
function allFinished(room) {
  return room.players.size > 0 &&
    [...room.players.values()].every(p => p.won);
}

/** Stamp the authoritative race clock and mint the per-race id at the moment a
 *  race actually starts (the 'go'). A fresh raceId per race — the Kafka key for
 *  this race's moves and the handle a replay is fetched by. Also snapshots the
 *  starting roster so a replay can rebuild the field before applying moves. */
function beginRace(room) {
  room.startedAt = Date.now(); // authoritative race clock start
  room.raceId    = crypto.randomUUID();
  room.moves     = [];
  // Frozen starting roster (before any move) — replay recreates players from this.
  room.startRoster = [...room.players.values()].map(
    ({ id, name, colorIdx, lane }) => ({ id, name, colorIdx, lane }));
}

/** 3-2-1 countdown, then broadcast 'go' */
function startCountdown(room, roomId) {
  // Defensive guard: the start-match handler validates this before calling us,
  // but if that check is ever bypassed we must not lock the room with too few
  // players.  Returning here leaves the room in its pre-start state so players
  // can keep joining and the host can retry.
  if (room.players.size < 2) return;
  room.started = true;
  lockLobby(room); // no more joiners once the countdown begins
  let count = 3;
  // Store handle on room so it can be cleared if the room tears down mid-countdown.
  // Check count > 0 before broadcasting so clients never receive count=0 ("Starting in 0…").
  room.countdownInterval = setInterval(() => {
    if (count > 0) {
      broadcast(roomId, { type: 'countdown', count: count-- });
    } else {
      clearInterval(room.countdownInterval);
      room.countdownInterval = null;
      beginRace(room);
      broadcast(roomId, { type: 'go', raceId: room.raceId, seed: room.seed, config: configForClient(room.config), players: snapshot(room) });
    }
  }, 1000);
}

// ── replay ───────────────────────────────────────────────────────────────────────
// Cap how much of the Kafka log the fallback will scan, and how long it waits,
// so a replay request can never hang or read the whole topic unbounded.
const REPLAY_SCAN_TIMEOUT_MS = 8000;

/** Fallback race loader: scan the Kafka topic from the beginning and collect the
 *  moves whose raceId matches. Returns the moves (ordered) or null on
 *  timeout/failure. Only the moves are recoverable this way — seed/config/roster
 *  aren't in the move stream — so this is a partial source used when Valkey has
 *  no record. Best-effort; a throwaway consumer group so it doesn't disturb the
 *  live one. */
async function loadRaceFromKafka(raceId) {
  if (!kafka || !raceId) return null;
  const consumer = kafka.consumer({ groupId: `replay-${raceId}-${crypto.randomUUID()}` });
  const moves = [];
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
    await new Promise((resolve) => {
      const done = () => { clearTimeout(timer); resolve(); };
      // The scan has no natural end (Kafka is a stream), so stop after a quiet
      // window / hard ceiling. Each matching message pushes the timeout out a
      // little so an active backlog is drained before we give up.
      let timer = setTimeout(done, REPLAY_SCAN_TIMEOUT_MS);
      consumer.run({
        eachMessage: async ({ message }) => {
          let e; try { e = JSON.parse(message.value.toString()); } catch { return; }
          if (e.action === 'move' && e.raceId === raceId) {
            moves.push({ playerId: e.playerId, dx: e.dx, dlane: e.dlane, t: moves.length });
            clearTimeout(timer); timer = setTimeout(done, 1500);
          }
        },
      }).catch(() => done());
    });
  } catch (err) {
    console.error('[replay] kafka scan error:', err.message);
  } finally {
    await consumer.disconnect().catch(() => {});
  }
  return moves.length ? moves : null;
}

/** Stream a recorded race to one watching socket as replay:true frames, reusing
 *  the exact 'go' → 'state' → 'race-over' shape the live client already renders.
 *  Moves are re-applied through the authoritative applyMove on a freshly rebuilt
 *  room, so the replay reproduces the original race move-for-move. Playback runs
 *  on a scaled version of the original cadence (`speed`), or as fast as the event
 *  loop allows for the instant-result case. */
async function playRace(ws, record, { speed = 1 } = {}) {
  const config = configFromStored(record.config);
  const room = makeRoom(`replay:${record.raceId}`, record.seed, config);
  for (const p of (record.players || [])) {
    room.players.set(p.id, makePlayer(p.id, p.name, p.colorIdx, config.lanes));
    // Restore the exact starting lane the run used (makePlayer derives it from
    // colorIdx, but the recorded roster is authoritative).
    if (p.lane != null) room.players.get(p.id).lane = p.lane;
  }
  room.started = true;
  room.startedAt = Date.now();

  sendTo(ws, { type: 'go', replay: true, raceId: record.raceId, seed: record.seed,
    config: configForClient(config), players: snapshot(room) });

  let prevT = 0;
  for (const m of record.moves) {
    if (ws.readyState !== WebSocket.OPEN) return; // watcher left — stop
    // Honour the original inter-move gap, scaled by speed. speed<=0 → instant.
    const gap = speed > 0 ? Math.max(0, (m.t - prevT)) / speed : 0;
    prevT = m.t;
    if (gap) await new Promise(r => setTimeout(r, Math.min(gap, 2000)));

    const result = applyMove(room, m.playerId, m.dx, m.dlane);
    if (!result.ok && result.effect !== 'block') continue;
    // The recorded finishTimeCs is authoritative (measured live); prefer it over
    // recomputing from replay wall-clock. Overwrite the player's stamp too so the
    // final race-over results carry the original times.
    if (result.won && m.finishTimeCs != null) {
      result.finishTimeCs = m.finishTimeCs;
      const pl = room.players.get(m.playerId);
      if (pl) pl.finishTimeCs = m.finishTimeCs;
    }
    sendTo(ws, { type: 'state', replay: true, players: snapshot(room), result, movedId: m.playerId });
    if (result.won && result.place != null) {
      const p = room.players.get(m.playerId);
      sendTo(ws, { type: 'finish', replay: true, playerId: m.playerId,
        name: p?.name ?? '?', place: result.place, finishTimeCs: result.finishTimeCs });
    }
  }

  const results = [...room.players.values()]
    .map(({ id, name, colorIdx, finishTimeCs, place }) =>
      ({ id, name, colorIdx, finishTimeCs, place, dnf: place == null }))
    .sort((a, b) => (a.place ?? Infinity) - (b.place ?? Infinity));
  sendTo(ws, { type: 'race-over', replay: true, results });
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
  kafka = new Kafka(kafkaCfg); // module-scoped so the replay fallback can reuse it

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

      // Buffer the applied move for replay: only moves belonging to the room's
      // current race, with an offset from the race start so playback can honour
      // the original cadence. This is the exact input stream replay re-applies.
      // When a move causes a finish, record the authoritative finishTimeCs on it
      // so replay reports the exact recorded time rather than one recomputed from
      // a reconstructed clock (which could round differently at a cs boundary).
      if (event.raceId && event.raceId === room.raceId) {
        const rec = {
          playerId: event.playerId,
          dx: event.dx, dlane: event.dlane,
          t: room.startedAt ? Date.now() - room.startedAt : 0,
        };
        if (result.won && result.finishTimeCs != null) rec.finishTimeCs = result.finishTimeCs;
        room.moves.push(rec);
      }

      // Broadcast updated state + result to everyone in the room
      broadcast(event.roomId, {
        type:     'state',
        players:  snapshot(room),
        result,
        movedId:  event.playerId,
      });

      // Announce each crab as it finishes (the race keeps going for the rest).
      if (result.won && result.place != null) {
        const p = room.players.get(event.playerId);
        broadcast(event.roomId, {
          type:         'finish',
          playerId:     event.playerId,
          name:         p?.name ?? '?',
          place:        result.place,
          finishTimeCs: result.finishTimeCs,
        });
        // Record this finish on the leaderboard. startedAt is guaranteed by
        // applyMove (it guards on it before awarding a win); the extra check
        // here is defensive. Fire-and-forget — same pattern as saveRace — so
        // a Valkey hiccup never blocks the Kafka consumer loop.
        if (p && room.startedAt && result.finishTimeCs != null) {
          recordScore(p.name, result.finishTimeCs, p.colorIdx, room.seed, room.raceId, p.id);
        }
        onFinish(room, event.roomId);
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
        if (!room) {
          // First joiner also fixes the room's track config (lanes / length /
          // obstacle kinds), clamped server-side. Later joiners inherit room.config.
          const config = buildRoomConfig(msg.config);
          room = makeRoom(roomId, seed, config);
          rooms.set(roomId, room);
          // The lobby stays open for LOBBY_OPEN_MS, then locks automatically so
          // late arrivals can't join a stale room. The creator can lock it sooner
          // by starting the match.
          room.lockTimer = setTimeout(() => {
            lockLobby(room);
            broadcast(roomId, { type: 'lobby-locked', players: snapshot(room) });
            console.log(`[room:${roomId}] lobby locked (timeout)`);
          }, LOBBY_OPEN_MS);

          // Auto-close if nobody joins within SOLO_AUTOCLOSE_MS. The timer is
          // cancelled as soon as a second player arrives (see below). This keeps
          // stale solo lobbies from lingering in the room list forever.
          room.soloAutocloseTimer = setTimeout(() => {
            if (room.players.size <= 1 && !room.started) {
              if (room.lockTimer)        clearTimeout(room.lockTimer);
              if (room.dnfTimer)         clearTimeout(room.dnfTimer);
              if (room.countdownInterval) clearInterval(room.countdownInterval);
              // Notify the lone host before evicting them.
              broadcast(roomId, { type: 'room-closed', reason: 'No players joined within 30 seconds.' });
              rooms.delete(roomId);
              console.log(`[room:${roomId}] auto-closed (no joiners within ${SOLO_AUTOCLOSE_MS / 1000}s)`);
            }
          }, SOLO_AUTOCLOSE_MS);
        }

        if (room.started || room.locked) {
          sendTo(ws, { type: 'error', msg: 'Lobby closed — try another room' });
          return;
        }

        const playerId = `p${Date.now().toString(36)}${Math.random().toString(36).slice(2, 5)}`;

        // Pick the lowest colorIdx (0–3) not currently in use; if all four are
        // taken (rooms with 5+ players) fall back to cycling by join order.
        let colorIdx = 0;
        for (let i = 0; i < 4; i++) {
          if (!room.usedColors.has(i)) { colorIdx = i; break; }
          if (i === 3) colorIdx = room.players.size % 4; // all taken — wrap
        }
        room.usedColors.add(colorIdx);

        // Pick the lowest starting lane not currently in use; if all lanes are
        // taken fall back to the colorIdx-derived default.
        let startLane = colorIdx % room.config.lanes;
        for (let i = 0; i < room.config.lanes; i++) {
          if (!room.usedLanes.has(i)) { startLane = i; break; }
        }
        room.usedLanes.add(startLane);

        const newPlayer = makePlayer(playerId, name, colorIdx, room.config.lanes);
        newPlayer.lane = startLane; // override with the explicitly assigned lane
        newPlayer.startLane = startLane; // remembered so resetRoom can restore it
        room.players.set(playerId, newPlayer);
        clients.set(ws, { playerId, roomId, name });
        // First player in the room is its host — only they may start the match.
        if (!room.hostId) room.hostId = playerId;

        // A second player arrived — the solo auto-close is no longer needed.
        if (room.players.size >= 2 && room.soloAutocloseTimer) {
          clearTimeout(room.soloAutocloseTimer);
          room.soloAutocloseTimer = null;
          broadcast(roomId, { type: 'solo-autoclose-cancelled' });
        }
        // Mint a reconnect token so the client can resume after a brief disconnect.
        const rcToken = crypto.randomUUID();
        reconnectTokens.set(rcToken, { playerId, roomId, timer: null });

        sendTo(ws, {
          type: 'joined', playerId, colorIdx, seed: room.seed,
          config: configForClient(room.config), players: snapshot(room),
          isHost: playerId === room.hostId, opensInMs: LOBBY_OPEN_MS,
          soloAutocloseMs: room.soloAutocloseTimer
            ? Math.max(0, room.createdAt + SOLO_AUTOCLOSE_MS - Date.now())
            : 0,
          reconnectToken: rcToken,
        });
        broadcast(roomId, { type: 'player-joined', name, players: snapshot(room) });
        console.log(`[room:${roomId}] ${name} joined (${room.players.size} players)`);
        return;
      }

      // ── move ────────────────────────────────────────────────────────────────
      if (msg.type === 'move') {
        const info = clients.get(ws);
        if (!info) return;

        // Flood guard: drop moves once a player exceeds the per-second ceiling.
        // Silent drop (no error) — a legit fast tapper just loses the overflow.
        if (!await rateLimit(`rl:move:${info.playerId}`, RL_MOVE_LIMIT, RL_MOVE_WINDOW_SECS)) return;

        // A move only counts once the race is live; that's also when raceId
        // exists. No race → nothing to key on, so drop it (matches applyMove,
        // which rejects moves before the race starts).
        const room = rooms.get(info.roomId);
        if (!room || !room.raceId) return;

        // Validate move magnitude: each move must be exactly one step in at
        // most one axis. Legal values for dx and dlane are each in {-1, 0, 1}
        // and the two deltas must not both be non-zero (no diagonals).
        // Reject anything outside this set — a client sending dx:1000 would
        // skip straight to the finish line and bypass every obstacle.
        const dx    = Number.isInteger(msg.dx)    ? msg.dx    : 0;
        const dlane = Number.isInteger(msg.dlane) ? msg.dlane : 0;
        if (dx < -1 || dx > 1 || dlane < -1 || dlane > 1) {
          console.warn(`[move] rejected out-of-range delta from ${info.playerId}: dx=${dx} dlane=${dlane}`);
          return;
        }
        if (dx !== 0 && dlane !== 0) {
          console.warn(`[move] rejected diagonal move from ${info.playerId}: dx=${dx} dlane=${dlane}`);
          return;
        }

        // Publish to Kafka keyed on raceId so all of one race's moves share a
        // partition (→ total order for that race) and consecutive races in the
        // same room stay separable. raceId also rides in the value for replay.
        await producer.send({
          topic: TOPIC,
          messages: [{
            key:   room.raceId,
            value: JSON.stringify({
              action:   'move',
              raceId:   room.raceId,
              roomId:   info.roomId,
              playerId: info.playerId,
              dx, dlane,
              ts: Date.now(),
            }),
          }],
        }).catch(err => console.error('[kafka] produce error:', err.message));
        return;
      }

      // ── start-match: the room host locks the lobby and kicks off the race ────
      // Only the host (first joiner) may start, and only with 2+ players so a
      // lone crab doesn't launch a "multiplayer" race by itself.
      if (msg.type === 'start-match') {
        const info = clients.get(ws);
        if (!info) return;
        const room = rooms.get(info.roomId);
        if (!room || room.started) return;
        if (info.playerId !== room.hostId) {
          sendTo(ws, { type: 'error', msg: 'Only the lobby host can start the match' });
          return;
        }
        if (room.players.size < 2) {
          sendTo(ws, { type: 'error', msg: 'Need at least 2 players to start' });
          return;
        }
        startCountdown(room, info.roomId);
        return;
      }

      // ── solo-start: 1-player room, start immediately (no countdown) ─────────
      // Strictly solo-only: reject if there is more than one player in the room
      // so a non-host in a multiplayer lobby cannot use this path to bypass the
      // host check and the countdown that start-match enforces. The sender must
      // also be the host (the sole player of a solo room always is, but the
      // explicit check closes the door on any edge-case where room state diverges).
      if (msg.type === 'solo-start') {
        const info = clients.get(ws);
        if (!info) return;
        const room = rooms.get(info.roomId);
        if (!room || room.started) return;
        if (room.players.size !== 1) {
          sendTo(ws, { type: 'error', msg: 'solo-start is not allowed in a multiplayer room' });
          return;
        }
        if (info.playerId !== room.hostId) {
          sendTo(ws, { type: 'error', msg: 'Only the lobby host can start the race' });
          return;
        }
        room.started = true;
        beginRace(room);
        lockLobby(room);
        broadcast(info.roomId, { type: 'go', raceId: room.raceId, seed: room.seed, config: configForClient(room.config), players: snapshot(room) });
      }

      // ── reconnect: client is resuming after a brief disconnect ─────────────
      // The client sends { type:'reconnect', token, roomId }. If the token is
      // still valid (grace period hasn't expired) we cancel the removal timer,
      // re-associate the new WebSocket with the existing player slot, and send
      // the client a 'reconnected' message with the current room state so it can
      // restore its UI without a full rejoin.
      if (msg.type === 'reconnect') {
        const token  = String(msg.token  || '').slice(0, 64);
        const roomId = String(msg.roomId || '').slice(0, 32);
        const entry  = reconnectTokens.get(token);
        if (!entry || entry.roomId !== roomId) {
          sendTo(ws, { type: 'error', msg: 'Invalid or expired reconnect token' });
          return;
        }
        const room = rooms.get(roomId);
        if (!room) {
          reconnectTokens.delete(token);
          sendTo(ws, { type: 'error', msg: 'Room no longer exists' });
          return;
        }
        const player = room.players.get(entry.playerId);
        if (!player) {
          reconnectTokens.delete(token);
          sendTo(ws, { type: 'error', msg: 'Player slot no longer available' });
          return;
        }
        // Cancel the grace-period removal timer.
        if (entry.timer) { clearTimeout(entry.timer); entry.timer = null; }
        // Re-associate the fresh socket with the existing player.
        player.disconnected = false;
        clients.set(ws, { playerId: entry.playerId, roomId, name: player.name });
        broadcast(roomId, { type: 'player-reconnected', playerId: entry.playerId, players: snapshot(room) });
        sendTo(ws, {
          type:           'reconnected',
          playerId:       entry.playerId,
          colorIdx:       player.colorIdx,
          seed:           room.seed,
          config:         configForClient(room.config),
          players:        snapshot(room),
          isHost:         entry.playerId === room.hostId,
          raceInProgress: room.started && !room.finished,
          raceId:         room.raceId || null,
          reconnectToken: token, // reuse the same token
        });
        console.log(`[room:${roomId}] ${player.name} reconnected`);
        return;
      }

      // ── watch: stream a recorded race back to this socket ───────────────────
      // A spectator socket — it never joins a room. Loads the race from Valkey
      // (primary) and streams replay:true frames. If only the Kafka fallback has
      // it, we can replay moves but not the seed/config/roster, so we require the
      // Valkey record for a faithful replay and report unavailable otherwise.
      if (msg.type === 'watch') {
        const raceId = String(msg.raceId || '').slice(0, 64);
        const speed  = Number(msg.speed);
        const record = await loadRace(raceId);
        if (!record) {
          // Note whether the moves at least exist in Kafka, for diagnostics.
          const kMoves = await loadRaceFromKafka(raceId);
          sendTo(ws, { type: 'replay-unavailable', raceId,
            reason: kMoves ? 'record expired (moves in kafka only)' : 'not found' });
          return;
        }
        playRace(ws, record, { speed: Number.isFinite(speed) ? speed : 1 })
          .catch(err => { console.error('[replay] playRace error:', err.message);
            sendTo(ws, { type: 'replay-unavailable', raceId, reason: 'playback error' }); });
        return;
      }
    });

    ws.on('close', () => {
      const info = clients.get(ws);
      if (info) {
        const room = rooms.get(info.roomId);
        if (room) {
          // Find the reconnect token for this player (if any).
          let rcToken = null;
          for (const [t, v] of reconnectTokens) {
            if (v.playerId === info.playerId && v.roomId === info.roomId) { rcToken = t; break; }
          }

          if (rcToken) {
            // Grace period: keep the player slot alive, give the client 15 s to reconnect.
            const player = room.players.get(info.playerId);
            if (player) player.disconnected = true;
            // Broadcast to others so they know this player is temporarily away.
            broadcast(info.roomId, {
              type:     'player-disconnected',
              playerId: info.playerId,
              players:  snapshot(room),
            });
            // Schedule the full removal once the grace period expires.
            const entry = reconnectTokens.get(rcToken);
            entry.timer = setTimeout(() => {
              reconnectTokens.delete(rcToken);
              const r = rooms.get(info.roomId);
              if (!r) return;
              const p = r.players.get(info.playerId);
              if (!p) return; // already cleaned up (should not happen in normal flow)
              // Release colour/lane and remove the player.
              r.usedColors.delete(p.colorIdx);
              r.usedLanes.delete(p.lane);
              r.players.delete(info.playerId);
              // Host promotion if needed.
              if (info.playerId === r.hostId) {
                const next = r.players.keys().next();
                r.hostId = next.done ? null : next.value;
                if (r.hostId && !r.started) {
                  broadcast(info.roomId, { type: 'host-changed', hostId: r.hostId });
                }
              }
              broadcast(info.roomId, {
                type:     'player-left',
                playerId: info.playerId,
                players:  snapshot(r),
              });
              // If the race is live, a timed-out disconnected player counts as
              // gone — check whether the remaining field has all finished.
              if (r.started && !r.finished && allFinished(r)) {
                endRace(r, info.roomId);
              }
              if (r.players.size === 0) {
                if (r.lockTimer)          clearTimeout(r.lockTimer);
                if (r.dnfTimer)           clearTimeout(r.dnfTimer);
                if (r.countdownInterval)  clearInterval(r.countdownInterval);
                if (r.soloAutocloseTimer) clearTimeout(r.soloAutocloseTimer);
                rooms.delete(info.roomId);
                console.log(`[room:${info.roomId}] closed (empty)`);
              }
            }, GRACE_PERIOD_MS);
            console.log(`[room:${info.roomId}] ${info.name} disconnected (grace period started)`);
          } else {
            // No reconnect token — immediate removal (spectators, already-expired tokens).
            const leaving = room.players.get(info.playerId);
            if (leaving) {
              room.usedColors.delete(leaving.colorIdx);
              room.usedLanes.delete(leaving.lane);
            }
            room.players.delete(info.playerId);
            if (info.playerId === room.hostId) {
              const next = room.players.keys().next();
              room.hostId = next.done ? null : next.value;
              if (room.hostId && !room.started) {
                broadcast(info.roomId, { type: 'host-changed', hostId: room.hostId });
              }
            }
            broadcast(info.roomId, {
              type:     'player-left',
              playerId: info.playerId,
              players:  snapshot(room),
            });
            if (room.started && !room.finished && allFinished(room)) {
              endRace(room, info.roomId);
            }
            if (room.players.size === 0) {
              if (room.lockTimer)          clearTimeout(room.lockTimer);
              if (room.dnfTimer)           clearTimeout(room.dnfTimer);
              if (room.countdownInterval)  clearInterval(room.countdownInterval);
              if (room.soloAutocloseTimer) clearTimeout(room.soloAutocloseTimer);
              rooms.delete(info.roomId);
              console.log(`[room:${info.roomId}] closed (empty)`);
            }
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
