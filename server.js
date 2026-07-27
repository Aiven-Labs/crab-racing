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

/** POST /leaderboard body: { name, timeCs } → records score, returns board. */
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
    await recordScore(name, timeCs, colorIdx, seed, raceId);
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
const LB_KEY      = 'leaderboard';        // sorted set: member = name, score = time (centiseconds)
const LB_COLOR_KEY = 'leaderboard:colors'; // hash: name → crab colorIdx
const LB_SEED_KEY  = 'leaderboard:seeds';  // hash: name → obstacle seed for that run
const LB_MAX      = 10;                    // keep only the top N
const LB_TTL_SECS = 60 * 60;               // ephemeral: whole board expires after 1h idle
const LB_RACE_KEY = 'leaderboard:races';   // hash: name → raceId of their best run
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
 *  `raceId` = the recorded race this run belongs to (for the watch link). */
async function recordScore(name, timeCs, colorIdx, seed = '', raceId = '') {
  if (valkey?.status !== 'ready' || !name || !isValidTimeCs(timeCs)) return;
  try {
    // Keep the player's *best* time only: GT means "update only if greater",
    // so we store the negated time and a fastest run wins the highest score.
    await valkey.zadd(LB_KEY, 'GT', -timeCs, name);
    // Remember the crab colour for this name so the board can render it.
    await valkey.hset(LB_COLOR_KEY, name, String(colorIdx | 0));
    // Remember the seed the run was raced on. Skip when unknown so we don't
    // overwrite a real seed with an empty one from an older client.
    if (seed)   await valkey.hset(LB_SEED_KEY, name, seed);
    // Remember which recorded race this run was, so the board can link to a
    // replay. Same skip-when-unknown rule as the seed.
    if (raceId) await valkey.hset(LB_RACE_KEY, name, raceId);
    // Trim to the top LB_MAX (drop everything below rank LB_MAX).
    await valkey.zremrangebyrank(LB_KEY, 0, -(LB_MAX + 1));
    await valkey.expire(LB_KEY, LB_TTL_SECS);
    await valkey.expire(LB_COLOR_KEY, LB_TTL_SECS);
    await valkey.expire(LB_SEED_KEY, LB_TTL_SECS);
    await valkey.expire(LB_RACE_KEY, LB_TTL_SECS);
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
    const races  = await valkey.hgetall(LB_RACE_KEY);  // { name: "raceId" }
    const board = [];
    for (let i = 0; i < flat.length; i += 2) {
      const name = flat[i];
      board.push({
        name,
        timeCs:   -Number(flat[i + 1]),
        colorIdx: Number(colors[name]) || 0,
        seed:     seeds[name] || '',
        // raceId links a board row to its watchable replay (absent if the run
        // predates replay support or the record has since expired).
        raceId:   races[name] || '',
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

// How long a freshly-created lobby stays open for others to join. After this the
// lobby locks automatically; the creator can also lock it early by starting the
// match. Locked lobbies reject new joiners (see the join handler).
const LOBBY_OPEN_MS = 60 * 1000;

function makeRoom(id, seed, config) {
  const { obstacles, map } = buildObstacles(seed, config);
  return {
    id, players: new Map(), started: false, finished: false, locked: false,
    hostId: null, lockTimer: null, seed, config, obsMap: map, obstacles,
    // Race timing / finish tracking. startedAt is stamped when the 'go' fires,
    // so finish times are measured server-side from a single authoritative clock.
    startedAt: null, dnfTimer: null, finishCount: 0,
    // raceId is a per-race UUID stamped when the race actually begins (go/solo).
    // It's the Kafka message key for this race's moves and the id a replay is
    // fetched by. `moves` buffers the applied move stream so we can persist the
    // whole race for replay when it ends. Both reset for each new race in a room.
    raceId: null, moves: [],
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
  if (!p || p.won || !room.started || room.finished) return { ok: false };

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
 *  clock. 0 if the race hasn't started (shouldn't happen for a valid move). */
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
  room.started = true;
  lockLobby(room); // no more joiners once the countdown begins
  let count = 3;
  const tick = setInterval(() => {
    broadcast(roomId, { type: 'countdown', count });
    if (count-- <= 0) {
      clearInterval(tick);
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
        }

        if (room.started || room.locked) {
          sendTo(ws, { type: 'error', msg: 'Lobby closed — try another room' });
          return;
        }

        const playerId = `p${Date.now().toString(36)}${Math.random().toString(36).slice(2, 5)}`;
        const colorIdx = room.players.size % 4;
        room.players.set(playerId, makePlayer(playerId, name, colorIdx, room.config.lanes));
        clients.set(ws, { playerId, roomId, name });
        // First player in the room is its host — only they may start the match.
        if (!room.hostId) room.hostId = playerId;

        sendTo(ws, {
          type: 'joined', playerId, colorIdx, seed: room.seed,
          config: configForClient(room.config), players: snapshot(room),
          isHost: playerId === room.hostId, opensInMs: LOBBY_OPEN_MS,
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

        const { dx = 0, dlane = 0 } = msg;

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

      // ── solo-multi: 1-player room, start immediately (for testing) ──────────
      if (msg.type === 'solo-start') {
        const info = clients.get(ws);
        if (!info) return;
        const room = rooms.get(info.roomId);
        if (!room || room.started) return;
        room.started = true;
        beginRace(room);
        lockLobby(room);
        broadcast(info.roomId, { type: 'go', raceId: room.raceId, seed: room.seed, config: configForClient(room.config), players: snapshot(room) });
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
          room.players.delete(info.playerId);
          // If the host bailed, promote the next remaining player so an open
          // lobby always has someone who can start it.
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
          // A leaver could be the last un-finished crab — if everyone still here
          // has finished, close the race out instead of waiting on the DNF timer.
          if (room.started && !room.finished && allFinished(room)) {
            endRace(room, info.roomId);
          }
          if (room.players.size === 0) {
            if (room.lockTimer) clearTimeout(room.lockTimer);
            if (room.dnfTimer)  clearTimeout(room.dnfTimer);
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
