/**
 * Events + marketing consent.
 *
 * Events are owned by the Inkless leaderboard service ($LEADERBOARD_URL) —
 * this server stores nothing about them. We read its public GET /api/events
 * (cached) and add, per event:
 *
 *   seed   derived from the slug ("event:<slug>"), so every race at an event
 *          runs the identical track and nobody can change it short of renaming
 *          the event. The track config is the server default.
 *
 * Marketing consent: POST /api/events/:slug/consent publishes straight to
 * Kafka ($MARKETING_TOPIC). Nothing is written to our DB; the request only
 * succeeds once Kafka has acknowledged the record, so a 2xx means it's durable.
 * Both opt-ins and explicit opt-outs are published so downstream can honour
 * withdrawals.
 *
 * Event scores go straight to the leaderboard service (POST /api/v1/scores,
 * game token $LEADERBOARD_TOKEN) — no Kafka. Per its guide the token never
 * reaches the browser and the score is computed server-side:
 *   1. POST /api/events/:slug/race   → single-use race token (Valkey, 30 min),
 *                                      stamped with the server's start time.
 *   2. POST /api/events/:slug/score  { token, moves, finishTimeCs, name }
 *      The server replays `moves` on the event's locked track and only posts
 *      if they genuinely reach the finish. The claimed time must be consistent
 *      with the move timestamps and can't beat the server's own clock.
 *   The response carries the leaderboard's claim_url (player adds nickname +
 *   email there) and leaderboard_url.
 *
 * HTTP API (JSON):
 *   GET  /api/events                 events from the leaderboard service, plus seed/track
 *   GET  /api/events/:slug           one event
 *   POST /api/events/:slug/race      start a scored event race
 *   POST /api/events/:slug/score     submit a finished event race
 *   POST /api/events/:slug/consent   { email, marketingOptIn }
 * Pages:
 *   /event/:slug                     consent form + "race the event track" (event.html)
 *   /#event=:slug                    game.html with the event's locked seed
 */
const crypto = require('crypto');

const LEADERBOARD_URL = (process.env.LEADERBOARD_URL
  || 'https://01a1c5d8-10ba-7615-bcdb-557ec417a581-8000.eur-1.aiven.app').replace(/\/+$/, '');
const LEADERBOARD_TOKEN = process.env.LEADERBOARD_TOKEN || '';
const LEADERBOARD_GAME  = process.env.LEADERBOARD_GAME || 'crab-racing';
const MARKETING_TOPIC = process.env.MARKETING_TOPIC || 'marketing-consent';
const RACE_TOKEN_TTL_SECS = 30 * 60;
const MAX_MOVES           = 2000;
const CLOCK_SLACK_CS      = 300; // 3s of client/server clock + network tolerance
// Floor on input speed: ~30 moves/s is faster than key auto-repeat, so a run
// averaging quicker than this has forged timestamps.
const MIN_CS_PER_MOVE     = 3;
const RL_RACE_LIMIT = 30, RL_RACE_WINDOW_SECS = 60 * 10; // event races per IP per 10 min
const CACHE_TTL_MS    = 60_000;
const FETCH_TIMEOUT_MS = 5_000;
const RL_CONSENT_LIMIT = 10, RL_CONSENT_WINDOW_SECS = 60 * 10; // per IP per 10 min

const SLUG_RE  = /^[a-z0-9][a-z0-9-]{0,62}$/;
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

let deps = {
  producer: null, rateLimit: async () => true, sendJson: null, buildTrack: null,
  valkey: () => null,      // getter — the client is created in server.js main()
  simulateRun: null,       // (seed, moves) → { won, finishedAtMove }
};

/** Locked per-event seed. Pure function of the slug — nothing to store. */
const eventSeed = (slug) => `event:${slug}`;

// ── upstream events (cached) ───────────────────────────────────────────────────
let cache = { at: 0, events: null };
let inFlight = null;

async function fetchEvents() {
  const res = await fetch(`${LEADERBOARD_URL}/api/events`, { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) });
  if (!res.ok) throw new Error(`leaderboard /api/events HTTP ${res.status}`);
  const body = await res.json();
  const list = Array.isArray(body?.events) ? body.events : [];
  return list
    .filter(e => SLUG_RE.test(String(e.slug || '')))
    // The service hosts several games' events; only race the ones that include
    // ours (an event without a games list is treated as open to every game).
    .filter(e => !Array.isArray(e.games) || e.games.includes(LEADERBOARD_GAME))
    .map(e => ({
    slug:     e.slug,
    name:     String(e.name || e.slug),
    startsAt: e.starts_at || null, // UTC, may be null = always open
    endsAt:   e.ends_at || null,
  }));
}

/** Events list; serves stale on upstream failure, throws only if we never had one. */
async function getEvents() {
  if (cache.events && Date.now() - cache.at < CACHE_TTL_MS) return cache.events;
  inFlight ??= fetchEvents()
    .then(events => { cache = { at: Date.now(), events }; return events; })
    .catch(err => {
      console.error('[events] upstream fetch failed:', err.message);
      if (cache.events) return cache.events;
      throw err;
    })
    .finally(() => { inFlight = null; });
  return inFlight;
}

async function findEvent(slug) {
  if (!SLUG_RE.test(slug)) return null;
  return (await getEvents()).find(e => e.slug === slug) || null;
}

const withTrack = (ev) => ({ ...ev, locked: true, seed: eventSeed(ev.slug), track: deps.buildTrack({}) });

// ── setup ──────────────────────────────────────────────────────────────────────
async function ensureTopic(admin, existingTopics) {
  if (!existingTopics.includes(MARKETING_TOPIC)) {
    await admin.createTopics({ topics: [{ topic: MARKETING_TOPIC, numPartitions: 3, replicationFactor: 1 }] });
    console.log(`✓ Created Kafka topic "${MARKETING_TOPIC}"`);
  }
}

function init({ producer, rateLimit, sendJson, buildTrack, valkey, simulateRun }) {
  deps = { producer, rateLimit, sendJson, buildTrack, valkey, simulateRun };
  console.log(`✓ events sourced from ${LEADERBOARD_URL}`);
  if (!LEADERBOARD_TOKEN) console.error('[events] LEADERBOARD_TOKEN unset — event scores will not be posted');
}

// ── HTTP ───────────────────────────────────────────────────────────────────────
function readJson(req, limit = 4 * 1024) {
  return new Promise((resolve) => {
    let raw = '';
    req.on('data', c => { raw += c; if (raw.length > limit) { req.destroy(); resolve(undefined); } });
    req.on('end', () => { try { resolve(JSON.parse(raw)); } catch { resolve(undefined); } });
    req.on('error', () => resolve(undefined));
  });
}

const clientIp = (req) => String(req.headers['x-forwarded-for'] || '').split(',')[0].trim()
  || req.socket.remoteAddress || 'unknown';

/** Route handler. Returns true if it handled the request. */
function handle(req, res, route) {
  if (route !== '/api/events' && !route.startsWith('/api/events/')) return false;
  const { sendJson } = deps;
  const parts = route.split('/').filter(Boolean).slice(2);
  const run = async () => {
    if (parts.length === 0 && req.method === 'GET') {
      return sendJson(res, 200, (await getEvents()).map(withTrack));
    }
    if (parts.length === 1 && req.method === 'GET') {
      const ev = await findEvent(parts[0]);
      return ev ? sendJson(res, 200, withTrack(ev)) : sendJson(res, 404, { error: 'event not found' });
    }
    if (parts.length === 2 && parts[1] === 'consent' && req.method === 'POST') {
      return consent(req, res, parts[0]);
    }
    if (parts.length === 2 && parts[1] === 'race' && req.method === 'POST') {
      return startRace(req, res, parts[0]);
    }
    if (parts.length === 2 && parts[1] === 'score' && req.method === 'POST') {
      return submitScore(req, res, parts[0]);
    }
    sendJson(res, 404, { error: 'not found' });
  };
  run().catch(err => {
    console.error('[events] error:', err.message);
    if (!res.headersSent) sendJson(res, 503, { error: 'events unavailable' });
  });
  return true;
}

async function consent(req, res, slug) {
  const { sendJson } = deps;
  if (!await deps.rateLimit(`rl:consent:${clientIp(req)}`, RL_CONSENT_LIMIT, RL_CONSENT_WINDOW_SECS)) {
    return sendJson(res, 429, { error: 'rate limited' });
  }
  const body = await readJson(req);
  if (!body) return sendJson(res, 400, { error: 'bad json' });

  const email = String(body.email || '').trim().toLowerCase();
  if (email.length > 254 || !EMAIL_RE.test(email)) return sendJson(res, 400, { error: 'invalid email' });
  if (typeof body.marketingOptIn !== 'boolean') {
    return sendJson(res, 400, { error: 'marketingOptIn must be true or false' });
  }

  const ev = await findEvent(slug);
  if (!ev) return sendJson(res, 404, { error: 'event not found' });
  if (!deps.producer) return sendJson(res, 503, { error: 'consent unavailable' });

  const record = {
    type:           'marketing-consent',
    schemaVersion:  1,
    id:             crypto.randomUUID(),     // dedupe key for at-least-once delivery
    source:         'crab-racing',
    eventSlug:      ev.slug,
    eventName:      ev.name,
    email,
    marketingOptIn: body.marketingOptIn,
    consentedAt:    new Date().toISOString(),
    // Evidence of what the person agreed to (bounded; supplied by event.html).
    consentText:    String(body.consentText || '').slice(0, 500),
    userAgent:      String(req.headers['user-agent'] || '').slice(0, 300),
  };
  try {
    await deps.producer.send({
      topic: MARKETING_TOPIC,
      acks: -1, // all in-sync replicas — 2xx means durable
      messages: [{
        // Same person → same partition → their consent changes stay ordered.
        key: crypto.createHash('sha256').update(email).digest('hex'),
        headers: { 'content-type': 'application/json', 'schema-version': '1' },
        value: JSON.stringify(record),
      }],
    });
  } catch (err) {
    console.error('[events] kafka produce failed:', err.message);
    return sendJson(res, 503, { error: 'could not record consent, please try again' });
  }
  sendJson(res, 202, { ok: true, marketingOptIn: record.marketingOptIn });
}

// ── event scores → leaderboard service ────────────────────────────────────────
const raceKey = (token) => `evrace:${token}`;
const TOKEN_RE = /^[0-9a-f]{32}$/;

/** Issue a single-use race token. Fails closed (503) without Valkey — an
 *  unverifiable time must never land on an event's prize board. */
async function startRace(req, res, slug) {
  const { sendJson } = deps;
  if (!LEADERBOARD_TOKEN) return sendJson(res, 503, { error: 'event scoring not configured' });
  if (!await deps.rateLimit(`rl:evrace:${clientIp(req)}`, RL_RACE_LIMIT, RL_RACE_WINDOW_SECS)) {
    return sendJson(res, 429, { error: 'rate limited' });
  }
  const ev = await findEvent(slug);
  if (!ev) return sendJson(res, 404, { error: 'event not found' });
  const valkey = deps.valkey();
  if (valkey?.status !== 'ready') return sendJson(res, 503, { error: 'event scoring unavailable' });

  const token = crypto.randomBytes(16).toString('hex');
  await valkey.set(raceKey(token), JSON.stringify({ slug, startedAt: Date.now() }), 'EX', RACE_TOKEN_TTL_SECS);
  sendJson(res, 200, { token });
}

/** Check a submitted run. Returns an error string, or null if it's valid. */
function validateRun(body, startedAt) {
  const moves = body.moves;
  if (!Array.isArray(moves) || moves.length === 0 || moves.length > MAX_MOVES) return 'invalid moves';
  let lastT = 0;
  for (const m of moves) {
    if (!m || ![-1, 0, 1].includes(m.dx) || ![-1, 0, 1].includes(m.dlane)) return 'invalid moves';
    const t = Number(m.t);
    if (!Number.isInteger(t) || t < lastT) return 'invalid move timing';
    lastT = t;
  }
  const finishTimeCs = Number(body.finishTimeCs);
  if (!Number.isInteger(finishTimeCs) || finishTimeCs <= 0) return 'invalid finishTimeCs';
  // The finishing move happens at (or just before) the finish time.
  if (finishTimeCs < lastT) return 'finish time is before the last move';
  if (finishTimeCs < moves.length * MIN_CS_PER_MOVE) return 'moves are faster than humanly possible';
  // Can't have taken longer on the client than has elapsed on the server.
  const serverElapsedCs = Math.round((Date.now() - startedAt) / 10);
  if (finishTimeCs > serverElapsedCs + CLOCK_SLACK_CS) return 'finish time exceeds race session';
  return null;
}

async function submitScore(req, res, slug) {
  const { sendJson } = deps;
  if (!LEADERBOARD_TOKEN) return sendJson(res, 503, { error: 'event scoring not configured' });
  const body = await readJson(req, 64 * 1024);
  if (!body) return sendJson(res, 400, { error: 'bad json' });

  const token = String(body.token || '');
  if (!TOKEN_RE.test(token)) return sendJson(res, 403, { error: 'invalid race token' });
  const valkey = deps.valkey();
  if (valkey?.status !== 'ready') return sendJson(res, 503, { error: 'event scoring unavailable' });
  // Single-use: GETDEL so a token can never post twice, even concurrently.
  const raw = await valkey.getdel(raceKey(token));
  if (!raw) return sendJson(res, 403, { error: 'invalid or already used race token' });
  const session = JSON.parse(raw);
  if (session.slug !== slug) return sendJson(res, 403, { error: 'race token is for a different event' });

  const invalid = validateRun(body, session.startedAt);
  if (invalid) return sendJson(res, 422, { error: invalid });

  // Re-simulate on the locked event track — the only way to know the moves
  // actually reach the finish rather than trusting the client.
  const sim = deps.simulateRun(eventSeed(slug), body.moves);
  if (!sim.won) return sendJson(res, 422, { error: 'run does not reach the finish' });
  // Ignore any inputs after the finishing move; its timestamp anchors the time.
  const finishTimeCs = Number(body.finishTimeCs);

  const ev = await findEvent(slug);
  if (!ev) return sendJson(res, 404, { error: 'event not found' });

  const sessionId = crypto.randomBytes(16).toString('hex'); // also the player's claim key
  let upstream;
  try {
    const r = await fetch(`${LEADERBOARD_URL}/api/v1/scores`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${LEADERBOARD_TOKEN}`, 'Content-Type': 'application/json' },
      signal: AbortSignal.timeout(10_000),
      body: JSON.stringify({
        game:       LEADERBOARD_GAME,
        session_id: sessionId,
        score:      finishTimeCs * 10, // time_ms
        event:      slug,
        source:     'web',
        details:    {
          name:  String(body.name || 'Crab').slice(0, 20),
          moves: sim.finishedAtMove,
          seed:  eventSeed(slug),
        },
      }),
    });
    upstream = await r.json().catch(() => ({}));
    if (!r.ok) {
      console.error(`[events] leaderboard rejected score (HTTP ${r.status}):`, JSON.stringify(upstream).slice(0, 300));
      return sendJson(res, 502, { error: 'leaderboard rejected the score' });
    }
  } catch (err) {
    console.error('[events] leaderboard post failed:', err.message);
    return sendJson(res, 502, { error: 'could not reach the leaderboard' });
  }
  console.log(`[events] posted ${finishTimeCs * 10}ms to ${slug} (session ${sessionId.slice(0, 8)}…)`);
  sendJson(res, 200, {
    ok: true, eventSlug: slug, eventName: ev.name, timeMs: finishTimeCs * 10,
    claimUrl: upstream.claim_url || null, leaderboardUrl: upstream.leaderboard_url || null,
  });
}

module.exports = { init, ensureTopic, handle, eventSeed, MARKETING_TOPIC };
