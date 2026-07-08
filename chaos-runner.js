/**
 * Crab Racing — local chaos runner
 *
 * Spins up N headless "racer" bots that connect to the server over WebSocket,
 * pile into a room, and then race in a deliberately chaotic way: random move
 * timing, random lane jitter, occasional illegal moves, rate-limit flooding,
 * and random disconnect/reconnect churn. Use it to smoke-test the server under
 * concurrent, adversarial load without opening a dozen browser tabs.
 *
 *   node chaos-runner.js [options]
 *
 * Options (all optional):
 *   --url <ws://host:port>   server WebSocket URL      (default ws://localhost:3000)
 *   --racers <n>             number of bots            (default 6)
 *   --room <id>              room to pile into          (default chaos)
 *   --seed <s>               fixed obstacle seed        (default random per run)
 *   --chaos <0..1>           how unhinged the bots are  (default 0.5)
 *   --duration <secs>        hard stop after N seconds  (default 60)
 *   --rooms <n>              spread bots over N rooms    (default 1)
 *   --out <prefix>           write <prefix>.log + <prefix>.json report
 *   --quiet                  only print summary + errors
 *
 * Requires the server (node server.js) plus its Kafka + Valkey to be up, e.g.
 * `docker compose up -d` then `node server.js`. This script only needs `ws`,
 * which is already a project dependency.
 *
 * Error logging: transport (ws) errors and server 'error' frames are always
 * counted and shown in the summary. With --out, every error is also streamed to
 * <prefix>.log as it happens, and the full stats (with a timestamped error list)
 * are dumped to <prefix>.json on exit — so a killed/crashed run leaves a trail.
 */
'use strict';

const WebSocket = require('ws');
const fs = require('fs');

// ── args ──────────────────────────────────────────────────────────────────────
function parseArgs(argv) {
  const opts = {
    url: 'ws://localhost:3000',
    racers: 6,
    room: 'chaos',
    seed: null,
    chaos: 0.5,
    duration: 60,
    rooms: 1,
    out: null,
    quiet: false,
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    const next = () => argv[++i];
    switch (a) {
      case '--url':      opts.url = next(); break;
      case '--racers':   opts.racers = Number(next()); break;
      case '--room':     opts.room = next(); break;
      case '--seed':     opts.seed = next(); break;
      case '--chaos':    opts.chaos = Number(next()); break;
      case '--duration': opts.duration = Number(next()); break;
      case '--rooms':    opts.rooms = Number(next()); break;
      case '--out':      opts.out = next(); break;
      case '--quiet':    opts.quiet = true; break;
      case '--help': case '-h':
        console.log(fs.readFileSync(__filename, 'utf8')
          .split('\n').slice(1, 35).join('\n').replace(/^ \*?/gm, ''));
        process.exit(0);
      default:
        console.error(`unknown option: ${a}`);
        process.exit(1);
    }
  }
  opts.chaos = Math.max(0, Math.min(1, opts.chaos));
  opts.racers = Math.max(1, opts.racers | 0);
  opts.rooms = Math.max(1, opts.rooms | 0);
  return opts;
}

const OPTS = parseArgs(process.argv);

// Deterministic-ish PRNG so a run is reproducible-flavoured but still varied.
// (Not cryptographic; just for jitter.) Seeded off the run seed if given.
let _s = (OPTS.seed ? hash(OPTS.seed) : 0x9e3779b9) >>> 0;
function rnd() {
  _s ^= _s << 13; _s ^= _s >>> 17; _s ^= _s << 5; _s >>>= 0;
  return _s / 0xffffffff;
}
function hash(str) {
  let h = 2166136261 >>> 0;
  for (let i = 0; i < str.length; i++) { h ^= str.charCodeAt(i); h = Math.imul(h, 16777619); }
  return h >>> 0;
}
const randInt = (n) => Math.floor(rnd() * n);
const pick = (arr) => arr[randInt(arr.length)];
const coin = (p = 0.5) => rnd() < p;

// ── aggregate stats across all bots ─────────────────────────────────────────────
const stats = {
  connects: 0, disconnects: 0, reconnects: 0,
  joined: 0, joinRejected: 0,
  movesSent: 0, garbageSent: 0,
  finishes: 0, dnf: 0, racesOver: 0,
  errors: 0,
  serverErrors: [],   // messages from server 'error' frames
  errorLog: [],       // [{ t: ISO, kind, racer, msg }] — full error trail for the report
};

const log = OPTS.quiet ? () => {} : (...a) => console.log(...a);

// Optional error log stream (--out). Opened lazily on first error so a clean run
// with --out still writes an (empty-error) .json report but no stray .log churn.
let logStream = null;
function logFile() {
  if (!OPTS.out) return null;
  if (!logStream) logStream = fs.createWriteStream(`${OPTS.out}.log`, { flags: 'a' });
  return logStream;
}

/** Record an error everywhere: counters, the in-memory trail, the console, and
 *  (with --out) the streamed .log file. `kind` is 'ws' | 'server'. */
function recordError(kind, racer, msg) {
  const entry = { t: new Date().toISOString(), kind, racer, msg };
  stats.errorLog.push(entry);
  const line = `[${entry.t}] ${kind} ${racer}: ${msg}`;
  logFile()?.write(line + '\n');
}
const CRAB_NAMES = ['Pinchy', 'Sheldon', 'Snips', 'Klaw', 'Molt', 'Barnacle',
  'Chomp', 'Scuttle', 'Nacre', 'Hermit', 'Coral', 'Bubbles', 'Rocky', 'Sandy'];

// ── a single bot racer ────────────────────────────────────────────────────────
class Racer {
  constructor(idx, roomId) {
    this.idx = idx;
    this.name = `${pick(CRAB_NAMES)}-${idx}`;
    this.roomId = roomId;
    this.ws = null;
    this.playerId = null;
    this.isHost = false;
    this.config = null;   // { lanes, finishCell/zones ... } from server
    this.racing = false;
    this.done = false;    // this bot has finished or the race is over
    this.moveTimer = null;
    this.alive = true;    // false once the runner is shutting down
    // Position tracked from 'state' frames so the bot can tell when it's wedged
    // against a blocking obstacle and steer around it (see doMove). Without this
    // a forward-only bot stalls at the first rock and never finishes.
    this.cell = 0;
    this.lane = 0;
    this.lanes = 3;
    this.prevCell = -1;   // cell at the previous move
    this.stalled = 0;     // consecutive moves that didn't advance our cell
    this.maxCell = 0;     // furthest cell reached (diagnostic: spot wedged bots)
  }

  connect() {
    this.ws = new WebSocket(OPTS.url);
    stats.connects++;
    this.ws.on('open', () => this.onOpen());
    this.ws.on('message', (raw) => this.onMessage(raw));
    this.ws.on('close', () => this.onClose());
    this.ws.on('error', (err) => {
      stats.errors++;
      // ECONNREFUSED & friends carry the detail on .code, not .message.
      const detail = err.message || err.code || String(err);
      recordError('ws', this.name, detail);
      log(`  ⚠ [${this.name}] ws error: ${detail}`);
    });
  }

  send(obj) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(obj));
    }
  }

  onOpen() {
    // Join the room. First bot into a room supplies the seed + a chaotic config
    // (later joiners inherit it, so only the creator's values matter).
    const join = { type: 'join', name: this.name, roomId: this.roomId };
    if (OPTS.seed) join.seed = OPTS.seed;
    // Occasionally throw a wild config at the room creator path — the server
    // clamps everything, so this exercises buildRoomConfig's guards.
    if (coin(OPTS.chaos * 0.5)) {
      join.config = {
        lanes: pick([1, 3, 12, 1e9, -4, 'nope']),
        trackLength: pick([10, 40, 200, 1e9, 0]),
        dnfTimeoutSecs: pick([3, 8, 120, -1]),
        obstacleKinds: pick([['hole', 'rock'], ['bogus'], [], ['jelly', 'net', 'beach']]),
      };
    }
    this.send(join);
  }

  onMessage(raw) {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {
      case 'joined':
        this.playerId = msg.playerId;
        this.isHost = !!msg.isHost;
        this.config = msg.config;
        this.lanes = (msg.config && msg.config.lanes) || this.lanes;
        stats.joined++;
        log(`  ✔ [${this.name}] joined ${this.roomId}` + (this.isHost ? ' (host)' : ''));
        break;

      case 'state': {
        // Track our own position so doMove knows whether the last move advanced
        // us. Every move round-trips as a 'state' broadcast to the whole room.
        const me = (msg.players || []).find(p => p.id === this.playerId);
        if (me) { this.cell = me.cell; this.lane = me.lane; if (me.cell > this.maxCell) this.maxCell = me.cell; }
        break;
      }

      case 'error':
        stats.serverErrors.push(msg.msg);
        if (/closed/i.test(msg.msg)) stats.joinRejected++;
        recordError('server', this.name, msg.msg);
        log(`  ⚠ [${this.name}] server: ${msg.msg}`);
        break;

      case 'countdown':
        break; // 3-2-1, ignore

      case 'go':
        this.config = msg.config || this.config;
        this.lanes = (this.config && this.config.lanes) || this.lanes;
        this.startRacing();
        break;

      case 'finish':
        if (msg.playerId === this.playerId) {
          stats.finishes++;
          this.done = true;
          this.stopRacing();
          log(`  🏁 [${this.name}] finished #${msg.place} @ ${msg.finishTimeCs}cs`);
        }
        break;

      case 'race-over':
        if (!this.done) {
          const me = (msg.results || []).find(r => r.id === this.playerId);
          if (me && me.dnf) { stats.dnf++; log(`  💤 [${this.name}] DNF`); }
        }
        this.done = true;
        this.stopRacing();
        break;
    }
  }

  onClose() {
    this.stopRacing();
    if (!this.alive) return;
    stats.disconnects++;
    // Chaotic churn: sometimes a dropped bot claws its way back in. Only useful
    // before the race locks — a reconnect after start just gets rejected, which
    // is itself a valid thing to exercise.
    if (coin(OPTS.chaos * 0.6)) {
      const delay = 200 + randInt(1500);
      setTimeout(() => {
        if (!this.alive) return;
        stats.reconnects++;
        log(`  ↻ [${this.name}] reconnecting…`);
        this.playerId = null;
        this.connect();
      }, delay);
    }
  }

  startRacing() {
    if (this.racing || this.done) return;
    this.racing = true;
    log(`  🚦 [${this.name}] racing`);
    this.scheduleMove();
  }

  stopRacing() {
    this.racing = false;
    if (this.moveTimer) { clearTimeout(this.moveTimer); this.moveTimer = null; }
  }

  scheduleMove() {
    if (!this.racing || this.done || !this.alive) return;
    // Move cadence: calmer bots tap steadily; chaotic bots burst and stall.
    // Base ~120ms; chaos widens the jitter and adds occasional flurries.
    const burst = coin(OPTS.chaos * 0.3);
    const delay = burst ? 5 + randInt(20) : 60 + randInt(200 + OPTS.chaos * 400);
    this.moveTimer = setTimeout(() => { this.doMove(); this.scheduleMove(); }, delay);
  }

  doMove() {
    if (!this.racing || this.done) return;

    // Rare pure garbage frame — the server should JSON-parse-guard and ignore it.
    if (coin(OPTS.chaos * 0.05)) {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.ws.send(pick(['not json', '{"type":', ' ', '[]']));
        stats.garbageSent++;
      }
      return;
    }

    // Track whether the last move actually advanced us. A blocking rock leaves
    // the cell unchanged, so a rising `stalled` count means we're wedged and
    // need to steer around it rather than headbutt the rock forever.
    if (this.cell === this.prevCell) this.stalled++; else this.stalled = 0;
    this.prevCell = this.cell;

    // Base intent: crawl forward. When wedged, move forward *and diagonally* into
    // an adjacent lane (dx=1 + dlane) to slip past the blocker — the track never
    // fully blocks a column, so a lane hop always opens a path. Chaos still layers
    // in the occasional lane wander, backpedal and illegal jump so the server's
    // bounds/obstacle guards keep getting exercised — but the forward-diagonal
    // recovery means the bot makes real progress and races actually finish.
    let dx = 1, dlane = 0;
    if (this.stalled >= 2) {
      dlane = (this.lane + 1 < this.lanes) ? 1 : -1;         // hop toward an open lane
    } else {
      const r = rnd();
      if (r < OPTS.chaos * 0.20) dlane = pick([-1, 1]);      // idle lane wander
      else if (r < OPTS.chaos * 0.26) dx = pick([-1, 0]);    // stall / backpedal
      else if (r < OPTS.chaos * 0.30) { dx = pick([-5, 5]); dlane = pick([-9, 9]); } // illegal jump
    }

    this.send({ type: 'move', dx, dlane });
    stats.movesSent++;

    // Flood test: a chaotic bot occasionally machine-guns moves to trip the
    // per-player Valkey rate limiter (RL_MOVE_LIMIT/sec). Server should silently
    // drop the overflow, not fall over.
    if (coin(OPTS.chaos * 0.08)) {
      for (let i = 0; i < 30; i++) { this.send({ type: 'move', dx: 1, dlane: 0 }); stats.movesSent++; }
    }
  }

  shutdown() {
    this.alive = false;
    this.stopRacing();
    if (this.ws) { try { this.ws.close(); } catch {} }
  }
}

// ── orchestration ───────────────────────────────────────────────────────────────
function main() {
  const runSeed = OPTS.seed || String(randInt(0xffffffff));
  console.log('🦀  Crab Racing — chaos runner');
  console.log(`    server   ${OPTS.url}`);
  console.log(`    racers   ${OPTS.racers} across ${OPTS.rooms} room(s)`);
  console.log(`    chaos    ${OPTS.chaos}   duration ${OPTS.duration}s   seed ${runSeed}\n`);
  if (OPTS.out) console.log(`    report   ${OPTS.out}.json  (errors also streamed to ${OPTS.out}.log)\n`);
  stats.meta = { url: OPTS.url, racers: OPTS.racers, rooms: OPTS.rooms,
    chaos: OPTS.chaos, duration: OPTS.duration, seed: runSeed,
    startedAt: new Date().toISOString() };

  // Spread racers across rooms so we exercise multiple concurrent lobbies.
  const racers = [];
  for (let i = 0; i < OPTS.racers; i++) {
    const roomId = OPTS.rooms > 1 ? `${OPTS.room}-${i % OPTS.rooms}` : OPTS.room;
    racers.push(new Racer(i, roomId));
  }

  // Group by room so we can pick a host per room and know when to start.
  const byRoom = new Map();
  for (const r of racers) {
    if (!byRoom.has(r.roomId)) byRoom.set(r.roomId, []);
    byRoom.get(r.roomId).push(r);
  }

  // Stagger connects a little so joins don't all land in the same tick.
  racers.forEach((r) => setTimeout(() => r.connect(), randInt(800)));

  // After a short lobby window, each room's host starts the match. A room with a
  // single bot uses solo-start (server allows a 1-player race for testing).
  const START_AFTER_MS = 2500;
  setTimeout(() => {
    for (const [roomId, group] of byRoom) {
      const joined = group.filter(r => r.playerId);
      if (joined.length === 0) { log(`  … room ${roomId}: nobody joined`); continue; }
      const host = joined.find(r => r.isHost) || joined[0];
      if (joined.length >= 2) {
        log(`  ▶ room ${roomId}: host ${host.name} starting (${joined.length} bots)`);
        host.send({ type: 'start-match' });
      } else {
        log(`  ▶ room ${roomId}: solo-start ${host.name}`);
        host.send({ type: 'solo-start' });
      }
    }
  }, START_AFTER_MS);

  // Hard stop: shut everything down and print the tally.
  setTimeout(() => finish(racers), OPTS.duration * 1000);

  // Ctrl-C also lands on the same summary path.
  process.on('SIGINT', () => { console.log('\n(interrupted)'); finish(racers); });
}

let finished = false;
function finish(racers) {
  if (finished) return;
  finished = true;
  racers.forEach(r => r.shutdown());

  const s = stats;
  console.log('\n──────── chaos summary ────────');
  console.log(`  connects / disconnects / reconnects : ${s.connects} / ${s.disconnects} / ${s.reconnects}`);
  console.log(`  joins ok / rejected                 : ${s.joined} / ${s.joinRejected}`);
  console.log(`  moves sent / garbage frames         : ${s.movesSent} / ${s.garbageSent}`);
  console.log(`  finishes / DNF                      : ${s.finishes} / ${s.dnf}`);
  console.log(`  ws errors                           : ${s.errors}`);

  // Per-bot progress — furthest cell reached vs the room's finish line. A cluster
  // of bots stuck well short of the finish points at a wedged/impassable track
  // (e.g. a 1-lane room where a single rock blocks the only column).
  const raced = racers.filter(r => r.maxCell > 0 || r.config);
  if (raced.length) {
    console.log('  progress (maxCell → finishCell, room config lanes):');
    for (const r of raced) {
      const fc = r.config && r.config.finishCell != null ? r.config.finishCell
               : (r.config && Array.isArray(r.config.zones)
                    ? r.config.zones.reduce((a, z) => a + z.len, 0) - r.config.zones[r.config.zones.length - 1].len
                    : '?');
      const ln = r.config ? r.config.lanes : '?';
      const flag = (fc !== '?' && r.maxCell < fc && !r.done) ? '  ⚠ stuck' : '';
      console.log(`      ${r.name.padEnd(12)} cell ${String(r.maxCell).padStart(3)} / ${fc}   lanes=${ln}${flag}`);
    }
  }
  if (s.serverErrors.length) {
    const counts = {};
    for (const m of s.serverErrors) counts[m] = (counts[m] || 0) + 1;
    console.log('  server error frames:');
    for (const [m, n] of Object.entries(counts)) console.log(`      ${n}× ${m}`);
  }
  console.log('───────────────────────────────');

  // Persist the full report (--out): stats snapshot + the timestamped error trail.
  if (OPTS.out) {
    stats.meta.endedAt = new Date().toISOString();
    try {
      fs.writeFileSync(`${OPTS.out}.json`, JSON.stringify(stats, null, 2));
      console.log(`  report written: ${OPTS.out}.json` +
        (stats.errorLog.length ? ` / ${OPTS.out}.log (${stats.errorLog.length} errors)` : ' (no errors)'));
    } catch (err) {
      console.error(`  ⚠ could not write report: ${err.message}`);
    }
    if (logStream) logStream.end();
  }

  // Give sockets a beat to close, then exit.
  setTimeout(() => process.exit(0), 300);
}

main();
