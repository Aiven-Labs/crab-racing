# Rate limiting a multiplayer game with Valkey

*How we keep Crab Racing fair and flood-proof — without a limiter that falls
apart the moment we add a second server.*

## The setup

Crab Racing is a real-time multiplayer game. Browsers talk to a Node server over
WebSockets; every move is published to Kafka, applied server-side, and
broadcast back out to everyone in the room. There's also a small HTTP surface —
notably `POST /leaderboard`, which records a finish time.

Two of those paths are wide open to abuse:

- **Moves.** One `move` message in means one Kafka produce plus a broadcast to
  every client in the room. A client that mashes a key — or a script that skips
  the client entirely — can amplify a single connection into a flood.
- **The leaderboard POST.** It's an unauthenticated HTTP endpoint. Nothing stops
  someone from hammering it.

So we need rate limiting. The interesting question isn't *whether* — it's
*where the counter lives*.

## The trap: an in-memory limiter

The obvious move is a `Map` of counters in the Node process. A few lines, no new
dependency, done.

Except Crab Racing's room and client state is already **per-process** — it lives
in plain `Map`s inside `server.js`. That's fine for a single instance, but the
moment the game runs behind a load balancer with more than one instance, an
in-memory rate limiter has a hole in it: **the counter only sees the traffic
that landed on that one instance.** Reconnect, get balanced to a different
instance, and your counter resets. An attacker doesn't even have to try hard —
normal reconnection behavior spreads their requests across instances and dilutes
every limit.

An in-memory limiter, in other words, is only as correct as your instance count.
At one instance it works; at two it's theater.

## The fix: let Valkey hold the counter

We already run [Valkey](https://valkey.io/) (a Redis-compatible store) for the
ephemeral leaderboard. It's the natural home for a rate-limit counter too,
because it's **shared by every instance**. One counter, one source of truth, no
matter how many Node processes sit in front of it.

The classic fixed-window counter is two commands — `INCR` to count, `EXPIRE` to
reset the window:

```js
// Returns true if the action is allowed, false if the caller is over the limit.
// key e.g. `rl:move:<playerId>` or `rl:lb:<ip>`; limit = max ops per window.
async function rateLimit(key, limit, windowSecs) {
  if (valkey?.status !== 'ready') return true; // fail open — see below
  const n = await valkey.incr(key);
  if (n === 1) await valkey.expire(key, windowSecs); // first hit starts the window
  return n <= limit;
}
```

The first request for a key increments to `1` and stamps a TTL; every subsequent
request in that window just increments. When the window's TTL lapses, the key
disappears and the count starts over. Because Valkey executes each command
atomically, two instances incrementing the same key can't race.

Wiring it in:

```js
// Moves — gate before producing to Kafka.
if (!await rateLimit(`rl:move:${info.playerId}`, 20, 1)) return;

// Leaderboard POST — reject with a 429 when over the limit.
if (!await rateLimit(`rl:lb:${ip}`, 5, 60)) {
  return sendJson(res, 429, { error: 'rate limited' });
}
```

## The one rule that matters: fail open

There's a subtle decision baked into that helper's first line. If Valkey is
unreachable, `rateLimit` returns `true` — it lets the request through.

That's deliberate, and it matches how everything else in Crab Racing treats
Valkey. The leaderboard already no-ops when the store is down instead of
crashing the game. Rate limiting has to behave the same way: **a Valkey blip
should never stop people from playing.** A brief window where we can't enforce
limits is a far better failure than a game that goes dark because a cache
hiccuped. Fail open, always.

## Conventions

A couple of small habits keep this tidy:

- **Prefix rate-limit keys with `rl:`** and always give them a short TTL, so
  they expire on their own and never accumulate.
- **Fail open** whenever the client isn't `ready`.

## If fixed windows aren't enough

Fixed windows have one known wart: a burst right at the boundary can briefly do
up to *2×* the limit (the tail of one window plus the head of the next). For
moves and a leaderboard POST that's well within tolerance. If we ever need it
tighter, the upgrade path is a **token bucket implemented as a Lua script** —
Valkey runs the whole script atomically, so the read-modify-write stays race-free
across instances. Same store, same fail-open rule, just a smarter counter.

---

*Tracked in [issue #39](https://github.com/Aiven-Labs/crab-racing/issues/39).
The cross-instance state problem that motivates all of this is
[#32](https://github.com/Aiven-Labs/crab-racing/issues/32).*
