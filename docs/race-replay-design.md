# Race replay — design

*A design for recording races and playing them back, built on the Kafka move
log we already produce.*

## Goal

Let a finished race be replayed move-for-move: the crabs re-crawl the identical
track (same seed + config) and cross the line in the same order at the same
times. Useful for leaderboard "watch this run" links, debugging a contested
finish, and demoing the game without live players.

## What we have today

Every move already flows through Kafka:

```
Browser ──WS──► server ──produce──► topic "race-moves" ──consume──► applyMove ──► broadcast state
```

The server is authoritative: `applyMove` (server.js) is a pure function of the
move + room state, and the track is a pure function of `(seed, config)`. So the
full history needed to reconstruct a race is just **its ordered move stream plus
its seed and config** — all of which already exist. We don't need to store
snapshots; we replay the inputs.

Two gaps stop us from using it as-is:

1. **No race identity.** The Kafka message `key` is the `roomId`, and the value
   carries `roomId` but no per-race id. A room hosts many races over its life,
   so "give me race X's moves" isn't expressible.
2. **No replay path.** The live consumer runs `fromBeginning:false` and applies
   moves to *current* room state. There's nothing that reads a past race back.

## Decision 1 — key on a per-race UUID

**Change the Kafka key from `roomId` to a per-race `raceId` (UUID).**

Why this is safe for ordering: the key only controls *partitioning*. All
messages with the same key land on the same partition and are therefore totally
ordered. Today every move in a room shares a key (`roomId`) → per-room ordering.
Switching to `raceId` keeps every move *within one race* on one partition, so
**per-race ordering is preserved** — which is exactly the unit replay cares
about. As a bonus, two consecutive races in the same room no longer share a
partition key, so they're cleanly separable.

Concretely:

- `makeRoom` (or `startCountdown`/`solo-start`, whenever a race actually begins)
  stamps `room.raceId = randomUUID()`. A fresh id per race, not per room.
- The producer uses it as both key and value field:

  ```js
  await producer.send({
    topic: TOPIC,
    messages: [{
      key: info.raceId,                       // was: info.roomId
      value: JSON.stringify({
        action: 'move', raceId: info.raceId,  // new
        roomId: info.roomId, playerId: info.playerId,
        dx, dlane, ts: Date.now(),
      }),
    }],
  });
  ```

- `raceId` is also stored on the leaderboard entry (new field in `recordScore`
  → `LB_*` hashes) so a board row can link to its replay.

The live consumer keys its in-memory room lookup on `roomId` (unchanged); it
just gains a `raceId` field it can ignore. Nothing about live play changes.

### Migration note

`clients.get(ws)` currently holds `{ playerId, roomId, name }`. It needs the
active `raceId` too, set when the race starts (`go`). Moves sent before `go`
don't exist (the client can't move pre-countdown), so there's no window where a
move lacks a `raceId`.

## Decision 2 — replay via a `fromBeginning:true` consumer

**Reconstruct a race by reading its moves back from Kafka and re-applying them
through the existing `applyMove`.** No second datastore — Kafka is already the
durable log.

Replay is a *separate, on-demand* consumer, not the live one:

```js
async function loadRace(raceId) {
  // seed + config for this race are needed to rebuild the identical track.
  // Store them at race start in Valkey (race:<id>:meta) so replay doesn't have
  // to reconstruct room config from scratch.
  const meta = await getRaceMeta(raceId);           // { seed, config, players }
  const room = makeRoom(`replay:${raceId}`, meta.seed, meta.config);
  for (const p of meta.players) room.players.set(p.id, makePlayer(...));

  const moves = [];
  const consumer = kafka.consumer({ groupId: `replay-${raceId}-${uuid}` });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
  await consumer.run({ eachMessage: async ({ message }) => {
    const e = JSON.parse(message.value.toString());
    if (e.action === 'move' && e.raceId === raceId) moves.push(e);
  }});
  // …read to end-of-log, then disconnect. moves[] is now the race, in order.
  return { room, moves };
}
```

Then playback re-applies them on a timeline:

```js
// Deliver to the watching client at the original cadence (ts deltas), or as
// fast as possible for a "instant result" reconstruction.
for (const m of moves) {
  const result = applyMove(room, m.playerId, m.dx, m.dlane);
  ws.send(JSON.stringify({ type: 'state', players: snapshot(room), result, movedId: m.playerId, replay: true }));
  await sleep(nextTsDelta);   // omit for instant replay
}
ws.send(JSON.stringify({ type: 'race-over', results: finalResults(room), replay: true }));
```

Because `applyMove` and the obstacle generator are deterministic and unchanged,
the replay reproduces the original race exactly — same stuns, blocks, finish
order and times.

### Why not filter by key server-side?

KafkaJS has no "read only this key" primitive — a consumer reads whole
partitions. Two options:

1. **Scan + filter by `raceId`** (shown above). Simple, works with the current
   4-partition topic. Cost: reads more than one race's worth of messages.
2. **Compact/dedicated partition per race** — overkill for this scale.

Given race move-counts are small (hundreds), option 1 is fine. If Kafka
retention has already dropped the race, replay falls back to "unavailable" (see
below).

## Retention & fallback

Kafka retention bounds how far back replay works. The move log isn't infinite,
so:

- Store a **compact per-race record in Valkey at finish** —
  `race:<raceId>` = `{ seed, config, players, moves:[…] }` with a TTL (say 24h,
  longer than the 1h leaderboard TTL). This is the *primary* replay source; it
  survives Kafka retention and avoids a full-partition scan.
- Kafka scan becomes the *fallback* / audit path.

This mirrors the existing "degrade, don't crash" posture: if neither source has
the race, the endpoint returns `{ error: 'replay unavailable' }` and the UI
hides the watch button.

> Open question: if Valkey holds the whole move list anyway, do we still need the
> Kafka scan path at all? Leaning yes for now — it's the audit/source-of-truth
> and costs nothing until invoked — but we could drop it to keep the surface
> small. Flagging for review.

## Surface

- `GET /replay/:raceId` → the compact race record (JSON) for a client-side
  player, **or**
- WebSocket `{ type: 'watch', raceId }` → server streams `state` frames tagged
  `replay:true` at the original cadence, reusing the exact frames the client
  already renders. The client's renderer needs no changes beyond ignoring input
  while `replay` is set.

Recommend the **WS streaming** path: it reuses the live render loop verbatim, so
a replay looks identical to a live race.

## Work breakdown

1. `raceId` plumbing: stamp on race start, thread through `clients` map, use as
   Kafka key + value field. (small, server.js)
2. Persist race meta + moves to Valkey at finish (`endRace`). (small)
3. Replay loader: Valkey-first, Kafka-scan fallback. (medium)
4. WS `watch` handler that streams `replay:true` state frames. (medium)
5. Client: honour `replay:true` (disable input, show a "replay" banner), add a
   watch button to leaderboard rows that have a `raceId`. (medium, game.html)
6. Extend chaos-runner with a `--replay <raceId>` verifier that watches a replay
   and asserts the finish order/times match the original. (small, testing)

## Non-goals

- Editing/scrubbing a replay (pause/seek) — v2.
- Replaying *partial* / in-progress races — only finished races are recorded.
- Cross-version replay — a replay is only guaranteed reproducible against the
  same server build (the obstacle generator must stay byte-identical, as the
  existing code comments already require).
