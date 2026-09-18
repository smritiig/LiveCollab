# Distributed presence leases (Patch 2)

Presence remains independent of document streams, replay, sequence/version and OCC.
Each connection retains its server-generated connectionId, clientId, username and instanceId.

For room ID `<id>` the exact keys are:

| Key | Type and value |
| --- | --- |
| `livecollab:room:<id>:presence` | Hash: connectionId -> JSON participant |
| `livecollab:room:<id>:presence-expiry` | Sorted set: connectionId -> absolute Redis-time expiry in milliseconds |
| `livecollab:room:<id>:presence-revision` | Integer membership revision |
| `livecollab:room:<id>:presence-events` | Stream of revision notifications, independently consumed by every backend |

Defaults: heartbeat/ping every 10 seconds, lease 30 seconds, sweep every 5 seconds.
Override with `LIVECOLLAB_PRESENCE_HEARTBEAT_MS`, `LIVECOLLAB_PRESENCE_LEASE_MS`,
`LIVECOLLAB_PRESENCE_SWEEP_MS`. Nonpositive values use defaults; lease duration
is raised to at least three heartbeat intervals.

Lua uses Redis TIME for join, renewal and cleanup. Renewal requires both existing
metadata and an unexpired lease, uses ZADD XX and never shortens expiry. It emits
no revision or notification. Delayed renewal cannot resurrect a removed connection.
Graceful removal deletes exactly that connection's metadata and expiry atomically;
only an actual membership removal increments revision and emits a notification.

The WebSocket sends control pings. Received pongs or data refresh its read deadline
and permit rate-limited renewal. A timer alone never renews membership. Missing
activity times out after the lease interval; Redis failure or lease loss closes the
socket so reconnect gets a new identity. Gorilla permits control writes alongside
normal writes. The reader owns pong handling and read-deadline changes.

Every backend scans presence hashes globally at startup and every sweep interval,
including rooms with no local clients. Lua atomically prunes expired (or lease-less)
metadata and expiry entries, increments revision once per changed batch and appends
one notification. Concurrent sweepers and repeated cleanup are idempotent. Global
snapshot reads perform the same prune before returning metadata and revision, so
expired members never appear in a first-read snapshot and revision guarding still
rejects delayed snapshots. After a total outage, the first surviving backend cleans
all discovered rooms; snapshots do not wait for that scan.

Operational limits: removal after a crash is normally within lease + sweep interval,
plus scheduling/Redis delays. Redis unavailability delays convergence. This patch
uses one SCAN pass per backend and O(room membership) atomic pruning, appropriate
for the existing topology; very large registries may need bounded batching later.
Existing Patch 1 records without leases are treated as expired; deploy both updated
backends together and reconnect old sockets. Existing stream retention is unchanged.
Very long initial replay blocks the socket reader, so replay exceeding the lease
can require reconnect; document replay itself is unchanged.

Tests use a controllable Redis fixture clock and channels. Real Lua tests additionally
run with LIVECOLLAB_TEST_PRESENCE_REDIS_ADDR set, expiring only isolated test keys.
