# Phase 2: Reconnect Correctness Across Backend Instances

## Goal

Phase 2 answers a narrower question than "can LiveCollab scale?":

> If a client disconnects, operations continue on another node, and a backend process restarts, can the reconnecting client recover every missed operation exactly once, in order, and converge to the authoritative state?

## Scenario

```text
Alice -> backend-a ----+
                       +-> Redis Streams
Bob   -> backend-b ----+
```

1. Both clients connect to the same room through different backend instances.
2. Alice writes event 1 through backend-a.
3. Bob receives event 1 through backend-b's Redis Stream watcher.
4. Bob disconnects and remembers sequence 1.
5. Alice writes events 2-5 and receives acknowledgements.
6. backend-a terminates.
7. backend-a restarts with no in-process room state.
8. The restarted process hydrates room version 5 / sequence 5 from Redis.
9. Bob reconnects to the restarted backend with `lastSequence=1`.
10. The backend reads the room stream and sends events 2-5.

## Invariants

### Cross-node fanout

An event accepted by backend-a must be observable by a client connected to backend-b.

### Zero gaps after reconnect

If Bob last observed event 1 and the authoritative stream contains 2-5, the replay must be exactly:

```text
2, 3, 4, 5
```

### No duplicate replay

Each sequence number must appear at most once in Bob's reconnect replay.

### Monotonic event order

Every replayed sequence must be greater than the sequence before it.

### Eventual convergence after restart

After replay completes, Bob's document/version/sequence must represent the same final operation acknowledged before backend-a terminated.

## Distributed write path

Real Redis mode uses one Lua operation to atomically:

1. read the current document version;
2. compare it with the client's `baseVersion`;
3. reject a stale update when the configured policy is `reject`;
4. allocate the next room sequence;
5. update the authoritative content and version;
6. append the accepted operation to the Redis Stream.

This avoids acknowledging a state transition that exists only in one Go process.

## Resume protocol

A reconnect request carries:

```text
clientId
lastSequence
```

The server replies:

```text
resume_started
0..N content_update replay messages
resume_complete
```

The client uses the sequence field for duplicate suppression and monotonicity checks.

## Why both version and sequence exist

`serverVersion` describes the document revision used for optimistic concurrency control.

`sequence` describes the total order of accepted room events used for delivery and resume.

They happen to advance together in the Phase 2 editor, but they represent different contracts and are kept separate so future non-document events do not have to mutate document version.

## Testing strategy

The default `npm run test:phase2` starts:

- a deterministic Redis-protocol test double;
- backend-a;
- backend-b;
- two native WebSocket scenario clients.

The test double is deliberately small and is not shipped as application infrastructure. It exists so the cross-process algorithm can run in CI without requiring Docker.

For production-path validation, set `LIVECOLLAB_REDIS_ADDR=127.0.0.1:6379` and run the same scenario against Redis from Docker Compose.

## Deliberate limitations

Phase 2 does not implement:

- distributed/global presence;
- Redis Cluster or Sentinel failover;
- stream trimming or snapshot compaction;
- client edits authored while offline;
- exactly-once side effects outside the document state;
- authentication/authorization;
- a generic scenario DSL or SDK for third-party applications.

These are deferred because none is required to test the reconnect-correctness thesis.

## Phase 3 decision gate

Before generalizing the tool, evaluate whether the Phase 1 and Phase 2 artifacts materially improve debugging compared with application-specific end-to-end tests and raw logs.

Good signals include:

- an engineer can explain the failure faster from `report.html` than from raw logs;
- a real incident can be described as an operation sequence plus invariant;
- the invariant logic is reusable across more than one scenario;
- someone wants the scenario in CI;
- another WebSocket application can expose enough state with limited instrumentation.

If those signals are weak, keep LiveCollab as a production-style collaborative application with unusually strong correctness testing instead of forcing the runner into a standalone product.
