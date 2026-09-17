# LiveCollab

**LiveCollab is a fault-tolerant real-time collaborative editor that dogfoods distributed correctness testing and production observability.**

The project deliberately goes beyond a happy-path WebSocket demo. It asks whether multiple clients remain correct when edits are concurrent, connections disappear, backend processes restart, and shared infrastructure becomes slow.

## What the system does

```text
React clients
     |
     | HTTP / WebSocket
     v
   nginx
     |
     +-------------------+
     |                   |
     v                   v
backend-a             backend-b
     |                   |
     +------ Redis -------+
             Streams
```

The Go backends use Redis Streams as a shared event log and room-state store. Clients carry stable IDs, document versions, operation IDs, and resume cursors so a reconnecting browser can replay events it missed.

The project also contains a correctness runner that intentionally creates failures and verifies user-visible guarantees such as:

- stale edits cannot silently overwrite newer acknowledged state;
- reconnecting clients receive every missed event;
- replay contains no duplicates;
- replay order is monotonic;
- clients eventually converge after a backend restart.

Phase 3 adds the operational layer needed to understand the same system in production-style conditions: Prometheus metrics, OTLP traces through an OpenTelemetry Collector, structured correlated logs, Grafana dashboards, SLI/SLO rules, alerts, and a controlled Redis-latency incident.

---

# Phase 2 - correctness validation

## Real local validation on Windows 11 + Docker + Redis

The Phase 2 reconnect scenario was run locally against **real Redis in Docker**, not only the deterministic Redis test double.

Validated environment:

```text
Windows 11
Docker version 29.5.3
Docker Compose version v5.1.4
Go 1.26.2 windows/amd64
Node v24.12.0
```

Observed correctness report from the local run:

```text
LiveCollab Phase 2 Correctness Report

Scenario: reconnect-replay-across-backend-restart
Result: PASSED

Topology:
  Alice -> backend-a
  Bob   -> backend-b
  backend-a/backend-b -> Redis Streams
  Bob reconnects -> restarted backend-a

Failure/recovery sequence:
  Alice connected to backend-a while Bob connected to backend-b.
  Bob observed event 1 through backend-b, proving cross-node Redis Stream fanout.
  Bob disconnected after event 1.
  Alice committed and received acknowledgements for events 2 through 5.
  backend-a terminated after acknowledging event 5.
  backend-a restarted with empty process memory and hydrated room state from Redis.
  Bob reconnected to the restarted backend-a with resume cursor 1.
  Bob replayed events 2, 3, 4, 5 and converged to event 5.

Invariants:
  PASS  cross_node_fanout
        Bob, connected to backend-b, must observe Alice's event written through backend-a.
  PASS  zero_event_gaps_after_reconnect
        Expected replay 2->3->4->5; observed 2->3->4->5.
  PASS  no_duplicate_replay
        4 replay events, 4 unique sequence numbers.
  PASS  monotonic_event_order
        Replay order was 2->3->4->5.
  PASS  eventual_convergence_after_restart
        Bob ended at event 5, version 5; Redis-backed server is event 5, version 5.

Replay observed by Bob:
  expected: 2, 3, 4, 5
  observed: 2, 3, 4, 5

Final state:
  Bob content: "Start [2] [3] [4] [5]"
  Expected:    "Start [2] [3] [4] [5]"
  Server:      version 5, event 5
```

This is the core reliability evidence behind the project: the client recovered an exact, gap-free, duplicate-free ordered history after disconnecting and reconnecting through a restarted backend whose process memory had been lost.

The exact local validation transcript is preserved at `artifacts/phase2/local-validation-windows-real-redis.txt`.

---

# Phase 3 - production observability

Phase 3 answers:

> **How do we know this distributed collaboration system is healthy, and how do we diagnose why it becomes unhealthy?**

## Observability architecture

```text
                                +----------------------+
                                |      Prometheus      |
                                +----------+-----------+
                                           |
backend-a ---- /metrics -------------------+----> Grafana
backend-b ---- /metrics -------------------+       |
                                                   | dashboards + alerts
backend-a ---- OTLP/HTTP ----+                      |
backend-b ---- OTLP/HTTP ----+--> OTel Collector --> Tempo
                                                   |
backend-a ---- JSON stdout logs --------------------+ correlation by trace/operation IDs
backend-b ---- JSON stdout logs
```

### Metrics

Each Go backend exposes:

```text
GET /metrics
```

Important signals include:

```text
livecollab_websocket_connections
livecollab_operations_total
livecollab_operation_duration_seconds
livecollab_redis_command_duration_seconds
livecollab_stream_propagation_lag_seconds
livecollab_reconnects_total
livecollab_resume_success_total
livecollab_resume_failures_total
livecollab_replayed_events_total
livecollab_sequence_gaps_total
livecollab_stale_write_rejections_total
```

The correctness-oriented metrics are intentional. Infrastructure can be healthy while a user is still missing an event, so sequence gaps and reconnect failures are surfaced as first-class operational signals.

### Traces

The backend emits OTLP/HTTP JSON traces to the OpenTelemetry Collector for:

```text
livecollab.document.operation
redis.eval.apply_operation (child span)
livecollab.websocket.resume
HTTP request paths
```

Traces include operation IDs, room IDs, client IDs, versions, event sequences, and accepted/rejected state where relevant. The Collector exports traces to Tempo, which is provisioned as a Grafana data source.

### Structured logs

Backends emit JSON logs with correlation fields:

```json
{
  "event": "operation_completed",
  "instanceId": "backend-a",
  "traceId": "...",
  "operationId": "op-419",
  "roomId": "abc123",
  "clientId": "alice",
  "eventSequence": 42,
  "serverVersion": 42,
  "durationMs": 18.7
}
```

Inspect them with:

```powershell
docker compose logs -f backend-a backend-b
```

### Dashboard

Grafana automatically provisions **LiveCollab Reliability Overview** with panels for:

- active WebSocket connections;
- operation rate;
- operation p95 latency;
- Redis p95 latency;
- Redis Stream propagation lag;
- reconnect success/failure and replay volume;
- sequence gaps;
- stale writes rejected;
- HTTP p95 latency;
- Redis errors.

### SLO and alerts

The local demo defines explicit reliability objectives:

```text
Operation p95 latency       < 500ms
Redis command p95           < 250ms incident threshold
Reconnect recovery          target >= 99.9%
Sequence gaps               = 0
```

Prometheus alert rules include:

```text
LiveCollabHighRedisLatency
LiveCollabOperationLatencySLOBreach
LiveCollabSequenceGapDetected
LiveCollabReconnectFailureRate
```

These are demonstration SLOs for the portfolio environment, not claims derived from real production traffic.

---

# Run LiveCollab Phase 3 on Windows 11

Requirements already validated for the project:

```text
Docker + Docker Compose
Go 1.23+
Node 22+
```

Start the distributed backend and observability stack:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start-phase3.ps1
```

Or directly:

```powershell
docker compose up --build -d
docker compose ps
```

Start the React development UI:

```powershell
cd frontend
npm install
npm run dev
```

Open:

```text
LiveCollab UI   http://localhost:5173
Gateway          http://localhost:8080
backend-a        http://localhost:8081
backend-b        http://localhost:8082
Grafana          http://localhost:3000
Prometheus       http://localhost:9090
Tempo            http://localhost:3200
OTLP/HTTP        http://localhost:4318
```

Grafana anonymous access is enabled only for this local development environment.

## Validate telemetry

With Compose running:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\test-phase3-metrics.ps1
```

Expected:

```text
PASS backend port 8081 exposes Phase 3 metrics
PASS backend port 8082 exposes Phase 3 metrics
PASS Prometheus is scraping both backends
PASS Tempo is ready
PASS Grafana is healthy
Phase 3 observability smoke test PASSED
```

## Tempo storage on Docker Desktop

Tempo runs as an unprivileged container user. Phase 3 stores trace blocks under the image-owned `/var/tempo` directory so the named volume remains writable without running Tempo as root.

If you started an earlier Phase 3 archive that mounted the trace volume at `/tmp/tempo`, recreate only the Tempo volume once after updating:

```powershell
docker compose stop otel-collector
docker compose rm -f tempo
docker volume rm livecollab-phase3_livecollab-tempo
docker compose up -d tempo otel-collector grafana
```

This removes local trace data only. It does not remove the Redis, Prometheus, or Grafana volumes.

## Run the Redis-latency incident

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\phase3-incident.ps1
```

The script enables a debug-only artificial Redis delay on both backend instances and generates Redis-backed traffic long enough to cross the alert threshold.

Expected signal path:

```text
Redis latency injected
        |
        v
Redis p95 metric rises
        |
        v
Prometheus alert fires
        |
        v
room-check HTTP traces become slower in Tempo
        |
        v
JSON redis_command logs show increased durationMs
        |
        v
latency removed
        |
        v
dashboard returns toward baseline
```

The debug endpoint exists only when `LIVECOLLAB_ENABLE_DEBUG_FAULTS=true`, which the local Compose file enables intentionally.

---

# Regression tests

Backend:

```powershell
npm run test:backend
npm run test:race
```

Phase 1 stale-write correctness:

```powershell
npm run test:phase1
```

Phase 2 reconnect correctness:

```powershell
npm run test:phase2
```

Phase 2 against real Redis:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\test-phase2-real-redis.ps1
```

Run the core verification set:

```powershell
npm run verify
```

---

# Repository structure

```text
LiveCollab/
├── backend/
│   ├── telemetry.go              # Prometheus metrics, JSON logger, OTLP exporter
│   ├── redis_store.go            # instrumented Redis operations + fault delay
│   ├── ws.go                     # operation/reconnect telemetry
│   ├── server.go                 # /metrics + debug incident endpoints
│   └── ...
├── frontend/
│   └── src/pages/RoomPage.tsx
├── correctness-tests/
│   ├── run-phase1.js
│   ├── run-phase2.js
│   └── test-redis-server.js
├── infra/
│   ├── nginx.conf
│   ├── otel/collector.yaml
│   ├── prometheus/
│   │   ├── prometheus.yml
│   │   └── livecollab-rules.yml
│   ├── tempo/tempo.yaml
│   └── grafana/
│       ├── dashboards/livecollab-overview.json
│       └── provisioning/
├── scripts/
│   ├── start-phase3.ps1
│   ├── test-phase3-metrics.ps1
│   ├── phase3-incident.ps1
│   └── test-phase2-real-redis.ps1
├── artifacts/
│   ├── phase1/
│   └── phase2/
├── docs/
│   ├── phase1.md
│   ├── phase2.md
│   └── phase3.md
└── docker-compose.yml
```

# Project thesis

The collaborative editor is the product surface. The distributed correctness runner proves user-visible guarantees under failure. The observability layer makes those same guarantees and dependencies diagnosable while the system is running.

The project is intentionally **not** being pivoted into a generic observability product. It remains one coherent production-style application that demonstrates full-stack engineering, backend protocol design, event-driven distributed systems, failure recovery, correctness testing, and operational observability.
