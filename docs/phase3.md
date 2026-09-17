# Phase 3 - Production Observability

Phase 3 keeps LiveCollab's product behavior unchanged and adds a production-style observability layer around the distributed collaboration path introduced in Phase 2.

## Goal

Answer three operational questions without manually correlating ad-hoc logs:

1. Is the real-time system healthy right now?
2. Are reconnect, ordering, and convergence guarantees degrading?
3. When latency spikes, which dependency or path is responsible?

## Architecture

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

backend telemetry
     |
     +-- /metrics --------------------> Prometheus ---> Grafana
     |
     +-- OTLP/HTTP traces -----------> OTel Collector ---> Tempo ---> Grafana
     |
     +-- structured JSON logs -------> docker compose logs
```

The backend deliberately keeps the Go dependency surface small. The existing minimal Redis client remains dependency-free, Prometheus metrics are emitted in the text exposition format, and traces are exported as OTLP/HTTP JSON to the OpenTelemetry Collector.

## Signals

### Metrics

Each backend exposes `GET /metrics`.

Core platform metrics:

- `livecollab_websocket_connections`
- `livecollab_operations_total`
- `livecollab_operation_errors_total`
- `livecollab_redis_errors_total`
- `livecollab_http_requests_total`

Correctness/recovery metrics:

- `livecollab_stale_write_rejections_total`
- `livecollab_reconnects_total`
- `livecollab_resume_success_total`
- `livecollab_resume_failures_total`
- `livecollab_replayed_events_total`
- `livecollab_sequence_gaps_total`
- `livecollab_stream_events_total`

Latency histograms:

- `livecollab_operation_duration_seconds`
- `livecollab_redis_command_duration_seconds`
- `livecollab_stream_propagation_lag_seconds`
- `livecollab_resume_duration_seconds`
- `livecollab_http_request_duration_seconds`

### Traces

The backend creates OTLP traces for:

- `livecollab.document.operation`
- `redis.eval.apply_operation` as a child span of document operations
- `livecollab.websocket.resume`
- HTTP request paths such as `HTTP GET /rooms/check`

Trace attributes include operation ID, room ID, client ID, base/server versions, event sequence, accepted/rejected status, and replay information where relevant.

### Structured logs

Docker logs are JSON records with stable correlation fields such as:

```json
{
  "timestamp": "...",
  "level": "info",
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

This allows an operation observed in the correctness report or UI to be searched in backend logs using the same operation ID and trace ID.

## SLIs and SLOs

Phase 3 defines a small set of portfolio/demo SLOs rather than pretending they are production targets derived from real traffic.

### Operation latency SLI

```text
p95 server-side operation duration
```

Demo objective:

```text
p95 < 500ms
```

### Redis dependency latency SLI

```text
p95 instrumented Redis command duration
```

Incident threshold:

```text
p95 < 250ms
```

### Reconnect correctness SLI

```text
successful reconnect resumes / reconnect attempts
```

Long-term target concept:

```text
>= 99.9%
```

### Correctness invariant signal

```text
sequence gaps = 0
```

A sequence gap is treated as a correctness incident, not merely a performance degradation.

## Alerts

Prometheus loads `infra/prometheus/livecollab-rules.yml`.

Included alerts:

- `LiveCollabHighRedisLatency`
- `LiveCollabOperationLatencySLOBreach`
- `LiveCollabSequenceGapDetected`
- `LiveCollabReconnectFailureRate`

The sequence-gap alert is deliberately critical because a gap means a reconnecting client may not have observed a complete event history.

## Grafana dashboard

Grafana provisions `LiveCollab Reliability Overview` automatically.

Panels include:

- active WebSocket connections;
- operations per second;
- sequence gaps;
- stale writes rejected;
- operation p95 latency;
- Redis p95 latency;
- Redis Stream propagation lag p95;
- reconnect successes/failures and replay counts;
- HTTP p95 latency;
- Redis errors.

Grafana also provisions Tempo so traces can be explored from the same UI.

## Controlled Redis-latency incident

Phase 3 includes an explicit debug-only fault hook. It is enabled only in the local Docker Compose environment via:

```text
LIVECOLLAB_ENABLE_DEBUG_FAULTS=true
```

The endpoint is:

```text
POST /debug/faults/redis-delay?ms=350
```

The PowerShell incident script applies the delay to both backend instances, generates Redis-backed traffic for long enough to cross the alert threshold, then restores normal operation.

Run:

```powershell
.\scripts\phase3-incident.ps1
```

Expected observation sequence:

```text
Redis delay injected
        |
        v
livecollab_redis_command_duration_seconds rises
        |
        v
Redis p95 dashboard panel crosses 250ms
        |
        v
LiveCollabHighRedisLatency fires
        |
        v
HTTP /rooms/check traces become slower in Tempo
        |
        v
JSON redis_command logs show elevated durationMs
        |
        v
incident script removes delay
        |
        v
metrics return toward baseline
```

This is intentionally application-visible observability: the same Redis dependency responsible for state persistence, room hydration, event replay, and cross-node fanout is the dependency being stressed.

## Phase 3 validation

Run the backend and correctness regressions:

```powershell
npm run test:backend
npm run test:phase1
npm run test:phase2
```

With Docker Compose running, validate the telemetry stack:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\test-phase3-metrics.ps1
```

The smoke test verifies that:

- both Go backends expose the Phase 3 metrics;
- Prometheus reports both backend scrape targets as healthy;
- Tempo reports ready.
- Grafana is healthy.

Tempo stores local trace blocks in `/var/tempo`, which is owned by the non-root user used by the Tempo image. Mounting an empty named volume at `/tmp/tempo` can leave the volume root-owned on Docker Desktop and cause `permission denied` during startup. If upgrading from the earlier mount path, remove only the `livecollab-tempo` volume once before restarting the trace services.

Then execute the incident:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\phase3-incident.ps1
```

## Scope boundary

Phase 3 is not a pivot into a generic observability SaaS. The collaborative editor remains the product surface and the correctness engine remains its internal reliability test system.

Phase 3 simply gives that distributed application the telemetry an engineer would need to operate and investigate it.

Deferred work includes Loki/log aggregation, Kubernetes, cloud deployment, Redis failover, stream retention/compaction, distributed presence, authentication, rate limiting, and generic external application instrumentation.
