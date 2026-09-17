# Phase 1: Product-thesis experiment

## Question

Can LiveCollab detect, explain, and reproduce a user-visible real-time correctness bug that a normal propagation test treats as success?

## Input

The Phase 1 runner accepts four conceptual inputs, currently encoded directly in `correctness-tests/run-phase1.js`:

1. A running Go WebSocket application.
2. Two browser clients, Alice and Bob.
3. A controlled ordering fault: hold Bob's operation until Alice's operation is acknowledged.
4. One invariant: a stale full-document update cannot silently remove a newer acknowledged contribution.

## Output

Each run produces:

- pass or fail
- final state and version observed by each client
- operation-level WebSocket timeline
- structured server lifecycle events
- invariant evidence
- likely defect class and suggested fix
- a portable application-order replay file

## Scenario

```text
Initial state: "Start", version 1

Bob prepares:
"Start [Bob]", baseVersion 1

The proxy holds Bob's frame.

Alice sends:
"Start [Alice]", baseVersion 1

The server applies and acknowledges Alice as version 2.
The proxy releases Bob's stale version-1 frame.
```

### Unsafe policy

The stale frame is accepted as version 3. All clients converge on `"Start [Bob]"`, but Alice's acknowledged contribution has disappeared. The invariant fails.

### Safe policy

The stale frame is rejected. Bob receives the current room snapshot and both clients remain on `"Start [Alice]"`, version 2. The invariant passes.

## Why convergence alone is insufficient

The unsafe run demonstrates an important distinction:

```text
clients converged = true
application correct = false
```

A load or propagation test that only checks whether every client eventually receives the final broadcast reports success. LiveCollab checks whether the execution preserved the application's stated guarantee.

## Validation gates before Phase 2

Continue toward a broader product only if most of these hold:

- The report identifies the relevant operations without manual log correlation.
- Engineers consider the timeline more useful than an ordinary failed E2E assertion.
- The recorded ordering reproduces the violation reliably.
- The same scenario verifies the fix.
- At least one external engineer offers a real incident to model.
- A second application can express correctness using the same core concepts: operation ID, acknowledgement, client observation, version or sequence, and invariant.

Pause or pivot if:

- every application requires a fully custom runner and evaluator
- instrumentation costs more than the debugging time it saves
- the report is effectively just reformatted logs
- engineers like the demonstration but would not put the check in CI
- a normal Playwright test provides the same diagnosis with substantially less setup

## Candidate Phase 2

The smallest credible next step is not Kubernetes or AI. It is a second invariant against the same protocol, likely one of:

- no event gap after reconnect
- client sequence never regresses
- one operation ID is applied at most once

A second reference workload should be added only after one of those invariants is reusable without application-specific knowledge.
