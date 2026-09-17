#!/usr/bin/env node

const { EventEmitter } = require("node:events");
const { spawn } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const ROOT = path.resolve(__dirname, "..");
const BACKEND_DIR = path.join(ROOT, "backend");
const ARTIFACT_DIR = path.join(ROOT, "artifacts", "phase2", "reconnect-replay");
const BACKEND_A_PORT = Number(process.env.LIVECOLLAB_BACKEND_A_PORT || 18081);
const BACKEND_B_PORT = Number(process.env.LIVECOLLAB_BACKEND_B_PORT || 18082);
const TEST_REDIS_PORT = Number(process.env.LIVECOLLAB_TEST_REDIS_PORT || 16379);
const EXTERNAL_REDIS_ADDR = process.env.LIVECOLLAB_REDIS_ADDR || "";
const REDIS_ADDR = EXTERNAL_REDIS_ADDR || `127.0.0.1:${TEST_REDIS_PORT}`;

if (typeof WebSocket !== "function") {
  throw new Error(
    "Phase 2 runner requires Node.js 22+ for the built-in WebSocket client. " +
      "The LiveCollab application itself can still run on older supported Node versions."
  );
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function ensureDir(directory) {
  fs.mkdirSync(directory, { recursive: true });
}

function runCommand(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: options.cwd || ROOT,
      env: { ...process.env, ...(options.env || {}) },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => (stdout += chunk.toString()));
    child.stderr.on("data", (chunk) => (stderr += chunk.toString()));
    child.on("error", reject);
    child.on("exit", (code, signal) => {
      if (code === 0) resolve({ stdout, stderr });
      else reject(new Error(`${command} ${args.join(" ")} failed (code=${code}, signal=${signal})\n${stdout}\n${stderr}`));
    });
  });
}

function startManagedProcess(name, command, args, options = {}) {
  const child = spawn(command, args, {
    cwd: options.cwd || ROOT,
    env: { ...process.env, ...(options.env || {}) },
    stdio: ["ignore", "pipe", "pipe"],
    detached: process.platform !== "win32",
  });
  const logs = [];
  const append = (stream, chunk) => {
    const text = chunk.toString();
    logs.push(`[${name}:${stream}] ${text}`);
    if (process.env.LIVECOLLAB_VERBOSE === "1") {
      process[stream === "stdout" ? "stdout" : "stderr"].write(`[${name}] ${text}`);
    }
  };
  child.stdout.on("data", (chunk) => append("stdout", chunk));
  child.stderr.on("data", (chunk) => append("stderr", chunk));
  return { name, child, logs };
}

async function stopManagedProcess(managed) {
  if (!managed || managed.child.exitCode !== null) return;
  const child = managed.child;
  try {
    if (process.platform === "win32") child.kill("SIGTERM");
    else process.kill(-child.pid, "SIGTERM");
  } catch {
    return;
  }
  const exited = await Promise.race([
    new Promise((resolve) => child.once("exit", () => resolve(true))),
    delay(2500).then(() => false),
  ]);
  if (!exited) {
    try {
      if (process.platform === "win32") child.kill("SIGKILL");
      else process.kill(-child.pid, "SIGKILL");
    } catch {
      // already stopped
    }
  }
}

async function waitForHttp(url, managed, timeoutMs = 15000) {
  const deadline = Date.now() + timeoutMs;
  let lastError;
  while (Date.now() < deadline) {
    if (managed?.child.exitCode !== null) {
      throw new Error(`${managed.name} exited before ready.\n${managed.logs.join("")}`);
    }
    try {
      const response = await fetch(url);
      if (response.ok) return;
      lastError = new Error(`HTTP ${response.status}`);
    } catch (error) {
      lastError = error;
    }
    await delay(100);
  }
  throw new Error(`Timed out waiting for ${url}: ${lastError?.message || "unknown"}\n${managed?.logs.join("") || ""}`);
}

async function buildBackend() {
  const binary = path.join(
    os.tmpdir(),
    `livecollab-phase2-${process.pid}${process.platform === "win32" ? ".exe" : ""}`
  );
  await runCommand("go", ["build", "-mod=vendor", "-o", binary, "."], { cwd: BACKEND_DIR });
  return binary;
}

function startBackend(binary, name, port, traceFile) {
  return startManagedProcess(name, binary, [], {
    cwd: BACKEND_DIR,
    env: {
      PORT: String(port),
      ALLOWED_ORIGINS: "null",
      LIVECOLLAB_STALE_WRITE_POLICY: "reject",
      LIVECOLLAB_REDIS_ADDR: REDIS_ADDR,
      LIVECOLLAB_INSTANCE_ID: name,
      LIVECOLLAB_TRACE_FILE: traceFile,
    },
  });
}

class ScenarioClient {
  constructor(actor, clientId, scenarioStart) {
    this.actor = actor;
    this.clientId = clientId;
    this.scenarioStart = scenarioStart;
    this.socket = null;
    this.records = [];
    this.emitter = new EventEmitter();
  }

  record(stage, data = {}) {
    const entry = {
      source: "client",
      actor: this.actor,
      stage,
      timestamp: new Date().toISOString(),
      elapsedMs: Date.now() - this.scenarioStart,
      type: data.type || null,
      operationId: data.operationId || null,
      sequence: typeof data.sequence === "number" ? data.sequence : null,
      serverVersion: typeof data.serverVersion === "number" ? data.serverVersion : null,
      accepted: typeof data.accepted === "boolean" ? data.accepted : null,
      content: typeof data.content === "string" ? data.content : null,
      reason: data.reason || null,
      replayed: typeof data.replayed === "number" ? data.replayed : null,
      fromSequence: typeof data.fromSequence === "number" ? data.fromSequence : null,
      latestSequence: typeof data.latestSequence === "number" ? data.latestSequence : null,
    };
    this.records.push(entry);
    this.emitter.emit("record", entry);
    return entry;
  }

  async connect(baseUrl, roomId, username, lastSequence) {
    const query = new URLSearchParams({
      roomId,
      username,
      clientId: this.clientId,
      lastSequence: String(lastSequence),
    });
    const url = `${baseUrl}/ws?${query}`;
    this.record("connecting", { sequence: lastSequence });

    const socket = new WebSocket(url);
    this.socket = socket;
    socket.addEventListener("message", (event) => {
      const raw = typeof event.data === "string" ? event.data : String(event.data);
      let data;
      try {
        data = JSON.parse(raw);
      } catch {
        data = { type: "invalid_json", content: raw };
      }
      this.record("received", data);
    });
    socket.addEventListener("close", () => this.record("closed"));
    socket.addEventListener("error", () => this.record("socket_error"));

    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`${this.actor} WebSocket open timeout: ${url}`)), 8000);
      socket.addEventListener("open", () => {
        clearTimeout(timer);
        this.record("opened");
        resolve();
      }, { once: true });
      socket.addEventListener("error", () => {
        clearTimeout(timer);
        reject(new Error(`${this.actor} WebSocket failed: ${url}`));
      }, { once: true });
    });
  }

  send(message) {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      throw new Error(`${this.actor} socket is not open`);
    }
    this.record("sent", message);
    this.socket.send(JSON.stringify(message));
  }

  waitFor(predicate, timeoutMs = 8000, description = "matching message") {
    const existing = this.records.find((entry) => predicate(entry));
    if (existing) return Promise.resolve(existing);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.emitter.off("record", listener);
        reject(new Error(`${this.actor} timed out waiting for ${description}`));
      }, timeoutMs);
      const listener = (entry) => {
        if (!predicate(entry)) return;
        clearTimeout(timer);
        this.emitter.off("record", listener);
        resolve(entry);
      };
      this.emitter.on("record", listener);
    });
  }

  async close() {
    if (!this.socket || this.socket.readyState === WebSocket.CLOSED) return;
    const socket = this.socket;
    socket.close(1000, "scenario_disconnect");
    await Promise.race([
      this.waitFor((entry) => entry.stage === "closed", 2000, "socket close"),
      delay(2100),
    ]);
  }
}

async function createRoom(baseHttpUrl) {
  const response = await fetch(`${baseHttpUrl}/rooms`, { method: "POST" });
  if (!response.ok) throw new Error(`create room failed: HTTP ${response.status}`);
  const data = await response.json();
  return data.roomId;
}

async function roomState(baseHttpUrl, roomId) {
  const response = await fetch(`${baseHttpUrl}/rooms/check?roomId=${encodeURIComponent(roomId)}`);
  if (!response.ok) throw new Error(`room check failed: HTTP ${response.status}`);
  return response.json();
}

function strictlyIncreasing(values) {
  return values.every((value, index) => index === 0 || value > values[index - 1]);
}

function contiguous(values) {
  return values.every((value, index) => index === 0 || value === values[index - 1] + 1);
}

function readJsonLines(file) {
  if (!fs.existsSync(file)) return [];
  return fs
    .readFileSync(file, "utf8")
    .split(/\r?\n/)
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function renderText(report) {
  const lines = [];
  lines.push("LiveCollab Phase 2 Correctness Report", "");
  lines.push(`Scenario: ${report.scenario}`);
  lines.push(`Result: ${report.result.toUpperCase()}`, "");
  lines.push("Topology:");
  lines.push("  Alice -> backend-a");
  lines.push("  Bob   -> backend-b");
  lines.push("  backend-a/backend-b -> Redis Streams");
  lines.push("  Bob reconnects -> restarted backend-a", "");
  lines.push("Failure/recovery sequence:");
  for (const step of report.summarySteps) lines.push(`  ${step}`);
  lines.push("", "Invariants:");
  for (const invariant of report.invariants) {
    lines.push(`  ${invariant.passed ? "PASS" : "FAIL"}  ${invariant.id}`);
    lines.push(`        ${invariant.details}`);
  }
  lines.push("", "Replay observed by Bob:");
  lines.push(`  expected: ${report.expectedReplay.join(", ")}`);
  lines.push(`  observed: ${report.observedReplay.join(", ")}`);
  lines.push("", "Final state:");
  lines.push(`  Bob content: ${JSON.stringify(report.finalState.bobContent)}`);
  lines.push(`  Expected:    ${JSON.stringify(report.finalState.expectedContent)}`);
  lines.push(`  Server:      version ${report.finalState.serverVersion}, event ${report.finalState.serverSequence}`);
  lines.push("", `Artifacts: ${path.relative(ROOT, ARTIFACT_DIR)}`);
  return `${lines.join("\n")}\n`;
}

function writeArtifacts(report) {
  ensureDir(ARTIFACT_DIR);
  fs.writeFileSync(path.join(ARTIFACT_DIR, "report.json"), JSON.stringify(report, null, 2));
  const text = renderText(report);
  fs.writeFileSync(path.join(ARTIFACT_DIR, "report.txt"), text);
  fs.writeFileSync(
    path.join(ARTIFACT_DIR, "timeline.jsonl"),
    `${report.timeline.map((entry) => JSON.stringify(entry)).join("\n")}\n`
  );
  fs.writeFileSync(
    path.join(ARTIFACT_DIR, "replay.json"),
    JSON.stringify({
      schemaVersion: 1,
      scenario: report.scenario,
      roomId: report.roomId,
      bobResumeFrom: report.bobResumeFrom,
      expectedReplay: report.expectedReplay,
      finalContent: report.finalState.expectedContent,
      operations: report.operations,
    }, null, 2)
  );

  const invariantRows = report.invariants
    .map((item) => `<tr><td class="${item.passed ? "pass" : "fail"}">${item.passed ? "PASS" : "FAIL"}</td><td><strong>${escapeHtml(item.id)}</strong><br>${escapeHtml(item.details)}</td></tr>`)
    .join("");
  const timelineRows = report.timeline
    .map((event) => `<tr><td>${escapeHtml(event.timestamp || "")}</td><td>${escapeHtml(event.actor || event.instanceId || event.source || "")}</td><td>${escapeHtml(event.stage || "")}</td><td>${escapeHtml(event.operationId || "")}</td><td>${escapeHtml(event.sequence ?? event.eventSequence ?? "")}</td><td>${escapeHtml(event.content || "")}</td></tr>`)
    .join("");

  fs.writeFileSync(path.join(ARTIFACT_DIR, "report.html"), `<!doctype html>
<html><head><meta charset="utf-8"><title>LiveCollab Phase 2 Report</title>
<style>body{font-family:system-ui,sans-serif;max-width:1100px;margin:40px auto;padding:0 20px;color:#1f2937}h1{margin-bottom:4px}.pass{color:#087443;font-weight:700}.fail{color:#b42318;font-weight:700}.card{border:1px solid #ddd;border-radius:12px;padding:18px;margin:18px 0}table{width:100%;border-collapse:collapse;font-size:14px}th,td{text-align:left;border-bottom:1px solid #eee;padding:8px;vertical-align:top}code{background:#f3f4f6;padding:2px 5px;border-radius:4px}.status{font-size:24px;font-weight:800}</style></head>
<body><h1>LiveCollab Phase 2</h1><div class="status ${report.result === "passed" ? "pass" : "fail"}">${escapeHtml(report.result.toUpperCase())}</div><p>${escapeHtml(report.scenario)}</p>
<div class="card"><h2>What happened</h2><ol>${report.summarySteps.map((step) => `<li>${escapeHtml(step)}</li>`).join("")}</ol></div>
<div class="card"><h2>Correctness invariants</h2><table>${invariantRows}</table></div>
<div class="card"><h2>Reconnect replay</h2><p>Bob resumed after event <code>${report.bobResumeFrom}</code>.</p><p>Expected: <code>${escapeHtml(report.expectedReplay.join(", "))}</code><br>Observed: <code>${escapeHtml(report.observedReplay.join(", "))}</code></p><p>Final content: <code>${escapeHtml(report.finalState.bobContent)}</code></p></div>
<div class="card"><h2>Timeline</h2><table><thead><tr><th>Time</th><th>Actor</th><th>Stage</th><th>Operation</th><th>Event</th><th>Content</th></tr></thead><tbody>${timelineRows}</tbody></table></div>
</body></html>`);
  return text;
}

async function runScenario() {
  fs.rmSync(ARTIFACT_DIR, { recursive: true, force: true });
  ensureDir(ARTIFACT_DIR);

  const traceA1 = path.join(ARTIFACT_DIR, "backend-a-before-restart.jsonl");
  const traceA2 = path.join(ARTIFACT_DIR, "backend-a-after-restart.jsonl");
  const traceB = path.join(ARTIFACT_DIR, "backend-b.jsonl");
  const binary = await buildBackend();
  const managed = [];
  let redis;
  let backendA;
  let backendB;
  const scenarioStart = Date.now();
  const alice = new ScenarioClient("Alice", "alice-phase2", scenarioStart);
  const bob = new ScenarioClient("Bob", "bob-phase2", scenarioStart);

  try {
    if (!EXTERNAL_REDIS_ADDR) {
      redis = startManagedProcess(
        "test-redis",
        process.execPath,
        [path.join(ROOT, "correctness-tests", "test-redis-server.js")],
        { env: { LIVECOLLAB_TEST_REDIS_PORT: String(TEST_REDIS_PORT) } }
      );
      managed.push(redis);
      await new Promise((resolve, reject) => {
        const deadline = Date.now() + 5000;
        const check = () => {
          if (redis.logs.join("").includes("READY")) return resolve();
          if (redis.child.exitCode !== null) return reject(new Error(redis.logs.join("")));
          if (Date.now() > deadline) return reject(new Error("test Redis startup timeout"));
          setTimeout(check, 25);
        };
        check();
      });
    }

    backendA = startBackend(binary, "backend-a", BACKEND_A_PORT, traceA1);
    backendB = startBackend(binary, "backend-b", BACKEND_B_PORT, traceB);
    managed.push(backendA, backendB);
    await Promise.all([
      waitForHttp(`http://127.0.0.1:${BACKEND_A_PORT}/health/ready`, backendA),
      waitForHttp(`http://127.0.0.1:${BACKEND_B_PORT}/health/ready`, backendB),
    ]);

    const roomId = await createRoom(`http://127.0.0.1:${BACKEND_A_PORT}`);
    await alice.connect(`ws://127.0.0.1:${BACKEND_A_PORT}`, roomId, "Alice", 0);
    await bob.connect(`ws://127.0.0.1:${BACKEND_B_PORT}`, roomId, "Bob", 0);
    await Promise.all([
      alice.waitFor((e) => e.type === "room_state", 5000, "initial room state"),
      bob.waitFor((e) => e.type === "room_state", 5000, "initial room state"),
    ]);

    // Seed event proves live cross-node fanout through Redis Streams.
    alice.send({ type: "content_update", operationId: "op-1", clientId: alice.clientId, baseVersion: 0, content: "Start" });
    await alice.waitFor((e) => e.type === "operation_result" && e.operationId === "op-1" && e.accepted === true, 5000, "op-1 ack");
    await bob.waitFor((e) => e.type === "content_update" && e.sequence === 1, 5000, "cross-node stream event 1");

    const bobResumeFrom = 1;
    await bob.close();

    const operations = [];
    let baseVersion = 1;
    let finalContent = "Start";
    for (let sequence = 2; sequence <= 5; sequence += 1) {
      finalContent = `${finalContent} [${sequence}]`;
      const operationId = `op-${sequence}`;
      operations.push({ operationId, baseVersion, expectedSequence: sequence, content: finalContent });
      alice.send({
        type: "content_update",
        operationId,
        clientId: alice.clientId,
        baseVersion,
        content: finalContent,
      });
      const ack = await alice.waitFor(
        (e) => e.type === "operation_result" && e.operationId === operationId && e.accepted === true,
        5000,
        `${operationId} ack`
      );
      if (ack.sequence !== sequence) throw new Error(`${operationId}: expected event ${sequence}, received ${ack.sequence}`);
      baseVersion = ack.serverVersion;
    }

    // Kill the node that accepted Alice's last operation, then restart it with
    // empty process memory. Shared state and replay must come from Redis.
    await stopManagedProcess(backendA);
    managed.splice(managed.indexOf(backendA), 1);
    backendA = startBackend(binary, "backend-a-restarted", BACKEND_A_PORT, traceA2);
    managed.push(backendA);
    await waitForHttp(`http://127.0.0.1:${BACKEND_A_PORT}/health/ready`, backendA);

    const stateAfterRestart = await roomState(`http://127.0.0.1:${BACKEND_A_PORT}`, roomId);
    if (stateAfterRestart.sequence !== 5 || stateAfterRestart.serverVersion !== 5) {
      throw new Error(`restarted backend did not hydrate Redis state: ${JSON.stringify(stateAfterRestart)}`);
    }

    const replayStartIndex = bob.records.length;
    await bob.connect(`ws://127.0.0.1:${BACKEND_A_PORT}`, roomId, "Bob", bobResumeFrom);
    await bob.waitFor((e) => e.type === "resume_complete" && e.latestSequence === 5, 8000, "resume completion");

    const reconnectRecords = bob.records.slice(replayStartIndex);
    const observedReplay = reconnectRecords
      .filter((e) => e.type === "content_update" && typeof e.sequence === "number")
      .map((e) => e.sequence);
    const expectedReplay = [2, 3, 4, 5];
    const replayMessages = reconnectRecords.filter((e) => e.type === "content_update");
    const finalReplayMessage = replayMessages.at(-1);
    const uniqueReplay = new Set(observedReplay);

    const invariants = [
      {
        id: "cross_node_fanout",
        passed: bob.records.some((e) => e.type === "content_update" && e.sequence === 1),
        details: "Bob, connected to backend-b, must observe Alice's event written through backend-a.",
      },
      {
        id: "zero_event_gaps_after_reconnect",
        passed: JSON.stringify(observedReplay) === JSON.stringify(expectedReplay) && contiguous(observedReplay),
        details: `Expected replay ${expectedReplay.join("->")}; observed ${observedReplay.join("->") || "none"}.`,
      },
      {
        id: "no_duplicate_replay",
        passed: uniqueReplay.size === observedReplay.length,
        details: `${observedReplay.length} replay events, ${uniqueReplay.size} unique sequence numbers.`,
      },
      {
        id: "monotonic_event_order",
        passed: strictlyIncreasing(observedReplay),
        details: `Replay order was ${observedReplay.join("->") || "none"}.`,
      },
      {
        id: "eventual_convergence_after_restart",
        passed:
          finalReplayMessage?.content === finalContent &&
          finalReplayMessage?.serverVersion === 5 &&
          stateAfterRestart.sequence === 5,
        details: `Bob ended at event ${finalReplayMessage?.sequence ?? "?"}, version ${finalReplayMessage?.serverVersion ?? "?"}; Redis-backed server is event ${stateAfterRestart.sequence}, version ${stateAfterRestart.serverVersion}.`,
      },
    ];

    const serverEvents = [
      ...readJsonLines(traceA1),
      ...readJsonLines(traceB),
      ...readJsonLines(traceA2),
    ].map((entry) => ({ ...entry, source: "server" }));
    const clientEvents = [...alice.records, ...bob.records];
    const timeline = [...serverEvents, ...clientEvents].sort((a, b) => String(a.timestamp).localeCompare(String(b.timestamp)));

    const report = {
      schemaVersion: 1,
      generatedAt: new Date().toISOString(),
      scenario: "reconnect-replay-across-backend-restart",
      result: invariants.every((item) => item.passed) ? "passed" : "failed",
      roomId,
      redisMode: EXTERNAL_REDIS_ADDR ? "external" : "embedded-test-double",
      topology: {
        backendA: `127.0.0.1:${BACKEND_A_PORT}`,
        backendB: `127.0.0.1:${BACKEND_B_PORT}`,
        redis: REDIS_ADDR,
      },
      bobResumeFrom,
      expectedReplay,
      observedReplay,
      operations: [{ operationId: "op-1", baseVersion: 0, expectedSequence: 1, content: "Start" }, ...operations],
      invariants,
      finalState: {
        bobContent: finalReplayMessage?.content || "",
        expectedContent: finalContent,
        bobVersion: finalReplayMessage?.serverVersion || 0,
        bobSequence: finalReplayMessage?.sequence || 0,
        serverVersion: stateAfterRestart.serverVersion,
        serverSequence: stateAfterRestart.sequence,
      },
      summarySteps: [
        "Alice connected to backend-a while Bob connected to backend-b.",
        "Bob observed event 1 through backend-b, proving cross-node Redis Stream fanout.",
        "Bob disconnected after event 1.",
        "Alice committed and received acknowledgements for events 2 through 5.",
        "backend-a terminated after acknowledging event 5.",
        "backend-a restarted with empty process memory and hydrated room state from Redis.",
        "Bob reconnected to the restarted backend-a with resume cursor 1.",
        `Bob replayed events ${observedReplay.join(", ") || "none"} and converged to event ${finalReplayMessage?.sequence ?? "?"}.`,
      ],
      timeline,
    };

    const text = writeArtifacts(report);
    console.log(text);
    if (report.result !== "passed") process.exitCode = 1;
    return report;
  } finally {
    await Promise.allSettled([alice.close(), bob.close()]);
    for (const processHandle of managed.reverse()) {
      await stopManagedProcess(processHandle);
    }
    fs.rmSync(binary, { force: true });
  }
}

runScenario().catch((error) => {
  console.error(error.stack || error.message || String(error));
  process.exitCode = 1;
});
