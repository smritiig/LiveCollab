#!/usr/bin/env node

const { chromium } = require("playwright");
const { EventEmitter } = require("node:events");
const { spawn } = require("node:child_process");
const crypto = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const ROOT = path.resolve(__dirname, "..");
const BACKEND_DIR = path.join(ROOT, "backend");
const ARTIFACT_ROOT = path.join(ROOT, "artifacts", "phase1");
const BACKEND_PORT = Number(process.env.LIVECOLLAB_BACKEND_PORT || 18080);
const BACKEND_URL = `http://127.0.0.1:${BACKEND_PORT}`;
const WEBSOCKET_URL = `ws://127.0.0.1:${BACKEND_PORT}/ws`;

const SCENARIO = {
  name: "stale-concurrent-edit",
  initialContent: "Start",
  aliceContent: "Start [Alice]",
  bobContent: "Start [Bob]",
  aliceMarker: "[Alice]",
  invariant: {
    id: "no_silent_stale_write_overwrite",
    description:
      "A stale full-document update must not silently overwrite a newer acknowledged update.",
  },
};

function parseArgs(argv) {
  const options = { policy: null, expect: null, replay: null };
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    const [key, inlineValue] = argument.split("=", 2);
    if (key === "--policy") {
      options.policy = inlineValue || argv[++index];
    } else if (key === "--expect") {
      options.expect = inlineValue || argv[++index];
    } else if (key === "--replay") {
      options.replay = inlineValue || argv[++index];
    } else if (key === "--help" || key === "-h") {
      options.help = true;
    } else {
      throw new Error(`Unknown argument: ${argument}`);
    }
  }
  return options;
}

function usage() {
  console.log(`LiveCollab Phase 1 correctness runner

Usage:
  node correctness-tests/run-phase1.js
  node correctness-tests/run-phase1.js --policy accept --expect failed
  node correctness-tests/run-phase1.js --policy reject --expect passed
  node correctness-tests/run-phase1.js --replay artifacts/phase1/unsafe-accept/replay.json
`);
}

function delay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function ensureDirectory(directory) {
  fs.mkdirSync(directory, { recursive: true });
}

function sha256(value) {
  return crypto.createHash("sha256").update(value).digest("hex").slice(0, 12);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function commandToString(command, args) {
  return [command, ...args].join(" ");
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
    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });
    child.on("error", reject);
    child.on("exit", (code, signal) => {
      if (code === 0) {
        resolve({ stdout, stderr });
        return;
      }
      reject(
        new Error(
          `${commandToString(command, args)} failed (code=${code}, signal=${signal})\n${stdout}\n${stderr}`
        )
      );
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
    const line = chunk.toString();
    logs.push(`[${name}:${stream}] ${line}`);
    if (process.env.LIVECOLLAB_VERBOSE === "1") {
      process[stream === "stdout" ? "stdout" : "stderr"].write(
        `[${name}] ${line}`
      );
    }
  };

  child.stdout.on("data", (chunk) => append("stdout", chunk));
  child.stderr.on("data", (chunk) => append("stderr", chunk));
  return { name, child, logs };
}

async function stopManagedProcess(managed) {
  if (!managed || managed.child.exitCode !== null) return;

  const { child } = managed;
  try {
    if (process.platform === "win32") {
      child.kill("SIGTERM");
    } else {
      process.kill(-child.pid, "SIGTERM");
    }
  } catch {
    return;
  }

  const exited = await Promise.race([
    new Promise((resolve) => child.once("exit", () => resolve(true))),
    delay(3000).then(() => false),
  ]);

  if (!exited) {
    try {
      if (process.platform === "win32") {
        child.kill("SIGKILL");
      } else {
        process.kill(-child.pid, "SIGKILL");
      }
    } catch {
      // Process already exited.
    }
  }
}

async function waitForHttp(url, managed, timeoutMs = 20000) {
  const deadline = Date.now() + timeoutMs;
  let lastError = null;

  while (Date.now() < deadline) {
    if (managed?.child.exitCode !== null) {
      throw new Error(
        `${managed.name} exited before becoming ready.\n${managed.logs.join("")}`
      );
    }

    try {
      const response = await fetch(url);
      if (response.ok) return;
      lastError = new Error(`HTTP ${response.status}`);
    } catch (error) {
      lastError = error;
    }
    await delay(120);
  }

  throw new Error(
    `Timed out waiting for ${url}: ${lastError?.message || "unknown error"}\n${
      managed?.logs.join("") || ""
    }`
  );
}

async function waitForCondition(description, predicate, timeoutMs = 8000) {
  const deadline = Date.now() + timeoutMs;
  let lastValue;
  let lastError;

  while (Date.now() < deadline) {
    try {
      lastValue = await predicate();
      if (lastValue) return lastValue;
    } catch (error) {
      lastError = error;
    }
    await delay(30);
  }

  throw new Error(
    `Timed out waiting for ${description}. Last value: ${JSON.stringify(
      lastValue
    )}${lastError ? `; last error: ${lastError.message}` : ""}`
  );
}

async function buildBackend() {
  const binary = path.join(
    os.tmpdir(),
    `livecollab-phase1-${process.pid}${process.platform === "win32" ? ".exe" : ""}`
  );
  await runCommand("go", ["build", "-mod=vendor", "-o", binary, "."], { cwd: BACKEND_DIR });
  return binary;
}

function startBackend(binary, policy, traceFile) {
  return startManagedProcess("backend", binary, [], {
    cwd: BACKEND_DIR,
    env: {
      PORT: String(BACKEND_PORT),
      // Browser pages created with setContent have an opaque `null` origin.
      ALLOWED_ORIGINS: "null",
      LIVECOLLAB_STALE_WRITE_POLICY: policy,
      LIVECOLLAB_TRACE_FILE: traceFile,
    },
  });
}

function parseJsonMessage(message) {
  const raw = Buffer.isBuffer(message) ? message.toString("utf8") : String(message);
  try {
    return { raw, data: JSON.parse(raw) };
  } catch {
    return { raw, data: null };
  }
}

function createWebSocketController(context, actor, scenarioStartMs) {
  const records = [];
  const emitter = new EventEmitter();
  let armedHold = null;
  let heldMessage = null;
  let connectedRoute = null;

  function record(stage, data = {}, raw = null) {
    const entry = {
      source: "proxy",
      actor,
      stage,
      timestamp: new Date().toISOString(),
      elapsedMs: Date.now() - scenarioStartMs,
      messageType: data?.type || null,
      operationId: data?.operationId || null,
      clientId: data?.clientId || null,
      baseVersion:
        typeof data?.baseVersion === "number" ? data.baseVersion : null,
      previousVersion:
        typeof data?.previousVersion === "number" ? data.previousVersion : null,
      serverVersion:
        typeof data?.serverVersion === "number" ? data.serverVersion : null,
      accepted:
        typeof data?.accepted === "boolean" ? data.accepted : null,
      stale: data?.stale === true,
      reason: data?.reason || null,
      content: typeof data?.content === "string" ? data.content : null,
      raw,
    };
    records.push(entry);
    emitter.emit("record", entry);
    return entry;
  }

  const routeInstalled = context.routeWebSocket("**/*", (route) => {
    const server = route.connectToServer();
    connectedRoute = { route, server };
    record("proxy_connected");

    route.onMessage((message) => {
      const { raw, data } = parseJsonMessage(message);
      const sentRecord = record("client_sent", data, raw);

      if (armedHold && data && armedHold.predicate(data)) {
        heldMessage = { raw, data, server };
        record("proxy_held", data, raw);
        const resolver = armedHold.resolve;
        armedHold = null;
        resolver(sentRecord);
        return;
      }

      server.send(message);
    });

    server.onMessage((message) => {
      const { raw, data } = parseJsonMessage(message);
      record("client_received", data, raw);
      route.send(message);
    });
  });

  function waitForRecord(predicate, timeoutMs = 8000) {
    const existing = records.find(predicate);
    if (existing) return Promise.resolve(existing);

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        emitter.off("record", listener);
        reject(new Error(`Timed out waiting for ${actor} WebSocket record`));
      }, timeoutMs);

      const listener = (entry) => {
        if (!predicate(entry)) return;
        clearTimeout(timer);
        emitter.off("record", listener);
        resolve(entry);
      };
      emitter.on("record", listener);
    });
  }

  function holdNext(predicate) {
    if (armedHold || heldMessage) {
      throw new Error(`${actor} already has an armed or held WebSocket message`);
    }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        if (armedHold) armedHold = null;
        reject(new Error(`Timed out waiting to hold ${actor}'s WebSocket message`));
      }, 8000);
      armedHold = {
        predicate,
        resolve: (entry) => {
          clearTimeout(timer);
          resolve(entry);
        },
      };
    });
  }

  function releaseHeld() {
    if (!heldMessage) {
      throw new Error(`${actor} has no held WebSocket message`);
    }
    const current = heldMessage;
    heldMessage = null;
    record("proxy_released", current.data, current.raw);
    current.server.send(current.raw);
    return current.data;
  }

  return {
    actor,
    records,
    waitForRecord,
    holdNext,
    releaseHeld,
    isConnected: () => connectedRoute !== null,
    ready: routeInstalled,
  };
}

const CLIENT_HARNESS_HTML = `<!doctype html>
<html>
<head><meta charset="utf-8"><title>LiveCollab correctness client</title></head>
<body>
  <div id="status">disconnected</div>
  <div id="version">0</div>
  <textarea id="editor"></textarea>
  <script>
    (() => {
      const state = {
        content: "",
        version: 0,
        connected: false,
        lastOperationResult: null,
        messages: [],
      };
      let socket = null;
      let username = "";
      let clientId = "";

      function render() {
        document.querySelector("#status").textContent = state.connected ? "connected" : "disconnected";
        document.querySelector("#version").textContent = String(state.version);
        document.querySelector("#editor").value = state.content;
      }

      window.livecollab = {
        connect({ url, roomId, name }) {
          username = name;
          clientId = name.toLowerCase() + "-phase1-client";
          socket = new WebSocket(url + "?roomId=" + encodeURIComponent(roomId) + "&username=" + encodeURIComponent(name));
          socket.onopen = () => {
            state.connected = true;
            render();
          };
          socket.onclose = () => {
            state.connected = false;
            render();
          };
          socket.onmessage = (event) => {
            const message = JSON.parse(event.data);
            state.messages.push(message);
            if (message.type === "room_state" || message.type === "content_update") {
              state.content = message.content || "";
              if (typeof message.serverVersion === "number") state.version = message.serverVersion;
            }
            if (message.type === "operation_result") {
              state.lastOperationResult = message;
              if (typeof message.serverVersion === "number") state.version = message.serverVersion;
            }
            render();
          };
        },
        sendContent(content, operationId) {
          if (!socket || socket.readyState !== WebSocket.OPEN) throw new Error("socket not open");
          const baseVersion = state.version;
          state.content = content;
          render();
          socket.send(JSON.stringify({
            type: "content_update",
            operationId,
            clientId,
            baseVersion,
            content,
          }));
          return { operationId, baseVersion, content };
        },
        snapshot() {
          return JSON.parse(JSON.stringify(state));
        },
      };
      render();
    })();
  </script>
</body>
</html>`;

async function initializeClientPage(page, roomId, username) {
  await page.setContent(CLIENT_HARNESS_HTML);
  await page.evaluate(
    ({ url, roomId: id, name }) => window.livecollab.connect({ url, roomId: id, name }),
    { url: WEBSOCKET_URL, roomId, name: username }
  );
}

async function sendClientUpdate(page, content, operationId) {
  return page.evaluate(
    ({ value, id }) => window.livecollab.sendContent(value, id),
    { value: content, id: operationId }
  );
}

async function clientSnapshot(page, name) {
  const state = await page.evaluate(() => window.livecollab.snapshot());
  return {
    name,
    content: state.content,
    version: state.version,
    stateHash: sha256(state.content),
    operationStatus: state.lastOperationResult,
  };
}

function readTraceEvents(traceFile, scenarioStartMs) {
  if (!fs.existsSync(traceFile)) return [];
  const text = fs.readFileSync(traceFile, "utf8").trim();
  if (!text) return [];

  return text
    .split(/\r?\n/)
    .filter(Boolean)
    .map((line) => JSON.parse(line))
    .map((event) => ({
      source: "server",
      actor: event.clientId || "server",
      ...event,
      elapsedMs: Math.max(0, Date.parse(event.timestamp) - scenarioStartMs),
      messageType: null,
    }));
}

function relevantTimeline(proxyRecords, traceEvents, operationIds) {
  const ids = new Set(operationIds);
  const proxy = proxyRecords.filter((event) => {
    if (event.stage === "proxy_held" || event.stage === "proxy_released") {
      return ids.has(event.operationId);
    }
    if (!ids.has(event.operationId)) return false;
    return (
      event.messageType === "content_update" ||
      event.messageType === "operation_result"
    );
  });
  const server = traceEvents.filter((event) => ids.has(event.operationId));

  return [...proxy, ...server]
    .sort((left, right) => {
      if (left.elapsedMs !== right.elapsedMs) return left.elapsedMs - right.elapsedMs;
      if (left.source === right.source) {
        return (left.sequence || 0) - (right.sequence || 0);
      }
      return left.source === "proxy" ? -1 : 1;
    })
    .map((event, index) => ({ ...event, displaySequence: index + 1 }));
}

function describeTimelineEvent(event) {
  const operation = event.operationId || "operation";
  if (event.stage === "client_sent") {
    return `${event.actor} sent ${operation} from version ${event.baseVersion}`;
  }
  if (event.stage === "proxy_held") {
    return `Fault proxy held ${event.actor}'s ${operation}`;
  }
  if (event.stage === "proxy_released") {
    return `Fault proxy released ${event.actor}'s delayed ${operation}`;
  }
  if (event.stage === "server_received") {
    return `Server received ${operation} (base ${event.baseVersion}, current ${event.serverVersion})`;
  }
  if (event.stage === "server_applied") {
    const stale = event.stale ? " — STALE WRITE ACCEPTED" : "";
    return `Server applied ${operation} as version ${event.serverVersion}${stale}`;
  }
  if (event.stage === "server_rejected") {
    return `Server rejected ${operation}: ${event.reason}`;
  }
  if (event.stage === "server_acknowledged") {
    return `Server returned ${event.accepted ? "ACK" : "CONFLICT"} for ${operation}`;
  }
  if (event.stage === "client_received" && event.messageType === "operation_result") {
    return `${event.actor} received ${event.accepted ? "ACK" : "CONFLICT"} for ${operation}`;
  }
  if (event.stage === "client_received" && event.messageType === "content_update") {
    return `${event.actor} observed ${operation} at version ${event.serverVersion}`;
  }
  return `${event.stage}: ${operation}`;
}

function evaluateInvariant({ traceEvents, aliceOperationId, bobOperationId, finalState }) {
  const aliceAcknowledged = traceEvents.some(
    (event) =>
      event.operationId === aliceOperationId &&
      event.stage === "server_acknowledged" &&
      event.accepted === true
  );
  const bobDecision = traceEvents.find(
    (event) =>
      event.operationId === bobOperationId &&
      (event.stage === "server_applied" || event.stage === "server_rejected")
  );
  const staleWriteAccepted = Boolean(
    bobDecision?.stage === "server_applied" &&
      bobDecision?.accepted === true &&
      bobDecision?.stale === true
  );
  const aliceContributionPreserved = finalState.every((state) =>
    state.content.includes(SCENARIO.aliceMarker)
  );
  const clientsConverged =
    new Set(finalState.map((state) => `${state.version}:${state.stateHash}`)).size === 1;
  const passed = !(
    aliceAcknowledged &&
    staleWriteAccepted &&
    !aliceContributionPreserved
  );

  return {
    id: SCENARIO.invariant.id,
    description: SCENARIO.invariant.description,
    passed,
    aliceAcknowledged,
    staleWriteAccepted,
    aliceContributionPreserved,
    clientsConverged,
    evidence: {
      aliceOperationId,
      bobOperationId,
      bobBaseVersion: bobDecision?.baseVersion ?? null,
      serverVersionBeforeBob: bobDecision?.previousVersion ?? null,
      bobAccepted: bobDecision?.accepted ?? null,
      bobReason: bobDecision?.reason || null,
    },
  };
}

function buildTextReport(report) {
  const lines = [];
  lines.push("LiveCollab Correctness Test");
  lines.push("=".repeat(58));
  lines.push(`Scenario: ${report.scenario}`);
  lines.push(`Stale-write policy: ${report.policy}`);
  lines.push(`Result: ${report.result.toUpperCase()}`);
  lines.push("");
  lines.push("Invariant");
  lines.push("-".repeat(58));
  lines.push(report.invariant.description);
  lines.push("");
  lines.push("Controlled input");
  lines.push("-".repeat(58));
  lines.push(`Initial document: ${JSON.stringify(report.input.initialContent)}`);
  lines.push(`Alice update:    ${JSON.stringify(report.input.aliceContent)}`);
  lines.push(`Bob update:      ${JSON.stringify(report.input.bobContent)}`);
  lines.push("Bob's frame is held until Alice's update is acknowledged.");
  lines.push("");
  lines.push("Timeline");
  lines.push("-".repeat(58));
  for (const event of report.timeline) {
    lines.push(`${String(event.elapsedMs).padStart(5, " ")} ms  ${describeTimelineEvent(event)}`);
  }
  lines.push("");
  lines.push("Final client state");
  lines.push("-".repeat(58));
  for (const state of report.finalState) {
    lines.push(
      `${state.name.padEnd(5)} v${String(state.version).padEnd(3)} ${state.stateHash}  ${JSON.stringify(
        state.content
      )}`
    );
  }
  lines.push("");

  if (report.result === "failed") {
    lines.push("INVARIANT VIOLATION");
    lines.push("-".repeat(58));
    lines.push(
      `${report.invariant.evidence.bobOperationId} was authored from version ${report.invariant.evidence.bobBaseVersion}, ` +
        `but the server was already at version ${report.invariant.evidence.serverVersionBeforeBob}.`
    );
    lines.push("The stale write was accepted and Alice's acknowledged contribution disappeared.");
    lines.push("");
    lines.push("Suggested fix");
    lines.push("-".repeat(58));
    lines.push("Reject updates whose baseVersion does not equal the current room version,");
    lines.push("then return the authoritative room snapshot so the client can resynchronize.");
  } else {
    lines.push("INVARIANT PRESERVED");
    lines.push("-".repeat(58));
    lines.push("The server detected Bob's stale base version and rejected the update.");
    lines.push("Alice's acknowledged contribution remained in every final client state.");
  }

  lines.push("");
  lines.push(`Replay: node correctness-tests/run-phase1.js --replay ${report.replayPath}`);
  return `${lines.join("\n")}\n`;
}

function buildHtmlReport(report) {
  const timelineItems = report.timeline
    .map(
      (event) => `<li><time>${escapeHtml(event.elapsedMs)} ms</time><span>${escapeHtml(
        describeTimelineEvent(event)
      )}</span></li>`
    )
    .join("\n");

  const stateCards = report.finalState
    .map(
      (state) => `<article class="state-card">
        <h3>${escapeHtml(state.name)}</h3>
        <p>Version ${escapeHtml(state.version)} · hash ${escapeHtml(state.stateHash)}</p>
        <pre>${escapeHtml(state.content)}</pre>
      </article>`
    )
    .join("\n");

  const failed = report.result === "failed";
  const finding = failed
    ? `${report.invariant.evidence.bobOperationId} was based on version ${report.invariant.evidence.bobBaseVersion}, accepted after version ${report.invariant.evidence.serverVersionBeforeBob}, and removed Alice's acknowledged contribution.`
    : `The stale update was rejected with ${report.invariant.evidence.bobReason}, and all clients preserved Alice's acknowledged contribution.`;

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>LiveCollab Phase 1 — ${escapeHtml(report.result)}</title>
  <style>
    :root { font-family: Inter, ui-sans-serif, system-ui, sans-serif; color: #172033; background: #f4f6fa; }
    body { margin: 0; padding: 32px; }
    main { max-width: 980px; margin: auto; }
    header, section { background: white; border: 1px solid #dfe5ef; border-radius: 18px; padding: 24px; margin-bottom: 18px; box-shadow: 0 10px 28px rgba(20, 31, 56, .06); }
    h1, h2, h3, p { margin-top: 0; }
    .result { display: inline-block; padding: 7px 12px; border-radius: 999px; font-weight: 800; background: ${failed ? "#ffe4e6" : "#dcfce7"}; color: ${failed ? "#9f1239" : "#166534"}; }
    .metadata { display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 12px; margin-top: 20px; }
    .metadata div { background: #f7f9fc; border-radius: 12px; padding: 14px; }
    ol { list-style: none; padding: 0; margin: 0; }
    li { display: grid; grid-template-columns: 90px 1fr; gap: 16px; padding: 11px 0; border-bottom: 1px solid #edf0f5; }
    li:last-child { border-bottom: 0; }
    time { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; color: #68738a; }
    .states { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 14px; }
    .state-card { border: 1px solid #dfe5ef; border-radius: 14px; padding: 16px; }
    pre { white-space: pre-wrap; overflow-wrap: anywhere; background: #101827; color: #e5edf9; padding: 14px; border-radius: 10px; }
    code { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
  </style>
</head>
<body>
<main>
  <header>
    <span class="result">${escapeHtml(report.result.toUpperCase())}</span>
    <h1>LiveCollab correctness report</h1>
    <p>${escapeHtml(report.invariant.description)}</p>
    <div class="metadata">
      <div><strong>Scenario</strong><br />${escapeHtml(report.scenario)}</div>
      <div><strong>Policy</strong><br />${escapeHtml(report.policy)}</div>
      <div><strong>Clients converged</strong><br />${escapeHtml(report.invariant.clientsConverged)}</div>
      <div><strong>Alice preserved</strong><br />${escapeHtml(report.invariant.aliceContributionPreserved)}</div>
    </div>
  </header>
  <section><h2>${failed ? "Invariant violation" : "Invariant preserved"}</h2><p>${escapeHtml(
    finding
  )}</p></section>
  <section><h2>Execution timeline</h2><ol>${timelineItems}</ol></section>
  <section><h2>Final client state</h2><div class="states">${stateCards}</div></section>
  <section><h2>Replay</h2><pre><code>node correctness-tests/run-phase1.js --replay ${escapeHtml(
    report.replayPath
  )}</code></pre></section>
</main>
</body>
</html>`;
}

function writeArtifacts(report, outputDirectory) {
  ensureDirectory(outputDirectory);
  const replay = {
    schemaVersion: 1,
    scenario: report.scenario,
    policy: report.policy,
    expectedResult: report.result,
    input: report.input,
    orderedSteps: [
      "connect Alice and Bob",
      "write initial document",
      "hold Bob's content_update frame",
      "send and acknowledge Alice's update",
      "release Bob's stale frame",
      "evaluate the invariant",
    ],
  };

  const replayFile = path.join(outputDirectory, "replay.json");
  fs.writeFileSync(replayFile, `${JSON.stringify(replay, null, 2)}\n`);
  report.replayPath = path.relative(ROOT, replayFile).replaceAll(path.sep, "/");

  const text = buildTextReport(report);
  fs.writeFileSync(path.join(outputDirectory, "report.txt"), text);
  fs.writeFileSync(
    path.join(outputDirectory, "report.json"),
    `${JSON.stringify(report, null, 2)}\n`
  );
  fs.writeFileSync(path.join(outputDirectory, "report.html"), buildHtmlReport(report));
  return text;
}

async function createRoom() {
  const response = await fetch(`${BACKEND_URL}/rooms`, { method: "POST" });
  if (!response.ok) {
    throw new Error(`Create room failed: HTTP ${response.status}`);
  }
  const payload = await response.json();
  return payload.roomId;
}

async function runScenario({ browser, binary, policy, outputDirectory, input = SCENARIO }) {
  ensureDirectory(outputDirectory);
  const traceFile = path.join(outputDirectory, "server-events.jsonl");
  fs.rmSync(traceFile, { force: true });

  const backend = startBackend(binary, policy, traceFile);
  let aliceContext;
  let bobContext;

  try {
    await waitForHttp(`${BACKEND_URL}/health/ready`, backend);
    const roomId = await createRoom();
    const scenarioStartMs = Date.now();

    aliceContext = await browser.newContext();
    bobContext = await browser.newContext();
    const alice = createWebSocketController(aliceContext, "Alice", scenarioStartMs);
    const bob = createWebSocketController(bobContext, "Bob", scenarioStartMs);
    await Promise.all([alice.ready, bob.ready]);
    const alicePage = await aliceContext.newPage();
    const bobPage = await bobContext.newPage();

    await Promise.all([
      initializeClientPage(alicePage, roomId, "Alice"),
      initializeClientPage(bobPage, roomId, "Bob"),
    ]);
    await waitForCondition("both WebSocket routes to connect", () =>
      alice.isConnected() && bob.isConnected()
    );
    await waitForCondition("both browser clients to connect", async () => {
      const [aliceState, bobState] = await Promise.all([
        alicePage.evaluate(() => window.livecollab.snapshot()),
        bobPage.evaluate(() => window.livecollab.snapshot()),
      ]);
      return aliceState.connected && bobState.connected;
    });

    const initialOperationId = "alice-initial-001";
    const initialAckPromise = alice.waitForRecord(
      (event) =>
        event.stage === "client_received" &&
        event.messageType === "operation_result" &&
        event.operationId === initialOperationId &&
        event.accepted === true
    );
    await sendClientUpdate(alicePage, input.initialContent, initialOperationId);
    await initialAckPromise;
    await waitForCondition("initial document to synchronize", async () => {
      const [aliceState, bobState] = await Promise.all([
        alicePage.evaluate(() => window.livecollab.snapshot()),
        bobPage.evaluate(() => window.livecollab.snapshot()),
      ]);
      return (
        aliceState.content === input.initialContent &&
        bobState.content === input.initialContent &&
        aliceState.version === 1 &&
        bobState.version === 1
      );
    });

    const bobOperationId = "bob-op-001";
    const heldBobPromise = bob.holdNext(
      (message) =>
        message.type === "content_update" && message.operationId === bobOperationId
    );
    await sendClientUpdate(bobPage, input.bobContent, bobOperationId);
    const bobHeldRecord = await heldBobPromise;

    const aliceOperationId = "alice-op-001";
    const aliceAckPromise = alice.waitForRecord(
      (event) =>
        event.stage === "client_received" &&
        event.messageType === "operation_result" &&
        event.operationId === aliceOperationId &&
        event.accepted === true
    );
    await sendClientUpdate(alicePage, input.aliceContent, aliceOperationId);
    await aliceAckPromise;

    await waitForCondition("Bob to observe Alice's acknowledged update", async () => {
      const state = await bobPage.evaluate(() => window.livecollab.snapshot());
      return state.content === input.aliceContent && state.version === 2;
    });

    const releasedBob = bob.releaseHeld();
    const expectedAccepted = policy === "accept";
    await bob.waitForRecord(
      (event) =>
        event.stage === "client_received" &&
        event.messageType === "operation_result" &&
        event.operationId === releasedBob.operationId &&
        event.accepted === expectedAccepted
    );

    const expectedFinalContent =
      policy === "accept" ? input.bobContent : input.aliceContent;
    const expectedFinalVersion = policy === "accept" ? 3 : 2;
    await waitForCondition("final browser states to settle", async () => {
      const [aliceState, bobState] = await Promise.all([
        alicePage.evaluate(() => window.livecollab.snapshot()),
        bobPage.evaluate(() => window.livecollab.snapshot()),
      ]);
      return (
        aliceState.content === expectedFinalContent &&
        bobState.content === expectedFinalContent &&
        aliceState.version === expectedFinalVersion &&
        bobState.version === expectedFinalVersion
      );
    });

    await delay(100);
    const finalState = await Promise.all([
      clientSnapshot(alicePage, "Alice"),
      clientSnapshot(bobPage, "Bob"),
    ]);
    const traceEvents = readTraceEvents(traceFile, scenarioStartMs);
    const invariant = evaluateInvariant({
      traceEvents,
      aliceOperationId,
      bobOperationId,
      finalState,
    });
    const timeline = relevantTimeline(
      [...alice.records, ...bob.records],
      traceEvents,
      [aliceOperationId, bobOperationId]
    );

    const report = {
      schemaVersion: 1,
      generatedAt: new Date().toISOString(),
      scenario: input.name || SCENARIO.name,
      roomId,
      policy,
      result: invariant.passed ? "passed" : "failed",
      input: {
        initialContent: input.initialContent,
        aliceContent: input.aliceContent,
        bobContent: input.bobContent,
      },
      invariant,
      operations: {
        initialOperationId,
        aliceOperationId,
        bobOperationId: bobHeldRecord.operationId,
      },
      timeline,
      finalState,
      replayPath: "",
    };

    const text = writeArtifacts(report, outputDirectory);
    console.log(text);
    return report;
  } finally {
    await Promise.allSettled([aliceContext?.close(), bobContext?.close()]);
    await stopManagedProcess(backend);
  }
}

function findChromiumExecutable() {
  const candidates = [
    process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE,
    "/usr/bin/chromium",
    "/usr/bin/chromium-browser",
    "/usr/bin/google-chrome",
    "/usr/bin/google-chrome-stable",
  ].filter(Boolean);
  return candidates.find((candidate) => fs.existsSync(candidate));
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    usage();
    return;
  }

  let runs;
  if (options.replay) {
    const replayPath = path.isAbsolute(options.replay)
      ? options.replay
      : path.join(ROOT, options.replay);
    const replay = JSON.parse(fs.readFileSync(replayPath, "utf8"));
    runs = [
      {
        policy: replay.policy,
        expected: replay.expectedResult,
        input: { ...SCENARIO, ...replay.input },
        outputDirectory: path.join(ARTIFACT_ROOT, "replay"),
      },
    ];
  } else if (options.policy) {
    if (!["accept", "reject"].includes(options.policy)) {
      throw new Error("--policy must be accept or reject");
    }
    runs = [
      {
        policy: options.policy,
        expected: options.expect || null,
        input: SCENARIO,
        outputDirectory: path.join(
          ARTIFACT_ROOT,
          options.policy === "accept" ? "unsafe-accept" : "safe-reject"
        ),
      },
    ];
  } else {
    fs.rmSync(ARTIFACT_ROOT, { recursive: true, force: true });
    runs = [
      {
        policy: "accept",
        expected: "failed",
        input: SCENARIO,
        outputDirectory: path.join(ARTIFACT_ROOT, "unsafe-accept"),
      },
      {
        policy: "reject",
        expected: "passed",
        input: SCENARIO,
        outputDirectory: path.join(ARTIFACT_ROOT, "safe-reject"),
      },
    ];
  }

  ensureDirectory(ARTIFACT_ROOT);
  const binary = await buildBackend();
  let browser;

  try {
    const executablePath = findChromiumExecutable();
    browser = await chromium.launch({
      headless: true,
      ...(executablePath ? { executablePath } : {}),
      args: ["--no-sandbox", "--disable-dev-shm-usage"],
    });

    const reports = [];
    for (const run of runs) {
      const report = await runScenario({
        browser,
        binary,
        policy: run.policy,
        outputDirectory: run.outputDirectory,
        input: run.input,
      });
      reports.push({ report, expected: run.expected });
    }

    const mismatches = reports.filter(
      ({ report, expected }) => expected && report.result !== expected
    );
    if (mismatches.length > 0) {
      for (const mismatch of mismatches) {
        console.error(
          `Expected ${mismatch.expected} but observed ${mismatch.report.result} for policy ${mismatch.report.policy}`
        );
      }
      process.exitCode = 1;
      return;
    }

    if (!options.policy && !options.replay) {
      const unsafe = reports.find(({ report }) => report.policy === "accept")?.report;
      const safe = reports.find(({ report }) => report.policy === "reject")?.report;
      if (unsafe?.result !== "failed" || safe?.result !== "passed") {
        throw new Error(
          "Phase 1 thesis was not proven: unsafe mode must fail and safe mode must pass"
        );
      }
      console.log("Phase 1 validation PASSED");
      console.log("- Unsafe last-write-wins mode: invariant violation detected");
      console.log("- Safe base-version mode: the same scenario passed");
      console.log(`- Reports: ${path.relative(ROOT, ARTIFACT_ROOT)}`);
    } else if (options.replay) {
      console.log(`Replay reproduced expected result: ${reports[0].report.result}`);
    }
  } finally {
    await browser?.close().catch(() => {});
    fs.rmSync(binary, { force: true });
  }
}

main().catch((error) => {
  console.error(error.stack || error.message || String(error));
  process.exitCode = 1;
});
