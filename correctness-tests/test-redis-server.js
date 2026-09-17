#!/usr/bin/env node

const net = require("node:net");

const port = Number(process.env.LIVECOLLAB_TEST_REDIS_PORT || 16379);
const host = process.env.LIVECOLLAB_TEST_REDIS_HOST || "127.0.0.1";

const strings = new Map();
const streams = new Map();
let lastMs = 0;
let sameMsCounter = 0;

function nextStreamId() {
  const now = Date.now();
  if (now === lastMs) sameMsCounter += 1;
  else {
    lastMs = now;
    sameMsCounter = 0;
  }
  return `${now}-${sameMsCounter}`;
}

function compareStreamIds(a, b) {
  if (a === "-") return -1;
  if (b === "+") return -1;
  if (a === "+") return 1;
  if (b === "-") return 1;
  const [ams, aseq] = String(a).split("-").map(Number);
  const [bms, bseq] = String(b).split("-").map(Number);
  if (ams !== bms) return ams - bms;
  return (aseq || 0) - (bseq || 0);
}

function encode(value) {
  if (value === null || value === undefined) return "$-1\r\n";
  if (Number.isInteger(value)) return `:${value}\r\n`;
  if (Array.isArray(value)) {
    return `*${value.length}\r\n${value.map(encode).join("")}`;
  }
  if (value && value.simple) return `+${value.simple}\r\n`;
  if (value && value.error) return `-${value.error}\r\n`;
  const text = String(value);
  return `$${Buffer.byteLength(text)}\r\n${text}\r\n`;
}

function tryParseCommand(buffer) {
  if (buffer.length === 0 || buffer[0] !== 42) return null; // '*'
  const firstLineEnd = buffer.indexOf("\r\n");
  if (firstLineEnd < 0) return null;
  const count = Number(buffer.subarray(1, firstLineEnd).toString());
  let offset = firstLineEnd + 2;
  const args = [];

  for (let i = 0; i < count; i += 1) {
    if (offset >= buffer.length || buffer[offset] !== 36) return null; // '$'
    const lenEnd = buffer.indexOf("\r\n", offset);
    if (lenEnd < 0) return null;
    const length = Number(buffer.subarray(offset + 1, lenEnd).toString());
    const dataStart = lenEnd + 2;
    const dataEnd = dataStart + length;
    if (buffer.length < dataEnd + 2) return null;
    args.push(buffer.subarray(dataStart, dataEnd).toString());
    offset = dataEnd + 2;
  }
  return { args, consumed: offset };
}

function entriesFor(key) {
  if (!streams.has(key)) streams.set(key, []);
  return streams.get(key);
}

function entryToResp(entry) {
  const fields = [];
  for (const [key, value] of Object.entries(entry.fields)) {
    fields.push(key, value);
  }
  return [entry.id, fields];
}

async function handleCommand(args) {
  if (!args.length) return { error: "ERR empty command" };
  const command = args[0].toUpperCase();

  if (command === "PING") return { simple: "PONG" };

  if (command === "GET") {
    return strings.has(args[1]) ? strings.get(args[1]) : null;
  }

  if (command === "SET") {
    strings.set(args[1], args[2]);
    return { simple: "OK" };
  }

  if (command === "INCR") {
    const next = Number(strings.get(args[1]) || "0") + 1;
    strings.set(args[1], String(next));
    return next;
  }

  if (command === "XADD") {
    const key = args[1];
    const id = args[2] === "*" ? nextStreamId() : args[2];
    const fields = {};
    for (let index = 3; index + 1 < args.length; index += 2) {
      fields[args[index]] = args[index + 1];
    }
    entriesFor(key).push({ id, fields });
    return id;
  }

  if (command === "XRANGE" || command === "XREVRANGE") {
    const key = args[1];
    const start = args[2];
    const end = args[3];
    const countIndex = args.findIndex((value) => value.toUpperCase() === "COUNT");
    const limit = countIndex >= 0 ? Number(args[countIndex + 1]) : Infinity;
    let entries = [...entriesFor(key)];
    if (command === "XREVRANGE") entries.reverse();
    entries = entries.filter((entry) => {
      if (command === "XRANGE") {
        return compareStreamIds(entry.id, start) >= 0 && compareStreamIds(entry.id, end) <= 0;
      }
      return compareStreamIds(entry.id, end) >= 0 && compareStreamIds(entry.id, start) <= 0;
    });
    return entries.slice(0, limit).map(entryToResp);
  }

  if (command === "XREAD") {
    const streamsIndex = args.findIndex((value) => value.toUpperCase() === "STREAMS");
    const blockIndex = args.findIndex((value) => value.toUpperCase() === "BLOCK");
    const countIndex = args.findIndex((value) => value.toUpperCase() === "COUNT");
    const key = args[streamsIndex + 1];
    const lastId = args[streamsIndex + 2];
    const blockMs = blockIndex >= 0 ? Number(args[blockIndex + 1]) : 0;
    const limit = countIndex >= 0 ? Number(args[countIndex + 1]) : 100;
    const deadline = Date.now() + blockMs;

    while (true) {
      const available = entriesFor(key).filter((entry) => compareStreamIds(entry.id, lastId) > 0);
      if (available.length > 0) {
        return [[key, available.slice(0, limit).map(entryToResp)]];
      }
      if (Date.now() >= deadline) return null;
      await new Promise((resolve) => setTimeout(resolve, Math.min(10, blockMs)));
    }
  }

  if (command === "EVAL") {
    const numberOfKeys = Number(args[2]);
    const keys = args.slice(3, 3 + numberOfKeys);
    const argv = args.slice(3 + numberOfKeys);

    // Create-room script: four keys and no ARGV.
    if (numberOfKeys === 4 && argv.length === 0) {
      strings.set(keys[0], "1");
      strings.set(keys[1], "");
      strings.set(keys[2], "0");
      strings.set(keys[3], "0");
      return 1;
    }

    // Phase 2 apply-operation script. The test server intentionally implements
    // the script semantics rather than a general Lua interpreter.
    if (numberOfKeys === 4 && argv.length === 6) {
      const [baseVersionRaw, content, operationId, clientId, username, policy] = argv;
      const currentVersion = Number(strings.get(keys[0]) || "0");
      const currentContent = strings.get(keys[1]) || "";
      const currentSequence = Number(strings.get(keys[2]) || "0");
      const baseVersion = Number(baseVersionRaw);
      const stale = baseVersion !== currentVersion ? 1 : 0;

      if (stale === 1 && policy === "reject") {
        return [0, currentVersion, currentVersion, currentContent, currentSequence, "", stale, "stale_base_version"];
      }

      const nextVersion = currentVersion + 1;
      const nextSequence = currentSequence + 1;
      strings.set(keys[0], String(nextVersion));
      strings.set(keys[1], content);
      strings.set(keys[2], String(nextSequence));
      const streamId = nextStreamId();
      entriesFor(keys[3]).push({
        id: streamId,
        fields: {
          sequence: String(nextSequence),
          version: String(nextVersion),
          operationId,
          clientId,
          username,
          content,
        },
      });
      return [1, currentVersion, nextVersion, content, nextSequence, streamId, stale, ""];
    }

    return { error: "ERR unsupported EVAL script in LiveCollab test Redis" };
  }

  return { error: `ERR unsupported command ${command}` };
}

const server = net.createServer((socket) => {
  let buffer = Buffer.alloc(0);
  let chain = Promise.resolve();

  // Backend termination is an intentional Phase 2 fault. A TCP reset from a
  // killed backend must not crash the Redis test double itself.
  socket.on("error", () => {
    // The real Redis path naturally tolerates client disconnects; mirror that
    // behavior here so the deterministic test harness does not become flaky.
  });

  socket.on("data", (chunk) => {
    buffer = Buffer.concat([buffer, chunk]);
    while (true) {
      const parsed = tryParseCommand(buffer);
      if (!parsed) break;
      buffer = buffer.subarray(parsed.consumed);
      chain = chain
        .then(() => handleCommand(parsed.args))
        .then((response) => {
          if (!socket.destroyed) socket.write(encode(response));
        })
        .catch((error) => {
          if (!socket.destroyed) socket.write(encode({ error: `ERR ${error.message}` }));
        });
    }
  });
});

server.listen(port, host, () => {
  console.log(`LiveCollab test Redis READY ${host}:${port}`);
});

function shutdown() {
  server.close(() => process.exit(0));
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
