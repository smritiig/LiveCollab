import assert from "node:assert/strict";
import test from "node:test";
import { applyPresenceMessage, initialPresence } from "../src/presence.ts";

const first = { connectionId: "socket-a", clientId: "shared-client", username: "Smriti", instanceId: "a" };
const second = { ...first, connectionId: "socket-b", instanceId: "b" };
const update = (presenceRevision, participants) => ({ type: "presence_update", presenceRevision, participants });

test("delayed older snapshot cannot overwrite a newer notification", async () => {
  let state = initialPresence();
  let release;
  const barrier = new Promise(resolve => { release = resolve; });
  const delayed = (async () => {
    const older = update(1, [first]);
    await barrier;
    state = applyPresenceMessage(state, older);
  })();
  state = applyPresenceMessage(state, update(2, [first, second]));
  const newer = state;
  release();
  await delayed;
  assert.equal(state, newer);
  assert.equal(state.revision, 2);
  assert.deepEqual(state.participants, [first, second]);
});

test("same username and clientId retain two connection identities; one leave retains the other", () => {
  let state = applyPresenceMessage(initialPresence(), update(2, [first, second]));
  assert.equal(state.participants.length, 2);
  assert.deepEqual(state.participants.map(p => p.connectionId), ["socket-a", "socket-b"]);
  state = applyPresenceMessage(state, update(3, [second]));
  assert.deepEqual(state.participants, [second]);
});

test("room_state and conflict resync cannot clear or replace global presence", () => {
  const state = applyPresenceMessage(initialPresence(), update(4, [first, second]));
  for (const message of [
    { type: "room_state", content: "initial", sequence: 0 },
    { type: "room_state", users: ["Smriti"], reason: "resync_after_conflict", sequence: 500 },
    { type: "content_update", sequence: 501 },
  ]) assert.equal(applyPresenceMessage(state, message), state);
});

test("duplicate revisions are ignored and a newer empty list clears presence", () => {
  const state = applyPresenceMessage(initialPresence(), update(3, [first]));
  assert.equal(applyPresenceMessage(state, update(3, [second])), state);
  assert.deepEqual(applyPresenceMessage(state, update(4, [])), { revision: 4, participants: [] });
});

test("malformed snapshots cannot advance the revision", () => {
  const state = applyPresenceMessage(initialPresence(), update(1, [first]));
  for (const message of [update(2, null), update(2, [first, first]), update(2, [{}]), update(-1, []), update(2.5, [])]) {
    assert.equal(applyPresenceMessage(state, message), state);
  }
});
