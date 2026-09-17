export type Participant = {
  connectionId: string;
  clientId: string;
  username: string;
  instanceId?: string;
};

export type PresenceState = {
  revision: number;
  participants: Participant[];
};

export function initialPresence(): PresenceState {
  return { revision: -1, participants: [] };
}

// Presence has its own revision, independent of document sequence/version.
// room_state (including conflict resync) never owns the participant list.
export function applyPresenceMessage(current: PresenceState, message: unknown): PresenceState {
  if (typeof message !== "object" || message === null) return current;
  const data = message as Record<string, unknown>;
  if (data.type !== "presence_update" ||
      typeof data.presenceRevision !== "number" ||
      !Number.isSafeInteger(data.presenceRevision) ||
      data.presenceRevision < 0 ||
      data.presenceRevision <= current.revision ||
      !Array.isArray(data.participants)) return current;

  const participants: Participant[] = [];
  const connections = new Set<string>();
  for (const record of data.participants) {
    if (typeof record !== "object" || record === null ||
        typeof record.connectionId !== "string" || !record.connectionId ||
        typeof record.clientId !== "string" || typeof record.username !== "string" ||
        (record.instanceId !== undefined && typeof record.instanceId !== "string") ||
        connections.has(record.connectionId)) return current;
    connections.add(record.connectionId);
    participants.push(record);
  }
  return { revision: data.presenceRevision, participants };
}
