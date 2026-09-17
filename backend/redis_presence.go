package main

import (
	"encoding/json"
	"fmt"
)

func roomPresenceKey(roomID string) string { return "livecollab:room:" + roomID + ":presence" }
func roomPresenceRevisionKey(roomID string) string {
	return "livecollab:room:" + roomID + ":presence-revision"
}
func roomPresenceStreamKey(roomID string) string {
	return "livecollab:room:" + roomID + ":presence-events"
}

// Patch 1 deliberately has no expiry: backend crashes can leave stale presence
// records until Patch 2 adds leases/heartbeats/expiry cleanup.
const joinPresenceScript = `
-- livecollab_presence_join
if redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2]) == 0 then
 return tonumber(redis.call('GET', KEYS[2]) or '0')
end
local revision = redis.call('INCR', KEYS[2])
redis.call('XADD', KEYS[3], '*', 'revision', tostring(revision))
return revision
`

const leavePresenceScript = `
-- livecollab_presence_leave
if redis.call('HDEL', KEYS[1], ARGV[1]) == 0 then
 return tonumber(redis.call('GET', KEYS[2]) or '0')
end
local revision = redis.call('INCR', KEYS[2])
redis.call('XADD', KEYS[3], '*', 'revision', tostring(revision))
return revision
`

const presenceSnapshotScript = `
-- livecollab_presence_snapshot
return {redis.call('GET', KEYS[2]) or '0', redis.call('HVALS', KEYS[1])}
`

func (s *RedisStore) JoinPresence(roomID string, participant Participant) error {
	if participant.ConnectionID == "" {
		return fmt.Errorf("presence requires connectionId")
	}
	record, err := json.Marshal(participant)
	if err != nil {
		return err
	}
	_, err = s.do("EVAL", joinPresenceScript, "3", roomPresenceKey(roomID), roomPresenceRevisionKey(roomID), roomPresenceStreamKey(roomID), participant.ConnectionID, string(record))
	return err
}

func (s *RedisStore) LeavePresence(roomID, connectionID string) error {
	_, err := s.do("EVAL", leavePresenceScript, "3", roomPresenceKey(roomID), roomPresenceRevisionKey(roomID), roomPresenceStreamKey(roomID), connectionID)
	return err
}

func (s *RedisStore) GetPresence(roomID string) (PresenceSnapshot, error) {
	raw, err := s.do("EVAL", presenceSnapshotScript, "2", roomPresenceKey(roomID), roomPresenceRevisionKey(roomID))
	if err != nil {
		return PresenceSnapshot{}, err
	}
	values, ok := raw.([]any)
	if !ok || len(values) != 2 {
		return PresenceSnapshot{}, fmt.Errorf("invalid presence snapshot: %#v", raw)
	}
	revision, err := redisInt64(values[0])
	if err != nil {
		return PresenceSnapshot{}, err
	}
	records, ok := values[1].([]any)
	if !ok {
		return PresenceSnapshot{}, fmt.Errorf("invalid presence records: %#v", values[1])
	}
	snapshot := PresenceSnapshot{Revision: revision, Participants: make([]Participant, 0, len(records))}
	for _, record := range records {
		var participant Participant
		if err := json.Unmarshal([]byte(redisString(record)), &participant); err != nil {
			return PresenceSnapshot{}, err
		}
		snapshot.Participants = append(snapshot.Participants, participant)
	}
	sortParticipants(snapshot.Participants)
	return snapshot, nil
}

// Return the final notification cursor in the batch; the watcher loads one
// authoritative snapshot for the entire batch. This is independent of document
// stream parsing, sequence numbers, versions and replay.
func (s *RedisStore) ReadPresenceChanges(roomID, cursor string) (string, error) {
	raw, err := s.do("XREAD", "COUNT", "100", "BLOCK", "250", "STREAMS", roomPresenceStreamKey(roomID), cursor)
	if err != nil {
		return cursor, err
	}
	if raw == nil {
		return cursor, nil
	}
	streams, ok := raw.([]any)
	if !ok || len(streams) != 1 {
		return cursor, fmt.Errorf("invalid presence XREAD: %#v", raw)
	}
	stream, ok := streams[0].([]any)
	if !ok || len(stream) != 2 {
		return cursor, fmt.Errorf("invalid presence stream: %#v", streams[0])
	}
	entries, ok := stream[1].([]any)
	if !ok {
		return cursor, fmt.Errorf("invalid presence entries: %#v", stream[1])
	}
	for _, value := range entries {
		entry, ok := value.([]any)
		if !ok || len(entry) != 2 {
			return cursor, fmt.Errorf("invalid presence entry: %#v", value)
		}
		cursor = redisString(entry[0])
	}
	return cursor, nil
}
