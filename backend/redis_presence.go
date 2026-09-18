package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func roomPresenceKey(roomID string) string { return "livecollab:room:" + roomID + ":presence" }
func roomPresenceRevisionKey(roomID string) string {
	return "livecollab:room:" + roomID + ":presence-revision"
}
func roomPresenceStreamKey(roomID string) string {
	return "livecollab:room:" + roomID + ":presence-events"
}

func roomPresenceExpiryKey(roomID string) string {
	return "livecollab:room:" + roomID + ":presence-expiry"
}

// Redis time is authoritative; application time is used only for scheduling.
const presenceNowScript = `
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
`

const joinPresenceScript = `
-- livecollab_presence_join
` + presenceNowScript + `
if redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2]) == 0 then
 return tonumber(redis.call('GET', KEYS[2]) or '0')
end
redis.call('ZADD', KEYS[4], now + tonumber(ARGV[3]), ARGV[1])
local revision = redis.call('INCR', KEYS[2])
redis.call('XADD', KEYS[3], '*', 'revision', tostring(revision))
return revision
`

const leavePresenceScript = `
-- livecollab_presence_leave
local removed = redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[1])
if removed == 0 then
 return tonumber(redis.call('GET', KEYS[2]) or '0')
end
local revision = redis.call('INCR', KEYS[2])
redis.call('XADD', KEYS[3], '*', 'revision', tostring(revision))
return revision
`

const renewPresenceScript = `
-- livecollab_presence_renew
` + presenceNowScript + `
local expiry = redis.call('ZSCORE', KEYS[4], ARGV[1])
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 0 or not expiry or tonumber(expiry) <= now then
 return 0
end
-- Never resurrect a removed/expired membership or shorten an existing lease.
local nextExpiry = now + tonumber(ARGV[2])
if nextExpiry > tonumber(expiry) then
 redis.call('ZADD', KEYS[4], 'XX', nextExpiry, ARGV[1])
end
return 1
`

// Also treat old Patch 1 records without a lease as expired. Snapshot reads and
// sweepers use the same atomic cleanup so expiry always advances the revision
// before the smaller snapshot is exposed to revision-guarded clients.
const prunePresenceBody = presenceNowScript + `
local removed = 0
for _, connectionId in ipairs(redis.call('HKEYS', KEYS[1])) do
 local expiry = redis.call('ZSCORE', KEYS[4], connectionId)
 if not expiry or tonumber(expiry) <= now then
  removed = removed + redis.call('HDEL', KEYS[1], connectionId)
  redis.call('ZREM', KEYS[4], connectionId)
 end
end
redis.call('ZREMRANGEBYSCORE', KEYS[4], '-inf', now)
if removed > 0 then
 local revision = redis.call('INCR', KEYS[2])
 redis.call('XADD', KEYS[3], '*', 'revision', tostring(revision))
end
`

const prunePresenceScript = `
-- livecollab_presence_prune
` + prunePresenceBody + `return removed`

const presenceSnapshotScript = `
-- livecollab_presence_snapshot
` + prunePresenceBody + `
return {redis.call('GET', KEYS[2]) or '0', redis.call('HVALS', KEYS[1])}
`

func (s *RedisStore) presenceEval(script, roomID string, args ...string) (any, error) {
	command := []string{"EVAL", script, "4", roomPresenceKey(roomID), roomPresenceRevisionKey(roomID), roomPresenceStreamKey(roomID), roomPresenceExpiryKey(roomID)}
	return s.do(append(command, args...)...)
}

func (s *RedisStore) JoinPresence(roomID string, participant Participant, lease ...time.Duration) error {
	if participant.ConnectionID == "" {
		return fmt.Errorf("presence requires connectionId")
	}
	duration := defaultPresenceLease
	if len(lease) > 0 {
		duration = lease[0]
	}
	if duration.Milliseconds() <= 0 {
		return fmt.Errorf("presence lease must be positive")
	}
	record, err := json.Marshal(participant)
	if err != nil {
		return err
	}
	_, err = s.presenceEval(joinPresenceScript, roomID, participant.ConnectionID, string(record), strconv.FormatInt(duration.Milliseconds(), 10))
	return err
}

func (s *RedisStore) LeavePresence(roomID, connectionID string) error {
	_, err := s.presenceEval(leavePresenceScript, roomID, connectionID)
	return err
}

func (s *RedisStore) RenewPresence(roomID, connectionID string, lease time.Duration) (bool, error) {
	if lease.Milliseconds() <= 0 {
		return false, fmt.Errorf("presence lease must be positive")
	}
	raw, err := s.presenceEval(renewPresenceScript, roomID, connectionID, strconv.FormatInt(lease.Milliseconds(), 10))
	if err != nil {
		return false, err
	}
	renewed, err := redisInt64(raw)
	return renewed == 1, err
}

func (s *RedisStore) PrunePresence(roomID string) (int64, error) {
	raw, err := s.presenceEval(prunePresenceScript, roomID)
	if err != nil {
		return 0, err
	}
	return redisInt64(raw)
}

// Discover rooms globally, including those whose only backend crashed. No
// dependency on this backend's in-memory RoomManager.Rooms membership.
func (s *RedisStore) ScanPresenceRooms(cursor string) ([]string, string, error) {
	raw, err := s.do("SCAN", cursor, "MATCH", "livecollab:room:*:presence", "COUNT", "100")
	if err != nil {
		return nil, cursor, err
	}
	result, ok := raw.([]any)
	if !ok || len(result) != 2 {
		return nil, cursor, fmt.Errorf("invalid presence SCAN: %#v", raw)
	}
	keys, ok := result[1].([]any)
	if !ok {
		return nil, cursor, fmt.Errorf("invalid presence keys: %#v", result[1])
	}
	rooms := make([]string, 0, len(keys))
	for _, key := range keys {
		rooms = append(rooms, strings.TrimSuffix(strings.TrimPrefix(redisString(key), "livecollab:room:"), ":presence"))
	}
	return rooms, redisString(result[0]), nil
}

func (s *RedisStore) GetPresence(roomID string) (PresenceSnapshot, error) {
	raw, err := s.presenceEval(presenceSnapshotScript, roomID)
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
