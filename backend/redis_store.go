package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type DistributedEvent struct {
	StreamID    string
	Sequence    int64
	Version     int64
	OperationID string
	ClientID    string
	Username    string
	Content     string
}

type RedisStore struct {
	client            *RedisClient
	telemetry         *Telemetry
	artificialDelayMs atomic.Int64
}

func NewRedisStore(addr string) *RedisStore {
	return &RedisStore{client: NewRedisClient(addr)}
}

func (s *RedisStore) SetTelemetry(telemetry *Telemetry) {
	s.telemetry = telemetry
}

func (s *RedisStore) SetArtificialDelay(ms int64) {
	if ms < 0 {
		ms = 0
	}
	s.artificialDelayMs.Store(ms)
}

func (s *RedisStore) ArtificialDelay() int64 {
	return s.artificialDelayMs.Load()
}

func (s *RedisStore) do(args ...string) (any, error) {
	command := "UNKNOWN"
	if len(args) > 0 {
		command = strings.ToUpper(args[0])
	}
	delay := s.artificialDelayMs.Load()
	if delay > 0 {
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}
	started := time.Now()
	value, err := s.client.Do(args...)
	duration := time.Since(started).Seconds() + float64(delay)/1000.0
	if s.telemetry != nil {
		// Blocking XREAD is a long-poll by design and must not pollute the Redis
		// dependency-latency SLI. Other commands represent request-path work.
		if command != "XREAD" {
			s.telemetry.Metrics.ObserveRedis(duration)
		}
		if err != nil {
			s.telemetry.Metrics.RedisError()
		}
		if command != "XREAD" || err != nil {
			s.telemetry.Logger.Event("info", "redis_command", map[string]any{
				"command":    command,
				"durationMs": duration * 1000,
				"error":      errorString(err),
			})
		}
	}
	return value, err
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (s *RedisStore) Ping() error {
	value, err := s.do("PING")
	if err != nil {
		return err
	}
	if redisString(value) != "PONG" {
		return fmt.Errorf("unexpected Redis PING response: %v", value)
	}
	return nil
}

func roomExistsKey(roomID string) string   { return "livecollab:room:" + roomID + ":exists" }
func roomContentKey(roomID string) string  { return "livecollab:room:" + roomID + ":content" }
func roomVersionKey(roomID string) string  { return "livecollab:room:" + roomID + ":version" }
func roomSequenceKey(roomID string) string { return "livecollab:room:" + roomID + ":sequence" }
func roomStreamKey(roomID string) string   { return "livecollab:room:" + roomID + ":events" }

func (s *RedisStore) CreateRoom(roomID string) error {
	script := `
redis.call('SET', KEYS[1], '1')
redis.call('SET', KEYS[2], '')
redis.call('SET', KEYS[3], '0')
redis.call('SET', KEYS[4], '0')
return 1
`
	_, err := s.do(
		"EVAL", script, "4",
		roomExistsKey(roomID), roomContentKey(roomID), roomVersionKey(roomID), roomSequenceKey(roomID),
	)
	return err
}

func (s *RedisStore) GetSnapshot(roomID string) (RoomSnapshot, bool, error) {
	exists, err := s.do("GET", roomExistsKey(roomID))
	if err != nil {
		return RoomSnapshot{}, false, err
	}
	if redisString(exists) != "1" {
		return RoomSnapshot{}, false, nil
	}

	content, err := s.do("GET", roomContentKey(roomID))
	if err != nil {
		return RoomSnapshot{}, false, err
	}
	versionRaw, err := s.do("GET", roomVersionKey(roomID))
	if err != nil {
		return RoomSnapshot{}, false, err
	}
	sequenceRaw, err := s.do("GET", roomSequenceKey(roomID))
	if err != nil {
		return RoomSnapshot{}, false, err
	}
	version, err := redisInt64(versionRaw)
	if err != nil {
		return RoomSnapshot{}, false, err
	}
	sequence, err := redisInt64(sequenceRaw)
	if err != nil {
		return RoomSnapshot{}, false, err
	}

	return RoomSnapshot{Content: redisString(content), Version: version, Sequence: sequence}, true, nil
}

const applyOperationScript = `
local currentVersion = tonumber(redis.call('GET', KEYS[1]) or '0')
local currentContent = redis.call('GET', KEYS[2]) or ''
local currentSequence = tonumber(redis.call('GET', KEYS[3]) or '0')
local baseVersion = tonumber(ARGV[1])
local stale = 0
if baseVersion ~= currentVersion then stale = 1 end
if stale == 1 and ARGV[6] == 'reject' then
  return {0, currentVersion, currentVersion, currentContent, currentSequence, '', stale, 'stale_base_version'}
end
local nextVersion = currentVersion + 1
local nextSequence = redis.call('INCR', KEYS[3])
redis.call('SET', KEYS[1], tostring(nextVersion))
redis.call('SET', KEYS[2], ARGV[2])
local streamID = redis.call('XADD', KEYS[4], '*',
  'sequence', tostring(nextSequence),
  'version', tostring(nextVersion),
  'operationId', ARGV[3],
  'clientId', ARGV[4],
  'username', ARGV[5],
  'content', ARGV[2])
return {1, currentVersion, nextVersion, ARGV[2], nextSequence, streamID, stale, ''}
`

func (s *RedisStore) ApplyContentUpdate(roomID, content string, baseVersion int64, policy StaleWritePolicy, operationID, clientID, username, traceID, parentSpanID string) (ApplyContentResult, error) {
	redisStarted := time.Now()
	redisSpanID := NewSpanID()
	raw, err := s.do(
		"EVAL", applyOperationScript, "4",
		roomVersionKey(roomID), roomContentKey(roomID), roomSequenceKey(roomID), roomStreamKey(roomID),
		strconv.FormatInt(baseVersion, 10), content, operationID, clientID, username, string(policy),
	)
	redisEnded := time.Now()
	if s.telemetry != nil && traceID != "" {
		span := Span{TraceID: traceID, SpanID: redisSpanID, ParentID: parentSpanID, Name: "redis.eval.apply_operation", Start: redisStarted, End: redisEnded, Attributes: map[string]any{
			"db.system": "redis", "db.operation.name": "EVAL", "room.id": roomID, "operation.id": operationID,
		}}
		if err != nil {
			span.Error = err.Error()
		}
		s.telemetry.Tracer.Export(span)
		s.telemetry.Logger.Event("info", "redis_apply_operation", map[string]any{"traceId": traceID, "spanId": redisSpanID, "operationId": operationID, "roomId": roomID, "durationMs": redisEnded.Sub(redisStarted).Seconds() * 1000, "error": errorString(err)})
	}
	if err != nil {
		return ApplyContentResult{}, err
	}

	values, ok := raw.([]any)
	if !ok || len(values) < 8 {
		return ApplyContentResult{}, fmt.Errorf("unexpected Redis apply response: %#v", raw)
	}
	acceptedInt, err := redisInt64(values[0])
	if err != nil {
		return ApplyContentResult{}, err
	}
	previousVersion, err := redisInt64(values[1])
	if err != nil {
		return ApplyContentResult{}, err
	}
	serverVersion, err := redisInt64(values[2])
	if err != nil {
		return ApplyContentResult{}, err
	}
	sequence, err := redisInt64(values[4])
	if err != nil {
		return ApplyContentResult{}, err
	}
	staleInt, err := redisInt64(values[6])
	if err != nil {
		return ApplyContentResult{}, err
	}

	return ApplyContentResult{
		Accepted:        acceptedInt == 1,
		Reason:          redisString(values[7]),
		Content:         redisString(values[3]),
		BaseVersion:     baseVersion,
		PreviousVersion: previousVersion,
		ServerVersion:   serverVersion,
		Sequence:        sequence,
		StreamID:        redisString(values[5]),
		Stale:           staleInt == 1,
	}, nil
}

func (s *RedisStore) LatestStreamID(roomID string) (string, error) {
	raw, err := s.do("XREVRANGE", roomStreamKey(roomID), "+", "-", "COUNT", "1")
	if err != nil {
		return "", err
	}
	events, err := parseStreamEntries(raw)
	if err != nil {
		return "", err
	}
	if len(events) == 0 {
		return "0-0", nil
	}
	return events[0].StreamID, nil
}

func (s *RedisStore) ReadAfterStreamID(roomID, lastStreamID string, blockMs int) ([]DistributedEvent, error) {
	raw, err := s.do(
		"XREAD", "COUNT", "100", "BLOCK", strconv.Itoa(blockMs), "STREAMS", roomStreamKey(roomID), lastStreamID,
	)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, nil
	}

	outer, ok := raw.([]any)
	if !ok || len(outer) == 0 {
		return nil, nil
	}
	streamTuple, ok := outer[0].([]any)
	if !ok || len(streamTuple) < 2 {
		return nil, fmt.Errorf("unexpected XREAD response: %#v", raw)
	}
	return parseStreamEntries(streamTuple[1])
}

func (s *RedisStore) EventsAfterSequence(roomID string, lastSequence int64) ([]DistributedEvent, error) {
	raw, err := s.do("XRANGE", roomStreamKey(roomID), "-", "+", "COUNT", "1000")
	if err != nil {
		return nil, err
	}
	events, err := parseStreamEntries(raw)
	if err != nil {
		return nil, err
	}
	filtered := make([]DistributedEvent, 0, len(events))
	for _, event := range events {
		if event.Sequence > lastSequence {
			filtered = append(filtered, event)
		}
	}
	return filtered, nil
}

func parseStreamEntries(raw any) ([]DistributedEvent, error) {
	if raw == nil {
		return nil, nil
	}
	entries, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected stream entries: %#v", raw)
	}

	result := make([]DistributedEvent, 0, len(entries))
	for _, item := range entries {
		tuple, ok := item.([]any)
		if !ok || len(tuple) < 2 {
			return nil, fmt.Errorf("unexpected stream entry: %#v", item)
		}
		streamID := redisString(tuple[0])
		fieldValues, ok := tuple[1].([]any)
		if !ok || len(fieldValues)%2 != 0 {
			return nil, fmt.Errorf("unexpected stream fields: %#v", tuple[1])
		}
		fields := make(map[string]string, len(fieldValues)/2)
		for index := 0; index < len(fieldValues); index += 2 {
			fields[redisString(fieldValues[index])] = redisString(fieldValues[index+1])
		}
		sequence, err := strconv.ParseInt(fields["sequence"], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse sequence: %w", err)
		}
		version, err := strconv.ParseInt(fields["version"], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse version: %w", err)
		}
		result = append(result, DistributedEvent{
			StreamID:    streamID,
			Sequence:    sequence,
			Version:     version,
			OperationID: fields["operationId"],
			ClientID:    fields["clientId"],
			Username:    fields["username"],
			Content:     fields["content"],
		})
	}
	return result, nil
}

func (e DistributedEvent) String() string {
	return strings.Join([]string{e.StreamID, strconv.FormatInt(e.Sequence, 10), e.OperationID}, ":")
}
