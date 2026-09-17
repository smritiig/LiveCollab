package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type TraceEvent struct {
	Sequence        uint64  `json:"sequence"`
	TraceID         string  `json:"traceId,omitempty"`
	SpanID          string  `json:"spanId,omitempty"`
	DurationMs      float64 `json:"durationMs,omitempty"`
	Timestamp       string  `json:"timestamp"`
	Stage           string  `json:"stage"`
	InstanceID      string  `json:"instanceId,omitempty"`
	RoomID          string  `json:"roomId,omitempty"`
	OperationID     string  `json:"operationId,omitempty"`
	ClientID        string  `json:"clientId,omitempty"`
	EventSequence   int64   `json:"eventSequence,omitempty"`
	StreamID        string  `json:"streamId,omitempty"`
	BaseVersion     int64   `json:"baseVersion,omitempty"`
	PreviousVersion int64   `json:"previousVersion,omitempty"`
	ServerVersion   int64   `json:"serverVersion,omitempty"`
	Accepted        *bool   `json:"accepted,omitempty"`
	Stale           bool    `json:"stale,omitempty"`
	Reason          string  `json:"reason,omitempty"`
	Content         string  `json:"content,omitempty"`
}

type TraceRecorder struct {
	mu       sync.Mutex
	sequence uint64
	file     *os.File
}

func NewTraceRecorder(path string) (*TraceRecorder, error) {
	recorder := &TraceRecorder{}
	if path == "" {
		return recorder, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	recorder.file = file
	return recorder, nil
}

func (r *TraceRecorder) Record(event TraceEvent) {
	if r == nil || r.file == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	r.sequence++
	event.Sequence = r.sequence
	if event.Timestamp == "" {
		event.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return
	}
	encoded = append(encoded, '\n')
	_, _ = r.file.Write(encoded)
}

func (r *TraceRecorder) Close() error {
	if r == nil || r.file == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.file.Close()
}

func boolPointer(value bool) *bool    { return &value }
func int64Pointer(value int64) *int64 { return &value }
