package main

import (
	"context"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

type RoomManager struct {
	Rooms      map[string]*Room
	store      *RedisStore
	instanceID string
	recorder   *TraceRecorder
	telemetry  *Telemetry
	ctx        context.Context
	cancel     context.CancelFunc
	watchersMu sync.Mutex
	watchers   map[string]struct{}
	mu         sync.RWMutex
}

func NewRoomManager() *RoomManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &RoomManager{
		Rooms:    make(map[string]*Room),
		ctx:      ctx,
		cancel:   cancel,
		watchers: make(map[string]struct{}),
	}
}

func NewDistributedRoomManager(store *RedisStore, instanceID string, recorder *TraceRecorder, telemetry *Telemetry) *RoomManager {
	manager := NewRoomManager()
	manager.store = store
	manager.instanceID = instanceID
	manager.recorder = recorder
	manager.telemetry = telemetry
	return manager
}

func (m *RoomManager) Close() {
	if m.cancel != nil {
		m.cancel()
	}
}

func (m *RoomManager) CreateRoom() (*Room, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	roomID := generateRoomID()
	room := &Room{ID: roomID, Clients: []*Client{}, Store: m.store}
	if m.store != nil {
		if err := m.store.CreateRoom(roomID); err != nil {
			return nil, err
		}
	}
	m.Rooms[roomID] = room
	m.ensureWatcher(room)
	return room, nil
}

func (m *RoomManager) GetRoom(roomID string) (*Room, bool) {
	m.mu.RLock()
	room, exists := m.Rooms[roomID]
	m.mu.RUnlock()
	if exists {
		m.ensureWatcher(room)
		return room, true
	}

	if m.store == nil {
		return nil, false
	}

	snapshot, exists, err := m.store.GetSnapshot(roomID)
	if err != nil || !exists {
		if err != nil {
			log.Printf("load distributed room %s: %v", roomID, err)
		}
		return nil, false
	}

	m.mu.Lock()
	if existing, ok := m.Rooms[roomID]; ok {
		room = existing
	} else {
		room = &Room{
			ID:       roomID,
			Content:  snapshot.Content,
			Version:  snapshot.Version,
			Sequence: snapshot.Sequence,
			Clients:  []*Client{},
			Store:    m.store,
		}
		m.Rooms[roomID] = room
	}
	m.mu.Unlock()

	m.ensureWatcher(room)
	return room, true
}

func (m *RoomManager) DeleteRoom(roomID string) {
	if m.store != nil {
		// Redis owns distributed room lifetime in Phase 2. Keep the local room
		// cached so its stream watcher can continue serving future reconnects.
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.Rooms, roomID)
}

func (m *RoomManager) ensureWatcher(room *Room) {
	if m.store == nil || room == nil {
		return
	}

	m.watchersMu.Lock()
	if _, exists := m.watchers[room.ID]; exists {
		m.watchersMu.Unlock()
		return
	}
	room.mu.RLock()
	initialSequence := room.Sequence
	room.mu.RUnlock()
	m.watchers[room.ID] = struct{}{}
	m.watchersMu.Unlock()

	go m.watchRoom(room, initialSequence)
}

func (m *RoomManager) watchRoom(room *Room, initialSequence int64) {
	// Skip only the state hydrated before this watcher started. Subsequent
	// snapshot/ack updates must never suppress fan-out. The stream cursor, not
	// room.Sequence, tracks delivery progress after this fixed startup boundary.
	lastStreamID := "0-0"

	for {
		select {
		case <-m.ctx.Done():
			return
		default:
		}

		events, err := m.store.ReadAfterStreamID(room.ID, lastStreamID, 250)
		if err != nil {
			log.Printf("watch room %s: %v", room.ID, err)
			time.Sleep(150 * time.Millisecond)
			continue
		}
		for _, event := range events {
			lastStreamID = event.StreamID
			if event.Sequence <= initialSequence {
				continue
			}
			room.ApplyDistributedEvent(event)
			if m.telemetry != nil {
				m.telemetry.Metrics.StreamEvent()
				if lag, ok := streamLagSeconds(event.StreamID); ok {
					m.telemetry.Metrics.ObserveStreamLag(lag)
				}
			}
			m.recorder.Record(TraceEvent{
				Stage:         "stream_event_received",
				InstanceID:    m.instanceID,
				RoomID:        room.ID,
				OperationID:   event.OperationID,
				ClientID:      event.ClientID,
				EventSequence: event.Sequence,
				StreamID:      event.StreamID,
				ServerVersion: event.Version,
				Content:       event.Content,
			})
			if m.telemetry != nil {
				m.telemetry.Logger.Event("info", "stream_event_applied", map[string]any{
					"roomId": room.ID, "operationId": event.OperationID, "clientId": event.ClientID,
					"eventSequence": event.Sequence, "serverVersion": event.Version, "streamId": event.StreamID,
				})
			}
			room.BroadcastJSON(messageFromDistributedEvent(event))
		}
	}
}

func messageFromDistributedEvent(event DistributedEvent) Message {
	return Message{
		Type:          "content_update",
		Content:       event.Content,
		Username:      event.Username,
		OperationID:   event.OperationID,
		ClientID:      event.ClientID,
		ServerVersion: event.Version,
		Sequence:      event.Sequence,
		StreamID:      event.StreamID,
		Accepted:      boolPointer(true),
	}
}

func streamLagSeconds(streamID string) (float64, bool) {
	parts := strings.SplitN(streamID, "-", 2)
	if len(parts) == 0 {
		return 0, false
	}
	millis, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || millis <= 0 {
		return 0, false
	}
	lag := time.Since(time.UnixMilli(millis)).Seconds()
	if lag < 0 {
		lag = 0
	}
	return lag, true
}
