package main

import "sync"

type RoomManager struct {
	Rooms map[string]*Room
	mu    sync.RWMutex
}

func NewRoomManager() *RoomManager {
	return &RoomManager{
		Rooms: make(map[string]*Room),
	}
}
func (m *RoomManager) CreateRoom() *Room {
	m.mu.Lock()
	defer m.mu.Unlock()

	roomID := generateRoomID()

	room := &Room{
		ID:      roomID,
		Content: "",
		Clients: []*Client{},
	}

	m.Rooms[roomID] = room
	return room
}
func (m *RoomManager) GetRoom(roomID string) (*Room, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	room, exists := m.Rooms[roomID]
	return room, exists
}
func (m *RoomManager) DeleteRoom(roomID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.Rooms, roomID)
}
