package main

import (
	"fmt"
	"sync"
)

type Room struct {
	ID       string
	Content  string
	Version  int64
	Sequence int64
	Clients  []*Client
	Store    *RedisStore
	mu       sync.RWMutex
}

type RoomSnapshot struct {
	Content  string
	Version  int64
	Sequence int64
}

type ApplyContentResult struct {
	Accepted        bool
	Reason          string
	Content         string
	BaseVersion     int64
	PreviousVersion int64
	ServerVersion   int64
	Sequence        int64
	StreamID        string
	Stale           bool
}

func (r *Room) AddClient(client *Client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Clients = append(r.Clients, client)
}

func (r *Room) GetUsernames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	usernames := make([]string, 0, len(r.Clients))
	for _, client := range r.Clients {
		usernames = append(usernames, client.Username)
	}
	return usernames
}

func (r *Room) clientsSnapshot() []*Client {
	r.mu.RLock()
	defer r.mu.RUnlock()
	clients := make([]*Client, len(r.Clients))
	copy(clients, r.Clients)
	return clients
}

func (r *Room) Broadcast(message string) {
	for _, client := range r.clientsSnapshot() {
		if client.Conn != nil {
			_ = client.WriteMessage(1, []byte(message))
		}
	}
}

func (r *Room) BroadcastJSON(message Message) {
	for _, client := range r.clientsSnapshot() {
		if client.Conn == nil {
			continue
		}
		if err := client.WriteJSON(message); err != nil {
			fmt.Println("broadcast error:", err)
		}
	}
}

func (r *Room) ApplyOperation(content string, baseVersion int64, policy StaleWritePolicy, operationID, clientID, username, traceID, parentSpanID string) (ApplyContentResult, error) {
	if r.Store != nil {
		result, err := r.Store.ApplyContentUpdate(r.ID, content, baseVersion, policy, operationID, clientID, username, traceID, parentSpanID)
		if err != nil {
			return ApplyContentResult{}, err
		}
		if result.Accepted {
			r.applySnapshot(RoomSnapshot{Content: result.Content, Version: result.ServerVersion, Sequence: result.Sequence})
		} else {
			r.applySnapshot(RoomSnapshot{Content: result.Content, Version: result.ServerVersion, Sequence: result.Sequence})
		}
		return result, nil
	}

	return r.applyLocalContentUpdate(content, baseVersion, policy), nil
}

func (r *Room) ApplyContentUpdate(content string, baseVersion int64, policy StaleWritePolicy) ApplyContentResult {
	return r.applyLocalContentUpdate(content, baseVersion, policy)
}

func (r *Room) applyLocalContentUpdate(content string, baseVersion int64, policy StaleWritePolicy) ApplyContentResult {
	r.mu.Lock()
	defer r.mu.Unlock()

	previousVersion := r.Version
	stale := baseVersion != previousVersion

	if stale && policy == StaleWriteReject {
		return ApplyContentResult{
			Accepted:        false,
			Reason:          "stale_base_version",
			Content:         r.Content,
			BaseVersion:     baseVersion,
			PreviousVersion: previousVersion,
			ServerVersion:   r.Version,
			Sequence:        r.Sequence,
			Stale:           true,
		}
	}

	r.Content = content
	r.Version++
	r.Sequence++

	return ApplyContentResult{
		Accepted:        true,
		Content:         r.Content,
		BaseVersion:     baseVersion,
		PreviousVersion: previousVersion,
		ServerVersion:   r.Version,
		Sequence:        r.Sequence,
		Stale:           stale,
	}
}

func (r *Room) ApplyDistributedEvent(event DistributedEvent) bool {
	r.mu.Lock()
	if event.Sequence <= r.Sequence {
		r.mu.Unlock()
		return false
	}
	r.Content = event.Content
	r.Version = event.Version
	r.Sequence = event.Sequence
	r.mu.Unlock()
	return true
}

func (r *Room) applySnapshot(snapshot RoomSnapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if snapshot.Sequence < r.Sequence {
		return
	}
	r.Content = snapshot.Content
	r.Version = snapshot.Version
	r.Sequence = snapshot.Sequence
}

func (r *Room) BroadcastPresence() {
	r.BroadcastJSON(Message{Type: "presence_update", Users: r.GetUsernames()})
}

func (r *Room) RemoveClient(target *Client) {
	r.mu.Lock()
	defer r.mu.Unlock()

	updatedClients := make([]*Client, 0, len(r.Clients))
	for _, client := range r.Clients {
		if client != target {
			updatedClients = append(updatedClients, client)
		}
	}
	r.Clients = updatedClients
}

func (r *Room) ClientCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.Clients)
}

func (r *Room) GetSnapshot() RoomSnapshot {
	if r.Store != nil {
		snapshot, exists, err := r.Store.GetSnapshot(r.ID)
		if err == nil && exists {
			r.applySnapshot(snapshot)
		}
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	return RoomSnapshot{Content: r.Content, Version: r.Version, Sequence: r.Sequence}
}
