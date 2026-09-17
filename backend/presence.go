package main

import (
	"crypto/rand"
	"encoding/hex"
	"log"
	"sort"
	"time"
)

// Presence is connection-based. clientId and username may be shared by tabs.
type Participant struct {
	ConnectionID string `json:"connectionId"`
	ClientID     string `json:"clientId"`
	Username     string `json:"username"`
	InstanceID   string `json:"instanceId"`
}

type PresenceSnapshot struct {
	Revision     int64         `json:"presenceRevision"`
	Participants []Participant `json:"participants"`
}

type PresenceUpdate struct {
	Type string `json:"type"`
	PresenceSnapshot
}

func newConnectionID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}

func (c *Client) participant() Participant {
	return Participant{ConnectionID: c.ConnectionID, ClientID: c.ClientID, Username: c.Username, InstanceID: c.InstanceID}
}

// The revision check and socket write share a lock, including initial snapshots
// sent by serveWS. A delayed snapshot cannot follow a newer one on this socket.
func (c *Client) writePresence(snapshot PresenceSnapshot) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.presenceSent && snapshot.Revision <= c.presenceRevision {
		return nil
	}
	if err := c.Conn.WriteJSON(PresenceUpdate{Type: "presence_update", PresenceSnapshot: snapshot}); err != nil {
		return err
	}
	c.presenceRevision = snapshot.Revision
	c.presenceSent = true
	return nil
}

func (r *Room) localPresenceSnapshot() PresenceSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	snapshot := PresenceSnapshot{Revision: r.localPresenceRevision, Participants: make([]Participant, 0, len(r.Clients))}
	for _, c := range r.Clients {
		snapshot.Participants = append(snapshot.Participants, c.participant())
	}
	sortParticipants(snapshot.Participants)
	return snapshot
}

func sortParticipants(participants []Participant) {
	sort.Slice(participants, func(i, j int) bool { return participants[i].ConnectionID < participants[j].ConnectionID })
}

func (r *Room) broadcastPresenceSnapshot(snapshot PresenceSnapshot) {
	for _, c := range r.clientsSnapshot() {
		if c.Conn != nil {
			if err := c.writePresence(snapshot); err != nil {
				log.Printf("presence broadcast room %s: %v", r.ID, err)
			}
		}
	}
}

// Preserve legacy local room_state users without leaking a backend-local list
// into distributed document snapshots or conflict resynchronization messages.
func (r *Room) roomStateUsers() []string {
	if r.Store != nil {
		return nil
	}
	return r.GetUsernames()
}

func (m *RoomManager) ensurePresenceWatcher(room *Room) {
	if m.store == nil {
		return
	}
	m.watchersMu.Lock()
	if m.presenceWatchers == nil {
		m.presenceWatchers = make(map[string]struct{})
	}
	if _, exists := m.presenceWatchers[room.ID]; exists {
		m.watchersMu.Unlock()
		return
	}
	m.presenceWatchers[room.ID] = struct{}{}
	m.watchersMu.Unlock()
	go m.watchPresence(room)
}

func (m *RoomManager) watchPresence(room *Room) {
	// Every backend reads independently. Start at zero so watcher startup cannot
	// miss a join; notifications refresh current state rather than replaying joins.
	cursor := "0-0"
	for {
		select {
		case <-m.ctx.Done():
			return
		default:
		}
		next, err := m.store.ReadPresenceChanges(room.ID, cursor)
		if m.ctx.Err() != nil {
			return
		}
		if err == nil && next == cursor {
			continue
		}
		if err == nil {
			var snapshot PresenceSnapshot
			snapshot, err = m.store.GetPresence(room.ID)
			if err == nil {
				room.broadcastPresenceSnapshot(snapshot)
				cursor = next
				continue
			}
		}
		if m.ctx.Err() != nil {
			return
		}
		// Keep the old cursor on read/snapshot failure so the notification is retried.
		log.Printf("watch presence room %s: %v", room.ID, err)
		select {
		case <-m.ctx.Done():
			return
		case <-time.After(150 * time.Millisecond):
		}
	}
}
