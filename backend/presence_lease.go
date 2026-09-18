package main

import (
	"log"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultPresenceHeartbeat = 10 * time.Second
	defaultPresenceLease     = 30 * time.Second
	defaultPresenceSweep     = 5 * time.Second
)

type PresenceTiming struct {
	HeartbeatInterval time.Duration
	LeaseDuration     time.Duration
	SweepInterval     time.Duration
}

func (p PresenceTiming) withDefaults() PresenceTiming {
	if p.HeartbeatInterval <= 0 {
		p.HeartbeatInterval = defaultPresenceHeartbeat
	}
	if p.LeaseDuration <= 0 {
		p.LeaseDuration = defaultPresenceLease
	}
	if p.SweepInterval <= 0 {
		p.SweepInterval = defaultPresenceSweep
	}
	if p.LeaseDuration < 3*p.HeartbeatInterval {
		p.LeaseDuration = 3 * p.HeartbeatInterval
	}
	return p
}

// Return reader-owned activity reporting and a cancellation function. Only
// actual pong/data reads can renew a lease; ping ticks alone never renew it.
func (m *RoomManager) startPresenceHeartbeat(client *Client) (func() error, func()) {
	activity := make(chan time.Time, 1)
	stop := make(chan struct{})
	timing := m.presenceTiming
	noteActivity := func() error {
		now := time.Now()
		if err := client.Conn.SetReadDeadline(now.Add(timing.LeaseDuration)); err != nil {
			return err
		}
		select {
		case activity <- now:
		default:
		}
		return nil
	}
	_ = client.Conn.SetReadDeadline(time.Now().Add(timing.LeaseDuration))
	client.Conn.SetPongHandler(func(string) error { return noteActivity() })
	go func() {
		ticker := time.NewTicker(timing.HeartbeatInterval)
		defer ticker.Stop()
		lastRenewal := time.Now()
		for {
			select {
			case <-stop:
				return
			case <-m.ctx.Done():
				return
			case <-ticker.C:
				if err := client.Conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(timing.HeartbeatInterval)); err != nil {
					_ = client.Conn.Close()
					return
				}
			case observed := <-activity:
				if time.Since(observed) >= timing.LeaseDuration {
					_ = client.Conn.Close()
					return
				}
				if time.Since(lastRenewal) < timing.HeartbeatInterval {
					continue
				}
				renewed, err := m.store.RenewPresence(client.RoomID, client.ConnectionID, timing.LeaseDuration)
				if err != nil || !renewed {
					if err != nil {
						log.Printf("renew presence room %s connection %s: %v", client.RoomID, client.ConnectionID, err)
					}
					// Force a new connection identity after lease loss, never revive the old one.
					_ = client.Conn.Close()
					return
				}
				lastRenewal = time.Now()
			}
		}
	}()
	return noteActivity, func() { close(stop) }
}

func (m *RoomManager) sweepPresenceOnce() error {
	cursor := "0"
	for {
		if m.ctx.Err() != nil {
			return m.ctx.Err()
		}
		rooms, next, err := m.store.ScanPresenceRooms(cursor)
		if err != nil {
			return err
		}
		for _, roomID := range rooms {
			if m.ctx.Err() != nil {
				return m.ctx.Err()
			}
			if _, err := m.store.PrunePresence(roomID); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == "0" {
			return nil
		}
	}
}

func (m *RoomManager) runPresenceSweeper() {
	ticker := time.NewTicker(m.presenceTiming.SweepInterval)
	defer ticker.Stop()
	for {
		if err := m.sweepPresenceOnce(); err != nil && m.ctx.Err() == nil {
			log.Printf("sweep presence: %v", err)
		}
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
