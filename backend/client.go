package main

import (
	"sort"
	"sync"

	"github.com/gorilla/websocket"
)

type Client struct {
	Conn         *websocket.Conn
	Username     string
	RoomID       string
	ClientID     string
	ConnectionID string
	InstanceID   string
	writeMu      sync.Mutex
	// Protected by writeMu, independently of document replay sequencing.
	presenceRevision int64
	presenceSent     bool

	// deliveryMu serializes live content delivery and the replay/live handoff.
	// Historical writes use writeMu only, so live events can buffer during replay.
	deliveryMu   sync.Mutex
	replaying    bool
	lastSequence int64
	pending      map[int64]Message
}

// beginReplay must be called before the client is registered with its room.
func (c *Client) beginReplay(lastSequence int64) {
	c.replaying = true
	c.lastSequence = lastSequence
	c.pending = make(map[int64]Message)
}

func (c *Client) writeContentUpdate(message Message) error {
	c.deliveryMu.Lock()
	defer c.deliveryMu.Unlock()
	if message.Sequence <= c.lastSequence {
		return nil
	}
	if c.replaying {
		c.pending[message.Sequence] = message
		return nil
	}
	if err := c.WriteJSON(message); err != nil {
		return err
	}
	c.lastSequence = message.Sequence
	return nil
}

func (c *Client) finishReplay(complete Message) error {
	c.deliveryMu.Lock()
	defer c.deliveryMu.Unlock()
	if err := c.WriteJSON(complete); err != nil {
		return err
	}
	c.lastSequence = complete.LatestSequence
	sequences := make([]int64, 0, len(c.pending))
	for sequence := range c.pending {
		if sequence > c.lastSequence {
			sequences = append(sequences, sequence)
		}
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	for _, sequence := range sequences {
		if err := c.WriteJSON(c.pending[sequence]); err != nil {
			return err
		}
		c.lastSequence = sequence
	}
	c.pending = nil
	c.replaying = false
	return nil
}

func (c *Client) WriteJSON(v interface{}) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.Conn.WriteJSON(v)
}

func (c *Client) WriteMessage(messageType int, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.Conn.WriteMessage(messageType, data)
}

type Message struct {
	Type            string   `json:"type"`
	Content         string   `json:"content,omitempty"`
	Username        string   `json:"username,omitempty"`
	Users           []string `json:"users,omitempty"`
	OperationID     string   `json:"operationId,omitempty"`
	ClientID        string   `json:"clientId,omitempty"`
	BaseVersion     *int64   `json:"baseVersion,omitempty"`
	PreviousVersion int64    `json:"previousVersion,omitempty"`
	ServerVersion   int64    `json:"serverVersion,omitempty"`
	Sequence        int64    `json:"sequence,omitempty"`
	StreamID        string   `json:"streamId,omitempty"`
	FromSequence    int64    `json:"fromSequence,omitempty"`
	LatestSequence  int64    `json:"latestSequence,omitempty"`
	Replayed        int      `json:"replayed,omitempty"`
	Accepted        *bool    `json:"accepted,omitempty"`
	Stale           bool     `json:"stale,omitempty"`
	Reason          string   `json:"reason,omitempty"`
}
