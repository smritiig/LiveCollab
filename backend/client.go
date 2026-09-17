package main

import (
	"sync"

	"github.com/gorilla/websocket"
)

type Client struct {
	Conn     *websocket.Conn
	Username string
	RoomID   string
	ClientID string
	writeMu  sync.Mutex
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
