package main

import "github.com/gorilla/websocket"

type Client struct {
	Conn     *websocket.Conn
	Username string
	RoomID   string
}
type Message struct {
	Type     string   `json:"type"`
	Content  string   `json:"content"`
	Username string   `json:"username,omitempty"`
	Users    []string `json:"users,omitempty"`
}
