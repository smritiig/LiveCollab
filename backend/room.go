package main

import (
	"fmt"
	"sync"
)

type Room struct {
	ID      string
	Content string
	Clients []*Client
	mu      sync.RWMutex
}

func (r *Room) AddClient(client *Client) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.Clients = append(r.Clients, client)
}

func (r *Room) GetUsernames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	usernames := []string{}

	for _, client := range r.Clients {
		usernames = append(usernames, client.Username)
	}

	return usernames
}
func (r *Room) Broadcast(message string) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, client := range r.Clients {
		if client.Conn != nil {
			client.Conn.WriteMessage(1, []byte(message))
		}
	}
}
func (r *Room) BroadcastJSON(message Message) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, client := range r.Clients {
		if client.Conn != nil {
			err := client.Conn.WriteJSON(message)
			if err != nil {
				fmt.Println("broadcast error:", err)
				continue
			}
		}
	}
}
func (r *Room) UpdateContent(content string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.Content = content
}
func (r *Room) BroadcastPresence() {
	r.BroadcastJSON(Message{
		Type:  "presence_update",
		Users: r.GetUsernames(),
	})
}
func (r *Room) RemoveClient(target *Client) {
	r.mu.Lock()
	defer r.mu.Unlock()

	updatedClients := []*Client{}

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
func (r *Room) GetContent() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.Content
}
