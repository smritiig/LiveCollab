package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func serveWS(manager *RoomManager, w http.ResponseWriter, r *http.Request) {
	roomID := r.URL.Query().Get("roomId")
	username := r.URL.Query().Get("username")

	if roomID == "" || username == "" {
		http.Error(w, "roomId and username are required", http.StatusBadRequest)
		return
	}

	room, exists := manager.GetRoom(roomID)
	if !exists {
		http.Error(w, "room not found", http.StatusNotFound)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	client := &Client{
		Conn:     conn,
		Username: username,
		RoomID:   roomID,
	}

	defer func() {
		room.RemoveClient(client)
		room.BroadcastPresence()

		if room.ClientCount() == 0 {
			manager.DeleteRoom(room.ID)
			fmt.Println("deleted empty room:", room.ID)
		}

		conn.Close()
	}()

	room.AddClient(client)
	room.BroadcastPresence()

	room.BroadcastJSON(Message{
		Type:     "content_update",
		Content:  room.Content,
		Username: client.Username,
	})
	client.Conn.WriteJSON(Message{
		Type:    "room_state",
		Content: room.GetContent(),
		Users:   room.GetUsernames(),
	})
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			fmt.Println("read error:", err)
			break
		}

		var incoming Message
		err = json.Unmarshal(msg, &incoming)
		if err != nil {
			fmt.Println("invalid json message")
			continue
		}

		switch incoming.Type {

		case "content_update":
			room.UpdateContent(incoming.Content)

			room.BroadcastJSON(Message{
				Type:    "content_update",
				Content: room.Content,
			})
		case "typing":
			room.BroadcastJSON(Message{
				Type:     "typing",
				Username: client.Username,
			})
		default:
			fmt.Println("unknown message type")
		}
	}
}
