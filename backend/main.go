package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func enableCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "http://localhost:5173")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
func main() {
	manager := NewRoomManager()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "LiveCollab server is running 🚀")
	})

	http.HandleFunc("/rooms", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		room := manager.CreateRoom()
		w.Header().Set("Content-Type", "application/json")

		response := map[string]string{
			"roomId": room.ID,
		}

		json.NewEncoder(w).Encode(response)
	})

	http.HandleFunc("/rooms/check", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		roomID := r.URL.Query().Get("roomId")
		if roomID == "" {
			http.Error(w, "roomId is required", http.StatusBadRequest)
			return
		}

		room, exists := manager.GetRoom(roomID)
		if !exists {
			http.Error(w, "room not found", http.StatusNotFound)
			return
		}

		fmt.Fprintf(w, "room exists: %s", room.ID)
	})
	http.HandleFunc("/rooms/join", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

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

		fakeClient := &Client{
			Username: username,
			RoomID:   room.ID,
		}

		room.AddClient(fakeClient)

		w.Header().Set("Content-Type", "application/json")

		response := map[string]interface{}{
			"roomId": room.ID,
			"users":  room.GetUsernames(),
		}

		json.NewEncoder(w).Encode(response)
	})
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWS(manager, w, r)
	})
	fmt.Println("Server running on http://localhost:8080")
	http.ListenAndServe(":8080", enableCORS(http.DefaultServeMux))
}
