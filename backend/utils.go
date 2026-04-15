package main

import (
	"crypto/rand"
	"encoding/hex"
)

func generateRoomID() string {
	bytes := make([]byte, 3) // 6 characters
	_, err := rand.Read(bytes)
	if err != nil {
		return "fallback123"
	}
	return hex.EncodeToString(bytes)
}
