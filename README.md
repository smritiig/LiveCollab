# LiveCollab

A multi-room real-time collaborative editor built with **Go**, **React**, **TypeScript**, and **WebSockets**.

LiveCollab allows users to create or join isolated rooms, collaborate on shared text in real time, see who is online, view typing activity, and share invite links for live sessions.

---

## Features

- **Multi-room collaboration** with isolated room state
- **Real-time text synchronization** using WebSockets
- **Online users list** with live presence updates
- **Typing indicator** for active collaborators
- **Last updated by** attribution
- **Shareable room links**
- **Debounced updates** to reduce unnecessary WebSocket traffic
- **Connection status** feedback in the UI
- **Automatic room cleanup** when all users disconnect

---

## Tech Stack

### Frontend
- React
- TypeScript
- Vite
- React Router
- Native WebSocket API

### Backend
- Go
- Gorilla WebSocket
- In-memory room manager
- Mutex-based concurrency protection

---

## Architecture Overview

### Frontend
The frontend provides:
- a landing page to create or join collaboration rooms
- a room page with the shared editor, online users list, typing indicator, and invite link support
- WebSocket-based real-time communication with the backend

### Backend
The backend is responsible for:
- creating and managing rooms
- maintaining shared room content
- tracking connected users per room
- broadcasting updates only to users within the same room
- cleaning up empty rooms when all users disconnect

### Communication Flow

```text
Client → WebSocket → Go Server → Room State Update → Broadcast to Room Clients
