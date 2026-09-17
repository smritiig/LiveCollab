package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
)

func serveWS(manager *RoomManager, config Config, recorder *TraceRecorder, telemetry *Telemetry, w http.ResponseWriter, r *http.Request) {
	roomID := r.URL.Query().Get("roomId")
	username := r.URL.Query().Get("username")
	clientID := r.URL.Query().Get("clientId")
	if clientID == "" {
		clientID = username
	}
	lastSequence, _ := strconv.ParseInt(r.URL.Query().Get("lastSequence"), 10, 64)

	if roomID == "" || username == "" {
		http.Error(w, "roomId and username are required", http.StatusBadRequest)
		return
	}

	room, exists := manager.GetRoom(roomID)
	if !exists {
		http.Error(w, "room not found", http.StatusNotFound)
		return
	}

	upgrader := websocket.Upgrader{CheckOrigin: func(request *http.Request) bool {
		return originAllowed(request.Header.Get("Origin"), config.AllowedOrigins)
	}}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	connectionID, err := newConnectionID()
	if err != nil {
		_ = conn.Close()
		return
	}
	client := &Client{Conn: conn, Username: username, RoomID: roomID, ClientID: clientID, ConnectionID: connectionID, InstanceID: config.InstanceID}
	if room.Store != nil && lastSequence > 0 {
		client.beginReplay(lastSequence)
	}
	telemetry.Metrics.WebSocketConnected()
	if lastSequence > 0 {
		telemetry.Metrics.ReconnectStarted()
	}
	telemetry.Logger.Event("info", "websocket_connected", map[string]any{"roomId": roomID, "clientId": clientID, "username": username, "resumeCursor": lastSequence})
	recorder.Record(TraceEvent{
		Stage:         "client_connected",
		InstanceID:    config.InstanceID,
		RoomID:        roomID,
		ClientID:      clientID,
		EventSequence: lastSequence,
	})

	defer func() {
		telemetry.Metrics.WebSocketDisconnected()
		telemetry.Logger.Event("info", "websocket_disconnected", map[string]any{"roomId": roomID, "clientId": clientID})
		room.RemoveClient(client)
		if room.Store != nil {
			// Best-effort graceful removal only. Patch 2 will address crash expiry.
			if err := room.Store.LeavePresence(room.ID, client.ConnectionID); err != nil {
				telemetry.Logger.Event("error", "presence_leave_failed", map[string]any{"roomId": room.ID, "connectionId": client.ConnectionID, "error": err.Error()})
			}
		} else {
			room.BroadcastPresence()
		}
		recorder.Record(TraceEvent{
			Stage:      "client_disconnected",
			InstanceID: config.InstanceID,
			RoomID:     roomID,
			ClientID:   clientID,
		})
		if room.ClientCount() == 0 {
			manager.DeleteRoom(room.ID)
		}
		_ = conn.Close()
	}()

	room.AddClient(client)
	if room.Store != nil {
		if err := room.Store.JoinPresence(room.ID, client.participant()); err != nil {
			telemetry.Logger.Event("error", "presence_join_failed", map[string]any{"roomId": room.ID, "connectionId": client.ConnectionID, "error": err.Error()})
			_ = client.WriteJSON(Message{Type: "error", Reason: "presence_unavailable"})
			return
		}
		manager.ensurePresenceWatcher(room)
	} else {
		room.BroadcastPresence()
	}

	if room.Store != nil && lastSequence > 0 {
		if err := resumeClient(room, client, lastSequence, config, recorder, telemetry); err != nil {
			telemetry.Metrics.ResumeFailed()
			_ = client.WriteJSON(Message{Type: "error", Reason: "resume_failed"})
			return
		}
	} else {
		snapshot := room.GetSnapshot()
		if err := client.WriteJSON(Message{
			Type:          "room_state",
			Content:       snapshot.Content,
			Users:         room.roomStateUsers(),
			ServerVersion: snapshot.Version,
			Sequence:      snapshot.Sequence,
		}); err != nil {
			return
		}
	}

	if room.Store != nil {
		presence, err := room.Store.GetPresence(room.ID)
		if err != nil {
			_ = client.WriteJSON(Message{Type: "error", Reason: "presence_unavailable"})
			return
		}
		if err := client.writePresence(presence); err != nil {
			return
		}
	}

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			break
		}

		var incoming Message
		if err := json.Unmarshal(raw, &incoming); err != nil {
			_ = client.WriteJSON(Message{Type: "error", Reason: "invalid_json"})
			continue
		}

		switch incoming.Type {
		case "content_update":
			handleContentUpdate(room, client, incoming, config, recorder, telemetry)
		case "typing":
			room.BroadcastJSON(Message{Type: "typing", Username: client.Username})
		default:
			_ = client.WriteJSON(Message{Type: "error", Reason: "unknown_message_type"})
		}
	}
}

func resumeClient(room *Room, client *Client, lastSequence int64, config Config, recorder *TraceRecorder, telemetry *Telemetry) error {
	started := time.Now()
	traceID := NewTraceID()
	spanID := NewSpanID()
	boundary, err := room.Store.ReplayBoundary(room.ID)
	if err != nil {
		return err
	}
	if lastSequence > boundary.Sequence {
		return fmt.Errorf("resume cursor %d exceeds boundary %d", lastSequence, boundary.Sequence)
	}
	if err := client.WriteJSON(Message{
		Type:           "resume_started",
		FromSequence:   lastSequence,
		LatestSequence: boundary.Sequence,
		ServerVersion:  boundary.Version,
	}); err != nil {
		return err
	}

	replayed := 0
	err = room.Store.EventsAfterSequence(room.ID, lastSequence, boundary, func(event DistributedEvent) error {
		if err := client.WriteJSON(messageFromDistributedEvent(event)); err != nil {
			return err
		}
		recorder.Record(TraceEvent{
			Stage:         "resume_event_replayed",
			InstanceID:    config.InstanceID,
			RoomID:        room.ID,
			OperationID:   event.OperationID,
			ClientID:      client.ClientID,
			EventSequence: event.Sequence,
			StreamID:      event.StreamID,
			ServerVersion: event.Version,
			Content:       event.Content,
		})
		replayed++
		return nil
	})
	if err != nil {
		if errors.Is(err, errReplayGap) {
			telemetry.Metrics.SequenceGap()
			recorder.Record(TraceEvent{Stage: "resume_gap_detected", InstanceID: config.InstanceID,
				RoomID: room.ID, ClientID: client.ClientID, EventSequence: lastSequence + int64(replayed) + 1, Reason: err.Error()})
		}
		return err
	}

	if err := client.finishReplay(Message{
		Type:           "resume_complete",
		FromSequence:   lastSequence,
		LatestSequence: boundary.Sequence,
		ServerVersion:  boundary.Version,
		Replayed:       replayed,
	}); err != nil {
		return err
	}

	telemetry.Metrics.ResumeSucceeded()
	telemetry.Metrics.ReplayedEvents(replayed)
	telemetry.Metrics.ObserveResume(time.Since(started).Seconds())
	telemetry.Logger.Event("info", "resume_complete", map[string]any{"traceId": traceID, "roomId": room.ID, "clientId": client.ClientID, "fromSequence": lastSequence, "latestSequence": boundary.Sequence, "replayed": replayed, "durationMs": time.Since(started).Seconds() * 1000})
	telemetry.Tracer.Export(Span{TraceID: traceID, SpanID: spanID, Name: "livecollab.websocket.resume", Start: started, End: time.Now(), Attributes: map[string]any{"room.id": room.ID, "client.id": client.ClientID, "resume.from_sequence": lastSequence, "resume.latest_sequence": boundary.Sequence, "resume.replayed": replayed}})
	recorder.Record(TraceEvent{
		Stage:         "resume_complete",
		TraceID:       traceID,
		SpanID:        spanID,
		DurationMs:    time.Since(started).Seconds() * 1000,
		InstanceID:    config.InstanceID,
		RoomID:        room.ID,
		ClientID:      client.ClientID,
		EventSequence: boundary.Sequence,
		ServerVersion: boundary.Version,
		Reason:        fmt.Sprintf("replayed_%d", replayed),
	})
	return nil
}

func handleContentUpdate(room *Room, client *Client, incoming Message, config Config, recorder *TraceRecorder, telemetry *Telemetry) {
	started := time.Now()
	traceID := NewTraceID()
	spanID := NewSpanID()
	telemetry.Metrics.OperationStarted()
	clientID := incoming.ClientID
	if clientID == "" {
		clientID = client.ClientID
	}

	if incoming.OperationID == "" || incoming.BaseVersion == nil {
		snapshot := room.GetSnapshot()
		_ = client.WriteJSON(Message{
			Type:          "operation_result",
			OperationID:   incoming.OperationID,
			ClientID:      clientID,
			Accepted:      boolPointer(false),
			Reason:        "operation_id_and_base_version_required",
			ServerVersion: snapshot.Version,
			Sequence:      snapshot.Sequence,
		})
		return
	}

	before := room.GetSnapshot()
	telemetry.Logger.Event("info", "operation_received", map[string]any{"traceId": traceID, "operationId": incoming.OperationID, "clientId": clientID, "roomId": room.ID, "baseVersion": *incoming.BaseVersion, "serverVersion": before.Version})
	recorder.Record(TraceEvent{
		Stage:         "server_received",
		TraceID:       traceID,
		SpanID:        spanID,
		InstanceID:    config.InstanceID,
		RoomID:        room.ID,
		OperationID:   incoming.OperationID,
		ClientID:      clientID,
		BaseVersion:   *incoming.BaseVersion,
		ServerVersion: before.Version,
		EventSequence: before.Sequence,
		Content:       incoming.Content,
	})

	result, err := room.ApplyOperation(
		incoming.Content,
		*incoming.BaseVersion,
		config.StaleWritePolicy,
		incoming.OperationID,
		clientID,
		client.Username,
		traceID,
		spanID,
	)
	if err != nil {
		telemetry.Metrics.OperationError()
		telemetry.Metrics.ObserveOperation(time.Since(started).Seconds())
		telemetry.Logger.Event("error", "operation_failed", map[string]any{"traceId": traceID, "operationId": incoming.OperationID, "roomId": room.ID, "error": err.Error(), "durationMs": time.Since(started).Seconds() * 1000})
		telemetry.Tracer.Export(Span{TraceID: traceID, SpanID: spanID, Name: "livecollab.document.operation", Start: started, End: time.Now(), Error: err.Error(), Attributes: map[string]any{"room.id": room.ID, "operation.id": incoming.OperationID, "client.id": clientID}})
		recorder.Record(TraceEvent{
			Stage:       "server_apply_error",
			TraceID:     traceID,
			SpanID:      spanID,
			InstanceID:  config.InstanceID,
			RoomID:      room.ID,
			OperationID: incoming.OperationID,
			ClientID:    clientID,
			Reason:      err.Error(),
		})
		_ = client.WriteJSON(Message{Type: "operation_result", OperationID: incoming.OperationID, Accepted: boolPointer(false), Reason: "storage_error"})
		return
	}

	if !result.Accepted && result.Stale {
		telemetry.Metrics.StaleWriteRejected()
	}
	stage := "server_applied"
	if !result.Accepted {
		stage = "server_rejected"
	}
	recorder.Record(TraceEvent{
		Stage:           stage,
		TraceID:         traceID,
		SpanID:          spanID,
		InstanceID:      config.InstanceID,
		RoomID:          room.ID,
		OperationID:     incoming.OperationID,
		ClientID:        clientID,
		EventSequence:   result.Sequence,
		StreamID:        result.StreamID,
		BaseVersion:     result.BaseVersion,
		PreviousVersion: result.PreviousVersion,
		ServerVersion:   result.ServerVersion,
		Accepted:        boolPointer(result.Accepted),
		Stale:           result.Stale,
		Reason:          result.Reason,
		Content:         result.Content,
	})

	operationResult := Message{
		Type:            "operation_result",
		OperationID:     incoming.OperationID,
		ClientID:        clientID,
		BaseVersion:     int64Pointer(result.BaseVersion),
		PreviousVersion: result.PreviousVersion,
		ServerVersion:   result.ServerVersion,
		Sequence:        result.Sequence,
		StreamID:        result.StreamID,
		Accepted:        boolPointer(result.Accepted),
		Stale:           result.Stale,
		Reason:          result.Reason,
	}
	if err := client.WriteJSON(operationResult); err == nil {
		recorder.Record(TraceEvent{
			Stage:           "server_acknowledged",
			TraceID:         traceID,
			SpanID:          spanID,
			InstanceID:      config.InstanceID,
			RoomID:          room.ID,
			OperationID:     incoming.OperationID,
			ClientID:        clientID,
			EventSequence:   result.Sequence,
			StreamID:        result.StreamID,
			BaseVersion:     result.BaseVersion,
			PreviousVersion: result.PreviousVersion,
			ServerVersion:   result.ServerVersion,
			Accepted:        boolPointer(result.Accepted),
			Stale:           result.Stale,
			Reason:          result.Reason,
		})
	}

	telemetry.Metrics.ObserveOperation(time.Since(started).Seconds())
	telemetry.Logger.Event("info", "operation_completed", map[string]any{
		"traceId": traceID, "operationId": incoming.OperationID, "clientId": clientID, "roomId": room.ID,
		"accepted": result.Accepted, "stale": result.Stale, "serverVersion": result.ServerVersion,
		"eventSequence": result.Sequence, "durationMs": time.Since(started).Seconds() * 1000,
	})
	telemetry.Tracer.Export(Span{TraceID: traceID, SpanID: spanID, Name: "livecollab.document.operation", Start: started, End: time.Now(), Attributes: map[string]any{
		"room.id": room.ID, "operation.id": incoming.OperationID, "client.id": clientID, "operation.accepted": result.Accepted,
		"operation.stale": result.Stale, "document.base_version": result.BaseVersion, "document.server_version": result.ServerVersion, "event.sequence": result.Sequence,
	}})

	if !result.Accepted {
		_ = client.WriteJSON(Message{
			Type:          "room_state",
			Content:       result.Content,
			Users:         room.roomStateUsers(),
			ServerVersion: result.ServerVersion,
			Sequence:      result.Sequence,
			Reason:        "resync_after_conflict",
		})
		return
	}

	// Redis Streams are the sole producer of distributed content fan-out.
	if room.Store != nil {
		return
	}

	eventMessage := Message{
		Type:            "content_update",
		Content:         result.Content,
		Username:        client.Username,
		OperationID:     incoming.OperationID,
		ClientID:        clientID,
		BaseVersion:     int64Pointer(result.BaseVersion),
		PreviousVersion: result.PreviousVersion,
		ServerVersion:   result.ServerVersion,
		Sequence:        result.Sequence,
		StreamID:        result.StreamID,
		Accepted:        boolPointer(true),
		Stale:           result.Stale,
	}
	room.BroadcastJSON(eventMessage)
}
