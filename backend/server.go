package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

type traceContextKey string

const (
	traceIDKey traceContextKey = "traceID"
	spanIDKey  traceContextKey = "spanID"
)

type Application struct {
	manager   *RoomManager
	config    Config
	recorder  *TraceRecorder
	telemetry *Telemetry
}

func NewApplication(manager *RoomManager, config Config, recorder *TraceRecorder, telemetry *Telemetry) *Application {
	return &Application{manager: manager, config: config, recorder: recorder, telemetry: telemetry}
}

func (a *Application) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, "LiveCollab server %s is running", a.config.InstanceID)
	})

	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(a.telemetry.Metrics.Prometheus(a.config.InstanceID)))
	})

	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": "ok", "instanceId": a.config.InstanceID,
		})
	})

	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, _ *http.Request) {
		status := http.StatusOK
		payload := map[string]string{
			"status": "ready", "instanceId": a.config.InstanceID,
		}
		if a.manager.store != nil {
			if err := a.manager.store.Ping(); err != nil {
				status = http.StatusServiceUnavailable
				payload["status"] = "not_ready"
				payload["reason"] = "redis_unavailable"
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(payload)
	})

	mux.HandleFunc("POST /rooms", func(w http.ResponseWriter, _ *http.Request) {
		room, err := a.manager.CreateRoom()
		if err != nil {
			http.Error(w, "room storage unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"roomId": room.ID})
	})

	mux.HandleFunc("GET /rooms/check", func(w http.ResponseWriter, r *http.Request) {
		roomID := r.URL.Query().Get("roomId")
		if roomID == "" {
			http.Error(w, "roomId is required", http.StatusBadRequest)
			return
		}

		redisStarted := time.Now()

		room, exists := a.manager.GetRoom(roomID)

		redisEnded := time.Now()

		traceID, _ := r.Context().Value(traceIDKey).(string)
		parentSpanID, _ := r.Context().Value(spanIDKey).(string)
		redisSpanID := NewSpanID()

		if traceID != "" {
			a.telemetry.Tracer.Export(Span{
				TraceID:  traceID,
				SpanID:   redisSpanID,
				ParentID: parentSpanID,
				Name:     "redis.get.room_snapshot",
				Start:    redisStarted,
				End:      redisEnded,
				Attributes: map[string]any{
					"room.id": roomID,
				},
			})

			a.telemetry.Logger.Event("info", "redis_room_snapshot", map[string]any{
				"traceId":    traceID,
				"spanId":     redisSpanID,
				"roomId":     roomID,
				"durationMs": redisEnded.Sub(redisStarted).Seconds() * 1000,
				"found":      exists,
			})
		}
		if !exists {
			http.Error(w, "room not found", http.StatusNotFound)
			return
		}
		snapshot := room.GetSnapshot()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"roomId":        room.ID,
			"serverVersion": snapshot.Version,
			"sequence":      snapshot.Sequence,
			"instanceId":    a.config.InstanceID,
		})
	})

	mux.HandleFunc("POST /debug/faults/redis-delay", func(w http.ResponseWriter, r *http.Request) {
		if !a.config.EnableDebugFaults || a.manager.store == nil {
			http.Error(w, "debug faults disabled", http.StatusNotFound)
			return
		}
		ms, err := strconv.ParseInt(r.URL.Query().Get("ms"), 10, 64)
		if err != nil || ms < 0 || ms > 5000 {
			http.Error(w, "ms must be between 0 and 5000", http.StatusBadRequest)
			return
		}
		a.manager.store.SetArtificialDelay(ms)
		a.telemetry.Logger.Event("warn", "debug_redis_delay_changed", map[string]any{"delayMs": ms})
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"instanceId": a.config.InstanceID, "redisDelayMs": ms})
	})

	mux.HandleFunc("GET /debug/faults", func(w http.ResponseWriter, _ *http.Request) {
		if !a.config.EnableDebugFaults || a.manager.store == nil {
			http.Error(w, "debug faults disabled", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"instanceId": a.config.InstanceID, "redisDelayMs": a.manager.store.ArtificialDelay()})
	})

	mux.HandleFunc("GET /ws", func(w http.ResponseWriter, r *http.Request) {
		serveWS(a.manager, a.config, a.recorder, a.telemetry, w, r)
	})

	return enableCORS(a.observeHTTP(mux), a.config.AllowedOrigins)
}

func (a *Application) observeHTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" || r.URL.Path == "/ws" {
			next.ServeHTTP(w, r)
			return
		}

		started := time.Now()
		traceID := NewTraceID()
		spanID := NewSpanID()

		ctx := context.WithValue(r.Context(), traceIDKey, traceID)
		ctx = context.WithValue(ctx, spanIDKey, spanID)
		r = r.WithContext(ctx)

		a.telemetry.Metrics.HTTPRequest()

		next.ServeHTTP(w, r)

		ended := time.Now()
		duration := ended.Sub(started)

		a.telemetry.Metrics.ObserveHTTP(duration.Seconds())

		a.telemetry.Logger.Event("info", "http_request", map[string]any{
			"traceId":    traceID,
			"method":     r.Method,
			"path":       r.URL.Path,
			"durationMs": duration.Seconds() * 1000,
		})

		a.telemetry.Tracer.Export(Span{
			TraceID: traceID,
			SpanID:  spanID,
			Name:    "HTTP " + r.Method + " " + r.URL.Path,
			Start:   started,
			End:     ended,
			Attributes: map[string]any{
				"http.request.method": r.Method,
				"url.path":            r.URL.Path,
			},
		})
	})
}

func enableCORS(next http.Handler, allowedOrigins []string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if originAllowed(origin, allowedOrigins) && origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			if !originAllowed(origin, allowedOrigins) {
				http.Error(w, "origin not allowed", http.StatusForbidden)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
