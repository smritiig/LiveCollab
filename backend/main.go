package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	config := LoadConfig()
	telemetry := NewTelemetry(config)
	recorder, err := NewTraceRecorder(config.TraceFile)
	if err != nil {
		log.Fatalf("create trace recorder: %v", err)
	}
	defer func() {
		if err := recorder.Close(); err != nil {
			log.Printf("close trace recorder: %v", err)
		}
	}()

	manager := NewRoomManager()
	mode := "local-memory"
	if config.RedisAddr != "" {
		store := NewRedisStore(config.RedisAddr)
		store.SetTelemetry(telemetry)
		store.SetArtificialDelay(config.RedisDelayMs)
		if err := waitForRedis(store, 10*time.Second); err != nil {
			log.Fatalf("connect Redis at %s: %v", config.RedisAddr, err)
		}
		manager = NewDistributedRoomManager(store, config.InstanceID, recorder, telemetry)
		mode = "redis-streams"
	}
	defer manager.Close()

	application := NewApplication(manager, config, recorder, telemetry)
	server := &http.Server{
		Addr:              ":" + config.Port,
		Handler:           application.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	shutdownContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-shutdownContext.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("graceful shutdown: %v", err)
		}
	}()

	fmt.Printf(
		"LiveCollab server %s listening on http://localhost:%s (mode: %s, stale-write policy: %s)\n",
		config.InstanceID, config.Port, mode, config.StaleWritePolicy,
	)
	telemetry.Logger.Event("info", "server_started", map[string]any{
		"mode": mode, "port": config.Port, "staleWritePolicy": config.StaleWritePolicy,
		"otlpEndpoint": config.OTLPEndpoint,
	})
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server failed: %v", err)
	}
}

func waitForRedis(store *RedisStore, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := store.Ping(); err == nil {
			return nil
		} else {
			lastErr = err
		}
		time.Sleep(200 * time.Millisecond)
	}
	return lastErr
}
