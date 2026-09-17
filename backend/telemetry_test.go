package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMetricsPrometheusIncludesCorrectnessSignals(t *testing.T) {
	metrics := NewMetrics()
	metrics.WebSocketConnected()
	metrics.OperationStarted()
	metrics.ReconnectStarted()
	metrics.ResumeSucceeded()
	metrics.ReplayedEvents(4)
	metrics.SequenceGap()
	metrics.ObserveOperation(0.042)
	metrics.ObserveRedis(0.011)

	output := metrics.Prometheus("backend-test")
	for _, expected := range []string{
		`livecollab_websocket_connections{instance_id="backend-test"} 1`,
		`livecollab_operations_total{instance_id="backend-test"} 1`,
		`livecollab_resume_success_total{instance_id="backend-test"} 1`,
		`livecollab_replayed_events_total{instance_id="backend-test"} 4`,
		`livecollab_sequence_gaps_total{instance_id="backend-test"} 1`,
		`livecollab_operation_duration_seconds_bucket`,
		`livecollab_redis_command_duration_seconds_bucket`,
	} {
		if !strings.Contains(output, expected) {
			t.Fatalf("metrics output missing %q\n%s", expected, output)
		}
	}
}

func TestOTLPExporterPostsTraceJSON(t *testing.T) {
	received := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/traces" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("unexpected content type: %s", r.Header.Get("Content-Type"))
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			return
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Errorf("decode body: %v", err)
			return
		}
		received <- payload
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	exporter := NewOTLPExporter(server.URL, "livecollab-test", "backend-test")
	exporter.Export(Span{
		TraceID: randomHex(16),
		SpanID:  randomHex(8),
		Name:    "livecollab.document.operation",
		Start:   time.Now().Add(-10 * time.Millisecond),
		End:     time.Now(),
		Attributes: map[string]any{
			"operation.id":   "op-1",
			"event.sequence": int64(1),
		},
	})

	select {
	case payload := <-received:
		if _, ok := payload["resourceSpans"]; !ok {
			t.Fatalf("OTLP payload missing resourceSpans: %#v", payload)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for OTLP export")
	}
}

func TestStreamLagSeconds(t *testing.T) {
	id := time.Now().Add(-50 * time.Millisecond).UnixMilli()
	lag, ok := streamLagSeconds(strconv.FormatInt(id, 10) + "-0")
	if !ok {
		t.Fatal("expected valid stream ID")
	}
	if lag < 0.02 || lag > 1.0 {
		t.Fatalf("unexpected stream lag: %f", lag)
	}
}
