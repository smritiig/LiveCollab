package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type histogram struct {
	buckets []float64
	counts  []uint64
	count   uint64
	sum     float64
}

func newHistogram(buckets ...float64) *histogram {
	cp := append([]float64(nil), buckets...)
	sort.Float64s(cp)
	return &histogram{buckets: cp, counts: make([]uint64, len(cp))}
}

func (h *histogram) observe(value float64) {
	h.count++
	h.sum += value
	for i, bucket := range h.buckets {
		if value <= bucket {
			h.counts[i]++
		}
	}
}

type Metrics struct {
	mu sync.RWMutex

	websocketConnections int64
	operationsTotal      uint64
	operationErrors      uint64
	staleWriteRejections uint64
	reconnectsTotal      uint64
	resumeSuccessTotal   uint64
	resumeFailuresTotal  uint64
	replayedEventsTotal  uint64
	sequenceGapsTotal    uint64
	streamEventsTotal    uint64
	redisErrorsTotal     uint64
	httpRequestsTotal    uint64

	operationDuration *histogram
	redisDuration     *histogram
	streamLag         *histogram
	resumeDuration    *histogram
	httpDuration      *histogram
}

func NewMetrics() *Metrics {
	return &Metrics{
		operationDuration: newHistogram(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
		redisDuration:     newHistogram(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5),
		streamLag:         newHistogram(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
		resumeDuration:    newHistogram(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
		httpDuration:      newHistogram(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
	}
}

func (m *Metrics) WebSocketConnected()    { atomic.AddInt64(&m.websocketConnections, 1) }
func (m *Metrics) WebSocketDisconnected() { atomic.AddInt64(&m.websocketConnections, -1) }
func (m *Metrics) OperationStarted()      { atomic.AddUint64(&m.operationsTotal, 1) }
func (m *Metrics) OperationError()        { atomic.AddUint64(&m.operationErrors, 1) }
func (m *Metrics) StaleWriteRejected()    { atomic.AddUint64(&m.staleWriteRejections, 1) }
func (m *Metrics) ReconnectStarted()      { atomic.AddUint64(&m.reconnectsTotal, 1) }
func (m *Metrics) ResumeSucceeded()       { atomic.AddUint64(&m.resumeSuccessTotal, 1) }
func (m *Metrics) ResumeFailed()          { atomic.AddUint64(&m.resumeFailuresTotal, 1) }
func (m *Metrics) ReplayedEvents(n int)   { atomic.AddUint64(&m.replayedEventsTotal, uint64(n)) }
func (m *Metrics) SequenceGap()           { atomic.AddUint64(&m.sequenceGapsTotal, 1) }
func (m *Metrics) StreamEvent()           { atomic.AddUint64(&m.streamEventsTotal, 1) }
func (m *Metrics) RedisError()            { atomic.AddUint64(&m.redisErrorsTotal, 1) }
func (m *Metrics) HTTPRequest()           { atomic.AddUint64(&m.httpRequestsTotal, 1) }

func (m *Metrics) ObserveOperation(seconds float64) { m.observe(m.operationDuration, seconds) }
func (m *Metrics) ObserveRedis(seconds float64)     { m.observe(m.redisDuration, seconds) }
func (m *Metrics) ObserveStreamLag(seconds float64) { m.observe(m.streamLag, seconds) }
func (m *Metrics) ObserveResume(seconds float64)    { m.observe(m.resumeDuration, seconds) }
func (m *Metrics) ObserveHTTP(seconds float64)      { m.observe(m.httpDuration, seconds) }

func (m *Metrics) observe(h *histogram, value float64) {
	if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return
	}
	m.mu.Lock()
	h.observe(value)
	m.mu.Unlock()
}

func writeMetricHelpType(b *strings.Builder, name, help, metricType string) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, metricType)
}

func writeHistogram(b *strings.Builder, name, help string, h *histogram, labels string) {
	writeMetricHelpType(b, name, help, "histogram")
	suffix := ""
	if labels != "" {
		suffix = "," + labels
	}
	for i, bucket := range h.buckets {
		fmt.Fprintf(b, "%s_bucket{le=%q%s} %d\n", name, strconv.FormatFloat(bucket, 'f', -1, 64), suffix, h.counts[i])
	}
	fmt.Fprintf(b, "%s_bucket{le=\"+Inf\"%s} %d\n", name, suffix, h.count)
	if labels == "" {
		fmt.Fprintf(b, "%s_sum %.9f\n%s_count %d\n", name, h.sum, name, h.count)
	} else {
		fmt.Fprintf(b, "%s_sum{%s} %.9f\n%s_count{%s} %d\n", name, labels, h.sum, name, labels, h.count)
	}
}

func (m *Metrics) Prometheus(instanceID string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	labels := fmt.Sprintf("instance_id=%q", instanceID)
	var b strings.Builder
	writeMetricHelpType(&b, "livecollab_websocket_connections", "Current active WebSocket connections.", "gauge")
	fmt.Fprintf(&b, "livecollab_websocket_connections{%s} %d\n", labels, atomic.LoadInt64(&m.websocketConnections))

	counters := []struct {
		name string
		help string
		val  uint64
	}{
		{"livecollab_operations_total", "Content operations received by this backend.", atomic.LoadUint64(&m.operationsTotal)},
		{"livecollab_operation_errors_total", "Content operations that failed due to storage or processing errors.", atomic.LoadUint64(&m.operationErrors)},
		{"livecollab_stale_write_rejections_total", "Stale full-document writes rejected by optimistic concurrency control.", atomic.LoadUint64(&m.staleWriteRejections)},
		{"livecollab_reconnects_total", "WebSocket connections that supplied a resume cursor.", atomic.LoadUint64(&m.reconnectsTotal)},
		{"livecollab_resume_success_total", "Reconnect resumes completed successfully.", atomic.LoadUint64(&m.resumeSuccessTotal)},
		{"livecollab_resume_failures_total", "Reconnect resumes that failed.", atomic.LoadUint64(&m.resumeFailuresTotal)},
		{"livecollab_replayed_events_total", "Events replayed to reconnecting clients.", atomic.LoadUint64(&m.replayedEventsTotal)},
		{"livecollab_sequence_gaps_total", "Detected non-contiguous event sequence gaps.", atomic.LoadUint64(&m.sequenceGapsTotal)},
		{"livecollab_stream_events_total", "Redis Stream events applied by this backend.", atomic.LoadUint64(&m.streamEventsTotal)},
		{"livecollab_redis_errors_total", "Redis command failures observed by this backend.", atomic.LoadUint64(&m.redisErrorsTotal)},
		{"livecollab_http_requests_total", "HTTP requests observed by this backend.", atomic.LoadUint64(&m.httpRequestsTotal)},
	}
	for _, counter := range counters {
		writeMetricHelpType(&b, counter.name, counter.help, "counter")
		fmt.Fprintf(&b, "%s{%s} %d\n", counter.name, labels, counter.val)
	}

	writeHistogram(&b, "livecollab_operation_duration_seconds", "Server-side duration of a content operation through durable apply and acknowledgement.", m.operationDuration, labels)
	writeHistogram(&b, "livecollab_redis_command_duration_seconds", "Duration of instrumented Redis commands.", m.redisDuration, labels)
	writeHistogram(&b, "livecollab_stream_propagation_lag_seconds", "Time from Redis Stream append to another backend applying the event.", m.streamLag, labels)
	writeHistogram(&b, "livecollab_resume_duration_seconds", "Duration of a reconnect replay and convergence cycle.", m.resumeDuration, labels)
	writeHistogram(&b, "livecollab_http_request_duration_seconds", "Duration of HTTP requests served by the backend.", m.httpDuration, labels)
	return b.String()
}

type JSONLogger struct {
	enabled    bool
	instanceID string
}

func NewJSONLogger(enabled bool, instanceID string) *JSONLogger {
	return &JSONLogger{enabled: enabled, instanceID: instanceID}
}

func (l *JSONLogger) Event(level, event string, fields map[string]any) {
	if l == nil || !l.enabled {
		return
	}
	payload := map[string]any{
		"timestamp":  time.Now().UTC().Format(time.RFC3339Nano),
		"level":      level,
		"event":      event,
		"instanceId": l.instanceID,
	}
	for key, value := range fields {
		payload[key] = value
	}
	encoded, err := json.Marshal(payload)
	if err == nil {
		log.Print(string(encoded))
	}
}

type Span struct {
	TraceID    string
	SpanID     string
	ParentID   string
	Name       string
	Start      time.Time
	End        time.Time
	Attributes map[string]any
	Error      string
}

type OTLPExporter struct {
	endpoint    string
	serviceName string
	instanceID  string
	client      *http.Client
}

func NewOTLPExporter(endpoint, serviceName, instanceID string) *OTLPExporter {
	return &OTLPExporter{
		endpoint: strings.TrimRight(endpoint, "/"), serviceName: serviceName, instanceID: instanceID,
		client: &http.Client{Timeout: 2 * time.Second},
	}
}

func randomHex(bytesLen int) string {
	buffer := make([]byte, bytesLen)
	if _, err := rand.Read(buffer); err != nil {
		return strings.Repeat("0", bytesLen*2)
	}
	return hex.EncodeToString(buffer)
}

func NewTraceID() string { return randomHex(16) }
func NewSpanID() string  { return randomHex(8) }

func (e *OTLPExporter) Export(span Span) {
	if e == nil || e.endpoint == "" {
		return
	}
	if span.End.IsZero() {
		span.End = time.Now()
	}

	attrs := make([]map[string]any, 0, len(span.Attributes))
	keys := make([]string, 0, len(span.Attributes))
	for key := range span.Attributes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		attrs = append(attrs, map[string]any{"key": key, "value": otlpValue(span.Attributes[key])})
	}
	status := map[string]any{"code": 1}
	if span.Error != "" {
		status = map[string]any{"code": 2, "message": span.Error}
	}
	otlpSpan := map[string]any{
		"traceId":           span.TraceID,
		"spanId":            span.SpanID,
		"name":              span.Name,
		"kind":              2,
		"startTimeUnixNano": strconv.FormatInt(span.Start.UnixNano(), 10),
		"endTimeUnixNano":   strconv.FormatInt(span.End.UnixNano(), 10),
		"attributes":        attrs,
		"status":            status,
	}
	if span.ParentID != "" {
		otlpSpan["parentSpanId"] = span.ParentID
	}

	body := map[string]any{
		"resourceSpans": []any{map[string]any{
			"resource": map[string]any{"attributes": []any{
				map[string]any{"key": "service.name", "value": map[string]any{"stringValue": e.serviceName}},
				map[string]any{"key": "service.instance.id", "value": map[string]any{"stringValue": e.instanceID}},
			}},
			"scopeSpans": []any{map[string]any{
				"scope": map[string]any{"name": "livecollab.manual-otlp", "version": "3.0.0"},
				"spans": []any{otlpSpan},
			}},
		}},
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return
	}
	go func() {
		req, err := http.NewRequest(http.MethodPost, e.endpoint+"/v1/traces", bytes.NewReader(encoded))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		response, err := e.client.Do(req)
		if err != nil {
			return
		}
		_ = response.Body.Close()
	}()
}

func otlpValue(value any) map[string]any {
	switch typed := value.(type) {
	case bool:
		return map[string]any{"boolValue": typed}
	case int:
		return map[string]any{"intValue": strconv.Itoa(typed)}
	case int64:
		return map[string]any{"intValue": strconv.FormatInt(typed, 10)}
	case uint64:
		return map[string]any{"intValue": strconv.FormatUint(typed, 10)}
	case float64:
		return map[string]any{"doubleValue": typed}
	default:
		return map[string]any{"stringValue": fmt.Sprint(value)}
	}
}

type Telemetry struct {
	Metrics *Metrics
	Logger  *JSONLogger
	Tracer  *OTLPExporter
}

func NewTelemetry(config Config) *Telemetry {
	return &Telemetry{
		Metrics: NewMetrics(),
		Logger:  NewJSONLogger(config.JSONLogs, config.InstanceID),
		Tracer:  NewOTLPExporter(config.OTLPEndpoint, config.ServiceName, config.InstanceID),
	}
}
