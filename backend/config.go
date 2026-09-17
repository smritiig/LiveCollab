package main

import (
	"os"
	"strconv"
	"strings"
)

type StaleWritePolicy string

const (
	StaleWriteReject StaleWritePolicy = "reject"
	StaleWriteAccept StaleWritePolicy = "accept"
)

type Config struct {
	Port              string
	AllowedOrigins    []string
	StaleWritePolicy  StaleWritePolicy
	TraceFile         string
	RedisAddr         string
	InstanceID        string
	ServiceName       string
	OTLPEndpoint      string
	JSONLogs          bool
	EnableDebugFaults bool
	RedisDelayMs      int64
}

func LoadConfig() Config {
	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = "8080"
	}

	origins := splitAndTrim(os.Getenv("ALLOWED_ORIGINS"))
	if len(origins) == 0 {
		origins = []string{"http://localhost:5173", "http://127.0.0.1:5173"}
	}

	policy := StaleWritePolicy(strings.ToLower(strings.TrimSpace(os.Getenv("LIVECOLLAB_STALE_WRITE_POLICY"))))
	if policy != StaleWriteAccept && policy != StaleWriteReject {
		policy = StaleWriteReject
	}

	instanceID := strings.TrimSpace(os.Getenv("LIVECOLLAB_INSTANCE_ID"))
	if instanceID == "" {
		hostname, _ := os.Hostname()
		if hostname == "" {
			hostname = "local"
		}
		instanceID = hostname + ":" + port
	}

	serviceName := strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME"))
	if serviceName == "" {
		serviceName = "livecollab-backend"
	}

	return Config{
		Port:              port,
		AllowedOrigins:    origins,
		StaleWritePolicy:  policy,
		TraceFile:         strings.TrimSpace(os.Getenv("LIVECOLLAB_TRACE_FILE")),
		RedisAddr:         strings.TrimSpace(os.Getenv("LIVECOLLAB_REDIS_ADDR")),
		InstanceID:        instanceID,
		ServiceName:       serviceName,
		OTLPEndpoint:      strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")),
		JSONLogs:          envBool("LIVECOLLAB_JSON_LOGS", true),
		EnableDebugFaults: envBool("LIVECOLLAB_ENABLE_DEBUG_FAULTS", false),
		RedisDelayMs:      envInt64("LIVECOLLAB_REDIS_DELAY_MS", 0),
	}
}

func splitAndTrim(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func originAllowed(origin string, allowed []string) bool {
	if origin == "" {
		return true
	}
	for _, candidate := range allowed {
		if candidate == "*" || candidate == origin {
			return true
		}
	}
	return false
}

func envBool(name string, fallback bool) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envInt64(name string, fallback int64) int64 {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}
