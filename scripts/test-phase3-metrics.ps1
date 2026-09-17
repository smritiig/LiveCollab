$ErrorActionPreference = "Stop"

$required = @(
    "livecollab_websocket_connections",
    "livecollab_operations_total",
    "livecollab_redis_command_duration_seconds_bucket",
    "livecollab_stream_propagation_lag_seconds_bucket",
    "livecollab_resume_success_total",
    "livecollab_sequence_gaps_total"
)

foreach ($port in @(8081, 8082)) {
    $metrics = (Invoke-WebRequest -UseBasicParsing -Uri "http://localhost:$port/metrics").Content
    foreach ($metric in $required) {
        if (-not $metrics.Contains($metric)) {
            throw "Backend on port $port is missing metric: $metric"
        }
    }
    Write-Host "PASS backend port $port exposes Phase 3 metrics" -ForegroundColor Green
}

$prometheus = Invoke-RestMethod -Uri "http://localhost:9090/api/v1/targets"
$healthyTargets = @($prometheus.data.activeTargets | Where-Object { $_.health -eq "up" -and $_.labels.job -eq "livecollab-backends" })
if ($healthyTargets.Count -lt 2) {
    throw "Expected two healthy LiveCollab Prometheus targets, found $($healthyTargets.Count)."
}
Write-Host "PASS Prometheus is scraping both backends" -ForegroundColor Green

$tempoReady = Invoke-WebRequest -UseBasicParsing -Uri "http://localhost:3200/ready"
if ($tempoReady.StatusCode -ne 200) {
    throw "Tempo readiness check failed with HTTP $($tempoReady.StatusCode)."
}
Write-Host "PASS Tempo is ready" -ForegroundColor Green

$grafanaHealth = Invoke-RestMethod -Uri "http://localhost:3000/api/health"
if ($grafanaHealth.database -ne "ok") {
    throw "Grafana health check failed."
}
Write-Host "PASS Grafana is healthy" -ForegroundColor Green

Write-Host "Phase 3 observability smoke test PASSED" -ForegroundColor Cyan
