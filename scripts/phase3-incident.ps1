param(
    [int]$DelayMs = 350,
    [int]$DurationSeconds = 45
)

$ErrorActionPreference = "Stop"

if ($DelayMs -lt 0 -or $DelayMs -gt 5000) {
    throw "DelayMs must be between 0 and 5000."
}

function Set-RedisDelay([string]$BaseUrl, [int]$Milliseconds) {
    Invoke-RestMethod -Method Post -Uri "$BaseUrl/debug/faults/redis-delay?ms=$Milliseconds" | Out-Null
}

Write-Host "LiveCollab Phase 3 incident: Redis latency injection" -ForegroundColor Cyan
Write-Host "Injecting ${DelayMs}ms artificial Redis latency on both backend instances." -ForegroundColor Yellow

Set-RedisDelay "http://localhost:8081" $DelayMs
Set-RedisDelay "http://localhost:8082" $DelayMs

try {
    $room = Invoke-RestMethod -Method Post -Uri "http://localhost:8081/rooms"
    $roomId = $room.roomId
    Write-Host "Created incident room: $roomId"
    Write-Host "Generating Redis-backed room reads for ${DurationSeconds}s..."

    $deadline = (Get-Date).AddSeconds($DurationSeconds)
    $requests = 0
    while ((Get-Date) -lt $deadline) {
        try {
            Invoke-RestMethod -Method Get -Uri "http://localhost:8082/rooms/check?roomId=$roomId" | Out-Null
            $requests++
        }
        catch {
            Write-Warning $_.Exception.Message
        }
    }

    Write-Host "Generated $requests slow Redis-backed requests." -ForegroundColor Green
    Write-Host ""
    Write-Host "Inspect:" -ForegroundColor Cyan
    Write-Host "  Grafana dashboard : http://localhost:3000/d/livecollab-overview/livecollab-reliability-overview"
    Write-Host "  Prometheus alerts : http://localhost:9090/alerts"
    Write-Host "  Tempo via Grafana : Explore -> Tempo -> search service livecollab-backend"
    Write-Host "  JSON logs         : docker compose logs backend-a backend-b --tail 100"
    Write-Host ""
    Write-Host "Expected signals:" -ForegroundColor Cyan
    Write-Host "  livecollab_redis_command_duration_seconds p95 > 250ms"
    Write-Host "  LiveCollabHighRedisLatency alert transitions to firing after 30s"
    Write-Host "  HTTP room-check traces become visibly slower"
    Write-Host ""
    Write-Host "Waiting 20s so Prometheus can evaluate the alert before recovery..."
    Start-Sleep -Seconds 20
}
finally {
    Write-Host "Removing injected Redis latency." -ForegroundColor Yellow
    Set-RedisDelay "http://localhost:8081" 0
    Set-RedisDelay "http://localhost:8082" 0
    Write-Host "Recovery complete. Watch the dashboard return toward baseline." -ForegroundColor Green
}
