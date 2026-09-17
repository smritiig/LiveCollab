$ErrorActionPreference = "Stop"

Write-Host "Starting LiveCollab Phase 3..." -ForegroundColor Cyan
docker compose up --build -d

docker compose ps

Write-Host ""
Write-Host "LiveCollab gateway : http://localhost:8080" -ForegroundColor Green
Write-Host "Grafana            : http://localhost:3000" -ForegroundColor Green
Write-Host "Prometheus         : http://localhost:9090" -ForegroundColor Green
Write-Host "Tempo API          : http://localhost:3200" -ForegroundColor Green
Write-Host "OTLP/HTTP          : http://localhost:4318" -ForegroundColor Green
Write-Host ""
Write-Host "Start the React development UI separately:" -ForegroundColor Yellow
Write-Host "  cd frontend"
Write-Host "  npm install"
Write-Host "  npm run dev"
