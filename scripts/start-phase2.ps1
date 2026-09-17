$ErrorActionPreference = "Stop"

Write-Host "Starting LiveCollab Phase 2 distributed backend..."
docker compose up --build -d

docker compose ps

Write-Host ""
Write-Host "Gateway:   http://localhost:8080"
Write-Host "Backend A: http://localhost:8081"
Write-Host "Backend B: http://localhost:8082"
Write-Host "Redis:     localhost:6379"
Write-Host ""
Write-Host "Start the React frontend in another PowerShell window:"
Write-Host "  cd frontend"
Write-Host "  npm install"
Write-Host "  npm run dev"
