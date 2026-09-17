$ErrorActionPreference = "Stop"

docker compose up -d redis
$env:LIVECOLLAB_REDIS_ADDR = "127.0.0.1:6379"
try {
    npm run test:phase2
}
finally {
    Remove-Item Env:LIVECOLLAB_REDIS_ADDR -ErrorAction SilentlyContinue
}
