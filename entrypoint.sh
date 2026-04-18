#!/bin/bash

# Start FlareSolverr in the background
echo "🚀 Starting FlareSolverr..."
cd /app/flaresolverr && node dist/index.js &

# Start Byparr in the background
echo "🛡️ Starting Byparr..."
/app/byparr/byparr &

# Start EasyProxy (Gunicorn)
echo "🎬 Starting EasyProxy..."
cd /app
WORKERS_COUNT=${WORKERS:-$(nproc 2>/dev/null || echo 1)}
xvfb-run -a --server-args='-screen 0 1366x768x24' gunicorn --bind 0.0.0.0:${PORT:-7860} --workers $WORKERS_COUNT --worker-class aiohttp.worker.GunicornWebWorker --timeout 120 --graceful-timeout 120 app:app
