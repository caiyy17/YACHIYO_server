#!/bin/zsh

# Stop one exact screen session name.
stop_screen_if_running() {
    local screen_name=$1
    if screen -S "$screen_name" -Q select . >/dev/null 2>&1; then
        echo "Stopping existing screen session: $screen_name"
        screen -S "$screen_name" -X quit
    fi
}

# Wait until the service endpoint returns the expected response.
wait_for_service() {
    local url=$1
    local expected=$2
    local name=$3
    local screen_name=$4
    local timeout=${5:-120}
    local elapsed=0
    local response=""

    echo "Waiting for $name to be ready..."
    while [ $elapsed -lt $timeout ]; do
        if ! screen -S "$screen_name" -Q select . >/dev/null 2>&1; then
            echo "ERROR: $name process exited during startup" >&2
            return 1
        fi
        response=$(curl -fsS --max-time 2 "$url" 2>/dev/null)
        if [ $? -eq 0 ] && [[ "$response" == *"$expected"* ]]; then
            echo "$name is ready (${elapsed}s)"
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done

    echo "ERROR: $name was not ready after ${timeout}s" >&2
    return 1
}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# ============================================================
# This script starts only the main WebSocket server and WebRTC gateway.
# All model services (ASR, LLM, TTS, VectorDatabase) must be
# started separately. See Modules_standalone/*/README.md.
# ============================================================

stop_screen_if_running "yachiyo"
stop_screen_if_running "webrtc"

echo "Starting pipeline server..."

# 1. Main server - port 8910
echo "[1/2] Starting YACHIYO server..."
if ! screen -dmS yachiyo zsh -c "source ~/.zshrc; conda activate yachiyo && exec uvicorn server_fastapi:app --host 0.0.0.0 --port 8910"; then
    echo "ERROR: failed to start YACHIYO screen session" >&2
    exit 1
fi
if ! wait_for_service "http://127.0.0.1:8910/clients/" '"clients"' "YACHIYO" "yachiyo"; then
    stop_screen_if_running "yachiyo"
    exit 1
fi

# 2. WebRTC server - port 15168
echo "[2/2] Starting WebRTC server..."
if ! screen -dmS webrtc zsh -c "source ~/.zshrc; conda activate yachiyo && exec python server_webrtc.py --port 15168"; then
    echo "ERROR: failed to start WebRTC screen session" >&2
    stop_screen_if_running "yachiyo"
    exit 1
fi
if ! wait_for_service "http://127.0.0.1:15168/status" '"running"' "WebRTC" "webrtc"; then
    stop_screen_if_running "webrtc"
    stop_screen_if_running "yachiyo"
    exit 1
fi

echo ""
echo "YACHIYO and WebRTC servers started."
