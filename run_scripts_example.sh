#!/bin/zsh

# Kill existing screen sessions with the given name
kill_screen_if_exists() {
    screen_name=$1
    screen -ls | grep "$screen_name" > /dev/null
    if [ $? -eq 0 ]; then
        echo "Killing existing screen session: $screen_name"
        screen -S "$screen_name" -X quit
    fi
}

# Wait for a service port to be ready
wait_for_port() {
    local port=$1
    local name=$2
    local timeout=${3:-120}
    local elapsed=0
    echo "Waiting for $name (port $port) to be ready..."
    while ! ss -tlnp 2>/dev/null | grep -q ":$port "; do
        sleep 2
        elapsed=$((elapsed + 2))
        if [ $elapsed -ge $timeout ]; then
            echo "WARNING: $name (port $port) not ready after ${timeout}s, continuing anyway"
            return 1
        fi
    done
    echo "$name (port $port) is ready (${elapsed}s)"
    return 0
}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# ============================================================
# This script starts only the pipeline server itself.
# All model services (ASR, LLM, TTS, VectorDatabase) must be
# started separately. See Modules_standalone/*/README.md.
# ============================================================

kill_screen_if_exists "yachio"
kill_screen_if_exists "webrtc"

echo "Starting pipeline server..."

# 1. Main server - port 8910
echo "[1/2] Starting YACHIO server..."
screen -dmS yachio zsh -c "source ~/.zshrc; conda activate yachio && uvicorn server_fastapi:app --reload --host 0.0.0.0 --port 8910; exec zsh"
wait_for_port 8910 "YACHIO"

# 2. WebRTC server - port 15168
echo "[2/2] Starting WebRTC server..."
screen -dmS webrtc zsh -c "source ~/.zshrc; conda activate yachio && python server_webrtc.py --port 15168; exec zsh"
wait_for_port 15168 "WebRTC"

echo ""
echo "Pipeline server started. Make sure model services are running:"
echo "  - ASR:            see Modules_standalone/QwenASR/README.md"
echo "  - LLM:            see Modules_standalone/VLLM/README.md"
echo "  - TTS:            see Modules_standalone/QwenTTS/README.md"
echo "  - VectorDatabase: see Modules_standalone/VectorDatabase/README.md"
