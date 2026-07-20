#!/bin/zsh

# Stop one exact screen session name.
stop_screen_if_running() {
    local screen_name=$1
    if screen -S "$screen_name" -Q select . >/dev/null 2>&1; then
        echo "Stopping screen session: $screen_name"
        screen -S "$screen_name" -X quit
    fi
}

# Stop only the main WebSocket server and WebRTC gateway.
stop_screen_if_running "yachiyo"
stop_screen_if_running "webrtc"

echo "YACHIYO and WebRTC servers stopped."
echo "Model services (asr, llm, tts, database) are not managed by this script."
