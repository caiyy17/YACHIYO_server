#!/bin/zsh

# Kill existing screen sessions with the given name
kill_screen_if_exists() {
    screen_name=$1
    screen -ls | grep "$screen_name" > /dev/null
    if [ $? -eq 0 ]; then
        echo "Killing screen session: $screen_name"
        screen -S "$screen_name" -X quit
    fi
}

# Stop pipeline server (does not stop model services)
kill_screen_if_exists "yachio"
kill_screen_if_exists "webrtc"

echo "Pipeline server stopped."
echo "Model services (asr, llm, tts, database) are not managed by this script."
