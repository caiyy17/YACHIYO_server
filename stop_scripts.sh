#!/bin/zsh

# 定义一个函数用于杀掉同名的 screen 会话
kill_screen_if_exists() {
    screen_name=$1
    screen -ls | grep "$screen_name" > /dev/null
    if [ $? -eq 0 ]; then
        echo "Killing existing screen session: $screen_name"
        screen -S "$screen_name" -X quit
    fi
}

# 获取当前脚本所在的目录
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# 检查并杀掉已有的 screen1, screen2, screen3 会话
kill_screen_if_exists "yyassistant"
kill_screen_if_exists "asr"
kill_screen_if_exists "tts"
kill_screen_if_exists "motion"
kill_screen_if_exists "llm"