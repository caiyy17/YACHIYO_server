#!/bin/bash

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
kill_screen_if_exists "t2e"
kill_screen_if_exists "rag"


# 创建 screen 会话，切换到脚本所在目录，激活 conda 环境并运行脚本
screen -dmS asr zsh -c "source ~/.zshrc; cd ../SenseVoice && mamba activate whisper && python custom_sensevoice.py; exec zsh"
screen -dmS tts zsh -c "source ~/.zshrc; cd ../XzJosh-Bert-VITS2-2.3 && mamba activate bertvits && python custom_bertvits.py; exec zsh"
screen -dmS t2e zsh -c "source ~/.zshrc; cd Standalone_services/T2E && mamba activate t2e && python custom_t2m.py; exec zsh"
screen -dmS rag zsh -c "source ~/.zshrc; cd ../motion_hint && mamba activate rag && python flask_motion.py; exec zsh"

screen -dmS yyassistant zsh -c "source ~/.zshrc; mamba activate yyassistant && python assistant_server.py; exec zsh"

echo "All scripts are running in separate screens."
