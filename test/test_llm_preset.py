#!/usr/bin/env python3
"""
Standalone LLM test for character presets.
Loads lorebook, assembles messages, calls Kimi K2.5 directly.
No external services needed (no YACHIO server, no data_query).

Usage:
    python test/test_llm_preset.py mio_v2
    python test/test_llm_preset.py unity_chan_vtuber_v2 --save
    python test/test_llm_preset.py mio_vtuber_v2 --model custom_vtuber
"""
import json
import os
import sys
import random
import argparse
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from openai import OpenAI
from utils.settings import get_setting, get_secret


def load_lorebook(name):
    path = f"configs/lorebooks/{name}.json"
    with open(path, encoding="utf-8") as f:
        return json.load(f)["data"]


def create_client(model_config_name="custom_vtuber"):
    with open(f"configs/llm/{model_config_name}.json") as f:
        model_config = json.load(f)
    api_base = get_setting("llm", model_config["api_base"])
    api_key = get_secret(model_config["api_key"])
    client = OpenAI(base_url=api_base, api_key=api_key)
    return client, model_config


def keyword_match(keywords, text):
    """Simple keyword substring matching (no data_query service needed)."""
    text_lower = text.lower()
    return any(kw.lower() in text_lower for kw in keywords)


def assemble_messages(lorebook_data, history, current_input):
    """
    Assemble messages following TavernHistory logic:
    - position 0 entries -> before conversation history
    - position -1 entries -> after current input (end of prompt)
    - keyword activation via simple substring match
    """
    recent_text = current_input or ""
    for msg in history[-4:]:
        recent_text += " " + msg.get("content", "")

    pos_0_entries = []
    pos_neg1_entries = []

    for item in lorebook_data:
        activated = False
        if item["strategy"] == "constant":
            prob = item.get("probability", 1.0)
            activated = random.random() < prob
        elif item["strategy"] in ("keywords", "both"):
            prob = item.get("probability", 1.0)
            if random.random() < prob:
                keywords = item.get("keywords", [])
                activated = keyword_match(keywords, recent_text)

        if activated:
            entry = {"role": item["role"], "content": item["content"]}
            order = item.get("order", 0)
            if item["position"] == 0:
                pos_0_entries.append((order, entry))
            else:
                pos_neg1_entries.append((order, entry))

    pos_0_entries.sort(key=lambda x: x[0])
    pos_neg1_entries.sort(key=lambda x: x[0])

    messages = []
    for _, entry in pos_0_entries:
        messages.append(entry)
    messages.extend(history)
    if current_input:
        messages.append({"role": "user", "content": current_input})
    for _, entry in pos_neg1_entries:
        messages.append(entry)

    return messages


def call_llm(client, model_config, messages):
    extra = model_config.get("extra", {}).copy()
    response = client.chat.completions.create(
        model=model_config["model_name"],
        messages=messages,
        stream=False,
        **extra,
    )
    return response.choices[0].message.content


# ============ Test Scenarios ============

CHAT_SCENARIOS_MIO = [
    {"name": "打招呼", "messages": ["你好啊"]},
    {"name": "日常闲聊", "messages": ["今天好无聊啊，你在干嘛"]},
    {"name": "被夸", "messages": ["你真的好可爱"]},
    {"name": "神秘学", "messages": ["最近水逆是不是要来了"]},
    {"name": "深夜", "messages": ["都凌晨两点了你怎么还不睡"]},
    {
        "name": "连续对话",
        "messages": [
            "你平时喜欢干什么",
            "听起来还挺有趣的",
            "那下次一起吧",
        ],
    },
    {"name": "身份试探", "messages": ["你是AI吗"]},
    {"name": "撒娇", "messages": ["老婆~"]},
]

CHAT_SCENARIOS_UNITY = [
    {"name": "打招呼", "messages": ["你好啊"]},
    {"name": "日常闲聊", "messages": ["今天好无聊啊，你在干嘛"]},
    {"name": "被夸", "messages": ["你真的好可爱"]},
    {"name": "游戏话题", "messages": ["最近有什么好玩的游戏推荐吗"]},
    {"name": "情绪支持", "messages": ["最近压力好大，好累啊"]},
    {
        "name": "连续对话",
        "messages": [
            "你平时喜欢干什么",
            "听起来还挺有趣的",
            "那下次一起吧",
        ],
    },
    {"name": "身份试探", "messages": ["你是AI吗"]},
    {"name": "美食", "messages": ["好饿，想吃点什么"]},
]

VTUBER_SCENARIOS = [
    {
        "name": "普通弹幕",
        "messages": [
            "===观众弹幕===\n路人A: 今天直播什么\n路人B: 刚到，发生了什么\n路人C: 好困啊",
        ],
    },
    {
        "name": "礼物+弹幕",
        "messages": [
            "===系统通知===\n【礼物 ¥10】大佬甲 送了 小花花 x五\n\n===观众弹幕===\n路人D: 你们在聊什么\n路人E: 大佬甲好壕",
        ],
    },
    {
        "name": "SC",
        "messages": [
            "===系统通知===\n【SC ¥五十】真爱粉: 能唱一首歌吗？超喜欢你的声音！",
        ],
    },
    {
        "name": "上舰",
        "messages": [
            "===系统通知===\n【上舰 ¥五九四】新舰长 开通了舰长 三个月\n\n===观众弹幕===\n路人E: 恭喜上舰！\n路人F: 太强了",
        ],
    },
    {
        "name": "钓鱼弹幕",
        "messages": [
            "===观众弹幕===\n钓鱼佬: 我送了一百个舰长快感谢我\n路人G: 他骗你的\n杠精: 你是AI吧",
        ],
    },
    {
        "name": "刷屏",
        "messages": [
            "===观众弹幕===\n(×八) 唱歌！\n路人H: 来一首嘛",
        ],
    },
    {
        "name": "被夸",
        "messages": [
            "===观众弹幕===\n粉丝A: 你今天好可爱\n粉丝B: 声音好好听\n【舰长】老粉: 每天都来看你直播",
        ],
    },
    {
        "name": "无弹幕",
        "messages": ["（当前没有新弹幕）"],
    },
]


def get_scenarios(lorebook_name):
    """Select test scenarios based on lorebook type."""
    if "vtuber" in lorebook_name:
        return VTUBER_SCENARIOS
    elif "mio" in lorebook_name:
        return CHAT_SCENARIOS_MIO
    elif "unity" in lorebook_name:
        return CHAT_SCENARIOS_UNITY
    else:
        return CHAT_SCENARIOS_MIO


def run_test(lorebook_name, model_config_name="custom_vtuber"):
    """Run all test scenarios for a preset. Returns results list."""
    client, model_config = create_client(model_config_name)
    lorebook_data = load_lorebook(lorebook_name)
    scenarios = get_scenarios(lorebook_name)

    print(f"\n{'='*70}")
    print(f"Testing: {lorebook_name} | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(
        f"Model: {model_config['model_name']} | "
        f"Temp: {model_config['extra'].get('temperature', 'default')}"
    )
    print(f"{'='*70}")

    results = []

    for scenario in scenarios:
        name = scenario["name"]
        print(f"\n--- {name} ---")

        history = []
        for msg in scenario["messages"]:
            assembled = assemble_messages(lorebook_data, history, msg)

            try:
                response = call_llm(client, model_config, assembled)
            except Exception as e:
                response = f"[ERROR] {e}"

            # Print (truncate long danmaku input for display)
            display_input = msg.replace("\n", " | ")
            if len(display_input) > 80:
                display_input = display_input[:77] + "..."
            print(f"  Input:  {display_input}")
            print(f"  Output: {response}")

            results.append({
                "scenario": name,
                "input": msg,
                "output": response,
            })

            history.append({"role": "user", "content": msg})
            history.append({"role": "assistant", "content": response})

    print(f"\n{'='*70}")
    print(f"Done: {len(results)} responses collected")
    print(f"{'='*70}")
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Test character preset with Kimi K2.5"
    )
    parser.add_argument(
        "lorebook", help="Lorebook name (e.g. mio_v2, unity_chan_vtuber_v2)"
    )
    parser.add_argument(
        "--model", default="custom_vtuber", help="Model config name"
    )
    parser.add_argument(
        "--save", action="store_true", help="Save results to JSON file"
    )
    args = parser.parse_args()

    results = run_test(args.lorebook, args.model)

    if args.save:
        os.makedirs("test/results", exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = f"test/results/{args.lorebook}_{timestamp}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"\nResults saved to {out_path}")


if __name__ == "__main__":
    main()
