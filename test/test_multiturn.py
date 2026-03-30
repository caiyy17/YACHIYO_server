#!/usr/bin/env python3
"""Quick multi-turn test for any lorebook."""
import json, os, sys, random, re, argparse
from datetime import datetime
from openai import OpenAI

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.settings import get_setting, get_secret
from Modules.llm_utils.Tools import resolve_variables

ACTION_LIST = set("站立、轻微摇摆、摇摆、东张西望、眺望、蹦跳、走来走去、扭腰、背手、双手背后、叉腰、合十、挥手、点头、摇头、低头、抬头、歪头、歪头晃、指、招手、双手招手、挥手告别、举手、摊手、摆手、拥抱、鼓掌、比耶、遮眼、转圈、鞠躬、伸展、半蹲、跳、跺脚、捂脸、缩起来、嘘、哈欠、转圈舞、跳舞".split("、"))
EXPR_LIST = set("眨眼、开心、温柔、委屈、惊讶、怀疑、坚定、呆住、恍惚、走神、恐惧、黑化、无神、无聊、星星眼、爱心眼、感兴趣、警觉、张嘴、大张嘴、咧嘴、嘟嘴、微张嘴、拉长脸、嘴巴圆、兴奋、猫嘴、不开心、哼、坏笑、微笑、愉快、沮丧、尴尬笑、吐舌头、调皮吐舌、严肃、困扰、生气、皱眉、流泪、害羞、脸色发白".split("、"))

PROMPT_SETS = {
    "set_a": [
        "嗨，在干嘛",
        "你最近在读什么书",
        "外面好冷啊",
        "我今天加班到现在才回来",
        "你喜欢吃什么",
        "给我讲个你最喜欢的故事",
        "你觉得人活着是为了什么",
        "我要去睡了，晚安",
    ],
    "set_b": [
        "早上好",
        "今天天气怎么样",
        "我好累啊",
        "你有什么烦恼吗",
        "推荐个电影给我",
        "你会跳舞吗",
        "我喜欢你",
        "拜拜",
    ],
    "set_c": [
        "在吗",
        "你吃饭了没",
        "我刚被老板骂了",
        "陪我聊聊天",
        "你觉得什么样的人最讨厌",
        "我出门散步去了",
        "路上看到一只猫",
        "回来了，困死了",
    ],
}


def load_lorebook(name):
    with open(f"configs/lorebooks/{name}.json", encoding="utf-8") as f:
        return json.load(f)["data"]


def get_pipeline_vars(lorebook_name):
    """Try to find pipeline config to get vars."""
    candidates = [
        f"configs/{lorebook_name}_text.json",
        f"configs/dev_text.json",
    ]
    for path in candidates:
        if os.path.exists(path):
            with open(path) as f:
                cfg = json.load(f)
            for node in cfg["pipeline"]:
                if "vars" in node.get("config", {}):
                    return node["config"]["vars"]
    return {"location": "Tokyo"}


def assemble_messages(lorebook_data, history, current_input, static_vars):
    pos_0, pos_neg1 = [], []
    for item in lorebook_data:
        if item["strategy"] == "constant":
            prob = item.get("probability", 1.0)
            if random.random() >= prob:
                continue
        content = item["content"]
        if "{{" in content:
            content = resolve_variables(content, static_vars)
        entry = {"role": item["role"], "content": content}
        order = item.get("order", 0)
        if item["position"] == 0:
            pos_0.append((order, entry))
        else:
            pos_neg1.append((order, entry))
    pos_0.sort(key=lambda x: x[0])
    pos_neg1.sort(key=lambda x: x[0])
    messages = [e for _, e in pos_0]
    messages.extend(history)
    if current_input:
        messages.append({"role": "user", "content": current_input})
    messages.extend([e for _, e in pos_neg1])
    return messages


def check_format(history, lorebook_name):
    """Check action/expression list compliance. unity_chan has free-form actions."""
    if lorebook_name == "unity_chan":
        # unity_chan uses free-form [action](expression)
        print("  (unity_chan uses free-form actions/expressions, skipping list check)")
        return
    violations = 0
    for entry in history:
        if entry["role"] == "assistant":
            content = entry["content"]
            actions = re.findall(r'\[([^\]]+)\]', content)
            exprs = re.findall(r'\(([^)]+)\)', content)
            for a in actions:
                if a not in ACTION_LIST:
                    print(f"  ACTION VIOLATION: [{a}]")
                    violations += 1
            for e in exprs:
                if e not in EXPR_LIST:
                    print(f"  EXPRESSION VIOLATION: ({e})")
                    violations += 1
    if violations == 0:
        print("  All tags within allowed lists!")
    else:
        print(f"  {violations} violation(s) found")


def run_test(lorebook_name, prompt_set="set_a", model_name="dev_llm"):
    with open(f"configs/llm/{model_name}.json") as f:
        model_config = json.load(f)
    api_base = get_setting("llm", model_config["api_base"])
    api_key = get_secret(model_config["api_key"])
    client = OpenAI(base_url=api_base, api_key=api_key)

    lorebook_data = load_lorebook(lorebook_name)
    static_vars = get_pipeline_vars(lorebook_name)
    prompts = PROMPT_SETS[prompt_set]

    print(f"=== {lorebook_name} Multi-turn | {prompt_set} | {datetime.now().strftime('%Y-%m-%d %H:%M')} ===")
    print()

    history = []
    for i, prompt in enumerate(prompts, 1):
        assembled = assemble_messages(lorebook_data, history, prompt, static_vars)
        extra = model_config.get("extra", {}).copy()
        response = client.chat.completions.create(
            model=model_config["model_name"],
            messages=assembled,
            stream=False,
            **extra,
        )
        text = response.choices[0].message.content
        print(f"[{i}] User: {prompt}")
        print(f"    Reply: {text}")
        print()
        history.append({"role": "user", "content": prompt})
        history.append({"role": "assistant", "content": text})

    print("=== Format Check ===")
    check_format(history, lorebook_name)
    print("=== Done ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("lorebook", help="Lorebook name (dev_default, sora, unity_chan, dev_ch2)")
    parser.add_argument("--set", default="set_a", choices=PROMPT_SETS.keys())
    parser.add_argument("--model", default="dev_llm")
    args = parser.parse_args()
    run_test(args.lorebook, args.set, args.model)
