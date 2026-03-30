#!/usr/bin/env python3
"""Multi-turn test for prompt iteration."""
import json, os, sys, random, re
from datetime import datetime
from openai import OpenAI

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.settings import get_setting, get_secret
from Modules.llm_utils.Tools import resolve_variables

LOREBOOK = sys.argv[1] if len(sys.argv) > 1 else "dev_smpl"

with open(f"configs/lorebooks/{LOREBOOK}.json", encoding="utf-8") as f:
    lorebook_data = json.load(f)["data"]
with open("configs/llm/dev_llm.json") as f:
    model_config = json.load(f)
api_base = get_setting("llm", model_config["api_base"])
api_key = get_secret(model_config["api_key"])
client = OpenAI(base_url=api_base, api_key=api_key)
static_vars = {"location": "Tokyo"}

def resolve_content(content):
    if "{{" in content:
        return resolve_variables(content, static_vars)
    return content

def assemble(history, user_input):
    pos_0, pos_neg1 = [], []
    for item in lorebook_data:
        if item["strategy"] == "constant":
            if random.random() >= item.get("probability", 1.0):
                continue
        entry = {"role": item["role"], "content": resolve_content(item["content"])}
        order = item.get("order", 0)
        (pos_0 if item["position"] == 0 else pos_neg1).append((order, entry))
    pos_0.sort(key=lambda x: x[0])
    pos_neg1.sort(key=lambda x: x[0])
    msgs = [e for _, e in pos_0] + history
    if user_input:
        msgs.append({"role": "user", "content": user_input})
    msgs += [e for _, e in pos_neg1]
    return msgs

prompts = [
    "早上好", "你好啊", "外面好冷", "我今天好累",
    "我出门了", "你喜欢吃什么", "给我讲个故事", "我喜欢你",
]

print(f"=== {LOREBOOK} | {datetime.now().strftime('%Y-%m-%d %H:%M')} ===\n")

history = []
for i, p in enumerate(prompts, 1):
    msgs = assemble(history, p)
    extra = model_config.get("extra", {}).copy()
    resp = client.chat.completions.create(
        model=model_config["model_name"], messages=msgs, stream=False, **extra
    )
    text = resp.choices[0].message.content
    print(f"[{i}] User: {p}")
    print(f"    Reply: {text}")
    print()
    history.append({"role": "user", "content": p})
    history.append({"role": "assistant", "content": text})
