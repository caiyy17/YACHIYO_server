#!/usr/bin/env python3
"""Quick multi-turn test for Sora lorebook."""
import json, os, sys, random, re
from datetime import datetime
from openai import OpenAI

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.settings import get_setting, get_secret
from Modules.llm_utils.Tools import resolve_variables

with open("configs/lorebooks/sora.json", encoding="utf-8") as f:
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

def assemble_messages(history, current_input):
    pos_0, pos_neg1 = [], []
    for item in lorebook_data:
        if item["strategy"] == "constant":
            prob = item.get("probability", 1.0)
            if random.random() >= prob:
                continue
        entry = {"role": item["role"], "content": resolve_content(item["content"])}
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

prompts = [
    "嗨，在干嘛",
    "你最近在读什么书",
    "外面好冷啊",
    "我今天加班到现在才回来",
    "你喜欢吃什么",
    "给我讲个你最喜欢的故事",
    "你觉得人活着是为了什么",
    "我要去睡了，晚安",
]

print(f"=== Sora Multi-turn Re-test | {datetime.now().strftime('%Y-%m-%d %H:%M')} ===")
print()

history = []
for i, prompt in enumerate(prompts, 1):
    assembled = assemble_messages(history, prompt)
    extra = model_config.get("extra", {}).copy()
    response = client.chat.completions.create(
        model=model_config["model_name"],
        messages=assembled,
        stream=False,
        **extra,
    )
    text = response.choices[0].message.content
    print(f"[{i}] User: {prompt}")
    print(f"    Sora: {text}")
    print()
    history.append({"role": "user", "content": prompt})
    history.append({"role": "assistant", "content": text})

# Format check
action_list = set("站立、轻微摇摆、摇摆、东张西望、眺望、蹦跳、走来走去、扭腰、背手、双手背后、叉腰、合十、挥手、点头、摇头、低头、抬头、歪头、歪头晃、指、招手、双手招手、挥手告别、举手、摊手、摆手、拥抱、鼓掌、比耶、遮眼、转圈、鞠躬、伸展、半蹲、跳、跺脚、捂脸、缩起来、嘘、哈欠、转圈舞、跳舞".split("、"))
expr_list = set("眨眼、开心、温柔、委屈、惊讶、怀疑、坚定、呆住、恍惚、走神、恐惧、黑化、无神、无聊、星星眼、爱心眼、感兴趣、警觉、张嘴、大张嘴、咧嘴、嘟嘴、微张嘴、拉长脸、嘴巴圆、兴奋、猫嘴、不开心、哼、坏笑、微笑、愉快、沮丧、尴尬笑、吐舌头、调皮吐舌、严肃、困扰、生气、皱眉、流泪、害羞、脸色发白".split("、"))

print("=== Format Check ===")
violations = 0
for entry in history:
    if entry["role"] == "assistant":
        content = entry["content"]
        actions = re.findall(r'\[([^\]]+)\]', content)
        exprs = re.findall(r'\(([^)]+)\)', content)
        for a in actions:
            if a not in action_list:
                print(f"  ACTION VIOLATION: [{a}]")
                violations += 1
        for e in exprs:
            if e not in expr_list:
                print(f"  EXPRESSION VIOLATION: ({e})")
                violations += 1
if violations == 0:
    print("  All actions and expressions within allowed lists!")
print("=== Done ===")
