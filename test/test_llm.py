#!/usr/bin/env python3
"""
Consolidated LLM evaluation/diagnostic script.

Three modes selected via --mode:
  single       Single-turn LLM evaluation for character presets (scenario-based,
               direct Kimi/LLM call; from test_llm_preset.py).
  multi        Multi-turn LLM evaluation for any lorebook (prompt-set driven, direct
               LLM call; from test_multiturn.py, which subsumes test_iter.py).
  instruction  Instruction / prompt compliance diagnostic over the unity_chan_text
               pipeline (action/expression tag adherence, tag frequency,
               forbidden words, format; connects to the running YACHIYO server).

Model-quality findings are reported without a regression threshold. A non-zero
exit status means the evaluation could not complete because of an operational
error such as a failed request, protocol error, or missing output.

Examples:
    python test/test_llm.py --mode single unity_chan
    python test/test_llm.py --mode single unity_chan_vtuber --model gemma --save
    python test/test_llm.py --mode multi unity_chan --set set_a --model gemma
    python test/test_llm.py --mode instruction
"""
import asyncio
import json
import os
import sys
import random
import re
import argparse
import time
import uuid
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from openai import OpenAI
import httpx
import websockets

from utils.settings import get_setting, get_secret
from Modules.llm_utils.Tools import resolve_variables

LLM_TIMEOUT = 120
HTTP_TIMEOUT = 30


# ============================================================================
# Mode: single  (from test_llm_preset.py)
# Standalone LLM test for character presets. Loads lorebook, assembles
# messages, and calls the configured LLM endpoint directly.
# ============================================================================

def load_lorebook(name):
    path = f"configs/lorebooks/{name}.json"
    with open(path, encoding="utf-8") as f:
        return json.load(f)["data"]


def create_client(model_config_name="gemma"):
    with open("configs/settings/llm.json") as f:
        model_config = json.load(f)[model_config_name]
    api_base = get_setting("llm", model_config["api_base"])
    api_key = get_secret(model_config["api_key"])
    client = OpenAI(base_url=api_base, api_key=api_key, timeout=LLM_TIMEOUT)
    return client, model_config


def keyword_match(keywords, text):
    """Simple keyword substring matching (no data_query service needed)."""
    text_lower = text.lower()
    return any(kw.lower() in text_lower for kw in keywords)


def assemble_messages_single(lorebook_data, history, current_input):
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
    text = response.choices[0].message.content
    if not text or not text.strip():
        raise RuntimeError("LLM returned empty output")
    return text


# ------------ single mode: test scenarios ------------

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


def run_single(args):
    """Run single-turn evaluation scenarios. Returns operational success."""
    lorebook_name = args.lorebook
    model_config_name = args.model
    client, model_config = create_client(model_config_name)
    lorebook_data = load_lorebook(lorebook_name)
    scenarios = get_scenarios(lorebook_name)

    print(f"\n{'='*70}")
    print(f"Evaluating: {lorebook_name} | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(
        f"Model: {model_config['model_name']} | "
        f"Temp: {model_config['extra'].get('temperature', 'default')}"
    )
    print(f"{'='*70}")

    results = []
    success = True

    for scenario in scenarios:
        name = scenario["name"]
        print(f"\n--- {name} ---")

        history = []
        for msg in scenario["messages"]:
            assembled = assemble_messages_single(lorebook_data, history, msg)

            request_ok = True
            try:
                response = call_llm(client, model_config, assembled)
            except Exception as e:
                response = f"[ERROR] {e}"
                request_ok = False
                success = False

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

            if not request_ok:
                break
            history.append({"role": "user", "content": msg})
            history.append({"role": "assistant", "content": response})

    print(f"\n{'='*70}")
    print(f"Done: {len(results)} responses collected")
    print(f"{'='*70}")

    if args.save:
        os.makedirs("test/results", exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = f"test/results/{lorebook_name}_{timestamp}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"\nResults saved to {out_path}")

    return success


# ============================================================================
# Mode: multi  (from test_multiturn.py; test_iter.py is a near-duplicate subset)
# Quick multi-turn test for any lorebook, direct LLM call.
# ============================================================================

MULTI_ACTION_LIST = set("站立、轻微摇摆、摇摆、东张西望、眺望、蹦跳、走来走去、扭腰、背手、双手背后、叉腰、合十、挥手、点头、摇头、低头、抬头、歪头、歪头晃、指、招手、双手招手、挥手告别、举手、摊手、摆手、拥抱、鼓掌、比耶、遮眼、转圈、鞠躬、伸展、半蹲、跳、跺脚、捂脸、缩起来、嘘、哈欠、转圈舞、跳舞".split("、"))
MULTI_EXPR_LIST = set("眨眼、开心、温柔、委屈、惊讶、怀疑、坚定、呆住、恍惚、走神、恐惧、黑化、无神、无聊、星星眼、爱心眼、感兴趣、警觉、张嘴、大张嘴、咧嘴、嘟嘴、微张嘴、拉长脸、嘴巴圆、兴奋、猫嘴、不开心、哼、坏笑、微笑、愉快、沮丧、尴尬笑、吐舌头、调皮吐舌、严肃、困扰、生气、皱眉、流泪、害羞、脸色发白".split("、"))

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


def get_pipeline_vars(lorebook_name):
    """Try to find pipeline config to get vars."""
    candidates = [
        f"configs/{lorebook_name}_text.json",
        f"configs/unity_chan_text.json",
    ]
    for path in candidates:
        if os.path.exists(path):
            with open(path) as f:
                cfg = json.load(f)
            for node in cfg["pipeline"]:
                if "vars" in node.get("config", {}):
                    return node["config"]["vars"]
    return {"location": "Tokyo"}


def assemble_messages_multi(lorebook_data, history, current_input, static_vars):
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
    """Report action/expression list findings. unity_chan is free-form."""
    if lorebook_name == "unity_chan":
        # unity_chan uses free-form [action](expression)
        print("  (unity_chan uses free-form actions/expressions, skipping list check)")
        return True
    violations = 0
    for entry in history:
        if entry["role"] == "assistant":
            content = entry["content"]
            actions = re.findall(r'\[([^\]]+)\]', content)
            exprs = re.findall(r'\(([^)]+)\)', content)
            for a in actions:
                if a not in MULTI_ACTION_LIST:
                    print(f"  ACTION VIOLATION: [{a}]")
                    violations += 1
            for e in exprs:
                if e not in MULTI_EXPR_LIST:
                    print(f"  EXPRESSION VIOLATION: ({e})")
                    violations += 1
    if violations == 0:
        print("  No out-of-list tags observed.")
    else:
        print(f"  {violations} violation(s) found")
    return violations == 0


def run_multi(args):
    lorebook_name = args.lorebook
    prompt_set = args.set
    model_name = args.model

    with open("configs/settings/llm.json") as f:
        model_config = json.load(f)[model_name]
    api_base = get_setting("llm", model_config["api_base"])
    api_key = get_secret(model_config["api_key"])
    client = OpenAI(base_url=api_base, api_key=api_key, timeout=LLM_TIMEOUT)

    lorebook_data = load_lorebook(lorebook_name)
    static_vars = get_pipeline_vars(lorebook_name)
    prompts = PROMPT_SETS[prompt_set]

    print(f"=== {lorebook_name} Multi-turn | {prompt_set} | {datetime.now().strftime('%Y-%m-%d %H:%M')} ===")
    print()

    history = []
    for i, prompt in enumerate(prompts, 1):
        assembled = assemble_messages_multi(lorebook_data, history, prompt, static_vars)
        try:
            text = call_llm(client, model_config, assembled)
        except Exception as e:
            print(f"[{i}] ERROR: {type(e).__name__}: {e}")
            return False
        print(f"[{i}] User: {prompt}")
        print(f"    Reply: {text}")
        print()
        history.append({"role": "user", "content": prompt})
        history.append({"role": "assistant", "content": text})

    print("=== Format Diagnostics ===")
    check_format(history, lorebook_name)
    print("=== Evaluation Complete ===")
    return True


# ============================================================================
# Mode: instruction. Connects to the running YACHIYO server over WebSocket using
# the unity_chan_text pipeline (LLM + 2 RAGs, no ASR/TTS). Checks tag list
# adherence, tag frequency, forbidden words and format.
# ============================================================================

SERVER = "http://localhost:8910"
WS_URL = "ws://localhost:8910/ws"
PIPELINE = "unity_chan_text"

ACTIONS = set("站立 轻微摇摆 摇摆 东张西望 眺望 蹦跳 走来走去 扭腰 背手 双手背后 叉腰 合十 托下巴 托脸 挥手 点头 摇头 低头 抬头 歪头 歪头晃 指 招手 双手招手 挥手告别 举手 摊手 摆手 拥抱 鼓掌 比耶 遮眼 转圈 鞠躬 伸展 半蹲 跳 跺脚 捂脸 缩起来 嘘 哈欠 转圈舞 跳舞".split())

EXPRESSIONS = set("眨眼 开心 左眼眨 右眼眨 温柔眨左眼 温柔眨右眼 温柔 委屈 惊讶 怀疑 坚定 呆住 恍惚 走神 恐惧 黑化 无神 无聊 星星眼 爱心眼 感兴趣 警觉 张嘴 大张嘴 咧嘴 嘟嘴 微张嘴 拉长脸 嘴巴圆 兴奋 嘟嘟嘴 嘟嘴张开 猫嘴 不开心 哼 坏笑 微笑 愉快 沮丧 尴尬笑 吐舌头 调皮吐舌 严肃 困扰 生气 眉毛上扬 抬眉 皱眉 流泪 害羞 脸色发白 脸红 哈欠 小声".split())

FORBIDDEN = ["微微", "缓缓", "轻轻", "不由自主", "眼眸", "嘴角上扬", "仿佛"]

TAG_PATTERN = re.compile(r'\[([^\]]+)\]\(([^)]+)\)')

INSTRUCTION_PROMPTS = [
    "你好啊",
    "今天天气怎么样",
    "你喜欢吃什么",
    "给我讲个月亮上的故事吧",
    "你会后空翻吗？翻一个看看",
    "好无聊啊，有什么好玩的",
    "你是AI吗",
    "你最近在看什么动漫",
    "你觉得人类世界怎么样",
    "帮我算个塔罗牌",
    "你唱歌好听吗",
    "你害怕什么",
    "晚安",
    "你有朋友吗",
    "你平时都干什么",
    "我喜欢你",
]


async def collect_response(ws, timeout=30):
    """Collect full streamed text + RAG results."""
    full_text = ""
    started = False
    actions = []
    expressions = []
    action_hints = []
    expression_hints = []

    try:
        while True:
            msg = await asyncio.wait_for(ws.recv(), timeout=timeout)
            data = json.loads(msg)
            signal = data.get("signal", "")
            text = data.get("text", "")

            if signal == "SoS":
                started = True
                full_text = ""
            elif signal == "EoS":
                if not started:
                    raise RuntimeError("received EoS before SoS")
                # Collect remaining non-streaming messages
                try:
                    while True:
                        msg = await asyncio.wait_for(ws.recv(), timeout=5)
                        data = json.loads(msg)
                        if data.get("action"):
                            actions.append(data["action"])
                        if data.get("expression"):
                            expressions.append(data["expression"])
                        if data.get("action_hint"):
                            action_hints.append(data["action_hint"])
                        if data.get("expression_hint"):
                            expression_hints.append(data["expression_hint"])
                except asyncio.TimeoutError:
                    pass
                break
            elif started and text:
                full_text += text

            # Collect RAG results from streaming messages too
            if data.get("action"):
                actions.append(data["action"])
            if data.get("expression"):
                expressions.append(data["expression"])
            if data.get("action_hint"):
                action_hints.append(data["action_hint"])
            if data.get("expression_hint"):
                expression_hints.append(data["expression_hint"])
    except asyncio.TimeoutError as e:
        raise TimeoutError("response did not complete with EoS") from e

    return full_text.strip(), action_hints, expression_hints, actions, expressions


def analyze_response(idx, prompt, text, action_hints, expression_hints):
    """Analyze a single response for compliance."""
    issues = []

    # 1. Check tags in raw text
    tags = TAG_PATTERN.findall(text)
    tag_count = len(tags)

    for action, expression in tags:
        if action not in ACTIONS:
            issues.append(f"动作不在列表: [{action}]")
        if expression not in EXPRESSIONS:
            issues.append(f"表情不在列表: ({expression})")

    # Also check hints from RAG (these are what LLM actually generated)
    for ah in action_hints:
        if ah not in ACTIONS:
            issues.append(f"RAG动作hint不在列表: {ah}")
    for eh in expression_hints:
        if eh not in EXPRESSIONS:
            issues.append(f"RAG表情hint不在列表: {eh}")

    # 2. Check forbidden words
    for word in FORBIDDEN:
        if word in text:
            issues.append(f"禁用词: {word}")

    # 3. Check tag density (split by tags, check segment lengths)
    if tag_count > 0:
        segments = TAG_PATTERN.split(text)
        # segments: [before_tag1, action1, expr1, between, action2, expr2, after, ...]
        speech_segments = []
        i = 0
        while i < len(segments):
            if i % 3 == 0:  # text segment
                speech_segments.append(segments[i].strip())
            i += 1

        short_after_tag = 0
        for j, seg in enumerate(speech_segments[1:], 1):  # skip first (before first tag)
            if seg and len(seg) < 5:  # very short speech after tag
                short_after_tag += 1

        if short_after_tag > 1:
            issues.append(f"连续短句带标签({short_after_tag}处<5字)")

    # 4. Check Arabic numerals
    if re.search(r'\d', text):
        issues.append("含阿拉伯数字")

    # 5. Check for 哈欠 used as expression
    for _, expr in tags:
        if expr == "哈欠":
            issues.append("哈欠被当作表情（应为动作）")

    return issues


async def run_instruction_async(args):
    client_id = f"prompt_test_{uuid.uuid4().hex}"
    registered = False
    success = True
    total = 0
    compliant = 0
    all_issues = []

    try:
        async with httpx.AsyncClient(timeout=180) as http:
            response = await http.post(
                f"{SERVER}/register/", json={"client_id": client_id}
            )
            response.raise_for_status()
            registered = True
            print(f"Register: {response.json()}")

            response = await http.post(
                f"{SERVER}/init_pipeline/{client_id}",
                json={"config": PIPELINE},
            )
            response.raise_for_status()
            print(f"Init: {response.json()}")

        print("Waiting 5s...")
        await asyncio.sleep(5)

        async with websockets.connect(
            f"{WS_URL}/{client_id}", open_timeout=HTTP_TIMEOUT, close_timeout=10
        ) as ws:
            for i, prompt in enumerate(INSTRUCTION_PROMPTS):
                print(f"\n[{i+1}/{len(INSTRUCTION_PROMPTS)}] User: {prompt}")
                await ws.send(json.dumps({"text": prompt, "timestamp": time.time()}))

                text, action_hints, expression_hints, actions, expressions = \
                    await collect_response(ws)
                if not text:
                    raise RuntimeError(f"instruction {i + 1} returned empty text")

                print(f"  Response: {text[:150]}{'...' if len(text)>150 else ''}")

                # Show tags
                tags = TAG_PATTERN.findall(text)
                if tags:
                    for a, e in tags:
                        a_ok = "✓" if a in ACTIONS else "✗"
                        e_ok = "✓" if e in EXPRESSIONS else "✗"
                        print(f"  Tag: [{a}]{a_ok} ({e}){e_ok}")

                # Show RAG hints, mapped positionally to the matched RAG action/expression
                for k, (ah, eh) in enumerate(zip(action_hints, expression_hints)):
                    ah_ok = "✓" if ah in ACTIONS else "✗"
                    eh_ok = "✓" if eh in EXPRESSIONS else "✗"
                    matched_a = actions[k] if k < len(actions) else "?"
                    matched_e = expressions[k] if k < len(expressions) else "?"
                    print(f"  Hint: [{ah}]{ah_ok} ({eh}){eh_ok} → {matched_a}/{matched_e}")

                # Compliance findings are diagnostic; only transport/protocol/output
                # failures determine the process exit status.
                issues = analyze_response(i, prompt, text, action_hints, expression_hints)
                total += 1
                if not issues:
                    compliant += 1
                    print("  OK ✓")
                else:
                    for issue in issues:
                        print(f"  ISSUE: {issue}")
                    all_issues.extend((prompt, issue) for issue in issues)

                await asyncio.sleep(1)
    except Exception as e:
        success = False
        print(f"[ERROR] Instruction mode failed: {type(e).__name__}: {e}")
    finally:
        if registered:
            try:
                async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as http:
                    response = await http.post(
                        f"{SERVER}/unregister/", json={"client_id": client_id}
                    )
                    response.raise_for_status()
                    print(f"Unregister: {response.json()}")
            except Exception as e:
                success = False
                print(f"[ERROR] Unregister failed: {type(e).__name__}: {e}")
        try:
            os.unlink(f"logs/client_{client_id}.log")
        except FileNotFoundError:
            pass
        except OSError as e:
            success = False
            print(f"[ERROR] Client log cleanup failed: {e}")

    print(f"\n{'='*60}")
    rate = 100 * compliant / total if total else 0
    print(f"COMPLIANCE: {compliant}/{total} ({rate:.0f}%)")
    if all_issues:
        print(f"\nAll issues ({len(all_issues)}):")
        for prompt, issue in all_issues:
            print(f"  [{prompt[:15]}] {issue}")
    if total != len(INSTRUCTION_PROMPTS):
        success = False
    return success


def run_instruction(args):
    return asyncio.run(run_instruction_async(args))


# ============================================================================
# Dispatcher
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Consolidated LLM evaluation (single / multi / instruction modes)."
    )
    parser.add_argument(
        "--mode",
        choices=["single", "multi", "instruction"],
        default="multi",
        help="Evaluation mode: single (preset scenarios), multi (multi-turn lorebook), "
             "instruction (server-side prompt diagnostics). Default: multi.",
    )
    # single / multi take a positional lorebook name; instruction connects to server.
    parser.add_argument(
        "lorebook",
        nargs="?",
        help="Lorebook name. Required for single/multi "
             "(e.g. unity_chan, unity_chan_vtuber, sample). "
             "Ignored for instruction mode (uses the unity_chan_text pipeline).",
    )
    parser.add_argument(
        "--model",
        default=None,
        help="Model config name in configs/settings/llm.json. "
             "Default: gemma (single) / gemma (multi).",
    )
    parser.add_argument(
        "--set",
        default="set_a",
        choices=PROMPT_SETS.keys(),
        help="Prompt set for multi mode.",
    )
    parser.add_argument(
        "--save",
        action="store_true",
        help="Save results to JSON file (single mode).",
    )
    args = parser.parse_args()

    try:
        if args.mode == "single":
            if not args.lorebook:
                parser.error("single mode requires a lorebook name argument")
            if args.model is None:
                args.model = "gemma"
            success = run_single(args)
        elif args.mode == "multi":
            if not args.lorebook:
                parser.error("multi mode requires a lorebook name argument")
            if args.model is None:
                args.model = "gemma"
            success = run_multi(args)
        else:
            success = run_instruction(args)
    except Exception as e:
        print(f"[ERROR] {args.mode} mode crashed: {type(e).__name__}: {e}")
        success = False
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
