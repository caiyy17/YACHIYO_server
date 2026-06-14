"""
Test dev_default lorebook prompt compliance.
Checks: action/expression list adherence, tag frequency, forbidden words, format.
Uses dev_text pipeline (LLM + 2 RAGs, no ASR/TTS).
"""
import asyncio
import json
import time
import re
import httpx
import websockets

SERVER = "http://localhost:8910"
WS_URL = "ws://localhost:8910/ws"
PIPELINE = "dev_text"

ACTIONS = set("左手挠头 左手托腮 翻书 歪头 写字 点头 摇头 思考".split())

EXPRESSIONS = set("眨眼 开心 左眼眨 右眼眨 温柔眨左眼 温柔眨右眼 温柔 委屈 惊讶 怀疑 坚定 呆住 恍惚 走神 恐惧 黑化 无神 无聊 星星眼 爱心眼 感兴趣 警觉 张嘴 大张嘴 咧嘴 嘟嘴 微张嘴 拉长脸 嘴巴圆 兴奋 嘟嘟嘴 嘟嘴张开 猫嘴 不开心 哼 坏笑 微笑 愉快 沮丧 尴尬笑 吐舌头 调皮吐舌 严肃 困扰 生气 眉毛上扬 抬眉 皱眉 流泪 害羞 脸色发白 脸红 哈欠 小声".split())

FORBIDDEN = ["微微", "缓缓", "轻轻", "不由自主", "眼眸", "嘴角上扬", "仿佛"]

TAG_PATTERN = re.compile(r'\[([^\]]+)\]\(([^)]+)\)')

PROMPTS = [
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
    except asyncio.TimeoutError:
        pass

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


async def main():
    client_id = f"prompt_test_{int(time.time())}"

    async with httpx.AsyncClient(timeout=30) as http:
        await http.post(f"{SERVER}/register/", json={"client_id": client_id})
        r = await http.post(f"{SERVER}/init_pipeline/{client_id}",
                           json={"config": PIPELINE, "force": True})
        print(f"Init: {r.json()}")

    print("Waiting 5s...")
    await asyncio.sleep(5)

    total = 0
    compliant = 0
    all_issues = []

    async with websockets.connect(f"{WS_URL}/{client_id}") as ws:
        for i, prompt in enumerate(PROMPTS):
            print(f"\n[{i+1}/{len(PROMPTS)}] User: {prompt}")
            await ws.send(json.dumps({"text": prompt, "timestamp": time.time()}))

            text, action_hints, expression_hints, actions, expressions = \
                await collect_response(ws)

            print(f"  Response: {text[:150]}{'...' if len(text)>150 else ''}")

            # Show tags
            tags = TAG_PATTERN.findall(text)
            if tags:
                for a, e in tags:
                    a_ok = "✓" if a in ACTIONS else "✗"
                    e_ok = "✓" if e in EXPRESSIONS else "✗"
                    print(f"  Tag: [{a}]{a_ok} ({e}){e_ok}")

            # Show RAG hints
            for ah, eh in zip(action_hints, expression_hints):
                ah_ok = "✓" if ah in ACTIONS else "✗"
                eh_ok = "✓" if eh in EXPRESSIONS else "✗"
                print(f"  Hint: [{ah}]{ah_ok} ({eh}){eh_ok} → {actions[action_hints.index(ah)] if ah in action_hints else '?'}/{expressions[expression_hints.index(eh)] if eh in expression_hints else '?'}")

            issues = analyze_response(i, prompt, text, action_hints, expression_hints)
            total += 1
            if not issues:
                compliant += 1
                print(f"  OK ✓")
            else:
                for issue in issues:
                    print(f"  ISSUE: {issue}")
                all_issues.extend([(prompt, issue) for issue in issues])

            await asyncio.sleep(1)

    print(f"\n{'='*60}")
    print(f"COMPLIANCE: {compliant}/{total} ({100*compliant/total:.0f}%)")
    if all_issues:
        print(f"\nAll issues ({len(all_issues)}):")
        for prompt, issue in all_issues:
            print(f"  [{prompt[:15]}] {issue}")

    async with httpx.AsyncClient(timeout=10) as http:
        await http.post(f"{SERVER}/unregister/", json={"client_id": client_id})


if __name__ == "__main__":
    asyncio.run(main())
