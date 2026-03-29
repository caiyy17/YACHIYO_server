"""
Quick test for dev_default lorebook tag compliance.
Uses dev_text pipeline (LLM + Expression RAG + Motion RAG, no ASR/TTS).
"""
import asyncio
import json
import time
import re
import httpx
import websockets

SERVER = "http://localhost:8910"
WS_URL = "ws://localhost:8910/ws"
CLIENT_ID = f"lorebook_test_{int(time.time())}"
PIPELINE = "dev_text"

ACTIONS = set("站立 轻微摇摆 摇摆 东张西望 眺望 蹦跳 走来走去 扭腰 背手 双手背后 叉腰 合十 托下巴 托脸 挥手 点头 摇头 低头 抬头 歪头 歪头晃 指 招手 双手招手 挥手告别 举手 摊手 拥抱 鼓掌 比耶 遮眼 转圈 鞠躬 伸展 半蹲 跳 跺脚 捂脸 缩起来 嘘 哈欠 转圈舞 跳舞".split())

EXPRESSIONS = set("眨眼 开心 左眼眨 右眼眨 温柔眨左眼 温柔眨右眼 温柔 委屈 惊讶 怀疑 坚定 呆住 恐惧 黑化 无神 星星眼 爱心眼 感兴趣 警觉 张嘴 大张嘴 咧嘴 嘟嘴 微张嘴 拉长脸 嘴巴圆 兴奋 嘟嘟嘴 嘟嘴张开 猫嘴 不开心 哼 坏笑 微笑 愉快 沮丧 尴尬笑 吐舌头 调皮吐舌 严肃 困扰 生气 眉毛上扬 抬眉 皱眉 流泪 害羞 脸色发白".split())

PROMPTS = [
    "你好啊",
    "今天天气怎么样",
    "你喜欢吃什么",
    "给我讲个月亮上的故事吧",
    "你会后空翻吗？翻一个看看",
    "好无聊啊，有什么好玩的",
    "你是AI吗",
    "你最近在看什么动漫",
]

TAG_PATTERN = re.compile(r'\[([^\]]+)\]\(([^)]+)\)')


def check_tags(text):
    """Check all [action](expression) tags in text against allowed lists."""
    tags = TAG_PATTERN.findall(text)
    results = []
    for action, expression in tags:
        a_ok = action in ACTIONS
        e_ok = expression in EXPRESSIONS
        results.append({
            "action": action, "action_ok": a_ok,
            "expression": expression, "expression_ok": e_ok,
        })
    return results


async def main():
    # Register + init pipeline
    async with httpx.AsyncClient() as http:
        r = await http.post(f"{SERVER}/register/", json={"client_id": CLIENT_ID})
        print(f"Register: {r.status_code}")
        r = await http.post(f"{SERVER}/init_pipeline/{CLIENT_ID}",
                           json={"config": PIPELINE, "force": True})
        print(f"Init pipeline ({PIPELINE}): {r.status_code} {r.text}")

    print("Waiting 5s for pipeline init...")
    await asyncio.sleep(5)

    total_tags = 0
    valid_tags = 0
    rag_results = []

    async with websockets.connect(f"{WS_URL}/{CLIENT_ID}") as ws:
        for i, prompt in enumerate(PROMPTS):
            print(f"\n{'='*60}")
            print(f"[{i+1}/{len(PROMPTS)}] User: {prompt}")

            await ws.send(json.dumps({"text": prompt, "timestamp": time.time()}))

            # Collect streaming text + RAG results
            full_text = ""
            started = False
            round_actions = []
            round_expressions = []
            action_hints = []
            expression_hints = []

            try:
                while True:
                    msg = await asyncio.wait_for(ws.recv(), timeout=30)
                    data = json.loads(msg)
                    signal = data.get("signal", "")
                    text = data.get("text", "")

                    if signal == "SoS":
                        started = True
                        full_text = ""
                    elif signal == "EoS":
                        break
                    elif started and text:
                        full_text += text

                    # Collect RAG-resolved action/expression
                    if data.get("action"):
                        round_actions.append(data["action"])
                    if data.get("expression"):
                        round_expressions.append(data["expression"])
                    if data.get("action_hint"):
                        action_hints.append(data["action_hint"])
                    if data.get("expression_hint"):
                        expression_hints.append(data["expression_hint"])
            except asyncio.TimeoutError:
                print("  [TIMEOUT]")
                continue

            full_text = full_text.strip()
            print(f"  Response: {full_text}")

            # Check raw text tags (LLM compliance)
            tags = check_tags(full_text)
            if tags:
                for t in tags:
                    total_tags += 1
                    a_mark = "OK" if t["action_ok"] else "BAD"
                    e_mark = "OK" if t["expression_ok"] else "BAD"
                    if t["action_ok"] and t["expression_ok"]:
                        valid_tags += 1
                    print(f"  LLM Tag: [{t['action']}]({t['expression']}) → action:{a_mark} expression:{e_mark}")

            # Show RAG results
            if round_actions or round_expressions:
                for a, ah in zip(round_actions or [''], action_hints or ['']):
                    print(f"  RAG Action: {ah} → {a}")
                for e, eh in zip(round_expressions or [''], expression_hints or ['']):
                    print(f"  RAG Expression: {eh} → {e}")
                rag_results.append({
                    "actions": list(zip(action_hints, round_actions)),
                    "expressions": list(zip(expression_hints, round_expressions)),
                })

            if not tags and not round_actions:
                print("  [No tags found]")

            await asyncio.sleep(1)

    print(f"\n{'='*60}")
    print(f"LLM TAG COMPLIANCE: {valid_tags}/{total_tags} fully compliant"
          + (f" ({100*valid_tags/total_tags:.0f}%)" if total_tags else ""))
    print(f"Total responses: {len(PROMPTS)}")
    print(f"RAG resolved: {len(rag_results)} responses had RAG results")

    # Unregister
    async with httpx.AsyncClient() as http:
        await http.post(f"{SERVER}/unregister/", json={"client_id": CLIENT_ID})


if __name__ == "__main__":
    asyncio.run(main())
