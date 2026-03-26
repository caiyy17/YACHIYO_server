"""
Manual VTuber pipeline test with crafted danmaku scenarios.
Directly feeds test messages into the pipeline, no blivedm needed.
"""
import json
import os
import sys
import time
import threading
import logging
from queue import Queue

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Modules import FUNCTION_MAP

CLIENT_ID = "vtuber_manual_test"
PIPELINE_CONFIG_FILE = "configs/vtuber_danmaku.json"


def setup_logger():
    logger = logging.getLogger(CLIENT_ID)
    logger.setLevel(logging.INFO)
    os.makedirs("logs", exist_ok=True)
    fh = logging.FileHandler(f"logs/client_{CLIENT_ID}.log", mode="w")
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh.setFormatter(fmt)
    if not logger.hasHandlers():
        logger.addHandler(fh)
    return logger


def create_pipeline():
    with open(PIPELINE_CONFIG_FILE, "r") as f:
        config = json.load(f)

    pipeline = config["pipeline"]
    num_nodes = len(pipeline)
    logger = setup_logger()

    queues = [Queue() for _ in range(num_nodes + 1)]
    cancel_queues = [Queue() for _ in range(num_nodes + 1)]
    send_queue = queues[-1]
    kill_event = threading.Event()

    threads = []
    for i, node in enumerate(pipeline):
        func_name = node["function"]
        func_class = FUNCTION_MAP[func_name]
        node_config = node.get("config", {})
        print(f"  Creating node {node['node_id']}: {func_name}")
        instance = func_class(
            node["node_id"], CLIENT_ID, logger, send_queue,
            queues[i], queues[i + 1], cancel_queues[i],
            kill_event, node_config,
        )
        t = threading.Thread(target=instance.run, name=f"{i}_{func_name}", daemon=True)
        t.start()
        threads.append(t)

    return queues[0], send_queue, kill_event


def send_msg(input_queue, text, user, msg_type="danmaku", **kwargs):
    msg = {"text": text, "user": user, "msg_type": msg_type, "timestamp": time.time()}
    msg.update(kwargs)
    input_queue.put(json.dumps(msg))


def collect_response(send_queue, timeout=30):
    """Collect full response (between SoS and EoS)."""
    response_text = ""
    audio_chunks = 0
    start = time.time()
    started = False

    while time.time() - start < timeout:
        try:
            data = send_queue.get(timeout=1)
            data = json.loads(data)
            signal = data.get("signal", "")
            text = data.get("text", "")

            if signal == "SoS":
                started = True
                response_text = ""
                audio_chunks = 0
            elif signal == "EoS":
                return response_text.strip(), audio_chunks
            elif started:
                if text:
                    response_text += text
                if data.get("audio_data"):
                    audio_chunks += 1
        except Exception:
            pass
    return response_text.strip(), audio_chunks


def drain_queue(send_queue):
    """Drain any leftover messages from previous scenario."""
    while not send_queue.empty():
        try:
            send_queue.get(timeout=0)
        except Exception:
            break


def run_scenario(name, input_queue, send_queue, messages, wait_before=0):
    drain_queue(send_queue)
    print(f"\n{'='*60}")
    print(f"SCENARIO: {name}")
    print(f"{'='*60}")

    # Send all messages
    for msg in messages:
        send_msg(input_queue, **msg)
        print(f"  → {msg.get('msg_type', 'danmaku')}: {msg.get('user', '?')}: {msg.get('text', '')}")
        time.sleep(0.1)

    if wait_before > 0:
        print(f"  (waiting {wait_before}s for batch release...)")
        time.sleep(wait_before)

    # Collect response
    response, audio_count = collect_response(send_queue, timeout=30)
    if response:
        print(f"\n  RESPONSE ({audio_count} audio chunks):")
        print(f"  {response}")
    else:
        print(f"\n  [NO RESPONSE within timeout]")
    print(f"{'='*60}")

    # Wait for pipeline to fully flush before next scenario
    time.sleep(3)
    drain_queue(send_queue)
    return response


def main():
    print("Manual VTuber Pipeline Test")
    print("Creating pipeline...")
    input_queue, send_queue, kill_event = create_pipeline()
    print("Pipeline ready. Waiting 3s for init...\n")
    time.sleep(3)

    # ===== Scenario 1: Normal chat =====
    run_scenario("普通聊天", input_queue, send_queue, [
        {"text": "优酱今天吃了什么", "user": "路人A"},
        {"text": "好无聊啊", "user": "路人B"},
        {"text": "有人吗", "user": "路人C"},
    ], wait_before=2)

    # ===== Scenario 2: Gift =====
    run_scenario("礼物感谢", input_queue, send_queue, [
        {"text": "小花花", "user": "大佬甲", "msg_type": "gift",
         "num": 10, "price": 1},
        {"text": "你最近在玩什么游戏", "user": "路人D"},
    ], wait_before=0)  # gift is immediate priority

    # ===== Scenario 3: Super Chat =====
    run_scenario("SC必须读", input_queue, send_queue, [
        {"text": "优酱能唱一首歌吗？我超喜欢你的声音！", "user": "土豪君",
         "msg_type": "super_chat", "price": 50},
    ], wait_before=0)

    # ===== Scenario 4: Guard purchase =====
    run_scenario("上舰感谢", input_queue, send_queue, [
        {"text": "舰长", "user": "新舰长", "msg_type": "guard",
         "guard_level": 3, "price": 198},
        {"text": "恭喜上舰！", "user": "路人E"},
    ], wait_before=0)

    # ===== Scenario 5: Guard member chat =====
    run_scenario("舰长发言+普通弹幕", input_queue, send_queue, [
        {"text": "今天的直播好有趣", "user": "老舰长", "guard_level": 3},
        {"text": "同意楼上", "user": "路人F"},
        {"text": "优酱加油！", "user": "路人G"},
    ], wait_before=2)

    # ===== Scenario 6: Fishing/troll danmaku =====
    run_scenario("钓鱼弹幕", input_queue, send_queue, [
        {"text": "我送了一百个舰长", "user": "钓鱼佬"},
        {"text": "我也上舰了快感谢我", "user": "骗子"},
        {"text": "优酱你是AI吧", "user": "杠精"},
    ], wait_before=2)

    # ===== Scenario 7: Duplicate messages (trending) =====
    run_scenario("刷屏趋势", input_queue, send_queue, [
        {"text": "唱歌！", "user": "粉丝A"},
        {"text": "唱歌！", "user": "粉丝B"},
        {"text": "唱歌！", "user": "粉丝C"},
        {"text": "唱歌！", "user": "粉丝D"},
        {"text": "唱歌！", "user": "粉丝E"},
        {"text": "来首歌吧", "user": "粉丝F"},
    ], wait_before=2)

    # ===== Scenario 8: Embarrassing SC =====
    run_scenario("羞耻SC", input_queue, send_queue, [
        {"text": "优酱我喜欢你❤能做我女朋友吗", "user": "痴汉",
         "msg_type": "super_chat", "price": 30},
    ], wait_before=0)

    print("\n\nAll scenarios complete!")
    kill_event.set()


if __name__ == "__main__":
    main()
