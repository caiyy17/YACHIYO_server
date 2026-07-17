"""
Consolidated VTuber / live danmaku test script.

Four modes (select with --mode):
- blivedm : Minimal Bilibili connection smoke test. Finds an active VTuber room,
            connects via blivedm, prints/counts danmaku for ~60s. No pipeline, no
            main server. Run this FIRST to verify the Bilibili connection works
            before the heavier real-config modes. (folds test_blivedm_minimal.py)
- manual  : Scripted/crafted danmaku fed directly into the pipeline. Deterministic,
            no network. (formerly test_vtuber_manual.py)
- live    : Connects to a real Bilibili room and feeds live danmaku directly into the
            in-process pipeline, no main server. (formerly test_vtuber_standalone.py)
- server  : Real Bilibili + full main server end-to-end via register/websocket.
            (formerly test_vtuber_danmaku.py)
"""
import argparse
import asyncio
import aiohttp
import json
import os
import random
import sys
import time
import threading
import uuid
import logging
from queue import Queue

import requests
import websockets

import blivedm
import blivedm.models.web as web_models

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Suppress noisy blivedm logging completely (from standalone test)
logging.getLogger("blivedm").setLevel(logging.CRITICAL)


# =============================================================================
# Mode: blivedm  (from test_blivedm_minimal.py)
# Minimal Bilibili connection smoke test: find an active VTuber room, connect via
# blivedm, print/count danmaku for ~60s. No pipeline, no main server.
# Run this FIRST to verify the Bilibili connection works.
# =============================================================================

BLIVEDM_LISTEN_DURATION = 60  # seconds


def blivedm_create_bilibili_session():
    """Create aiohttp session with headers and cookies to pass Bilibili anti-crawler."""
    cookies = {"buvid3": str(uuid.uuid4()) + "infoc"}
    return aiohttp.ClientSession(headers=BILIBILI_HEADERS, cookies=cookies)


async def blivedm_find_active_vtuber_room(session):
    """Find an active VTuber live room (area_id=371, parent_area_id=9)."""
    params = {
        "platform": "web",
        "parent_area_id": 9,
        "cate_id": 0,
        "area_id": 371,
        "sort_type": "online",
        "page": 1,
        "page_size": 20,
    }
    async with session.get(BILIBILI_ROOM_LIST_API, params=params) as resp:
        data = await resp.json()
        rooms = data.get("data", [])
        if not rooms:
            print("No active VTuber rooms found!")
            return None, None, None
        room = random.choice(rooms[:min(10, len(rooms))])
        return room["roomid"], room["title"], room["uname"]


class BlivedmTestHandler(blivedm.BaseHandler):
    def __init__(self):
        self.msg_count = 0

    def _on_danmaku(self, client, message: web_models.DanmakuMessage):
        self.msg_count += 1
        print(f"[弹幕] {message.uname}: {message.msg}")

    def _on_gift(self, client, message: web_models.GiftMessage):
        self.msg_count += 1
        print(f"[礼物] {message.uname} 送了 {message.gift_name} x{message.num}")

    def _on_super_chat(self, client, message: web_models.SuperChatMessage):
        self.msg_count += 1
        print(f"[SC] {message.uname} (¥{message.price}): {message.message}")

    def _on_buy_guard(self, client, message: web_models.GuardBuyMessage):
        self.msg_count += 1
        print(f"[舰长] {message.uname} 开通了舰长 (level {message.guard_level})")


async def run_blivedm():
    session = blivedm_create_bilibili_session()
    try:
        print("Searching for active VTuber rooms...")
        room_id, title, uname = await blivedm_find_active_vtuber_room(session)
        if room_id is None:
            return

        print(f"\nConnecting to room {room_id}: {uname} - {title}")
        print(f"Listening for {BLIVEDM_LISTEN_DURATION} seconds...\n{'='*60}")

        handler = BlivedmTestHandler()
        client = blivedm.BLiveClient(room_id, session=session)
        client.set_handler(handler)
        client.start()

        try:
            await asyncio.sleep(BLIVEDM_LISTEN_DURATION)
        finally:
            client.stop()
            await client.join()

        print(f"\n{'='*60}")
        print(f"Total messages received: {handler.msg_count}")
    finally:
        await session.close()


# =============================================================================
# Mode: manual  (from test_vtuber_manual.py)
# Scripted/crafted danmaku fed into the pipeline directly; deterministic.
# =============================================================================

MANUAL_CLIENT_ID = "vtuber_manual_test"
MANUAL_PIPELINE_CONFIG_FILE = "configs/unity_chan_live.json"


def manual_setup_logger():
    from Modules import FUNCTION_MAP  # noqa: F401  (ensure project import path works)
    logger = logging.getLogger(MANUAL_CLIENT_ID)
    logger.setLevel(logging.INFO)
    os.makedirs("logs", exist_ok=True)
    fh = logging.FileHandler(f"logs/client_{MANUAL_CLIENT_ID}.log", mode="w")
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh.setFormatter(fmt)
    if not logger.hasHandlers():
        logger.addHandler(fh)
    return logger


def manual_create_pipeline():
    from Modules import FUNCTION_MAP

    with open(MANUAL_PIPELINE_CONFIG_FILE, "r") as f:
        config = json.load(f)

    pipeline = config["pipeline"]
    num_nodes = len(pipeline)
    logger = manual_setup_logger()

    queues = [Queue() for _ in range(num_nodes + 1)]
    cancel_queues = [Queue() for _ in range(num_nodes)]
    send_queue = queues[-1]

    threads = []
    for i, node in enumerate(pipeline):
        func_name = node["function"]
        func_class = FUNCTION_MAP[func_name]
        node_config = node.get("config", {})
        print(f"  Creating node {node['node_id']}: {func_name}")
        instance = func_class(
            node["node_id"], MANUAL_CLIENT_ID, logger, send_queue,
            queues[i], queues[i + 1], cancel_queues[i],
            node_config,
        )
        t = threading.Thread(target=instance.run, name=f"{i}_{func_name}", daemon=True)
        t.start()
        threads.append(t)

    return queues[0], send_queue, cancel_queues


def manual_send_msg(input_queue, text, user, msg_type="danmaku", **kwargs):
    msg = {"text": text, "user": user, "msg_type": msg_type, "timestamp": time.time()}
    msg.update(kwargs)
    input_queue.put(json.dumps(msg))


def manual_collect_response(send_queue, timeout=30):
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


def manual_drain_queue(send_queue):
    """Drain any leftover messages from previous scenario."""
    while not send_queue.empty():
        try:
            send_queue.get(timeout=0)
        except Exception:
            break


def manual_run_scenario(name, input_queue, send_queue, messages, wait_before=0):
    manual_drain_queue(send_queue)
    print(f"\n{'='*60}")
    print(f"SCENARIO: {name}")
    print(f"{'='*60}")

    # Send all messages
    for msg in messages:
        manual_send_msg(input_queue, **msg)
        print(f"  → {msg.get('msg_type', 'danmaku')}: {msg.get('user', '?')}: {msg.get('text', '')}")
        time.sleep(0.1)

    if wait_before > 0:
        print(f"  (waiting {wait_before}s for batch release...)")
        time.sleep(wait_before)

    # Collect response
    response, audio_count = manual_collect_response(send_queue, timeout=30)
    if response:
        print(f"\n  RESPONSE ({audio_count} audio chunks):")
        print(f"  {response}")
    else:
        print(f"\n  [NO RESPONSE within timeout]")
    print(f"{'='*60}")

    # Wait for pipeline to fully flush before next scenario
    time.sleep(3)
    manual_drain_queue(send_queue)
    return response


def run_manual():
    print("Manual VTuber Pipeline Test")
    print("Creating pipeline...")
    input_queue, send_queue, cancel_queues = manual_create_pipeline()
    print("Pipeline ready. Waiting 3s for init...\n")
    time.sleep(3)

    # ===== Scenario 1: Normal chat =====
    manual_run_scenario("普通聊天", input_queue, send_queue, [
        {"text": "优酱今天吃了什么", "user": "路人A"},
        {"text": "好无聊啊", "user": "路人B"},
        {"text": "有人吗", "user": "路人C"},
    ], wait_before=2)

    # ===== Scenario 2: Gift =====
    manual_run_scenario("礼物感谢", input_queue, send_queue, [
        {"text": "小花花", "user": "大佬甲", "msg_type": "gift",
         "num": 10, "price": 1},
        {"text": "你最近在玩什么游戏", "user": "路人D"},
    ], wait_before=0)  # gift is immediate priority

    # ===== Scenario 3: Super Chat =====
    manual_run_scenario("SC必须读", input_queue, send_queue, [
        {"text": "优酱能唱一首歌吗？我超喜欢你的声音！", "user": "土豪君",
         "msg_type": "super_chat", "price": 50},
    ], wait_before=0)

    # ===== Scenario 4: Guard purchase =====
    manual_run_scenario("上舰感谢", input_queue, send_queue, [
        {"text": "舰长", "user": "新舰长", "msg_type": "guard",
         "guard_level": 3, "price": 198},
        {"text": "恭喜上舰！", "user": "路人E"},
    ], wait_before=0)

    # ===== Scenario 5: Guard member chat =====
    manual_run_scenario("舰长发言+普通弹幕", input_queue, send_queue, [
        {"text": "今天的直播好有趣", "user": "老舰长", "guard_level": 3},
        {"text": "同意楼上", "user": "路人F"},
        {"text": "优酱加油！", "user": "路人G"},
    ], wait_before=2)

    # ===== Scenario 6: Fishing/troll danmaku =====
    manual_run_scenario("钓鱼弹幕", input_queue, send_queue, [
        {"text": "我送了一百个舰长", "user": "钓鱼佬"},
        {"text": "我也上舰了快感谢我", "user": "骗子"},
        {"text": "优酱你是AI吧", "user": "杠精"},
    ], wait_before=2)

    # ===== Scenario 7: Duplicate messages (trending) =====
    manual_run_scenario("刷屏趋势", input_queue, send_queue, [
        {"text": "唱歌！", "user": "粉丝A"},
        {"text": "唱歌！", "user": "粉丝B"},
        {"text": "唱歌！", "user": "粉丝C"},
        {"text": "唱歌！", "user": "粉丝D"},
        {"text": "唱歌！", "user": "粉丝E"},
        {"text": "来首歌吧", "user": "粉丝F"},
    ], wait_before=2)

    # ===== Scenario 8: Embarrassing SC =====
    manual_run_scenario("羞耻SC", input_queue, send_queue, [
        {"text": "优酱我喜欢你❤能做我女朋友吗", "user": "痴汉",
         "msg_type": "super_chat", "price": 30},
    ], wait_before=0)

    print("\n\nAll scenarios complete!")
    for cq in cancel_queues:
        cq.put(json.dumps({"signal": "cancel", "timestamp": float("inf")}))
        cq.put(json.dumps({"signal": "kill"}))


# =============================================================================
# Mode: live  (from test_vtuber_standalone.py)
# Connects to a real Bilibili room, feeds live danmaku directly into the
# in-process pipeline, no main server.
# =============================================================================

LIVE_CLIENT_ID = "vtuber_standalone"
LIVE_PIPELINE_CONFIG_FILE = "configs/unity_chan_live.json"
# Set to a room ID to force connect, or None for auto-discovery of an active room
LIVE_FORCE_ROOM_ID = None

BILIBILI_ROOM_LIST_API = "https://api.live.bilibili.com/room/v1/area/getRoomList"
BILIBILI_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept-Encoding": "gzip, deflate",
    "Referer": "https://live.bilibili.com/",
    "Origin": "https://live.bilibili.com",
}


def live_create_bilibili_session():
    from utils.settings import get_secret
    cookies = {"buvid3": str(uuid.uuid4()) + "infoc"}
    sessdata = get_secret("BILIBILI_SESSDATA", "")
    if sessdata:
        cookies["SESSDATA"] = sessdata
        print("[BILI] Logged in with SESSDATA")
    else:
        print("[BILI] No SESSDATA, usernames will be masked")
    return aiohttp.ClientSession(headers=BILIBILI_HEADERS, cookies=cookies)


async def live_find_active_vtuber_room(session, exclude_rooms=None):
    exclude_rooms = exclude_rooms or set()
    params = {
        "platform": "web",
        "parent_area_id": 9,
        "cate_id": 0,
        "area_id": 371,
        "sort_type": "online",
        "page": 1,
        "page_size": 30,
    }
    try:
        async with session.get(BILIBILI_ROOM_LIST_API, params=params) as resp:
            data = await resp.json()
            rooms = data.get("data", [])
            available = [r for r in rooms if r["roomid"] not in exclude_rooms]
            if not available:
                available = rooms
            if not available:
                return None, None, None
            room = random.choice(available[:min(15, len(available))])
            return room["roomid"], room["title"], room["uname"]
    except Exception as e:
        print(f"[ERROR] find_active_vtuber_room: {e}")
        return None, None, None


def live_setup_logger():
    logger = logging.getLogger(LIVE_CLIENT_ID)
    logger.setLevel(logging.INFO)
    os.makedirs("logs", exist_ok=True)
    fh = logging.FileHandler(f"logs/client_{LIVE_CLIENT_ID}.log", mode="w")
    ch = logging.StreamHandler()
    ch.setLevel(logging.WARNING)  # Only warnings/errors to console
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)
    if not logger.hasHandlers():
        logger.addHandler(fh)
        logger.addHandler(ch)
    return logger


def live_create_pipeline():
    """Create pipeline modules connected by queues, same as YACHIYO server does."""
    with open(LIVE_PIPELINE_CONFIG_FILE, "r") as f:
        config = json.load(f)

    pipeline = config["pipeline"]
    num_nodes = len(pipeline)
    logger = live_setup_logger()

    # Create queues (num_nodes + 1: input for each node + final output)
    queues = [Queue() for _ in range(num_nodes + 1)]
    cancel_queues = [Queue() for _ in range(num_nodes)]
    send_queue = queues[-1]  # Last queue is the output

    # Module class lookup
    from Modules import FUNCTION_MAP

    threads = []
    for i, node in enumerate(pipeline):
        func_name = node["function"]
        func_class = FUNCTION_MAP[func_name]
        node_config = node.get("config", {})

        print(f"  Creating node {node['node_id']}: {func_name} ({func_class.__name__})")
        instance = func_class(
            node["node_id"],
            LIVE_CLIENT_ID,
            logger,
            send_queue,
            queues[i],
            queues[i + 1],
            cancel_queues[i],
            node_config,
        )
        t = threading.Thread(target=instance.run, name=f"{i}_{func_name}", daemon=True)
        t.start()
        threads.append(t)

    return queues[0], send_queue, cancel_queues, threads


# ===== blivedm Handler (live mode) =====
class LiveDanmakuForwarder(blivedm.BaseHandler):
    def __init__(self, input_queue):
        self.input_queue = input_queue
        self.msg_count = 0

    GUARD_NAMES = {0: "", 1: "总督", 2: "提督", 3: "舰长"}

    def _send(self, msg_dict):
        msg_dict["timestamp"] = time.time()
        self.input_queue.put(json.dumps(msg_dict))

    def _on_danmaku(self, client, message: web_models.DanmakuMessage):
        self.msg_count += 1
        guard = self.GUARD_NAMES.get(message.privilege_type, "")
        tag = f"({guard})" if guard else ""
        print(f"[弹幕] {message.uname}{tag}: {message.msg}")
        self._send({
            "text": message.msg,
            "user": message.uname,
            "msg_type": "danmaku",
            "guard_level": message.privilege_type,
        })

    def _on_gift(self, client, message: web_models.GiftMessage):
        self.msg_count += 1
        # total_coin is in gold/silver coins; gold coins / 1000 = RMB
        price = message.total_coin / 1000 if message.coin_type == "gold" else 0
        print(f"[礼物] {message.uname} 送了 {message.gift_name} x{message.num} (¥{price:.0f})")
        self._send({
            "text": message.gift_name,
            "user": message.uname,
            "msg_type": "gift",
            "num": message.num,
            "price": price,
        })

    def _on_super_chat(self, client, message: web_models.SuperChatMessage):
        self.msg_count += 1
        print(f"[SC ¥{message.price}] {message.uname}: {message.message}")
        self._send({
            "text": message.message,
            "user": message.uname,
            "msg_type": "super_chat",
            "price": message.price,
        })

    def _on_buy_guard(self, client, message: web_models.GuardBuyMessage):
        self.msg_count += 1
        guard_name = self.GUARD_NAMES.get(message.guard_level, "舰长")
        print(f"[上舰] {message.username} 开通了{guard_name}")
        self._send({
            "text": guard_name,
            "user": message.username,
            "msg_type": "guard",
            "guard_level": message.guard_level,
            "price": message.price / 1000,
        })


# ===== Output Consumer (live mode) =====
def live_get_wav_duration(base64_audio):
    """Calculate duration in seconds from base64 WAV audio."""
    try:
        import base64
        from pydub import AudioSegment
        from io import BytesIO
        audio_bytes = base64.b64decode(base64_audio)
        audio = AudioSegment.from_file(BytesIO(audio_bytes), format="wav")
        return len(audio) / 1000.0  # pydub returns milliseconds
    except Exception:
        return 0.0


def live_output_consumer(send_queue, stop_event, stats):
    """Background thread that reads pipeline output."""
    current_response = ""
    current_audio_duration = 0.0
    response_start = None

    while not stop_event.is_set():
        try:
            data = send_queue.get(timeout=1)
            data = json.loads(data)
            signal = data.get("signal", "")
            text = data.get("text", "")
            audio_data = data.get("audio_data", "")

            if signal == "SoS":
                current_response = ""
                current_audio_duration = 0.0
                response_start = time.time()
            elif signal == "EoS":
                if current_response.strip():
                    elapsed = time.time() - response_start if response_start else 0
                    stats["responses"].append({
                        "text": current_response,
                        "time": time.time(),
                        "audio_duration": current_audio_duration,
                        "llm_elapsed": elapsed,
                    })
                    print(
                        f"\n{'='*60}\n"
                        f"[RESPONSE] (audio {current_audio_duration:.1f}s, pipeline {elapsed:.1f}s)\n"
                        f"{current_response}\n"
                        f"{'='*60}"
                    )
                current_response = ""
                current_audio_duration = 0.0
            else:
                if text:
                    current_response += text
                if audio_data:
                    current_audio_duration += live_get_wav_duration(audio_data)
        except Exception:
            pass


# ===== Evaluation (live mode) =====
def live_evaluate(stats):
    responses = stats["responses"]
    if not responses:
        return {"status": "no responses yet"}

    now = time.time()
    recent = [r for r in responses if now - r["time"] < 3600]
    if not recent:
        return {"status": "no recent responses"}

    # Frequency
    if len(recent) > 1:
        span = (recent[-1]["time"] - recent[0]["time"]) / 60
        freq = len(recent) / max(span, 0.1)
    else:
        freq = 0

    # Diversity
    prefixes = [r["text"][:15] for r in recent]
    diversity = len(set(prefixes)) / len(prefixes)

    # TTS duration
    durations = [r["audio_duration"] for r in recent if r["audio_duration"] > 0]
    avg_dur = sum(durations) / max(len(durations), 1)

    # LLM time
    llm_times = [r["llm_elapsed"] for r in recent if r["llm_elapsed"] > 0]
    avg_llm = sum(llm_times) / max(len(llm_times), 1)

    return {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total_responses": len(recent),
        "response_freq_per_min": round(freq, 2),
        "diversity": round(diversity, 3),
        "avg_tts_duration_s": round(avg_dur, 1),
        "avg_llm_time_s": round(avg_llm, 1),
    }


async def run_live():
    print("=" * 60)
    print("VTuber Danmaku Pipeline - Standalone Test")
    print("=" * 60)

    # Create pipeline
    print("\n[PIPELINE] Creating pipeline...")
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    input_queue, send_queue, cancel_queues, threads = live_create_pipeline()
    stop_event = threading.Event()
    print(f"[PIPELINE] Ready! {len(threads)} nodes running.\n")

    # Start output consumer
    stats = {"responses": []}
    consumer_thread = threading.Thread(
        target=live_output_consumer, args=(send_queue, stop_event, stats), daemon=True
    )
    consumer_thread.start()

    # Setup Bilibili
    bili_session = live_create_bilibili_session()
    visited_rooms = set()
    start_time = time.time()
    last_eval = time.time()

    try:
        while True:
            if LIVE_FORCE_ROOM_ID and LIVE_FORCE_ROOM_ID not in visited_rooms:
                room_id, title, uname = LIVE_FORCE_ROOM_ID, "(forced)", "(forced)"
            else:
                room_id, title, uname = await live_find_active_vtuber_room(
                    bili_session, visited_rooms
                )
            if room_id is None:
                print("[WARN] No active rooms, retrying in 30s...")
                await asyncio.sleep(30)
                continue

            visited_rooms.add(room_id)
            print(f"\n[ROOM] Connecting to {room_id}: {uname} - {title}")

            handler = LiveDanmakuForwarder(input_queue)
            blived = blivedm.BLiveClient(room_id, session=bili_session)
            blived.set_handler(handler)
            blived.start()

            try:
                while True:
                    await asyncio.sleep(10)
                    if not blived.is_running:
                        print(f"\n[ROOM] Room {room_id} offline, switching...")
                        break

                    # Hourly eval
                    if time.time() - last_eval >= 3600:
                        report = live_evaluate(stats)
                        last_eval = time.time()
                        print(f"\n[EVAL] {json.dumps(report, indent=2, ensure_ascii=False)}\n")

                    elapsed = (time.time() - start_time) / 60
                    print(
                        f"  [{elapsed:.0f}min] msgs={handler.msg_count} "
                        f"responses={len(stats['responses'])}",
                        end="\r",
                    )
            except KeyboardInterrupt:
                raise
            finally:
                blived.stop()
                await blived.join()

            await asyncio.sleep(5)

    except KeyboardInterrupt:
        print("\n\n[INFO] Shutting down...")
    finally:
        report = live_evaluate(stats)
        print(f"\n[FINAL EVAL] {json.dumps(report, indent=2, ensure_ascii=False)}")
        for cq in cancel_queues:
            cq.put(json.dumps({"signal": "cancel", "timestamp": float("inf")}))
            cq.put(json.dumps({"signal": "kill"}))
        stop_event.set()
        await bili_session.close()
        print("[INFO] Done.")


# =============================================================================
# Mode: server  (from test_vtuber_danmaku.py)
# Real Bilibili + full main server end-to-end via register/websocket.
# =============================================================================

YACHIYO_SERVER = "http://127.0.0.1:8910"
YACHIYO_WS = "ws://127.0.0.1:8910/ws"
SERVER_CLIENT_ID = "vtuber_test"
SERVER_PIPELINE_CONFIG = "vtuber_danmaku"
EVAL_INTERVAL = 3600  # 1 hour in seconds
ROOM_CHECK_INTERVAL = 30  # Check room liveness every 30s
LISTEN_DURATION = None  # None = run forever


# ===== Bilibili Room Discovery (server mode) =====
def server_create_bilibili_session():
    cookies = {"buvid3": str(uuid.uuid4()) + "infoc"}
    return aiohttp.ClientSession(headers=BILIBILI_HEADERS, cookies=cookies)


async def server_find_active_vtuber_room(session, exclude_rooms=None):
    """Find an active VTuber room, excluding recently visited ones."""
    exclude_rooms = exclude_rooms or set()
    params = {
        "platform": "web",
        "parent_area_id": 9,
        "cate_id": 0,
        "area_id": 371,
        "sort_type": "online",
        "page": 1,
        "page_size": 30,
    }
    try:
        async with session.get(BILIBILI_ROOM_LIST_API, params=params) as resp:
            data = await resp.json()
            rooms = data.get("data", [])
            # Filter out excluded rooms
            available = [r for r in rooms if r["roomid"] not in exclude_rooms]
            if not available:
                available = rooms  # Fall back to all rooms
            if not available:
                return None, None, None
            room = random.choice(available[:min(15, len(available))])
            return room["roomid"], room["title"], room["uname"]
    except Exception as e:
        print(f"[ERROR] find_active_vtuber_room: {e}")
        return None, None, None


# ===== YACHIYO Pipeline Client (server mode) =====
class YachiyoClient:
    def __init__(self):
        self.ws = None
        self.connected = False
        self.responses = []  # {data, time}
        self.danmaku_sent = []  # {text, user, msg_type, timestamp}
        self.response_texts = []  # Full response texts (accumulated between SoS/EoS)
        self._current_response = ""
        self._current_response_start = None

    async def setup(self):
        """Register client and init pipeline on YACHIYO server."""
        print(f"[YACHIYO] Registering client '{SERVER_CLIENT_ID}'...")
        try:
            r = requests.post(f"{YACHIYO_SERVER}/register/", json={"client_id": SERVER_CLIENT_ID})
            print(f"[YACHIYO] Register: {r.json()}")
        except Exception as e:
            print(f"[YACHIYO] Register failed: {e}")
            return False

        print(f"[YACHIYO] Initializing pipeline '{SERVER_PIPELINE_CONFIG}'...")
        try:
            r = requests.post(
                f"{YACHIYO_SERVER}/init_pipeline/{SERVER_CLIENT_ID}",
                json={"config": SERVER_PIPELINE_CONFIG, "force": True},
            )
            print(f"[YACHIYO] Init pipeline: {r.json()}")
        except Exception as e:
            print(f"[YACHIYO] Init pipeline failed: {e}")
            return False

        # Wait for pipeline threads to initialize
        print("[YACHIYO] Waiting for pipeline initialization (10s)...")
        await asyncio.sleep(10)

        print(f"[YACHIYO] Connecting WebSocket...")
        try:
            self.ws = await websockets.connect(
                f"{YACHIYO_WS}/{SERVER_CLIENT_ID}", max_size=16 * 1024 * 1024
            )
            self.connected = True
            print("[YACHIYO] WebSocket connected!")
            return True
        except Exception as e:
            print(f"[YACHIYO] WebSocket connection failed: {e}")
            return False

    async def send_danmaku(self, text, user, msg_type="danmaku"):
        """Send a danmaku message to the pipeline."""
        if not self.connected or not self.ws:
            return
        msg = {
            "text": text,
            "user": user,
            "msg_type": msg_type,
            "timestamp": time.time(),
        }
        try:
            await self.ws.send(json.dumps(msg))
            self.danmaku_sent.append(msg)
        except Exception as e:
            print(f"[YACHIYO] Send failed: {e}")
            self.connected = False

    async def receive_loop(self):
        """Background task to receive responses from pipeline."""
        while self.connected:
            try:
                data = await asyncio.wait_for(self.ws.recv(), timeout=5)
                parsed = json.loads(data)
                self.responses.append({"data": parsed, "time": time.time()})

                signal = parsed.get("signal", "")
                text = parsed.get("text", "")

                if signal == "SoS":
                    self._current_response = ""
                    self._current_response_start = time.time()
                elif signal == "EoS":
                    if self._current_response.strip():
                        duration = parsed.get("estimated_duration", 0)
                        resp_len = parsed.get("response_length", 0)
                        elapsed = (
                            time.time() - self._current_response_start
                            if self._current_response_start
                            else 0
                        )
                        self.response_texts.append({
                            "text": self._current_response,
                            "time": time.time(),
                            "estimated_duration": duration,
                            "response_length": resp_len,
                            "llm_elapsed": elapsed,
                        })
                        print(
                            f"\n[RESPONSE] ({duration:.1f}s est, {elapsed:.1f}s LLM) "
                            f"{self._current_response[:200]}"
                        )
                    self._current_response = ""
                elif text:
                    self._current_response += text
                    # Also print action/expression tags
                    action = parsed.get("action", "")
                    expression = parsed.get("expression", "")
                    tags = ""
                    if action:
                        tags += f"[{action}]"
                    if expression:
                        tags += f"({expression})"
                    if tags:
                        print(f"  {tags}{text}", end="", flush=True)
                    else:
                        print(f"  {text}", end="", flush=True)

            except asyncio.TimeoutError:
                continue
            except websockets.exceptions.ConnectionClosed:
                print("[YACHIYO] WebSocket disconnected")
                self.connected = False
                break
            except Exception as e:
                print(f"[YACHIYO] Receive error: {e}")
                await asyncio.sleep(1)

    async def close(self):
        self.connected = False
        if self.ws:
            await self.ws.close()


# ===== blivedm Handler (server mode) =====
class ServerDanmakuForwarder(blivedm.BaseHandler):
    def __init__(self, yachiyo_client, loop):
        self.yachiyo = yachiyo_client
        self.loop = loop
        self.msg_count = 0

    def _on_danmaku(self, client, message: web_models.DanmakuMessage):
        self.msg_count += 1
        print(f"[弹幕] {message.uname}: {message.msg}")
        asyncio.run_coroutine_threadsafe(
            self.yachiyo.send_danmaku(message.msg, message.uname, "danmaku"),
            self.loop,
        )

    def _on_gift(self, client, message: web_models.GiftMessage):
        self.msg_count += 1
        gift_text = f"{message.gift_name} x{message.num}"
        print(f"[礼物] {message.uname} 送了 {gift_text}")
        asyncio.run_coroutine_threadsafe(
            self.yachiyo.send_danmaku(gift_text, message.uname, "gift"),
            self.loop,
        )

    def _on_super_chat(self, client, message: web_models.SuperChatMessage):
        self.msg_count += 1
        print(f"[SC] {message.uname} (¥{message.price}): {message.message}")
        asyncio.run_coroutine_threadsafe(
            self.yachiyo.send_danmaku(message.message, message.uname, "super_chat"),
            self.loop,
        )

    def _on_buy_guard(self, client, message: web_models.GuardBuyMessage):
        self.msg_count += 1
        print(f"[舰长] {message.uname} 开通了舰长")
        asyncio.run_coroutine_threadsafe(
            self.yachiyo.send_danmaku(
                f"guard level {message.guard_level}", message.uname, "guard"
            ),
            self.loop,
        )


# ===== Evaluation (server mode) =====
class Evaluator:
    def __init__(self, yachiyo_client):
        self.yachiyo = yachiyo_client
        self.eval_count = 0

    def evaluate(self):
        """Run hourly evaluation and return report."""
        self.eval_count += 1
        now = time.time()
        hour_ago = now - 3600

        # Recent data
        recent_danmaku = [
            d for d in self.yachiyo.danmaku_sent if d["timestamp"] > hour_ago
        ]
        recent_responses = [
            r for r in self.yachiyo.response_texts if r["time"] > hour_ago
        ]

        # 1. Response frequency
        if recent_responses and len(recent_responses) > 1:
            duration_min = (recent_responses[-1]["time"] - recent_responses[0]["time"]) / 60.0
            freq = len(recent_responses) / max(duration_min, 0.1)
        else:
            freq = len(recent_responses)

        # 2. Response diversity (unique first 15 chars)
        if recent_responses:
            prefixes = [r["text"][:15] for r in recent_responses]
            diversity = len(set(prefixes)) / len(prefixes)
        else:
            diversity = 0

        # 3. Gift acknowledgment (check if gifts in danmaku got thanked)
        gift_danmaku = [d for d in recent_danmaku if d["msg_type"] in ("gift", "guard", "super_chat")]
        gift_acked = 0
        for gift in gift_danmaku:
            # Check responses within 60s after gift
            for r in recent_responses:
                if 0 < (r["time"] - gift["timestamp"]) < 60:
                    text = r["text"]
                    if any(w in text for w in ["谢", "感谢", "哇", "好人", "礼物", "太客气"]):
                        gift_acked += 1
                        break
        gift_ack_rate = gift_acked / max(len(gift_danmaku), 1)

        # 4. TTS duration stats
        durations = [r["estimated_duration"] for r in recent_responses if r["estimated_duration"] > 0]
        avg_duration = sum(durations) / max(len(durations), 1)
        duration_in_range = sum(1 for d in durations if 3 <= d <= 20) / max(len(durations), 1)

        # 5. LLM response time
        llm_times = [r["llm_elapsed"] for r in recent_responses if r["llm_elapsed"] > 0]
        avg_llm_time = sum(llm_times) / max(len(llm_times), 1)

        report = {
            "eval_number": self.eval_count,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "total_danmaku_received": len(recent_danmaku),
            "total_responses": len(recent_responses),
            "response_freq_per_min": round(freq, 2),
            "response_diversity": round(diversity, 3),
            "gift_count": len(gift_danmaku),
            "gift_ack_rate": round(gift_ack_rate, 3),
            "avg_tts_duration_s": round(avg_duration, 1),
            "tts_duration_in_range_ratio": round(duration_in_range, 3),
            "avg_llm_response_time_s": round(avg_llm_time, 1),
        }
        return report


async def run_server():
    print("=" * 60)
    print("VTuber Danmaku Pipeline Test")
    print("=" * 60)

    # Setup YACHIYO pipeline
    yachiyo = YachiyoClient()
    if not await yachiyo.setup():
        print("[FATAL] Failed to setup YACHIYO pipeline. Exiting.")
        return

    # Start WebSocket receive loop
    loop = asyncio.get_event_loop()
    receive_task = asyncio.create_task(yachiyo.receive_loop())

    # Setup evaluator
    evaluator = Evaluator(yachiyo)

    # Setup Bilibili session
    bili_session = server_create_bilibili_session()

    visited_rooms = set()
    last_eval_time = time.time()
    start_time = time.time()

    try:
        while True:
            # Find a room
            room_id, title, uname = await server_find_active_vtuber_room(
                bili_session, visited_rooms
            )
            if room_id is None:
                print("[WARN] No active rooms found, retrying in 30s...")
                await asyncio.sleep(30)
                continue

            visited_rooms.add(room_id)
            print(f"\n{'='*60}")
            print(f"[ROOM] Connecting to {room_id}: {uname} - {title}")
            print(f"{'='*60}")

            # Connect blivedm
            handler = ServerDanmakuForwarder(yachiyo, loop)
            blived = blivedm.BLiveClient(room_id, session=bili_session)
            blived.set_handler(handler)
            blived.start()

            # Monitor room and check for evaluation
            try:
                while True:
                    await asyncio.sleep(ROOM_CHECK_INTERVAL)

                    # Check if room is still alive
                    if not blived.is_running:
                        print(f"\n[ROOM] Room {room_id} disconnected, switching...")
                        break

                    # Check duration limit
                    if LISTEN_DURATION and (time.time() - start_time) > LISTEN_DURATION:
                        print("\n[INFO] Duration limit reached, stopping.")
                        raise KeyboardInterrupt

                    # Hourly evaluation
                    if time.time() - last_eval_time >= EVAL_INTERVAL:
                        report = evaluator.evaluate()
                        last_eval_time = time.time()
                        print(f"\n{'='*60}")
                        print("HOURLY EVALUATION REPORT")
                        print(json.dumps(report, indent=2, ensure_ascii=False))
                        print(f"{'='*60}\n")

                    # Print status every check
                    elapsed_min = (time.time() - start_time) / 60
                    print(
                        f"  [STATUS] {elapsed_min:.0f}min | "
                        f"danmaku={len(yachiyo.danmaku_sent)} | "
                        f"responses={len(yachiyo.response_texts)} | "
                        f"room_msgs={handler.msg_count}",
                        end="\r",
                    )

            except KeyboardInterrupt:
                raise
            finally:
                blived.stop()
                await blived.join()
                print(f"[ROOM] Left room {room_id} (received {handler.msg_count} messages)")

            # Brief pause before switching
            await asyncio.sleep(5)

    except KeyboardInterrupt:
        print("\n\n[INFO] Shutting down...")
    finally:
        # Final evaluation
        if yachiyo.response_texts:
            report = evaluator.evaluate()
            print(f"\n{'='*60}")
            print("FINAL EVALUATION REPORT")
            print(json.dumps(report, indent=2, ensure_ascii=False))
            print(f"{'='*60}")

        receive_task.cancel()
        await yachiyo.close()
        await bili_session.close()
        print("[INFO] Cleanup complete.")


# =============================================================================
# Dispatcher
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Consolidated VTuber / live danmaku test (blivedm | manual | live | server)."
    )
    parser.add_argument(
        "--mode",
        choices=["blivedm", "manual", "live", "server"],
        default="manual",
        help="blivedm: minimal Bilibili connection smoke test (no pipeline, run first); "
             "manual: scripted danmaku into in-process pipeline (no network); "
             "live: real Bilibili into in-process pipeline (no main server); "
             "server: real Bilibili + full main server end-to-end.",
    )
    args = parser.parse_args()

    if args.mode == "blivedm":
        asyncio.run(run_blivedm())
    elif args.mode == "manual":
        run_manual()
    elif args.mode == "live":
        asyncio.run(run_live())
    elif args.mode == "server":
        asyncio.run(run_server())


if __name__ == "__main__":
    main()
