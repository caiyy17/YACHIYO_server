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
import base64
import json
import os
import random
import sys
import time
import threading
import uuid
import logging
import wave
from io import BytesIO
from queue import Empty, Queue

import requests
import websockets
from websockets.exceptions import ConnectionClosed

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
HTTP_TIMEOUT = 30
PIPELINE_STOP_TIMEOUT = 40


def wav_duration(base64_audio):
    """Decode one base64 WAV chunk, raising on malformed audio."""
    audio_bytes = base64.b64decode(base64_audio, validate=True)
    with wave.open(BytesIO(audio_bytes), "rb") as wav_file:
        frame_rate = wav_file.getframerate()
        if frame_rate <= 0:
            raise ValueError("WAV frame rate must be positive")
        return wav_file.getnframes() / frame_rate


def stop_pipeline(cancel_queues, threads, timeout=PIPELINE_STOP_TIMEOUT):
    """Stop every pipeline node and confirm every worker actually exited."""
    for cancel_queue in cancel_queues:
        cancel_queue.put(json.dumps({"signal": "cancel", "timestamp": float("inf")}))
        cancel_queue.put(json.dumps({"signal": "kill"}))

    deadline = time.monotonic() + timeout
    for thread in threads:
        thread.join(max(0, deadline - time.monotonic()))
    alive = [thread.name for thread in threads if thread.is_alive()]
    if alive:
        print(f"[FAIL] Pipeline threads did not stop: {alive}")
        return False
    return True


def cleanup_local_test_log(client_id):
    """Close this test logger and remove only its per-run log file."""
    logger = logging.getLogger(client_id)
    for handler in list(logger.handlers):
        handler.close()
        logger.removeHandler(handler)

    log_path = os.path.join("logs", f"client_{client_id}.log")
    try:
        os.remove(log_path)
    except FileNotFoundError:
        pass
    except OSError as e:
        print(f"[FAIL] Could not remove test log {log_path}: {e}")
        return False
    return True


async def stop_blivedm(client, timeout=15):
    """Stop a blivedm client and confirm its background task exited."""
    client.stop()
    try:
        await asyncio.wait_for(client.join(), timeout=timeout)
    except asyncio.CancelledError:
        print("[FAIL] blivedm cleanup was cancelled")
        return False
    except Exception as e:
        print(f"[FAIL] blivedm cleanup failed: {type(e).__name__}: {e}")
        return False
    if client.is_running:
        print("[FAIL] blivedm client is still running after join")
        return False
    return True


def blivedm_create_bilibili_session():
    """Create aiohttp session with headers and cookies to pass Bilibili anti-crawler."""
    cookies = {"buvid3": str(uuid.uuid4()) + "infoc"}
    return aiohttp.ClientSession(
        headers=BILIBILI_HEADERS,
        cookies=cookies,
        timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
    )


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
        resp.raise_for_status()
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
    session = None
    try:
        session = blivedm_create_bilibili_session()
        print("Searching for active VTuber rooms...")
        room_id, title, uname = await blivedm_find_active_vtuber_room(session)
        if room_id is None:
            return False

        print(f"\nConnecting to room {room_id}: {uname} - {title}")
        print(f"Listening for {BLIVEDM_LISTEN_DURATION} seconds...\n{'='*60}")

        handler = BlivedmTestHandler()
        client = blivedm.BLiveClient(room_id, session=session)
        client.set_handler(handler)

        cleanup_ok = True
        try:
            client.start()
            await asyncio.sleep(BLIVEDM_LISTEN_DURATION)
        finally:
            cleanup_ok = await stop_blivedm(client)

        if not cleanup_ok:
            return False

        print(f"\n{'='*60}")
        print(f"Total messages received: {handler.msg_count}")
        if handler.msg_count == 0:
            print("[FAIL] No live messages received")
            return False
        return True
    except Exception as e:
        print(f"[FAIL] blivedm mode failed: {e}")
        return False
    finally:
        if session is not None:
            await session.close()


# =============================================================================
# Mode: manual  (from test_vtuber_manual.py)
# Scripted/crafted danmaku fed into the pipeline directly; deterministic.
# =============================================================================

MANUAL_CLIENT_ID = f"vtuber_manual_test_{uuid.uuid4().hex}"
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
    try:
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
            if instance.init_error:
                raise RuntimeError(f"{func_name}: {instance.init_error}")
            t = threading.Thread(target=instance.run, name=f"{i}_{func_name}", daemon=True)
            t.start()
            threads.append(t)
    except Exception:
        stop_pipeline(cancel_queues, threads)
        raise

    return queues[0], send_queue, cancel_queues, threads


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
            raw = send_queue.get(timeout=1)
        except Empty:
            continue

        data = json.loads(raw)
        signal = data.get("signal", "")
        text = data.get("text", "")

        if signal == "SoS":
            started = True
            response_text = ""
            audio_chunks = 0
        elif signal == "EoS":
            if not started:
                raise RuntimeError("received EoS before SoS")
            if not response_text.strip():
                raise RuntimeError("received an empty response")
            if "timestamp" not in data:
                raise RuntimeError("EoS is missing its batch timestamp")
            return response_text.strip(), audio_chunks, data["timestamp"]
        elif started:
            if text:
                response_text += text
            if data.get("audio_data"):
                wav_duration(data["audio_data"])
                audio_chunks += 1
    raise TimeoutError("response did not complete with EoS")


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
    try:
        response, audio_count, batch_timestamp = manual_collect_response(
            send_queue, timeout=30
        )
        print(f"\n  RESPONSE ({audio_count} audio chunks):")
        print(f"  {response}")
        input_queue.put(json.dumps({
            "signal": "playback_complete",
            "timestamp": time.time(),
            "last_batch_timestamp": batch_timestamp,
        }))
        success = True
    except Exception as e:
        print(f"\n  [FAIL] {e}")
        success = False
    print(f"{'='*60}")

    # Wait for pipeline to fully flush before next scenario
    time.sleep(3)
    manual_drain_queue(send_queue)
    return success


def run_manual():
    print("Manual VTuber Pipeline Test")
    print("Creating pipeline...")
    try:
        input_queue, send_queue, cancel_queues, threads = manual_create_pipeline()
    except Exception as e:
        print(f"[FAIL] Pipeline setup failed: {e}")
        cleanup_local_test_log(MANUAL_CLIENT_ID)
        return False

    success = False
    try:
        print("Pipeline ready. Waiting 3s for init...\n")
        time.sleep(3)
        results = []

        # ===== Scenario 1: Normal chat =====
        results.append(manual_run_scenario("普通聊天", input_queue, send_queue, [
            {"text": "优酱今天吃了什么", "user": "路人A"},
            {"text": "好无聊啊", "user": "路人B"},
        ], wait_before=2))

        # ===== Scenario 2: Gift =====
        results.append(manual_run_scenario("礼物感谢", input_queue, send_queue, [
            {"text": "小花花", "user": "大佬甲", "msg_type": "gift",
             "num": 10, "price": 1},
        ], wait_before=0))  # gift is immediate priority

        # ===== Scenario 3: Super Chat =====
        results.append(manual_run_scenario("SC必须读", input_queue, send_queue, [
            {"text": "优酱能唱一首歌吗？我超喜欢你的声音！", "user": "土豪君",
             "msg_type": "super_chat", "price": 50},
        ], wait_before=0))

        # ===== Scenario 4: Guard purchase =====
        results.append(manual_run_scenario("上舰感谢", input_queue, send_queue, [
            {"text": "舰长", "user": "新舰长", "msg_type": "guard",
             "guard_level": 3, "price": 198},
        ], wait_before=0))

        # ===== Scenario 5: Guard member chat =====
        results.append(manual_run_scenario("舰长发言+普通弹幕", input_queue, send_queue, [
            {"text": "今天的直播好有趣", "user": "老舰长", "guard_level": 3},
            {"text": "同意楼上", "user": "路人F"},
        ], wait_before=2))

        # ===== Scenario 6: Fishing/troll danmaku =====
        results.append(manual_run_scenario("钓鱼弹幕", input_queue, send_queue, [
            {"text": "我送了一百个舰长", "user": "钓鱼佬"},
            {"text": "我也上舰了快感谢我", "user": "骗子"},
        ], wait_before=2))

        # ===== Scenario 7: Duplicate messages (trending) =====
        results.append(manual_run_scenario("刷屏趋势", input_queue, send_queue, [
            {"text": "唱歌！", "user": "粉丝A"},
            {"text": "唱歌！", "user": "粉丝B"},
        ], wait_before=2))

        # ===== Scenario 8: Embarrassing SC =====
        results.append(manual_run_scenario("羞耻SC", input_queue, send_queue, [
            {"text": "优酱我喜欢你❤能做我女朋友吗", "user": "痴汉",
             "msg_type": "super_chat", "price": 30},
        ], wait_before=0))

        success = all(results)
        print("\n\nAll scenarios complete!")
        if not success:
            print(f"[FAIL] {results.count(False)} scenario(s) failed")
    except Exception as e:
        print(f"[FAIL] Manual mode failed: {e}")
    finally:
        pipeline_ok = stop_pipeline(cancel_queues, threads)
        log_ok = cleanup_local_test_log(MANUAL_CLIENT_ID)
        cleanup_ok = pipeline_ok and log_ok

    return success and cleanup_ok


# =============================================================================
# Mode: live  (from test_vtuber_standalone.py)
# Connects to a real Bilibili room, feeds live danmaku directly into the
# in-process pipeline, no main server.
# =============================================================================

LIVE_CLIENT_ID = f"vtuber_standalone_{uuid.uuid4().hex}"
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
    return aiohttp.ClientSession(
        headers=BILIBILI_HEADERS,
        cookies=cookies,
        timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
    )


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
            resp.raise_for_status()
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
        raise


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
    try:
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
            if instance.init_error:
                raise RuntimeError(f"{func_name}: {instance.init_error}")
            t = threading.Thread(target=instance.run, name=f"{i}_{func_name}", daemon=True)
            t.start()
            threads.append(t)
    except Exception:
        stop_pipeline(cancel_queues, threads)
        raise

    return queues[0], send_queue, cancel_queues, threads


# ===== blivedm Handler (live mode) =====
class LiveDanmakuForwarder(blivedm.BaseHandler):
    def __init__(self, input_queue, stats):
        self.input_queue = input_queue
        self.stats = stats
        self.msg_count = 0

    GUARD_NAMES = {0: "", 1: "总督", 2: "提督", 3: "舰长"}

    def _send(self, msg_dict):
        self.stats["messages"] += 1
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
    return wav_duration(base64_audio)


def live_output_consumer(input_queue, send_queue, stop_event, stats):
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
                if response_start is None:
                    raise RuntimeError("received EoS before SoS")
                if not current_response.strip():
                    raise RuntimeError("received an empty response")
                if "timestamp" not in data:
                    raise RuntimeError("EoS is missing its batch timestamp")
                elapsed = time.time() - response_start
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
                response_start = None
                input_queue.put(json.dumps({
                    "signal": "playback_complete",
                    "timestamp": time.time(),
                    "last_batch_timestamp": data["timestamp"],
                }))
            else:
                if text:
                    current_response += text
                if audio_data:
                    current_audio_duration += live_get_wav_duration(audio_data)
        except Empty:
            continue
        except Exception as e:
            stats["errors"].append(f"output receiver failed: {type(e).__name__}: {e}")
            return


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
    try:
        input_queue, send_queue, cancel_queues, threads = live_create_pipeline()
    except Exception as e:
        print(f"[FAIL] Pipeline setup failed: {e}")
        cleanup_local_test_log(LIVE_CLIENT_ID)
        return False

    stop_event = threading.Event()
    print(f"[PIPELINE] Ready! {len(threads)} nodes running.\n")

    # Start output consumer
    stats = {"messages": 0, "responses": [], "errors": []}
    consumer_thread = threading.Thread(
        target=live_output_consumer,
        args=(input_queue, send_queue, stop_event, stats),
        daemon=True,
    )
    try:
        consumer_thread.start()
    except Exception as e:
        print(f"[FAIL] Output consumer setup failed: {e}")
        stop_pipeline(cancel_queues, threads)
        cleanup_local_test_log(LIVE_CLIENT_ID)
        return False

    # Setup Bilibili
    bili_session = None
    visited_rooms = set()
    start_time = time.time()
    last_eval = time.time()
    run_error = None

    try:
        bili_session = live_create_bilibili_session()
        while True:
            if stats["errors"]:
                raise RuntimeError(stats["errors"][-1])
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

            handler = LiveDanmakuForwarder(input_queue, stats)
            blived = blivedm.BLiveClient(room_id, session=bili_session)
            blived.set_handler(handler)
            blived.start()

            try:
                while True:
                    await asyncio.sleep(10)
                    if stats["errors"]:
                        raise RuntimeError(stats["errors"][-1])
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
                if not await stop_blivedm(blived):
                    raise RuntimeError("blivedm client cleanup failed")

            await asyncio.sleep(5)

    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\n\n[INFO] Shutting down...")
    except Exception as e:
        run_error = e
        print(f"\n[FAIL] Live mode failed: {e}")
    finally:
        report = live_evaluate(stats)
        print(f"\n[FINAL EVAL] {json.dumps(report, indent=2, ensure_ascii=False)}")
        stop_event.set()
        consumer_thread.join(5)
        consumer_ok = not consumer_thread.is_alive()
        if not consumer_ok:
            print("[FAIL] Output consumer thread did not stop")
        pipeline_ok = stop_pipeline(cancel_queues, threads)
        session_ok = True
        if bili_session is not None:
            try:
                await bili_session.close()
            except Exception as e:
                session_ok = False
                print(f"[FAIL] Bilibili session cleanup failed: {e}")
        log_ok = cleanup_local_test_log(LIVE_CLIENT_ID)
        print("[INFO] Done.")

    if stats["messages"] == 0:
        print("[FAIL] No live messages received")
    if not stats["responses"]:
        print("[FAIL] No pipeline responses received")
    for error in stats["errors"]:
        print(f"[FAIL] {error}")
    return (
        run_error is None
        and stats["messages"] > 0
        and bool(stats["responses"])
        and not stats["errors"]
        and consumer_ok
        and pipeline_ok
        and session_ok
        and log_ok
    )


# =============================================================================
# Mode: server  (from test_vtuber_danmaku.py)
# Real Bilibili + full main server end-to-end via register/websocket.
# =============================================================================

YACHIYO_SERVER = "http://127.0.0.1:8910"
YACHIYO_WS = "ws://127.0.0.1:8910/ws"
SERVER_CLIENT_ID = f"vtuber_test_{uuid.uuid4().hex}"
SERVER_PIPELINE_CONFIG = "unity_chan_live"
EVAL_INTERVAL = 3600  # 1 hour in seconds
ROOM_CHECK_INTERVAL = 30  # Check room liveness every 30s
LISTEN_DURATION = None  # None = run forever


# ===== Bilibili Room Discovery (server mode) =====
def server_create_bilibili_session():
    cookies = {"buvid3": str(uuid.uuid4()) + "infoc"}
    return aiohttp.ClientSession(
        headers=BILIBILI_HEADERS,
        cookies=cookies,
        timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
    )


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
            resp.raise_for_status()
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
        raise


# ===== YACHIYO Pipeline Client (server mode) =====
class YachiyoClient:
    def __init__(self):
        self.ws = None
        self.connected = False
        self.registered = False
        self.closing = False
        self.receiver_error = None
        self.send_futures = set()
        self.send_lock = threading.Lock()
        self.responses = []  # {data, time}
        self.danmaku_sent = []  # {text, user, msg_type, timestamp}
        self.response_texts = []  # Full response texts (accumulated between SoS/EoS)
        self._current_response = ""
        self._current_response_start = None

    async def setup(self):
        """Register client and init pipeline on YACHIYO server."""
        print(f"[YACHIYO] Registering client '{SERVER_CLIENT_ID}'...")
        try:
            r = await asyncio.to_thread(
                requests.post,
                f"{YACHIYO_SERVER}/register/",
                json={"client_id": SERVER_CLIENT_ID},
                timeout=HTTP_TIMEOUT,
            )
            r.raise_for_status()
            self.registered = True
            print(f"[YACHIYO] Register: {r.json()}")
        except Exception as e:
            print(f"[YACHIYO] Register failed: {e}")
            return False

        print(f"[YACHIYO] Initializing pipeline '{SERVER_PIPELINE_CONFIG}'...")
        try:
            r = await asyncio.to_thread(
                requests.post,
                f"{YACHIYO_SERVER}/init_pipeline/{SERVER_CLIENT_ID}",
                json={"config": SERVER_PIPELINE_CONFIG},
                timeout=180,
            )
            r.raise_for_status()
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
                f"{YACHIYO_WS}/{SERVER_CLIENT_ID}",
                max_size=16 * 1024 * 1024,
                open_timeout=HTTP_TIMEOUT,
                close_timeout=10,
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
            self.receiver_error = f"send failed: {type(e).__name__}: {e}"
            self.connected = False

    def track_send(self, future):
        with self.send_lock:
            self.send_futures.add(future)

        def done(completed):
            with self.send_lock:
                self.send_futures.discard(completed)
            try:
                completed.result()
            except Exception as e:
                self.receiver_error = f"send task failed: {type(e).__name__}: {e}"

        future.add_done_callback(done)

    async def wait_for_sends(self):
        with self.send_lock:
            pending = list(self.send_futures)
        if not pending:
            return True
        try:
            await asyncio.wait_for(
                asyncio.gather(*(asyncio.wrap_future(future) for future in pending)),
                timeout=10,
            )
            return True
        except asyncio.CancelledError:
            for future in pending:
                future.cancel()
            print("[FAIL] Send task cleanup was cancelled")
            return False
        except Exception as e:
            for future in pending:
                future.cancel()
            print(f"[FAIL] Send task cleanup failed: {type(e).__name__}: {e}")
            return False

    async def receive_loop(self):
        """Background task to receive responses from pipeline."""
        while self.connected:
            try:
                data = await asyncio.wait_for(self.ws.recv(), timeout=5)
                parsed = json.loads(data)
                if not isinstance(parsed, dict):
                    raise TypeError("receiver message must be a JSON object")
                if parsed.get("audio_data"):
                    wav_duration(parsed["audio_data"])
                self.responses.append({"data": parsed, "time": time.time()})

                signal = parsed.get("signal", "")
                text = parsed.get("text", "")

                if signal == "SoS":
                    self._current_response = ""
                    self._current_response_start = time.time()
                elif signal == "EoS":
                    if self._current_response_start is None:
                        raise RuntimeError("received EoS before SoS")
                    if not self._current_response.strip():
                        raise RuntimeError("received an empty response")
                    if "timestamp" not in parsed:
                        raise RuntimeError("EoS is missing its batch timestamp")
                    duration = parsed.get("estimated_duration", 0)
                    resp_len = parsed.get("response_length", 0)
                    elapsed = time.time() - self._current_response_start
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
                    self._current_response_start = None
                    await self.ws.send(json.dumps({
                        "signal": "playback_complete",
                        "timestamp": time.time(),
                        "last_batch_timestamp": parsed["timestamp"],
                    }))
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
            except ConnectionClosed as e:
                print("[YACHIYO] WebSocket disconnected")
                if not self.closing:
                    self.receiver_error = f"WebSocket disconnected: {e}"
                self.connected = False
                break
            except Exception as e:
                print(f"[YACHIYO] Receive error: {e}")
                self.receiver_error = f"receiver failed: {type(e).__name__}: {e}"
                self.connected = False
                break

    async def close(self):
        cleanup_ok = await self.wait_for_sends()
        self.closing = True
        self.connected = False
        if self.ws:
            try:
                await asyncio.wait_for(self.ws.close(), timeout=10)
            except Exception as e:
                cleanup_ok = False
                print(f"[FAIL] WebSocket cleanup failed: {e}")
            self.ws = None
        if self.registered:
            try:
                response = await asyncio.to_thread(
                    requests.post,
                    f"{YACHIYO_SERVER}/unregister/",
                    json={"client_id": SERVER_CLIENT_ID},
                    timeout=HTTP_TIMEOUT,
                )
                response.raise_for_status()
                print(f"[YACHIYO] Unregister: {response.json()}")
                self.registered = False
            except Exception as e:
                cleanup_ok = False
                print(f"[FAIL] Unregister failed: {e}")
        try:
            os.remove(os.path.join("logs", f"client_{SERVER_CLIENT_ID}.log"))
        except FileNotFoundError:
            pass
        except OSError as e:
            cleanup_ok = False
            print(f"[FAIL] Test log cleanup failed: {e}")
        return cleanup_ok


# ===== blivedm Handler (server mode) =====
class ServerDanmakuForwarder(blivedm.BaseHandler):
    def __init__(self, yachiyo_client, loop):
        self.yachiyo = yachiyo_client
        self.loop = loop
        self.msg_count = 0

    def _on_danmaku(self, client, message: web_models.DanmakuMessage):
        self.msg_count += 1
        print(f"[弹幕] {message.uname}: {message.msg}")
        self.yachiyo.track_send(asyncio.run_coroutine_threadsafe(
            self.yachiyo.send_danmaku(message.msg, message.uname, "danmaku"),
            self.loop,
        ))

    def _on_gift(self, client, message: web_models.GiftMessage):
        self.msg_count += 1
        gift_text = f"{message.gift_name} x{message.num}"
        print(f"[礼物] {message.uname} 送了 {gift_text}")
        self.yachiyo.track_send(asyncio.run_coroutine_threadsafe(
            self.yachiyo.send_danmaku(gift_text, message.uname, "gift"),
            self.loop,
        ))

    def _on_super_chat(self, client, message: web_models.SuperChatMessage):
        self.msg_count += 1
        print(f"[SC] {message.uname} (¥{message.price}): {message.message}")
        self.yachiyo.track_send(asyncio.run_coroutine_threadsafe(
            self.yachiyo.send_danmaku(message.message, message.uname, "super_chat"),
            self.loop,
        ))

    def _on_buy_guard(self, client, message: web_models.GuardBuyMessage):
        self.msg_count += 1
        print(f"[舰长] {message.uname} 开通了舰长")
        self.yachiyo.track_send(asyncio.run_coroutine_threadsafe(
            self.yachiyo.send_danmaku(
                f"guard level {message.guard_level}", message.uname, "guard"
            ),
            self.loop,
        ))


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
        await yachiyo.close()
        return False

    # Start WebSocket receive loop
    loop = asyncio.get_event_loop()
    receive_task = asyncio.create_task(yachiyo.receive_loop())

    # Setup evaluator
    evaluator = Evaluator(yachiyo)

    # Setup Bilibili session
    bili_session = None

    visited_rooms = set()
    last_eval_time = time.time()
    start_time = time.time()
    run_error = None

    try:
        bili_session = server_create_bilibili_session()
        while True:
            if yachiyo.receiver_error:
                raise RuntimeError(yachiyo.receiver_error)
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
                    if yachiyo.receiver_error:
                        raise RuntimeError(yachiyo.receiver_error)

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
                if not await stop_blivedm(blived):
                    raise RuntimeError("blivedm client cleanup failed")
                print(f"[ROOM] Left room {room_id} (received {handler.msg_count} messages)")

            # Brief pause before switching
            await asyncio.sleep(5)

    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\n\n[INFO] Shutting down...")
    except Exception as e:
        run_error = e
        print(f"\n[FAIL] Server mode failed: {e}")
    finally:
        # Final evaluation
        if yachiyo.response_texts:
            report = evaluator.evaluate()
            print(f"\n{'='*60}")
            print("FINAL EVALUATION REPORT")
            print(json.dumps(report, indent=2, ensure_ascii=False))
            print(f"{'='*60}")

        receive_task.cancel()
        task_ok = True
        try:
            await asyncio.wait_for(receive_task, timeout=10)
        except asyncio.CancelledError:
            pass
        except asyncio.TimeoutError:
            task_ok = False
            print("[FAIL] Receiver task did not stop")
        except Exception as e:
            task_ok = False
            print(f"[FAIL] Receiver task cleanup failed: {e}")
        cleanup_ok = await yachiyo.close()
        session_ok = True
        if bili_session is not None:
            try:
                await bili_session.close()
            except Exception as e:
                session_ok = False
                print(f"[FAIL] Bilibili session cleanup failed: {e}")
        print("[INFO] Cleanup complete.")

    if not yachiyo.danmaku_sent:
        print("[FAIL] No live messages were sent to YACHIYO")
    if not yachiyo.response_texts:
        print("[FAIL] No complete pipeline responses received")
    if yachiyo.receiver_error:
        print(f"[FAIL] {yachiyo.receiver_error}")
    return (
        run_error is None
        and bool(yachiyo.danmaku_sent)
        and bool(yachiyo.response_texts)
        and yachiyo.receiver_error is None
        and task_ok
        and cleanup_ok
        and session_ok
    )


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

    try:
        if args.mode == "blivedm":
            success = asyncio.run(run_blivedm())
        elif args.mode == "manual":
            success = run_manual()
        elif args.mode == "live":
            success = asyncio.run(run_live())
        else:
            success = asyncio.run(run_server())
    except Exception as e:
        print(f"[FAIL] {args.mode} mode crashed: {type(e).__name__}: {e}")
        success = False
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
