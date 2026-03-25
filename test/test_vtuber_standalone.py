"""
Standalone VTuber Danmaku Pipeline Test

Directly instantiates pipeline modules in-process (no YACHIO server needed).
Connects to a real Bilibili VTuber room, feeds danmaku through the pipeline,
and prints LLM responses.
"""
import asyncio
import aiohttp
import json
import random
import time
import uuid
import sys
import os
import threading
from queue import Queue

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import blivedm
import blivedm.models.web as web_models

from Modules.danmaku_buffer_vtuber.DanmakuBufferStep import DanmakuBufferStep
from Modules.llm_openai.OpenaiStep import OpenaiStep

# Suppress noisy blivedm logging completely
logging_module = __import__("logging")
logging_module.getLogger("blivedm").setLevel(logging_module.CRITICAL)


# ===== Configuration =====
CLIENT_ID = "vtuber_standalone"
PIPELINE_CONFIG_FILE = "configs/vtuber_danmaku.json"
# Set to a room ID to force connect, or None for auto-discovery
FORCE_ROOM_ID = 26966466  # 栞栞Shiori

BILIBILI_ROOM_LIST_API = "https://api.live.bilibili.com/room/v1/area/getRoomList"
BILIBILI_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept-Encoding": "gzip, deflate",
    "Referer": "https://live.bilibili.com/",
    "Origin": "https://live.bilibili.com",
}


def create_bilibili_session():
    from utils.settings import get_secret
    cookies = {"buvid3": str(uuid.uuid4()) + "infoc"}
    sessdata = get_secret("BILIBILI_SESSDATA", "")
    if sessdata:
        cookies["SESSDATA"] = sessdata
        print("[BILI] Logged in with SESSDATA")
    else:
        print("[BILI] No SESSDATA, usernames will be masked")
    return aiohttp.ClientSession(headers=BILIBILI_HEADERS, cookies=cookies)


async def find_active_vtuber_room(session, exclude_rooms=None):
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


# ===== Pipeline Setup =====
import logging

def setup_logger():
    logger = logging.getLogger(CLIENT_ID)
    logger.setLevel(logging.INFO)
    os.makedirs("logs", exist_ok=True)
    fh = logging.FileHandler(f"logs/client_{CLIENT_ID}.log", mode="w")
    ch = logging.StreamHandler()
    ch.setLevel(logging.WARNING)  # Only warnings/errors to console
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)
    if not logger.hasHandlers():
        logger.addHandler(fh)
        logger.addHandler(ch)
    return logger


def create_pipeline():
    """Create pipeline modules connected by queues, same as YACHIO server does."""
    with open(PIPELINE_CONFIG_FILE, "r") as f:
        config = json.load(f)

    pipeline = config["pipeline"]
    num_nodes = len(pipeline)
    logger = setup_logger()

    # Create queues (num_nodes + 1: input for each node + final output)
    queues = [Queue() for _ in range(num_nodes + 1)]
    cancel_queues = [Queue() for _ in range(num_nodes + 1)]
    send_queue = queues[-1]  # Last queue is the output
    kill_event = threading.Event()

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
            CLIENT_ID,
            logger,
            send_queue,
            queues[i],
            queues[i + 1],
            cancel_queues[i],
            kill_event,
            node_config,
        )
        t = threading.Thread(target=instance.run, name=f"{i}_{func_name}", daemon=True)
        t.start()
        threads.append(t)

    return queues[0], send_queue, kill_event, threads


# ===== blivedm Handler =====
class DanmakuForwarder(blivedm.BaseHandler):
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
        price_yuan = message.total_coin / 1000 if message.coin_type == "gold" else 0
        print(f"[礼物] {message.uname} 送了 {message.gift_name} x{message.num} (¥{price_yuan:.0f})")
        self._send({
            "text": message.gift_name,
            "user": message.uname,
            "msg_type": "gift",
            "gift_num": message.num,
            "price_yuan": price_yuan,
        })

    def _on_super_chat(self, client, message: web_models.SuperChatMessage):
        self.msg_count += 1
        print(f"[SC ¥{message.price}] {message.uname}: {message.message}")
        self._send({
            "text": message.message,
            "user": message.uname,
            "msg_type": "super_chat",
            "price_yuan": message.price,
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
            "price_yuan": message.price / 1000,
        })


# ===== Output Consumer =====
def get_wav_duration(base64_audio):
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


def output_consumer(send_queue, kill_event, stats):
    """Background thread that reads pipeline output."""
    current_response = ""
    current_audio_duration = 0.0
    response_start = None

    while not kill_event.is_set():
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
                    current_audio_duration += get_wav_duration(audio_data)
        except Exception:
            pass


# ===== Evaluation =====
def evaluate(stats):
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


# ===== Main =====
async def main():
    print("=" * 60)
    print("VTuber Danmaku Pipeline - Standalone Test")
    print("=" * 60)

    # Create pipeline
    print("\n[PIPELINE] Creating pipeline...")
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    input_queue, send_queue, kill_event, threads = create_pipeline()
    print(f"[PIPELINE] Ready! {len(threads)} nodes running.\n")

    # Start output consumer
    stats = {"responses": []}
    consumer_thread = threading.Thread(
        target=output_consumer, args=(send_queue, kill_event, stats), daemon=True
    )
    consumer_thread.start()

    # Setup Bilibili
    bili_session = create_bilibili_session()
    visited_rooms = set()
    start_time = time.time()
    last_eval = time.time()

    try:
        while True:
            if FORCE_ROOM_ID and FORCE_ROOM_ID not in visited_rooms:
                room_id, title, uname = FORCE_ROOM_ID, "(forced)", "(forced)"
            else:
                room_id, title, uname = await find_active_vtuber_room(
                    bili_session, visited_rooms
                )
            if room_id is None:
                print("[WARN] No active rooms, retrying in 30s...")
                await asyncio.sleep(30)
                continue

            visited_rooms.add(room_id)
            print(f"\n[ROOM] Connecting to {room_id}: {uname} - {title}")

            handler = DanmakuForwarder(input_queue)
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
                        report = evaluate(stats)
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
        report = evaluate(stats)
        print(f"\n[FINAL EVAL] {json.dumps(report, indent=2, ensure_ascii=False)}")
        kill_event.set()
        await bili_session.close()
        print("[INFO] Done.")


if __name__ == "__main__":
    asyncio.run(main())
