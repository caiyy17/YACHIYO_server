"""
VTuber Danmaku Pipeline Test Script

Connects to a real Bilibili VTuber live room via blivedm,
forwards danmaku to YACHIYO pipeline, receives and evaluates LLM responses.
Auto-switches rooms when streamer goes offline.
Hourly evaluation loop.
"""
import asyncio
import aiohttp
import json
import random
import time
import uuid
import sys
import os
import requests
import websockets

import blivedm
import blivedm.models.web as web_models


# ===== Configuration =====
YACHIYO_SERVER = "http://127.0.0.1:8910"
YACHIYO_WS = "ws://127.0.0.1:8910/ws"
CLIENT_ID = "vtuber_test"
PIPELINE_CONFIG = "vtuber_danmaku"
EVAL_INTERVAL = 3600  # 1 hour in seconds
ROOM_CHECK_INTERVAL = 30  # Check room liveness every 30s
LISTEN_DURATION = None  # None = run forever

BILIBILI_ROOM_LIST_API = "https://api.live.bilibili.com/room/v1/area/getRoomList"
BILIBILI_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept-Encoding": "gzip, deflate",
    "Referer": "https://live.bilibili.com/",
    "Origin": "https://live.bilibili.com",
}


# ===== Bilibili Room Discovery =====
def create_bilibili_session():
    cookies = {"buvid3": str(uuid.uuid4()) + "infoc"}
    return aiohttp.ClientSession(headers=BILIBILI_HEADERS, cookies=cookies)


async def find_active_vtuber_room(session, exclude_rooms=None):
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


# ===== YACHIYO Pipeline Client =====
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
        print(f"[YACHIYO] Registering client '{CLIENT_ID}'...")
        try:
            r = requests.post(f"{YACHIYO_SERVER}/register/", json={"client_id": CLIENT_ID})
            print(f"[YACHIYO] Register: {r.json()}")
        except Exception as e:
            print(f"[YACHIYO] Register failed: {e}")
            return False

        print(f"[YACHIYO] Initializing pipeline '{PIPELINE_CONFIG}'...")
        try:
            r = requests.post(
                f"{YACHIYO_SERVER}/init_pipeline/{CLIENT_ID}",
                json={"config": PIPELINE_CONFIG, "force": True},
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
                f"{YACHIYO_WS}/{CLIENT_ID}", max_size=16 * 1024 * 1024
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


# ===== blivedm Handler =====
class DanmakuForwarder(blivedm.BaseHandler):
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


# ===== Evaluation =====
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


# ===== Main =====
async def main():
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
    bili_session = create_bilibili_session()

    visited_rooms = set()
    last_eval_time = time.time()
    start_time = time.time()

    try:
        while True:
            # Find a room
            room_id, title, uname = await find_active_vtuber_room(
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
            handler = DanmakuForwarder(yachiyo, loop)
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


if __name__ == "__main__":
    asyncio.run(main())
