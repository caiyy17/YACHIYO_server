"""
Consolidated VTuber / live danmaku test script.

Three modes (select with --mode). Every pipeline test runs on the REAL
server path (register + init_pipeline + WebSocket) — the input source is
the only thing that varies:
- blivedm : Minimal Bilibili connection smoke test. Finds an active VTuber room,
            connects via blivedm, prints/counts danmaku for ~60s. No pipeline, no
            main server. Run this FIRST to verify the Bilibili connection works
            before the heavier real-config modes.
- manual  : Scripted/crafted danmaku over the real server. Deterministic
            scenario suite (batching, priorities, playback_complete pacing).
- server  : Real Bilibili danmaku + full main server end-to-end.
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
from pathlib import Path
from queue import Empty, Queue

import requests
import websockets
from websockets.exceptions import ConnectionClosed

import blivedm
import blivedm.models.web as web_models

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PROJECT_ROOT = Path(__file__).resolve().parent.parent

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


def cleanup_client_artifacts(client_id):
    ok = True
    paths = (
        PROJECT_ROOT / "history" / f"history_{client_id}.json",
        PROJECT_ROOT / "logs" / f"client_{client_id}.log",
    )
    for path in paths:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except OSError as error:
            ok = False
            print(f"[FAIL] Artifact cleanup failed for {path}: {error}")
    return ok


def wav_duration(base64_audio):
    """Decode one base64 WAV chunk, raising on malformed audio."""
    audio_bytes = base64.b64decode(base64_audio, validate=True)
    with wave.open(BytesIO(audio_bytes), "rb") as wav_file:
        frame_rate = wav_file.getframerate()
        if frame_rate <= 0:
            raise ValueError("WAV frame rate must be positive")
        return wav_file.getnframes() / frame_rate


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
# Mode: manual
# Scripted/crafted danmaku over the REAL server (register + init_pipeline +
# WebSocket): deterministic scenarios on the production path — entry
# validation, event routing and the send loop are all the real thing.
# =============================================================================

MANUAL_CLIENT_ID = f"vtuber_manual_test_{uuid.uuid4().hex}"
MANUAL_PIPELINE_CONFIG = "unity_chan_live"


class ManualSession:
    """Real-server session for the scripted scenarios. A background thread
    pumps the WebSocket into recv_queue (the same .get() interface the
    collector reads); send() is thread-safe from the scenario thread."""

    def __init__(self):
        self.recv_queue = Queue()
        self.loop = None
        self.ws = None
        self.thread = None
        self.ready = threading.Event()
        self.error = None
        self.registered = False

    def start(self):
        r = requests.post(f"{YACHIYO_SERVER}/register/",
                          json={"client_id": MANUAL_CLIENT_ID},
                          timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        self.registered = True
        print(f"[YACHIYO] Register: {r.json()}")
        r = requests.post(f"{YACHIYO_SERVER}/init_pipeline/{MANUAL_CLIENT_ID}",
                          json={"config": MANUAL_PIPELINE_CONFIG},
                          timeout=180)
        r.raise_for_status()
        print(f"[YACHIYO] Init pipeline: {r.json()}")
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        if not self.ready.wait(timeout=HTTP_TIMEOUT) or self.error:
            raise RuntimeError(f"WebSocket connect failed: {self.error}")

    def _run(self):
        asyncio.run(self._pump())

    async def _pump(self):
        try:
            async with websockets.connect(
                f"{YACHIYO_WS}/{MANUAL_CLIENT_ID}",
                max_size=16 * 1024 * 1024,
            ) as ws:
                self.ws = ws
                self.loop = asyncio.get_running_loop()
                self.ready.set()
                async for raw in ws:
                    self.recv_queue.put(raw)
        except ConnectionClosed:
            pass
        except Exception as e:
            self.error = e
            self.ready.set()

    def send(self, msg):
        fut = asyncio.run_coroutine_threadsafe(
            self.ws.send(json.dumps(msg)), self.loop)
        fut.result(timeout=HTTP_TIMEOUT)

    def close(self):
        """Close the socket (the server disposes the pipeline) and
        unregister. Best effort: cleanup must not mask a test failure."""
        ok = True
        try:
            if self.loop and self.ws:
                asyncio.run_coroutine_threadsafe(
                    self.ws.close(), self.loop).result(timeout=10)
            if self.thread:
                self.thread.join(timeout=10)
                ok = ok and not self.thread.is_alive()
        except Exception as e:
            print(f"[FAIL] session close: {e}")
            ok = False
        if self.registered:
            try:
                response = requests.post(
                    f"{YACHIYO_SERVER}/unregister/",
                    json={"client_id": MANUAL_CLIENT_ID},
                    timeout=HTTP_TIMEOUT,
                )
                response.raise_for_status()
                self.registered = False
            except Exception as e:
                print(f"[FAIL] unregister: {e}")
                ok = False
        artifacts_ok = cleanup_client_artifacts(MANUAL_CLIENT_ID)
        return ok and artifacts_ok


def manual_send_msg(session, text, user, msg_type="danmaku", **kwargs):
    msg = {"text": text, "user": user, "msg_type": msg_type, "timestamp": time.time()}
    msg.update(kwargs)
    session.send(msg)


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
        # text rides flat on classic sentence messages, and under
        # pass_data on stream envelopes (item_SoS)
        text = data.get("text", "") \
            or (data.get("pass_data") or {}).get("text", "")

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


def manual_run_scenario(name, session, messages, wait_before=0):
    manual_drain_queue(session.recv_queue)
    print(f"\n{'='*60}")
    print(f"SCENARIO: {name}")
    print(f"{'='*60}")

    # Send all messages
    for msg in messages:
        manual_send_msg(session, **msg)
        print(f"  → {msg.get('msg_type', 'danmaku')}: {msg.get('user', '?')}: {msg.get('text', '')}")
        time.sleep(0.1)

    if wait_before > 0:
        print(f"  (waiting {wait_before}s for batch release...)")
        time.sleep(wait_before)

    # Collect response
    try:
        response, audio_count, batch_timestamp = manual_collect_response(
            session.recv_queue, timeout=30
        )
        print(f"\n  RESPONSE ({audio_count} audio chunks):")
        print(f"  {response}")
        # the pacing ack rides the real path: entry -> event handler
        session.send({
            "signal": "playback_complete",
            "timestamp": time.time(),
            "last_batch_timestamp": batch_timestamp,
        })
        success = True
    except Exception as e:
        print(f"\n  [FAIL] {e}")
        success = False
    print(f"{'='*60}")

    # Wait for pipeline to fully flush before next scenario
    time.sleep(3)
    manual_drain_queue(session.recv_queue)
    return success


def run_manual():
    print("Manual VTuber Pipeline Test (real server)")
    session = ManualSession()
    try:
        session.start()
    except Exception as e:
        print(f"[FAIL] Session setup failed: {e}")
        session.close()
        return False

    success = False
    try:
        results = []

        # ===== Scenario 1: Normal chat =====
        results.append(manual_run_scenario("普通聊天", session, [
            {"text": "优酱今天吃了什么", "user": "路人A"},
            {"text": "好无聊啊", "user": "路人B"},
        ], wait_before=2))

        # ===== Scenario 2: Gift =====
        results.append(manual_run_scenario("礼物感谢", session, [
            {"text": "小花花", "user": "大佬甲", "msg_type": "gift",
             "num": 10, "price": 1},
        ], wait_before=0))  # gift is immediate priority

        # ===== Scenario 3: Super Chat =====
        results.append(manual_run_scenario("SC必须读", session, [
            {"text": "优酱能唱一首歌吗？我超喜欢你的声音！", "user": "土豪君",
             "msg_type": "super_chat", "price": 50},
        ], wait_before=0))

        # ===== Scenario 4: Guard purchase =====
        results.append(manual_run_scenario("上舰感谢", session, [
            {"text": "舰长", "user": "新舰长", "msg_type": "guard",
             "guard_level": 3, "price": 198},
        ], wait_before=0))

        # ===== Scenario 5: Guard member chat =====
        results.append(manual_run_scenario("舰长发言+普通弹幕", session, [
            {"text": "今天的直播好有趣", "user": "老舰长", "guard_level": 3},
            {"text": "同意楼上", "user": "路人F"},
        ], wait_before=2))

        # ===== Scenario 6: Fishing/troll danmaku =====
        results.append(manual_run_scenario("钓鱼弹幕", session, [
            {"text": "我送了一百个舰长", "user": "钓鱼佬"},
            {"text": "我也上舰了快感谢我", "user": "骗子"},
        ], wait_before=2))

        # ===== Scenario 7: Duplicate messages (trending) =====
        results.append(manual_run_scenario("刷屏趋势", session, [
            {"text": "唱歌！", "user": "粉丝A"},
            {"text": "唱歌！", "user": "粉丝B"},
        ], wait_before=2))

        # ===== Scenario 8: Embarrassing SC =====
        results.append(manual_run_scenario("羞耻SC", session, [
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
        cleanup_ok = session.close()

    return success and cleanup_ok


# Bilibili API constants (shared by the blivedm and server modes)
BILIBILI_ROOM_LIST_API = "https://api.live.bilibili.com/room/v1/area/getRoomList"
BILIBILI_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept-Encoding": "gzip, deflate",
    "Referer": "https://live.bilibili.com/",
    "Origin": "https://live.bilibili.com",
}


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
                # text rides flat on classic sentence messages, and under
                # pass_data on stream envelopes (item_SoS)
                carried = parsed.get("pass_data") or {}
                text = parsed.get("text", "") or carried.get("text", "")

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
                    action = parsed.get("action", "") \
                        or carried.get("action_hint", "")
                    expression = parsed.get("expression", "") \
                        or carried.get("expression", "")
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
        artifacts_ok = cleanup_client_artifacts(SERVER_CLIENT_ID)
        return cleanup_ok and artifacts_ok


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
        description="Consolidated VTuber / live danmaku test (blivedm | manual | server)."
    )
    parser.add_argument(
        "--mode",
        choices=["blivedm", "manual", "server"],
        default="manual",
        help="blivedm: minimal Bilibili connection smoke test (no pipeline, run first); "
             "manual: scripted danmaku over the real server (deterministic); "
             "server: real Bilibili + full main server end-to-end.",
    )
    args = parser.parse_args()

    try:
        if args.mode == "blivedm":
            success = asyncio.run(run_blivedm())
        elif args.mode == "manual":
            success = run_manual()
        else:
            success = asyncio.run(run_server())
    except Exception as e:
        print(f"[FAIL] {args.mode} mode crashed: {type(e).__name__}: {e}")
        success = False
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
