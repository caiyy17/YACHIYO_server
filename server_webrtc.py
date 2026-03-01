"""
Generic WebRTC server that bridges WebRTC clients to the pipeline.

Architecture:
  Client (WebRTC) <-> server_webrtc <-> server_fastapi (WebSocket) <-> Pipeline

Audio-Video-Data Synchronization:
  Audio: 48kHz / 960 = 50fps (20ms per frame)
  Video: 30fps (~33.3ms per frame)
  Data:  20fps (text, motion, etc.)
  GCD(50, 30, 20) = 10 → group rate 10fps → 100ms per group
  Group: 5 audio + 3 video + 2 data = 100ms (atomic sync unit)

  The pipeline's FrameSplitter outputs one message per 100ms group:
    {"audio": ["<pcm1>", ..., "<pcm5>"], "video": ["<jpeg1>", ..., "<jpeg3>"],
     "data": [{...}, null]}

  A group consumer task runs at GROUP_FPS (10fps, 100ms):
    - Dequeue one pipeline group, or fill with empty data (silence/black/null)
    - Fill audio/video/data buffers atomically from the same group
  Audio/video/data consumers independently pop from their buffers at their own fps.

  Sync guarantee:
    - Group-driven filling: all buffers filled atomically from same group
    - Shared start_time: all consumers use the same time origin
    - Absolute timing: frame_time = start + index * ptime (no cumulative error)
    - Single event loop: under load, all tasks delay together, never desync

Standard frame format:
  Input (WebRTC → pipeline):
    {"audio": ["<pcm1>", ..., "<pcm5>"], "video": ["<jpeg1>", ..., "<jpeg3>"],
     "data": [{...}, null], "timestamp": ...}
    {"signal": "vad_start/vad_end", "timestamp": ...}
  Input uses the same group structure as output (5 audio + 3 video + 2 data = 100ms).
  Signals are forwarded immediately via WebSocket (not grouped).
  Output (pipeline → WebRTC):
    {"audio": ["<pcm1>", ..., "<pcm5>"], "video": ["<jpeg1>", ..., "<jpeg3>"],
     "data": [{...}, null]}
    {"signal": "SoS/EoS", "timestamp": ...}

Client is responsible for register/init_pipeline/unregister via server_fastapi's REST API.
server_webrtc only handles WebRTC ↔ pipeline WebSocket bridging.

Usage:
  python server_webrtc.py [--port 18082] [--main-server http://localhost:8000]
"""

import argparse
import asyncio
import base64
import fractions
import io
import json
import logging
import time
from math import gcd
from queue import SimpleQueue, Empty
from collections import deque

import av
import numpy as np
from aiohttp import web
from aiortc import (
    MediaStreamTrack,
    RTCPeerConnection,
    RTCSessionDescription,
)
from PIL import Image

# ============================================================
# Constants
# ============================================================
SAMPLE_RATE = 48000
AUDIO_PTIME = 0.02
AUDIO_SAMPLES = int(SAMPLE_RATE * AUDIO_PTIME)  # 960
AUDIO_TIME_BASE = fractions.Fraction(1, SAMPLE_RATE)

VIDEO_WIDTH, VIDEO_HEIGHT = 320, 240
VIDEO_FPS = 30
VIDEO_CLOCK_RATE = 90000
VIDEO_TIME_BASE = fractions.Fraction(1, VIDEO_CLOCK_RATE)
# Integer division — avoids float precision bug (1/30 * 90000 = 2999.999... in IEEE 754)
VIDEO_TIMESTAMP_INCREMENT = VIDEO_CLOCK_RATE // VIDEO_FPS  # 3000

# Data (text, motion, etc.) frame rate
DATA_FPS = 20

# Sync group: GCD of all fps = group rate, per_group = fps / group_rate
_AUDIO_FPS = SAMPLE_RATE // AUDIO_SAMPLES       # 50
_GROUP_FPS = gcd(gcd(_AUDIO_FPS, VIDEO_FPS), DATA_FPS)  # gcd(50,30,20) = 10
AUDIO_PER_GROUP = _AUDIO_FPS // _GROUP_FPS  # 5
VIDEO_PER_GROUP = VIDEO_FPS // _GROUP_FPS   # 3
DATA_PER_GROUP  = DATA_FPS  // _GROUP_FPS   # 2

# Cancel timestamp offset: cancel signals are shifted back to avoid
# racing with data signals stamped at the same group boundary.
# Cancel offset must exceed one group period (100ms) because cancel and
# vad_start sent in the same client frame may be flushed in different
# server-side group boundaries, up to 100ms apart.
CANCEL_TIMESTAMP_OFFSET = -0.15

logger = logging.getLogger("server_webrtc")


# ============================================================
# GroupDispatcher: buffers for audio/video/data frames
#
# A group consumer task calls fill_next_group() at GROUP_FPS (100ms).
# Each call dequeues one pipeline group (or fills with empty data).
# Audio/video/data consumers pop from buffers at their own fps.
# ============================================================
class GroupDispatcher:
    """Holds audio, video, and data buffers filled by the group consumer task.

    fill_next_group() is called at GROUP_FPS to fill all buffers atomically.
    get_audio()/get_video()/get_data() are called by consumers at their own rate.
    Consumers never trigger unpacking — the group task is the sole driver.
    """

    def __init__(self, group_queue: SimpleQueue, on_signal_callback):
        self._group_queue = group_queue
        self._on_signal = on_signal_callback
        self._audio_buffer = deque()
        self._video_buffer = deque()
        self._data_buffer = deque()
        # Shared by all consumers — set by group consumer on first tick
        self.start_time = None

    def fill_next_group(self, cancel_timestamp=0):
        """Dequeue one media group into buffers, or fill with empty data.
        Signal-only messages fire callback and are skipped.
        Groups with timestamp < cancel_timestamp are discarded."""
        while True:
            try:
                msg = self._group_queue.get_nowait()
            except Empty:
                # No group available — fill with empty data
                self._fill_empty()
                return

            # Discard cancelled groups
            if msg.get("timestamp", float("inf")) < cancel_timestamp:
                continue

            # Handle signal (may coexist with media, or be signal-only)
            if msg.get("signal"):
                self._on_signal(msg)

            has_media = False

            # Unpack audio frames
            audio_list = msg.get("audio", [])
            if isinstance(audio_list, list) and audio_list:
                for a_b64 in audio_list:
                    pcm = np.frombuffer(
                        base64.b64decode(a_b64), dtype=np.int16
                    )
                    if len(pcm) < AUDIO_SAMPLES:
                        pcm = np.pad(pcm, (0, AUDIO_SAMPLES - len(pcm)))
                    elif len(pcm) > AUDIO_SAMPLES:
                        pcm = pcm[:AUDIO_SAMPLES]
                    self._audio_buffer.append(pcm)
                has_media = True

            # Unpack video frames (list of base64 JPEGs)
            video = msg.get("video")
            if isinstance(video, list) and video:
                for v_b64 in video:
                    self._video_buffer.append(v_b64)
                has_media = True
            elif isinstance(video, str) and video:
                self._video_buffer.append(video)
                has_media = True

            # Unpack data frames (list of dict/null)
            data_list = msg.get("data")
            if isinstance(data_list, list):
                for d in data_list:
                    self._data_buffer.append(d)

            if has_media:
                return
            # Signal-only message — continue to next

    def _fill_empty(self):
        """Fill all buffers with one group of empty data."""
        for _ in range(AUDIO_PER_GROUP):
            self._audio_buffer.append(np.zeros(AUDIO_SAMPLES, dtype=np.int16))
        for _ in range(VIDEO_PER_GROUP):
            self._video_buffer.append(None)
        for _ in range(DATA_PER_GROUP):
            self._data_buffer.append(None)

    def get_audio(self):
        """Pop next audio frame. Returns ndarray (silence if buffer empty)."""
        if self._audio_buffer:
            return self._audio_buffer.popleft()
        return np.zeros(AUDIO_SAMPLES, dtype=np.int16)

    def get_video(self):
        """Pop next video frame. Returns base64 str or None."""
        return self._video_buffer.popleft() if self._video_buffer else None

    def get_data(self):
        """Pop next data frame. Returns dict or None."""
        return self._data_buffer.popleft() if self._data_buffer else None


# ============================================================
# Output tracks (pipeline → WebRTC client)
#
# Buffers are filled by the group consumer task at GROUP_FPS.
# Tracks pop from buffers at their own rate.
# Timing: frame_time = start_time + frame_index * ptime
# ============================================================
class OutputAudioTrack(MediaStreamTrack):
    """Consumes audio frames from GroupDispatcher at 50fps (20ms)."""
    kind = "audio"

    def __init__(self, dispatcher: GroupDispatcher):
        super().__init__()
        self._dispatcher = dispatcher
        self._timestamp = 0

    async def recv(self):
        if self._dispatcher.start_time is None:
            self._dispatcher.start_time = time.time()

        wait = self._dispatcher.start_time + (self._timestamp / SAMPLE_RATE) - time.time()
        if wait > 0:
            await asyncio.sleep(wait)

        pcm = self._dispatcher.get_audio()
        frame = av.AudioFrame(format="s16", layout="mono", samples=AUDIO_SAMPLES)
        frame.sample_rate = SAMPLE_RATE
        frame.pts = self._timestamp
        frame.time_base = AUDIO_TIME_BASE
        frame.planes[0].update(pcm.tobytes())
        self._timestamp += AUDIO_SAMPLES
        return frame


class OutputVideoTrack(MediaStreamTrack):
    """Consumes video frames from GroupDispatcher at 30fps (~33.3ms)."""
    kind = "video"

    def __init__(self, dispatcher: GroupDispatcher):
        super().__init__()
        self._dispatcher = dispatcher
        self._timestamp = 0
        self._cached_b64 = None
        self._cached_frame = None
        self._idle_frame = self._make_black_frame()

    def _make_black_frame(self):
        y = np.full((VIDEO_HEIGHT, VIDEO_WIDTH), 16, dtype=np.uint8)
        u = np.full((VIDEO_HEIGHT // 2, VIDEO_WIDTH // 2), 128, dtype=np.uint8)
        v = np.full((VIDEO_HEIGHT // 2, VIDEO_WIDTH // 2), 128, dtype=np.uint8)
        frame = av.VideoFrame(VIDEO_WIDTH, VIDEO_HEIGHT, "yuv420p")
        frame.planes[0].update(y.tobytes())
        frame.planes[1].update(u.tobytes())
        frame.planes[2].update(v.tobytes())
        return frame

    def _decode_image(self, b64_data):
        """Decode base64 JPEG to av.VideoFrame (yuv420p). Caches repeated frames."""
        if b64_data == self._cached_b64 and self._cached_frame is not None:
            return self._cached_frame
        try:
            img = Image.open(io.BytesIO(base64.b64decode(b64_data))).convert("RGB")
            if img.size != (VIDEO_WIDTH, VIDEO_HEIGHT):
                img = img.resize((VIDEO_WIDTH, VIDEO_HEIGHT))
            rgb_frame = av.VideoFrame.from_ndarray(np.array(img), format="rgb24")
            frame = rgb_frame.reformat(format="yuv420p")
            self._cached_b64 = b64_data
            self._cached_frame = frame
            return frame
        except Exception:
            return self._idle_frame

    async def recv(self):
        if self._dispatcher.start_time is None:
            self._dispatcher.start_time = time.time()

        wait = self._dispatcher.start_time + (self._timestamp / VIDEO_CLOCK_RATE) - time.time()
        if wait > 0:
            await asyncio.sleep(wait)

        b64 = self._dispatcher.get_video()
        if b64 is not None:
            frame = self._decode_image(b64)
        else:
            frame = self._idle_frame

        frame.pts = self._timestamp
        frame.time_base = VIDEO_TIME_BASE
        self._timestamp += VIDEO_TIMESTAMP_INCREMENT
        return frame


# ============================================================
# WebRTC Client Session
# ============================================================
class WebRTCSession:
    """Manages one WebRTC client's bidirectional connection to the pipeline."""

    def __init__(self, client_id, main_server_url, on_session_end=None):
        self.client_id = client_id
        self.main_server_url = main_server_url
        self.main_ws_url = main_server_url.replace("http", "ws", 1) + "/ws"

        self.pc = RTCPeerConnection()
        self.group_queue = SimpleQueue()  # Groups from pipeline
        self.ws = None
        self.ws_ready = asyncio.Event()
        self._closed = asyncio.Event()  # Signaled on cleanup to unblock waiters
        self.dc_server = None
        self.connected = False
        self._on_session_end = on_session_end  # Callback to remove from server's sessions
        self._input_video_buffer = deque()  # Client video frames waiting to be grouped
        self._input_video_event = asyncio.Event()  # Signaled when new video frame arrives
        self._last_video_frame = None  # Last received video frame (for drop fill)
        self._input_data_buffer = deque()  # Client data frames waiting to be grouped
        self._input_data_event = asyncio.Event()  # Signaled when new data frame arrives
        self._input_signal_buffer = deque()  # Client signals waiting for next group boundary
        self.cancel_timestamp = 0  # Output-side cancel filtering

    def _on_signal(self, msg):
        """Called when a signal-only message (SoS/EoS) is unpacked by dispatcher."""
        if not self.dc_server or self.dc_server.readyState != "open":
            return
        msg.pop("timestamp", None)
        self.dc_server.send(json.dumps(msg))

    def _send_data(self, data):
        """Send a single data frame via DataChannel."""
        if not self.dc_server or self.dc_server.readyState != "open":
            return
        if isinstance(data, dict):
            data.pop("timestamp", None)
        self.dc_server.send(json.dumps(data))

    async def _group_consumer(self, dispatcher):
        """Fill dispatcher buffers at GROUP_FPS (100ms).
        This is the sole driver of group unpacking — consumers never trigger it."""
        if dispatcher.start_time is None:
            dispatcher.start_time = time.time()

        group_index = 0
        group_period = 1.0 / _GROUP_FPS
        while self.connected:
            target = dispatcher.start_time + group_index * group_period
            wait = target - time.time()
            if wait > 0:
                await asyncio.sleep(wait)

            dispatcher.fill_next_group(self.cancel_timestamp)
            group_index += 1

    async def _dispatch_data(self, dispatcher):
        """Consume data frames from dispatcher at DATA_FPS.
        Uses absolute timing from dispatcher.start_time (same as audio/video tracks)."""
        while dispatcher.start_time is None and self.connected:
            await asyncio.sleep(0.005)
        if not self.connected:
            return

        frame_index = 0
        ptime = 1.0 / DATA_FPS
        while self.connected:
            target = dispatcher.start_time + frame_index * ptime
            wait = target - time.time()
            if wait > 0:
                await asyncio.sleep(wait)

            data = dispatcher.get_data()
            # None (null slot or empty group) → send {}; dict → send as-is
            self._send_data(data if data else {})

            frame_index += 1

    async def handle_offer(self, offer_sdp, offer_type):
        """Process WebRTC offer, set up tracks, return answer."""
        offer = RTCSessionDescription(sdp=offer_sdp, type=offer_type)

        # All tracks share the same dispatcher (and thus the same start_time)
        dispatcher = GroupDispatcher(self.group_queue, self._on_signal)
        out_audio = OutputAudioTrack(dispatcher)
        out_video = OutputVideoTrack(dispatcher)
        self.pc.addTrack(out_audio)
        self.pc.addTrack(out_video)

        # Group consumer fills buffers at GROUP_FPS; data dispatched at DATA_FPS
        self._group_task = asyncio.ensure_future(self._group_consumer(dispatcher))
        self._data_task = asyncio.ensure_future(self._dispatch_data(dispatcher))

        self.dc_server = self.pc.createDataChannel("server-data", ordered=True)

        @self.dc_server.on("open")
        def on_dc_open():
            logger.info(f"[{self.client_id}] Server DC opened")

        @self.pc.on("track")
        def on_track(track):
            logger.info(f"[{self.client_id}] Received {track.kind} track")
            if track.kind == "audio":
                asyncio.ensure_future(self._relay_audio_input(track))
            elif track.kind == "video":
                asyncio.ensure_future(self._relay_video_input(track))

        @self.pc.on("datachannel")
        def on_datachannel(channel):
            logger.info(f"[{self.client_id}] Client DC: {channel.label}")

            @channel.on("message")
            def on_message(raw_msg):
                asyncio.ensure_future(self._forward_dc_message(raw_msg))

        @self.pc.on("connectionstatechange")
        async def on_state():
            state = self.pc.connectionState
            logger.info(f"[{self.client_id}] Connection: {state}")
            if state in ("failed", "closed"):
                await self.cleanup()

        # Set connected before setRemoteDescription — on_track fires during
        # setRemoteDescription, and relay coroutines check self.connected.
        self.connected = True
        await self.pc.setRemoteDescription(offer)
        answer = await self.pc.createAnswer()
        await self.pc.setLocalDescription(answer)

        asyncio.ensure_future(self._pipeline_session())

        return self.pc.localDescription.sdp, self.pc.localDescription.type

    async def _forward_dc_message(self, raw_msg):
        """Handle DataChannel message from client.
        Signal messages (vad_start/vad_end) → forward immediately to pipeline.
        Data messages → buffer for group inclusion."""
        try:
            msg = json.loads(raw_msg)
        except json.JSONDecodeError:
            return

        if msg.get("signal"):
            # Signal → buffer for next group boundary
            self._input_signal_buffer.append(raw_msg)
        else:
            # Data frame → buffer for group inclusion
            self._input_data_buffer.append(msg)
            self._input_data_event.set()

    async def _relay_audio_input(self, track):
        """Buffer audio frames, wait for matching video/data, send as group.
        Group: 5 audio + 3 video + 2 data (audio-driven, 100ms).
        Timeout prevents stalling on dropped video frames."""
        await self.ws_ready.wait()
        logger.info(f"[{self.client_id}] Audio relay started")

        audio_group = []
        try:
            while self.connected:
                av_frame = await track.recv()
                pcm = av_frame.to_ndarray().flatten().astype(np.int16)
                layout = av_frame.layout.name if av_frame.layout else "mono"
                if layout == "stereo":
                    pcm = pcm[::2]

                audio_group.append(
                    base64.b64encode(pcm.tobytes()).decode("ascii")
                )

                if len(audio_group) < AUDIO_PER_GROUP:
                    continue

                # 5 audio frames ready — wait for 3 video frames
                # Timeout = one video frame period (~33ms) to handle drops
                timeout = 1.0 / VIDEO_FPS
                while len(self._input_video_buffer) < VIDEO_PER_GROUP:
                    try:
                        await asyncio.wait_for(
                            self._input_video_event.wait(), timeout=timeout
                        )
                        self._input_video_event.clear()
                    except asyncio.TimeoutError:
                        break  # Video frame likely dropped, send what we have

                # Drop stale video frames, keep at most VIDEO_PER_GROUP
                while len(self._input_video_buffer) > VIDEO_PER_GROUP:
                    self._input_video_buffer.popleft()

                video_group = []
                for _ in range(VIDEO_PER_GROUP):
                    if self._input_video_buffer:
                        video_group.append(self._input_video_buffer.popleft())

                # Fill dropped frames with last received frame
                while len(video_group) < VIDEO_PER_GROUP and self._last_video_frame:
                    video_group.append(self._last_video_frame)

                # Wait for data frames (same logic as video)
                data_timeout = 1.0 / DATA_FPS
                while len(self._input_data_buffer) < DATA_PER_GROUP:
                    try:
                        await asyncio.wait_for(
                            self._input_data_event.wait(), timeout=data_timeout
                        )
                        self._input_data_event.clear()
                    except asyncio.TimeoutError:
                        break

                # Drop stale data frames, keep at most DATA_PER_GROUP
                while len(self._input_data_buffer) > DATA_PER_GROUP:
                    self._input_data_buffer.popleft()
                data_group = []
                for _ in range(DATA_PER_GROUP):
                    if self._input_data_buffer:
                        data_group.append(self._input_data_buffer.popleft())
                    else:
                        data_group.append(None)

                # Flush pending signals at group boundary
                while self._input_signal_buffer:
                    sig = json.loads(self._input_signal_buffer.popleft())
                    sig["timestamp"] = time.time()
                    if sig.get("signal") == "cancel":
                        sig["timestamp"] += CANCEL_TIMESTAMP_OFFSET
                        self.cancel_timestamp = sig["timestamp"]
                    await self.ws.send(json.dumps(sig))

                msg = {
                    "audio": audio_group,
                    "timestamp": time.time(),
                }
                if video_group:
                    msg["video"] = video_group
                msg["data"] = data_group

                await self.ws.send(json.dumps(msg))
                audio_group = []
        except Exception as e:
            logger.info(f"[{self.client_id}] Audio relay ended: {e}")

    async def _relay_video_input(self, track):
        """Buffer video frames for audio relay to pack into groups."""
        logger.info(f"[{self.client_id}] Video input started")

        try:
            while self.connected:
                frame = await track.recv()
                img = Image.fromarray(frame.to_ndarray(format="rgb24"))
                buf = io.BytesIO()
                img.save(buf, format="JPEG", quality=85)
                b64_frame = base64.b64encode(buf.getvalue()).decode("ascii")
                self._input_video_buffer.append(b64_frame)
                self._last_video_frame = b64_frame
                self._input_video_event.set()
        except Exception as e:
            logger.info(f"[{self.client_id}] Video input ended: {e}")

    def _notify_client(self, signal, message=""):
        """Send a signal to client via DataChannel (best-effort)."""
        try:
            if self.dc_server and self.dc_server.readyState == "open":
                msg = {"signal": signal}
                if message:
                    msg["message"] = message
                self.dc_server.send(json.dumps(msg))
        except Exception:
            pass

    async def _pipeline_session(self):
        """Connect WebSocket to pipeline and relay responses.
        Client is responsible for register/init_pipeline/unregister via main server."""
        try:
            import websockets
            async with websockets.connect(
                f"{self.main_ws_url}/{self.client_id}",
                max_size=1024 * 1024 * 16,
            ) as ws:
                self.ws = ws
                self.ws_ready.set()
                logger.info(f"[{self.client_id}] WebSocket connected")

                await self._relay_pipeline_output(ws)

        except Exception as e:
            logger.error(f"[{self.client_id}] Pipeline session error: {e}")
            self._notify_client("error", f"Pipeline connection failed: {e}")
        finally:
            self.ws = None
            self.ws_ready.clear()
            # Pipeline gone — clean up entire session
            await self.cleanup()

    async def _relay_pipeline_output(self, ws):
        """Receive pipeline messages and queue complete groups."""
        try:
            while self.connected:
                raw = await asyncio.wait_for(ws.recv(), timeout=60)
                msg = json.loads(raw)
                self.group_queue.put(msg)

                if msg.get("signal") == "EoS":
                    logger.info(f"[{self.client_id}] EoS received (queued)")

        except asyncio.TimeoutError:
            logger.info(f"[{self.client_id}] Pipeline response timeout")
            self._notify_client("error", "Pipeline response timeout")
        except Exception as e:
            if self.connected:  # Only notify if not already cleaning up
                logger.info(f"[{self.client_id}] Pipeline relay ended: {e}")
                self._notify_client("error", f"Pipeline disconnected: {e}")

    async def cleanup(self):
        if self._closed.is_set():
            return  # Already cleaned up
        self._closed.set()
        self.connected = False
        logger.info(f"[{self.client_id}] Cleaning up session")

        # Cancel async tasks
        for task in (getattr(self, '_group_task', None),
                     getattr(self, '_data_task', None)):
            if task and not task.done():
                task.cancel()

        # Close pipeline WebSocket
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass
            self.ws = None
        self.ws_ready.clear()

        # Close WebRTC connection
        if self.pc:
            try:
                await self.pc.close()
            except Exception:
                pass

        # Remove from server's session list
        if self._on_session_end:
            self._on_session_end(self.client_id)


# ============================================================
# WebRTC Server
# ============================================================
class WebRTCServer:
    def __init__(self, main_server_url):
        self.main_server_url = main_server_url
        self.sessions = {}

    async def handle_offer(self, request):
        """POST /offer/{client_id} - WebRTC signaling endpoint."""
        client_id = request.match_info["client_id"]
        body = await request.json()

        if client_id in self.sessions:
            await self.sessions[client_id].cleanup()

        session = WebRTCSession(
            client_id, self.main_server_url,
            on_session_end=lambda cid: self.sessions.pop(cid, None),
        )
        self.sessions[client_id] = session

        sdp, type_ = await session.handle_offer(body["sdp"], body["type"])

        logger.info(f"[{client_id}] WebRTC session created")
        return web.json_response({"sdp": sdp, "type": type_})

    async def handle_status(self, request):
        """GET /status - Server status."""
        sessions = {
            cid: {"connected": s.connected}
            for cid, s in self.sessions.items()
        }
        return web.json_response({
            "status": "running",
            "sessions": sessions,
        })

    async def cleanup(self):
        for session in self.sessions.values():
            await session.cleanup()
        self.sessions.clear()


def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def main():
    parser = argparse.ArgumentParser(description="Generic WebRTC Server")
    parser.add_argument("--port", type=int, default=18082)
    parser.add_argument("--main-server", default="http://localhost:8000")
    args = parser.parse_args()

    setup_logger()

    server = WebRTCServer(args.main_server)

    app = web.Application()
    app.router.add_post("/offer/{client_id}", server.handle_offer)
    app.router.add_get("/status", server.handle_status)

    async def on_shutdown(app):
        await server.cleanup()

    app.on_shutdown.append(on_shutdown)

    logger.info(f"WebRTC server starting on port {args.port}")
    logger.info(f"  Main server: {args.main_server}")
    web.run_app(app, host="0.0.0.0", port=args.port)


if __name__ == "__main__":
    main()
