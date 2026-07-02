"""
Generic WebRTC server that bridges WebRTC clients to the pipeline.

Architecture:
  Client (WebRTC) <-> server_webrtc <-> server_fastapi (WebSocket) <-> Pipeline

Audio-Video-Data Synchronization:
  All frame rates are configurable via WebRTCSession init. Default:
    Audio: 48kHz / 960 = 50fps (20ms per frame)
    Video: 30fps (~33.3ms per frame)
    Data:  20fps (text, motion, etc.)
    GCD(50, 30, 20) = 10 → group rate 10fps → 100ms per group
    Group: 5 audio + 3 video + 2 data = 100ms (atomic sync unit)

  The pipeline's FrameSplitter outputs one message per group period:
    {"audio": ["<pcm1>", ..., "<pcm5>"], "video": ["<jpeg1>", ..., "<jpeg3>"],
     "data": [{...}, null]}

  A group consumer task runs at group_fps:
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
    {"audio": ["<pcm1>", ...], "video": ["<jpeg1>", ...],
     "data": [{...}, null], "timestamp": ...}
    {"signal": "recording_start/recording_end", "timestamp": ...}
  Input uses the same group structure as output.
  Signals are forwarded immediately via WebSocket (not grouped).
  Output (pipeline → WebRTC):
    {"audio": ["<pcm1>", ...], "video": ["<jpeg1>", ...],
     "data": [{...}, null]}
    {"signal": "SoS/EoS", "timestamp": ...}

Client is responsible for register/init_pipeline/unregister via server_fastapi's REST API.
server_webrtc only handles WebRTC ↔ pipeline WebSocket bridging.

Usage:
  python server_webrtc.py [--port 15168] [--main-server http://localhost:8910]
"""

import argparse
import asyncio
import base64
import fractions
import io
import json
import logging
import os
import time
from math import gcd
from queue import Queue, Empty
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
# Default constants (can be overridden per-session)
# ============================================================
DEFAULT_SAMPLE_RATE = 48000
DEFAULT_AUDIO_PTIME = 0.02
DEFAULT_VIDEO_FPS = 30
DEFAULT_VIDEO_WIDTH = 320
DEFAULT_VIDEO_HEIGHT = 240
DEFAULT_DATA_FPS = 20
DEFAULT_STARTUP_BUFFER = 2
DEFAULT_CONSUMER_OFFSET = 0.005   # 5ms: consumer fills buffer before track consumes
DEFAULT_ASSEMBLER_OFFSET = 0.05   # 50ms: assembler waits for frames to arrive

VIDEO_CLOCK_RATE = 90000  # RTP clock rate for video (fixed by RTP spec)

# Cancel timestamp offset: cancel signals are shifted back to avoid
# racing with data signals stamped at the same group boundary.
# Cancel offset must exceed one group period (100ms) because cancel and
# recording_start sent in the same client frame may be flushed in different
# server-side group boundaries, up to 100ms apart.
CANCEL_TIMESTAMP_OFFSET = -0.15

logger = logging.getLogger("server_webrtc")
stats_logger = logging.getLogger("webrtc_stats")


# ============================================================
# GroupDispatcher: buffers for audio/video/data frames
#
# A group consumer task calls fill_next_group() at group_fps.
# Each call dequeues one pipeline group (or fills with empty data).
# Audio/video/data consumers pop from buffers at their own fps.
# ============================================================
class GroupDispatcher:
    """Holds audio, video, and data buffers filled by the group consumer task.

    fill_next_group() is called at group_fps to fill all buffers atomically.
    get_audio()/get_video()/get_data() are called by consumers at their own rate.
    Consumers never trigger unpacking — the group task is the sole driver.
    """

    def __init__(self, group_queue, on_signal_callback,
                 audio_samples, audio_per_group, video_per_group, data_per_group):
        self._group_queue = group_queue
        self._on_signal = on_signal_callback
        self._audio_buffer = deque()
        self._video_buffer = deque()
        self._data_buffer = deque()
        # Shared by all consumers — set by group consumer on first tick
        self.start_time = None

        self._audio_samples = audio_samples
        self._audio_per_group = audio_per_group
        self._video_per_group = video_per_group
        self._data_per_group = data_per_group

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
                return False

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
                    if len(pcm) < self._audio_samples:
                        pcm = np.pad(pcm, (0, self._audio_samples - len(pcm)))
                    elif len(pcm) > self._audio_samples:
                        pcm = pcm[:self._audio_samples]
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
                return True
            # Signal-only message — continue to next

    def _fill_empty(self):
        """Fill all buffers with one group of empty data."""
        for _ in range(self._audio_per_group):
            self._audio_buffer.append(np.zeros(self._audio_samples, dtype=np.int16))
        for _ in range(self._video_per_group):
            self._video_buffer.append(None)
        for _ in range(self._data_per_group):
            self._data_buffer.append(None)

    def get_audio(self):
        """Pop next audio frame. Returns ndarray (silence if buffer empty)."""
        if self._audio_buffer:
            return self._audio_buffer.popleft()
        return np.zeros(self._audio_samples, dtype=np.int16)

    def get_video(self):
        """Pop next video frame. Returns base64 str or None."""
        return self._video_buffer.popleft() if self._video_buffer else None

    def get_data(self):
        """Pop next data frame. Returns dict or None."""
        return self._data_buffer.popleft() if self._data_buffer else None


# ============================================================
# Output tracks (pipeline → WebRTC client)
#
# Buffers are filled by the group consumer task at group_fps.
# Tracks pop from buffers at their own rate.
# Timing: frame_time = start_time + frame_index * ptime
# ============================================================
class OutputAudioTrack(MediaStreamTrack):
    """Consumes audio frames from GroupDispatcher at audio_fps."""
    kind = "audio"

    def __init__(self, dispatcher, sample_rate, audio_samples, client_id=""):
        super().__init__()
        self._dispatcher = dispatcher
        self._timestamp = 0
        self._sample_rate = sample_rate
        self._audio_samples = audio_samples
        self._time_base = fractions.Fraction(1, sample_rate)
        self._client_id = client_id
        # Stats (log every 10s)
        self._stats_interval = sample_rate * 10  # 480000 samples = 500 frames
        self._max_jitter_ms = 0.0
        self._empty_count = 0
        self._frame_count = 0

    async def recv(self):
        if self._dispatcher.start_time is None:
            self._dispatcher.start_time = time.time()

        target = self._dispatcher.start_time + (self._timestamp / self._sample_rate)
        wait = target - time.time()
        if wait > 0:
            await asyncio.sleep(wait)

        jitter_ms = (time.time() - target) * 1000
        if abs(jitter_ms) > abs(self._max_jitter_ms):
            self._max_jitter_ms = jitter_ms
        self._frame_count += 1
        if not self._dispatcher._audio_buffer:
            self._empty_count += 1

        pcm = self._dispatcher.get_audio()
        frame = av.AudioFrame(format="s16", layout="mono", samples=self._audio_samples)
        frame.sample_rate = self._sample_rate
        frame.pts = self._timestamp
        frame.time_base = self._time_base
        frame.planes[0].update(pcm.tobytes())
        self._timestamp += self._audio_samples

        if self._timestamp % self._stats_interval == 0:
            elapsed = time.time() - self._dispatcher.start_time
            stats_logger.info(
                f"audio_out client={self._client_id} "
                f"t={elapsed:.1f}s frames={self._timestamp // self._audio_samples} "
                f"buf_empty={self._empty_count}/{self._frame_count} "
                f"jitter_max={self._max_jitter_ms:.1f}ms"
            )
            self._max_jitter_ms = 0.0
            self._empty_count = 0
            self._frame_count = 0

        return frame


class OutputVideoTrack(MediaStreamTrack):
    """Consumes video frames from GroupDispatcher at video_fps."""
    kind = "video"

    def __init__(self, dispatcher, video_fps, video_width, video_height, client_id=""):
        super().__init__()
        self._dispatcher = dispatcher
        self._timestamp = 0
        self._video_fps = video_fps
        self._video_width = video_width
        self._video_height = video_height
        self._ts_increment = VIDEO_CLOCK_RATE // video_fps
        self._time_base = fractions.Fraction(1, VIDEO_CLOCK_RATE)
        self._cached_b64 = None
        self._cached_frame = None
        self._idle_frame = self._make_black_frame()
        self._client_id = client_id
        # Stats (log every 10s)
        self._stats_interval = VIDEO_CLOCK_RATE * 10  # 900000 ticks = 300 frames
        self._max_jitter_ms = 0.0
        self._idle_count = 0
        self._frame_count = 0

    def _make_black_frame(self):
        y = np.full((self._video_height, self._video_width), 16, dtype=np.uint8)
        u = np.full((self._video_height // 2, self._video_width // 2), 128, dtype=np.uint8)
        v = np.full((self._video_height // 2, self._video_width // 2), 128, dtype=np.uint8)
        frame = av.VideoFrame(self._video_width, self._video_height, "yuv420p")
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
            if img.size != (self._video_width, self._video_height):
                img = img.resize((self._video_width, self._video_height))
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

        target = self._dispatcher.start_time + (self._timestamp / VIDEO_CLOCK_RATE)
        wait = target - time.time()
        if wait > 0:
            await asyncio.sleep(wait)

        jitter_ms = (time.time() - target) * 1000
        if abs(jitter_ms) > abs(self._max_jitter_ms):
            self._max_jitter_ms = jitter_ms
        self._frame_count += 1

        b64 = self._dispatcher.get_video()
        if b64 is not None:
            frame = self._decode_image(b64)
        else:
            frame = self._idle_frame
            self._idle_count += 1

        frame.pts = self._timestamp
        frame.time_base = self._time_base
        self._timestamp += self._ts_increment

        if self._timestamp % self._stats_interval == 0:
            elapsed = time.time() - self._dispatcher.start_time
            stats_logger.info(
                f"video_out client={self._client_id} "
                f"t={elapsed:.1f}s frames={self._timestamp // self._ts_increment} "
                f"idle={self._idle_count}/{self._frame_count} "
                f"jitter_max={self._max_jitter_ms:.1f}ms"
            )
            self._max_jitter_ms = 0.0
            self._idle_count = 0
            self._frame_count = 0

        return frame


# ============================================================
# WebRTC Client Session
# ============================================================
class WebRTCSession:
    """Manages one WebRTC client's bidirectional connection to the pipeline."""

    def __init__(self, client_id, main_server_url,
                 sample_rate=DEFAULT_SAMPLE_RATE,
                 audio_ptime=DEFAULT_AUDIO_PTIME,
                 video_fps=DEFAULT_VIDEO_FPS,
                 video_width=DEFAULT_VIDEO_WIDTH,
                 video_height=DEFAULT_VIDEO_HEIGHT,
                 data_fps=DEFAULT_DATA_FPS,
                 startup_buffer=DEFAULT_STARTUP_BUFFER,
                 consumer_offset=DEFAULT_CONSUMER_OFFSET,
                 assembler_offset=DEFAULT_ASSEMBLER_OFFSET,
                 on_session_end=None):
        self.client_id = client_id
        self.main_server_url = main_server_url
        self.main_ws_url = main_server_url.replace("http", "ws", 1) + "/ws"

        # Audio params
        self.sample_rate = sample_rate
        self.audio_samples = int(sample_rate * audio_ptime)

        # Video params
        self.video_fps = video_fps
        self.video_width = video_width
        self.video_height = video_height

        # Data params
        self.data_fps = data_fps

        # Group structure (GCD-based)
        audio_fps = sample_rate // self.audio_samples
        group_fps = gcd(gcd(audio_fps, video_fps), data_fps)
        self.audio_per_group = audio_fps // group_fps
        self.video_per_group = video_fps // group_fps
        self.data_per_group = data_fps // group_fps
        self.group_period = 1.0 / group_fps

        # Startup buffer and timing offsets
        self.startup_buffer = startup_buffer
        self.consumer_offset = consumer_offset
        self.assembler_offset = assembler_offset

        logger.info(
            f"[{client_id}] Session params: "
            f"audio={sample_rate}Hz/{self.audio_samples}samp, "
            f"video={video_fps}fps/{video_width}x{video_height}, "
            f"data={data_fps}fps, "
            f"group={self.audio_per_group}a+{self.video_per_group}v+{self.data_per_group}d "
            f"({self.group_period*1000:.0f}ms)"
        )

        # Connection state
        self.pc = RTCPeerConnection()
        self.group_queue = Queue()  # Groups from pipeline
        self.ws = None
        self.ws_ready = asyncio.Event()
        self._closed = asyncio.Event()  # Signaled on cleanup to unblock waiters
        self.dc_server = None
        self.connected = False
        self._on_session_end = on_session_end  # Callback to remove from server's sessions
        # Input buffers: each receiver appends (pts, data) pairs independently
        self._input_audio_buffer = deque()   # (pts, b64_pcm) from audio track
        self._input_video_buffer = deque()   # (pts, b64_jpeg) from video track
        self._last_video_frame = None        # Last received video frame (for drop fill)
        self._input_data_buffer = deque()    # data dicts from DataChannel
        self._input_signal_buffer = deque()  # Client signals waiting for next group boundary
        self.cancel_timestamp = 0            # Output-side cancel filtering
        self._audio_pts_origin = None        # First audio PTS (for gap detection)

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
        """Fill dispatcher buffers at group rate.
        This is the sole driver of group unpacking — consumers never trigger it.
        Startup: fills empty until group_queue has enough groups (jitter buffer).
        consumer_offset: fills slightly before track consumes to avoid race."""
        if dispatcher.start_time is None:
            dispatcher.start_time = time.time()

        group_index = 0
        buffering = True
        buffering_deadline = dispatcher.start_time + self.startup_buffer * self.group_period * 5

        # Stats
        _si = 100  # log every 100 groups (~10s)
        _empty = 0
        _max_jitter_ms = 0.0
        _max_qsize = 0

        while self.connected:
            target = dispatcher.start_time + group_index * self.group_period - self.consumer_offset
            wait = target - time.time()
            if wait > 0:
                await asyncio.sleep(wait)

            jitter_ms = (time.time() - target) * 1000
            if abs(jitter_ms) > abs(_max_jitter_ms):
                _max_jitter_ms = jitter_ms
            qs = self.group_queue.qsize()
            if qs > _max_qsize:
                _max_qsize = qs

            if buffering:
                if (qs >= self.startup_buffer
                        or time.time() > buffering_deadline):
                    buffering = False
                    logger.info(
                        f"[{self.client_id}] Jitter buffer primed "
                        f"({qs} groups)"
                    )
                else:
                    dispatcher._fill_empty()
                    _empty += 1
                    group_index += 1
                    continue

            got_media = dispatcher.fill_next_group(self.cancel_timestamp)
            if not got_media:
                _empty += 1
            group_index += 1

            if group_index % _si == 0:
                elapsed = time.time() - dispatcher.start_time
                stats_logger.info(
                    f"consumer client={self.client_id} "
                    f"t={elapsed:.1f}s idx={group_index} "
                    f"qsize={qs} qsize_max={_max_qsize} "
                    f"abuf={len(dispatcher._audio_buffer)} "
                    f"vbuf={len(dispatcher._video_buffer)} "
                    f"dbuf={len(dispatcher._data_buffer)} "
                    f"empty={_empty}/{_si} "
                    f"jitter_max={_max_jitter_ms:.1f}ms"
                )
                _empty = 0
                _max_jitter_ms = 0.0
                _max_qsize = 0

    async def _dispatch_data(self, dispatcher):
        """Consume data frames from dispatcher at data_fps.
        Uses absolute timing from dispatcher.start_time (same as audio/video tracks)."""
        while dispatcher.start_time is None and self.connected:
            await asyncio.sleep(0.005)
        if not self.connected:
            return

        frame_index = 0
        ptime = 1.0 / self.data_fps
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

        dispatcher = GroupDispatcher(
            self.group_queue, self._on_signal,
            audio_samples=self.audio_samples,
            audio_per_group=self.audio_per_group,
            video_per_group=self.video_per_group,
            data_per_group=self.data_per_group,
        )
        out_audio = OutputAudioTrack(dispatcher, self.sample_rate, self.audio_samples, self.client_id)
        out_video = OutputVideoTrack(dispatcher, self.video_fps,
                                     self.video_width, self.video_height, self.client_id)
        self.pc.addTrack(out_audio)
        self.pc.addTrack(out_video)

        # Output: group consumer fills dispatcher buffers at group rate
        self._group_task = asyncio.ensure_future(self._group_consumer(dispatcher))
        self._data_task = asyncio.ensure_future(self._dispatch_data(dispatcher))
        # Input: group assembler pulls from input buffers at group rate
        self._assembler_task = asyncio.ensure_future(self._group_assembler())

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
        # setRemoteDescription, and relay coroutines check self.connected
        self.connected = True
        await self.pc.setRemoteDescription(offer)
        answer = await self.pc.createAnswer()
        await self.pc.setLocalDescription(answer)

        asyncio.ensure_future(self._pipeline_session())

        return self.pc.localDescription.sdp, self.pc.localDescription.type

    async def _forward_dc_message(self, raw_msg):
        """Handle DataChannel message from client.
        Signal messages (recording_start/recording_end) → buffer for next group boundary.
        Data messages → buffer for group inclusion."""
        try:
            msg = json.loads(raw_msg)
        except json.JSONDecodeError:
            return

        if msg.get("signal"):
            self._input_signal_buffer.append(raw_msg)
        else:
            self._input_data_buffer.append(msg)

    async def _relay_audio_input(self, track):
        """Receive audio frames and buffer with PTS for group assembler."""
        logger.info(f"[{self.client_id}] Audio input started")
        try:
            while self.connected:
                av_frame = await track.recv()
                pcm = av_frame.to_ndarray().flatten().astype(np.int16)
                layout = av_frame.layout.name if av_frame.layout else "mono"
                if layout == "stereo":
                    pcm = pcm[::2]
                b64 = base64.b64encode(pcm.tobytes()).decode("ascii")
                self._input_audio_buffer.append((av_frame.pts, b64))
        except Exception as e:
            logger.info(f"[{self.client_id}] Audio input ended: {e}")

    async def _relay_video_input(self, track):
        """Receive video frames and buffer with PTS for group assembler."""
        logger.info(f"[{self.client_id}] Video input started")
        try:
            while self.connected:
                frame = await track.recv()
                img = Image.fromarray(frame.to_ndarray(format="rgb24"))
                buf = io.BytesIO()
                img.save(buf, format="JPEG", quality=85)
                b64_frame = base64.b64encode(buf.getvalue()).decode("ascii")
                self._input_video_buffer.append((frame.pts, b64_frame))
                self._last_video_frame = b64_frame
        except Exception as e:
            logger.info(f"[{self.client_id}] Video input ended: {e}")

    async def _group_assembler(self):
        """Assemble input groups at group_period intervals using PTS.
        Runs independently from receivers. Waits for first audio+video frames
        to align wall clock, then uses PTS ranges to select frames per group.
        Audio gaps detected via PTS and filled with silence."""
        await self.ws_ready.wait()
        logger.info(f"[{self.client_id}] Group assembler started")

        # Pre-compute constants
        group_audio_span = self.audio_per_group * self.audio_samples  # PTS span per group (audio clock)
        video_pts_per_frame = VIDEO_CLOCK_RATE // self.video_fps
        group_video_span = self.video_per_group * video_pts_per_frame  # PTS span per group (video clock)
        silence_b64 = base64.b64encode(
            np.zeros(self.audio_samples, dtype=np.int16).tobytes()
        ).decode("ascii")

        # Wait for both audio and video to have data before starting
        while self.connected:
            if self._input_audio_buffer and self._input_video_buffer:
                break
            await asyncio.sleep(0.005)
        if not self.connected:
            return

        # Align to the LATEST frames in buffer (not oldest) so wall time
        # matches current content, not stale frames from before pipeline was ready
        audio_pts_origin = self._input_audio_buffer[-1][0]
        video_pts_origin = self._input_video_buffer[-1][0]
        wall_origin = time.time()
        # Clear data buffer — discard anything that arrived before alignment
        self._input_data_buffer.clear()
        logger.info(
            f"[{self.client_id}] Group assembler aligned: "
            f"audio_pts0={audio_pts_origin}, video_pts0={video_pts_origin}, "
            f"offset={self.assembler_offset*1000:.0f}ms"
        )

        group_index = 0

        # Stats
        _si = 100  # log every 100 groups (~10s)
        _max_jitter_ms = 0.0
        _audio_match = 0     # exact PTS match
        _audio_silence = 0   # silence fills (gap/empty/future)
        _audio_stale = 0     # stale frames discarded
        _video_fill = 0      # video slots filled with last frame
        _video_real = 0      # real video frames used
        _a_margin_min_ms = float('inf')   # min(latest_audio_pts - audio_pts_end) in ms
        _a_margin_max_ms = float('-inf')
        _v_margin_min_ms = float('inf')
        _v_margin_max_ms = float('-inf')

        while self.connected:
            # Wait for group to complete before processing:
            # (group_index+1)*period = group end time, +offset = extra margin
            target = wall_origin + (group_index + 1) * self.group_period + self.assembler_offset
            wait = target - time.time()
            if wait > 0:
                await asyncio.sleep(wait)

            jitter_ms = (time.time() - target) * 1000
            if abs(jitter_ms) > abs(_max_jitter_ms):
                _max_jitter_ms = jitter_ms

            # PTS ranges for this group
            audio_pts_start = audio_pts_origin + group_index * group_audio_span
            audio_pts_end = audio_pts_start + group_audio_span
            video_pts_start = video_pts_origin + group_index * group_video_span
            video_pts_end = video_pts_start + group_video_span

            # Measure margin before processing (save for post-process correction)
            a_m = None
            v_m = None
            margin_upper_ms = (self.group_period + self.assembler_offset) * 1000  # 150ms
            a_target_margin = int(self.assembler_offset * self.sample_rate)       # 2400 samples
            v_target_margin = int(self.assembler_offset * VIDEO_CLOCK_RATE)       # 4500 ticks
            saved_a_latest = self._input_audio_buffer[-1][0] if self._input_audio_buffer else None
            saved_v_latest = self._input_video_buffer[-1][0] if self._input_video_buffer else None
            if saved_a_latest is not None:
                a_m = (saved_a_latest - audio_pts_end) / self.sample_rate * 1000
                _a_margin_min_ms = min(_a_margin_min_ms, a_m)
                _a_margin_max_ms = max(_a_margin_max_ms, a_m)
            if saved_v_latest is not None:
                v_m = (saved_v_latest - video_pts_end) / VIDEO_CLOCK_RATE * 1000
                _v_margin_min_ms = min(_v_margin_min_ms, v_m)
                _v_margin_max_ms = max(_v_margin_max_ms, v_m)

            # Log abnormal ticks
            if (a_m is not None and a_m <= 0) or (v_m is not None and v_m <= 0) \
                    or not self._input_audio_buffer or not self._input_video_buffer:
                elapsed_tick = time.time() - wall_origin
                stats_logger.info(
                    f"ABNORMAL client={self.client_id} "
                    f"t={elapsed_tick:.2f}s idx={group_index} "
                    f"a_m={f'{a_m:.1f}' if a_m is not None else 'n/a'}ms "
                    f"v_m={f'{v_m:.1f}' if v_m is not None else 'n/a'}ms "
                    f"abuf={len(self._input_audio_buffer)} "
                    f"vbuf={len(self._input_video_buffer)}"
                )

            # === Audio: discard stale, take in range, fill silence ===
            audio_group = []
            while self._input_audio_buffer:
                pts, _ = self._input_audio_buffer[0]
                if pts < audio_pts_start:
                    self._input_audio_buffer.popleft()
                    _audio_stale += 1
                else:
                    break
            while len(audio_group) < self.audio_per_group and self._input_audio_buffer:
                pts, b64 = self._input_audio_buffer[0]
                if pts < audio_pts_end:
                    self._input_audio_buffer.popleft()
                    audio_group.append(b64)
                    _audio_match += 1
                else:
                    break
            while len(audio_group) < self.audio_per_group:
                audio_group.append(silence_b64)
                _audio_silence += 1

            # === Video: take frames in PTS range, fill with last frame ===
            video_group = []
            # Discard stale video frames (before this group)
            while self._input_video_buffer:
                pts, _ = self._input_video_buffer[0]
                if pts < video_pts_start:
                    self._input_video_buffer.popleft()
                else:
                    break

            # Take frames within this group's PTS range
            while len(video_group) < self.video_per_group and self._input_video_buffer:
                pts, b64 = self._input_video_buffer[0]
                if pts < video_pts_end:
                    self._input_video_buffer.popleft()
                    video_group.append(b64)
                    self._last_video_frame = b64
                    _video_real += 1
                else:
                    break

            # Fill missing with last received frame
            while len(video_group) < self.video_per_group and self._last_video_frame:
                video_group.append(self._last_video_frame)
                _video_fill += 1

            # === Data: take data_per_group items (no PTS, arrival order) ===
            data_group = []
            for _ in range(self.data_per_group):
                if self._input_data_buffer:
                    data_group.append(self._input_data_buffer.popleft())
                else:
                    data_group.append(None)

            # === Flush pending signals at group boundary ===
            # Timestamp set at send time, same basis as group timestamp
            while self._input_signal_buffer:
                sig = json.loads(self._input_signal_buffer.popleft())
                sig["timestamp"] = time.time()
                if sig.get("signal") == "cancel":
                    sig["timestamp"] += CANCEL_TIMESTAMP_OFFSET
                    self.cancel_timestamp = sig["timestamp"]
                if self.ws:
                    await self.ws.send(json.dumps(sig))

            # === Send group ===
            if self.ws:
                msg = {
                    "audio": audio_group,
                    "timestamp": time.time(),
                }
                if video_group:
                    msg["video"] = video_group
                msg["data"] = data_group
                await self.ws.send(json.dumps(msg))

            # Post-process correction: adjust origin for next tick
            if saved_a_latest is not None and (a_m <= 0 or a_m >= margin_upper_ms):
                shift = audio_pts_end - saved_a_latest + a_target_margin
                audio_pts_origin -= shift
                logger.info(
                    f"[{self.client_id}] Audio PTS corrected: "
                    f"margin={a_m:.1f}ms, shift={-shift}, "
                    f"abuf_was={len(self._input_audio_buffer) if saved_a_latest else 0}"
                )
            if saved_v_latest is not None and (v_m <= 0 or v_m >= margin_upper_ms):
                shift = video_pts_end - saved_v_latest + v_target_margin
                video_pts_origin -= shift
                logger.info(
                    f"[{self.client_id}] Video PTS corrected: "
                    f"margin={v_m:.1f}ms, shift={-shift}, "
                    f"vbuf_was={len(self._input_video_buffer) if saved_v_latest else 0}"
                )

            group_index += 1

            # Periodic stats
            if group_index % _si == 0:
                elapsed = time.time() - wall_origin
                # A/V sync: latest audio vs latest video in ms from their origins
                av_gap_str = "n/a"
                if self._input_audio_buffer and self._input_video_buffer:
                    a_latest_ms = (self._input_audio_buffer[-1][0] - audio_pts_origin) / self.sample_rate * 1000
                    v_latest_ms = (self._input_video_buffer[-1][0] - video_pts_origin) / VIDEO_CLOCK_RATE * 1000
                    av_gap_str = f"{a_latest_ms - v_latest_ms:.1f}"
                stats_logger.info(
                    f"assembler client={self.client_id} "
                    f"t={elapsed:.1f}s idx={group_index} "
                    f"in_abuf={len(self._input_audio_buffer)} "
                    f"in_vbuf={len(self._input_video_buffer)} "
                    f"audio_match={_audio_match} silence={_audio_silence} stale={_audio_stale} "
                    f"video_real={_video_real} video_fill={_video_fill} "
                    f"a_margin=[{_a_margin_min_ms:.1f},{_a_margin_max_ms:.1f}]ms "
                    f"v_margin=[{_v_margin_min_ms:.1f},{_v_margin_max_ms:.1f}]ms "
                    f"av_gap={av_gap_str}ms "
                    f"jitter_max={_max_jitter_ms:.1f}ms"
                )
                _max_jitter_ms = 0.0
                _audio_match = 0
                _audio_silence = 0
                _audio_stale = 0
                _video_real = 0
                _video_fill = 0
                _a_margin_min_ms = float('inf')
                _a_margin_max_ms = float('-inf')
                _v_margin_min_ms = float('inf')
                _v_margin_max_ms = float('-inf')

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

                # Notify pipeline that WebRTC connection is ready
                await ws.send(json.dumps({
                    "signal": "connection_start",
                    "timestamp": time.time(),
                }))

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
            if self.connected:
                logger.info(f"[{self.client_id}] Pipeline relay ended: {e}")
                self._notify_client("error", f"Pipeline disconnected: {e}")

    async def cleanup(self):
        if self._closed.is_set():
            return
        self._closed.set()
        self.connected = False
        logger.info(f"[{self.client_id}] Cleaning up session")

        # Cancel async tasks
        for task in (getattr(self, '_group_task', None),
                     getattr(self, '_data_task', None),
                     getattr(self, '_assembler_task', None)):
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

        # Optional session params from request body
        session_kwargs = {}
        for key in ("sample_rate", "audio_ptime", "video_fps",
                     "video_width", "video_height", "data_fps", "startup_buffer"):
            if key in body:
                session_kwargs[key] = body[key]

        session = WebRTCSession(
            client_id, self.main_server_url,
            on_session_end=lambda cid: self.sessions.pop(cid, None),
            **session_kwargs,
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
    # Stats logger: writes to file for long-session analysis
    stats_logger.setLevel(logging.INFO)
    stats_logger.propagate = False
    os.makedirs("logs", exist_ok=True)
    fh = logging.FileHandler("logs/webrtc_stats.log", mode="w")
    fh.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    stats_logger.addHandler(fh)


def main():
    parser = argparse.ArgumentParser(description="Generic WebRTC Server")
    parser.add_argument("--port", type=int, default=15168)
    parser.add_argument("--main-server", default="http://localhost:8910")
    args = parser.parse_args()

    setup_logger()

    server = WebRTCServer(args.main_server)

    import aiohttp_cors

    app = web.Application()
    cors = aiohttp_cors.setup(app, defaults={
        "*": aiohttp_cors.ResourceOptions(
            allow_headers="*", allow_methods="*",
        )
    })
    cors.add(app.router.add_post("/offer/{client_id}", server.handle_offer))
    cors.add(app.router.add_get("/status", server.handle_status))

    # Serve webrtc_client/ at root
    client_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "webrtc_client")
    app.router.add_static("/static", client_dir)
    async def index(request):
        return web.FileResponse(
            os.path.join(client_dir, "index.html")
        )
    app.router.add_get("/", index)

    async def on_shutdown(app):
        await server.cleanup()

    app.on_shutdown.append(on_shutdown)

    logger.info(f"WebRTC server starting on port {args.port}")
    logger.info(f"  Main server: {args.main_server}")
    web.run_app(app, host="0.0.0.0", port=args.port)


if __name__ == "__main__":
    main()
