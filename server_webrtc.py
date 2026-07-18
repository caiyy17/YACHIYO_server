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
    - Dequeue one pipeline group when available; on underrun the send
      points fall back themselves (audio silence / idle frame / {})
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
  Signals are standalone WebSocket messages (not grouped): held
  assembler_offset + dc_offset after arrival, due ones flush at the next
  group boundary (FIFO).
  Data items are stamped with arrival time and window-matched into the
  group carrying media of the same client-side moment (window lags the
  tick grid by dc_offset); overflow/late items are dropped, gaps repeat
  the last item — same semantics as the video lane.
  Output (pipeline → WebRTC):
    {"audio": ["<pcm1>", ...], "video": ["<jpeg1>", ...],
     "data": [{...}, null]}
    {"signal": "SoS/EoS", "timestamp": ...}

Client is responsible for register/init_pipeline/unregister via server_fastapi's REST API.
server_webrtc only handles WebRTC ↔ pipeline WebSocket bridging.

Session params:
  Lane rates (audio_fps / video_fps / data_fps) come from the top-level
  "webrtc" section of the client's pipeline config, fetched from the main
  server at offer time — they must agree with the FrameSplitter's group
  packing, so the config file is the single source. Gateway-only tunables
  (startup_buffer / assembler_offset_ms and the trims audio_offset_ms /
  video_offset_ms / dc_offset_ms / cancel_offset_ms, all defaulting to 0)
  live in the same section but have no splitter counterpart. The audio
  sample rate is not configurable: WebRTC/Opus fixes it at 48kHz.
  Resolution (video_width / video_height) is the client's own choice,
  passed in the offer body; the video track rescales every outgoing frame to
  it regardless of what the pipeline renders.

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
from math import ceil, gcd, isfinite
from queue import Queue, Empty
from collections import deque

import aiohttp
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
SAMPLE_RATE = 48000       # fixed by WebRTC: Opus always runs at a 48kHz clock
VIDEO_CLOCK_RATE = 90000  # RTP clock rate for video (fixed by RTP spec)
# Supported video/data lane rates: the divisors of the 90kHz RTP clock in
# 10..60 (exact integer video PTS steps; below 10 needs a larger assembler
# offset than the default, above 60 exceeds real webrtc streams; the data
# lane shares the list to keep the group GCD in the same family)
SUPPORTED_LANE_FPS = (10, 12, 15, 16, 18, 20, 24, 25, 30, 36, 40, 45, 48,
                      50, 60)
DEFAULT_AUDIO_FPS = 50    # 20ms audio frames
DEFAULT_VIDEO_FPS = 30
DEFAULT_VIDEO_WIDTH = 320
DEFAULT_VIDEO_HEIGHT = 240
DEFAULT_DATA_FPS = 20
DEFAULT_STARTUP_BUFFER = 2
DEFAULT_CONSUMER_OFFSET = 0.005   # consumer fills buffers 5ms before tracks consume
DEFAULT_ASSEMBLER_OFFSET = 0.1    # assembler waits this long past each group end
DEFAULT_AUDIO_OFFSET = 0.0        # per-lane window trim beyond the tick grid
DEFAULT_VIDEO_OFFSET = 0.0
DEFAULT_DC_OFFSET = 0.0           # data window lag; signal hold adds assembler_offset
DEFAULT_CANCEL_OFFSET = 0         # added to cancel's stamp; 0: FIFO flush order protects the paired start
MAX_LANE_BUFFER = 1000            # frames; a lane past this means the assembler stalled (e.g. another lane never opened) -> abort

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

    def fill_next_group(self):
        """Dequeue one media group into buffers when available.
        Signal-only messages fire callback and are skipped. No cancel
        gating here: the pipeline exit is a transport hop, and stale
        filtering belongs to the consuming edges (pipeline nodes on the
        way in, the client's own event handling on the way out)."""
        while True:
            try:
                msg = self._group_queue.get_nowait()
            except Empty:
                # No group available — leave the buffers as they are; the
                # send points fall back on their own (audio: silence,
                # video: idle frame, data: {})
                return False

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
                 sample_rate=SAMPLE_RATE,
                 audio_ptime=1 / DEFAULT_AUDIO_FPS,
                 video_fps=DEFAULT_VIDEO_FPS,
                 video_width=DEFAULT_VIDEO_WIDTH,
                 video_height=DEFAULT_VIDEO_HEIGHT,
                 data_fps=DEFAULT_DATA_FPS,
                 startup_buffer=DEFAULT_STARTUP_BUFFER,
                 consumer_offset=DEFAULT_CONSUMER_OFFSET,
                 assembler_offset=DEFAULT_ASSEMBLER_OFFSET,
                 audio_offset=DEFAULT_AUDIO_OFFSET,
                 video_offset=DEFAULT_VIDEO_OFFSET,
                 dc_offset=DEFAULT_DC_OFFSET,
                 cancel_offset=DEFAULT_CANCEL_OFFSET,
                 expect_data=False,
                 on_session_end=None):
        self.client_id = client_id
        self.main_server_url = main_server_url
        self.main_ws_url = main_server_url.replace("http", "ws", 1) + "/ws"

        # Audio params
        self.sample_rate = sample_rate
        self.audio_samples = int(round(sample_rate * audio_ptime))

        # Video params
        self.video_fps = video_fps
        self.video_width = video_width
        self.video_height = video_height

        # Data params
        self.data_fps = data_fps
        # whether the pipeline consumes the data lane (collector data lane
        # wired): gates the startup wait on data accumulation
        self.expect_data = expect_data

        # Group structure (GCD-based)
        audio_fps = sample_rate // self.audio_samples
        group_fps = gcd(gcd(audio_fps, video_fps), data_fps)
        self.audio_per_group = audio_fps // group_fps
        self.video_per_group = video_fps // group_fps
        self.data_per_group = data_fps // group_fps
        self.group_period = 1.0 / group_fps

        # Startup buffer and timing offsets. Per-lane offsets are trims
        # beyond the tick grid: audio/video shift their PTS windows, dc
        # shifts the data window and adds to the signal hold (total hold =
        # assembler_offset + dc_offset). The signal hold must exceed the
        # media path's total lag (arrival lead ~80ms + assembler_offset +
        # one group period) for e.g. recording_end to land after the tail.
        self.startup_buffer = startup_buffer
        self.consumer_offset = consumer_offset
        self.assembler_offset = assembler_offset
        self.audio_offset = audio_offset
        self.video_offset = video_offset
        self.dc_offset = dc_offset
        self.cancel_offset = cancel_offset

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
        self._input_data_buffer = deque()    # (arrival_ts, data dict) from DataChannel
        self._last_data_item = None          # Last received data item (for gap fill)
        # Client signals waiting for next group boundary: (due_time, raw_msg),
        # due dc_offset after arrival.
        self._input_signal_buffer = deque()
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
        Startup: waits until group_queue has enough groups (jitter buffer).
        consumer_offset: fills slightly before track consumes to avoid race."""
        # The clock origin is established by the first track recv() — which
        # aiortc only calls once the connection is up. Setting it here (the
        # task starts at offer time, before the handshake completes) would
        # start the grid early and force the tracks into a catch-up burst.
        while dispatcher.start_time is None and self.connected:
            await asyncio.sleep(0.005)
        if not self.connected:
            return

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
                    _empty += 1
                    group_index += 1
                    continue

            got_media = dispatcher.fill_next_group()
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
        Signals are held assembler_offset + dc_offset and flush at group
        boundaries (FIFO). Data items are stamped with arrival time and
        window-matched into the group carrying media of the same
        client-side moment."""
        try:
            msg = json.loads(raw_msg)
        except json.JSONDecodeError:
            return

        if msg.get("signal"):
            self._input_signal_buffer.append(
                (time.time() + self.assembler_offset + self.dc_offset,
                 raw_msg))
        else:
            self._input_data_buffer.append((time.time(), msg))
        await self._abort_if_lane_overflow()

    async def _abort_if_lane_overflow(self):
        """A lane buffer this deep means the group assembler is not draining
        it — typically one lane never opened, stalling startup while the
        others pile up. Abort the session instead of growing unbounded."""
        counts = {"audio": len(self._input_audio_buffer),
                  "video": len(self._input_video_buffer),
                  "data": len(self._input_data_buffer),
                  "signal": len(self._input_signal_buffer)}
        over = [k for k, v in counts.items() if v > MAX_LANE_BUFFER]
        if not over:
            return False
        logger.error(
            f"[{self.client_id}] lane buffer overflow ({','.join(over)} > "
            f"{MAX_LANE_BUFFER}); counts={counts}; assembler stalled "
            f"(a lane likely never opened) - aborting session")
        self._notify_client("error", f"lane overflow ({','.join(over)})")
        await self.cleanup()
        return True

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
                if await self._abort_if_lane_overflow():
                    return
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
                if await self._abort_if_lane_overflow():
                    return
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
        # Healthy margin band is (0, 2x offset). Correction targets carry
        # the same half-frame phase (and per-lane trim) the alignment
        # origin establishes, so a re-anchor lands frames at window
        # centers too, not on boundaries.
        margin_upper_ms = 2 * self.assembler_offset * 1000
        a_target_margin = (int((self.assembler_offset + self.audio_offset)
                               * self.sample_rate)
                           - self.audio_samples // 2)
        v_target_margin = (int((self.assembler_offset + self.video_offset)
                               * VIDEO_CLOCK_RATE)
                           - video_pts_per_frame // 2)

        # Wait until every lane holds one full group PLUS its own lane
        # offset worth of content: a lane's group-0 window is shifted back
        # by its trim (media) / dc_offset (data), so that extra depth is
        # exactly what makes window 0 fully covered. The data lane only
        # counts when the pipeline consumes it (expect_data).
        audio_need = self.audio_per_group + max(0, ceil(
            self.audio_offset * self.sample_rate / self.audio_samples))
        video_need = self.video_per_group + max(0, ceil(
            self.video_offset * self.video_fps))
        data_need = (self.data_per_group + max(0, ceil(
            self.dc_offset * self.data_fps))) if self.expect_data else 0
        _wait_start = time.time()
        _next_warn = 5.0
        while self.connected:
            if (len(self._input_audio_buffer) >= audio_need
                    and len(self._input_video_buffer) >= video_need
                    and len(self._input_data_buffer) >= data_need):
                break
            waited = time.time() - _wait_start
            if waited >= _next_warn:
                logger.warning(
                    f"[{self.client_id}] still waiting for startup "
                    f"accumulation ({waited:.0f}s): audio "
                    f"{len(self._input_audio_buffer)}/{audio_need}, video "
                    f"{len(self._input_video_buffer)}/{video_need}, data "
                    f"{len(self._input_data_buffer)}/{data_need}"
                )
                _next_warn += 5.0
            await asyncio.sleep(0.005)
        if not self.connected:
            return

        # Align at the moment every lane reaches its startup depth: each
        # origin is placed so group 0 is exactly the (trim-shifted) group
        # already buffered — the first tick then waits only
        # assembler_offset. Boundaries are pulled back a further half frame
        # so nominal frame instants sit at window centers: sender-side ms
        # quantization of pts (e.g. video 2970/3060 around the 3000 grid)
        # then never flips a frame across a window boundary. Per-lane
        # offsets shift each window by the lane's trim.
        audio_pts_origin = (self._input_audio_buffer[-1][0]
                            + self.audio_samples // 2 - group_audio_span
                            - int(self.audio_offset * self.sample_rate))
        video_pts_origin = (self._input_video_buffer[-1][0]
                            + video_pts_per_frame // 2 - group_video_span
                            - int(self.video_offset * VIDEO_CLOCK_RATE))
        wall_origin = time.time()
        # The data buffer is NOT cleared: the data windows lag the grid by
        # dc_offset (data arrives that much earlier than the media of the
        # same client moment), so group 0's data window is covered by items
        # that arrived BEFORE alignment; anything older dies in the per-tick
        # stale check.
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
        _data_real = 0       # data items window-matched into slots
        _data_fill = 0       # data slots filled with last item (or None)
        _data_stale = 0      # data items dropped (window passed / overflow)
        _a_margin_min_ms = float('inf')   # min(latest_audio_pts - audio_pts_end) in ms
        _a_margin_max_ms = float('-inf')
        _v_margin_min_ms = float('inf')
        _v_margin_max_ms = float('-inf')

        while self.connected:
            # Group k's frames are due at k*period (group 0 was pre-buffered
            # at alignment); +offset = arrival margin
            target = wall_origin + group_index * self.group_period + self.assembler_offset
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

            # === Data: window-matched by arrival time (video-like: stale
            # dropped, overflow dropped, gaps filled with the last item).
            # The window lags the tick grid by dc_offset, so a data item
            # rides the group carrying media captured at the same
            # client-side moment and stays level with same-moment signals
            # (held assembler_offset + dc_offset). ===
            data_win_end = wall_origin + group_index * self.group_period \
                - self.dc_offset
            data_win_start = data_win_end - self.group_period
            while self._input_data_buffer:
                ts, _ = self._input_data_buffer[0]
                if ts < data_win_start:
                    self._input_data_buffer.popleft()
                    _data_stale += 1
                else:
                    break
            data_group = []
            while len(data_group) < self.data_per_group \
                    and self._input_data_buffer:
                ts, item = self._input_data_buffer[0]
                if ts < data_win_end:
                    self._input_data_buffer.popleft()
                    data_group.append(item)
                    self._last_data_item = item
                    _data_real += 1
                else:
                    break
            while len(data_group) < self.data_per_group:
                data_group.append(self._last_data_item)
                _data_fill += 1

            # === Flush pending signals at group boundary ===
            # Timestamp set at send time, same basis as group timestamp.
            # Head not yet due (dc_offset hold) blocks the queue: FIFO order.
            while self._input_signal_buffer:
                if self._input_signal_buffer[0][0] > time.time():
                    break
                sig = json.loads(self._input_signal_buffer.popleft()[1])
                sig["timestamp"] = time.time()
                if sig.get("signal") == "cancel":
                    sig["timestamp"] += self.cancel_offset
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
                    f"data_real={_data_real} data_fill={_data_fill} "
                    f"data_stale={_data_stale} "
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
                _data_real = 0
                _data_fill = 0
                _data_stale = 0
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
# Offer <-> pipeline-config compatibility
# ============================================================
def _parse_sdp_media(sdp):
    """m-line kind -> the client's capabilities for that kind, unioned
    across m-lines: "send"/"recv" from the direction attribute (sendrecv
    is the RFC default when none is present)."""
    caps = {}
    kind = None
    direction = None

    def commit():
        if kind is None:
            return
        d = direction or "sendrecv"
        s = caps.setdefault(kind, set())
        if d in ("sendrecv", "sendonly"):
            s.add("send")
        if d in ("sendrecv", "recvonly"):
            s.add("recv")

    for line in sdp.splitlines():
        line = line.strip()
        if line.startswith("m="):
            commit()
            kind = line[2:].split(" ", 1)[0]
            direction = None
        elif line in ("a=sendrecv", "a=sendonly", "a=recvonly",
                      "a=inactive"):
            direction = line[2:]
    commit()
    return caps


def check_offer_compatibility(sdp, pipeline_conf):
    """Compare a client's SDP offer against what its pipeline config needs.
    Returns a list of human-readable mismatch strings (empty = compatible).

    Input side: the group assembler aligns on BOTH media lanes, so a
    pipeline that consumes webrtc input (has a frame_collector) requires
    the client to SEND audio and video. The DataChannel carries all
    signals (recording_start/end, cancel) and the data lane, so an
    application m-line is always required.
    Output side: each splitter media output that is wired (non-null
    target) needs a client m-line willing to RECEIVE that kind.
    Extra client tracks the pipeline does not consume are harmless and
    not reported."""
    nodes = pipeline_conf.get("pipeline") or []

    def find(fn):
        for n in nodes:
            if n.get("function") == fn:
                return n.get("config", {})
        return None

    collector = find("frame_collector")
    splitter = find("frame_splitter")
    if collector is None and splitter is None:
        return ["pipeline has no frame_collector or frame_splitter — "
                "not a webrtc pipeline (init a webrtc config first)"]
    if not pipeline_conf.get("webrtc"):
        return ['pipeline config has no top-level "webrtc" section — '
                "every webrtc-facing config must declare its lane rates "
                "explicitly"]

    media = _parse_sdp_media(sdp)
    errors = []
    if "application" not in media:
        errors.append(
            "offer has no application m-line (DataChannel) — signals "
            "(recording_start/end, cancel) and the data lane require it")
    if collector is not None:
        for kind in ("audio", "video"):
            if "send" not in media.get(kind, set()):
                errors.append(
                    f"pipeline consumes webrtc input but the offer has no "
                    f"sendable {kind} m-line (the group assembler aligns "
                    f"on both media lanes)")
    if splitter is not None:
        wired_out = {v.get("source")
                     for v in splitter.get("output_vars", [])
                     if v.get("target") is not None}
        for kind in ("audio", "video"):
            if kind in wired_out and "recv" not in media.get(kind, set()):
                errors.append(
                    f"pipeline sends {kind} output but the offer has no "
                    f"{kind} m-line willing to receive")
    return errors


# ============================================================
# WebRTC Server
# ============================================================
class WebRTCServer:
    def __init__(self, main_server_url):
        self.main_server_url = main_server_url
        self.sessions = {}

    async def _fetch_pipeline_config(self, client_id):
        """The client's full pipeline config from the main server. Empty
        dict if the client or its pipeline doesn't exist; None when the
        fetch itself failed (main server unreachable)."""
        url = f"{self.main_server_url}/clients/{client_id}"
        try:
            async with aiohttp.ClientSession() as http:
                async with http.get(
                    url, timeout=aiohttp.ClientTimeout(total=5)
                ) as r:
                    info = await r.json()
            return info.get("pipeline_config") or {}
        except Exception as e:
            logger.warning(
                f"[{client_id}] failed to fetch pipeline config: {e}"
            )
            return None

    async def handle_offer(self, request):
        """POST /offer/{client_id} - WebRTC signaling endpoint.

        Lane rates come from the config's "webrtc" section (single source
        with the splitter's packing); gateway offsets are optional keys in
        the same section; resolution comes from the offer body (the client's
        own choice, rescaled by the video track)."""
        client_id = request.match_info["client_id"]
        body = await request.json()

        if client_id in self.sessions:
            await self.sessions[client_id].cleanup()

        conf = await self._fetch_pipeline_config(client_id)
        if conf is None:
            # main server unreachable: proceed without the compatibility
            # check (the session cannot work anyway if it stays down)
            logger.warning(f"[{client_id}] pipeline config unavailable; "
                           f"skipping offer compatibility check")
            sec = {}
        else:
            sec = conf.get("webrtc") or {}
            # the offered connection must satisfy what the pipeline needs —
            # reject with the concrete gaps instead of hanging silently
            mismatches = check_offer_compatibility(body.get("sdp", ""), conf)
            if mismatches:
                logger.warning(
                    f"[{client_id}] offer rejected: {mismatches}")
                return web.json_response(
                    {"error": "offer does not match the pipeline config",
                     "mismatches": mismatches}, status=400)
        audio_fps = sec.get("audio_fps", DEFAULT_AUDIO_FPS)
        rates = {"audio_fps": audio_fps,
                 "video_fps": sec.get("video_fps", DEFAULT_VIDEO_FPS),
                 "data_fps": sec.get("data_fps", DEFAULT_DATA_FPS)}
        for name, v in rates.items():
            if not isinstance(v, int) or v <= 0:
                return web.json_response(
                    {"error": f"webrtc.{name} must be a positive integer, "
                              f"got {v!r}"}, status=400)
        # WebRTC audio is 20ms/50fps on the wire: aiortc's Opus encoder
        # hardcodes 960-sample frames and browsers default to 20ms, while
        # the SDP carries no frame-duration field to negotiate — any other
        # audio_fps can never be satisfied by a real connection.
        if audio_fps != 50:
            return web.json_response(
                {"error": f"webrtc.audio_fps must be 50 (WebRTC audio is "
                          f"20ms/50fps on the wire; the SDP cannot "
                          f"negotiate any other framing), got {audio_fps}"},
                status=400)
        for lane in ("video_fps", "data_fps"):
            if rates[lane] not in SUPPORTED_LANE_FPS:
                return web.json_response(
                    {"error": f"webrtc.{lane} must be one of "
                              f"{list(SUPPORTED_LANE_FPS)} (divisors of "
                              f"the 90kHz RTP clock in 10..60), got "
                              f"{rates[lane]}"}, status=400)

        session_kwargs = {
            "audio_ptime": 1 / audio_fps,
            "video_fps": rates["video_fps"],
            "data_fps": rates["data_fps"],
        }
        if "startup_buffer" in sec:
            session_kwargs["startup_buffer"] = sec["startup_buffer"]
        # Floor: the margin band (0, 2x offset) must tolerate one frame
        # interval of arrival quantization per lane, else the assembler
        # corrects (and shifts the PTS origin) on nearly every tick. The
        # default is checked too: very low lane fps needs an explicit key.
        assembler_ms = sec.get("assembler_offset_ms",
                               DEFAULT_ASSEMBLER_OFFSET * 1000)
        min_ms = max(1000 / audio_fps, 1000 / rates["video_fps"])
        if isinstance(assembler_ms, bool) or \
                not isinstance(assembler_ms, (int, float)) or \
                not isfinite(assembler_ms) or assembler_ms < min_ms:
            return web.json_response(
                {"error": f"webrtc.assembler_offset_ms must be a finite "
                          f"number >= one frame interval ({min_ms:.1f}ms),"
                          f" got {assembler_ms!r}"}, status=400)
        session_kwargs["assembler_offset"] = assembler_ms / 1000
        # Per-lane trims beyond the tick grid (default 0): audio/video
        # shift their PTS windows, dc shifts the data window and adds to
        # the signal hold (total hold = assembler_offset + dc_offset).
        for cfg_key, kwarg, frame_ms in (
                ("audio_offset_ms", "audio_offset", 1000 / audio_fps),
                ("video_offset_ms", "video_offset",
                 1000 / rates["video_fps"]),
                ("dc_offset_ms", "dc_offset", None)):
            lane_ms = sec.get(cfg_key, 0)
            if isinstance(lane_ms, bool) or \
                    not isinstance(lane_ms, (int, float)) or \
                    not isfinite(lane_ms):
                return web.json_response(
                    {"error": f"webrtc.{cfg_key} must be a finite number, "
                              f"got {lane_ms!r}"}, status=400)
            if frame_ms is None:
                # dc: signal hold (assembler + dc) must stay non-negative
                if assembler_ms + lane_ms < 0:
                    return web.json_response(
                        {"error": f"webrtc.{cfg_key} {lane_ms!r} makes the "
                                  f"signal hold negative (assembler_offset "
                                  f"{assembler_ms}ms + dc)"}, status=400)
            else:
                # audio/video: margin target (assembler + trim − half a
                # frame) must stay inside the correction band
                target_ms = assembler_ms + lane_ms - frame_ms / 2
                if not (0 < target_ms < 2 * assembler_ms):
                    return web.json_response(
                        {"error": f"webrtc.{cfg_key} {lane_ms!r} puts the "
                                  f"margin target ({target_ms:.1f}ms) outside "
                                  f"(0, {2 * assembler_ms:.0f}ms)"},
                        status=400)
            session_kwargs[kwarg] = lane_ms / 1000
        # Signed trim added to cancel's flush stamp (no semantic guard:
        # backdating widens the kill range, 0 relies on FIFO order)
        cancel_ms = sec.get("cancel_offset_ms", DEFAULT_CANCEL_OFFSET * 1000)
        if isinstance(cancel_ms, bool) or \
                not isinstance(cancel_ms, (int, float)) or \
                not isfinite(cancel_ms):
            return web.json_response(
                {"error": f"webrtc.cancel_offset_ms must be a finite number,"
                          f" got {cancel_ms!r}"}, status=400)
        session_kwargs["cancel_offset"] = cancel_ms / 1000
        # the startup wait covers the data lane only when the pipeline
        # actually consumes it (collector data lane wired)
        expect_data = False
        for n in ((conf or {}).get("pipeline") or []):
            if n.get("function") == "frame_collector":
                expect_data = any(
                    v.get("target") == "data" and v.get("source") is not None
                    for v in (n.get("config") or {}).get("input_vars", []))
                break
        session_kwargs["expect_data"] = expect_data
        # resolution is the client's own choice
        for key in ("video_width", "video_height"):
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
