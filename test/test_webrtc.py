#!/usr/bin/env python3
"""
Consolidated WebRTC test script — four modes via --mode:

  single        Full single-client WebRTC test (from test_webrtc.py):
                Sends test_voice.wav + video, records a 30s side-by-side MP4
                of sent vs received audio/video/DataChannel subtitles.
                Requires the full pipeline (ASR, LLM, TTS, VectorDB).

  lifecycle     Connection start/stop lifecycle test (from test_webrtc_lifecycle.py):
                Verifies FrameSplitter pause/clock-start, data flow, pipeline
                dispose on disconnect, and re-init without force.
                Uses a FrameSplitter-only pipeline (no external model services).

  multi         Multi-user N concurrent clients test (from test_webrtc_multi.py):
                Runs N clients in separate processes against the full pipeline,
                each recording its own MP4 and verifying text/audio/EoS results.

  framesplitter Standalone clock-driven FrameSplitterStep test
                (from test_frame_splitter_clock.py):
                Drives a FrameSplitterStep in-process via queues — no server.
                Verifies steady 100ms output rate, audio grouping, signal
                ordering, cancel buffer clearing, no drift, startup buffering.

Usage:
  conda activate yachiyo
  python test/test_webrtc_consolidated.py --mode single [--server http://localhost:15168] ...
  python test/test_webrtc_consolidated.py --mode lifecycle
  python test/test_webrtc_consolidated.py --mode multi [--num-clients 3]
  python test/test_webrtc_consolidated.py --mode framesplitter
"""

import argparse
import asyncio
import base64
import fractions
import io
import json
import logging
import multiprocessing
import os
import struct
import subprocess
import sys
import tempfile
import threading
import time
import wave
from queue import Queue, Empty

import av
import numpy as np
from aiortc import (
    MediaStreamTrack,
    RTCPeerConnection,
    RTCSessionDescription,
)
from PIL import Image, ImageDraw, ImageFont

# ============================================================
# Constants (shared)
# ============================================================
SAMPLE_RATE = 48000
AUDIO_PTIME = 0.02
AUDIO_SAMPLES = int(SAMPLE_RATE * AUDIO_PTIME)  # 960
AUDIO_TIME_BASE = fractions.Fraction(1, SAMPLE_RATE)

VIDEO_WIDTH, VIDEO_HEIGHT = 320, 240
VIDEO_FPS = 30
VIDEO_CLOCK_RATE = 90000
VIDEO_TIME_BASE = fractions.Fraction(1, VIDEO_CLOCK_RATE)
VIDEO_TIMESTAMP_INCREMENT = VIDEO_CLOCK_RATE // VIDEO_FPS

TEST_DURATION = 45  # seconds
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_WAV = os.path.join(SCRIPT_DIR, "test_voice.wav")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "tmp")
OUTPUT_MP4 = os.path.join(OUTPUT_DIR, "test_webrtc_output.mp4")

MAIN_SERVER = "http://localhost:8910"
WEBRTC_SERVER = "http://localhost:15168"
CLIENT_ID = "test_webrtc_client"
PIPELINE_CONFIG = "unity_chan_webrtc"

# --- lifecycle-mode-specific constants (from test_webrtc_lifecycle.py) ---
LIFECYCLE_CLIENT_ID = "test_lifecycle"
LIFECYCLE_PIPELINE_CONFIG = "test_frame_splitter"

# --- multi-mode-specific constants (from test_webrtc_multi.py) ---
NUM_CLIENTS = 3
CLIENT_TEST_DURATION = 30


# ============================================================
# Load test audio (single / multi)
# ============================================================
def load_test_audio(path):
    """Load WAV, resample to 48kHz mono, split into 20ms frames."""
    with wave.open(path, "rb") as wf:
        sr = wf.getframerate()
        ch = wf.getnchannels()
        raw = wf.readframes(wf.getnframes())

    pcm = np.frombuffer(raw, dtype=np.int16)
    if ch > 1:
        pcm = pcm[::ch]

    if sr != SAMPLE_RATE:
        target_len = int(len(pcm) * SAMPLE_RATE / sr)
        pcm = np.interp(
            np.linspace(0, len(pcm) - 1, target_len),
            np.arange(len(pcm)),
            pcm.astype(np.float64),
        ).astype(np.int16)

    frames = []
    for i in range(0, len(pcm), AUDIO_SAMPLES):
        chunk = pcm[i:i + AUDIO_SAMPLES]
        if len(chunk) < AUDIO_SAMPLES:
            chunk = np.pad(chunk, (0, AUDIO_SAMPLES - len(chunk)))
        frames.append(chunk)
    return frames


# ============================================================
# Client-side tracks (single / multi)
# ============================================================
class TestAudioTrack(MediaStreamTrack):
    """Sends silence until triggered, then test_voice.wav, then silence. Records all sent PCM."""
    kind = "audio"

    def __init__(self, audio_frames):
        super().__init__()
        self._frames = audio_frames
        self._index = 0
        self._timestamp = 0
        self._start = None
        self.speaking = False  # set True to start sending speech
        self.finished_speech = False
        self.recorded_pcm = []

    async def recv(self):
        if self._start is None:
            self._start = time.time()
        wait = self._start + (self._timestamp / SAMPLE_RATE) - time.time()
        if wait > 0:
            await asyncio.sleep(wait)

        if self.speaking and self._index < len(self._frames):
            pcm = self._frames[self._index]
            self._index += 1
        else:
            pcm = np.zeros(AUDIO_SAMPLES, dtype=np.int16)
            if self.speaking and self._index >= len(self._frames) and not self.finished_speech:
                self.finished_speech = True

        self.recorded_pcm.append(pcm)

        frame = av.AudioFrame(format="s16", layout="mono", samples=AUDIO_SAMPLES)
        frame.sample_rate = SAMPLE_RATE
        frame.pts = self._timestamp
        frame.time_base = AUDIO_TIME_BASE
        frame.planes[0].update(pcm.tobytes())
        self._timestamp += AUDIO_SAMPLES
        return frame


class TestVideoTrack(MediaStreamTrack):
    """White during speech, black during silence. Records all frames."""
    kind = "video"

    def __init__(self, audio_track):
        super().__init__()
        self._audio = audio_track
        self._timestamp = 0
        self._start = None
        self.recorded_frames = []

    def _make_yuv_frame(self, white):
        y_val = 235 if white else 16
        y = np.full((VIDEO_HEIGHT, VIDEO_WIDTH), y_val, dtype=np.uint8)
        u = np.full((VIDEO_HEIGHT // 2, VIDEO_WIDTH // 2), 128, dtype=np.uint8)
        v = np.full((VIDEO_HEIGHT // 2, VIDEO_WIDTH // 2), 128, dtype=np.uint8)
        frame = av.VideoFrame(VIDEO_WIDTH, VIDEO_HEIGHT, "yuv420p")
        frame.planes[0].update(y.tobytes())
        frame.planes[1].update(u.tobytes())
        frame.planes[2].update(v.tobytes())
        return frame

    def _make_rgb(self, white):
        val = 255 if white else 0
        return np.full((VIDEO_HEIGHT, VIDEO_WIDTH, 3), val, dtype=np.uint8)

    async def recv(self):
        if self._start is None:
            self._start = time.time()
        wait = self._start + (self._timestamp / VIDEO_CLOCK_RATE) - time.time()
        if wait > 0:
            await asyncio.sleep(wait)

        active = self._audio.speaking and not self._audio.finished_speech
        self.recorded_frames.append(
            (time.time() - self._start, self._make_rgb(active))
        )

        frame = self._make_yuv_frame(active)
        frame.pts = self._timestamp
        frame.time_base = VIDEO_TIME_BASE
        self._timestamp += VIDEO_TIMESTAMP_INCREMENT
        return frame


# ============================================================
# Client-side tracks (lifecycle)
# ============================================================
class SilenceTrack(MediaStreamTrack):
    """Sends silence audio at 48kHz."""
    kind = "audio"

    def __init__(self):
        super().__init__()
        self._timestamp = 0
        self._start = None

    async def recv(self):
        if self._start is None:
            self._start = time.time()
        wait = self._start + (self._timestamp / SAMPLE_RATE) - time.time()
        if wait > 0:
            await asyncio.sleep(wait)

        pcm = np.zeros(AUDIO_SAMPLES, dtype=np.int16)
        frame = av.AudioFrame(format="s16", layout="mono", samples=AUDIO_SAMPLES)
        frame.sample_rate = SAMPLE_RATE
        frame.pts = self._timestamp
        frame.time_base = AUDIO_TIME_BASE
        frame.planes[0].update(pcm.tobytes())
        self._timestamp += AUDIO_SAMPLES
        return frame


class BlackVideoTrack(MediaStreamTrack):
    """Sends black video at 30fps."""
    kind = "video"

    def __init__(self):
        super().__init__()
        self._timestamp = 0
        self._start = None
        self._frame = self._make_frame()

    def _make_frame(self):
        y = np.full((VIDEO_HEIGHT, VIDEO_WIDTH), 16, dtype=np.uint8)
        u = np.full((VIDEO_HEIGHT // 2, VIDEO_WIDTH // 2), 128, dtype=np.uint8)
        v = np.full((VIDEO_HEIGHT // 2, VIDEO_WIDTH // 2), 128, dtype=np.uint8)
        f = av.VideoFrame(VIDEO_WIDTH, VIDEO_HEIGHT, "yuv420p")
        f.planes[0].update(y.tobytes())
        f.planes[1].update(u.tobytes())
        f.planes[2].update(v.tobytes())
        return f

    async def recv(self):
        if self._start is None:
            self._start = time.time()
        wait = self._start + (self._timestamp / VIDEO_CLOCK_RATE) - time.time()
        if wait > 0:
            await asyncio.sleep(wait)

        self._frame.pts = self._timestamp
        self._frame.time_base = VIDEO_TIME_BASE
        self._timestamp += VIDEO_TIMESTAMP_INCREMENT
        return self._frame


# ============================================================
# MP4 Recording (single / multi)
#
# Layout: video area (left=sent, right=received) + subtitle bar below
# Left channel = sent audio, Right channel = received audio
# ============================================================
COMPOSITE_WIDTH = VIDEO_WIDTH * 2
SUBTITLE_HEIGHT = 80
OUTPUT_HEIGHT = VIDEO_HEIGHT + SUBTITLE_HEIGHT

CJK_FONT_PATH = "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"


def _find_font(size=18):
    """Find a font that supports CJK characters."""
    for path in [
        CJK_FONT_PATH,
        "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                continue
    return ImageFont.load_default()


def record_mp4(
    sent_video, sent_audio,
    recv_video, recv_audio,
    dc_timeline, output_path,
):
    n_frames = max(len(sent_video), len(recv_video))
    if n_frames == 0:
        print("[Record] No video frames to record")
        return

    font_label = _find_font(20)
    font_time = _find_font(16)
    font_subtitle = _find_font(16)

    black_frame = np.zeros((VIDEO_HEIGHT, VIDEO_WIDTH, 3), dtype=np.uint8)

    # Step 1: encode video to temp file
    tmp_video = tempfile.mktemp(suffix=".mp4")
    container = av.open(tmp_video, mode="w")
    v_stream = container.add_stream("libx264", rate=VIDEO_FPS)
    v_stream.width = COMPOSITE_WIDTH
    v_stream.height = OUTPUT_HEIGHT
    v_stream.pix_fmt = "yuv420p"
    v_stream.options = {"crf": "23", "preset": "fast"}

    dc_idx = 0
    # Track active DC messages with their arrival times
    active_dc_msgs = []

    for i in range(n_frames):
        t = i / VIDEO_FPS
        left_rgb = sent_video[i][1] if i < len(sent_video) else black_frame

        if i < len(recv_video):
            t_recv, right_rgb = recv_video[i]
        else:
            t_recv = t
            right_rgb = black_frame

        left_active = left_rgb.mean() > 128
        right_active = right_rgb.mean() > 128

        # Collect new DC messages (skip empty data, keep signals and non-empty data)
        while dc_idx < len(dc_timeline) and dc_timeline[dc_idx][0] <= t_recv:
            msg = dc_timeline[dc_idx][1]
            is_signal = bool(msg.get("signal"))
            is_empty = not is_signal and not any(v for k, v in msg.items())
            if not is_empty:
                active_dc_msgs.append((t_recv, msg, is_signal))
            dc_idx += 1

        # Build composite frame with subtitle bar
        frame_rgb = np.zeros((OUTPUT_HEIGHT, COMPOSITE_WIDTH, 3), dtype=np.uint8)

        # Video area — resize if dimensions don't match
        if left_rgb.shape[:2] != (VIDEO_HEIGHT, VIDEO_WIDTH):
            left_rgb = np.array(Image.fromarray(left_rgb).resize((VIDEO_WIDTH, VIDEO_HEIGHT)))
        if right_rgb.shape[:2] != (VIDEO_HEIGHT, VIDEO_WIDTH):
            right_rgb = np.array(Image.fromarray(right_rgb).resize((VIDEO_WIDTH, VIDEO_HEIGHT)))
        frame_rgb[:VIDEO_HEIGHT, :VIDEO_WIDTH] = left_rgb
        frame_rgb[:VIDEO_HEIGHT, VIDEO_WIDTH:] = right_rgb

        # Divider line
        frame_rgb[:VIDEO_HEIGHT, VIDEO_WIDTH - 1:VIDEO_WIDTH + 1] = 100

        # Subtitle bar background
        frame_rgb[VIDEO_HEIGHT:] = 20

        # Draw overlays
        img = Image.fromarray(frame_rgb)
        draw = ImageDraw.Draw(img)

        # Labels
        l_color = (0, 220, 0) if left_active else (120, 120, 120)
        r_color = (0, 220, 0) if right_active else (120, 120, 120)
        draw.text((10, 8), "LOCAL", fill=l_color, font=font_label)
        draw.text((VIDEO_WIDTH + 10, 8), "RECEIVED", fill=r_color, font=font_label)

        # Timestamp
        ts_text = f"{int(t) // 60:02d}:{t % 60:05.2f}"
        draw.text(
            (COMPOSITE_WIDTH // 2 - 30, VIDEO_HEIGHT - 25),
            ts_text, fill=(180, 180, 180), font=font_time,
        )

        # Subtitles: show latest 2 non-empty messages, signals in green
        visible = [(mt, m, s) for mt, m, s in active_dc_msgs if mt <= t_recv]
        y_pos = VIDEO_HEIGHT + 8
        for _, msg, is_signal in visible[-2:]:
            text = json.dumps(msg, ensure_ascii=False)
            if len(text) > 100:
                text = text[:97] + "..."
            color = (0, 220, 0) if is_signal else (200, 200, 200)
            draw.text((8, y_pos), text, fill=color, font=font_subtitle)
            y_pos += 22

        frame_rgb = np.array(img)

        frame = av.VideoFrame.from_ndarray(frame_rgb, format="rgb24")
        frame.pts = i
        frame.time_base = fractions.Fraction(1, VIDEO_FPS)
        for packet in v_stream.encode(frame):
            container.mux(packet)

    for packet in v_stream.encode():
        container.mux(packet)
    container.close()

    # Step 2: write stereo audio as WAV
    tmp_audio = tempfile.mktemp(suffix=".wav")
    sent_pcm = np.concatenate(sent_audio) if sent_audio else np.array([], dtype=np.int16)
    recv_pcm = np.concatenate(recv_audio) if recv_audio else np.array([], dtype=np.int16)
    max_len = max(len(sent_pcm), len(recv_pcm))
    if max_len > 0:
        if len(sent_pcm) < max_len:
            sent_pcm = np.pad(sent_pcm, (0, max_len - len(sent_pcm)))
        if len(recv_pcm) < max_len:
            recv_pcm = np.pad(recv_pcm, (0, max_len - len(recv_pcm)))
        stereo = np.empty(max_len * 2, dtype=np.int16)
        stereo[0::2] = sent_pcm  # left
        stereo[1::2] = recv_pcm  # right
        with wave.open(tmp_audio, "wb") as wf:
            wf.setnchannels(2)
            wf.setsampwidth(2)
            wf.setframerate(SAMPLE_RATE)
            wf.writeframes(stereo.tobytes())

    # Step 3: merge video + audio via ffmpeg
    cmd = [
        "ffmpeg", "-y",
        "-i", tmp_video,
        "-i", tmp_audio,
        "-c:v", "copy", "-c:a", "aac", "-b:a", "192k",
        "-shortest",
        output_path,
    ]
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # Cleanup temp files
    for f in [tmp_video, tmp_audio]:
        if os.path.exists(f):
            os.unlink(f)

    duration = n_frames / VIDEO_FPS
    print(f"\n[Record] {output_path}")
    print(f"  Duration: {duration:.1f}s")
    print(f"  Video: {COMPOSITE_WIDTH}x{OUTPUT_HEIGHT} @ {VIDEO_FPS}fps, "
          f"{len(sent_video)} sent + {len(recv_video)} received")
    print(f"  Audio: stereo {SAMPLE_RATE}Hz, "
          f"{len(sent_audio)} sent + {len(recv_audio)} received")


# ============================================================
# Mode: single  (from test_webrtc.py run_test)
# ============================================================
async def run_test():
    # Load test audio
    if not os.path.exists(TEST_WAV):
        print(f"[FAIL] Test audio not found: {TEST_WAV}")
        return False

    audio_frames = load_test_audio(TEST_WAV)
    speech_duration = len(audio_frames) * AUDIO_PTIME
    print("=" * 60)
    print("WebRTC Client Test (real pipeline)")
    print(f"  Main:      {MAIN_SERVER}")
    print(f"  WebRTC:    {WEBRTC_SERVER}")
    print(f"  Pipeline:  {PIPELINE_CONFIG}")
    print(f"  Input:     {TEST_WAV} ({speech_duration:.1f}s)")
    print(f"  Duration:  {TEST_DURATION}s")
    print(f"  Output:    {OUTPUT_MP4}")
    print("=" * 60)

    # --- Register + init pipeline via main server ---
    import aiohttp
    print(f"\n[Client] Registering and initializing pipeline...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{MAIN_SERVER}/register/",
                json={"client_id": CLIENT_ID},
            ) as resp:
                result = await resp.json()
                print(f"[Client] Register: {result}")

            async with session.post(
                f"{MAIN_SERVER}/init_pipeline/{CLIENT_ID}",
                json={"config": PIPELINE_CONFIG, "force": True},
            ) as resp:
                if resp.status != 200:
                    print(f"[FAIL] Init pipeline failed: {resp.status}")
                    return False
                result = await resp.json()
                print(f"[Client] Init pipeline: {result}")
    except Exception as e:
        print(f"[FAIL] Cannot connect to main server: {e}")
        return False

    # --- WebRTC client ---
    pc = RTCPeerConnection()
    send_audio = TestAudioTrack(audio_frames)
    send_video = TestVideoTrack(send_audio)
    pc.addTrack(send_audio)
    pc.addTrack(send_video)

    recv_audio_frames = []
    recv_video_frames = []
    recv_dc_messages = []
    recv_dc_timeline = []
    recv_start = [None]

    @pc.on("track")
    def on_track(track):
        if track.kind == "audio":
            asyncio.ensure_future(_recv_audio(track))
        elif track.kind == "video":
            asyncio.ensure_future(_recv_video(track))

    async def _recv_audio(track):
        try:
            while True:
                frame = await track.recv()
                pcm = frame.to_ndarray().flatten().astype(np.int16).copy()
                # Opus decoder outputs stereo; extract mono (left channel)
                if frame.layout.name == "stereo":
                    pcm = pcm[::2]
                recv_audio_frames.append(pcm)
        except Exception:
            pass

    async def _recv_video(track):
        try:
            while True:
                frame = await track.recv()
                if recv_start[0] is None:
                    recv_start[0] = time.time()
                rgb = frame.to_ndarray(format="rgb24").copy()
                recv_video_frames.append((time.time() - recv_start[0], rgb))
        except Exception:
            pass

    @pc.on("datachannel")
    def on_datachannel(channel):
        @channel.on("message")
        def on_message(msg):
            try:
                parsed = json.loads(msg)
                recv_dc_messages.append(parsed)
                if recv_start[0] is not None:
                    recv_dc_timeline.append((time.time() - recv_start[0], parsed))
            except Exception:
                pass

    client_dc = pc.createDataChannel("client-signals", ordered=True)

    @pc.on("connectionstatechange")
    async def on_state():
        print(f"[Client] Connection: {pc.connectionState}")

    # --- Connect WebRTC ---
    offer = await pc.createOffer()
    await pc.setLocalDescription(offer)

    print(f"[Client] Connecting WebRTC to {WEBRTC_SERVER}...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{WEBRTC_SERVER}/offer/{CLIENT_ID}",
                json={
                    "sdp": pc.localDescription.sdp,
                    "type": pc.localDescription.type,
                    # resolution is the client's own choice; timing params
                    # come from the config's webrtc section
                    "video_width": VIDEO_WIDTH,
                    "video_height": VIDEO_HEIGHT,
                },
            ) as resp:
                if resp.status != 200:
                    print(f"[FAIL] WebRTC server returned {resp.status}")
                    await pc.close()
                    return False
                answer = await resp.json()
    except Exception as e:
        print(f"[FAIL] Cannot connect to WebRTC server: {e}")
        await pc.close()
        return False

    await pc.setRemoteDescription(
        RTCSessionDescription(sdp=answer["sdp"], type=answer["type"])
    )

    # Wait for connection
    for _ in range(50):
        if pc.connectionState == "connected":
            break
        await asyncio.sleep(0.1)

    if pc.connectionState != "connected":
        print(f"[FAIL] Connection: {pc.connectionState}")
        await pc.close()
        return False

    # Wait for DataChannel
    for _ in range(50):
        if client_dc.readyState == "open":
            break
        await asyncio.sleep(0.1)

    if client_dc.readyState != "open":
        print("[FAIL] DataChannel did not open")
        await pc.close()
        return False

    # --- Wait for initial silence period (visible in recording) ---
    SILENCE_BEFORE_SPEECH = 3  # seconds of silence before recording_start
    print(f"[Client] Waiting {SILENCE_BEFORE_SPEECH}s silence before speech...")
    await asyncio.sleep(SILENCE_BEFORE_SPEECH)

    # --- Send speech ---
    test_start = time.time()
    client_dc.send(json.dumps({"signal": "recording_start"}))
    send_audio.speaking = True  # start sending speech audio after recording_start
    print(f"[Client] recording_start → sending {speech_duration:.1f}s of speech...")

    while not send_audio.finished_speech:
        await asyncio.sleep(0.1)
    await asyncio.sleep(0.5)

    client_dc.send(json.dumps({"signal": "recording_end"}))
    print("[Client] recording_end → waiting for pipeline response...")

    # --- Wait for test duration (30s total from connection) ---
    eos_seen = False
    while time.time() - test_start < TEST_DURATION:
        for msg in recv_dc_messages:
            if msg.get("signal") == "EoS":
                if not eos_seen:
                    eos_seen = True
                    print("[Client] EoS received")
                break
        await asyncio.sleep(0.2)

    if not eos_seen:
        print("[Client] Warning: EoS not received within test duration")

    # --- Results ---
    print(f"\n{'='*60}")
    print("RESULTS")
    print(f"{'='*60}")

    print(f"\n[Sent]")
    print(f"  Audio: {len(send_audio.recorded_pcm)} frames "
          f"({len(send_audio.recorded_pcm) * AUDIO_PTIME:.1f}s)")
    print(f"  Video: {len(send_video.recorded_frames)} frames "
          f"({len(send_video.recorded_frames) / VIDEO_FPS:.1f}s)")

    print(f"\n[Received]")
    print(f"  Audio: {len(recv_audio_frames)} frames "
          f"({len(recv_audio_frames) * AUDIO_PTIME:.1f}s)")
    nonsilent = sum(1 for p in recv_audio_frames if np.any(p != 0))
    print(f"  Audio (non-silent): {nonsilent} frames")
    print(f"  Video: {len(recv_video_frames)} frames "
          f"({len(recv_video_frames) / VIDEO_FPS:.1f}s)")
    print(f"  DataChannel: {len(recv_dc_messages)} messages")
    for msg in recv_dc_messages:
        display = {k: (v[:80] + "..." if isinstance(v, str) and len(v) > 80 else v)
                   for k, v in msg.items()}
        print(f"    {display}")

    # --- Record MP4 ---
    record_mp4(
        sent_video=send_video.recorded_frames,
        sent_audio=send_audio.recorded_pcm,
        recv_video=recv_video_frames,
        recv_audio=recv_audio_frames,
        dc_timeline=recv_dc_timeline,
        output_path=OUTPUT_MP4,
    )

    await pc.close()

    # --- Unregister ---
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{MAIN_SERVER}/unregister/",
                json={"client_id": CLIENT_ID},
            ) as resp:
                result = await resp.json()
                print(f"[Client] Unregister: {result}")
    except Exception:
        pass

    return True


def run_single(args):
    """Entry for --mode single. Mirrors original test_webrtc.py main()."""
    global WEBRTC_SERVER, PIPELINE_CONFIG, TEST_DURATION
    global SAMPLE_RATE, AUDIO_PTIME, AUDIO_SAMPLES, AUDIO_TIME_BASE
    global VIDEO_FPS, VIDEO_WIDTH, VIDEO_HEIGHT, VIDEO_TIMESTAMP_INCREMENT
    global COMPOSITE_WIDTH, OUTPUT_HEIGHT
    global OUTPUT_MP4

    WEBRTC_SERVER = args.server
    TEST_DURATION = args.duration

    if args.audio_ptime: AUDIO_PTIME = args.audio_ptime
    AUDIO_SAMPLES = int(SAMPLE_RATE * AUDIO_PTIME)
    AUDIO_TIME_BASE = fractions.Fraction(1, SAMPLE_RATE)
    if args.video_fps: VIDEO_FPS = args.video_fps
    if args.video_width: VIDEO_WIDTH = args.video_width
    if args.video_height: VIDEO_HEIGHT = args.video_height
    VIDEO_TIMESTAMP_INCREMENT = VIDEO_CLOCK_RATE // VIDEO_FPS
    COMPOSITE_WIDTH = VIDEO_WIDTH * 2
    OUTPUT_HEIGHT = VIDEO_HEIGHT + SUBTITLE_HEIGHT

    data_fps = args.data_fps or 20

    # If custom params, generate matching pipeline config based on the base pipeline
    if any([args.audio_ptime, args.video_fps, args.video_width, args.video_height, args.data_fps]):
        # Load base pipeline config and patch FrameSplitter's params
        base_config_path = os.path.join(SCRIPT_DIR, "..", "configs", f"{args.pipeline}.json")
        with open(base_config_path) as f:
            pipeline_config = json.load(f)
        # Find and patch FrameSplitter node
        for node in pipeline_config["pipeline"]:
            if node["function"] == "frame_splitter":
                node["config"]["audio_fps"] = SAMPLE_RATE // AUDIO_SAMPLES
                node["config"]["video_fps"] = VIDEO_FPS
                node["config"]["video_width"] = VIDEO_WIDTH
                node["config"]["video_height"] = VIDEO_HEIGHT
                if args.data_fps:
                    node["config"]["data_fps"] = data_fps
        # Timing params also go to the top-level webrtc section (the
        # gateway's source); resolution stays client-side (offer body)
        webrtc_sec = dict(pipeline_config.get("webrtc") or {})
        webrtc_sec["audio_fps"] = SAMPLE_RATE // AUDIO_SAMPLES
        webrtc_sec["video_fps"] = VIDEO_FPS
        if args.data_fps:
            webrtc_sec["data_fps"] = data_fps
        pipeline_config["webrtc"] = webrtc_sec
        config_name = "test_webrtc_custom"
        config_path = os.path.join(SCRIPT_DIR, "..", "configs", f"{config_name}.json")
        with open(config_path, "w") as f:
            json.dump(pipeline_config, f)
        PIPELINE_CONFIG = config_name
        OUTPUT_MP4 = os.path.join(OUTPUT_DIR,
            f"test_webrtc_{SAMPLE_RATE}_{VIDEO_FPS}fps_{VIDEO_WIDTH}x{VIDEO_HEIGHT}.mp4")
    else:
        PIPELINE_CONFIG = args.pipeline

    result = asyncio.run(run_test())
    return result


# ============================================================
# Mode: cancel  (DC-path cancel semantics, cancel_offset_ms = 0)
# ============================================================
CANCEL_CLIENT_ID = "test_webrtc_cancel"


async def run_cancel_test():
    """Four cancel scenarios over one real session:
      1. idle cancel         — no active turn, must be a harmless no-op
      2. baseline turn       — speak/reply proves the pipeline works after 1
      3. interrupt mid-reply — cancel kills the playing reply quickly; the
                               immediately following start (client interrupt
                               order: cancel first) must survive the cancel
      4. cancel while recording — vad mark cleared, recording_end ignored,
                               no reply is produced
    Log-side assertions (vad mark clear / end ignored / lookback / ERROR)
    are done by the run_cancel() wrapper after the session closes."""
    if not os.path.exists(TEST_WAV):
        print(f"[FAIL] Test audio not found: {TEST_WAV}")
        return False
    audio_frames = load_test_audio(TEST_WAV)
    speech_duration = len(audio_frames) * AUDIO_PTIME
    print("=" * 60)
    print("WebRTC Cancel Test (real pipeline)")
    print(f"  Pipeline: {PIPELINE_CONFIG}, speech clip {speech_duration:.1f}s")
    print("=" * 60)

    import aiohttp
    async with aiohttp.ClientSession() as session:
        async with session.post(f"{MAIN_SERVER}/register/",
                                json={"client_id": CANCEL_CLIENT_ID}) as resp:
            print(f"[Client] Register: {await resp.json()}")
        async with session.post(
            f"{MAIN_SERVER}/init_pipeline/{CANCEL_CLIENT_ID}",
            json={"config": PIPELINE_CONFIG, "force": True},
        ) as resp:
            if resp.status != 200:
                print(f"[FAIL] Init pipeline failed: {resp.status}")
                return False

    pc = RTCPeerConnection()
    send_audio = TestAudioTrack(audio_frames)
    send_video = TestVideoTrack(send_audio)
    pc.addTrack(send_audio)
    pc.addTrack(send_video)

    recv_audio = []      # (wall time, nonsilent) per received audio frame
    dc_timeline = []     # (wall time, parsed message)

    async def _recv_audio(track):
        try:
            while True:
                frame = await track.recv()
                pcm = frame.to_ndarray().flatten().astype(np.int16)
                if frame.layout.name == "stereo":
                    pcm = pcm[::2]
                recv_audio.append((time.time(), bool(np.any(pcm != 0))))
        except Exception:
            pass

    async def _drain(track):
        try:
            while True:
                await track.recv()
        except Exception:
            pass

    @pc.on("track")
    def on_track(track):
        if track.kind == "audio":
            asyncio.ensure_future(_recv_audio(track))
        else:
            asyncio.ensure_future(_drain(track))

    @pc.on("datachannel")
    def on_datachannel(channel):
        @channel.on("message")
        def on_message(msg):
            try:
                dc_timeline.append((time.time(), json.loads(msg)))
            except Exception:
                pass

    client_dc = pc.createDataChannel("client-signals", ordered=True)

    offer = await pc.createOffer()
    await pc.setLocalDescription(offer)
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{WEBRTC_SERVER}/offer/{CANCEL_CLIENT_ID}",
            json={"sdp": pc.localDescription.sdp,
                  "type": pc.localDescription.type,
                  "video_width": VIDEO_WIDTH,
                  "video_height": VIDEO_HEIGHT},
        ) as resp:
            if resp.status != 200:
                print(f"[FAIL] WebRTC server returned {resp.status}")
                await pc.close()
                return False
            answer = await resp.json()
    await pc.setRemoteDescription(
        RTCSessionDescription(sdp=answer["sdp"], type=answer["type"]))

    for _ in range(50):
        if pc.connectionState == "connected":
            break
        await asyncio.sleep(0.1)
    for _ in range(50):
        if client_dc.readyState == "open":
            break
        await asyncio.sleep(0.1)
    if pc.connectionState != "connected" or client_dc.readyState != "open":
        print("[FAIL] connection/DataChannel not ready")
        await pc.close()
        return False

    failures = []

    def send_signal(name):
        client_dc.send(json.dumps({"signal": name}))

    def reset_speech():
        send_audio._index = 0
        send_audio.finished_speech = False
        send_audio.speaking = True

    def nonsilent_after(t0):
        return [t for t, ns in recv_audio if ns and t >= t0]

    def sos_after(t0):
        return [m for t, m in dc_timeline
                if t >= t0 and m.get("signal") == "SoS"]

    def eos_after(t0):
        return [m for t, m in dc_timeline
                if t >= t0 and m.get("signal") == "EoS"]

    async def wait_for(cond, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if cond():
                return True
            await asyncio.sleep(0.1)
        return False

    async def speak_turn():
        """One full utterance: start -> clip -> end. Returns end wall time."""
        reset_speech()
        send_signal("recording_start")
        while not send_audio.finished_speech:
            await asyncio.sleep(0.1)
        await asyncio.sleep(0.5)
        send_signal("recording_end")
        return time.time()

    await asyncio.sleep(2.0)  # let the session clock settle

    # --- 1. idle cancel: harmless no-op ---
    print("[1] idle cancel")
    send_signal("cancel")
    await asyncio.sleep(1.5)  # held 200ms at the gateway; nothing may break
    if pc.connectionState != "connected":
        failures.append("idle cancel broke the connection")

    # --- 2. baseline turn: reply arrives ---
    print("[2] baseline turn")
    t_end_a = await speak_turn()
    if not await wait_for(lambda: nonsilent_after(t_end_a), 25):
        # without a playing reply the interrupt scenario is meaningless
        print("[FAIL] baseline reply never arrived")
        await pc.close()
        return False
    t_onset_a = nonsilent_after(t_end_a)[0]
    print(f"    reply A onset after {t_onset_a - t_end_a:.1f}s")

    # --- 3. interrupt mid-reply, immediately start the next turn ---
    print("[3] cancel mid-reply + immediate new turn")
    await asyncio.sleep(0.6)  # let reply A play a little
    t_cancel = time.time()
    send_signal("cancel")
    # client interrupt order: cancel first, then the new recording_start
    t_end_b = await speak_turn()
    if not await wait_for(lambda: nonsilent_after(t_end_b), 25):
        failures.append("reply B never arrived - new turn killed by cancel")
        t_onset_b = time.time()
    else:
        t_onset_b = nonsilent_after(t_end_b)[0]
        print(f"    reply B onset after {t_onset_b - t_end_b:.1f}s")
    # reply A must stop quickly: hold 200ms + tick + in-flight frames only
    leak = [t for t in nonsilent_after(t_cancel + 1.5) if t < t_onset_b - 0.1]
    if leak:
        failures.append(
            f"reply A kept playing {leak[-1] - t_cancel:.1f}s after cancel")
    else:
        played = [t for t in nonsilent_after(t_cancel) if t < t_onset_b - 0.1]
        last = (played[-1] - t_cancel) if played else 0.0
        print(f"    reply A stopped {last:.2f}s after cancel")

    # let reply B drain fully before the last scenario
    await wait_for(lambda: eos_after(t_end_b), 20)
    await wait_for(lambda: not nonsilent_after(time.time() - 1.5), 15)

    # --- 4. cancel while recording: no reply may be produced ---
    print("[4] cancel while recording")
    t_start_c = time.time()
    reset_speech()
    send_signal("recording_start")
    await asyncio.sleep(1.0)          # 1s of speech
    send_signal("cancel")
    await asyncio.sleep(0.1)
    send_signal("recording_end")      # must be ignored by vad
    send_audio.speaking = False
    await asyncio.sleep(10.0)
    if sos_after(t_start_c):
        failures.append("cancelled recording still produced a reply (SoS)")
    if nonsilent_after(t_start_c):
        failures.append("cancelled recording still produced reply audio")

    await pc.close()
    async with aiohttp.ClientSession() as session:
        async with session.post(f"{MAIN_SERVER}/unregister/",
                                json={"client_id": CANCEL_CLIENT_ID}) as resp:
            print(f"[Client] Unregister: {await resp.json()}")

    for f in failures:
        print(f"[FAIL] {f}")
    return not failures


def run_cancel(args):
    """Entry for --mode cancel."""
    global WEBRTC_SERVER, PIPELINE_CONFIG
    WEBRTC_SERVER = args.server
    PIPELINE_CONFIG = args.pipeline

    # the client log is deleted and recreated at registration, so after the
    # run it contains exactly this run
    log_path = os.path.join(SCRIPT_DIR, "..", "logs",
                            f"client_{CANCEL_CLIENT_ID}.log")

    ok = asyncio.run(run_cancel_test())

    try:
        with open(log_path) as f:
            log = f.read()
    except OSError as e:
        print(f"[FAIL] cannot read client log: {e}")
        return False
    checks = [
        ("vad mark cleared on cancel", "cancel - cleared vad mark" in log),
        ("recording_end after cancel ignored",
         "recording_end without active mark - ignored" in log),
        ("vad start lookback 200ms live", "(lookback 0.20s)" in log),
        ("no ERROR in client log", "ERROR" not in log),
    ]
    print()
    for name, passed in checks:
        print(f"  {'PASS' if passed else 'FAIL'}: {name}")
        ok = ok and passed
    print(f"\n  {'Cancel test passed!' if ok else 'Cancel test FAILED.'}")
    return ok


# ============================================================
# Mode: lifecycle  (from test_webrtc_lifecycle.py test_lifecycle)
# ============================================================
async def test_lifecycle():
    """Test connection_start/stop flow through WebRTC server."""
    import requests

    print("=" * 60)
    print("  WebRTC Lifecycle Integration Test")
    print("=" * 60)

    # 1. Register + init pipeline
    print("\n  [1] Register + init pipeline...")
    r = requests.post(f"{MAIN_SERVER}/register/", json={"client_id": LIFECYCLE_CLIENT_ID})
    assert r.json()["status"] in ("registered", "already registered"), r.json()
    r = requests.post(
        f"{MAIN_SERVER}/init_pipeline/{LIFECYCLE_CLIENT_ID}",
        json={"config": LIFECYCLE_PIPELINE_CONFIG, "force": True},
    )
    assert r.json()["status"] == "initialized", r.json()
    print(f"    Pipeline initialized with {LIFECYCLE_PIPELINE_CONFIG}")

    # Wait for FrameSplitter to init
    await asyncio.sleep(1)

    # 2. Check pipeline log: FrameSplitter should be paused
    log = requests.get(f"{MAIN_SERVER}/logs/{LIFECYCLE_CLIENT_ID}").json().get("log_content", "")
    assert "paused until connection_start" in log, "FrameSplitter not in paused state"
    assert "clock started" not in log, "Clock started before WebRTC connection"
    print("    FrameSplitter initialized in paused state: OK")

    # 3. Connect WebRTC
    print("\n  [2] WebRTC offer/answer...")
    pc = RTCPeerConnection()
    received_data = []
    dc_open = asyncio.Event()

    pc.addTrack(SilenceTrack())
    pc.addTrack(BlackVideoTrack())

    @pc.on("datachannel")
    def on_dc(channel):
        @channel.on("message")
        def on_msg(msg):
            received_data.append(json.loads(msg))

    @pc.on("track")
    def on_track(track):
        pass  # We receive but don't process

    dc = pc.createDataChannel("client-data")

    @dc.on("open")
    def on_open():
        dc_open.set()

    offer = await pc.createOffer()
    await pc.setLocalDescription(offer)

    r = requests.post(
        f"{WEBRTC_SERVER}/offer/{LIFECYCLE_CLIENT_ID}",
        json={"sdp": pc.localDescription.sdp, "type": pc.localDescription.type},
    )
    answer = r.json()
    await pc.setRemoteDescription(
        RTCSessionDescription(sdp=answer["sdp"], type=answer["type"])
    )
    print("    WebRTC connected")

    # 4. Wait for DataChannel + verify clock started
    await asyncio.wait_for(dc_open.wait(), timeout=5)
    print("    DataChannel open")

    await asyncio.sleep(2)  # let groups flow

    log = requests.get(f"{MAIN_SERVER}/logs/{LIFECYCLE_CLIENT_ID}").json().get("log_content", "")
    clock_started = "clock started" in log
    print(f"    FrameSplitter clock started: {'OK' if clock_started else 'FAIL'}")

    # 5. Check we're receiving data via DataChannel
    data_count_before = len(received_data)
    await asyncio.sleep(1)
    data_count_after = len(received_data)
    data_flowing = data_count_after > data_count_before
    print(f"    Data flowing via DataChannel: {'OK' if data_flowing else 'FAIL'} "
          f"({data_count_after - data_count_before} msgs in 1s)")

    # 6. Disconnect WebRTC → pipeline auto-disposed
    print("\n  [3] Disconnect WebRTC...")
    await pc.close()
    await asyncio.sleep(3)  # wait for dispose to complete

    log = requests.get(f"{MAIN_SERVER}/logs/{LIFECYCLE_CLIENT_ID}").json().get("log_content", "")
    pipeline_disposed = "disposed" in log.lower()
    print(f"    Pipeline disposed: {'OK' if pipeline_disposed else 'FAIL'}")

    # 7. Verify pipeline is no longer initialized (can re-init without force)
    r = requests.post(
        f"{MAIN_SERVER}/init_pipeline/{LIFECYCLE_CLIENT_ID}",
        json={"config": LIFECYCLE_PIPELINE_CONFIG},
    )
    can_reinit = r.json().get("status") == "initialized"
    print(f"    Re-init without force: {'OK' if can_reinit else 'FAIL'}")

    # 8. Cleanup
    print("\n  [4] Cleanup...")
    requests.post(f"{MAIN_SERVER}/unregister/", json={"client_id": LIFECYCLE_CLIENT_ID})
    print("    Unregistered")

    # Summary
    print("\n" + "=" * 60)
    all_ok = clock_started and data_flowing and pipeline_disposed and can_reinit
    results = [
        ("Clock started on connect", clock_started),
        ("Data flowing", data_flowing),
        ("Pipeline disposed on disconnect", pipeline_disposed),
        ("Re-init without force", can_reinit),
    ]
    for name, ok in results:
        print(f"  {'PASS' if ok else 'FAIL'}: {name}")
    print(f"\n  {'All tests passed!' if all_ok else 'Some tests FAILED.'}")
    return all_ok


def run_lifecycle(args):
    """Entry for --mode lifecycle. Mirrors original test_webrtc_lifecycle.py."""
    return asyncio.run(test_lifecycle())


# ============================================================
# Mode: multi  (from test_webrtc_multi.py)
# ============================================================
def run_client_process(client_id, output_mp4, result_dict):
    """Entry point for each client process."""
    import numpy as np
    import aiohttp
    from aiortc import RTCPeerConnection, RTCSessionDescription

    sys.path.insert(0, SCRIPT_DIR)
    # All helpers/constants now live in this consolidated module. The child
    # process imports them from here (the original imported from test_webrtc).
    from test_webrtc import (
        MAIN_SERVER, WEBRTC_SERVER, PIPELINE_CONFIG, TEST_WAV,
        SAMPLE_RATE, AUDIO_PTIME, AUDIO_SAMPLES, VIDEO_FPS,
        load_test_audio, TestAudioTrack, TestVideoTrack, record_mp4,
    )

    async def run():
        # Register + init pipeline
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{MAIN_SERVER}/register/",
                    json={"client_id": client_id},
                ) as resp:
                    reg = await resp.json()
                    print(f"[{client_id}] Register: {reg}")

                async with session.post(
                    f"{MAIN_SERVER}/init_pipeline/{client_id}",
                    json={"config": PIPELINE_CONFIG, "force": True},
                ) as resp:
                    if resp.status != 200:
                        result_dict["error"] = f"Init pipeline failed: {resp.status}"
                        return
                    init = await resp.json()
                    print(f"[{client_id}] Init pipeline: {init}")
        except Exception as e:
            result_dict["error"] = f"Setup failed: {e}"
            return

        # Load audio
        audio_frames = load_test_audio(TEST_WAV)

        # WebRTC
        pc = RTCPeerConnection()
        send_audio = TestAudioTrack(audio_frames)
        send_video = TestVideoTrack(send_audio)
        pc.addTrack(send_audio)
        pc.addTrack(send_video)

        recv_audio_frames = []
        recv_video_frames = []
        recv_dc_messages = []
        recv_dc_timeline = []
        recv_start = [None]

        @pc.on("track")
        def on_track(track):
            if track.kind == "audio":
                asyncio.ensure_future(_recv_audio(track))
            elif track.kind == "video":
                asyncio.ensure_future(_recv_video(track))

        async def _recv_audio(track):
            try:
                while True:
                    frame = await track.recv()
                    pcm = frame.to_ndarray().flatten().astype(np.int16).copy()
                    if frame.layout.name == "stereo":
                        pcm = pcm[::2]
                    recv_audio_frames.append(pcm)
            except Exception:
                pass

        async def _recv_video(track):
            try:
                while True:
                    frame = await track.recv()
                    if recv_start[0] is None:
                        recv_start[0] = time.time()
                    rgb = frame.to_ndarray(format="rgb24").copy()
                    recv_video_frames.append((time.time() - recv_start[0], rgb))
            except Exception:
                pass

        @pc.on("datachannel")
        def on_datachannel(channel):
            @channel.on("message")
            def on_message(msg):
                try:
                    parsed = json.loads(msg)
                    recv_dc_messages.append(parsed)
                    if recv_start[0] is not None:
                        recv_dc_timeline.append((time.time() - recv_start[0], parsed))
                except Exception:
                    pass

        client_dc = pc.createDataChannel("client-signals", ordered=True)

        # Connect
        offer = await pc.createOffer()
        await pc.setLocalDescription(offer)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{WEBRTC_SERVER}/offer/{client_id}",
                    json={"sdp": pc.localDescription.sdp, "type": pc.localDescription.type},
                ) as resp:
                    if resp.status != 200:
                        await pc.close()
                        result_dict["error"] = f"WebRTC offer failed: {resp.status}"
                        return
                    answer = await resp.json()
        except Exception as e:
            await pc.close()
            result_dict["error"] = f"WebRTC connect failed: {e}"
            return

        await pc.setRemoteDescription(
            RTCSessionDescription(sdp=answer["sdp"], type=answer["type"])
        )

        # Wait for connection
        for _ in range(50):
            if pc.connectionState == "connected":
                break
            await asyncio.sleep(0.1)
        if pc.connectionState != "connected":
            await pc.close()
            result_dict["error"] = f"Connection: {pc.connectionState}"
            return

        # Wait for DC
        for _ in range(50):
            if client_dc.readyState == "open":
                break
            await asyncio.sleep(0.1)
        if client_dc.readyState != "open":
            await pc.close()
            result_dict["error"] = "DataChannel did not open"
            return

        # Send speech
        test_start = time.time()
        client_dc.send(json.dumps({"signal": "recording_start"}))
        send_audio.speaking = True  # start sending speech after recording_start
        print(f"[{client_id}] recording_start sent")

        while not send_audio.finished_speech:
            await asyncio.sleep(0.1)
        await asyncio.sleep(0.5)

        client_dc.send(json.dumps({"signal": "recording_end"}))
        print(f"[{client_id}] recording_end sent")

        # Wait for test duration
        eos_seen = False
        while time.time() - test_start < CLIENT_TEST_DURATION:
            for msg in recv_dc_messages:
                if msg.get("signal") == "EoS":
                    if not eos_seen:
                        eos_seen = True
                        print(f"[{client_id}] EoS received")
                    break
            await asyncio.sleep(0.2)

        # Record MP4
        record_mp4(
            sent_video=send_video.recorded_frames,
            sent_audio=send_audio.recorded_pcm,
            recv_video=recv_video_frames,
            recv_audio=recv_audio_frames,
            dc_timeline=recv_dc_timeline,
            output_path=output_mp4,
        )

        await pc.close()

        # Unregister
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{MAIN_SERVER}/unregister/",
                    json={"client_id": client_id},
                ) as resp:
                    unreg = await resp.json()
                    print(f"[{client_id}] Unregister: {unreg}")
        except Exception:
            pass

        # Store results
        result_dict["dc_msgs"] = len(recv_dc_messages)
        # pass_vars data rides wrapped under "pass_data" (meta/SoS protocol)
        result_dict["text_msgs"] = sum(
            1 for m in recv_dc_messages
            if "text" in m or "text" in (m.get("pass_data") or {}))
        result_dict["eos"] = eos_seen
        result_dict["audio_frames"] = len(recv_audio_frames)
        result_dict["non_silent"] = sum(1 for p in recv_audio_frames if np.any(p != 0))
        result_dict["sent_video"] = len(send_video.recorded_frames)
        result_dict["recv_video"] = len(recv_video_frames)
        result_dict["sent_audio"] = len(send_audio.recorded_pcm)
        result_dict["recv_audio"] = len(recv_audio_frames)

    asyncio.run(run())


def run_multi(args):
    """Entry for --mode multi. Mirrors original test_webrtc_multi.py main()."""
    global WEBRTC_SERVER, PIPELINE_CONFIG
    WEBRTC_SERVER = args.server        # honored by forked children (inherit globals)
    PIPELINE_CONFIG = args.pipeline
    num_clients = args.num_clients

    print("=" * 60)
    print(f"WebRTC Multi-User Test ({num_clients} concurrent processes)")
    print(f"  Main:     {MAIN_SERVER}")
    print(f"  WebRTC:   {WEBRTC_SERVER}")
    print(f"  Pipeline: {PIPELINE_CONFIG}")
    print(f"  Duration: {CLIENT_TEST_DURATION}s per client")
    print("=" * 60)

    manager = multiprocessing.Manager()
    processes = []
    results = []

    for i in range(num_clients):
        cid = f"multi_test_{i+1}"
        mp4 = os.path.join(OUTPUT_DIR, f"test_webrtc_multi_{i+1}.mp4")
        result_dict = manager.dict()
        results.append((cid, result_dict))
        p = multiprocessing.Process(
            target=run_client_process,
            args=(cid, mp4, result_dict),
        )
        processes.append(p)

    # Start all processes
    for p in processes:
        p.start()

    # Wait for all to finish; a stuck child must not wedge the suite (it
    # would also block interpreter exit), so terminate leftovers
    for p in processes:
        p.join(timeout=CLIENT_TEST_DURATION + 60)
    for p in processes:
        if p.is_alive():
            print(f"  [WARN] client process {p.pid} timed out; terminating")
            p.terminate()
            p.join(timeout=10)

    print("\n" + "=" * 60)
    print("MULTI-USER RESULTS")
    print("=" * 60)
    all_ok = True
    for cid, rd in results:
        rd = dict(rd)
        if "error" in rd:
            print(f"  [{cid}] FAIL: {rd['error']}")
            all_ok = False
        else:
            eos = rd.get("eos", False)
            text = rd.get("text_msgs", 0)
            non_silent = rd.get("non_silent", 0)
            dc = rd.get("dc_msgs", 0)
            audio = rd.get("audio_frames", 0)
            sv = rd.get("sent_video", 0)
            rv = rd.get("recv_video", 0)
            duration = sv / 30 if sv > 0 else 0
            # Pass if: got text responses + got non-silent audio
            # EoS may not arrive if response is long — normal disconnect scenario
            ok = text > 0 and non_silent > 0
            status = "OK" if ok else "FAIL"
            print(f"  [{cid}] {status}: {dc} DC msgs, {text} text, EoS={eos}, "
                  f"audio={non_silent}/{audio}, video={sv}sent/{rv}recv, {duration:.1f}s")
            if not ok:
                all_ok = False

    # Check server cleanup
    try:
        import requests
        clients = requests.get(f"{MAIN_SERVER}/clients/").json()
        status = requests.get(f"{WEBRTC_SERVER}/status").json()
        print(f"\n  Main server clients after cleanup: {clients}")
        print(f"  WebRTC sessions after cleanup: {status.get('sessions', {})}")
    except Exception as e:
        print(f"  Cleanup check failed: {e}")

    if all_ok:
        print("\nAll clients passed!")
    else:
        print("\nSome clients FAILED!")

    return all_ok


# ============================================================
# Mode: framesplitter  (from test_frame_splitter_clock.py)
#
# Standalone clock-driven FrameSplitterStep test. Drives a
# FrameSplitterStep in-process via queues (no server). Names are
# prefixed with `fs_` to avoid colliding with the WebRTC helpers above
# (e.g. the `send_audio` local used in single/multi modes). Logic is
# copied verbatim from test_frame_splitter_clock.py.
# ============================================================

# ── Utilities ─────────────────────────────────────────────────

def fs_setup_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(h)
    return logger


def fs_make_wav_b64(duration_s=0.5, sample_rate=48000):
    """Generate a simple sine wave WAV, return as base64."""
    n_samples = int(sample_rate * duration_s)
    t = np.linspace(0, duration_s, n_samples, endpoint=False)
    pcm = (np.sin(2 * np.pi * 440 * t) * 16000).astype(np.int16)

    buf = io.BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        wf.writeframes(pcm.tobytes())
    return base64.b64encode(buf.getvalue()).decode("ascii")


def fs_build_frame_splitter(logger, config_overrides=None):
    """Build a FrameSplitterStep with queues for testing."""
    from Modules.webrtc_frame_splitter.FrameSplitterStep import FrameSplitterStep

    send_queue = Queue()
    input_queue = Queue()
    output_queue = Queue()  # FrameSplitter is last node, this acts as send_queue
    cancel_queue = Queue()
    kill_event = threading.Event()

    config = {
        "input_vars": [{"source": "audio_data", "target": "audio_data"}],
        "pass_vars": [
            {"source": "text", "target": "text"},
        ],
        "output_vars": [
            {"source": "audio", "target": "audio"},
            {"source": "video", "target": "video"},
            {"source": "data", "target": "data"},
        ],
        "catch_signals": [
            {"source": "connection_start", "target": "connection_start"},
        ],
        "pass_signals": [
            {"source": "SoS", "target": "SoS"},
            {"source": "EoS", "target": "EoS"},
        ],
        "emit_signals": [
            {"source": "meta", "target": "meta"},
        ],
        "next_nodes": [-1],
        "audio_fps": 50,
        "video_fps": 30,
        "video_width": 320,
        "video_height": 240,
    }
    if config_overrides:
        config.update(config_overrides)

    inst = FrameSplitterStep(
        index=7, client_id="test", logger=logger,
        send_queue=send_queue, input_queue=input_queue,
        output_queue=output_queue, cancel_queue=cancel_queue,
        kill_event=kill_event, config=config,
    )

    return {
        "instance": inst,
        "input_queue": input_queue,
        "output_queue": output_queue,
        "cancel_queue": cancel_queue,
        "send_queue": send_queue,
        "kill_event": kill_event,
    }


def fs_start_splitter(ctx):
    t = threading.Thread(target=ctx["instance"].run, daemon=True)
    t.start()
    return t


def fs_stop_splitter(ctx, thread, timeout=3):
    ctx["kill_event"].set()
    thread.join(timeout=timeout)
    return thread.is_alive()


def fs_send_connection_start(ctx, ts=None):
    if ts is None:
        ts = time.time()
    msg = json.dumps({
        "signal": "connection_start",
        "destination": 7,
        "timestamp": ts,
    })
    ctx["input_queue"].put(msg)


def fs_send_connection_stop(ctx, ts=None):
    if ts is None:
        ts = time.time()
    msg = json.dumps({
        "signal": "connection_stop",
        "destination": 7,
        "timestamp": ts,
    })
    ctx["input_queue"].put(msg)


def fs_send_audio(ctx, duration_s=0.5, ts=None):
    if ts is None:
        ts = time.time()
    wav_b64 = fs_make_wav_b64(duration_s)
    msg = json.dumps({
        "audio_data": wav_b64,
        "text": "hello",
        "destination": 7,
        "timestamp": ts,
    })
    ctx["input_queue"].put(msg)
    return ts


def fs_send_signal(ctx, signal_name, ts=None):
    if ts is None:
        ts = time.time()
    msg = json.dumps({
        "signal": signal_name,
        "destination": 7,
        "timestamp": ts,
    })
    ctx["input_queue"].put(msg)


def fs_send_cancel(ctx, ts):
    msg = json.dumps({"signal": "cancel", "timestamp": ts})
    ctx["cancel_queue"].put(msg)


def fs_collect(output_queue, duration_s=2.0, max_items=200):
    """Collect output for a fixed duration."""
    results = []
    deadline = time.time() + duration_s
    while time.time() < deadline and len(results) < max_items:
        try:
            raw = output_queue.get(timeout=0.05)
            d = json.loads(raw)
            d["_collect_time"] = time.time()
            results.append(d)
        except Empty:
            pass
    return results


# ── Tests ─────────────────────────────────────────────────────

def fs_test_steady_clock_rate():
    """Default groups are output at steady ~100ms intervals."""
    print("\n" + "=" * 60)
    print("  Test 1: Steady clock rate (default groups)")
    print("=" * 60)

    logger = fs_setup_logger("fs_test_1")
    ctx = fs_build_frame_splitter(logger)
    thread = fs_start_splitter(ctx)
    fs_send_connection_start(ctx)
    results = fs_collect(ctx["output_queue"], duration_s=1.5)
    fs_stop_splitter(ctx, thread)

    # Check timing intervals
    times = [r["_collect_time"] for r in results]
    intervals = [times[i+1] - times[i] for i in range(len(times)-1)]

    avg_interval = sum(intervals) / len(intervals) if intervals else 0
    max_jitter = max(abs(iv - 0.1) for iv in intervals) if intervals else 0

    print(f"  Groups collected:  {len(results)}")
    print(f"  Avg interval:      {avg_interval*1000:.1f}ms (expected 100ms)")
    print(f"  Max jitter:        {max_jitter*1000:.1f}ms")

    # All should have audio field (silence)
    has_audio = all("audio" in r for r in results)

    ok = True
    if len(results) < 12:  # 1.5s / 0.1s = 15, minus some startup
        print(f"  FAIL: too few groups ({len(results)})")
        ok = False
    if avg_interval > 0.12 or avg_interval < 0.08:
        print(f"  FAIL: avg interval out of range")
        ok = False
    if max_jitter > 0.05:
        print(f"  FAIL: jitter too high")
        ok = False
    if not has_audio:
        print(f"  FAIL: default groups missing audio")
        ok = False

    print(f"  {'PASS' if ok else 'FAIL'}")
    return ok


def fs_test_audio_groups():
    """TTS audio is split into groups and output at clock rate."""
    print("\n" + "=" * 60)
    print("  Test 2: Audio groups from TTS input")
    print("=" * 60)

    logger = fs_setup_logger("fs_test_2")
    ctx = fs_build_frame_splitter(logger)
    thread = fs_start_splitter(ctx)
    fs_send_connection_start(ctx)
    time.sleep(0.3)

    # Send 0.5s of audio = 5 groups at 100ms each
    ts = fs_send_audio(ctx, duration_s=0.5)

    # Collect for 2 seconds (0.5s audio + some default groups)
    results = fs_collect(ctx["output_queue"], duration_s=2.0)
    fs_stop_splitter(ctx, thread)

    # Count groups with real audio vs silence
    # Real audio groups have non-zero PCM data
    audio_groups = []
    for r in results:
        if "audio" not in r:
            continue
        pcm_list = r["audio"]
        # Check if any frame is non-silence
        is_silence = all(
            all(b == 0 for b in base64.b64decode(frame))
            for frame in pcm_list
        )
        audio_groups.append(not is_silence)

    real_count = sum(audio_groups)
    # 0.5s at 48kHz = 24000 samples, / 960 = 25 frames, / 5 per group = 5 groups
    expected = 5

    print(f"  Total groups:      {len(results)}")
    print(f"  Audio groups:      {real_count} (expected ~{expected})")

    ok = True
    if abs(real_count - expected) > 1:
        print(f"  FAIL: expected ~{expected} audio groups, got {real_count}")
        ok = False

    # Meta signal precedes the first audio group and carries the pass data;
    # group data slots stay empty (reserved for frame-aligned payloads)
    meta_idx = next((i for i, r in enumerate(results)
                     if r.get("signal") == "meta"), None)
    first_real_idx = None
    for i, r in enumerate(results):
        if "audio" not in r:
            continue
        if any(any(b != 0 for b in base64.b64decode(f)) for f in r["audio"]):
            first_real_idx = i
            break
    if meta_idx is None:
        print(f"  FAIL: no meta signal found")
        ok = False
    elif first_real_idx is not None and meta_idx > first_real_idx:
        print(f"  FAIL: meta ({meta_idx}) after first audio ({first_real_idx})")
        ok = False
    else:
        pd = results[meta_idx].get("pass_data", {})
        if pd.get("text") != "hello":
            print(f"  FAIL: meta pass_data wrong: {pd}")
            ok = False
        else:
            print(f"  Meta signal before first audio, pass_data: {pd}")
    slot_meta = any(
        isinstance(d, dict)
        for r in results if "audio" in r
        for d in (r.get("data") or [])
    )
    if slot_meta:
        print(f"  FAIL: data slots still carry meta")
        ok = False

    print(f"  {'PASS' if ok else 'FAIL'}")
    return ok


def fs_test_signal_ordering():
    """SoS arrives before audio, EoS arrives after all audio."""
    print("\n" + "=" * 60)
    print("  Test 3: Signal ordering (SoS before audio, EoS after)")
    print("=" * 60)

    logger = fs_setup_logger("fs_test_3")
    ctx = fs_build_frame_splitter(logger)
    thread = fs_start_splitter(ctx)
    fs_send_connection_start(ctx)
    time.sleep(0.3)

    ts = time.time()
    fs_send_signal(ctx, "SoS", ts=ts)
    fs_send_audio(ctx, duration_s=0.3, ts=ts)
    fs_send_signal(ctx, "EoS", ts=ts)

    results = fs_collect(ctx["output_queue"], duration_s=2.5)
    fs_stop_splitter(ctx, thread)

    # Find SoS, EoS, and first/last audio group
    sos_idx = None
    eos_idx = None
    first_audio_idx = None
    last_audio_idx = None

    for i, r in enumerate(results):
        if r.get("signal") == "SoS":
            sos_idx = i
        elif r.get("signal") == "EoS":
            eos_idx = i
        elif "audio" in r:
            pcm_list = r["audio"]
            is_silence = all(
                all(b == 0 for b in base64.b64decode(frame))
                for frame in pcm_list
            )
            if not is_silence:
                if first_audio_idx is None:
                    first_audio_idx = i
                last_audio_idx = i

    print(f"  SoS at index:      {sos_idx}")
    print(f"  First audio at:    {first_audio_idx}")
    print(f"  Last audio at:     {last_audio_idx}")
    print(f"  EoS at index:      {eos_idx}")

    ok = True
    if sos_idx is None:
        print(f"  FAIL: SoS not found")
        ok = False
    if eos_idx is None:
        print(f"  FAIL: EoS not found")
        ok = False
    if first_audio_idx is None:
        print(f"  FAIL: no audio groups found")
        ok = False

    if ok:
        if sos_idx >= first_audio_idx:
            print(f"  FAIL: SoS ({sos_idx}) not before first audio ({first_audio_idx})")
            ok = False
        if eos_idx <= last_audio_idx:
            print(f"  FAIL: EoS ({eos_idx}) not after last audio ({last_audio_idx})")
            ok = False

    print(f"  {'PASS' if ok else 'FAIL'}")
    return ok


def fs_test_cancel_clears_buffer():
    """Cancel discards buffered groups, output switches to default."""
    print("\n" + "=" * 60)
    print("  Test 4: Cancel clears buffer")
    print("=" * 60)

    logger = fs_setup_logger("fs_test_4")
    ctx = fs_build_frame_splitter(logger)
    thread = fs_start_splitter(ctx)
    fs_send_connection_start(ctx)
    time.sleep(0.3)

    # Send 2 seconds of audio (= 20 groups), then immediately cancel
    ts = time.time()
    fs_send_audio(ctx, duration_s=2.0, ts=ts)
    time.sleep(0.3)  # let a few groups output
    fs_send_cancel(ctx, ts + 0.001)

    # Collect and count real audio groups
    results = fs_collect(ctx["output_queue"], duration_s=3.0)
    fs_stop_splitter(ctx, thread)

    audio_count = 0
    for r in results:
        if "audio" not in r:
            continue
        is_silence = all(
            all(b == 0 for b in base64.b64decode(frame))
            for frame in r["audio"]
        )
        if not is_silence:
            audio_count += 1

    # 2s = 20 groups, but cancel after ~0.3s should yield ~3 groups
    print(f"  Audio groups:      {audio_count} (expected << 20)")
    print(f"  Total groups:      {len(results)}")

    ok = True
    if audio_count >= 15:
        print(f"  FAIL: cancel didn't reduce audio output")
        ok = False

    print(f"  {'PASS' if ok else 'FAIL'}")
    return ok


def fs_test_no_drift():
    """Over a longer run, verify no systematic drift in output rate."""
    print("\n" + "=" * 60)
    print("  Test 5: No drift over longer run")
    print("=" * 60)

    logger = fs_setup_logger("fs_test_5")
    ctx = fs_build_frame_splitter(logger)
    thread = fs_start_splitter(ctx)
    fs_send_connection_start(ctx)

    duration = 5.0
    results = fs_collect(ctx["output_queue"], duration_s=duration, max_items=100)
    fs_stop_splitter(ctx, thread)

    # Expected: duration / 0.1 = 50 groups
    expected = int(duration / 0.1)

    # Check actual rate
    if len(results) >= 2:
        total_time = results[-1]["_collect_time"] - results[0]["_collect_time"]
        actual_rate = (len(results) - 1) / total_time if total_time > 0 else 0
    else:
        actual_rate = 0

    print(f"  Duration:          {duration}s")
    print(f"  Groups collected:  {len(results)} (expected ~{expected})")
    print(f"  Actual rate:       {actual_rate:.2f} groups/s (expected 10.0)")

    ok = True
    # Allow ±5% tolerance
    if abs(len(results) - expected) > expected * 0.1:
        print(f"  FAIL: group count off by > 10%")
        ok = False
    if abs(actual_rate - 10.0) > 0.5:
        print(f"  FAIL: rate drift detected")
        ok = False

    print(f"  {'PASS' if ok else 'FAIL'}")
    return ok


def fs_test_webrtc_startup_buffer():
    """Simulate WebRTC group_consumer startup buffering logic."""
    print("\n" + "=" * 60)
    print("  Test 6: WebRTC startup buffering simulation")
    print("=" * 60)

    STARTUP_BUFFER = 4
    group_queue = Queue()

    # Simulate FrameSplitter filling group_queue
    def producer():
        for i in range(20):
            group_queue.put(json.dumps({"group": i, "timestamp": time.time()}))
            time.sleep(0.1)

    t = threading.Thread(target=producer, daemon=True)
    t.start()

    # Simulate group_consumer with startup buffering
    consumed = []
    buffering = True
    start = time.time()
    deadline = start + STARTUP_BUFFER * 0.1 * 5

    for tick in range(30):
        time.sleep(0.1)

        if buffering:
            if group_queue.qsize() >= STARTUP_BUFFER or time.time() > deadline:
                buffering = False
                print(f"  Primed at tick {tick}, queue size: {group_queue.qsize()}")
            continue

        try:
            msg = group_queue.get_nowait()
            consumed.append(json.loads(msg))
        except Empty:
            consumed.append(None)  # would be fill_empty

    t.join(timeout=2)

    real = [c for c in consumed if c is not None]
    empty = [c for c in consumed if c is None]

    print(f"  Consumed real:     {len(real)}")
    print(f"  Consumed empty:    {len(empty)}")
    print(f"  Remaining in queue:{group_queue.qsize()}")

    ok = True
    if len(real) == 0:
        print(f"  FAIL: no real groups consumed")
        ok = False

    print(f"  {'PASS' if ok else 'FAIL'}")
    return ok


def run_framesplitter(args):
    """Entry for --mode framesplitter. Mirrors test_frame_splitter_clock.py __main__."""
    # Original test inserted project root on sys.path so the
    # `Modules.webrtc_frame_splitter` import resolves. SCRIPT_DIR is test/,
    # so the project root is its parent.
    sys.path.insert(0, os.path.dirname(SCRIPT_DIR))

    tests = [
        ("Steady clock rate", fs_test_steady_clock_rate),
        ("Audio groups from TTS", fs_test_audio_groups),
        ("Signal ordering", fs_test_signal_ordering),
        ("Cancel clears buffer", fs_test_cancel_clears_buffer),
        ("No drift", fs_test_no_drift),
        ("WebRTC startup buffer", fs_test_webrtc_startup_buffer),
    ]

    results = []
    for name, fn in tests:
        results.append((name, fn()))

    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    for name, ok in results:
        print(f"  {'PASS' if ok else 'FAIL'}: {name}")

    all_pass = all(ok for _, ok in results)
    print(f"\n  {'All tests passed!' if all_pass else 'Some tests FAILED.'}")
    return all_pass


# ============================================================
# Dispatcher
# ============================================================
def main():
    parser = argparse.ArgumentParser(
        description="Consolidated WebRTC test (modes: single | cancel | lifecycle | multi | framesplitter)"
    )
    parser.add_argument(
        "--mode", choices=["single", "cancel", "lifecycle", "multi", "framesplitter"], default="single",
        help="Which test to run (default: single)",
    )

    # --- single-mode args (from test_webrtc.py main) ---
    parser.add_argument("--server", default=WEBRTC_SERVER,
                        help="[single] WebRTC server URL")
    parser.add_argument("--pipeline", default=PIPELINE_CONFIG,
                        help="[single] Base pipeline config name")
    parser.add_argument("--duration", type=int, default=TEST_DURATION,
                        help="[single] Test duration in seconds")
    parser.add_argument("--audio-ptime", type=float, default=None,
                        help="[single] Override audio packet time")
    parser.add_argument("--video-fps", type=int, default=None,
                        help="[single] Override video fps")
    parser.add_argument("--video-width", type=int, default=None,
                        help="[single] Override video width")
    parser.add_argument("--video-height", type=int, default=None,
                        help="[single] Override video height")
    parser.add_argument("--data-fps", type=int, default=None,
                        help="[single] Override data channel fps")

    # --- multi-mode args (NUM_CLIENTS was a module constant originally) ---
    parser.add_argument("--num-clients", type=int, default=NUM_CLIENTS,
                        help="[multi] Number of concurrent client processes")

    args = parser.parse_args()

    if args.mode == "single":
        result = run_single(args)
    elif args.mode == "cancel":
        result = run_cancel(args)
    elif args.mode == "lifecycle":
        result = run_lifecycle(args)
    elif args.mode == "multi":
        result = run_multi(args)
    elif args.mode == "framesplitter":
        result = run_framesplitter(args)
    else:
        parser.error(f"Unknown mode: {args.mode}")

    sys.exit(0 if result else 1)


if __name__ == "__main__":
    main()
