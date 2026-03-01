#!/usr/bin/env python3
"""
WebRTC client test — connects to a real running server_webrtc + pipeline.

Simulates a real WebRTC client:
  1. Sends test_voice.wav as microphone audio
  2. Sends video: white during speech, black during silence
  3. Sends vad_start/vad_end via DataChannel
  4. Receives audio, video, DataChannel from real pipeline
  5. Records 30s side-by-side MP4:
     Left half + left channel  = sent
     Right half + right channel = received
     Subtitles = DataChannel messages (replacement, cleared on EoS)

Requires:
  - server_webrtc.py running (default: localhost:18082)
  - server_fastapi running (default: localhost:8000)
  - All pipeline services (ASR, LLM, TTS, VectorDB)

Output:
  test/test_webrtc_output.mp4

Usage:
  conda activate yachio
  python test/test_webrtc.py [--server http://localhost:18082]
"""

import argparse
import asyncio
import fractions
import json
import os
import subprocess
import sys
import tempfile
import time
import wave

import av
import numpy as np
from aiortc import (
    MediaStreamTrack,
    RTCPeerConnection,
    RTCSessionDescription,
)
from PIL import Image, ImageDraw, ImageFont

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
VIDEO_TIMESTAMP_INCREMENT = VIDEO_CLOCK_RATE // VIDEO_FPS

TEST_DURATION = 45  # seconds
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_WAV = os.path.join(SCRIPT_DIR, "test_voice.wav")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "tmp")
OUTPUT_MP4 = os.path.join(OUTPUT_DIR, "test_webrtc_output.mp4")

MAIN_SERVER = "http://localhost:8000"
WEBRTC_SERVER = "http://localhost:18082"
CLIENT_ID = "test_webrtc_client"
PIPELINE_CONFIG = "unity_chan_webrtc"


# ============================================================
# Load test audio
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
# Client-side tracks
# ============================================================
class TestAudioTrack(MediaStreamTrack):
    """Sends test_voice.wav, then silence. Records all sent PCM."""
    kind = "audio"

    def __init__(self, audio_frames):
        super().__init__()
        self._frames = audio_frames
        self._index = 0
        self._timestamp = 0
        self._start = None
        self.finished_speech = False
        self.recorded_pcm = []

    async def recv(self):
        if self._start is None:
            self._start = time.time()
        wait = self._start + (self._timestamp / SAMPLE_RATE) - time.time()
        if wait > 0:
            await asyncio.sleep(wait)

        if self._index < len(self._frames):
            pcm = self._frames[self._index]
            self._index += 1
        else:
            pcm = np.zeros(AUDIO_SAMPLES, dtype=np.int16)
            if not self.finished_speech:
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

        active = not self._audio.finished_speech
        self.recorded_frames.append(
            (time.time() - self._start, self._make_rgb(active))
        )

        frame = self._make_yuv_frame(active)
        frame.pts = self._timestamp
        frame.time_base = VIDEO_TIME_BASE
        self._timestamp += VIDEO_TIMESTAMP_INCREMENT
        return frame


# ============================================================
# MP4 Recording
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

        # Video area
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
# Test
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

    # --- Send speech ---
    test_start = time.time()
    client_dc.send(json.dumps({"signal": "vad_start"}))
    print(f"[Client] vad_start → sending {speech_duration:.1f}s of speech...")

    while not send_audio.finished_speech:
        await asyncio.sleep(0.1)
    await asyncio.sleep(0.5)

    client_dc.send(json.dumps({"signal": "vad_end"}))
    print("[Client] vad_end → waiting for pipeline response...")

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


def main():
    result = asyncio.run(run_test())
    sys.exit(0 if result else 1)


if __name__ == "__main__":
    main()
