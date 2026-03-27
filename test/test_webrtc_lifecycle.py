"""
Integration test for WebRTC connection lifecycle (connection_start/stop).
Tests the full flow: client → WebRTC server → pipeline → FrameSplitter.

Requires:
  - server_fastapi running on localhost:8910
  - server_webrtc running on localhost:15168
  - No external model services needed (uses FrameSplitter-only pipeline)
"""
import asyncio
import fractions
import json
import sys
import time

import av
import numpy as np
import requests
from aiortc import (
    MediaStreamTrack,
    RTCPeerConnection,
    RTCSessionDescription,
)

MAIN_SERVER = "http://localhost:8910"
WEBRTC_SERVER = "http://localhost:15168"
CLIENT_ID = "test_lifecycle"
PIPELINE_CONFIG = "test_frame_splitter"

SAMPLE_RATE = 48000
AUDIO_SAMPLES = 960
AUDIO_TIME_BASE = fractions.Fraction(1, SAMPLE_RATE)
VIDEO_WIDTH, VIDEO_HEIGHT = 320, 240
VIDEO_FPS = 30
VIDEO_CLOCK_RATE = 90000
VIDEO_TIME_BASE = fractions.Fraction(1, VIDEO_CLOCK_RATE)
VIDEO_TIMESTAMP_INCREMENT = VIDEO_CLOCK_RATE // VIDEO_FPS


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


async def test_lifecycle():
    """Test connection_start/stop flow through WebRTC server."""
    print("=" * 60)
    print("  WebRTC Lifecycle Integration Test")
    print("=" * 60)

    # 1. Register + init pipeline
    print("\n  [1] Register + init pipeline...")
    r = requests.post(f"{MAIN_SERVER}/register/", json={"client_id": CLIENT_ID})
    assert r.json()["status"] in ("registered", "already registered"), r.json()
    r = requests.post(
        f"{MAIN_SERVER}/init_pipeline/{CLIENT_ID}",
        json={"config": PIPELINE_CONFIG, "force": True},
    )
    assert r.json()["status"] == "initialized", r.json()
    print(f"    Pipeline initialized with {PIPELINE_CONFIG}")

    # Wait for FrameSplitter to init
    await asyncio.sleep(1)

    # 2. Check pipeline log: FrameSplitter should be paused
    log = requests.get(f"{MAIN_SERVER}/logs/{CLIENT_ID}").json().get("log_content", "")
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
        f"{WEBRTC_SERVER}/offer/{CLIENT_ID}",
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

    log = requests.get(f"{MAIN_SERVER}/logs/{CLIENT_ID}").json().get("log_content", "")
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

    log = requests.get(f"{MAIN_SERVER}/logs/{CLIENT_ID}").json().get("log_content", "")
    pipeline_disposed = "disposed" in log.lower()
    print(f"    Pipeline disposed: {'OK' if pipeline_disposed else 'FAIL'}")

    # 7. Verify pipeline is no longer initialized (can re-init without force)
    r = requests.post(
        f"{MAIN_SERVER}/init_pipeline/{CLIENT_ID}",
        json={"config": PIPELINE_CONFIG},
    )
    can_reinit = r.json().get("status") == "initialized"
    print(f"    Re-init without force: {'OK' if can_reinit else 'FAIL'}")

    # 8. Cleanup
    print("\n  [4] Cleanup...")
    requests.post(f"{MAIN_SERVER}/unregister/", json={"client_id": CLIENT_ID})
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


if __name__ == "__main__":
    ok = asyncio.run(test_lifecycle())
    sys.exit(0 if ok else 1)
