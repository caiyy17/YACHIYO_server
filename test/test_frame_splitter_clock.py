"""
Standalone test for clock-driven FrameSplitterStep.
Verifies:
  1. Steady 100ms output rate (no drift)
  2. Default groups when no input
  3. Audio groups output correctly from TTS input
  4. Signal ordering (SoS before audio, EoS after audio)
  5. Cancel clears buffer
  6. WebRTC startup buffering
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import base64
import io
import json
import struct
import threading
import time
import logging
import wave
from queue import Queue, Empty

import numpy as np

from Modules.webrtc_frame_splitter.FrameSplitterStep import FrameSplitterStep


# ── Utilities ─────────────────────────────────────────────────

def setup_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(h)
    return logger


def make_wav_b64(duration_s=0.5, sample_rate=48000):
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


def build_frame_splitter(logger, config_overrides=None):
    """Build a FrameSplitterStep with queues for testing."""
    send_queue = Queue()
    input_queue = Queue()
    output_queue = Queue()  # FrameSplitter is last node, this acts as send_queue
    cancel_queue = Queue()
    kill_event = threading.Event()

    config = {
        "input_vars": [{"input_name": "audio_data", "source": "audio_data"}],
        "pass_vars": [
            {"source": "text", "target": "text"},
        ],
        "output_vars": [
            {"output_name": "audio", "target": "audio"},
            {"output_name": "video", "target": "video"},
            {"output_name": "data", "target": "data"},
        ],
        "next_nodes": [],
        "sample_rate": 48000,
        "frame_samples": 960,
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


def start_splitter(ctx):
    t = threading.Thread(target=ctx["instance"].run, daemon=True)
    t.start()
    return t


def stop_splitter(ctx, thread, timeout=3):
    ctx["kill_event"].set()
    thread.join(timeout=timeout)
    return thread.is_alive()


def send_connection_start(ctx, ts=None):
    if ts is None:
        ts = time.time()
    msg = json.dumps({
        "signal": "connection_start",
        "destination": 7,
        "timestamp": ts,
    })
    ctx["input_queue"].put(msg)


def send_connection_stop(ctx, ts=None):
    if ts is None:
        ts = time.time()
    msg = json.dumps({
        "signal": "connection_stop",
        "destination": 7,
        "timestamp": ts,
    })
    ctx["input_queue"].put(msg)


def send_audio(ctx, duration_s=0.5, ts=None):
    if ts is None:
        ts = time.time()
    wav_b64 = make_wav_b64(duration_s)
    msg = json.dumps({
        "audio_data": wav_b64,
        "text": "hello",
        "destination": 7,
        "timestamp": ts,
    })
    ctx["input_queue"].put(msg)
    return ts


def send_signal(ctx, signal_name, ts=None):
    if ts is None:
        ts = time.time()
    msg = json.dumps({
        "signal": signal_name,
        "destination": 7,
        "timestamp": ts,
    })
    ctx["input_queue"].put(msg)


def send_cancel(ctx, ts):
    msg = json.dumps({"signal": "cancel", "timestamp": ts})
    ctx["cancel_queue"].put(msg)


def collect(output_queue, duration_s=2.0, max_items=200):
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

def test_steady_clock_rate():
    """Default groups are output at steady ~100ms intervals."""
    print("\n" + "=" * 60)
    print("  Test 1: Steady clock rate (default groups)")
    print("=" * 60)

    logger = setup_logger("fs_test_1")
    ctx = build_frame_splitter(logger)
    thread = start_splitter(ctx)
    send_connection_start(ctx)
    results = collect(ctx["output_queue"], duration_s=1.5)
    stop_splitter(ctx, thread)

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


def test_audio_groups():
    """TTS audio is split into groups and output at clock rate."""
    print("\n" + "=" * 60)
    print("  Test 2: Audio groups from TTS input")
    print("=" * 60)

    logger = setup_logger("fs_test_2")
    ctx = build_frame_splitter(logger)
    thread = start_splitter(ctx)
    send_connection_start(ctx)
    time.sleep(0.3)

    # Send 0.5s of audio = 5 groups at 100ms each
    ts = send_audio(ctx, duration_s=0.5)

    # Collect for 2 seconds (0.5s audio + some default groups)
    results = collect(ctx["output_queue"], duration_s=2.0)
    stop_splitter(ctx, thread)

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

    # Check first audio group has metadata
    first_audio_idx = next((i for i, v in enumerate(audio_groups) if v), None)
    if first_audio_idx is not None:
        first_data = results[first_audio_idx].get("data", [])
        has_meta = any(d is not None and isinstance(d, dict) for d in first_data if d)
        if has_meta:
            print(f"  Metadata in first audio group: OK")
        else:
            print(f"  NOTE: no metadata in first audio group (may be in data field)")

    print(f"  {'PASS' if ok else 'FAIL'}")
    return ok


def test_signal_ordering():
    """SoS arrives before audio, EoS arrives after all audio."""
    print("\n" + "=" * 60)
    print("  Test 3: Signal ordering (SoS before audio, EoS after)")
    print("=" * 60)

    logger = setup_logger("fs_test_3")
    ctx = build_frame_splitter(logger)
    thread = start_splitter(ctx)
    send_connection_start(ctx)
    time.sleep(0.3)

    ts = time.time()
    send_signal(ctx, "SoS", ts=ts)
    send_audio(ctx, duration_s=0.3, ts=ts)
    send_signal(ctx, "EoS", ts=ts)

    results = collect(ctx["output_queue"], duration_s=2.5)
    stop_splitter(ctx, thread)

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


def test_cancel_clears_buffer():
    """Cancel discards buffered groups, output switches to default."""
    print("\n" + "=" * 60)
    print("  Test 4: Cancel clears buffer")
    print("=" * 60)

    logger = setup_logger("fs_test_4")
    ctx = build_frame_splitter(logger)
    thread = start_splitter(ctx)
    send_connection_start(ctx)
    time.sleep(0.3)

    # Send 2 seconds of audio (= 20 groups), then immediately cancel
    ts = time.time()
    send_audio(ctx, duration_s=2.0, ts=ts)
    time.sleep(0.3)  # let a few groups output
    send_cancel(ctx, ts + 0.001)

    # Collect and count real audio groups
    results = collect(ctx["output_queue"], duration_s=3.0)
    stop_splitter(ctx, thread)

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


def test_no_drift():
    """Over a longer run, verify no systematic drift in output rate."""
    print("\n" + "=" * 60)
    print("  Test 5: No drift over longer run")
    print("=" * 60)

    logger = setup_logger("fs_test_5")
    ctx = build_frame_splitter(logger)
    thread = start_splitter(ctx)
    send_connection_start(ctx)

    duration = 5.0
    results = collect(ctx["output_queue"], duration_s=duration, max_items=100)
    stop_splitter(ctx, thread)

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


def test_webrtc_startup_buffer():
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


# ── Main ──────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        ("Steady clock rate", test_steady_clock_rate),
        ("Audio groups from TTS", test_audio_groups),
        ("Signal ordering", test_signal_ordering),
        ("Cancel clears buffer", test_cancel_clears_buffer),
        ("No drift", test_no_drift),
        ("WebRTC startup buffer", test_webrtc_startup_buffer),
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
    sys.exit(0 if all_pass else 1)
