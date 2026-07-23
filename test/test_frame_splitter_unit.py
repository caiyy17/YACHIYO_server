"""Offline unit tests for the WebRTC FrameSplitterStep.

The tests instantiate the module with an inline configuration and local queues;
they do not load a pipeline config or contact either server.
"""

import base64
import io
import json
import logging
import os
import sys
import threading
import time
import unittest
import wave
from queue import Empty, Queue

import numpy as np

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# ============================================================
# Mode: framesplitter  (isolated FrameSplitter unit tests)
#
# Standalone clock-driven FrameSplitterStep test. Drives a
# FrameSplitterStep in-process via queues (no server). Names are
# prefixed with `fs_` to avoid colliding with the WebRTC helpers above
# (e.g. the `send_audio` local used in single/multi modes). Logic is
# kept local to this mode.
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
        "pass_signals": [
            {"source": "SoS", "target": "SoS"},
            {"source": "EoS", "target": "EoS"},
        ],
        "emit_signals": [
            {"source": "meta", "target": "meta"},
        ],
        "catch_events": [
            {"source": "connection_start", "target": "connection_start"},
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
        config=config,
    )

    return {
        "instance": inst,
        "input_queue": input_queue,
        "output_queue": output_queue,
        "cancel_queue": cancel_queue,
        "send_queue": send_queue,
    }


def fs_start_splitter(ctx):
    t = threading.Thread(target=ctx["instance"].run, daemon=True)
    t.start()
    return t


def fs_stop_splitter(ctx, thread, timeout=3):
    ctx["cancel_queue"].put(json.dumps({"signal": "cancel", "timestamp": float("inf")}))
    ctx["cancel_queue"].put(json.dumps({"signal": "kill"}))
    thread.join(timeout=timeout)
    return thread.is_alive()


def fs_send_connection_start(ctx, ts=None):
    # connection_start is a control-plane event: broadcast into the node's
    # control queue (in production the EventHandler does this)
    if ts is None:
        ts = time.time()
    msg = json.dumps({
        "signal": "connection_start",
        "timestamp": ts,
        "source": 0,
    })
    ctx["cancel_queue"].put(msg)


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
    """Default groups hold a steady ~100ms clock without long-run drift."""
    print("\n" + "=" * 60)
    print("  Test 1: Steady clock rate and no drift (default groups)")
    print("=" * 60)

    logger = fs_setup_logger("fs_test_1")
    ctx = fs_build_frame_splitter(logger)
    thread = fs_start_splitter(ctx)
    fs_send_connection_start(ctx)
    duration = 5.0
    results = fs_collect(
        ctx["output_queue"], duration_s=duration, max_items=100
    )
    teardown_failed = fs_stop_splitter(ctx, thread)

    # Check timing intervals
    times = [r["_collect_time"] for r in results]
    intervals = [times[i+1] - times[i] for i in range(len(times)-1)]

    avg_interval = sum(intervals) / len(intervals) if intervals else 0
    max_jitter = max(abs(iv - 0.1) for iv in intervals) if intervals else 0
    elapsed = times[-1] - times[0] if len(times) >= 2 else 0
    actual_rate = (len(times) - 1) / elapsed if elapsed > 0 else 0
    expected = int(duration / 0.1)

    print(f"  Duration:          {duration}s")
    print(f"  Groups collected:  {len(results)} (expected ~{expected})")
    print(f"  Long-term rate:    {actual_rate:.2f} groups/s (expected 10.0)")
    print(f"  Avg interval:      {avg_interval*1000:.1f}ms (expected 100ms)")
    print(f"  Max jitter:        {max_jitter*1000:.1f}ms")

    # All should have audio field (silence)
    has_audio = bool(results) and all("audio" in r for r in results)

    ok = not teardown_failed
    if teardown_failed:
        print("  FAIL: FrameSplitter thread still alive")
    if abs(len(results) - expected) > expected * 0.1:
        print("  FAIL: group count off by > 10%")
        ok = False
    if abs(actual_rate - 10.0) > 0.5:
        print("  FAIL: long-term rate drift detected")
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
    teardown_failed = fs_stop_splitter(ctx, thread)

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

    ok = not teardown_failed
    if teardown_failed:
        print("  FAIL: FrameSplitter thread still alive")
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
    teardown_failed = fs_stop_splitter(ctx, thread)

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

    ok = not teardown_failed
    if teardown_failed:
        print("  FAIL: FrameSplitter thread still alive")
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
    teardown_failed = fs_stop_splitter(ctx, thread)

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

    ok = not teardown_failed
    if teardown_failed:
        print("  FAIL: FrameSplitter thread still alive")
    if audio_count >= 15:
        print(f"  FAIL: cancel didn't reduce audio output")
        ok = False

    print(f"  {'PASS' if ok else 'FAIL'}")
    return ok


def run_framesplitter(args):
    """Entry for isolated FrameSplitter timing and packing tests."""
    # SCRIPT_DIR is test/, so add its parent for the Modules import.
    sys.path.insert(0, os.path.dirname(SCRIPT_DIR))

    tests = [
        ("Steady clock rate and no drift", fs_test_steady_clock_rate),
        ("Audio groups from TTS", fs_test_audio_groups),
        ("Signal ordering", fs_test_signal_ordering),
        ("Cancel clears buffer", fs_test_cancel_clears_buffer),
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



class FrameSplitterUnitTest(unittest.TestCase):
    def test_steady_clock_rate(self):
        self.assertTrue(fs_test_steady_clock_rate())

    def test_audio_groups(self):
        self.assertTrue(fs_test_audio_groups())

    def test_signal_ordering(self):
        self.assertTrue(fs_test_signal_ordering())

    def test_cancel_clears_buffer(self):
        self.assertTrue(fs_test_cancel_clears_buffer())


if __name__ == "__main__":
    unittest.main()
