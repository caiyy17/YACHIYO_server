"""Deterministic offline unit tests for VAD segmentation.

All cases use inline configurations, generated WAV chunks, and fake VAD
callers. No pipeline config or network service is accessed.
"""

import array
import base64
import io
import json
import logging
import os
import sys
import unittest
import wave
from queue import Empty, Queue
from unittest.mock import patch

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def setup_logger(name="vad_test"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        )
        logger.addHandler(handler)
    return logger


# ══════════════════════════════════════════════════════════════════════════
# MODE: vad  (deterministic signed-offset segmentation tests)
# ═══════════════════════════════════════════════════════════════════════

VAD_SAMPLE_RATE = 1000
VAD_BLOCK_SAMPLES = 100
VAD_START_TIMESTAMP = 100.0


def vad_require(condition, message):
    if not condition:
        raise AssertionError(message)


def vad_config(**overrides):
    config = {
        "input_vars": [{"source": "audio_data", "target": "audio_data"}],
        "pass_vars": [],
        "output_vars": [{"source": "audio_file", "target": "audio_file"}],
        "catch_signals": [
            {"source": "recording_start", "target": "recording_start"},
            {"source": "recording_end", "target": "recording_end"},
        ],
        "pass_signals": [],
        "emit_signals": [
            {"source": "vad_start", "target": "vad_start"},
            {"source": "vad_end", "target": "vad_end"},
        ],
        "next_nodes": [-1],
        "sample_rate": VAD_SAMPLE_RATE,
        "ring_seconds": 2,
        "manual_start_offset_ms": 0,
        "manual_end_offset_ms": 0,
        "stream": False,
        "stream_chunk_ms": 100,
    }
    config.update(overrides)
    return config


def vad_new_step(**overrides):
    from Modules.vad_base.VADStep import VADStep

    output_queue = Queue()
    step = VADStep(
        1, "vad_test", setup_logger("vad_test"), Queue(), Queue(),
        output_queue, Queue(), vad_config(**overrides),
    )
    vad_require(step.init_error is None, f"VAD init failed: {step.init_error}")
    return step, output_queue


def vad_wav_block(value, samples=VAD_BLOCK_SAMPLES):
    pcm = array.array("h", [value]) * samples
    if sys.byteorder != "little":
        pcm.byteswap()
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(VAD_SAMPLE_RATE)
        wav_file.writeframes(pcm.tobytes())
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def vad_feed(step, value, timestamp):
    step.span_process({
        "audio_data": vad_wav_block(value),
        "timestamp": timestamp,
    })


def vad_start(step, timestamp=VAD_START_TIMESTAMP):
    step.span_process({
        "signal": "recording_start",
        "timestamp": timestamp,
    })


def vad_end(step, timestamp):
    step.span_process({
        "signal": "recording_end",
        "timestamp": timestamp,
    })


def vad_drain(output_queue):
    messages = []
    while True:
        try:
            messages.append(json.loads(output_queue.get_nowait()))
        except Empty:
            return messages


def vad_kinds(messages):
    return [message.get("signal") or (
        "audio_file" if "audio_file" in message else "unknown"
    ) for message in messages]


def vad_decode(message):
    vad_require("audio_file" in message, f"not an audio message: {message}")
    try:
        raw = base64.b64decode(message["audio_file"], validate=True)
        with wave.open(io.BytesIO(raw), "rb") as wav_file:
            params = (
                wav_file.getnchannels(), wav_file.getsampwidth(),
                wav_file.getframerate(), wav_file.getcomptype(),
            )
            frames = wav_file.getnframes()
            pcm = array.array("h")
            pcm.frombytes(wav_file.readframes(frames))
    except Exception as error:
        raise AssertionError(f"invalid WAV output: {error}") from error
    if sys.byteorder != "little":
        pcm.byteswap()
    vad_require(
        params == (1, 2, VAD_SAMPLE_RATE, "NONE"),
        f"unexpected WAV parameters: {params}",
    )
    return list(pcm)


def vad_check_stamps(messages, expected=VAD_START_TIMESTAMP):
    for message in messages:
        vad_require(
            message.get("timestamp") == expected,
            f"offset changed the turn timestamp: {message}",
        )


def vad_check_audio(message, expected):
    actual = vad_decode(message)
    vad_require(
        actual == expected,
        f"PCM mismatch: got {len(actual)} samples, expected {len(expected)}",
    )


def vad_test_validator():
    from Modules.vad_server.ServerVADStep import ServerVADStep

    baseline = vad_config(
        auto_detect=False,
        start_offset_ms=0,
        end_offset_ms=0,
    )
    keys = (
        "manual_start_offset_ms", "manual_end_offset_ms",
        "start_offset_ms", "end_offset_ms",
    )
    for key in keys:
        for value in (-150, 0, 150):
            config = dict(baseline)
            config[key] = value
            errors = ServerVADStep.validate_config(config)
            vad_require(not errors, f"{key}={value} rejected: {errors}")
        for value in (True, "150"):
            config = dict(baseline)
            config[key] = value
            errors = ServerVADStep.validate_config(config)
            vad_require(
                any(key in error for error in errors),
                f"{key}={value!r} was not rejected: {errors}",
            )
    config = dict(baseline)
    config["exact_chunk"] = "true"
    errors = ServerVADStep.validate_config(config)
    vad_require(
        any("exact_chunk" in error for error in errors),
        f"non-boolean exact_chunk was not rejected: {errors}",
    )


def vad_test_start_lookback():
    step, output = vad_new_step(manual_start_offset_ms=-200)
    for value, end_ts in ((1, 99.8), (2, 99.9), (3, 100.0)):
        vad_feed(step, value, end_ts)
    vad_require(not vad_drain(output), "audio before start produced output")
    vad_start(step)
    start_messages = vad_drain(output)
    vad_require(vad_kinds(start_messages) == ["vad_start"],
                f"start lookback was not immediate: {vad_kinds(start_messages)}")
    vad_feed(step, 4, 100.1)
    vad_end(step, 100.1)
    end_messages = vad_drain(output)
    vad_require(vad_kinds(end_messages) == ["vad_end", "audio_file"],
                f"unexpected end output: {vad_kinds(end_messages)}")
    vad_check_audio(end_messages[1], [2] * 100 + [3] * 100 + [4] * 100)
    vad_check_stamps(start_messages + end_messages)


def vad_test_start_delay():
    step, output = vad_new_step(manual_start_offset_ms=150)
    vad_feed(step, 0, 100.0)
    vad_start(step)
    vad_require(not vad_drain(output), "delayed start was forwarded immediately")
    vad_feed(step, 1, 100.1)
    vad_require(not vad_drain(output), "delayed start fired before 150 ms")
    vad_feed(step, 2, 100.2)
    start_messages = vad_drain(output)
    vad_require(vad_kinds(start_messages) == ["vad_start"],
                f"delayed start did not fire once: {vad_kinds(start_messages)}")
    vad_feed(step, 3, 100.3)
    vad_end(step, 100.3)
    end_messages = vad_drain(output)
    vad_require(vad_kinds(end_messages) == ["vad_end", "audio_file"],
                f"unexpected end output: {vad_kinds(end_messages)}")
    vad_check_audio(end_messages[1], [2] * 50 + [3] * 100)
    vad_check_stamps(start_messages + end_messages)


def vad_test_positive_end():
    step, output = vad_new_step(manual_end_offset_ms=150)
    vad_feed(step, 0, 100.0)
    vad_start(step)
    start_messages = vad_drain(output)
    for value, end_ts in ((1, 100.1), (2, 100.2)):
        vad_feed(step, value, end_ts)
    vad_end(step, 100.2)
    vad_require(not vad_drain(output), "positive end finalized immediately")
    vad_feed(step, 3, 100.3)
    vad_require(not vad_drain(output), "positive end fired before 150 ms")
    vad_feed(step, 4, 100.4)
    end_messages = vad_drain(output)
    vad_require(vad_kinds(end_messages) == ["vad_end", "audio_file"],
                f"unexpected delayed end output: {vad_kinds(end_messages)}")
    expected = [1] * 100 + [2] * 100 + [3] * 100 + [4] * 50
    vad_check_audio(end_messages[1], expected)
    vad_check_stamps(start_messages + end_messages)


def vad_test_negative_end():
    step, output = vad_new_step(manual_end_offset_ms=-150)
    vad_feed(step, 0, 100.0)
    vad_start(step)
    start_messages = vad_drain(output)
    for value, end_ts in ((1, 100.1), (2, 100.2), (3, 100.3)):
        vad_feed(step, value, end_ts)
    vad_require(not vad_drain(output), "non-stream VAD emitted audio before end")
    vad_end(step, 100.3)
    end_messages = vad_drain(output)
    vad_require(vad_kinds(end_messages) == ["vad_end", "audio_file"],
                f"negative end was not immediate: {vad_kinds(end_messages)}")
    vad_check_audio(end_messages[1], [1] * 100 + [2] * 50)
    vad_check_stamps(start_messages + end_messages)


def vad_test_collapsed_nonstream():
    for name, end_offset in (("negative", -200), ("zero", 200)):
        step, output = vad_new_step(
            manual_start_offset_ms=300,
            manual_end_offset_ms=end_offset,
        )
        vad_feed(step, 0, 100.0)
        vad_start(step)
        vad_feed(step, 1, 100.1)
        vad_require(not vad_drain(output),
                    f"{name} collapsed case emitted before end")
        vad_end(step, 100.1)
        messages = vad_drain(output)
        vad_require(
            vad_kinds(messages) == ["vad_start", "vad_end", "audio_file"],
            f"{name} collapsed order: {vad_kinds(messages)}",
        )
        vad_check_audio(messages[2], [])
        vad_check_stamps(messages)


def vad_test_stream_holdback_and_collapse():
    step, output = vad_new_step(
        manual_end_offset_ms=-150, stream=True, stream_chunk_ms=100,
    )
    vad_feed(step, 0, 100.0)
    vad_start(step)
    messages = vad_drain(output)
    vad_require(vad_kinds(messages) == ["vad_start"],
                f"unexpected stream start: {vad_kinds(messages)}")
    vad_feed(step, 1, 100.1)
    vad_feed(step, 2, 100.2)
    vad_require(not vad_drain(output),
                "stream emitted samples still inside the -150 ms holdback")
    vad_feed(step, 3, 100.3)
    safe = vad_drain(output)
    vad_require(vad_kinds(safe) == ["audio_file"],
                f"stream did not release its safe prefix: {vad_kinds(safe)}")
    vad_check_audio(safe[0], [1] * 100)
    vad_end(step, 100.3)
    final = vad_drain(output)
    vad_require(vad_kinds(final) == ["audio_file", "vad_end"],
                f"unexpected stream final order: {vad_kinds(final)}")
    vad_check_audio(final[0], [2] * 50 + [0] * 50)
    vad_check_stamps(messages + safe + final)

    collapsed, collapsed_output = vad_new_step(
        manual_start_offset_ms=300,
        manual_end_offset_ms=-200,
        stream=True,
        stream_chunk_ms=100,
    )
    vad_feed(collapsed, 0, 100.0)
    vad_start(collapsed)
    vad_feed(collapsed, 1, 100.1)
    vad_require(not vad_drain(collapsed_output),
                "collapsed stream emitted before end")
    vad_end(collapsed, 100.1)
    collapsed_messages = vad_drain(collapsed_output)
    vad_require(
        vad_kinds(collapsed_messages) == ["vad_start", "vad_end"],
        f"collapsed stream emitted WAV or wrong order: "
        f"{vad_kinds(collapsed_messages)}",
    )
    vad_check_stamps(collapsed_messages)

    natural, natural_output = vad_new_step(
        stream=True, stream_chunk_ms=100, exact_chunk=False,
    )
    vad_feed(natural, 0, 100.0)
    vad_start(natural)
    natural_started = vad_drain(natural_output)
    vad_feed(natural, 1, 100.1)
    full = vad_drain(natural_output)
    natural.span_process({
        "audio_data": vad_wav_block(2, samples=50),
        "timestamp": 100.15,
    })
    vad_end(natural, 100.15)
    natural_finished = vad_drain(natural_output)
    vad_require(vad_kinds(full) == ["audio_file"],
                f"natural stream full chunk: {vad_kinds(full)}")
    vad_require(vad_kinds(natural_finished) == ["audio_file", "vad_end"],
                f"natural stream tail: {vad_kinds(natural_finished)}")
    vad_check_audio(full[0], [1] * 100)
    vad_check_audio(natural_finished[0], [2] * 50)
    vad_check_stamps(natural_started + full + natural_finished)


def vad_test_manual_timestamp_boundaries():
    # The start signal lies inside an already-ingested chunk. Its own stamp,
    # not the latest sample position, must select the final 50 samples.
    step, output = vad_new_step()
    vad_feed(step, 1, 100.0)
    vad_feed(step, 2, 100.1)
    vad_start(step, 100.05)
    start_messages = vad_drain(output)
    vad_require(vad_kinds(start_messages) == ["vad_start"],
                f"historical in-chunk start: {vad_kinds(start_messages)}")
    vad_end(step, 100.1)
    end_messages = vad_drain(output)
    vad_require(vad_kinds(end_messages) == ["vad_end", "audio_file"],
                f"historical in-chunk end: {vad_kinds(end_messages)}")
    vad_check_audio(end_messages[1], [2] * 50)
    vad_check_stamps(start_messages + end_messages, 100.05)

    # Offset targets exactly on chunk boundaries: the start excludes the
    # chunk ending there, while the end includes the chunk ending there.
    exact, exact_output = vad_new_step(
        manual_start_offset_ms=-50,
        manual_end_offset_ms=-50,
    )
    vad_feed(exact, 1, 99.9)
    vad_feed(exact, 2, 100.0)
    vad_start(exact, 100.05)       # target = 100.0
    vad_feed(exact, 3, 100.1)
    vad_end(exact, 100.15)         # target = 100.1
    exact_messages = vad_drain(exact_output)
    vad_require(
        vad_kinds(exact_messages) == ["vad_start", "vad_end", "audio_file"],
        f"exact timestamp boundaries: {vad_kinds(exact_messages)}",
    )
    vad_check_audio(exact_messages[2], [3] * 100)
    vad_check_stamps(exact_messages, 100.05)


def vad_test_manual_future_timestamp_boundaries():
    step, output = vad_new_step()
    vad_feed(step, 0, 100.0)
    vad_start(step, 100.05)
    vad_require(not vad_drain(output), "future in-chunk start fired early")
    vad_feed(step, 1, 100.1)
    start_messages = vad_drain(output)
    vad_require(vad_kinds(start_messages) == ["vad_start"],
                f"future in-chunk start: {vad_kinds(start_messages)}")
    vad_feed(step, 2, 100.2)
    vad_end(step, 100.25)
    vad_require(not vad_drain(output), "future in-chunk end fired early")
    vad_feed(step, 3, 100.3)
    end_messages = vad_drain(output)
    vad_require(vad_kinds(end_messages) == ["vad_end", "audio_file"],
                f"future in-chunk end: {vad_kinds(end_messages)}")
    vad_check_audio(
        end_messages[1], [1] * 50 + [2] * 100 + [3] * 50)
    vad_check_stamps(start_messages + end_messages, 100.05)


def vad_test_multi_chunk_timestamp_crop():
    step, output = vad_new_step(
        manual_start_offset_ms=-350,
        manual_end_offset_ms=-250,
    )
    for value, end_ts in (
            (1, 99.6), (2, 99.7), (3, 99.8), (4, 99.9), (5, 100.0)):
        vad_feed(step, value, end_ts)

    # target start = 100.0 - 350 ms = 99.65, halfway through chunk 2.
    vad_start(step, 100.0)
    started = vad_drain(output)
    vad_require(vad_kinds(started) == ["vad_start"],
                f"multi-chunk start: {vad_kinds(started)}")

    for value, end_ts in ((6, 100.1), (7, 100.2), (8, 100.3)):
        vad_feed(step, value, end_ts)
    # target end = 100.3 - 250 ms = 100.05, halfway through chunk 6.
    vad_end(step, 100.3)
    finished = vad_drain(output)
    vad_require(vad_kinds(finished) == ["vad_end", "audio_file"],
                f"multi-chunk end: {vad_kinds(finished)}")
    expected = (
        [2] * 50 + [3] * 100 + [4] * 100 + [5] * 100 + [6] * 50
    )
    vad_check_audio(finished[1], expected)
    vad_check_stamps(started + finished, 100.0)


def vad_test_multi_chunk_future_wait():
    step, output = vad_new_step(
        manual_start_offset_ms=350,
        manual_end_offset_ms=350,
    )
    vad_feed(step, 0, 100.0)
    vad_start(step, 100.0)  # target start = 100.35
    vad_require(not vad_drain(output), "350 ms start fired immediately")

    for value in (1, 2, 3):
        vad_feed(step, value, 100.0 + value * 0.1)
        vad_require(not vad_drain(output),
                    f"350 ms start fired on early chunk {value}")
    vad_feed(step, 4, 100.4)
    started = vad_drain(output)
    vad_require(vad_kinds(started) == ["vad_start"],
                f"350 ms start resolution: {vad_kinds(started)}")

    vad_feed(step, 5, 100.5)
    vad_end(step, 100.5)  # target end = 100.85
    vad_require(not vad_drain(output), "350 ms end finalized immediately")
    for value in (6, 7, 8):
        vad_feed(step, value, 100.0 + value * 0.1)
        vad_require(not vad_drain(output),
                    f"350 ms end fired on early chunk {value}")
    vad_feed(step, 9, 100.9)
    finished = vad_drain(output)
    vad_require(vad_kinds(finished) == ["vad_end", "audio_file"],
                f"350 ms end resolution: {vad_kinds(finished)}")
    expected = (
        [4] * 50 + [5] * 100 + [6] * 100 + [7] * 100
        + [8] * 100 + [9] * 50
    )
    vad_check_audio(finished[1], expected)
    vad_check_stamps(started + finished, 100.0)


def vad_test_exact_empty_duration():
    step, output = vad_new_step()
    vad_feed(step, 0, 100.0)
    vad_start(step, 100.0)
    started = vad_drain(output)
    vad_require(vad_kinds(started) == ["vad_start"],
                f"empty non-stream start: {vad_kinds(started)}")
    vad_end(step, 100.0)
    finished = vad_drain(output)
    vad_require(vad_kinds(finished) == ["vad_end", "audio_file"],
                f"empty non-stream end: {vad_kinds(finished)}")
    vad_check_audio(finished[1], [])
    vad_check_stamps(started + finished, 100.0)

    stream, stream_output = vad_new_step(stream=True, stream_chunk_ms=100)
    vad_feed(stream, 0, 100.0)
    vad_start(stream, 100.0)
    stream_started = vad_drain(stream_output)
    vad_end(stream, 100.0)
    stream_finished = vad_drain(stream_output)
    vad_require(vad_kinds(stream_started + stream_finished) == [
        "vad_start", "vad_end",
    ], f"empty stream envelope: "
       f"{vad_kinds(stream_started + stream_finished)}")
    vad_check_stamps(stream_started + stream_finished, 100.0)


def vad_test_stream_timestamp_boundaries():
    step, output = vad_new_step(stream=True, stream_chunk_ms=100)
    vad_feed(step, 0, 100.0)
    vad_start(step, 100.05)
    vad_feed(step, 1, 100.1)
    started = vad_drain(output)
    vad_require(vad_kinds(started) == ["vad_start"],
                f"stream timestamp start: {vad_kinds(started)}")

    vad_feed(step, 2, 100.2)
    first = vad_drain(output)
    vad_require(vad_kinds(first) == ["audio_file"],
                f"stream first timestamp chunk: {vad_kinds(first)}")
    vad_check_audio(first[0], [1] * 50 + [2] * 50)

    vad_end(step, 100.25)
    vad_require(not vad_drain(output), "stream timestamp end fired early")
    vad_feed(step, 3, 100.3)
    final = vad_drain(output)
    vad_require(vad_kinds(final) == ["audio_file", "vad_end"],
                f"stream timestamp final: {vad_kinds(final)}")
    vad_check_audio(final[0], [2] * 50 + [3] * 50)
    vad_check_stamps(started + first + final, 100.05)


def vad_test_stream_finalize_rechunks_holdback():
    step, output = vad_new_step(
        stream=True, stream_chunk_ms=100, manual_end_offset_ms=-500,
    )
    vad_feed(step, 0, 100.0)
    vad_start(step, 100.0)
    started = vad_drain(output)
    prior = []
    for value in range(1, 11):
        vad_feed(step, value, 100.0 + value * 0.1)
        prior += vad_drain(output)
    vad_require(
        all(len(vad_decode(message)) == 100 for message in prior),
        "open stream emitted a non-exact chunk",
    )

    # target=100.7 leaves 200 ms beyond the already emitted prefix. Finalize
    # must release that as two configured chunks, never one oversized WAV.
    vad_end(step, 101.2)
    finished = vad_drain(output)
    vad_require(vad_kinds(finished) == [
        "audio_file", "audio_file", "vad_end",
    ], f"holdback finalize envelope: {vad_kinds(finished)}")
    vad_check_audio(finished[0], [6] * 100)
    vad_check_audio(finished[1], [7] * 100)
    vad_check_stamps(started + prior + finished, 100.0)


def vad_test_cancel_trims_partial_ring_chunk():
    step, output = vad_new_step(manual_start_offset_ms=-100)
    vad_feed(step, 1, 100.1)        # chunk interval [100.0, 100.1)
    step.cancel_timestamp = 100.05  # invalidate only its first 50 ms
    vad_start(step, 100.1)          # requested target 100.0 clamps to 100.05
    started = vad_drain(output)
    vad_feed(step, 2, 100.2)
    vad_end(step, 100.2)
    finished = vad_drain(output)
    vad_require(vad_kinds(started + finished) == [
        "vad_start", "vad_end", "audio_file",
    ], f"partial cancel envelope: {vad_kinds(started + finished)}")
    vad_check_audio(finished[1], [1] * 50 + [2] * 100)
    vad_check_stamps(started + finished, 100.1)


def vad_test_pending_end_bypasses_open_segment_cap():
    step, output = vad_new_step(
        ring_seconds=1,
        manual_end_offset_ms=1000,
    )
    vad_feed(step, 0, 100.0)
    vad_start(step, 100.0)
    started = vad_drain(output)
    for value in range(1, 10):
        vad_feed(step, value, 100.0 + value * 0.1)
    vad_end(step, 100.9)  # target 101.9 is known but still in future audio
    vad_feed(step, 10, 101.0)
    vad_require(step.span_active,
                "pending explicit end was mistaken for an open segment")
    vad_require(not vad_drain(output),
                "pending explicit end was force-ended at ring_seconds")
    for value in range(11, 20):
        vad_feed(step, value, 100.0 + value * 0.1)
    finished = vad_drain(output)
    vad_require(vad_kinds(finished) == ["vad_end", "audio_file"],
                f"pending end completion: {vad_kinds(finished)}")
    vad_require(len(vad_decode(finished[1])) == 1900,
                "pending end segment length was not 1.9 seconds")
    vad_check_stamps(started + finished, 100.0)


class VadFakeDetector:
    def __init__(self, responses):
        self.responses = list(responses)
        self.closed = False
        self.feed_count = 0
        self.reset_count = 0

    def feed(self, _pcm):
        self.feed_count += 1
        return self.responses.pop(0)

    def reset(self):
        self.reset_count += 1

    def close(self):
        self.closed = True


class VadFakeEvents:
    def __init__(self):
        self.messages = []

    def submit(self, message):
        self.messages.append(message)


def vad_test_server_manual_pending():
    from Modules.vad_server.ServerVADStep import ServerVADStep

    detector = VadFakeDetector([[]])
    output = Queue()
    config = vad_config(
        auto_detect=True,
        start_offset_ms=0,
        end_offset_ms=0,
        __events=VadFakeEvents(),
    )
    with patch(
        "Modules.vad_server.ServerVADStep.ServerVADCaller",
        return_value=detector,
    ):
        step = ServerVADStep(
            1, "vad_manual_pending_test",
            setup_logger("vad_manual_pending_test"), Queue(), Queue(),
            output, Queue(), config,
        )

    # Establish the latest audio end at 100.0, then put the manual boundary
    # 50 ms into the next chunk. Pending _mark=None must not resume detection.
    vad_feed(step, 0, 100.0)
    vad_start(step, 100.05)
    vad_require(step.span_active and step._manual,
                "pending manual start incorrectly resumed detection")
    vad_require(detector.reset_count == 0,
                "pending manual start reset the detector")

    vad_feed(step, 1, 100.1)
    started = vad_drain(output)
    vad_require(vad_kinds(started) == ["vad_start"],
                f"server manual timestamp start: {vad_kinds(started)}")
    vad_end(step, 100.15)
    vad_require(step.span_active and detector.reset_count == 0,
                "pending manual end resumed detection early")

    vad_feed(step, 2, 100.2)
    finished = vad_drain(output)
    vad_require(vad_kinds(finished) == ["vad_end", "audio_file"],
                f"server manual timestamp end: {vad_kinds(finished)}")
    vad_check_audio(finished[1], [1] * 50 + [2] * 50)
    vad_check_stamps(started + finished, 100.05)
    vad_require(not step.span_active and not step._manual,
                "finished manual turn did not resume detection")
    vad_require(detector.reset_count == 1,
                f"detector reset count: {detector.reset_count}")
    vad_require(detector.feed_count == 1,
                "manual audio leaked into the suspended detector")
    step.custom_dispose()


def vad_test_manual_auto_boundary_parity():
    from Modules.vad_server.ServerVADStep import ServerVADStep

    # Manual reference: start looks back 50 ms from 100.1; end extends 25 ms
    # after 100.2 and therefore resolves in the next chunk.
    manual, manual_output = vad_new_step(
        manual_start_offset_ms=-50,
        manual_end_offset_ms=25,
    )
    vad_feed(manual, 1, 100.0)
    vad_feed(manual, 2, 100.1)
    vad_start(manual, 100.1)
    manual_messages = vad_drain(manual_output)
    vad_feed(manual, 3, 100.2)
    vad_end(manual, 100.2)
    vad_feed(manual, 4, 100.3)
    manual_messages += vad_drain(manual_output)

    detector = VadFakeDetector([
        [], [{"type": "speech_started"}],
        [{"type": "speech_stopped"}], [],
    ])
    auto_output = Queue()
    auto_config = vad_config(
        auto_detect=True,
        start_offset_ms=-50,
        end_offset_ms=25,
        __events=VadFakeEvents(),
    )
    with patch(
        "Modules.vad_server.ServerVADStep.ServerVADCaller",
        return_value=detector,
    ):
        auto = ServerVADStep(
            1, "vad_auto_parity_test", setup_logger("vad_auto_parity_test"),
            Queue(), Queue(), auto_output, Queue(), auto_config,
        )
    for value, end_ts in (
            (1, 100.0), (2, 100.1), (3, 100.2), (4, 100.3)):
        vad_feed(auto, value, end_ts)
    auto_messages = vad_drain(auto_output)

    vad_require(
        vad_kinds(manual_messages) == ["vad_start", "vad_end", "audio_file"],
        f"manual parity envelope: {vad_kinds(manual_messages)}",
    )
    vad_require(
        vad_kinds(auto_messages) == ["vad_start", "vad_end", "audio_file"],
        f"auto parity envelope: {vad_kinds(auto_messages)}",
    )
    expected = [2] * 50 + [3] * 100 + [4] * 25
    vad_check_audio(manual_messages[2], expected)
    vad_check_audio(auto_messages[2], expected)
    vad_check_stamps(manual_messages, 100.1)
    vad_check_stamps(auto_messages, 100.1)
    auto.custom_dispose()


def vad_test_auto_offsets():
    from Modules.vad_server.ServerVADStep import ServerVADStep

    detector = VadFakeDetector([
        [{"type": "speech_started"}], [], [], [],
        [{"type": "speech_stopped"}],
    ])
    events = VadFakeEvents()
    output = Queue()
    config = vad_config(
        auto_detect=True,
        start_offset_ms=150,
        end_offset_ms=-150,
        __events=events,
    )
    with patch(
        "Modules.vad_server.ServerVADStep.ServerVADCaller",
        return_value=detector,
    ):
        step = ServerVADStep(
            1, "vad_auto_test", setup_logger("vad_auto_test"), Queue(),
            Queue(), output, Queue(), config,
        )
    vad_require(step.init_error is None,
                f"auto VAD init failed: {step.init_error}")

    for value in (1, 2):
        vad_feed(
            step, value,
            timestamp=VAD_START_TIMESTAMP + (value - 1) * 0.1,
        )
        vad_require(not vad_drain(output),
                    f"auto delayed start fired on block {value}")
    vad_feed(step, 3, timestamp=VAD_START_TIMESTAMP + 0.2)
    start_messages = vad_drain(output)
    vad_require(vad_kinds(start_messages) == ["vad_start"],
                f"auto start output: {vad_kinds(start_messages)}")
    vad_feed(step, 4, timestamp=VAD_START_TIMESTAMP + 0.3)
    vad_require(not vad_drain(output), "auto non-stream emitted before stop")
    vad_feed(step, 5, timestamp=VAD_START_TIMESTAMP + 0.4)
    end_messages = vad_drain(output)
    vad_require(vad_kinds(end_messages) == ["vad_end", "audio_file"],
                f"auto end output: {vad_kinds(end_messages)}")
    vad_check_audio(end_messages[1], [3] * 50 + [4] * 50)
    vad_check_stamps(start_messages + end_messages)
    vad_require(
        events.messages == [{
            "signal": "cancel", "timestamp": VAD_START_TIMESTAMP,
            "source": 1,
        }],
        f"auto activation cancel mismatch: {events.messages}",
    )
    step.custom_dispose()
    vad_require(detector.closed, "fake detector was not closed")


def run_vad():
    tests = [
        ("Signed offset validation", vad_test_validator),
        ("Negative start lookback", vad_test_start_lookback),
        ("Positive start delay", vad_test_start_delay),
        ("Positive end delay", vad_test_positive_end),
        ("Negative end crop", vad_test_negative_end),
        ("Collapsed non-stream WAV", vad_test_collapsed_nonstream),
        ("Stream negative-end holdback", vad_test_stream_holdback_and_collapse),
        ("Manual timestamp boundaries", vad_test_manual_timestamp_boundaries),
        ("Manual future timestamp boundaries",
         vad_test_manual_future_timestamp_boundaries),
        ("Multi-chunk timestamp crop", vad_test_multi_chunk_timestamp_crop),
        ("Multi-chunk future wait", vad_test_multi_chunk_future_wait),
        ("Exact empty duration", vad_test_exact_empty_duration),
        ("Stream timestamp boundaries", vad_test_stream_timestamp_boundaries),
        ("Stream finalize re-chunks holdback",
         vad_test_stream_finalize_rechunks_holdback),
        ("Cancel trims partial ring chunk",
         vad_test_cancel_trims_partial_ring_chunk),
        ("Pending end bypasses open-segment cap",
         vad_test_pending_end_bypasses_open_segment_cap),
        ("Server manual pending timestamps", vad_test_server_manual_pending),
        ("Manual/automatic boundary parity",
         vad_test_manual_auto_boundary_parity),
        ("Automatic VAD offsets", vad_test_auto_offsets),
    ]
    results = []
    for name, test in tests:
        try:
            test()
        except Exception as error:
            print(f"  FAIL: {name}: {error}")
            results.append((name, False))
        else:
            print(f"  PASS: {name}")
            results.append((name, True))

    passed = sum(ok for _, ok in results)
    print(f"\n  VAD summary: {passed}/{len(results)} passed")
    return passed == len(results)



class VADSegmentationUnitTest(unittest.TestCase):
    def test_signed_offset_validation(self):
        vad_test_validator()

    def test_negative_start_lookback(self):
        vad_test_start_lookback()

    def test_positive_start_delay(self):
        vad_test_start_delay()

    def test_positive_end_delay(self):
        vad_test_positive_end()

    def test_negative_end_crop(self):
        vad_test_negative_end()

    def test_collapsed_nonstream_wav(self):
        vad_test_collapsed_nonstream()

    def test_stream_negative_end_holdback(self):
        vad_test_stream_holdback_and_collapse()

    def test_manual_timestamp_boundaries(self):
        vad_test_manual_timestamp_boundaries()

    def test_manual_future_timestamp_boundaries(self):
        vad_test_manual_future_timestamp_boundaries()

    def test_multi_chunk_timestamp_crop(self):
        vad_test_multi_chunk_timestamp_crop()

    def test_multi_chunk_future_wait(self):
        vad_test_multi_chunk_future_wait()

    def test_exact_empty_duration(self):
        vad_test_exact_empty_duration()

    def test_stream_timestamp_boundaries(self):
        vad_test_stream_timestamp_boundaries()

    def test_stream_finalize_rechunks_holdback(self):
        vad_test_stream_finalize_rechunks_holdback()

    def test_cancel_trims_partial_ring_chunk(self):
        vad_test_cancel_trims_partial_ring_chunk()

    def test_pending_end_bypasses_open_segment_cap(self):
        vad_test_pending_end_bypasses_open_segment_cap()

    def test_server_manual_pending_timestamps(self):
        vad_test_server_manual_pending()

    def test_manual_automatic_boundary_parity(self):
        vad_test_manual_auto_boundary_parity()

    def test_automatic_vad_offsets(self):
        vad_test_auto_offsets()


if __name__ == "__main__":
    unittest.main()
