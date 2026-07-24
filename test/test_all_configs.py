"""End-to-end tests for every formal top-level pipeline config.

This is the single registry and entrypoint for actual-config tests.  The
module/unit suites must build their own minimal configs and must not add
production configs here merely to exercise a module.

Whenever a formal ``configs/*.json`` file is added or removed, add or remove
its one explicit registry block below.  ``dev_*`` and ``evt_*`` are excluded.
"""

import argparse
import asyncio
import base64
import io
import json
import time
import uuid
import wave
from pathlib import Path

import httpx
import websockets


SERVER = "http://localhost:8910"
WS_URL = "ws://localhost:8910/ws"
WEBRTC_SERVER = "http://localhost:15168"
TTS_API = "http://127.0.0.1:8011/v1"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
TEST_WAV = Path(__file__).resolve().parent / "test_voice.wav"


# One explicit block per formal config.  Do not generate this registry by
# scanning the directory: a config addition/deletion must be an intentional
# test-suite change, and the consistency preflight below enforces that.
CONFIGS = {
    # configs/demo.json
    "demo": {
        "runner": "websocket",
        "input_type": "audio_file",
        "expected_streaming": ["text"],
        "expected_final": ["audio_data", "duration", "text"],
    },

    # configs/loopback.json
    "loopback": {
        "runner": "webrtc_loopback",
        "expected_media": ["audio", "video", "data"],
    },

    # configs/unity_chan_default.json
    "unity_chan_default": {
        "runner": "websocket",
        "input_type": "audio_file",
        "expected_streaming": ["text"],
        "expected_final": [
            "audio_data",
            "duration",
            "text",
            "action",
            "action_hint",
            "expression",
            "expression_hint",
        ],
    },

    # configs/unity_chan_humanoid.json
    "unity_chan_humanoid": {
        "runner": "server_vad",
        "input_type": "audio_chunks",
        "expected_signals": ["recording_start", "recording_end",
                             "SoS", "item_SoS", "item_EoS", "EoS"],
        "expected_chunk_fields": ["audio_data", "motion"],
        "audio_chunk_ms": 200,
        "motion_frames": 6,
    },

    # configs/unity_chan_live.json
    "unity_chan_live": {
        "runner": "websocket",
        "input_type": "danmaku",
        "expected_signals": ["SoS", "item_SoS", "item_EoS", "EoS"],
        "expected_chunk_fields": ["audio_data", "motion"],
        "audio_chunk_ms": 200,
        "motion_frames": 6,
    },

    # configs/unity_chan_text.json
    "unity_chan_text": {
        "runner": "websocket",
        "input_type": "text",
        "expected_streaming": ["text"],
        "expected_final": [
            "action",
            "expression",
            "action_hint",
            "expression_hint",
        ],
    },

    # configs/unity_chan_webrtc.json
    "unity_chan_webrtc": {
        "runner": "webrtc_pipeline",
        "expected_signals": ["SoS", "meta", "EoS"],
        "expected_video_color": [128, 0, 128],
    },
}


def formal_config_names():
    """Return the actual top-level formal config names on disk."""
    return {
        path.stem
        for path in (PROJECT_ROOT / "configs").glob("*.json")
        if not path.name.startswith(("dev_", "evt_"))
    }


def registry_consistency_errors():
    """Require the explicit registry to exactly match formal config files."""
    existing = formal_config_names()
    registered = set(CONFIGS)
    errors = []
    missing = sorted(existing - registered)
    extra = sorted(registered - existing)
    if missing:
        errors.append(
            "formal config(s) missing from CONFIGS: " + ", ".join(missing)
        )
    if extra:
        errors.append(
            "CONFIGS entry/entries without a formal config file: "
            + ", ".join(extra)
        )
    return errors


async def generate_test_audio(text="你好，今天天气怎么样"):
    """Generate a real WAV input through the configured TTS service."""
    async with httpx.AsyncClient(timeout=30) as http:
        response = await http.post(
            f"{TTS_API}/audio/speech",
            json={"model": "tts", "input": text, "voice": "test_cn"},
        )
    if response.status_code != 200 or not response.content:
        raise RuntimeError(
            f"TTS failed: {response.status_code} {response.text[:200]}"
        )
    return base64.b64encode(response.content).decode()


def wav_chunks(path, chunk_ms=20):
    """Split one real WAV into independently decodable WAV chunks."""
    with wave.open(str(path), "rb") as source:
        channels = source.getnchannels()
        sample_width = source.getsampwidth()
        sample_rate = source.getframerate()
        compression = source.getcomptype()
        compression_name = source.getcompname()
        frames_per_chunk = max(1, round(sample_rate * chunk_ms / 1000))
        chunks = []
        total_frames = 0
        while True:
            pcm = source.readframes(frames_per_chunk)
            if not pcm:
                break
            frame_count = len(pcm) // (channels * sample_width)
            output = io.BytesIO()
            with wave.open(output, "wb") as chunk:
                chunk.setnchannels(channels)
                chunk.setsampwidth(sample_width)
                chunk.setframerate(sample_rate)
                chunk.setcomptype(compression, compression_name)
                chunk.writeframes(pcm)
            total_frames += frame_count
            chunks.append((
                base64.b64encode(output.getvalue()).decode(),
                frame_count / sample_rate,
            ))
    if not chunks:
        raise RuntimeError(f"test WAV contains no audio: {path}")
    return chunks, total_frames / sample_rate


def wav_duration(encoded):
    with wave.open(io.BytesIO(base64.b64decode(encoded)), "rb") as audio:
        return audio.getnframes() / audio.getframerate()


def has_value(data, key):
    return key in data and data[key] not in (None, "", [], {})


def collect_fields(data, expected, collected):
    for key in expected:
        if has_value(data, key):
            collected[key] = data[key]


def require_signal_subsequence(expected, actual):
    """Require every expected signal in order, allowing unrelated signals."""
    position = 0
    for signal in actual:
        if position < len(expected) and signal == expected[position]:
            position += 1
    if position != len(expected):
        raise RuntimeError(
            "signal order mismatch: expected subsequence "
            f"{expected}, received {actual}"
        )


def cleanup_client_files(client_id):
    """Remove only the log and history created for this test client."""
    ok = True
    paths = (
        ("client log", PROJECT_ROOT / "logs" / f"client_{client_id}.log"),
        ("client history",
         PROJECT_ROOT / "history" / f"history_{client_id}.json"),
    )
    for label, path in paths:
        try:
            path.unlink(missing_ok=True)
        except OSError as error:
            print(f"  FAIL: {label} cleanup: {error}")
            ok = False
    return ok


async def register_client(client_id):
    async with httpx.AsyncClient(timeout=60) as http:
        response = await http.post(
            f"{SERVER}/register/", json={"client_id": client_id}
        )
        body = response.json()
        if response.status_code != 200 or body.get("status") != "registered":
            raise RuntimeError(
                f"register failed: {response.status_code} {body}"
            )


async def initialize_client(client_id, config_name):
    async with httpx.AsyncClient(timeout=60) as http:
        # A fresh client must not need force.  Force would hide stale-runtime
        # and lifecycle cleanup failures.
        response = await http.post(
            f"{SERVER}/init_pipeline/{client_id}",
            json={"config": config_name},
        )
        body = response.json()
        if response.status_code != 200 or body.get("status") != "initialized":
            raise RuntimeError(f"init failed: {response.status_code} {body}")


async def wait_gateway_closed(client_id):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            async with httpx.AsyncClient(timeout=5) as http:
                response = await http.get(f"{WEBRTC_SERVER}/status")
            body = response.json()
            if (
                response.status_code == 200
                and client_id not in body.get("sessions", {})
            ):
                return True
        except Exception as error:
            print(f"  FAIL: WebRTC cleanup check: {error}")
            return False
        await asyncio.sleep(0.2)
    print(f"  FAIL: WebRTC session still present: {client_id}")
    return False


async def cleanup_client(client_id, check_gateway=False):
    """Unregister one exact client and verify all owned state is gone."""
    ok = True
    try:
        async with httpx.AsyncClient(timeout=30) as http:
            response = await http.post(
                f"{SERVER}/unregister/", json={"client_id": client_id}
            )
            body = response.json()
            if (
                response.status_code != 200
                or body.get("status") not in {"unregistered", "not registered"}
            ):
                raise RuntimeError(
                    f"cleanup returned {response.status_code} {body}"
                )
            response = await http.get(f"{SERVER}/clients/")
            clients = response.json().get("clients", [])
            if response.status_code != 200 or client_id in clients:
                raise RuntimeError(f"client remains registered: {client_id}")
    except Exception as error:
        print(f"  FAIL: cleanup: {error}")
        ok = False
    if check_gateway:
        ok = await wait_gateway_closed(client_id) and ok
    return cleanup_client_files(client_id) and ok


async def receive_until_eos(websocket, expected_final, timeout=120):
    """Collect one pipeline response through EoS plus its short output tail."""
    messages = []
    streamed_text = []
    final_fields = {}
    started = False
    eos_seen = False
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        try:
            raw = await asyncio.wait_for(
                websocket.recv(), timeout=min(5, remaining)
            )
        except asyncio.TimeoutError:
            continue
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise RuntimeError(
                f"server returned non-object JSON: {type(data).__name__}"
            )
        messages.append(data)
        collect_fields(data, expected_final, final_fields)
        signal = data.get("signal")
        if signal == "SoS":
            started = True
        elif signal == "EoS":
            eos_seen = True
            break
        elif started and data.get("text"):
            streamed_text.append(data["text"])

    if not eos_seen:
        raise RuntimeError(f"EoS not received within {timeout}s")

    # A parallel branch can publish its final value immediately after the
    # signal branch.  Drain until the connection is idle, without requiring
    # the caller to know that branch ordering.
    while True:
        try:
            raw = await asyncio.wait_for(websocket.recv(), timeout=5)
        except asyncio.TimeoutError:
            break
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise RuntimeError(
                f"server returned non-object JSON: {type(data).__name__}"
            )
        messages.append(data)
        collect_fields(data, expected_final, final_fields)
        if data.get("text"):
            streamed_text.append(data["text"])
    return messages, "".join(streamed_text), final_fields


async def run_websocket_pipeline(client_id, spec):
    input_type = spec["input_type"]
    if input_type == "text":
        input_message = {
            "text": "你好啊，今天天气怎么样",
            "timestamp": time.time(),
        }
    elif input_type == "audio_file":
        input_message = {
            "audio_file": await generate_test_audio(),
            "timestamp": time.time(),
        }
    elif input_type == "danmaku":
        input_message = {
            "text": "今天直播好有趣",
            "user": "测试用户",
            "msg_type": "danmaku",
            "guard_level": 0,
            "num": 0,
            "price": 0,
            "timestamp": time.time(),
        }
    else:
        raise RuntimeError(f"unknown WebSocket input type: {input_type}")

    async with websockets.connect(
        f"{WS_URL}/{client_id}", max_size=None
    ) as websocket:
        await websocket.send(json.dumps(input_message))
        if input_type == "danmaku":
            for extra in ("好厉害啊", "666", "来首歌吧"):
                await websocket.send(json.dumps({
                    "text": extra,
                    "user": f"用户{extra}",
                    "msg_type": "danmaku",
                    "guard_level": 0,
                    "num": 0,
                    "price": 0,
                    "timestamp": time.time(),
                }))
                await asyncio.sleep(0.2)

        collected = spec.get("expected_chunk_fields") or spec["expected_final"]
        messages, full_text, final_fields = await receive_until_eos(
            websocket, collected
        )

    # Streaming-chunk configs share the paired-chunk contract with the
    # server_vad runner; classic configs assert streamed text + final fields.
    if "expected_chunk_fields" in spec:
        total, paired = verify_paired_chunk_output(messages, spec)
        return (
            f"{input_type} -> {total} x {spec['audio_chunk_ms']}ms "
            f"audio chunks ({paired} motion-paired)"
        )

    missing = []
    if "text" in spec["expected_streaming"] and not full_text:
        missing.append("streaming text")
    missing.extend(
        field for field in spec["expected_final"] if field not in final_fields
    )
    if missing:
        raise RuntimeError(f"missing output: {', '.join(missing)}")
    return (
        f"EoS, text={len(full_text)}, "
        f"final={','.join(final_fields)}"
    )


async def run_server_vad_pipeline(client_id, spec):
    """Drive the actual ServerVAD config with timestamped client WAV chunks."""
    chunks, input_duration = wav_chunks(TEST_WAV)
    async with websockets.connect(
        f"{WS_URL}/{client_id}", max_size=None
    ) as websocket:
        start_timestamp = time.time()
        await websocket.send(json.dumps({
            "signal": "recording_start",
            "timestamp": start_timestamp,
        }))
        elapsed = 0.0
        for encoded, duration in chunks:
            elapsed += duration
            await websocket.send(json.dumps({
                "audio_data": encoded,
                # Client audio chunk timestamps are their media end points.
                "timestamp": start_timestamp + elapsed,
            }))
        await websocket.send(json.dumps({
            "signal": "recording_end",
            "timestamp": start_timestamp + elapsed,
        }))

        messages, _, _ = await receive_until_eos(
            websocket, spec["expected_chunk_fields"]
        )

    total, paired = verify_paired_chunk_output(messages, spec)
    return (
        f"{len(chunks)} input chunks/{input_duration:.3f}s -> "
        f"{total} x {spec['audio_chunk_ms']}ms audio chunks "
        f"({paired} motion-paired)"
    )


def verify_paired_chunk_output(messages, spec):
    """Assert the streaming-chunk contract on one collected response:
    expected signal subsequence; within one item span the chunks are
    either all paired audio+motion or all audio-only (a hint-less
    sentence passes through without motion by design — mixing is a bug);
    every chunk carries the exact audio duration, paired chunks the exact
    motion frame count, and the run must contain at least one paired
    chunk. Returns (total, paired) chunk counts."""
    signals = [
        message["signal"] for message in messages
        if isinstance(message.get("signal"), str)
    ]
    require_signal_subsequence(spec["expected_signals"], signals)

    expected_duration = spec["audio_chunk_ms"] / 1000
    expected_motion_frames = spec["motion_frames"]
    total = paired_total = 0
    span_index = -1
    span_kinds = {}
    for index, message in enumerate(messages):
        if message.get("signal") == "item_SoS":
            span_index += 1
            continue
        if not (has_value(message, "audio_data")
                or has_value(message, "motion")):
            continue
        total += 1
        if not has_value(message, "audio_data"):
            raise RuntimeError(f"message {index} has motion without audio")
        duration = wav_duration(message["audio_data"])
        if abs(duration - expected_duration) > 1e-6:
            raise RuntimeError(
                f"output chunk (message {index}) audio is {duration:.6f}s, "
                f"expected {expected_duration:.6f}s"
            )
        paired = has_value(message, "motion")
        span_kinds.setdefault(span_index, set()).add(paired)
        if paired:
            paired_total += 1
            motion = message["motion"]
            if not isinstance(motion, list) \
                    or len(motion) != expected_motion_frames:
                size = len(motion) if isinstance(motion, list) \
                    else type(motion).__name__
                raise RuntimeError(
                    f"output chunk (message {index}) motion size is {size}, "
                    f"expected {expected_motion_frames}"
                )
    if total == 0:
        raise RuntimeError("no output chunks received")
    mixed = [span for span, kinds in span_kinds.items() if len(kinds) > 1]
    if mixed:
        raise RuntimeError(
            f"span(s) {mixed} mix paired and audio-only chunks"
        )
    if paired_total == 0:
        raise RuntimeError("no paired audio+motion chunk in the response")
    return total, paired_total


async def wait_for(predicate, timeout, description):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise RuntimeError(f"{description} not ready within {timeout}s")


async def run_webrtc(client_id, spec, loopback=False):
    """Exercise one real config over the production WebRTC gateway."""
    import numpy as np
    from aiortc import RTCPeerConnection, RTCSessionDescription
    from aiortc.mediastreams import MediaStreamError
    import test_webrtc as media

    pc = RTCPeerConnection()
    receiver_tasks = []
    receiver_errors = []
    received_data = []
    audio_frames = 0
    video_frames = 0
    nonsilent_audio = False
    content_video = False
    output_channel = None
    echo_marker = {"payload": f"all-config-echo-{uuid.uuid4().hex}"}
    echo_seen = asyncio.Event()

    if loopback:
        pc.addTrack(media.SilenceTrack())
        pc.addTrack(media.BlackVideoTrack())
    else:
        if not TEST_WAV.is_file():
            raise RuntimeError(f"test WAV not found: {TEST_WAV}")
        input_frames = media.load_test_audio(str(TEST_WAV))
        send_audio = media.TestAudioTrack(input_frames)
        send_video = media.TestVideoTrack(send_audio)
        pc.addTrack(send_audio)
        pc.addTrack(send_video)

    async def drain_track(track):
        nonlocal audio_frames, video_frames, nonsilent_audio, content_video
        try:
            while True:
                frame = await track.recv()
                if track.kind == "audio":
                    audio_frames += 1
                    pcm = frame.to_ndarray()
                    if np.any(pcm != 0):
                        nonsilent_audio = True
                elif track.kind == "video":
                    video_frames += 1
                    if not loopback:
                        rgb = frame.to_ndarray(format="rgb24")
                        mean = rgb.mean(axis=(0, 1))
                        expected = np.asarray(
                            spec["expected_video_color"], dtype=np.float64
                        )
                        if np.linalg.norm(mean - expected) < 80:
                            content_video = True
        except asyncio.CancelledError:
            raise
        except MediaStreamError:
            pass
        except Exception as error:
            if pc.connectionState not in ("closed", "failed"):
                receiver_errors.append(f"{track.kind} receiver: {error}")

    @pc.on("track")
    def on_track(track):
        receiver_tasks.append(asyncio.create_task(drain_track(track)))

    @pc.on("datachannel")
    def on_datachannel(channel):
        nonlocal output_channel
        output_channel = channel

        @channel.on("message")
        def on_message(raw):
            try:
                message = json.loads(raw)
                if isinstance(message, dict):
                    received_data.append(message)
                    if message == echo_marker:
                        echo_seen.set()
            except Exception as error:
                receiver_errors.append(f"DataChannel receiver: {error}")

    client_channel = pc.createDataChannel("all-config-input", ordered=True)
    offer = await pc.createOffer()
    await pc.setLocalDescription(offer)

    data_task = None
    try:
        async with httpx.AsyncClient(timeout=30) as http:
            response = await http.post(
                f"{WEBRTC_SERVER}/offer/{client_id}",
                json={
                    "sdp": pc.localDescription.sdp,
                    "type": pc.localDescription.type,
                    "video_width": media.VIDEO_WIDTH,
                    "video_height": media.VIDEO_HEIGHT,
                },
            )
        if response.status_code != 200:
            raise RuntimeError(
                f"WebRTC offer failed: {response.status_code} {response.text}"
            )
        answer = response.json()
        await pc.setRemoteDescription(
            RTCSessionDescription(sdp=answer["sdp"], type=answer["type"])
        )
        await wait_for(
            lambda: pc.connectionState == "connected",
            10,
            "WebRTC connection",
        )
        await wait_for(
            lambda: client_channel.readyState == "open",
            10,
            "client DataChannel",
        )
        await wait_for(
            lambda: output_channel is not None
                    and output_channel.readyState == "open",
            10,
            "server DataChannel",
        )

        if loopback:
            # The collector groups two 50ms data slots; repeat one unique
            # marker across enough real slots and require the exact dict back.
            for _ in range(4):
                client_channel.send(json.dumps(echo_marker))
                await asyncio.sleep(1 / media.DATA_FPS)
            await asyncio.wait_for(echo_seen.wait(), timeout=10)
            await wait_for(
                lambda: audio_frames > 0 and video_frames > 0,
                10,
                "loopback media",
            )
            if receiver_errors:
                raise RuntimeError("; ".join(receiver_errors))
            return (
                f"exact data echo, audio_frames={audio_frames}, "
                f"video_frames={video_frames}"
            )

        async def send_data_lane():
            sequence = 0
            try:
                while client_channel.readyState == "open":
                    client_channel.send(json.dumps({"payload": sequence}))
                    sequence += 1
                    await asyncio.sleep(1 / media.DATA_FPS)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                if client_channel.readyState == "open":
                    receiver_errors.append(f"DataChannel sender: {error}")

        data_task = asyncio.create_task(send_data_lane())
        await asyncio.sleep(1)
        client_channel.send(json.dumps({
            "direct": True, "signal": "recording_start"
        }))
        send_audio.speaking = True
        await wait_for(
            lambda: send_audio.finished_speech,
            10,
            "test speech",
        )
        await asyncio.sleep(0.5)
        client_channel.send(json.dumps({
            "direct": True, "signal": "recording_end"
        }))
        await wait_for(
            lambda: any(
                message.get("signal") == "EoS"
                for message in received_data
            ),
            120,
            "pipeline EoS",
        )
        await wait_for(
            lambda: nonsilent_audio and content_video,
            30,
            "generated audio/video",
        )

        signals = [
            message["signal"] for message in received_data
            if isinstance(message.get("signal"), str)
        ]
        require_signal_subsequence(spec["expected_signals"], signals)
        if receiver_errors:
            raise RuntimeError("; ".join(receiver_errors))
        return (
            f"signals={signals}, nonsilent_audio, content_video, "
            f"audio_frames={audio_frames}, video_frames={video_frames}"
        )
    finally:
        if data_task is not None:
            data_task.cancel()
            await asyncio.gather(data_task, return_exceptions=True)
        await pc.close()
        if receiver_tasks:
            await asyncio.gather(*receiver_tasks, return_exceptions=True)


async def run_webrtc_pipeline(client_id, spec):
    return await run_webrtc(client_id, spec, loopback=False)


async def run_webrtc_loopback(client_id, spec):
    return await run_webrtc(client_id, spec, loopback=True)


RUNNERS = {
    "websocket": run_websocket_pipeline,
    "server_vad": run_server_vad_pipeline,
    "webrtc_pipeline": run_webrtc_pipeline,
    "webrtc_loopback": run_webrtc_loopback,
}


async def test_config(config_name, spec):
    """Initialize and run one real config, then remove all client state."""
    client_id = f"e2e_{config_name}_{uuid.uuid4().hex[:10]}"
    registered = False
    success = False
    cleanup_ok = True
    runner_name = spec["runner"]
    print(f"\n[{config_name}] {runner_name}")
    try:
        await register_client(client_id)
        registered = True
        await initialize_client(client_id, config_name)
        detail = await RUNNERS[runner_name](client_id, spec)
        print(f"  PASS: {detail}")
        success = True
    except Exception as error:
        print(f"  FAIL: {error}")
    finally:
        if registered:
            cleanup_ok = await cleanup_client(
                client_id, check_gateway=runner_name.startswith("webrtc")
            )
        else:
            cleanup_ok = cleanup_client_files(client_id)
    return success and cleanup_ok


async def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Run every formal pipeline config end-to-end."
    )
    parser.add_argument(
        "configs",
        nargs="*",
        metavar="CONFIG",
        help="Config name(s); omit to run every registered formal config.",
    )
    args = parser.parse_args(argv)

    consistency_errors = registry_consistency_errors()
    if consistency_errors:
        print("FAIL: actual config registry is out of sync:")
        for error in consistency_errors:
            print(f"  - {error}")
        return 1

    unknown = [name for name in args.configs if name not in CONFIGS]
    if unknown:
        print(f"FAIL: unknown config(s): {', '.join(unknown)}")
        print(f"Known: {', '.join(CONFIGS)}")
        return 1

    names = args.configs or list(CONFIGS)
    results = {}
    for name in names:
        results[name] = await test_config(name, CONFIGS[name])

    passed = sum(results.values())
    print(f"\nSUMMARY: {passed}/{len(results)} passed")
    for name, ok in results.items():
        print(f"  {'PASS' if ok else 'FAIL'} {name}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
