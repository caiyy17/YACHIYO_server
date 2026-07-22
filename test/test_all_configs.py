"""End-to-end smoke tests for the supported representative pipelines."""

import argparse
import asyncio
import array
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
TTS_API = "http://127.0.0.1:8011/v1"
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Fields that must be present and non-empty in a completed response.
CONFIGS = {
    "unity_chan_text": {
        "input_type": "text",
        "expected_streaming": ["text"],
        "expected_final": ["action", "expression", "action_hint", "expression_hint"],
    },
    "demo": {
        "input_type": "audio",
        "expected_streaming": ["text"],
        "expected_final": ["audio_data", "duration", "text"],
    },
    "unity_chan_default": {
        "input_type": "audio",
        "expected_streaming": ["text"],
        "expected_final": [
            "audio_data", "duration", "text", "action", "action_hint",
            "expression", "expression_hint",
        ],
    },
    "unity_chan_default_vad": {
        "input_type": "vad_audio",
        "expected_streaming": ["text"],
        "expected_final": [
            "audio_data", "duration", "text", "action", "action_hint",
            "expression", "expression_hint",
        ],
    },
    "unity_chan_default_vad_auto": {
        "input_type": "vad_audio",
        "expected_streaming": ["text"],
        "expected_final": [
            "audio_data", "duration", "text", "action", "action_hint",
            "expression", "expression_hint",
        ],
    },
    "unity_chan_humanoid": {
        "input_type": "audio",
        "expected_streaming": ["text"],
        "expected_final": [
            "audio_data", "action", "duration", "text", "action_hint",
            "expression",
        ],
    },
    "unity_chan_live": {
        "input_type": "danmaku",
        "expected_streaming": ["text"],
        "expected_final": ["audio_data", "action", "duration"],
    },
}


async def generate_test_audio(text="你好，今天天气怎么样"):
    """Generate one real WAV input through the configured TTS service."""
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


async def generate_vad_chunks(text="你好，今天天气怎么样"):
    """Generate 48 kHz PCM16 and package each 20 ms input as a WAV chunk."""
    async with httpx.AsyncClient(timeout=30) as http:
        response = await http.post(
            f"{TTS_API}/audio/speech",
            json={
                "model": "tts",
                "input": text,
                "voice": "test_cn",
                "response_format": "pcm",
            },
        )
    if response.status_code != 200 or not response.content:
        raise RuntimeError(
            f"TTS PCM failed: {response.status_code} {response.text[:200]}"
        )
    sample_rate = int(response.headers.get("X-Sample-Rate", "24000"))
    if 48000 % sample_rate:
        raise RuntimeError(f"unsupported TTS sample rate: {sample_rate}")
    samples = array.array("h")
    samples.frombytes(response.content)
    factor = 48000 // sample_rate
    if factor != 1:
        samples = array.array(
            "h", (sample for sample in samples for _ in range(factor))
        )
    raw = samples.tobytes()
    chunk_bytes = 960 * 2
    return [wav_chunk(raw[i:i + chunk_bytes])
            for i in range(0, len(raw), chunk_bytes)
            if raw[i:i + chunk_bytes]]


def wav_chunk(pcm):
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(48000)
        wav_file.writeframes(pcm)
    return base64.b64encode(buffer.getvalue()).decode()


def collect_fields(data, expected_final, collected_final):
    for key in expected_final:
        if data.get(key):
            collected_final[key] = data[key]


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


async def test_config(config_name, spec):
    """Register, initialize, run, and always unregister one isolated client."""
    client_id = f"e2e_{config_name}_{uuid.uuid4().hex[:10]}"
    registered = False
    cleanup_ok = True
    success = False
    all_messages = []
    streamed_text = []
    final_fields = {}
    eos_seen = False

    print(f"\n[{config_name}] {spec['input_type']}")
    try:
        async with httpx.AsyncClient(timeout=30) as http:
            response = await http.post(
                f"{SERVER}/register/", json={"client_id": client_id}
            )
            body = response.json()
            if response.status_code != 200 or body.get("status") != "registered":
                raise RuntimeError(
                    f"register failed: {response.status_code} {body}"
                )
            registered = True

            # A fresh client must not need force. Using it here would hide stale
            # runtime or cleanup bugs in the lifecycle under test.
            response = await http.post(
                f"{SERVER}/init_pipeline/{client_id}",
                json={"config": config_name},
            )
            body = response.json()
            if response.status_code != 200 or body.get("status") != "initialized":
                raise RuntimeError(f"init failed: {response.status_code} {body}")

        input_type = spec["input_type"]
        if input_type == "text":
            input_message = {
                "text": "你好啊，今天天气怎么样",
                "timestamp": time.time(),
            }
        elif input_type == "audio":
            input_message = {
                "audio_file": await generate_test_audio(),
                "timestamp": time.time(),
            }
        elif input_type == "vad_audio":
            input_message = None
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
            raise RuntimeError(f"unknown input type: {input_type}")

        async with websockets.connect(
            f"{WS_URL}/{client_id}", max_size=None
        ) as websocket:
            if input_type == "vad_audio":
                chunks = await generate_vad_chunks()
                silence = wav_chunk(bytes(960 * 2))
                for _ in range(10):
                    await websocket.send(json.dumps({
                        "audio_data": silence,
                        "timestamp": time.time(),
                    }))
                    await asyncio.sleep(0.02)
                await websocket.send(json.dumps({
                    "signal": "recording_start",
                    "timestamp": time.time(),
                }))
                for chunk in chunks:
                    await websocket.send(json.dumps({
                        "audio_data": chunk,
                        "timestamp": time.time(),
                    }))
                    await asyncio.sleep(0.02)
                await websocket.send(json.dumps({
                    "signal": "recording_end",
                    "timestamp": time.time(),
                }))
                for _ in range(10):
                    await websocket.send(json.dumps({
                        "audio_data": silence,
                        "timestamp": time.time(),
                    }))
                    await asyncio.sleep(0.02)
            else:
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

            started = False
            deadline = time.monotonic() + 120
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
                all_messages.append(data)
                collect_fields(data, spec["expected_final"], final_fields)

                signal = data.get("signal")
                if signal == "SoS":
                    started = True
                elif signal == "EoS":
                    eos_seen = True
                    break
                elif started and data.get("text"):
                    streamed_text.append(data["text"])

            if not eos_seen:
                raise RuntimeError("EoS not received within 120s")

            # Some non-streaming branches arrive immediately after EoS.
            while True:
                try:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=10)
                except asyncio.TimeoutError:
                    break
                data = json.loads(raw)
                if not isinstance(data, dict):
                    raise RuntimeError(
                        f"server returned non-object JSON: {type(data).__name__}"
                    )
                all_messages.append(data)
                collect_fields(data, spec["expected_final"], final_fields)

        full_text = "".join(streamed_text)
        missing = []
        if "text" in spec["expected_streaming"] and not full_text:
            missing.append("streaming text")
        missing.extend(
            field for field in spec["expected_final"] if field not in final_fields
        )
        if missing:
            raise RuntimeError(f"missing output: {', '.join(missing)}")

        print(
            f"  PASS: EoS, text={len(full_text)}, "
            f"final={','.join(final_fields)}"
        )
        success = True
    except Exception as error:
        print(f"  FAIL: {error}")
        if all_messages:
            print(f"  received {len(all_messages)} message(s)")
    finally:
        if registered:
            try:
                async with httpx.AsyncClient(timeout=30) as http:
                    response = await http.post(
                        f"{SERVER}/unregister/", json={"client_id": client_id}
                    )
                body = response.json()
                if (
                    response.status_code != 200
                    or body.get("status") != "unregistered"
                ):
                    raise RuntimeError(
                        f"cleanup returned {response.status_code} {body}"
                    )
            except Exception as error:
                cleanup_ok = False
                print(f"  FAIL: cleanup: {error}")
        files_ok = cleanup_client_files(client_id)
        cleanup_ok = files_ok and cleanup_ok
    return success and cleanup_ok


async def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Run supported pipeline end-to-end smoke tests."
    )
    parser.add_argument(
        "configs", nargs="*", metavar="CONFIG",
        help="Config name(s); omit to run every supported config.",
    )
    args = parser.parse_args(argv)
    requested = args.configs
    unknown = [name for name in requested if name not in CONFIGS]
    if unknown:
        print(f"FAIL: unknown config(s): {', '.join(unknown)}")
        print(f"Known: {', '.join(CONFIGS)}")
        return 1

    names = requested or list(CONFIGS)
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
