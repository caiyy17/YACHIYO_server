"""
End-to-end test for ALL pipeline configs.
Registers, inits, sends input, verifies complete output fields.
"""
import asyncio
import json
import time
import base64
import httpx
import websockets
import sys
import os

SERVER = "http://localhost:8910"
WS_URL = "ws://localhost:8910/ws"
TTS_API = "http://127.0.0.1:8011/v1"

# Expected output fields for each config
# Fields that must be present and non-empty in the final collected output
CONFIGS = {
    # Text-only configs
    "unity_chan_text": {
        "input_type": "text",
        "expected_streaming": ["text"],  # streamed via SoS/text/EoS
        "expected_final": ["action", "expression", "action_hint", "expression_hint"],
    },
    # Audio input configs (ASR → LLM → ... → TTS)
    "demo": {
        "input_type": "audio",
        "expected_streaming": ["text"],
        "expected_final": ["audio_data", "text"],
    },
    "unity_chan_default": {
        "input_type": "audio",
        "expected_streaming": ["text"],
        "expected_final": ["audio_data", "text", "action", "action_hint",
                          "expression", "expression_hint"],
    },
    "unity_chan_smpl": {
        "input_type": "audio",
        "expected_streaming": ["text"],
        "expected_final": ["audio_data", "action",
                          "text", "action_hint", "expression"],
    },
    # Vtuber config (danmaku input)
    "unity_chan_live": {
        "input_type": "danmaku",
        "expected_streaming": ["text"],
        "expected_final": ["audio_data", "action"],
    },
}

# Skip webrtc for now (needs special audio_collector setup)
SKIP_CONFIGS = {"unity_chan_webrtc"}


async def generate_test_audio(text="你好，今天天气怎么样"):
    """Use TTS server to generate test audio for ASR input."""
    async with httpx.AsyncClient(timeout=30) as http:
        r = await http.post(f"{TTS_API}/audio/speech", json={
            "model": "tts",
            "input": text,
            "voice": "test_cn",
        })
        if r.status_code == 200:
            audio_b64 = base64.b64encode(r.content).decode()
            print(f"  Generated test audio: {len(r.content)} bytes")
            return audio_b64
        else:
            print(f"  TTS error: {r.status_code} {r.text[:200]}")
            return None


async def test_config(config_name, config_spec):
    """Test a single pipeline config end-to-end."""
    client_id = f"e2e_test_{config_name}_{int(time.time())}"
    input_type = config_spec["input_type"]
    expected_streaming = config_spec["expected_streaming"]
    expected_final = config_spec["expected_final"]

    print(f"\n{'='*70}")
    print(f"CONFIG: {config_name} (input: {input_type})")
    print(f"{'='*70}")

    # Register
    async with httpx.AsyncClient(timeout=30) as http:
        r = await http.post(f"{SERVER}/register/", json={"client_id": client_id})
        if r.status_code != 200:
            print(f"  FAIL: Register failed: {r.status_code} {r.text}")
            return False

        # Init pipeline
        r = await http.post(f"{SERVER}/init_pipeline/{client_id}",
                           json={"config": config_name, "force": True})
        if r.status_code != 200:
            print(f"  FAIL: Init failed: {r.status_code} {r.text}")
            await http.post(f"{SERVER}/unregister/", json={"client_id": client_id})
            return False

        resp = r.json()
        if resp.get("status") != "initialized":
            print(f"  FAIL: Init status: {resp}")
            await http.post(f"{SERVER}/unregister/", json={"client_id": client_id})
            return False

    print(f"  Pipeline initialized, waiting 5s...")
    await asyncio.sleep(5)

    # Prepare input message
    if input_type == "text":
        input_msg = {"text": "你好啊，今天天气怎么样", "timestamp": time.time()}
    elif input_type == "audio":
        audio_b64 = await generate_test_audio("你好，今天天气怎么样")
        if not audio_b64:
            print(f"  FAIL: Could not generate test audio")
            async with httpx.AsyncClient() as http:
                await http.post(f"{SERVER}/unregister/", json={"client_id": client_id})
            return False
        input_msg = {"audio_file": audio_b64, "timestamp": time.time()}
    elif input_type == "danmaku":
        input_msg = {
            "text": "今天直播好有趣",
            "user": "测试用户",
            "msg_type": "danmaku",
            "guard_level": 0,
            "num": 0,
            "price": 0,
            "timestamp": time.time(),
        }

    # Connect and send
    collected_streaming = {}  # field -> list of values
    collected_final = {}  # field -> value (from non-streaming messages)
    all_messages = []
    success = True

    try:
        async with websockets.connect(f"{WS_URL}/{client_id}", max_size=None) as ws:
            await ws.send(json.dumps(input_msg))
            print(f"  Sent {input_type} input")

            # For danmaku, might need to send more messages and wait longer
            if input_type == "danmaku":
                await asyncio.sleep(0.5)
                # Send a few more to trigger batch
                for extra in ["好厉害啊", "666", "来首歌吧"]:
                    await ws.send(json.dumps({
                        "text": extra, "user": f"用户{extra}",
                        "msg_type": "danmaku", "guard_level": 0,
                        "num": 0, "price": 0, "timestamp": time.time(),
                    }))
                    await asyncio.sleep(0.2)

            started = False
            timeout = 120  # generous timeout for motion gen
            start_time = time.time()

            while time.time() - start_time < timeout:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=5)
                    data = json.loads(msg)
                    all_messages.append(data)
                    signal = data.get("signal", "")

                    if signal == "SoS":
                        started = True
                        continue
                    if signal == "EoS":
                        # After EoS, wait a bit for remaining non-streaming messages
                        try:
                            while True:
                                msg = await asyncio.wait_for(ws.recv(), timeout=10)
                                data = json.loads(msg)
                                all_messages.append(data)
                                for key in expected_final:
                                    if key in data and data[key]:
                                        collected_final[key] = data[key]
                        except asyncio.TimeoutError:
                            pass
                        break

                    # Collect streaming text
                    if started:
                        text = data.get("text", "")
                        if text:
                            collected_streaming.setdefault("text", []).append(text)

                    # Collect non-streaming fields (action, expression, audio, etc.)
                    for key in expected_final:
                        if key in data and data[key]:
                            collected_final[key] = data[key]

                except asyncio.TimeoutError:
                    if started:
                        # Already started, might be waiting for slow motion gen
                        continue
                    else:
                        print(f"  FAIL: No response within timeout")
                        success = False
                        break

    except Exception as e:
        print(f"  FAIL: WebSocket error: {e}")
        success = False

    # Unregister
    async with httpx.AsyncClient(timeout=10) as http:
        await http.post(f"{SERVER}/unregister/", json={"client_id": client_id})

    if not success:
        return False

    # Verify streaming fields
    full_text = "".join(collected_streaming.get("text", []))
    print(f"  Streamed text: {full_text[:100]}{'...' if len(full_text) > 100 else ''}")

    for field in expected_streaming:
        if field == "text" and not full_text:
            print(f"  FAIL: Missing streaming field: {field}")
            success = False

    # Verify final fields
    print(f"  Final fields received: {list(collected_final.keys())}")
    for field in expected_final:
        if field not in collected_final:
            print(f"  FAIL: Missing final field: {field}")
            success = False
        else:
            val = collected_final[field]
            if isinstance(val, str) and len(val) > 80:
                print(f"  {field}: {val[:40]}...({len(val)} chars)")
            else:
                print(f"  {field}: {val}")

    if success:
        print(f"  PASS ✓")
    else:
        print(f"  FAIL ✗")
        print(f"  All messages ({len(all_messages)}):")
        for i, m in enumerate(all_messages[:20]):
            txt = json.dumps(m, ensure_ascii=False)
            print(f"    [{i}] {txt[:150]}")

    return success


async def main():
    configs_to_test = [c for c in CONFIGS if c not in SKIP_CONFIGS]

    # Optional: test only specific configs from command line
    if len(sys.argv) > 1:
        configs_to_test = [c for c in sys.argv[1:] if c in CONFIGS]

    print(f"Testing {len(configs_to_test)} configs: {configs_to_test}")
    print(f"Skipping: {SKIP_CONFIGS}")

    results = {}
    for config_name in configs_to_test:
        try:
            ok = await test_config(config_name, CONFIGS[config_name])
            results[config_name] = ok
        except Exception as e:
            print(f"  ERROR: {e}")
            results[config_name] = False

    # Summary
    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}")
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    for name, ok in results.items():
        status = "PASS ✓" if ok else "FAIL ✗"
        print(f"  {name}: {status}")
    print(f"\n{passed}/{total} passed")
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
