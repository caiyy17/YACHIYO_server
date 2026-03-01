"""
Latency benchmark for the paper evaluation section.
Tests: local pipeline, OpenAI API pipeline, WebRTC pipeline.
Measures per-stage and end-to-end latency.
"""
import requests
import asyncio
import websockets
import json
import time
import base64
import re
import statistics

SERVER = "http://localhost:8000"
WS_URL = "ws://localhost:8000/ws"
ROUNDS = 3


def load_audio():
    with open("test/test_voice.wav", "rb") as f:
        return base64.b64encode(f.read()).decode()


async def run_ws_pipeline(client_id, config_name, audio_b64, timeout=20):
    """Run a WebSocket pipeline and return timing data."""
    requests.post(f"{SERVER}/register/", json={"client_id": client_id})
    requests.post(
        f"{SERVER}/init_pipeline/{client_id}",
        json={"config": config_name, "force": True},
    )
    await asyncio.sleep(2)  # wait for init (including TTS model loading)

    msg = json.dumps({"audio_file": audio_b64, "timestamp": time.time()})
    start = time.time()
    results = []

    try:
        async with websockets.connect(
            f"{WS_URL}/{client_id}", max_size=16 * 1024 * 1024
        ) as ws:
            await ws.send(msg)
            while True:
                r = await asyncio.wait_for(ws.recv(), timeout=timeout)
                elapsed = time.time() - start
                d = json.loads(r)
                sig = d.get("signal", "")
                has_audio = "audio_data" in d and len(d.get("audio_data", "")) > 100
                results.append(
                    {"time": elapsed, "signal": sig, "has_audio": has_audio}
                )
                if sig == "EoS":
                    break
    except asyncio.TimeoutError:
        pass

    # Parse log for per-stage timing
    log = requests.get(f"{SERVER}/logs/{client_id}").json().get("log_content", "")
    requests.post(f"{SERVER}/unregister/", json={"client_id": client_id})

    return results, log


def parse_log_timing(log):
    """Extract per-stage timestamps from log."""
    timing = {}
    for line in log.split("\n"):
        ts_match = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})", line)
        if not ts_match:
            continue
        ts_str = ts_match.group(1)
        # Parse to seconds
        from datetime import datetime

        ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S,%f").timestamp()

        if "ASRStep: processing data" in line:
            timing["asr_start"] = ts
        elif "ASRStep" in line and "output data" in line:
            timing["asr_end"] = ts
        elif "OpenaiStep: processing data" in line:
            timing["llm_start"] = ts
        elif "OpenaiStep" in line and "output data" in line:
            if "llm_first_output" not in timing:
                timing["llm_first_output"] = ts
            timing["llm_last_output"] = ts
        elif "TTSStep: processing data" in line:
            if "tts_first_start" not in timing:
                timing["tts_first_start"] = ts
        elif "TTSStep" in line and "output data" in line:
            if "tts_first_end" not in timing:
                timing["tts_first_end"] = ts
            timing["tts_last_end"] = ts

    return timing


def compute_latencies(timing):
    """Compute per-stage latencies from timestamps."""
    lat = {}
    if "asr_start" in timing and "asr_end" in timing:
        lat["asr"] = timing["asr_end"] - timing["asr_start"]
    if "llm_start" in timing and "llm_first_output" in timing:
        lat["llm_first_token"] = timing["llm_first_output"] - timing["llm_start"]
    if "llm_start" in timing and "llm_last_output" in timing:
        lat["llm_total"] = timing["llm_last_output"] - timing["llm_start"]
    if "tts_first_start" in timing and "tts_first_end" in timing:
        lat["tts_first_sentence"] = timing["tts_first_end"] - timing["tts_first_start"]
    if "asr_start" in timing and "tts_first_end" in timing:
        lat["first_audio_server"] = timing["tts_first_end"] - timing["asr_start"]
    return lat


async def benchmark_ws(config_name, label):
    audio_b64 = load_audio()
    all_latencies = []
    all_e2e = []

    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"  Config: {config_name}, Rounds: {ROUNDS}")
    print(f"{'='*60}")

    for i in range(ROUNDS):
        client_id = f"bench_{config_name}_{i}"
        results, log = await run_ws_pipeline(client_id, config_name, audio_b64)

        # E2E from client perspective
        first_audio_time = None
        eos_time = None
        for r in results:
            if r["has_audio"] and first_audio_time is None:
                first_audio_time = r["time"]
            if r["signal"] == "EoS":
                eos_time = r["time"]

        # Per-stage from log
        timing = parse_log_timing(log)
        lat = compute_latencies(timing)
        lat["e2e_first_audio"] = first_audio_time
        lat["e2e_total"] = eos_time

        all_latencies.append(lat)
        audio_count = sum(1 for r in results if r["has_audio"])

        print(f"\n  Round {i+1}:")
        print(f"    ASR:              {lat.get('asr', 0)*1000:7.0f} ms")
        print(f"    LLM first token:  {lat.get('llm_first_token', 0)*1000:7.0f} ms")
        print(f"    LLM total:        {lat.get('llm_total', 0)*1000:7.0f} ms")
        print(f"    TTS 1st sentence: {lat.get('tts_first_sentence', 0)*1000:7.0f} ms")
        print(f"    Server 1st audio: {lat.get('first_audio_server', 0)*1000:7.0f} ms")
        print(f"    E2E first audio:  {first_audio_time*1000:7.0f} ms")
        print(f"    E2E total:        {eos_time*1000:7.0f} ms")
        print(f"    Audio chunks:     {audio_count}")

    # Compute averages
    print(f"\n  --- Average ({ROUNDS} rounds) ---")
    keys = ["asr", "llm_first_token", "llm_total", "tts_first_sentence",
            "first_audio_server", "e2e_first_audio", "e2e_total"]
    avg = {}
    for k in keys:
        vals = [l.get(k, 0) for l in all_latencies if l.get(k)]
        if vals:
            avg[k] = statistics.mean(vals)
            std = statistics.stdev(vals) if len(vals) > 1 else 0
            print(f"    {k:22s}: {avg[k]*1000:7.0f} ms (±{std*1000:.0f})")

    return avg


async def benchmark_webrtc():
    """Test WebRTC pipeline by initializing and sending via WebSocket (measures server-side latency)."""
    audio_b64 = load_audio()
    print(f"\n{'='*60}")
    print(f"  WebRTC Pipeline (unity_chan_webrtc)")
    print(f"  Config: unity_chan_webrtc, Rounds: {ROUNDS}")
    print(f"  Note: WebRTC-specific framing overhead measured via log")
    print(f"{'='*60}")

    # For WebRTC, we use the same WebSocket test but with the webrtc config
    # This tests the server-side pipeline including AudioCollector and FrameSplitter
    # The actual WebRTC transport latency would need a real WebRTC client
    all_latencies = []

    for i in range(ROUNDS):
        client_id = f"bench_webrtc_{i}"
        results, log = await run_ws_pipeline(
            client_id, "unity_chan_webrtc", audio_b64, timeout=25
        )

        timing = parse_log_timing(log)
        lat = compute_latencies(timing)

        # Check for FrameSplitter timing
        for line in log.split("\n"):
            ts_match = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})", line)
            if not ts_match:
                continue
            ts_str = ts_match.group(1)
            from datetime import datetime
            ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S,%f").timestamp()
            if "FrameSplitterStep: processing data" in line:
                if "frame_split_start" not in timing:
                    timing["frame_split_start"] = ts
            elif "FrameSplitterStep" in line and "output data" in line:
                if "frame_split_first_end" not in timing:
                    timing["frame_split_first_end"] = ts

        if "frame_split_start" in timing and "frame_split_first_end" in timing:
            lat["frame_split"] = timing["frame_split_first_end"] - timing["frame_split_start"]

        first_output = None
        for r in results:
            if r["has_audio"] or (r["signal"] and r["signal"] not in ("SoS", "")):
                if first_output is None and r["has_audio"]:
                    first_output = r["time"]
            if r["signal"] == "EoS":
                lat["e2e_total"] = r["time"]
        lat["e2e_first_audio"] = first_output

        all_latencies.append(lat)

        print(f"\n  Round {i+1}:")
        print(f"    ASR:              {lat.get('asr', 0)*1000:7.0f} ms")
        print(f"    LLM first token:  {lat.get('llm_first_token', 0)*1000:7.0f} ms")
        print(f"    TTS 1st sentence: {lat.get('tts_first_sentence', 0)*1000:7.0f} ms")
        print(f"    FrameSplitter:    {lat.get('frame_split', 0)*1000:7.0f} ms")
        print(f"    Server 1st audio: {lat.get('first_audio_server', 0)*1000:7.0f} ms")

    # Averages
    print(f"\n  --- Average ({ROUNDS} rounds) ---")
    keys = ["asr", "llm_first_token", "llm_total", "tts_first_sentence",
            "frame_split", "first_audio_server", "e2e_first_audio", "e2e_total"]
    for k in keys:
        vals = [l.get(k, 0) for l in all_latencies if l.get(k)]
        if vals:
            avg = statistics.mean(vals)
            std = statistics.stdev(vals) if len(vals) > 1 else 0
            print(f"    {k:22s}: {avg*1000:7.0f} ms (±{std*1000:.0f})")


async def benchmark_multi_user():
    """Test concurrent users sharing services."""
    audio_b64 = load_audio()
    N = 3
    print(f"\n{'='*60}")
    print(f"  Multi-User Concurrency Test ({N} users)")
    print(f"{'='*60}")

    # Register all clients
    for i in range(N):
        requests.post(f"{SERVER}/register/", json={"client_id": f"multi_{i}"})
        requests.post(
            f"{SERVER}/init_pipeline/multi_{i}",
            json={"config": "unity_chan", "force": True},
        )
    await asyncio.sleep(2)

    async def single_user(idx):
        client_id = f"multi_{idx}"
        msg = json.dumps({"audio_file": audio_b64, "timestamp": time.time()})
        start = time.time()
        first_audio = None
        eos = None
        try:
            async with websockets.connect(
                f"{WS_URL}/{client_id}", max_size=16 * 1024 * 1024
            ) as ws:
                await ws.send(msg)
                while True:
                    r = await asyncio.wait_for(ws.recv(), timeout=30)
                    elapsed = time.time() - start
                    d = json.loads(r)
                    if "audio_data" in d and len(d.get("audio_data", "")) > 100:
                        if first_audio is None:
                            first_audio = elapsed
                    if d.get("signal") == "EoS":
                        eos = elapsed
                        break
        except asyncio.TimeoutError:
            pass
        return {"user": idx, "first_audio": first_audio, "total": eos}

    # Run all users concurrently
    tasks = [single_user(i) for i in range(N)]
    results = await asyncio.gather(*tasks)

    for r in results:
        fa = r["first_audio"]
        tot = r["total"]
        print(f"  User {r['user']}: first_audio={fa*1000:.0f}ms, total={tot*1000:.0f}ms")

    avg_fa = statistics.mean([r["first_audio"] for r in results if r["first_audio"]])
    avg_tot = statistics.mean([r["total"] for r in results if r["total"]])
    print(f"  Average: first_audio={avg_fa*1000:.0f}ms, total={avg_tot*1000:.0f}ms")

    # Cleanup
    for i in range(N):
        requests.post(f"{SERVER}/unregister/", json={"client_id": f"multi_{i}"})


async def main():
    print("=" * 60)
    print("  YACHIO Latency Benchmark")
    print("  GPU: NVIDIA RTX 5090 (single card, all local services)")
    print("=" * 60)

    # 1. Local pipeline
    avg_local = await benchmark_ws(
        "unity_chan", "Test 1: Full Local (SenseVoice + Qwen + BertVITS)"
    )

    # 2. OpenAI API pipeline
    avg_openai = await benchmark_ws(
        "unity_chan_openai", "Test 2: Full OpenAI API (Whisper + GPT-4.1 + TTS-1)"
    )

    # 3. WebRTC pipeline
    await benchmark_webrtc()

    # 4. Multi-user concurrency
    await benchmark_multi_user()

    # Summary table
    print(f"\n{'='*60}")
    print("  SUMMARY TABLE (averages, ms)")
    print(f"{'='*60}")
    print(f"  {'Stage':<22} {'Local':>10} {'OpenAI API':>10}")
    print(f"  {'-'*22} {'-'*10} {'-'*10}")
    for k in ["asr", "llm_first_token", "llm_total", "tts_first_sentence",
              "first_audio_server", "e2e_first_audio", "e2e_total"]:
        local_v = avg_local.get(k, 0) * 1000
        openai_v = avg_openai.get(k, 0) * 1000
        print(f"  {k:<22} {local_v:>10.0f} {openai_v:>10.0f}")


if __name__ == "__main__":
    asyncio.run(main())
