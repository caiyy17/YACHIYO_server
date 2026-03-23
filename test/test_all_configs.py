"""
Test all pipeline configs: standard + SMPL, measure per-stage latency.
Reuses test_latency.py functions.
"""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from test_latency import run_ws_pipeline, parse_log_timing, compute_latencies, load_audio
import asyncio
import statistics

SERVER = "http://localhost:8910"
ROUNDS = 5  # first round warmup, use last 4


async def benchmark_config(config_name, label, rounds=ROUNDS):
    audio_b64 = load_audio()
    all_lat = []

    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"  Config: {config_name}, Rounds: {rounds} (first=warmup)")
    print(f"{'='*60}")

    for i in range(rounds):
        client_id = f"bench_{config_name}_{i}"
        try:
            results, log = await run_ws_pipeline(client_id, config_name, audio_b64, timeout=30)
        except Exception as e:
            print(f"  Round {i+1}: ERROR - {e}")
            continue

        first_audio_time = None
        eos_time = None
        for r in results:
            if r["has_audio"] and first_audio_time is None:
                first_audio_time = r["time"]
            if r["signal"] == "EoS":
                eos_time = r["time"]

        timing = parse_log_timing(log)
        lat = compute_latencies(timing)
        lat["e2e_first_audio"] = first_audio_time
        lat["e2e_total"] = eos_time
        audio_count = sum(1 for r in results if r["has_audio"])

        all_lat.append(lat)
        is_warmup = " (warmup)" if i == 0 else ""
        print(f"\n  Round {i+1}{is_warmup}:")
        print(f"    ASR:              {lat.get('asr', 0)*1000:7.0f} ms")
        print(f"    LLM first sent:   {lat.get('llm_first_token', 0)*1000:7.0f} ms")
        print(f"    LLM total:        {lat.get('llm_total', 0)*1000:7.0f} ms")
        print(f"    TTS 1st sentence: {lat.get('tts_first_sentence', 0)*1000:7.0f} ms")
        print(f"    Server 1st audio: {lat.get('first_audio_server', 0)*1000:7.0f} ms")
        print(f"    E2E first audio:  {(first_audio_time or 0)*1000:7.0f} ms")
        print(f"    E2E total:        {(eos_time or 0)*1000:7.0f} ms")
        print(f"    Audio chunks:     {audio_count}")

    # Average excluding warmup (round 0)
    valid = all_lat[1:] if len(all_lat) > 1 else all_lat
    print(f"\n  --- Average ({len(valid)} rounds, excl. warmup) ---")
    keys = ["asr", "llm_first_token", "llm_total", "tts_first_sentence",
            "first_audio_server", "e2e_first_audio", "e2e_total"]
    avg = {}
    for k in keys:
        vals = [l.get(k) for l in valid if l.get(k) is not None and l.get(k) > 0]
        if vals:
            m = statistics.mean(vals)
            s = statistics.stdev(vals) if len(vals) > 1 else 0
            avg[k] = (m, s)
            print(f"    {k:22s}: {m*1000:7.0f} ±{s*1000:3.0f} ms")
    return avg


async def main():
    import subprocess
    # Get VRAM snapshot
    r = subprocess.run(
        ["nvidia-smi", "--query-compute-apps=pid,used_memory,name", "--format=csv,noheader"],
        capture_output=True, text=True
    )
    print("GPU Processes:")
    print(r.stdout.strip())
    r2 = subprocess.run(
        ["nvidia-smi", "--query-gpu=memory.used,memory.free", "--format=csv,noheader"],
        capture_output=True, text=True
    )
    print(f"GPU Memory: {r2.stdout.strip()}")

    configs = [
        ("unity_chan", "Standard Pipeline"),
        ("unity_chan_smpl", "SMPL Pipeline"),
    ]

    for config_name, label in configs:
        try:
            await benchmark_config(config_name, label)
        except Exception as e:
            print(f"  Config {config_name} FAILED: {e}")


if __name__ == "__main__":
    asyncio.run(main())
