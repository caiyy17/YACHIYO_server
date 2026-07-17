"""
Consolidated connection/stress test harness.

Select a mode with --mode:
  services      Connectivity check for all dependent services (main server,
                gateway, model backends) before running heavier modes.
  concurrent    Server connection / API lifecycle test (register/unregister/
                get_clients/get_client/init_pipeline/get_client_log/websocket
                + concurrency). Requires a running server.
  latency       Latency benchmark: local, OpenAI API, WebRTC pipelines and
                multi-user concurrency, with per-stage timings. Requires a
                running server.
  backpressure  Standalone max_queue_size backpressure test; builds pipelines
                directly (imports from Modules). Does NOT need a running server.

Mode-specific helpers/constants are namespaced to avoid collisions.
"""

import argparse
import sys
import os

# Needed for backpressure mode, which builds pipelines directly from Modules.
# Kept at module level to mirror the original test_backpressure.py behavior.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import asyncio
import websockets
import json
import time
import base64
import re
import statistics
import threading
import logging
from queue import Queue, Empty

from Modules.base.BaseProcessingStep import BaseProcessingStep


# ══════════════════════════════════════════════════════════════════════════
# MODE: concurrent  (from test/test_concurrent.py)
# ══════════════════════════════════════════════════════════════════════════

# FastAPI server address
server_url = "http://localhost:8910"
websocket_url = "ws://localhost:8910/ws"


# Test POST request for client registration
def test_post_register(client_id):
    url = f"{server_url}/register/"
    data = {"client_id": client_id}
    headers = {
        "Content-Type": "application/json"
    }  # Content-Type header must be set to application/json when sending JSON data
    try:
        response = requests.post(
            url, json=data, headers=headers
        )  # Use json parameter to send JSON data
        if response.status_code == 200:
            print(f"POST /register/: {response.json()}")
        else:
            print(f"POST /register/ failed: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")


# Test client unregistration
def test_post_unregister(client_id):
    url = f"{server_url}/unregister/"
    data = {"client_id": client_id}
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(url, json=data, headers=headers)
        if response.status_code == 200:
            print(f"POST /unregister/: {response.json()}")
        else:
            print(f"POST /unregister/ failed: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")


# Test unregistering an unregistered client
def test_post_unregister_unregistered(client_id):
    url = f"{server_url}/unregister/"
    data = {"client_id": client_id}
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(url, json=data, headers=headers)
        if response.status_code == 400:
            print(f"POST /unregister/ unregistered client: {response.json()}")
        else:
            print(f"POST /unregister/ failed: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")


# Test getting all clients list
def test_get_clients():
    url = f"{server_url}/clients/"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            print(f"GET /clients/: {response.json()}")
        else:
            print(f"GET /clients/ failed: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")


# Test getting single client info
def test_get_client(client_id):
    url = f"{server_url}/clients/{client_id}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            print(f"GET /clients/{client_id}: {response.json()}")
        else:
            print(
                f"GET /clients/{client_id} failed: {response.status_code}, {response.text}"
            )
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")


# Test pipeline initialization
def test_init_pipeline(client_id, pipeline_config, force=False):
    url = f"{server_url}/init_pipeline/{client_id}"
    config_data = {"config": pipeline_config, "force": force}
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(url, json=config_data, headers=headers)
        if response.status_code == 200:
            print(f"POST /init_pipeline/: {response.json()}")
        else:
            print(
                f"POST /init_pipeline/ failed: {response.status_code}, {response.text}"
            )
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")


# Test getting client logs
def test_get_client_log(client_id):
    url = f"{server_url}/logs/{client_id}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            print(f"GET /logs/{client_id}: {response.json()}")
        else:
            print(
                f"GET /logs/{client_id} failed: {response.status_code}, {response.text}"
            )
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")


# Test WebSocket connection
async def test_websocket(
    client_id, process_func, messages=[], repeat=1, interval=0, timeout=10
):
    start = time.time()
    try:
        async with websockets.connect(
            f"{websocket_url}/{client_id}", max_size=1024 * 1024 * 16
        ) as websocket:
            # Send messages to server
            print(f"start time: {time.time() - start}")
            for i in range(repeat):
                for message in messages:
                    await websocket.send(message)
                    print(f"send time {i}: {time.time() - start}")
                    # Truncate to first 100 characters if too long
                    if len(message) > 100:
                        message = message[:100] + "..."
                    print(f"Sent: {message}")
                    await asyncio.sleep(interval)

            # Receive server responses
            index = 0
            while True:
                response = await asyncio.wait_for(websocket.recv(), timeout=timeout)
                print(f"Receive time {index} : {time.time() - start}")
                index += 1
                try:
                    process_func(response)
                except Exception as e:
                    print(f"Error processing response: {e}")
                # Truncate to first 100 characters if too long
                if len(response) > 100:
                    response = response[:100] + "..."
                print(f"Received: {response}")
    except asyncio.TimeoutError:
        print("WebSocket receive timeout.")

    except websockets.exceptions.ConnectionClosedError as e:
        print(f"WebSocket connection closed unexpectedly: {e}")
    except websockets.exceptions.InvalidStatusCode as e:
        print(f"WebSocket server returned an invalid status code: {e}")
    except Exception as e:
        print(f"WebSocket error: {e}")
    finally:
        print(f"WebSocket connection closed for client {client_id}.")


def run_concurrent():
    # Remove test/tmp directory if it exists
    if os.path.exists("test/tmp"):
        os.system("rm -rf test/tmp")
    os.mkdir("test/tmp")

    pipeline_config = "unity_chan_default"
    messages = []

    # Add messages
    audio_data = base64.b64encode(open("test/test_voice.wav", "rb").read()).decode(
        "utf-8"
    )
    messages.append(json.dumps({"text": "你好，世界！", "audio_file": audio_data, "timestamp": time.time()}))

    # messages.append(json.dumps({"signal": "cancel", "timestamp": time.time()}))
    # messages.append(json.dumps({"audio_file": audio_data, "timestamp": time.time()}))

    client_id = "test-id-0"
    force = True

    # Test POST register endpoint
    test_post_register(client_id)

    # Test getting clients list
    test_get_clients()

    # Test getting single client
    test_get_client(client_id)

    # Test pipeline initialization
    start = time.time()
    test_init_pipeline(client_id, pipeline_config, force=force)
    print(f"init time: {time.time() - start}")

    # Test getting client logs
    # test_get_client_log(client_id)

    # Test WebSocket connection
    def process_func(response):
        response = json.loads(response)
        timestamp = time.time()
        # Keep 4 decimal places

        # Save audio data to file
        try:
            audio_data = response["audio_data"]
            audio_data = base64.b64decode(audio_data)
            with open(f"test/tmp/output_{timestamp:.4f}.wav", "wb") as file:
                file.write(audio_data)
        except Exception as e:
            print(f"Error saving audio file: {e}")

        # Save response to file
        try:
            with open(f"test/tmp/response_{timestamp:.4f}.json", "w") as file:
                json.dump(response, file, ensure_ascii=False, indent=4)
        except Exception as e:
            print(f"Error saving response file: {e}")

    asyncio.run(
        test_websocket(
            client_id, process_func, messages, repeat=1, interval=0, timeout=10
        )
    )

    # # Test client unregistration
    # test_post_unregister(client_id)

    # # Test unregistering an unregistered client
    # test_post_unregister_unregistered("unregistered-client")

    # num_clients = 10
    # client_ids = []
    # for i in range(num_clients):
    #     client_ids.append(f"test-id-{i + 1}")

    # # Test POST register for multiple clients
    # for i in range(num_clients):
    #     client_id = client_ids[i]
    #     test_post_register(client_id)
    #     test_get_clients()
    #     start = time.time()
    #     test_init_pipeline(client_id, pipeline_config, force=True)
    #     print(f"init time: {time.time() - start}")

    # async def main():
    #     tasks = []
    #     for client_id in client_ids:
    #         print(f"Start testing WebSocket for client {client_id}.")
    #         task = asyncio.create_task(
    #             test_websocket(
    #                 client_id, process_func, messages, repeat=1, interval=0, timeout=5
    #             )
    #         )
    #         await asyncio.sleep(0.1)
    #         tasks.append(task)
    #     await asyncio.gather(*tasks)

    # asyncio.run(main())


# ══════════════════════════════════════════════════════════════════════════
# MODE: latency  (from test/test_latency.py)
# ══════════════════════════════════════════════════════════════════════════

SERVER = "http://localhost:8910"
WS_URL = "ws://localhost:8910/ws"
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
            # skip SoS/EoS signal frames and the destination=-1 prompt echo;
            # count only real token output
            if "'signal'" in line or "'destination': -1" in line:
                continue
            if "llm_first_output" not in timing:
                timing["llm_first_output"] = ts
            timing["llm_last_output"] = ts
        elif "TTSStep: processing data" in line:
            if "tts_first_start" not in timing:
                timing["tts_first_start"] = ts
        elif "TTSStep" in line and "output data" in line:
            if "'signal'" in line:  # skip SoS/EoS signal frames, count only real audio output
                continue
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
            json={"config": "unity_chan_default", "force": True},
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


async def latency_main():
    print("=" * 60)
    print("  YACHIYO Latency Benchmark")
    print("  GPU: NVIDIA RTX 5090 (single card, all local services)")
    print("=" * 60)

    # 1. unity_chan_default: Qwen3-ASR (local) + gemma (remote vLLM) + Qwen3-TTS (local)
    avg_local = await benchmark_ws(
        "unity_chan_default", "Test 1: unity_chan_default (Qwen3-ASR + gemma + Qwen3-TTS)"
    )

    # 2. demo: full OpenAI — Whisper (ASR) + GPT (LLM) + OpenAI TTS-1, all remote
    avg_openai = await benchmark_ws(
        "demo", "Test 2: demo (full OpenAI: Whisper + GPT + TTS-1)"
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


def run_latency():
    asyncio.run(latency_main())


# ══════════════════════════════════════════════════════════════════════════
# MODE: backpressure  (from test/test_backpressure.py)
# ══════════════════════════════════════════════════════════════════════════

# ── Simulated Modules ─────────────────────────────────────────

class StreamProducer(BaseProcessingStep):
    """Simulates LLM streaming: one input -> multiple outputs with delay."""
    def custom_init(self):
        self.num_outputs = self.get_config("num_outputs", 5)
        self.output_delay = self.get_config("output_delay", 0.05)

    def process(self, data, pass_data={}):
        self.output_to_queue({"signal": "SoS"}, pass_data)
        for i in range(self.num_outputs):
            if self.check_cancel():
                self.logger.info("cancelled during streaming")
                break
            time.sleep(self.output_delay)
            output_data = {}
            self.add_output(output_data, "text", f"chunk_{i}")
            self.output_to_queue(output_data, pass_data)
        self.output_to_queue({"signal": "EoS"}, pass_data)


class SlowConsumer(BaseProcessingStep):
    """Simulates TTS: slow processing per item."""
    def custom_init(self):
        self.process_time = self.get_config("process_time", 0.3)

    def process(self, data, pass_data={}):
        time.sleep(self.process_time)
        output_data = {}
        self.add_output(output_data, "result", f"done_{data.get('text', '')}")
        self.output_to_queue(output_data, pass_data)


# ── Utilities ─────────────────────────────────────────────────

def setup_logger(name="bp_test"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(h)
    return logger


class QueueMonitor:
    """Polls qsize() to track peak queue depth."""
    def __init__(self, q, interval=0.005):
        self.q = q
        self.interval = interval
        self.max_size = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=2)

    def _run(self):
        while not self._stop.is_set():
            s = self.q.qsize()
            if s > self.max_size:
                self.max_size = s
            self._stop.wait(self.interval)


def build_pipeline(node_specs, logger):
    """
    Build a pipeline like setup_processing_pipeline does.
    node_specs: [(class, node_id, config_dict), ...]
    Returns dict with queues, threads, etc.
    """
    send_queue = Queue()

    n = len(node_specs)
    queues = []
    cancel_queues = []

    for i in range(n):
        mqs = node_specs[i][2].get("max_queue_size", 0)
        queues.append(Queue(maxsize=mqs))
        cancel_queues.append(Queue())
    queues.append(send_queue)

    threads = []
    for i, (cls, node_id, config) in enumerate(node_specs):
        inst = cls(
            node_id, "test", logger,
            send_queue, queues[i], queues[i + 1],
            cancel_queues[i], config,
        )
        t = threading.Thread(target=inst.run, daemon=True, name=f"node_{node_id}")
        threads.append(t)

    return {
        "queues": queues,
        "cancel_queues": cancel_queues,
        "send_queue": send_queue,
        "threads": threads,
    }


def start_pipeline(p):
    for t in p["threads"]:
        t.start()


def stop_pipeline(p, timeout=5):
    for cq in p["cancel_queues"]:
        cq.put(json.dumps({"signal": "cancel", "timestamp": float("inf")}))
        cq.put(json.dumps({"signal": "kill"}))
    deadline = time.time() + timeout
    for t in p["threads"]:
        remaining = max(0.1, deadline - time.time())
        t.join(timeout=remaining)
    return [t.name for t in p["threads"] if t.is_alive()]


def send_input(p, text, ts=None):
    if ts is None:
        ts = time.time()
    p["queues"][0].put(json.dumps({"text": text, "timestamp": ts}))
    return ts


def send_cancel(p, ts):
    msg = json.dumps({"signal": "cancel", "timestamp": ts})
    for cq in p["cancel_queues"]:
        cq.put(msg)


def collect_outputs(send_queue, eos_count=1, timeout=30):
    """Collect from send_queue until eos_count EoS signals received or timeout."""
    results = []
    seen_eos = 0
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            data = send_queue.get(timeout=0.5)
            d = json.loads(data)
            results.append(d)
            if d.get("signal") == "EoS":
                seen_eos += 1
                if seen_eos >= eos_count:
                    break
        except Empty:
            pass
    return results


# ── Tests ─────────────────────────────────────────────────────

def test_basic_backpressure():
    """Backpressure limits queue size; all messages delivered."""
    print("\n" + "=" * 60)
    print("  Test 1: Basic backpressure")
    print("=" * 60)

    logger = setup_logger("bp_test_1")
    NUM_CHUNKS = 10
    MAX_Q = 3

    p = build_pipeline([
        (StreamProducer, 1, {
            "num_outputs": NUM_CHUNKS,
            "output_delay": 0.02,
            "output_vars": [{"source": "text", "target": "1_text"}],
            "next_nodes": [3],
        }),
        (SlowConsumer, 3, {
            "max_queue_size": MAX_Q,
            "process_time": 0.15,
            "input_vars": [{"source": "1_text", "target": "text"}],
            "output_vars": [{"source": "result", "target": "result"}],
            "pass_signals": [{"source": "SoS", "target": "SoS"},
                             {"source": "EoS", "target": "EoS"}],
            "next_nodes": [-1],
        }),
    ], logger)

    monitor = QueueMonitor(p["queues"][1])
    monitor.start()
    start_pipeline(p)

    t0 = time.time()
    send_input(p, "hello")
    results = collect_outputs(p["send_queue"])
    elapsed = time.time() - t0
    monitor.stop()

    signals = [r for r in results if r.get("signal")]
    data_msgs = [r for r in results if not r.get("signal")]
    sos = sum(1 for s in signals if s["signal"] == "SoS")
    eos = sum(1 for s in signals if s["signal"] == "EoS")

    print(f"  Data messages:     {len(data_msgs)} (expected {NUM_CHUNKS})")
    print(f"  Signals:           SoS={sos} EoS={eos}")
    print(f"  Queue peak:        {monitor.max_size} (limit {MAX_Q})")
    print(f"  Elapsed:           {elapsed:.2f}s")

    ok = True
    if len(data_msgs) != NUM_CHUNKS:
        print(f"  FAIL: expected {NUM_CHUNKS} data msgs, got {len(data_msgs)}")
        ok = False
    if monitor.max_size > MAX_Q:
        print(f"  FAIL: queue peak {monitor.max_size} > max_queue_size {MAX_Q}")
        ok = False
    if sos != 1 or eos != 1:
        print(f"  FAIL: expected 1 SoS + 1 EoS")
        ok = False

    alive = stop_pipeline(p)
    if alive:
        print(f"  FAIL: threads still alive: {alive}")
        ok = False

    print(f"  {'PASS' if ok else 'FAIL'}")
    return ok


def test_no_backpressure_baseline():
    """Without backpressure, queue grows beyond MAX_Q (proves test 1 is meaningful)."""
    print("\n" + "=" * 60)
    print("  Test 2: No backpressure baseline")
    print("=" * 60)

    logger = setup_logger("bp_test_2")
    NUM_CHUNKS = 10

    p = build_pipeline([
        (StreamProducer, 1, {
            "num_outputs": NUM_CHUNKS,
            "output_delay": 0.02,
            "output_vars": [{"source": "text", "target": "1_text"}],
            "next_nodes": [3],
        }),
        (SlowConsumer, 3, {
            # No max_queue_size — unbounded
            "process_time": 0.15,
            "input_vars": [{"source": "1_text", "target": "text"}],
            "output_vars": [{"source": "result", "target": "result"}],
            "pass_signals": [{"source": "SoS", "target": "SoS"},
                             {"source": "EoS", "target": "EoS"}],
            "next_nodes": [-1],
        }),
    ], logger)

    monitor = QueueMonitor(p["queues"][1])
    monitor.start()
    start_pipeline(p)

    send_input(p, "hello")
    results = collect_outputs(p["send_queue"])
    monitor.stop()

    data_msgs = [r for r in results if not r.get("signal")]

    print(f"  Data messages:     {len(data_msgs)} (expected {NUM_CHUNKS})")
    print(f"  Queue peak:        {monitor.max_size} (unbounded)")

    ok = True
    if len(data_msgs) != NUM_CHUNKS:
        print(f"  FAIL: expected {NUM_CHUNKS} data msgs, got {len(data_msgs)}")
        ok = False
    if monitor.max_size <= 3:
        print(f"  NOTE: queue peak only {monitor.max_size}, expected > 3 without backpressure")

    alive = stop_pipeline(p)
    if alive:
        print(f"  FAIL: threads still alive: {alive}")
        ok = False

    print(f"  {'PASS' if ok else 'FAIL'}")
    return ok


def test_cancel_during_backpressure():
    """Cancel while producer is blocked on put(). Must not hang."""
    print("\n" + "=" * 60)
    print("  Test 3: Cancel during backpressure")
    print("=" * 60)

    logger = setup_logger("bp_test_3")

    p = build_pipeline([
        (StreamProducer, 1, {
            "num_outputs": 50,
            "output_delay": 0.02,
            "output_vars": [{"source": "text", "target": "1_text"}],
            "next_nodes": [3],
        }),
        (SlowConsumer, 3, {
            "max_queue_size": 2,
            "process_time": 0.5,
            "input_vars": [{"source": "1_text", "target": "text"}],
            "output_vars": [{"source": "result", "target": "result"}],
            "pass_signals": [{"source": "SoS", "target": "SoS"},
                             {"source": "EoS", "target": "EoS"}],
            "next_nodes": [-1],
        }),
    ], logger)

    start_pipeline(p)
    ts = send_input(p, "hello")

    # Let it run, then cancel
    time.sleep(1.5)
    send_cancel(p, ts + 0.001)

    results = collect_outputs(p["send_queue"], timeout=8)
    data_msgs = [r for r in results if not r.get("signal")]

    print(f"  Data messages:     {len(data_msgs)} (expected << 50)")

    ok = True
    if len(data_msgs) >= 50:
        print(f"  FAIL: cancel didn't reduce output")
        ok = False

    alive = stop_pipeline(p)
    if alive:
        print(f"  FAIL: threads still alive (hang?): {alive}")
        ok = False

    print(f"  {'PASS' if ok else 'FAIL'}")
    return ok


def test_multiple_inputs():
    """Multiple sequential inputs all processed correctly under backpressure."""
    print("\n" + "=" * 60)
    print("  Test 4: Multiple sequential inputs")
    print("=" * 60)

    logger = setup_logger("bp_test_4")
    NUM_INPUTS = 3
    NUM_CHUNKS = 5
    MAX_Q = 3

    p = build_pipeline([
        (StreamProducer, 1, {
            "num_outputs": NUM_CHUNKS,
            "output_delay": 0.02,
            "output_vars": [{"source": "text", "target": "1_text"}],
            "next_nodes": [3],
        }),
        (SlowConsumer, 3, {
            "max_queue_size": MAX_Q,
            "process_time": 0.08,
            "input_vars": [{"source": "1_text", "target": "text"}],
            "output_vars": [{"source": "result", "target": "result"}],
            "pass_signals": [{"source": "SoS", "target": "SoS"},
                             {"source": "EoS", "target": "EoS"}],
            "next_nodes": [-1],
        }),
    ], logger)

    monitor = QueueMonitor(p["queues"][1])
    monitor.start()
    start_pipeline(p)

    for i in range(NUM_INPUTS):
        send_input(p, f"input_{i}")
        time.sleep(0.05)

    results = collect_outputs(p["send_queue"], eos_count=NUM_INPUTS)
    monitor.stop()

    data_msgs = [r for r in results if not r.get("signal")]
    eos_count = sum(1 for r in results if r.get("signal") == "EoS")

    print(f"  Data messages:     {len(data_msgs)} (expected {NUM_INPUTS * NUM_CHUNKS})")
    print(f"  EoS count:         {eos_count} (expected {NUM_INPUTS})")
    print(f"  Queue peak:        {monitor.max_size} (limit {MAX_Q})")

    ok = True
    if len(data_msgs) != NUM_INPUTS * NUM_CHUNKS:
        print(f"  FAIL: expected {NUM_INPUTS * NUM_CHUNKS} data msgs, got {len(data_msgs)}")
        ok = False
    if eos_count != NUM_INPUTS:
        print(f"  FAIL: expected {NUM_INPUTS} EoS, got {eos_count}")
        ok = False
    if monitor.max_size > MAX_Q:
        print(f"  FAIL: queue peak {monitor.max_size} > {MAX_Q}")
        ok = False

    alive = stop_pipeline(p)
    if alive:
        print(f"  FAIL: threads still alive: {alive}")
        ok = False

    print(f"  {'PASS' if ok else 'FAIL'}")
    return ok


def test_three_stage_pipeline():
    """Backpressure on final stage propagates to mid-stage via output blocking."""
    print("\n" + "=" * 60)
    print("  Test 5: Three-stage pipeline")
    print("=" * 60)

    logger = setup_logger("bp_test_5")
    NUM_CHUNKS = 10
    MAX_Q = 2

    p = build_pipeline([
        (StreamProducer, 1, {
            "num_outputs": NUM_CHUNKS,
            "output_delay": 0.02,
            "output_vars": [{"source": "text", "target": "1_text"}],
            "next_nodes": [3],
        }),
        (SlowConsumer, 3, {
            # Mid-stage: fast processing, but blocks on output to stage 3
            "process_time": 0.02,
            "input_vars": [{"source": "1_text", "target": "text"}],
            "output_vars": [{"source": "result", "target": "3_result"}],
            "pass_signals": [{"source": "SoS", "target": "SoS"},
                             {"source": "EoS", "target": "EoS"}],
            "next_nodes": [5],
        }),
        (SlowConsumer, 5, {
            "max_queue_size": MAX_Q,
            "process_time": 0.2,
            "input_vars": [{"source": "3_result", "target": "text"}],
            "output_vars": [{"source": "result", "target": "result"}],
            "pass_signals": [{"source": "SoS", "target": "SoS"},
                             {"source": "EoS", "target": "EoS"}],
            "next_nodes": [-1],
        }),
    ], logger)

    monitor = QueueMonitor(p["queues"][2])  # final stage's input queue
    monitor.start()
    start_pipeline(p)

    send_input(p, "hello")
    results = collect_outputs(p["send_queue"])
    monitor.stop()

    data_msgs = [r for r in results if not r.get("signal")]

    print(f"  Data messages:     {len(data_msgs)} (expected {NUM_CHUNKS})")
    print(f"  Final queue peak:  {monitor.max_size} (limit {MAX_Q})")

    ok = True
    if len(data_msgs) != NUM_CHUNKS:
        print(f"  FAIL: expected {NUM_CHUNKS} data msgs, got {len(data_msgs)}")
        ok = False
    if monitor.max_size > MAX_Q:
        print(f"  FAIL: final queue peak {monitor.max_size} > {MAX_Q}")
        ok = False

    alive = stop_pipeline(p)
    if alive:
        print(f"  FAIL: threads still alive: {alive}")
        ok = False

    print(f"  {'PASS' if ok else 'FAIL'}")
    return ok


def run_backpressure():
    tests = [
        ("Basic backpressure", test_basic_backpressure),
        ("No backpressure baseline", test_no_backpressure_baseline),
        ("Cancel during backpressure", test_cancel_during_backpressure),
        ("Multiple inputs", test_multiple_inputs),
        ("Three-stage pipeline", test_three_stage_pipeline),
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


# ══════════════════════════════════════════════════════════════════════════
# MODE: services  (connectivity check for all dependencies)
# ══════════════════════════════════════════════════════════════════════════

# Settings file holding the external service addresses (asr/llm/tts/...).
SERVICES_SETTINGS_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "configs", "settings", "settings.json",
)
SERVICES_TIMEOUT = 5  # seconds per probe


def _probe_service(name, base_url, path):
    """
    Probe a single service. "UP" = any HTTP response was received (even 4xx/5xx).
    "DOWN" = connection refused / timeout / DNS error / other request failure.

    Returns a dict with keys: name, url, up, status, error, ms.
    """
    url = base_url.rstrip("/") + path
    start = time.time()
    try:
        resp = requests.get(url, timeout=SERVICES_TIMEOUT)
        ms = (time.time() - start) * 1000
        return {"name": name, "url": url, "up": True,
                "status": resp.status_code, "error": None, "ms": ms}
    except requests.exceptions.RequestException as e:
        ms = (time.time() - start) * 1000
        return {"name": name, "url": url, "up": False,
                "status": None, "error": type(e).__name__, "ms": ms}


def run_services():
    """
    Connectivity check for ALL dependencies. Reads external service addresses
    from configs/settings/settings.json (dynamically), adds the two first-party
    servers, probes each with a short timeout, and reports UP/DOWN per service.
    Returns True only if ALL services are up.
    """
    print("=" * 70)
    print("  YACHIYO Service Connectivity Check")
    print("=" * 70)

    # (name, base_url, probe_path) for every dependency to check.
    probes = []

    # ── External services from settings.json ──────────────────────────
    try:
        with open(SERVICES_SETTINGS_PATH, "r") as f:
            settings = json.load(f)
    except Exception as e:
        print(f"  Failed to read settings: {SERVICES_SETTINGS_PATH}: {e}")
        settings = {}

    for section, entries in settings.items():
        if not isinstance(entries, dict):
            continue
        for key, value in entries.items():
            # Only treat http(s) URLs as service endpoints (skip e.g. datasets_path).
            if not isinstance(value, str) or not value.startswith("http"):
                continue
            url = value.rstrip("/")
            # OpenAI-compatible services already end in /v1 -> probe /models.
            # Others (data_query, motion) -> probe / (any HTTP response = UP).
            path = "/models" if url.endswith("/v1") else "/"
            probes.append((f"{section}.{key}", url, path))

    # ── First-party servers (not in settings.json) ───────────────────
    probes.append(("main_server", "http://localhost:8910", "/clients/"))
    probes.append(("webrtc_server", "http://localhost:15168", "/status"))

    # ── Probe & report ───────────────────────────────────────────────
    results = []
    name_w = max((len(p[0]) for p in probes), default=12)
    for name, base_url, path in probes:
        r = _probe_service(name, base_url, path)
        results.append(r)
        state = "UP  " if r["up"] else "DOWN"
        detail = (f"status={r['status']}" if r["up"]
                  else f"error={r['error']}")
        print(f"  [{state}] {r['name']:<{name_w}}  {r['url']:<45} "
              f"{detail:<22} {r['ms']:6.0f} ms")

    up_count = sum(1 for r in results if r["up"])
    total = len(results)
    print("-" * 70)
    print(f"  Summary: {up_count}/{total} up")
    all_up = up_count == total
    print(f"  {'All services reachable.' if all_up else 'Some services are DOWN.'}")
    return all_up


# ══════════════════════════════════════════════════════════════════════════
# Dispatch
# ══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Consolidated connection/stress test harness."
    )
    parser.add_argument(
        "--mode",
        choices=["services", "concurrent", "latency", "backpressure"],
        default="concurrent",
        help="Test mode: services (connectivity check for all dependencies), "
             "concurrent (server API + websocket), "
             "latency (latency benchmark), backpressure (standalone pipeline test).",
    )
    args = parser.parse_args()

    if args.mode == "services":
        ok = run_services()
        sys.exit(0 if ok else 1)
    elif args.mode == "concurrent":
        run_concurrent()
    elif args.mode == "latency":
        run_latency()
    elif args.mode == "backpressure":
        run_backpressure()
