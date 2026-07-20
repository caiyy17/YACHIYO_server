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
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

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
from websockets.exceptions import InvalidStatus

from Modules.base.BaseProcessingStep import BaseProcessingStep


# ══════════════════════════════════════════════════════════════════════════
# MODE: concurrent  (from test/test_concurrent.py)
# ══════════════════════════════════════════════════════════════════════════

server_url = "http://localhost:8910"
websocket_url = "ws://localhost:8910/ws"
REQUEST_TIMEOUT = 30


def client_log_path(client_id):
    return os.path.join(PROJECT_ROOT, "logs", f"client_{client_id}.log")


def request_json(method, path, expected=200, timeout=REQUEST_TIMEOUT, **kwargs):
    response = requests.request(
        method, f"{server_url}{path}", timeout=timeout, **kwargs
    )
    if response.status_code != expected:
        raise AssertionError(
            f"{method} {path}: expected {expected}, got "
            f"{response.status_code}: {response.text[:300]}"
        )
    try:
        return response.json()
    except ValueError as exc:
        raise AssertionError(f"{method} {path}: response is not JSON") from exc


def cleanup_client(client_id):
    result = request_json(
        "POST", "/unregister/", json={"client_id": client_id}
    )
    if result.get("status") not in ("unregistered", "not registered"):
        raise AssertionError(f"Unexpected unregister response: {result}")
    clients = request_json("GET", "/clients/").get("clients")
    if not isinstance(clients, list):
        raise AssertionError("GET /clients/: missing clients list")
    if client_id in clients:
        raise AssertionError(f"Client {client_id} remained registered after cleanup")


async def expect_websocket_rejected(client_id, status_code):
    try:
        websocket = await websockets.connect(f"{websocket_url}/{client_id}")
    except InvalidStatus as exc:
        actual = exc.response.status_code
        if actual != status_code:
            raise AssertionError(
                f"WebSocket expected HTTP {status_code}, got {actual}"
            ) from exc
        return
    await websocket.close()
    raise AssertionError("WebSocket connection unexpectedly succeeded")


async def send_text_and_wait_for_eos(websocket):
    await websocket.send(json.dumps({
        "text": "你好，请回复测试成功。",
        "timestamp": time.time(),
    }))
    text_parts = []
    deadline = asyncio.get_running_loop().time() + 90
    while True:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise AssertionError("WebSocket response did not reach EoS")
        raw = await asyncio.wait_for(websocket.recv(), timeout=remaining)
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise AssertionError("WebSocket response must be a JSON object")
        if data.get("text"):
            text_parts.append(data["text"])
        if data.get("signal") == "EoS":
            break
    if not "".join(text_parts).strip():
        raise AssertionError("WebSocket reached EoS without response text")


async def verify_timestamp_semantics(websocket, client_id):
    await websocket.send(json.dumps({
        "text": "nan timestamp probe",
        "timestamp": float("nan"),
    }))
    await asyncio.sleep(0.5)
    log = request_json("GET", f"/logs/{client_id}")["log_content"]
    invalid_count = log.count("missing/invalid timestamp")
    if invalid_count != 1 or "nan timestamp probe" not in log:
        raise AssertionError("NaN timestamp was not rejected at the WS entry")

    await websocket.send(json.dumps({
        "signal": "cancel",
        "timestamp": float("inf"),
    }))
    await asyncio.sleep(1.5)
    log = request_json("GET", f"/logs/{client_id}")["log_content"]
    if log.count("missing/invalid timestamp") != invalid_count:
        raise AssertionError("+Inf timestamp was incorrectly rejected")
    if "received cancel signal" not in log:
        raise AssertionError("+Inf cancel did not enter the pipeline")


async def concurrent_main():
    client_id = f"connect_test_{time.time_ns()}"
    log_path = client_log_path(client_id)
    websocket = None
    try:
        request_json("GET", f"/logs/{client_id}", expected=404)
        register = request_json(
            "POST", "/register/", json={"client_id": client_id}
        )
        if register.get("status") != "registered":
            raise AssertionError(f"Unexpected register response: {register}")

        clients = request_json("GET", "/clients/").get("clients", [])
        if client_id not in clients:
            raise AssertionError("Registered client is missing from /clients/")
        request_json("GET", f"/clients/{client_id}")

        with open(log_path, "a", encoding="utf-8") as log_file:
            for index in range(205):
                log_file.write(f"tail-probe-{index:03d}\n")
        log_lines = request_json("GET", f"/logs/{client_id}")[
            "log_content"
        ].splitlines()
        expected_lines = [f"tail-probe-{index:03d}" for index in range(5, 205)]
        if log_lines != expected_lines:
            raise AssertionError("Log endpoint did not return the exact last 200 lines")

        init = request_json(
            "POST", f"/init_pipeline/{client_id}", timeout=300,
            json={"config": "unity_chan_text"},
        )
        if init.get("status") != "initialized":
            raise AssertionError(f"Unexpected init response: {init}")

        websocket = await websockets.connect(
            f"{websocket_url}/{client_id}", max_size=None
        )
        await expect_websocket_rejected(client_id, 409)
        await send_text_and_wait_for_eos(websocket)
        await websocket.close()
        websocket = None

        await expect_websocket_rejected(client_id, 409)
        request_json(
            "POST", f"/init_pipeline/{client_id}", timeout=300,
            json={"config": "unity_chan_text"},
        )
        websocket = await websockets.connect(
            f"{websocket_url}/{client_id}", max_size=None
        )

        request_json(
            "POST", f"/init_pipeline/{client_id}", timeout=300,
            json={"config": "unity_chan_text"},
        )
        await asyncio.wait_for(websocket.wait_closed(), timeout=30)
        websocket = await websockets.connect(
            f"{websocket_url}/{client_id}", max_size=None
        )
        await send_text_and_wait_for_eos(websocket)
        await verify_timestamp_semantics(websocket, client_id)
        print(
            "Server API, log tail, WebSocket rejection, re-init and "
            "timestamp semantics: PASS"
        )
        return True
    except Exception as exc:
        print(f"Server lifecycle test failed: {exc}")
        return False
    finally:
        try:
            if websocket is not None:
                await websocket.close()
        finally:
            try:
                cleanup_client(client_id)
            finally:
                try:
                    os.unlink(log_path)
                except FileNotFoundError:
                    pass


def run_concurrent():
    return asyncio.run(concurrent_main())


# ══════════════════════════════════════════════════════════════════════════
# MODE: latency  (from test/test_latency.py)
# ══════════════════════════════════════════════════════════════════════════

SERVER = "http://localhost:8910"
WS_URL = "ws://localhost:8910/ws"
ROUNDS = 3


def load_audio():
    with open(os.path.join(PROJECT_ROOT, "test", "test_voice.wav"), "rb") as f:
        return base64.b64encode(f.read()).decode()


async def run_ws_pipeline(client_id, config_name, audio_b64, timeout=20):
    """Run a WebSocket pipeline and return timing data."""
    try:
        request_json("POST", "/register/", json={"client_id": client_id})
        request_json(
            "POST", f"/init_pipeline/{client_id}", timeout=300,
            json={"config": config_name},
        )
        msg = json.dumps({"audio_file": audio_b64, "timestamp": time.time()})
        start = time.time()
        results = []
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
        if not any(result["has_audio"] for result in results):
            raise AssertionError(f"{config_name}: no audio output")
        log = request_json("GET", f"/logs/{client_id}").get("log_content", "")
        return results, log
    finally:
        try:
            cleanup_client(client_id)
        finally:
            try:
                os.unlink(client_log_path(client_id))
            except FileNotFoundError:
                pass


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
    run_id = time.time_ns()

    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"  Config: {config_name}, Rounds: {ROUNDS}")
    print(f"{'='*60}")

    for i in range(ROUNDS):
        client_id = f"bench_{config_name}_{run_id}_{i}"
        results, log = await run_ws_pipeline(client_id, config_name, audio_b64)

        # E2E from client perspective
        first_audio_time = None
        eos_time = None
        for r in results:
            if r["has_audio"] and first_audio_time is None:
                first_audio_time = r["time"]
            if r["signal"] == "EoS":
                eos_time = r["time"]
        if first_audio_time is None or eos_time is None:
            raise AssertionError(
                f"{config_name} round {i + 1}: missing audio or EoS"
            )

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


async def benchmark_multi_user():
    """Test concurrent users sharing services."""
    audio_b64 = load_audio()
    user_count = 3
    run_id = time.time_ns()
    print(f"\n{'='*60}")
    print(f"  Multi-User Concurrency Test ({user_count} users)")
    print(f"{'='*60}")

    async def single_user(idx):
        client_id = f"multi_{run_id}_{idx}"
        results, _ = await run_ws_pipeline(
            client_id, "unity_chan_default", audio_b64, timeout=60
        )
        first_audio = next(
            result["time"] for result in results if result["has_audio"]
        )
        total = next(
            result["time"] for result in results
            if result["signal"] == "EoS"
        )
        return {"user": idx, "first_audio": first_audio, "total": total}

    results = await asyncio.gather(
        *(single_user(index) for index in range(user_count))
    )

    for r in results:
        fa = r["first_audio"]
        tot = r["total"]
        print(f"  User {r['user']}: first_audio={fa*1000:.0f}ms, total={tot*1000:.0f}ms")

    avg_fa = statistics.mean([r["first_audio"] for r in results if r["first_audio"]])
    avg_tot = statistics.mean([r["total"] for r in results if r["total"]])
    print(f"  Average: first_audio={avg_fa*1000:.0f}ms, total={avg_tot*1000:.0f}ms")

    return True


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

    # WebRTC transport has its own end-to-end test script.
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
    return True


def run_latency():
    return asyncio.run(latency_main())


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
        return not self._thread.is_alive()

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
    monitor_stopped = monitor.stop()

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
    if not monitor_stopped:
        print("  FAIL: queue monitor thread is still alive")
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
    monitor_stopped = monitor.stop()

    data_msgs = [r for r in results if not r.get("signal")]

    print(f"  Data messages:     {len(data_msgs)} (expected {NUM_CHUNKS})")
    print(f"  Queue peak:        {monitor.max_size} (unbounded)")

    ok = True
    if len(data_msgs) != NUM_CHUNKS:
        print(f"  FAIL: expected {NUM_CHUNKS} data msgs, got {len(data_msgs)}")
        ok = False
    if monitor.max_size <= 3:
        print(f"  FAIL: queue peak only {monitor.max_size}, expected > 3 without backpressure")
        ok = False
    if not monitor_stopped:
        print("  FAIL: queue monitor thread is still alive")
        ok = False

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
    backpressured = p["queues"][1].full()
    before_cancel = []
    while True:
        try:
            before_cancel.append(json.loads(p["send_queue"].get_nowait()))
        except Empty:
            break
    send_cancel(p, ts + 0.001)

    # Allow the one item already inside SlowConsumer.process() to finish,
    # then require a full quiet window.  Merely checking "fewer than all 50"
    # can pass even when cancel is ignored because this consumer is slow.
    settle_deadline = time.monotonic() + 1.2
    settled = []
    while time.monotonic() < settle_deadline:
        try:
            settled.append(json.loads(p["send_queue"].get(timeout=0.1)))
        except Empty:
            pass

    quiet_deadline = time.monotonic() + 1.2
    late = []
    while time.monotonic() < quiet_deadline:
        try:
            late.append(json.loads(p["send_queue"].get(timeout=0.1)))
        except Empty:
            pass

    print(f"  Settle outputs:    {len(settled)}")
    print(f"  Late outputs:      {len(late)} (expected 0)")

    ok = True
    if not backpressured:
        print("  FAIL: bounded queue was not full before cancel")
        ok = False
    if not any(not item.get("signal") for item in before_cancel):
        print("  FAIL: no data was processed before cancel")
        ok = False
    if late:
        print("  FAIL: output continued after cancel settled")
        ok = False

    stopped_by_cancel = [t.name for t in p["threads"] if not t.is_alive()]
    if stopped_by_cancel:
        print(f"  FAIL: cancel stopped worker threads: {stopped_by_cancel}")
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
    monitor_stopped = monitor.stop()

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
    if not monitor_stopped:
        print("  FAIL: queue monitor thread is still alive")
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
    monitor_stopped = monitor.stop()

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
    if not monitor_stopped:
        print("  FAIL: queue monitor thread is still alive")
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
    return all_pass


# ══════════════════════════════════════════════════════════════════════════
# MODE: services  (connectivity check for all dependencies)
# ══════════════════════════════════════════════════════════════════════════

# Settings file holding the external service addresses (asr/llm/tts/...).
SERVICES_SETTINGS_PATH = os.path.join(
    PROJECT_ROOT, "configs", "settings", "settings.json",
)
SERVICES_TIMEOUT = 5  # seconds per probe


def _probe_service(
    name, base_url, path, *, method="GET", expected_status=200,
    json_body=None, response_kind=None,
):
    """Probe one documented endpoint and validate its expected response."""
    url = base_url.rstrip("/") + path
    start = time.time()
    try:
        resp = requests.request(
            method, url, json=json_body, timeout=SERVICES_TIMEOUT
        )
        ms = (time.time() - start) * 1000
        error = None
        if resp.status_code != expected_status:
            error = f"expected HTTP {expected_status}"
        elif response_kind:
            try:
                payload = resp.json()
            except ValueError:
                error = "response is not JSON"
            else:
                valid = {
                    "models": isinstance(payload.get("data"), list),
                    "data_query": payload.get("error") == "Dataset not loaded",
                    "vad": payload.get("status") == "ok",
                    "clients": isinstance(payload.get("clients"), list),
                    "gateway": payload.get("status") == "running",
                }[response_kind] if isinstance(payload, dict) else False
                if not valid:
                    error = f"invalid {response_kind} response"
        return {"name": name, "url": url, "up": error is None,
                "status": resp.status_code, "error": error, "ms": ms}
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

    # Keyword arguments describe the documented response for each dependency.
    probes = []

    # ── External services from settings.json ──────────────────────────
    try:
        with open(SERVICES_SETTINGS_PATH, "r") as f:
            settings = json.load(f)
    except Exception as e:
        print(f"  Failed to read settings: {SERVICES_SETTINGS_PATH}: {e}")
        return False
    if not isinstance(settings, dict):
        print("  Failed to read settings: top-level value must be an object")
        return False

    required_urls = {
        "asr": "qwen_asr_api",
        "llm": "custom_api",
        "tts": "qwen_tts_api",
        "data_query": "data_api",
        "motion_generation": "motion_api",
        "vad": "vad_api",
    }
    for section, key in required_urls.items():
        entries = settings.get(section)
        value = entries.get(key) if isinstance(entries, dict) else None
        if not isinstance(value, str) or not value.startswith(("http://", "https://")):
            print(f"  Missing service URL: {section}.{key}")
            return False

    for section, entries in settings.items():
        if not isinstance(entries, dict):
            continue
        for key, value in entries.items():
            # Only treat http(s) URLs as service endpoints (skip e.g. datasets_path).
            if not isinstance(value, str) or not value.startswith("http"):
                continue
            url = value.rstrip("/")
            options = {}
            if url.endswith("/v1"):
                path = "/models"
                options["response_kind"] = "models"
            elif section == "vad":
                path = "/health"
                options["response_kind"] = "vad"
            elif section == "data_query":
                path = "/query"
                options.update({
                    "method": "POST",
                    "expected_status": 400,
                    "json_body": {
                        "dataset": "__health_probe__", "queries": ["health"]
                    },
                    "response_kind": "data_query",
                })
            else:
                path = "/"
            probes.append((f"{section}.{key}", url, path, options))

    # ── First-party servers (not in settings.json) ───────────────────
    probes.append((
        "main_server", "http://localhost:8910", "/clients/",
        {"response_kind": "clients"},
    ))
    probes.append((
        "webrtc_server", "http://localhost:15168", "/status",
        {"response_kind": "gateway"},
    ))

    # ── Probe & report ───────────────────────────────────────────────
    results = []
    name_w = max((len(p[0]) for p in probes), default=12)
    for name, base_url, path, options in probes:
        r = _probe_service(name, base_url, path, **options)
        results.append(r)
        state = "UP  " if r["up"] else "DOWN"
        detail = (f"status={r['status']}" if r["up"]
                  else f"status={r['status']} error={r['error']}")
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

def main():
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

    try:
        if args.mode == "services":
            return run_services()
        if args.mode == "concurrent":
            return run_concurrent()
        if args.mode == "latency":
            return run_latency()
        return run_backpressure()
    except Exception as exc:
        print(f"{args.mode} test failed: {exc}")
        return False


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
