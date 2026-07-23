"""
Consolidated connection/stress test harness.

Select a mode with --mode:
  services      Connectivity check for all dependent services (main server,
                gateway, model backends) before running heavier modes.
  concurrent    Server connection / API lifecycle test (register/unregister/
                get_clients/get_client/init_pipeline/get_client_log/websocket
                + concurrency). Requires a running server.
  latency       Latency benchmark: local and OpenAI API pipelines plus
                multi-user concurrency, with per-stage timings. It has no
                regression threshold; success means sampling completed.
                Requires a running server.
"""

import argparse
import os
import subprocess
import sys

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
from websockets.exceptions import InvalidStatus


# ══════════════════════════════════════════════════════════════════════════
# MODE: concurrent  (from test/test_concurrent.py)
# ══════════════════════════════════════════════════════════════════════════

server_url = "http://localhost:8910"
websocket_url = "ws://localhost:8910/ws"
REQUEST_TIMEOUT = 30


def client_log_path(client_id):
    return os.path.join(PROJECT_ROOT, "logs", f"client_{client_id}.log")


def client_history_path(client_id):
    return os.path.join(
        PROJECT_ROOT, "history", f"history_{client_id}.json"
    )


def cleanup_client_files(client_id):
    """Remove only the log and history created for this test client."""
    errors = []
    for label, path in (
        ("client log", client_log_path(client_id)),
        ("client history", client_history_path(client_id)),
    ):
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        except OSError as error:
            errors.append(f"{label}: {error}")
    if errors:
        raise AssertionError("; ".join(errors))


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
                cleanup_client_files(client_id)


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
            cleanup_client_files(client_id)


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
    """Benchmark concurrent users sharing services."""
    audio_b64 = load_audio()
    user_count = 3
    run_id = time.time_ns()
    print(f"\n{'='*60}")
    print(f"  Multi-User Concurrency Benchmark ({user_count} users)")
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
        "unity_chan_default", "Pipeline 1: unity_chan_default (Qwen3-ASR + gemma + Qwen3-TTS)"
    )

    # 2. demo: full OpenAI — Whisper (ASR) + GPT (LLM) + OpenAI TTS-1, all remote
    avg_openai = await benchmark_ws(
        "demo", "Pipeline 2: demo (full OpenAI: Whisper + GPT + TTS-1)"
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
    print("\n  Benchmark complete (no performance pass/fail threshold).")
    return True


def run_latency():
    return asyncio.run(latency_main())


# ═════════════════════════════════════════════════════════════════════════
# MODE: services  (connectivity check for all dependencies)
# ══════════════════════════════════════════════════════════════════════════

# Settings file holding the external service addresses (asr/llm/tts/...).
SERVICES_SETTINGS_PATH = os.path.join(
    PROJECT_ROOT, "configs", "settings", "settings.json",
)
SERVICES_TIMEOUT = 5  # seconds per probe
SOURCE_MTIME_TOLERANCE = 1.0


def _listener_pids(port):
    """Return the local processes listening on a TCP port."""
    result = subprocess.run(
        ["ss", "-H", "-ltnp", f"sport = :{port}"],
        capture_output=True,
        text=True,
        timeout=SERVICES_TIMEOUT,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or f"ss exited {result.returncode}"
        raise RuntimeError(detail)
    return sorted({int(pid) for pid in re.findall(r"pid=(\d+)", result.stdout)})


def _process_start_time(pid):
    """Read a Linux process start time as Unix seconds."""
    with open(f"/proc/{pid}/stat", "r", encoding="utf-8") as handle:
        stat_line = handle.read()
    closing_paren = stat_line.rfind(")")
    if closing_paren < 0:
        raise RuntimeError(f"invalid /proc/{pid}/stat")
    # Fields after comm start at field 3; starttime is field 22.
    start_ticks = int(stat_line[closing_paren + 2:].split()[19])
    with open("/proc/stat", "r", encoding="utf-8") as handle:
        boot_time = next(
            int(line.split()[1])
            for line in handle
            if line.startswith("btime ")
        )
    return boot_time + start_ticks / os.sysconf("SC_CLK_TCK")


def _process_workdir(pid):
    return os.path.realpath(os.readlink(f"/proc/{pid}/cwd"))


def _python_sources(relative_paths):
    for relative_path in relative_paths:
        path = os.path.join(PROJECT_ROOT, relative_path)
        if os.path.isfile(path):
            if path.endswith(".py"):
                yield path
            continue
        for root, dirs, filenames in os.walk(path):
            dirs[:] = [name for name in dirs if name != "__pycache__"]
            for filename in filenames:
                if filename.endswith(".py"):
                    yield os.path.join(root, filename)


def _runtime_freshness_error(port, relative_paths):
    """Report local source files newer than the process serving ``port``."""
    pids = _listener_pids(port)
    if not pids:
        return f"cannot identify the local listener process on port {port}"

    expected_workdir = os.path.realpath(PROJECT_ROOT)
    wrong_workdirs = []
    for pid in pids:
        workdir = _process_workdir(pid)
        if workdir != expected_workdir:
            wrong_workdirs.append((pid, workdir))
    if wrong_workdirs:
        detail = ", ".join(
            f"pid={pid} cwd={workdir}" for pid, workdir in wrong_workdirs
        )
        return f"listener is not running from {expected_workdir}: {detail}"

    started_at = min(_process_start_time(pid) for pid in pids)
    changed = []
    for path in _python_sources(relative_paths):
        if os.path.getmtime(path) > started_at + SOURCE_MTIME_TOLERANCE:
            changed.append(os.path.relpath(path, PROJECT_ROOT))
    if not changed:
        return None

    preview = ", ".join(sorted(changed)[:5])
    if len(changed) > 5:
        preview += f", ... (+{len(changed) - 5})"
    return (
        f"stale local process pid={','.join(map(str, pids))}; "
        f"restart after source changes: {preview}"
    )


def _probe_service(
    name, base_url, path, *, method="GET", expected_status=200,
    json_body=None, response_kind=None, freshness_port=None,
    freshness_paths=(),
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
        if error is None and freshness_port is not None:
            try:
                error = _runtime_freshness_error(
                    freshness_port, freshness_paths
                )
            except (
                IndexError,
                OSError,
                RuntimeError,
                StopIteration,
                subprocess.SubprocessError,
                ValueError,
            ) as exc:
                error = f"runtime freshness check failed: {exc}"
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
        {
            "response_kind": "clients",
            "freshness_port": 8910,
            "freshness_paths": ("server_fastapi.py", "Modules", "utils"),
        },
    ))
    probes.append((
        "webrtc_server", "http://localhost:15168", "/status",
        {
            "response_kind": "gateway",
            "freshness_port": 15168,
            "freshness_paths": ("server_webrtc.py",),
        },
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
        choices=["services", "concurrent", "latency"],
        default="concurrent",
        help="Test mode: services (connectivity check for all dependencies), "
             "concurrent (server API + websocket), "
             "latency (latency benchmark).",
    )
    args = parser.parse_args()

    try:
        if args.mode == "services":
            return run_services()
        if args.mode == "concurrent":
            return run_concurrent()
        return run_latency()
    except Exception as exc:
        print(f"{args.mode} mode failed: {exc}")
        return False


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
