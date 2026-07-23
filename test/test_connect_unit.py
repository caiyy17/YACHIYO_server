"""Offline connection and pipeline unit tests.

These tests use inline node configurations and fakes only. They never load a
pipeline configuration file and do not require running services.
"""

import json
import logging
import os
import subprocess
import sys
import threading
import time
import unittest
from queue import Empty, Queue
from unittest.mock import Mock, patch

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import requests

from Modules.base.BaseProcessingStep import BaseProcessingStep
from test_connect import (
    PROJECT_ROOT as CONNECT_PROJECT_ROOT,
    SERVICES_TIMEOUT,
    _probe_service,
    _runtime_freshness_error,
)

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

def check_basic_backpressure():
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


def check_no_backpressure_baseline():
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


def check_cancel_during_backpressure():
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


def check_multiple_inputs():
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


def check_three_stage_pipeline():
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
        ("Basic backpressure", check_basic_backpressure),
        ("No backpressure baseline", check_no_backpressure_baseline),
        ("Cancel during backpressure", check_cancel_during_backpressure),
        ("Multiple inputs", check_multiple_inputs),
        ("Three-stage pipeline", check_three_stage_pipeline),
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



class BackpressureUnitTest(unittest.TestCase):
    def test_basic_backpressure(self):
        self.assertTrue(check_basic_backpressure())

    def test_no_backpressure_baseline(self):
        self.assertTrue(check_no_backpressure_baseline())

    def test_cancel_during_backpressure(self):
        self.assertTrue(check_cancel_during_backpressure())

    def test_multiple_inputs(self):
        self.assertTrue(check_multiple_inputs())

    def test_three_stage_pipeline(self):
        self.assertTrue(check_three_stage_pipeline())


class ServiceFreshnessUnitTest(unittest.TestCase):
    def test_fresh_process(self):
        with (
            patch("test_connect._listener_pids", return_value=[123]),
            patch(
                "test_connect._process_workdir",
                return_value=CONNECT_PROJECT_ROOT,
            ),
            patch("test_connect._process_start_time", return_value=100.0),
            patch(
                "test_connect._python_sources",
                return_value=["server_fastapi.py"],
            ),
            patch.object(os.path, "getmtime", return_value=100.5),
        ):
            self.assertIsNone(
                _runtime_freshness_error(8910, ("server_fastapi.py",))
            )

    def test_stale_process(self):
        with (
            patch("test_connect._listener_pids", return_value=[123]),
            patch(
                "test_connect._process_workdir",
                return_value=CONNECT_PROJECT_ROOT,
            ),
            patch("test_connect._process_start_time", return_value=100.0),
            patch(
                "test_connect._python_sources",
                return_value=["server_fastapi.py"],
            ),
            patch.object(os.path, "getmtime", return_value=102.0),
        ):
            error = _runtime_freshness_error(8910, ("server_fastapi.py",))
        self.assertIn("stale local process pid=123", error)
        self.assertIn("server_fastapi.py", error)

    def test_probe_reports_ss_timeout(self):
        response = Mock(status_code=200)
        response.json.return_value = {"clients": []}
        timeout = subprocess.TimeoutExpired(["ss"], SERVICES_TIMEOUT)
        with (
            patch.object(requests, "request", return_value=response),
            patch(
                "test_connect._runtime_freshness_error",
                side_effect=timeout,
            ),
        ):
            result = _probe_service(
                "main_server",
                "http://localhost:8910",
                "/clients/",
                response_kind="clients",
                freshness_port=8910,
            )
        self.assertFalse(result["up"])
        self.assertIn("runtime freshness check failed", result["error"])

    def test_probe_reports_malformed_proc_stat(self):
        response = Mock(status_code=200)
        response.json.return_value = {"clients": []}
        with (
            patch.object(requests, "request", return_value=response),
            patch(
                "test_connect._runtime_freshness_error",
                side_effect=IndexError(),
            ),
        ):
            result = _probe_service(
                "main_server",
                "http://localhost:8910",
                "/clients/",
                response_kind="clients",
                freshness_port=8910,
            )
        self.assertFalse(result["up"])
        self.assertIn("runtime freshness check failed", result["error"])


if __name__ == "__main__":
    unittest.main()
