"""
Standalone backpressure test for max_queue_size feature.
Does not require the server to be running — builds pipelines directly.

Tests:
  1. Basic backpressure: queue size stays within limit, all messages delivered
  2. No backpressure baseline: queue grows beyond limit (proves test 1 is meaningful)
  3. Cancel during backpressure: no hang, cancel propagates
  4. Multiple sequential inputs: all processed correctly under backpressure
  5. Three-stage pipeline: backpressure on final stage, mid-stage blocks on output
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import json
import threading
import logging
from queue import Queue, Empty

from Modules.base.BaseProcessingStep import BaseProcessingStep


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
    Returns dict with queues, threads, kill_event, etc.
    """
    send_queue = Queue()
    kill_event = threading.Event()

    n = len(node_specs)
    queues = []
    cancel_queues = []

    for i in range(n):
        mqs = node_specs[i][2].get("max_queue_size", 0)
        queues.append(Queue(maxsize=mqs))
        cancel_queues.append(Queue())
    queues.append(send_queue)
    cancel_queues.append(Queue())

    threads = []
    for i, (cls, node_id, config) in enumerate(node_specs):
        inst = cls(
            node_id, "test", logger,
            send_queue, queues[i], queues[i + 1],
            cancel_queues[i], kill_event, config,
        )
        t = threading.Thread(target=inst.run, daemon=True, name=f"node_{node_id}")
        threads.append(t)

    return {
        "queues": queues,
        "cancel_queues": cancel_queues,
        "send_queue": send_queue,
        "kill_event": kill_event,
        "threads": threads,
    }


def start_pipeline(p):
    for t in p["threads"]:
        t.start()


def stop_pipeline(p, timeout=5):
    p["kill_event"].set()
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
            "output_vars": [{"output_name": "text", "target": "1_text"}],
            "next_nodes": [3],
        }),
        (SlowConsumer, 3, {
            "max_queue_size": MAX_Q,
            "process_time": 0.15,
            "input_vars": [{"input_name": "text", "source": "1_text"}],
            "output_vars": [{"output_name": "result", "target": "result"}],
            "next_nodes": [],
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
            "output_vars": [{"output_name": "text", "target": "1_text"}],
            "next_nodes": [3],
        }),
        (SlowConsumer, 3, {
            # No max_queue_size — unbounded
            "process_time": 0.15,
            "input_vars": [{"input_name": "text", "source": "1_text"}],
            "output_vars": [{"output_name": "result", "target": "result"}],
            "next_nodes": [],
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
            "output_vars": [{"output_name": "text", "target": "1_text"}],
            "next_nodes": [3],
        }),
        (SlowConsumer, 3, {
            "max_queue_size": 2,
            "process_time": 0.5,
            "input_vars": [{"input_name": "text", "source": "1_text"}],
            "output_vars": [{"output_name": "result", "target": "result"}],
            "next_nodes": [],
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
            "output_vars": [{"output_name": "text", "target": "1_text"}],
            "next_nodes": [3],
        }),
        (SlowConsumer, 3, {
            "max_queue_size": MAX_Q,
            "process_time": 0.08,
            "input_vars": [{"input_name": "text", "source": "1_text"}],
            "output_vars": [{"output_name": "result", "target": "result"}],
            "next_nodes": [],
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
            "output_vars": [{"output_name": "text", "target": "1_text"}],
            "next_nodes": [3],
        }),
        (SlowConsumer, 3, {
            # Mid-stage: fast processing, but blocks on output to stage 3
            "process_time": 0.02,
            "input_vars": [{"input_name": "text", "source": "1_text"}],
            "output_vars": [{"output_name": "result", "target": "3_result"}],
            "next_nodes": [5],
        }),
        (SlowConsumer, 5, {
            "max_queue_size": MAX_Q,
            "process_time": 0.2,
            "input_vars": [{"input_name": "text", "source": "3_result"}],
            "output_vars": [{"output_name": "result", "target": "result"}],
            "next_nodes": [],
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


# ── Main ──────────────────────────────────────────────────────

if __name__ == "__main__":
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
