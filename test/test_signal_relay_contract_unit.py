import json
import logging
import os
import queue
import sys
import threading
import unittest


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from Modules.base.BaseProcessingStep import BaseProcessingStep  # noqa: E402
from Modules.base.SpanProcessingStep import SpanProcessingStep  # noqa: E402


def _config(trace, *, fail_handler=False):
    return {
        "catch_signals": [
            {"source": "wire_signal", "target": "handled_signal"},
        ],
        "pass_signals": [
            {"source": "wire_signal", "target": "relayed_signal"},
        ],
        "emit_signals": [],
        "catch_events": [],
        "input_vars": [],
        "pass_vars": [],
        "output_vars": [],
        "next_nodes": [-1],
        "__trace": trace,
        "__fail_handler": fail_handler,
    }


class _TracingOutputQueue(queue.Queue):
    def __init__(self, trace):
        super().__init__()
        self.trace = trace

    def put(self, item, block=True, timeout=None):
        message = json.loads(item)
        self.trace.append(("relay", message["timestamp"]))
        super().put(item, block=block, timeout=timeout)


class _HandlerProbe:
    def _handle(self, data):
        self.get_config("__trace").append(("handler", data["timestamp"]))
        if self.get_config("__fail_handler", False):
            raise RuntimeError("intentional handler failure")


class _BaseProbe(_HandlerProbe, BaseProcessingStep):
    REQUIRED_CATCH_SIGNALS = ["handled_signal"]

    def process(self, data, pass_data={}):
        self._handle(data)


class _SpanProbe(_HandlerProbe, SpanProcessingStep):
    REQUIRED_CATCH_SIGNALS = ["handled_signal"]

    def span_process(self, data, pass_data={}):
        self._handle(data)


class _RunningProbe:
    def __init__(self, step_class, *, fail_handler=False):
        self.trace = []
        self.input = queue.Queue()
        self.output = _TracingOutputQueue(self.trace)
        self.cancel = queue.Queue()
        self.step = step_class(
            1,
            "signal-relay-contract-test",
            logging.getLogger(f"test.{step_class.__name__}"),
            queue.Queue(),
            self.input,
            self.output,
            self.cancel,
            _config(self.trace, fail_handler=fail_handler),
        )
        if self.step.init_error:
            raise RuntimeError(self.step.init_error)
        self.thread = threading.Thread(target=self.step.run)

    def start(self):
        self.thread.start()

    def put(self, message):
        self.input.put(json.dumps(message))

    def take(self, timeout=1):
        try:
            return json.loads(self.output.get(timeout=timeout))
        except queue.Empty as error:
            raise AssertionError(
                "timestamp-valid signal declared in pass_signals was not relayed"
            ) from error

    def close(self):
        self.cancel.put(json.dumps({"signal": "kill", "timestamp": 999}))
        # Wake a run loop that may currently be blocked in input_queue.get().
        self.put({"signal": "__test_wake__", "timestamp": 999})
        self.thread.join(timeout=2)
        if self.thread.is_alive():
            raise RuntimeError("probe thread did not stop")


class SignalRelayContractTest(unittest.TestCase):
    STEP_CLASSES = (_BaseProbe, _SpanProbe)

    def test_handler_runs_before_exactly_one_relay(self):
        for step_class in self.STEP_CLASSES:
            with self.subTest(step=step_class.__name__):
                runner = _RunningProbe(step_class)
                runner.start()
                try:
                    runner.put({"signal": "wire_signal", "timestamp": 10})
                    relayed = runner.take()

                    self.assertEqual(relayed["signal"], "relayed_signal")
                    self.assertEqual(relayed["timestamp"], 10)
                    self.assertEqual(relayed["destination"], -1)
                    self.assertEqual(
                        runner.trace,
                        [("handler", 10), ("relay", 10)],
                    )
                    self.assertTrue(runner.output.empty())
                finally:
                    runner.close()

    def test_handler_error_still_relays_exactly_once(self):
        for step_class in self.STEP_CLASSES:
            with self.subTest(step=step_class.__name__):
                runner = _RunningProbe(step_class, fail_handler=True)
                runner.start()
                try:
                    runner.put({"signal": "wire_signal", "timestamp": 10})
                    relayed = runner.take()

                    self.assertEqual(relayed["signal"], "relayed_signal")
                    self.assertEqual(relayed["timestamp"], 10)
                    self.assertEqual(
                        runner.trace,
                        [("handler", 10), ("relay", 10)],
                    )
                    self.assertTrue(runner.output.empty())
                finally:
                    runner.close()

    def test_old_timestamp_does_not_relay_but_boundary_timestamp_does(self):
        for step_class in self.STEP_CLASSES:
            with self.subTest(step=step_class.__name__):
                runner = _RunningProbe(step_class)
                # Preload the control queue so the watermark is installed before
                # either FIFO input is considered.
                runner.cancel.put(json.dumps({
                    "signal": "cancel",
                    "timestamp": 10,
                }))
                runner.put({"signal": "wire_signal", "timestamp": 9})
                runner.put({"signal": "wire_signal", "timestamp": 10})
                runner.start()
                try:
                    relayed = runner.take()

                    self.assertEqual(relayed["signal"], "relayed_signal")
                    self.assertEqual(relayed["timestamp"], 10)
                    self.assertEqual(
                        runner.trace,
                        [("handler", 10), ("relay", 10)],
                    )
                    self.assertTrue(runner.output.empty())
                finally:
                    runner.close()


if __name__ == "__main__":
    unittest.main()
