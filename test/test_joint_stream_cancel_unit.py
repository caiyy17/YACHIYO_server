"""Unit regressions for JointStream packing and cooperative cancellation."""

import copy
import json
import os
import queue
import sys
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Modules.parallel.JointStreamStep import (  # noqa: E402
    JointStreamStep,
    _CANCELLED,
    _DONE,
)


class _Logger:
    def __init__(self):
        self.messages = []

    def info(self, message, *args, **kwargs):
        self.messages.append(message)

    def warning(self, message, *args, **kwargs):
        self.messages.append(message)

    def error(self, message, *args, **kwargs):
        self.messages.append(message)


class _Caller:
    def __init__(self, stream):
        self.stream = stream

    def call_stream(self, **kwargs):
        return self.stream


class _ControlledIterator:
    """Blocks in the second next() so cancellation timing is deterministic."""

    def __init__(self):
        self.index = 0
        self.second_next_started = threading.Event()
        self.release_second = threading.Event()
        self.closed = threading.Event()

    def __iter__(self):
        return self

    def __next__(self):
        if self.index == 0:
            self.index += 1
            return {"value": 1}
        if self.index == 1:
            self.second_next_started.set()
            if not self.release_second.wait(timeout=2):
                raise RuntimeError("test did not release second next()")
            self.index += 1
            return {"value": 2}
        raise StopIteration

    def close(self):
        self.closed.set()


class JointStreamCancelTest(unittest.TestCase):
    @staticmethod
    def _bare_step():
        step = object.__new__(JointStreamStep)
        step.logger = _Logger()
        step._active_stop_event = None
        step.mode = "longest"
        step.anchor = None
        return step

    def _run_streams(self, specs, *, mode="longest", anchor=None,
                     inactive=()):
        """Run named finite streams; specs = (name, chunks, extend)."""
        step = self._bare_step()
        step.mode = mode
        step.anchor = anchor
        step.streams = []
        data = {}
        for name, chunks, extend in specs:
            input_name = f"input_{name}"
            data[input_name] = "" if name in inactive else name
            step.streams.append({
                "input": [{"source": input_name, "target": "prompt"}],
                "output": [{"source": name, "target": name}],
                "extend": extend,
                "is_anchor": name == anchor,
                "caller": _Caller(iter([{name: value} for value in chunks])),
            })

        emitted = []
        outputs = []
        step.emit_signal = lambda name, *args, **kwargs: emitted.append(name)
        step.envelope = lambda msg, *args, **kwargs: msg
        step.stamp = lambda msg, *args, **kwargs: msg
        step.check_cancel = lambda: False

        def pack(pack_data, mappings, chunk):
            for mapping in mappings:
                if mapping["source"] in chunk:
                    pack_data[mapping["target"]] = chunk[mapping["source"]]

        step._pack_chunk = pack
        step.output_to_queue = (
            lambda output, *args, **kwargs: outputs.append(dict(output))
        )
        step.process(data, {"timestamp": 1})
        return outputs, emitted

    def test_take_distinguishes_cancel_from_natural_done(self):
        step = self._bare_step()
        step.check_cancel = lambda: True

        self.assertIs(step._take(queue.Queue()), _CANCELLED)
        self.assertIsNot(_CANCELLED, _DONE)

    def test_pump_stops_at_next_iterator_boundary_and_closes(self):
        step = self._bare_step()
        stream = _ControlledIterator()
        output = queue.Queue()
        stop_event = threading.Event()
        thread = threading.Thread(
            target=step._pump,
            args=(stream, output, stop_event),
        )
        thread.start()

        self.assertTrue(stream.second_next_started.wait(timeout=1))
        stop_event.set()
        stream.release_second.set()
        thread.join(timeout=1)

        self.assertFalse(thread.is_alive())
        self.assertTrue(stream.closed.is_set())
        self.assertEqual(output.get_nowait(), {"value": 1})
        self.assertIs(output.get_nowait(), _DONE)
        self.assertTrue(output.empty())  # second chunk was cancelled

    def test_cancelled_process_does_not_emit_eos(self):
        step = self._bare_step()
        step.streams = [{
            "input": [{"source": "text", "target": "prompt"}],
            "output": [{"source": "value", "target": "value"}],
            "extend": False,
            "is_anchor": False,
            "caller": _Caller(iter(())),
        }]
        emitted = []
        step.emit_signal = lambda name, *args, **kwargs: emitted.append(name)
        step.envelope = lambda msg, *args, **kwargs: msg
        step.stamp = lambda msg, *args, **kwargs: msg
        step.check_cancel = lambda: True

        step.process({"text": "hello"}, {"timestamp": 1})

        self.assertEqual(emitted, ["SoS"])
        self.assertIsNone(step._active_stop_event)

    def test_natural_completion_emits_eos(self):
        step = self._bare_step()
        step.streams = [{
            "input": [{"source": "text", "target": "prompt"}],
            "output": [{"source": "value", "target": "value"}],
            "extend": False,
            "is_anchor": False,
            "caller": _Caller(iter(({"value": 1},))),
        }]
        emitted = []
        outputs = []
        step.emit_signal = lambda name, *args, **kwargs: emitted.append(name)
        step.envelope = lambda msg, *args, **kwargs: msg
        step.stamp = lambda msg, *args, **kwargs: msg
        step.check_cancel = lambda: False
        step._pack_chunk = (
            lambda pack, outputs_map, chunk: pack.update(chunk)
        )
        step.output_to_queue = (
            lambda data, *args, **kwargs: outputs.append(dict(data))
        )

        step.process({"text": "hello"}, {"timestamp": 1})

        self.assertEqual(outputs, [{"value": 1}])
        self.assertEqual(emitted, ["SoS", "EoS"])
        self.assertIsNone(step._active_stop_event)

    def test_longest_omits_finished_stream_without_extend(self):
        outputs, emitted = self._run_streams([
            ("short", ["s1", "s2"], False),
            ("long", ["l1", "l2", "l3", "l4"], False),
        ])

        self.assertEqual(outputs, [
            {"short": "s1", "long": "l1"},
            {"short": "s2", "long": "l2"},
            {"long": "l3"},
            {"long": "l4"},
        ])
        self.assertEqual(emitted, ["SoS", "EoS"])

    def test_longest_extend_repeats_exact_last_chunk(self):
        outputs, _ = self._run_streams([
            ("short", ["s1", "s2"], True),
            ("long", ["l1", "l2", "l3", "l4"], False),
        ])

        self.assertEqual(outputs, [
            {"short": "s1", "long": "l1"},
            {"short": "s2", "long": "l2"},
            {"short": "s2", "long": "l3"},
            {"short": "s2", "long": "l4"},
        ])

    def test_shortest_stops_at_first_stream_end(self):
        outputs, emitted = self._run_streams([
            ("short", ["s1", "s2"], True),
            ("long", ["l1", "l2", "l3", "l4"], False),
        ], mode="shortest")

        self.assertEqual(outputs, [
            {"short": "s1", "long": "l1"},
            {"short": "s2", "long": "l2"},
        ])
        self.assertEqual(emitted, ["SoS", "EoS"])

    def test_anchor_stops_at_anchor_and_discards_partial_pack(self):
        # The longer stream is deliberately first: its l3 is consumed before
        # the anchor reports _DONE, but must not leak past the anchor boundary.
        outputs, emitted = self._run_streams([
            ("long", ["l1", "l2", "l3", "l4"], False),
            ("anchor", ["a1", "a2"], False),
        ], mode="anchor", anchor="anchor")

        self.assertEqual(outputs, [
            {"long": "l1", "anchor": "a1"},
            {"long": "l2", "anchor": "a2"},
        ])
        self.assertEqual(emitted, ["SoS", "EoS"])

    def test_anchor_extends_stream_that_finishes_early(self):
        outputs, _ = self._run_streams([
            ("short", ["s1", "s2"], True),
            ("anchor", ["a1", "a2", "a3", "a4"], False),
        ], mode="anchor", anchor="anchor")

        self.assertEqual(outputs, [
            {"short": "s1", "anchor": "a1"},
            {"short": "s2", "anchor": "a2"},
            {"short": "s2", "anchor": "a3"},
            {"short": "s2", "anchor": "a4"},
        ])

    def test_inactive_anchor_falls_back_to_longest(self):
        outputs, emitted = self._run_streams([
            ("anchor", [], False),
            ("available", ["v1", "v2"], False),
        ], mode="anchor", anchor="anchor", inactive={"anchor"})

        self.assertEqual(outputs, [
            {"available": "v1"},
            {"available": "v2"},
        ])
        self.assertEqual(emitted, ["SoS", "EoS"])

    def test_config_modes_anchor_and_extend_validation(self):
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(project_root, "configs", "dev_joint_stream.json"),
                  encoding="utf-8") as f:
            full_config = json.load(f)
        base = next(
            node["config"] for node in full_config["pipeline"]
            if node["function"] == "call_joint_stream"
        )

        for mode in ("longest", "shortest"):
            config = copy.deepcopy(base)
            config["mode"] = mode
            self.assertEqual(JointStreamStep.validate_config(config), [])

        config = copy.deepcopy(base)
        config["mode"] = "anchor"
        config["anchor"] = "motion"
        self.assertEqual(JointStreamStep.validate_config(config), [])

        config["anchor"] = "missing"
        errors = JointStreamStep.validate_config(config)
        self.assertTrue(any("naming exactly one" in e for e in errors))

        config = copy.deepcopy(base)
        config["streams"][0]["extend"] = "yes"
        errors = JointStreamStep.validate_config(config)
        self.assertTrue(any("extend must be boolean" in e for e in errors))


if __name__ == "__main__":
    unittest.main()
