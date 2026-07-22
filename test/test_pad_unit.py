"""Unit tests for PadStep alignment and its published target duration."""

import base64
import io
import json
import os
import queue
import sys
import unittest
import wave

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Modules.pad.PadStep import PadStep


class _Logger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


def _wav(duration, rate=8000):
    frame_count = int(round(duration * rate))
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(rate)
        wav_file.writeframes(bytes(frame_count * 2))
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def _frames(duration, framerate=10):
    frame_count = int(round(duration * framerate))
    return [
        {
            **({
                "header": {
                    "framerate": framerate,
                    "duration": duration,
                }
            } if index == 0 else {}),
            "value": index,
        }
        for index in range(frame_count)
    ]


class PadDurationTest(unittest.TestCase):
    def _step(self, mode, *, anchor=None, behavior=None):
        config = {
            "input_vars": [
                {"source": "audio_data", "target": "audio_data"},
                {"source": "action", "target": "action"},
            ],
            "pass_vars": [],
            "output_vars": [
                {"source": "audio_data", "target": "audio_data"},
                {"source": "action", "target": "action"},
                {"source": "duration", "target": "duration"},
            ],
            "catch_signals": [],
            "pass_signals": [],
            "emit_signals": [],
            "next_nodes": [-1],
            "mode": mode,
            "behavior": behavior or {},
        }
        if anchor is not None:
            config["anchor"] = anchor
        self.assertEqual(PadStep.validate_config(config), [])
        output = queue.Queue()
        step = PadStep(
            1,
            "pad-test",
            _Logger(),
            queue.Queue(),
            queue.Queue(),
            output,
            queue.Queue(),
            config,
        )
        self.assertIsNone(step.init_error)
        return step, output

    @staticmethod
    def _process(step, output, audio_duration, action_duration):
        step.process(
            {
                "audio_data": _wav(audio_duration),
                "action": _frames(action_duration),
            },
            {"timestamp": 123.0},
        )
        return json.loads(output.get_nowait())

    def test_longest_publishes_target_when_short_lane_does_not_extend(self):
        step, output = self._step(
            "longest", behavior={"action": {"extend": False}}
        )
        result = self._process(step, output, 2.0, 1.0)

        self.assertEqual(result["duration"], 2.0)
        self.assertEqual(len(result["action"]), 10)
        self.assertEqual(result["action"][0]["header"]["duration"], 1.0)

    def test_anchor_publishes_anchor_when_long_lane_does_not_cut(self):
        step, output = self._step(
            "anchor",
            anchor="audio_data",
            behavior={"action": {"cut": False}},
        )
        result = self._process(step, output, 1.0, 2.0)

        self.assertEqual(result["duration"], 1.0)
        self.assertEqual(len(result["action"]), 20)
        self.assertEqual(result["action"][0]["header"]["duration"], 2.0)

    def test_shortest_publishes_target_when_long_lane_does_not_cut(self):
        step, output = self._step(
            "shortest", behavior={"audio_data": {"cut": False}}
        )
        result = self._process(step, output, 2.0, 1.0)

        self.assertEqual(result["duration"], 1.0)
        with wave.open(
            io.BytesIO(base64.b64decode(result["audio_data"])), "rb"
        ) as wav_file:
            self.assertEqual(wav_file.getnframes() / wav_file.getframerate(), 2.0)

    def test_unreadable_anchor_has_no_false_standard_duration(self):
        step, output = self._step("anchor", anchor="action")
        step.process(
            {"audio_data": _wav(1.0), "action": []},
            {"timestamp": 123.0},
        )
        result = json.loads(output.get_nowait())

        self.assertNotIn("duration", result)
        self.assertEqual(result["action"], [])


if __name__ == "__main__":
    unittest.main()
