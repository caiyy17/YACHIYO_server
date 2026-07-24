import base64
import io
import json
import logging
import os
import queue
import sys
import threading
import time
import unittest

from PIL import Image

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from Modules.base.ChunkGenerationStep import (  # noqa: E402
    ChunkGenerationSession,
    ChunkGenerationStep,
)
from Modules.motion_base.MotionChunkStep import (  # noqa: E402
    MotionChunkStep,
)
from Modules.video_base.VideoChunkGenerationStep import (  # noqa: E402
    VideoChunkGenerationStep,
)


def _config(output, *, inputs=("audio", "text"), color=None):
    config = {
        "catch_signals": [
            {"source": "tts_SoS", "target": "stream_start"},
            {"source": "tts_EoS", "target": "stream_end"},
        ],
        "pass_signals": [
            {"source": "tts_SoS", "target": "av_SoS"},
            {"source": "tts_EoS", "target": "av_EoS"},
        ],
        "emit_signals": [],
        "input_vars": [
            {"source": name, "target": name} for name in inputs
        ],
        "pass_vars": [
            {"source": name, "target": name} for name in inputs
        ],
        "output_vars": [{"source": output, "target": output}],
        "next_nodes": [-1],
        "chunk_duration_ms": 200,
    }
    if color is not None:
        config["color"] = color
    return config


class _RunningStep:
    def __init__(self, cls, config):
        self.input = queue.Queue()
        self.output = queue.Queue()
        self.cancel = queue.Queue()
        self.step = cls(
            1,
            "test",
            logging.getLogger(f"test.{cls.__name__}"),
            queue.Queue(),
            self.input,
            self.output,
            self.cancel,
            config,
        )
        if self.step.init_error:
            raise RuntimeError(self.step.init_error)
        self.thread = threading.Thread(target=self.step.run)
        self.thread.start()

    def put(self, message):
        self.input.put(json.dumps(message))

    def take(self, timeout=2):
        return json.loads(self.output.get(timeout=timeout))

    def close(self):
        self.cancel.put(json.dumps({"signal": "kill", "timestamp": 999}))
        self.thread.join(timeout=3)
        if self.thread.is_alive():
            raise RuntimeError("step thread did not stop")


class ChunkGenerationBaseTest(unittest.TestCase):
    def test_video_stream_is_fixed_solid_and_ordered(self):
        runner = _RunningStep(
            VideoChunkGenerationStep,
            _config("video", color=[12, 180, 40]),
        )
        try:
            runner.put({
                "signal": "tts_SoS",
                "timestamp": 10,
                "pass_data": {"speaker": "alice"},
            })
            runner.put({"timestamp": 10, "audio": "a0", "text": "t0"})
            runner.put({"timestamp": 10, "audio": "a1", "text": "t1"})
            runner.put({"signal": "tts_EoS", "timestamp": 10})

            start, first, second, end = [runner.take() for _ in range(4)]
            self.assertEqual(start["signal"], "av_SoS")
            self.assertEqual(start["pass_data"], {"speaker": "alice"})
            self.assertEqual(end["signal"], "av_EoS")

            for message, audio, text in (
                    (first, "a0", "t0"), (second, "a1", "t1")):
                self.assertEqual(message["audio"], audio)
                self.assertEqual(message["text"], text)
                self.assertEqual(len(message["video"]), 6)
                self.assertEqual(
                    len({frame["image"] for frame in message["video"]}), 1
                )
            self.assertEqual(
                first["video"][0]["header"], {"framerate": 30}
            )
            self.assertNotIn("header", second["video"][0])

            image = Image.open(io.BytesIO(base64.b64decode(
                first["video"][0]["image"]
            )))
            self.assertEqual(image.size, (320, 240))
            pixel = image.convert("RGB").getpixel((0, 0))
            self.assertTrue(all(abs(a - b) <= 4
                                for a, b in zip(pixel, (12, 180, 40))))

            # A new owning span gets a fresh stream header.
            runner.put({"signal": "tts_SoS", "timestamp": 20})
            runner.put({"timestamp": 20, "audio": "a2", "text": "t2"})
            runner.put({"signal": "tts_EoS", "timestamp": 20})
            runner.take()
            fresh = runner.take()
            runner.take()
            self.assertEqual(
                fresh["video"][0]["header"], {"framerate": 30}
            )
        finally:
            runner.close()

    def test_motion_stream_is_fixed_neutral_and_resets(self):
        runner = _RunningStep(
            MotionChunkStep,
            _config("motion", inputs=("prompt",)),
        )
        try:
            runner.put({"signal": "tts_SoS", "timestamp": 1})
            runner.put({"timestamp": 1, "prompt": "walk"})
            runner.put({"timestamp": 1, "prompt": "turn"})
            runner.put({"signal": "tts_EoS", "timestamp": 1})
            start, first, second, end = [runner.take() for _ in range(4)]

            self.assertEqual(start["signal"], "av_SoS")
            self.assertEqual(end["signal"], "av_EoS")
            self.assertEqual(len(first["motion"]), 6)
            self.assertEqual(len(second["motion"]), 6)
            self.assertEqual(first["motion"][0]["header"], {
                "framerate": 30,
                "format": "humanoid",
            })
            self.assertNotIn("header", second["motion"][0])
            self.assertIsNot(first["motion"][0], first["motion"][1])
            self.assertIsNot(
                first["motion"][0]["root_dxz"],
                first["motion"][1]["root_dxz"],
            )
        finally:
            runner.close()

    def test_empty_span_only_relays_envelope(self):
        runner = _RunningStep(
            VideoChunkGenerationStep, _config("video")
        )
        try:
            runner.put({"signal": "tts_SoS", "timestamp": 1})
            runner.put({"signal": "tts_EoS", "timestamp": 1})
            self.assertEqual(runner.take()["signal"], "av_SoS")
            self.assertEqual(runner.take()["signal"], "av_EoS")
            with self.assertRaises(queue.Empty):
                runner.output.get(timeout=0.1)
        finally:
            runner.close()

    def test_repeated_eos_is_internal_noop_but_still_passes(self):
        runner = _RunningStep(
            VideoChunkGenerationStep, _config("video")
        )
        try:
            runner.put({"signal": "tts_SoS", "timestamp": 1})
            runner.put({"signal": "tts_EoS", "timestamp": 1})
            runner.put({"signal": "tts_EoS", "timestamp": 1})
            self.assertEqual(
                [runner.take()["signal"] for _ in range(3)],
                ["av_SoS", "av_EoS", "av_EoS"],
            )
            self.assertFalse(runner.step.span_active)
        finally:
            runner.close()

    def test_packet_outside_span_is_dropped(self):
        runner = _RunningStep(
            VideoChunkGenerationStep, _config("video")
        )
        try:
            runner.put({"timestamp": 1, "audio": "a", "text": "t"})
            with self.assertRaises(queue.Empty):
                runner.output.get(timeout=0.2)
        finally:
            runner.close()

    def test_validation_requires_caught_boundaries_to_pass(self):
        config = _config("video")
        config["pass_signals"] = config["pass_signals"][:1]
        errors = VideoChunkGenerationStep.validate_config(config)
        self.assertTrue(any("tts_EoS" in error and "pass_signals" in error
                            for error in errors), errors)

        config = _config("video")
        config["catch_signals"][0]["source"] = None
        errors = VideoChunkGenerationStep.validate_config(config)
        self.assertTrue(any("stream_start" in error and "non-null" in error
                            for error in errors), errors)

    def test_non_integral_frame_duration_is_rejected(self):
        config = _config("video")
        config["chunk_duration_ms"] = 250  # 7.5 frames at 30fps
        errors = VideoChunkGenerationStep.validate_config(config)
        self.assertTrue(any("positive integer" in error for error in errors))


class _BlockingSession(ChunkGenerationSession):
    def __init__(self, owner):
        self.owner = owner
        self.aborted = False

    def generate_chunk(self, inputs, chunk_index):
        self.owner.seen = dict(inputs)
        self.owner.entered.set()
        self.owner.release.wait(timeout=2)
        return {"value": chunk_index}

    def abort(self):
        self.aborted = True
        self.owner.aborted.set()


class _BlockingStep(ChunkGenerationStep):
    OUTPUTS = ["value"]

    def generation_init(self):
        self.entered = threading.Event()
        self.release = threading.Event()
        self.aborted = threading.Event()
        self.seen = None

    def open_generation_session(self, start_context):
        return _BlockingSession(self)


class _FailingSession(ChunkGenerationSession):
    def __init__(self, owner):
        self.owner = owner

    def generate_chunk(self, inputs, chunk_index):
        if self.owner.fail_generate:
            self.owner.fail_generate = False
            raise RuntimeError("generation failed")
        return {"value": chunk_index}

    def finish(self):
        if self.owner.fail_finish:
            self.owner.fail_finish = False
            raise RuntimeError("finish failed")

    def abort(self):
        self.owner.abort_count += 1


class _FailingStep(ChunkGenerationStep):
    OUTPUTS = ["value"]

    def generation_init(self):
        self.fail_generate = True
        self.fail_finish = False
        self.abort_count = 0

    def open_generation_session(self, start_context):
        return _FailingSession(self)


class _ManualPipelinedSession(ChunkGenerationSession):
    """Pipelined fake whose replies are released manually by the test
    (owner.replies); submit never blocks and carries no reply."""

    pipelined = True

    def __init__(self, owner):
        self.owner = owner

    def submit(self, inputs, chunk_index):
        self.owner.submitted.append((chunk_index, dict(inputs)))
        self.owner.inflight += 1

    def poll(self):
        results = []
        while self.owner.inflight > 0 and not self.owner.replies.empty():
            results.append(self.owner.replies.get_nowait())
            self.owner.inflight -= 1
        return results

    def has_pending(self):
        return self.owner.inflight > 0

    def next_result(self):
        result = self.owner.replies.get(timeout=2)
        self.owner.inflight -= 1
        return result

    def abort(self):
        self.owner.inflight = 0
        self.owner.abort_count += 1


class _PipelinedStep(ChunkGenerationStep):
    OUTPUTS = ["value"]

    def generation_init(self):
        self.submitted = []
        self.inflight = 0
        self.abort_count = 0
        self.replies = queue.Queue()

    def open_generation_session(self, start_context):
        return _ManualPipelinedSession(self)


def _wait_for(condition, timeout=2):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.01)
    return condition()


class ChunkGenerationPipelinedTest(unittest.TestCase):
    def test_replies_pair_with_their_own_pass_data_and_eos_drains(self):
        runner = _RunningStep(
            _PipelinedStep, _config("value", inputs=("audio", "mood"))
        )
        try:
            runner.put({"signal": "tts_SoS", "timestamp": 10})
            self.assertEqual(runner.take()["signal"], "av_SoS")
            runner.put({"timestamp": 10, "audio": "a0", "mood": "happy"})
            runner.put({"timestamp": 10, "audio": "a1", "mood": "calm"})
            self.assertTrue(
                _wait_for(lambda: len(runner.step.submitted) == 2)
            )
            time.sleep(0.05)
            self.assertTrue(runner.output.empty())

            # Two replies arrive, then a third input polls them out: each
            # must carry the pass context of ITS OWN request, not the
            # current input's.
            runner.step.replies.put({"value": "r0"})
            runner.step.replies.put({"value": "r1"})
            runner.put({"timestamp": 10, "audio": "a2", "mood": "focus"})
            first, second = runner.take(), runner.take()
            self.assertEqual(
                (first["value"], first["audio"], first["mood"]),
                ("r0", "a0", "happy"),
            )
            self.assertEqual(
                (second["value"], second["audio"], second["mood"]),
                ("r1", "a1", "calm"),
            )

            # stream_end blocks until the in-flight tail returns, emits it,
            # then the envelope closes.
            runner.put({"signal": "tts_EoS", "timestamp": 10})
            runner.step.replies.put({"value": "r2"})
            tail = runner.take()
            self.assertEqual(
                (tail["value"], tail["audio"], tail["mood"]),
                ("r2", "a2", "focus"),
            )
            self.assertEqual(runner.take()["signal"], "av_EoS")
            self.assertFalse(runner.step.span_active)
        finally:
            runner.close()

    def test_cancel_discards_in_flight_and_next_span_is_clean(self):
        runner = _RunningStep(
            _PipelinedStep, _config("value", inputs=("audio", "mood"))
        )
        try:
            runner.put({"signal": "tts_SoS", "timestamp": 10})
            self.assertEqual(runner.take()["signal"], "av_SoS")
            runner.put({"timestamp": 10, "audio": "a0", "mood": "happy"})
            self.assertTrue(
                _wait_for(lambda: len(runner.step.submitted) == 1)
            )
            runner.cancel.put(json.dumps({
                "signal": "cancel", "timestamp": 11,
            }))
            self.assertTrue(
                _wait_for(lambda: runner.step.abort_count == 1)
            )
            time.sleep(0.05)
            self.assertTrue(runner.output.empty())
            self.assertFalse(runner.step.span_active)

            # A stale reply from the killed span must not leak into the
            # next one: the pending queue was cleared with the abort.
            runner.put({"signal": "tts_SoS", "timestamp": 12})
            self.assertEqual(runner.take()["signal"], "av_SoS")
            runner.put({"timestamp": 12, "audio": "b0", "mood": "next"})
            self.assertTrue(
                _wait_for(lambda: len(runner.step.submitted) == 2)
            )
            runner.step.replies.put({"value": "r-new"})
            runner.put({"signal": "tts_EoS", "timestamp": 12})
            chunk = runner.take()
            self.assertEqual(
                (chunk["value"], chunk["audio"], chunk["mood"]),
                ("r-new", "b0", "next"),
            )
            self.assertEqual(runner.take()["signal"], "av_EoS")
        finally:
            runner.close()


class ChunkGenerationCancelTest(unittest.TestCase):
    def test_cancel_after_generate_returns_cannot_publish_stale_chunk(self):
        config = _config("value", inputs=("audio", "mood"))
        config["pass_vars"].append({
            "source": "wire_prompt", "target": "prompt"
        })
        runner = _RunningStep(_BlockingStep, config)
        try:
            runner.put({
                "signal": "tts_SoS",
                "timestamp": 10,
                "pass_data": {"wire_prompt": "dance"},
            })
            self.assertEqual(runner.take()["signal"], "av_SoS")
            runner.put({
                "timestamp": 10,
                "audio": "a0",
                "mood": "happy",
            })
            self.assertTrue(runner.step.entered.wait(timeout=1))
            runner.cancel.put(json.dumps({
                "signal": "cancel",
                "timestamp": 11,
            }))
            runner.step.release.set()
            self.assertTrue(runner.step.aborted.wait(timeout=1))
            time.sleep(0.05)
            self.assertTrue(runner.output.empty())
            self.assertFalse(runner.step.span_active)
            self.assertEqual(runner.step.seen["prompt"], "dance")
            self.assertEqual(runner.step.seen["audio"], "a0")
            self.assertEqual(runner.step.seen["mood"], "happy")
        finally:
            runner.step.release.set()
            runner.close()

    def test_generation_error_still_passes_eos_and_recovers(self):
        runner = _RunningStep(
            _FailingStep, _config("value", inputs=("audio",))
        )
        try:
            runner.put({"signal": "tts_SoS", "timestamp": 1})
            self.assertEqual(runner.take()["signal"], "av_SoS")
            runner.put({"timestamp": 1, "audio": "bad"})
            runner.put({"signal": "tts_EoS", "timestamp": 1})
            self.assertEqual(runner.take()["signal"], "av_EoS")
            self.assertEqual(runner.step.abort_count, 1)
            self.assertFalse(runner.step.span_active)

            runner.put({"signal": "tts_SoS", "timestamp": 2})
            runner.put({"timestamp": 2, "audio": "good"})
            runner.put({"signal": "tts_EoS", "timestamp": 2})
            start, chunk, end = [runner.take() for _ in range(3)]
            self.assertEqual(start["signal"], "av_SoS")
            self.assertEqual(chunk["value"], 0)
            self.assertEqual(end["signal"], "av_EoS")
        finally:
            runner.close()

    def test_finish_error_still_passes_eos_and_aborts_session(self):
        runner = _RunningStep(
            _FailingStep, _config("value", inputs=("audio",))
        )
        runner.step.fail_generate = False
        runner.step.fail_finish = True
        try:
            runner.put({"signal": "tts_SoS", "timestamp": 1})
            self.assertEqual(runner.take()["signal"], "av_SoS")
            runner.put({"signal": "tts_EoS", "timestamp": 1})
            self.assertEqual(runner.take()["signal"], "av_EoS")
            time.sleep(0.1)
            self.assertEqual(runner.step.abort_count, 1)
            self.assertFalse(runner.step.span_active)
        finally:
            runner.close()


if __name__ == "__main__":
    unittest.main()
