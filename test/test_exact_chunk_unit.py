"""Unit tests for stream-only exact_chunk behavior."""

import base64
import io
import json
import os
import sys
import unittest
import wave
from unittest.mock import patch

import numpy as np
from pydub import AudioSegment

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Modules.motion_base.MotionStep import BaseMotionCaller, MotionStep  # noqa: E402
from Modules.motion_generation.MotionGenerationStep import (  # noqa: E402
    MotionGenerationCaller,
)
from Modules.parallel.JointStreamStep import JointStreamStep  # noqa: E402
from Modules.tts_base.TTSStep import BaseTTSCaller, TTSStep  # noqa: E402
from Modules.tts_openai.OpenaiTTSStep import OpenaiTTSCaller  # noqa: E402
from Modules.video_base.VideoStep import BaseVideoCaller, VideoStep  # noqa: E402


class _Logger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


def _wav(raw):
    with wave.open(io.BytesIO(raw), "rb") as wf:
        frames = wf.getnframes()
        return (
            frames,
            wf.getframerate(),
            wf.getnchannels(),
            wf.getsampwidth(),
            wf.readframes(frames),
        )


def _constant_segment(value, frames, rate=24000):
    sample = int(value).to_bytes(2, "little", signed=True)
    return AudioSegment(
        data=sample * frames,
        sample_width=2,
        frame_rate=rate,
        channels=1,
    )


class _MotionResponse:
    def __init__(self, lines=()):
        self.headers = {"X-Framerate": "30"}
        self.lines = list(lines)

    def raise_for_status(self):
        pass

    def iter_lines(self, decode_unicode=False):
        return iter(self.lines)

    def json(self):
        return {"error": "test response"}


class ExactChunkTest(unittest.TestCase):
    def test_openai_rechunk_default_pads_only_nonempty_tail(self):
        chunks = list(OpenaiTTSCaller._rechunk(
            iter((b"abc", b"defgh", b"ijklmno")), 10,
        ))
        self.assertEqual(chunks, [b"abcdefghij", b"klmno" + b"\x00" * 5])
        self.assertEqual(
            list(OpenaiTTSCaller._rechunk(iter((b"1234567890",)), 10)),
            [b"1234567890"],
        )
        self.assertEqual(list(OpenaiTTSCaller._rechunk(iter(()), 10)), [])

    def test_openai_rechunk_false_keeps_short_tail(self):
        chunks = list(OpenaiTTSCaller._rechunk(
            iter((b"abc", b"defgh", b"ijklmno")), 10,
            exact_chunk=False,
        ))
        self.assertEqual(chunks, [b"abcdefghij", b"klmno"])

    def test_base_tts_stream_padding_and_natural_tail(self):
        source = _constant_segment(7, 8400)  # 350 ms at 24 kHz

        exact = BaseTTSCaller({"stream_chunk_ms": 300}, _Logger())
        exact._build_segment = lambda *args, **kwargs: source
        exact_chunks = [item["audio"] for item in exact.call_stream("test")]
        self.assertEqual([_wav(item)[0] for item in exact_chunks], [7200, 7200])
        tail = _wav(exact_chunks[-1])
        self.assertEqual(tail[1:4], (24000, 1, 2))
        self.assertEqual(tail[4][:1200 * 2], b"\x07\x00" * 1200)
        self.assertEqual(tail[4][1200 * 2:], b"\x00" * (6000 * 2))

        natural = BaseTTSCaller(
            {"stream_chunk_ms": 300, "exact_chunk": False}, _Logger())
        natural._build_segment = lambda *args, **kwargs: source
        natural_chunks = [
            item["audio"] for item in natural.call_stream("test")
        ]
        self.assertEqual(
            [_wav(item)[0] for item in natural_chunks], [7200, 1200])

    def test_openai_empty_prompt_obeys_exact_chunk(self):
        exact = object.__new__(OpenaiTTSCaller)
        exact.config = {"stream_chunk_ms": 300}
        exact.empty_audio = AudioSegment.silent(duration=10, frame_rate=24000)
        exact_audio = list(exact.call_stream(""))[0]["audio"]
        self.assertEqual(_wav(exact_audio)[0], 7200)

        natural = object.__new__(OpenaiTTSCaller)
        natural.config = {"stream_chunk_ms": 300, "exact_chunk": False}
        natural.empty_audio = exact.empty_audio
        natural_audio = list(natural.call_stream(""))[0]["audio"]
        self.assertLess(_wav(natural_audio)[0], 7200)

    def test_base_motion_pads_only_stream(self):
        exact = BaseMotionCaller({
            "framerate": 30, "duration": 0.4, "stream_frames": 9,
        }, _Logger())
        self.assertEqual(
            [len(item["motion"]) for item in exact.call_stream("test")],
            [9, 9],
        )
        self.assertEqual(len(exact.call("test", 0.4)), 12)
        self.assertEqual(
            [len(item["motion"]) for item in exact.call_stream("test", 0.3)],
            [9],
        )

        natural = BaseMotionCaller({
            "framerate": 30, "duration": 0.4, "stream_frames": 9,
            "exact_chunk": False,
        }, _Logger())
        self.assertEqual(
            [len(item["motion"]) for item in natural.call_stream("test")],
            [9, 3],
        )
        self.assertEqual(len(natural.call("test", 0.4)), 12)

    def test_base_video_pads_only_stream(self):
        exact = BaseVideoCaller({
            "video_fps": 30, "duration": 0.4, "stream_frames": 9,
            "video_width": 2, "video_height": 2,
        }, _Logger())
        self.assertEqual(
            [len(item["video"]) for item in exact.call_stream("test")],
            [9, 9],
        )
        self.assertEqual(len(exact.call("test", 0.4)), 12)

        natural = BaseVideoCaller({
            "video_fps": 30, "duration": 0.4, "stream_frames": 9,
            "video_width": 2, "video_height": 2,
            "exact_chunk": False,
        }, _Logger())
        self.assertEqual(
            [len(item["video"]) for item in natural.call_stream("test")],
            [9, 3],
        )
        self.assertEqual(len(natural.call("test", 0.4)), 12)

    @staticmethod
    def _motion_caller(exact_chunk=True, stream_frames=9):
        caller = object.__new__(MotionGenerationCaller)
        caller.config = {
            "exact_chunk": exact_chunk,
            "stream_frames": stream_frames,
        }
        caller.logger = _Logger()
        caller.duration = 5.0
        caller.character = ""
        caller.model_name = "test"
        caller.extra = {}
        caller.continuous = False
        caller.humanoid_output = False
        caller.history_size = 5
        caller.addr_motion = "http://motion"
        caller.reset_history()
        return caller

    @staticmethod
    def _motion_lines(frames=3):
        poses = np.zeros((frames, 156), dtype=np.float32)
        trans = np.arange(frames * 3, dtype=np.float32).reshape(frames, 3)

        def encode(value):
            return base64.b64encode(value.tobytes()).decode("ascii")

        delta = {
            "type": "motion.delta",
            "poses": encode(poses),
            "poses_shape": list(poses.shape),
            "trans": encode(trans),
            "trans_shape": list(trans.shape),
        }
        return [
            "data: " + json.dumps(delta),
            "data: " + json.dumps({"type": "motion.done"}),
        ]

    def test_motion_http_keeps_duration_and_pads_output_tail(self):
        caller = self._motion_caller()
        caller.humanoid_output = True
        with patch(
                "Modules.motion_generation.MotionGenerationStep.requests.post",
                return_value=_MotionResponse(self._motion_lines())) as post:
            chunks = list(caller.call_stream("test"))
            self.assertEqual(post.call_args.kwargs["json"]["duration"], 5.0)
            self.assertEqual([len(chunk["motion"]) for chunk in chunks], [9])
            last_real = chunks[0]["motion"][2]
            self.assertNotEqual(last_real["root_dxz"], [0.0, 0.0])
            for filler in chunks[0]["motion"][3:]:
                self.assertEqual(filler, last_real)
                self.assertNotIn("header", filler)

        with patch(
                "Modules.motion_generation.MotionGenerationStep.requests.post",
                return_value=_MotionResponse()) as post:
            caller.call("test")
            self.assertEqual(post.call_args.kwargs["json"]["duration"], 5.0)

        natural = self._motion_caller(exact_chunk=False)
        with patch(
                "Modules.motion_generation.MotionGenerationStep.requests.post",
                return_value=_MotionResponse(self._motion_lines())):
            chunks = list(natural.call_stream("test"))
            self.assertEqual([len(chunk["motion"]) for chunk in chunks], [3])

        no_grid = self._motion_caller(stream_frames=0)
        with patch(
                "Modules.motion_generation.MotionGenerationStep.requests.post",
                return_value=_MotionResponse(self._motion_lines())):
            chunks = list(no_grid.call_stream("test"))
            self.assertEqual([len(chunk["motion"]) for chunk in chunks], [3])

    def test_exact_chunk_type_validation(self):
        for cls in (TTSStep, MotionStep, VideoStep):
            errors = cls.validate_config({"exact_chunk": "true"})
            self.assertTrue(any("exact_chunk" in error for error in errors))

        config = {
            "streams": [{
                "caller": "video_base",
                "input": [{"source": "prompt", "target": "prompt"}],
                "output": [{"source": "video", "target": "video"}],
                "config": {"exact_chunk": "true"},
            }],
        }
        errors = JointStreamStep.validate_config(config)
        self.assertTrue(any("exact_chunk" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
