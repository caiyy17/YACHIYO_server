"""Unit tests for TTS/video chunking followed by WebRTC frame splitting."""

import base64
import io
import json
import logging
import queue
import sys
import threading
import unittest
import warnings
import wave
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from Modules.tts_base.TTSStep import TTSStep  # noqa: E402
from Modules.video_base.VideoChunkGenerationStep import (  # noqa: E402
    VideoChunkGenerationStep,
)
from Modules.webrtc_frame_splitter.FrameSplitterStep import (  # noqa: E402
    FrameSplitterStep,
)


def _wire(source, target):
    return {"source": source, "target": target}


def _tts_config():
    return {
        "input_vars": [
            _wire("source_text", "text"),
            _wire(None, "language"),
            _wire(None, "speaker"),
            _wire(None, "duration"),
        ],
        "pass_vars": [_wire("source_item", "item")],
        "output_vars": [
            _wire("audio_file", "tts_audio"),
            _wire("duration", None),
        ],
        "catch_signals": [],
        "pass_signals": [],
        "emit_signals": [
            _wire("SoS", "span_begin"),
            _wire("EoS", "span_finish"),
        ],
        "next_nodes": [1],
        "stream": True,
        "stream_chunk_ms": 200,
        "exact_chunk": True,
    }


def _video_config():
    return {
        "input_vars": [_wire("tts_audio", "audio")],
        "pass_vars": [_wire("tts_audio", "paired_audio")],
        "output_vars": [_wire("video", "generated_video")],
        "catch_signals": [
            _wire("span_begin", "stream_start"),
            _wire("span_finish", "stream_end"),
        ],
        "pass_signals": [
            _wire("span_begin", "span_begin"),
            _wire("span_finish", "span_finish"),
        ],
        "emit_signals": [],
        "next_nodes": [2],
        "chunk_duration_ms": 200,
        "video_fps": 30,
        "video_width": 32,
        "video_height": 16,
        "color": [128, 0, 128],
    }


def _splitter_config():
    return {
        "input_vars": [
            _wire("paired_audio", "audio_data"),
            _wire("generated_video", "video_data"),
        ],
        "pass_vars": [],
        "output_vars": [
            _wire("audio", "tick_audio"),
            _wire("video", "tick_video"),
            _wire("data", "tick_data"),
        ],
        "catch_signals": [],
        "pass_signals": [_wire("forward_me", "forwarded")],
        "emit_signals": [],
        "catch_events": [_wire("start_clock", "connection_start")],
        "next_nodes": [-1],
        "audio_fps": 50,
        "video_fps": 30,
        "data_fps": 20,
        "video_width": 32,
        "video_height": 16,
    }

# Pydub's file-path loader in the repository's local placeholder TTS leaves
# its reader for GC; that pre-existing ResourceWarning is unrelated to this
# stream wiring contract.
warnings.filterwarnings(
    "ignore", category=ResourceWarning, message=r"unclosed file.*test_voice"
)


def _wav_duration(encoded):
    with wave.open(io.BytesIO(base64.b64decode(encoded)), "rb") as wav_file:
        return wav_file.getnframes() / wav_file.getframerate()


class _Chain:
    def __init__(self):
        logger = logging.getLogger("test.tts_video_splitter")
        self.input = queue.Queue()
        self.middle = queue.Queue()
        self.output = queue.Queue()
        self.tts_control = queue.Queue()
        self.video_control = queue.Queue()

        self.tts = TTSStep(
            0,
            "test",
            logger,
            queue.Queue(),
            self.input,
            self.middle,
            self.tts_control,
            _tts_config(),
        )
        self.video = VideoChunkGenerationStep(
            1,
            "test",
            logger,
            queue.Queue(),
            self.middle,
            self.output,
            self.video_control,
            _video_config(),
        )
        for step in (self.tts, self.video):
            if step.init_error:
                raise RuntimeError(step.init_error)

        self.threads = [
            threading.Thread(target=self.tts.run),
            threading.Thread(target=self.video.run),
        ]
        for thread in self.threads:
            thread.start()

    def close(self):
        command = json.dumps({"signal": "kill"})
        self.tts_control.put(command)
        self.video_control.put(command)
        for thread in self.threads:
            thread.join(timeout=3)
            if thread.is_alive():
                raise RuntimeError("pipeline test thread did not stop")


def _make_splitter():
    step = FrameSplitterStep(
        2,
        "test",
        logging.getLogger("test.tts_video_splitter.splitter"),
        queue.Queue(),
        queue.Queue(),
        queue.Queue(),
        queue.Queue(),
        _splitter_config(),
    )
    if step.init_error:
        raise RuntimeError(step.init_error)
    return step


class TTSVideoSplitterUnitTest(unittest.TestCase):
    def test_each_audio_chunk_gets_one_exact_video_chunk(self):
        warnings.simplefilter("ignore", ResourceWarning)
        chain = _Chain()
        try:
            chain.input.put(json.dumps({
                "destination": 0,
                "timestamp": 123.0,
                "source_text": "这是一段流式音视频模块协作测试。",
                "source_item": "unit-item",
            }))

            messages = []
            while True:
                message = json.loads(chain.output.get(timeout=5))
                messages.append(message)
                if message.get("signal") == "span_finish":
                    break

            self.assertEqual(messages[0]["signal"], "span_begin")
            self.assertEqual(messages[-1]["signal"], "span_finish")
            self.assertEqual(
                messages[0]["pass_data"]["item"], "unit-item"
            )

            chunks = [
                message for message in messages
                if "generated_video" in message
            ]
            self.assertGreater(len(chunks), 1)
            for chunk in chunks:
                self.assertIn("paired_audio", chunk)
                self.assertAlmostEqual(
                    _wav_duration(chunk["paired_audio"]), 0.2, places=6
                )
                self.assertEqual(len(chunk["generated_video"]), 6)
                self.assertEqual(
                    len({
                        frame["image"]
                        for frame in chunk["generated_video"]
                    }),
                    1,
                )

            self.assertEqual(
                chunks[0]["generated_video"][0]["header"],
                {"framerate": 30},
            )
            self.assertNotIn("header", chunks[1]["generated_video"][0])

            # The exact same combined packet splits into two WebRTC ticks:
            # 10 audio frames at 50fps and 6 video frames at 30fps.
            splitter = _make_splitter()
            splitter.input_queue.put(json.dumps(chunks[0]))
            splitter._fill_buffer()
            groups = [
                value
                for kind, value in splitter._group_buffer
                if kind == "group"
            ]
            self.assertEqual(len(groups), 2)
            self.assertTrue(
                all(len(group["tick_audio"]) == 5 for group in groups)
            )
            self.assertTrue(
                all(len(group["tick_video"]) == 3 for group in groups)
            )
            self.assertTrue(
                all(len(group["tick_data"]) == 2 for group in groups)
            )
        finally:
            chain.close()

    def test_splitter_relays_declared_and_drops_undeclared_signals(self):
        splitter = _make_splitter()

        splitter.input_queue.put(json.dumps({
            "destination": 2,
            "signal": "forward_me",
            "timestamp": 123.0,
        }))
        splitter._fill_buffer()

        kind, encoded = splitter._group_buffer.popleft()
        relayed = json.loads(encoded)
        self.assertEqual(kind, "signal")
        self.assertEqual(relayed["signal"], "forwarded")
        self.assertEqual(relayed["timestamp"], 123.0)
        self.assertEqual(relayed["destination"], -1)

        splitter.input_queue.put(json.dumps({
            "destination": 2,
            "signal": "drop_me",
            "timestamp": 123.0,
        }))
        splitter._fill_buffer()

        kind, group = splitter._group_buffer.popleft()
        self.assertEqual(kind, "group")
        self.assertAlmostEqual(splitter._group_period, 0.1)
        self.assertEqual(group["tick_audio"], splitter._silence_audio)
        self.assertEqual(
            group["tick_video"], ["idle"] * splitter.video_per_group
        )
        self.assertEqual(
            group["tick_data"], [None] * splitter.data_per_group
        )
        self.assertEqual(group["destination"], -1)


if __name__ == "__main__":
    unittest.main()
