"""Offline contract test for the stream-TTS -> chunk-video WebRTC config."""

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

from Modules import get_function_class_by_name  # noqa: E402
from Modules.tts_base.TTSStep import TTSStep  # noqa: E402
from Modules.video_base.VideoChunkGenerationStep import (  # noqa: E402
    VideoChunkGenerationStep,
)
from Modules.webrtc_frame_splitter.FrameSplitterStep import (  # noqa: E402
    FrameSplitterStep,
)
from utils.pipeline_validator import validate_pipeline  # noqa: E402


CONFIG_PATH = (
    PROJECT_ROOT / "configs" / "unity_chan_webrtc.json"
)

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
    def __init__(self, tts_config, video_config):
        logger = logging.getLogger("test.webrtc_stream_video")
        self.input = queue.Queue()
        self.middle = queue.Queue()
        self.output = queue.Queue()
        self.tts_control = queue.Queue()
        self.video_control = queue.Queue()

        self.tts = TTSStep(
            7,
            "test",
            logger,
            queue.Queue(),
            self.input,
            self.middle,
            self.tts_control,
            tts_config,
        )
        self.video = VideoChunkGenerationStep(
            8,
            "test",
            logger,
            queue.Queue(),
            self.middle,
            self.output,
            self.video_control,
            video_config,
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


class WebRTCStreamVideoConfigTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with CONFIG_PATH.open() as config_file:
            cls.pipeline_config = json.load(config_file)
        cls.nodes = {
            node["node_id"]: node for node in cls.pipeline_config["pipeline"]
        }

    def test_config_is_valid_and_chunk_clocks_match(self):
        errors, warnings = validate_pipeline(
            self.pipeline_config, get_function_class_by_name
        )
        self.assertEqual(errors, [])
        self.assertEqual(warnings, [])

        tts = self.nodes[7]["config"]
        video = self.nodes[8]["config"]
        splitter = self.nodes[9]["config"]
        webrtc = self.pipeline_config["webrtc"]

        self.assertTrue(tts["stream"])
        self.assertTrue(tts["exact_chunk"])
        self.assertEqual(tts["stream_chunk_ms"], 200)
        self.assertEqual(video["chunk_duration_ms"], 200)
        self.assertEqual(video["video_fps"], 30)
        self.assertEqual(splitter["video_fps"], webrtc["video_fps"])
        self.assertEqual(splitter["audio_fps"], webrtc["audio_fps"])
        self.assertEqual(splitter["data_fps"], webrtc["data_fps"])
        pass_map = {
            entry["source"]: entry["target"]
            for entry in splitter["pass_signals"]
        }
        self.assertEqual(pass_map["tts_SoS"], "meta")
        self.assertNotIn("tts_EoS", pass_map)

    def test_each_audio_chunk_gets_one_exact_video_chunk(self):
        warnings.simplefilter("ignore", ResourceWarning)
        chain = _Chain(
            self.nodes[7]["config"], self.nodes[8]["config"]
        )
        try:
            chain.input.put(json.dumps({
                "destination": 7,
                "timestamp": 123.0,
                "6_text": "这是一段流式音视频测试。",
                "6_action": "idle",
                "6_action_hint": "idle",
                "6_expression": "neutral",
                "6_expression_hint": "neutral",
                "response_id": "response-test",
                "item_id": "item-test",
            }))

            messages = []
            while True:
                message = json.loads(chain.output.get(timeout=5))
                messages.append(message)
                if message.get("signal") == "tts_EoS":
                    break

            self.assertEqual(messages[0]["signal"], "tts_SoS")
            self.assertEqual(messages[-1]["signal"], "tts_EoS")
            self.assertEqual(messages[0]["pass_data"]["text"],
                             "这是一段流式音视频测试。")

            chunks = [message for message in messages if "8_video" in message]
            self.assertGreater(len(chunks), 1)
            for chunk in chunks:
                self.assertIn("8_audio_data", chunk)
                self.assertAlmostEqual(
                    _wav_duration(chunk["8_audio_data"]), 0.2, places=6
                )
                self.assertEqual(len(chunk["8_video"]), 6)
                self.assertEqual(
                    len({frame["image"] for frame in chunk["8_video"]}), 1
                )

            self.assertEqual(
                chunks[0]["8_video"][0]["header"], {"framerate": 30}
            )
            self.assertNotIn("header", chunks[1]["8_video"][0])

            # The exact same combined packet splits into two WebRTC ticks:
            # 10 audio frames at 50fps and 6 video frames at 30fps.
            splitter = FrameSplitterStep(
                9,
                "test",
                logging.getLogger("test.webrtc_stream_video.splitter"),
                queue.Queue(),
                queue.Queue(),
                queue.Queue(),
                queue.Queue(),
                self.nodes[9]["config"],
            )
            self.assertIsNone(splitter.init_error)
            splitter._split_to_buffer({
                "audio_data": chunks[0]["8_audio_data"],
                "video_data": chunks[0]["8_video"],
            }, {})
            groups = [value for kind, value in splitter._group_buffer
                      if kind == "group"]
            self.assertEqual(len(groups), 2)
            self.assertTrue(all(len(group["audio"]) == 5 for group in groups))
            self.assertTrue(all(len(group["video"]) == 3 for group in groups))

            # The sentence start becomes the client's metadata signal. The
            # internal tts_EoS is deliberately not exposed by the splitter.
            splitter._group_buffer.clear()
            splitter.input_queue.put(json.dumps({
                "signal": "tts_SoS",
                "timestamp": 123.0,
                "pass_data": {"item_id": "item-test"},
            }))
            splitter._fill_buffer()
            kind, encoded = splitter._group_buffer.popleft()
            item_start = json.loads(encoded)
            self.assertEqual(kind, "signal")
            self.assertEqual(item_start["signal"], "meta")
            self.assertEqual(
                item_start["pass_data"]["item_id"], "item-test"
            )
        finally:
            chain.close()

    def test_dropped_tts_eos_does_not_leave_a_clock_tick_empty(self):
        splitter = FrameSplitterStep(
            9,
            "test",
            logging.getLogger("test.webrtc_stream_video.splitter_eos"),
            queue.Queue(),
            queue.Queue(),
            queue.Queue(),
            queue.Queue(),
            self.nodes[9]["config"],
        )
        self.assertIsNone(splitter.init_error)

        splitter.input_queue.put(json.dumps({
            "destination": 9,
            "signal": "tts_EoS",
            "timestamp": 123.0,
        }))
        splitter._fill_buffer()

        self.assertEqual(len(splitter._group_buffer), 1)
        kind, group = splitter._group_buffer.popleft()
        self.assertEqual(kind, "group")
        self.assertAlmostEqual(splitter._group_period, 0.1)
        self.assertEqual(group["audio"], splitter._silence_audio)
        self.assertEqual(
            group["video"], ["idle"] * splitter.video_per_group
        )
        self.assertEqual(group["data"], [None] * splitter.data_per_group)
        self.assertEqual(group["destination"], -1)


if __name__ == "__main__":
    unittest.main()
