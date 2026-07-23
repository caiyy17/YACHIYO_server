import array
import base64
import importlib
import io
import json
import os
import queue
import sys
import threading
import unittest
import wave
from unittest.mock import patch


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from Modules import get_function_class_by_name  # noqa: E402
from utils.pipeline_validator import validate_pipeline  # noqa: E402


CONFIG_PATH = os.path.join(
    PROJECT_ROOT, "configs", "unity_chan_humanoid_stream.json"
)


class _Logger:
    threshold = 0

    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


def _wav_bytes(duration_ms=200, sample_rate=24000, sample_value=0):
    samples = array.array(
        "h", [sample_value] * (sample_rate * duration_ms // 1000)
    )
    output = io.BytesIO()
    with wave.open(output, "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(sample_rate)
        wav_file.writeframes(samples.tobytes())
    return output.getvalue()


def _wav_duration(encoded):
    with wave.open(io.BytesIO(base64.b64decode(encoded)), "rb") as wav_file:
        return wav_file.getnframes() / wav_file.getframerate()


def _wav_payload(wav_bytes):
    with wave.open(io.BytesIO(wav_bytes), "rb") as wav_file:
        params = (
            wav_file.getnchannels(),
            wav_file.getsampwidth(),
            wav_file.getframerate(),
        )
        frames = wav_file.getnframes()
        pcm = wav_file.readframes(frames)
    return params, frames, pcm


class _FakeTTSCaller:
    chunks = [
        _wav_bytes(sample_value=120),
        _wav_bytes(sample_value=-340),
    ]

    def __init__(self, config, logger):
        self.config = config

    def call(self, *args, **kwargs):
        return _wav_bytes()

    def call_stream(self, *args, **kwargs):
        for chunk in self.chunks:
            yield {"audio": chunk}


class _FakeMotionSession:
    instances = []

    def __init__(self, *args, **kwargs):
        self.reset_contexts = []
        self.generated = []
        self.finish_count = 0
        self.abort_count = 0
        self.close_count = 0
        self.__class__.instances.append(self)

    def reset(self, start_context):
        self.reset_contexts.append(dict(start_context))

    def generate_chunk(self, inputs, chunk_index):
        self.generated.append((dict(inputs), chunk_index))
        frames = [
            {
                "root_dxz": [0.0, 0.0],
                "root_dy": 0.0,
                "root_dyaw": 0.0,
                "hips_pos": [0.0, 0.0, 0.0],
                "joints": {},
            }
            for _ in range(6)
        ]
        if chunk_index == 0:
            frames[0] = {
                "header": {"framerate": 30, "format": "humanoid"},
                **frames[0],
            }
        return {"motion": frames}

    def finish(self):
        self.finish_count += 1

    def abort(self):
        self.abort_count += 1

    def close(self):
        self.close_count += 1


class HumanoidStreamConfigTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(CONFIG_PATH, encoding="utf-8") as config_file:
            cls.pipeline = json.load(config_file)

    def test_config_is_valid_and_wires_server_vad_and_stable_hint(self):
        errors, warnings = validate_pipeline(
            self.pipeline, get_function_class_by_name
        )
        self.assertEqual(errors, [])
        self.assertEqual(warnings, [])

        nodes = {
            node["function"]: node["config"]
            for node in self.pipeline["pipeline"]
        }
        pipeline_shape = [
            (node["node_id"], node["function"], node["config"]["next_nodes"])
            for node in self.pipeline["pipeline"]
        ]
        self.assertEqual(pipeline_shape, [
            (1, "call_server_vad", [2]),
            (2, "call_openai_asr", [3]),
            (3, "call_openai_llm", [4]),
            (4, "call_data_query_link", [5]),
            (5, "call_openai_tts", [6]),
            (6, "call_motion_chunk_generation", [-1]),
        ])

        self.assertEqual(nodes["call_server_vad"], {
            "input_vars": [
                {"source": "audio_data", "target": "audio_data"},
            ],
            "pass_vars": [],
            "output_vars": [
                {"source": "audio_file", "target": "1_audio_file"},
            ],
            "catch_signals": [
                {"source": "recording_end", "target": "recording_end"},
                {"source": "recording_start", "target": "recording_start"},
            ],
            "pass_signals": [],
            "emit_signals": [
                {"source": "vad_start", "target": "recording_start"},
                {"source": "vad_end", "target": "recording_end"},
            ],
            "next_nodes": [2],
            "start_offset_ms": -200,
            "end_offset_ms": 0,
            "manual_start_offset_ms": 0,
            "manual_end_offset_ms": 0,
            "auto_detect": False,
            "model": "silero_vad",
        })
        self.assertIn(
            {"source": "1_audio_file", "target": "audio_file"},
            nodes["call_openai_asr"]["input_vars"],
        )

        tts = nodes["call_openai_tts"]
        motion = nodes["call_motion_chunk_generation"]

        self.assertTrue(tts["stream"])
        self.assertTrue(tts["exact_chunk"])
        self.assertEqual(tts["stream_chunk_ms"], 200)
        self.assertIn(
            {"source": "4_action", "target": "action_hint"},
            tts["pass_vars"],
        )
        self.assertIn(
            {"source": "action_hint", "target": "motion_hint"},
            motion["pass_vars"],
        )
        self.assertIn(
            {"source": "5_audio_data", "target": "audio_data"},
            motion["input_vars"],
        )
        self.assertEqual(motion["chunk_duration_ms"], 200)
        self.assertEqual(motion["init_test_text"], "test")
        self.assertEqual(motion["init_test_duration_ms"], 1000)
        self.assertIn(
            {"source": "tts_SoS", "target": "item_SoS"},
            motion["pass_signals"],
        )
        self.assertIn(
            {"source": "tts_EoS", "target": "item_EoS"},
            motion["pass_signals"],
        )

    def test_each_tts_audio_chunk_is_collected_with_one_motion_chunk(self):
        tts_module = importlib.import_module(
            "Modules.tts_openai.OpenaiTTSStep"
        )
        motion_module = importlib.import_module(
            "Modules.motion_generation.MotionChunkGenerationStep"
        )
        configs = {
            node["node_id"]: dict(node["config"])
            for node in self.pipeline["pipeline"]
        }
        configs[6]["ws_url"] = "ws://fake.invalid:18084"

        ingress = queue.Queue()
        between = queue.Queue()
        output = queue.Queue()
        tts_cancel = queue.Queue()
        motion_cancel = queue.Queue()
        threads = []
        _FakeMotionSession.instances.clear()

        with patch.object(tts_module, "OpenaiTTSCaller", _FakeTTSCaller), \
                patch.object(
                    motion_module,
                    "MotionWebSocketSession",
                    _FakeMotionSession,
                ):
            tts = tts_module.OpenaiTTSStep(
                5,
                "tts",
                _Logger(),
                queue.Queue(),
                ingress,
                between,
                tts_cancel,
                configs[5],
            )
            motion = motion_module.MotionChunkGenerationStep(
                6,
                "motion",
                _Logger(),
                queue.Queue(),
                between,
                output,
                motion_cancel,
                configs[6],
            )
            self.assertIsNone(tts.init_error)
            self.assertIsNone(motion.init_error)

            try:
                for step in (tts, motion):
                    thread = threading.Thread(target=step.run)
                    thread.start()
                    threads.append(thread)

                ingress.put(json.dumps({
                    "timestamp": 10,
                    "4_text": "hello",
                    "4_action": "walk forward naturally",
                    "4_expression": "neutral",
                    "4_expression_hint": "calm",
                    "response_id": "response-1",
                    "item_id": "item-1",
                }))

                messages = [
                    json.loads(output.get(timeout=2)) for _ in range(4)
                ]
                start, first, second, end = messages
                self.assertEqual(start["signal"], "item_SoS")
                self.assertEqual(end["signal"], "item_EoS")
                self.assertEqual(
                    start["pass_data"]["item_id"], "item-1"
                )
                self.assertEqual(
                    start["pass_data"]["action_hint"],
                    "walk forward naturally",
                )

                for chunk in (first, second):
                    self.assertAlmostEqual(
                        _wav_duration(chunk["audio_data"]), 0.2
                    )
                    self.assertEqual(len(chunk["motion"]), 6)

                expected = [
                    _wav_payload(chunk) for chunk in _FakeTTSCaller.chunks
                ]
                collected = [
                    _wav_payload(base64.b64decode(chunk["audio_data"]))
                    for chunk in (first, second)
                ]
                self.assertEqual(
                    [params for params, _, _ in collected],
                    [params for params, _, _ in expected],
                )
                self.assertEqual(
                    sum(frames for _, frames, _ in collected),
                    sum(frames for _, frames, _ in expected),
                )
                self.assertEqual(
                    b"".join(pcm for _, _, pcm in collected),
                    b"".join(pcm for _, _, pcm in expected),
                )

                session = _FakeMotionSession.instances[0]
                self.assertEqual(len(session.reset_contexts), 2)
                self.assertEqual(
                    session.reset_contexts[0]["motion_hint"], "test"
                )
                self.assertEqual(
                    session.reset_contexts[1]["motion_hint"],
                    "walk forward naturally",
                )
                self.assertEqual(len(session.generated), 7)
                self.assertEqual(
                    [index for _, index in session.generated],
                    [0, 1, 2, 3, 4, 0, 1],
                )
                self.assertEqual(session.finish_count, 2)
                self.assertEqual(
                    [inputs["audio_data"]
                     for inputs, _ in session.generated[-2:]],
                    [first["audio_data"], second["audio_data"]],
                )
            finally:
                for cancel_queue in (tts_cancel, motion_cancel):
                    cancel_queue.put(json.dumps({
                        "signal": "kill", "timestamp": 999
                    }))
                for thread in threads:
                    thread.join(timeout=3)
                    self.assertFalse(thread.is_alive())


if __name__ == "__main__":
    unittest.main()
