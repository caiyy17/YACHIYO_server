"""Unit contract for a generic streaming TTS-to-motion module pair."""

import array
import base64
import importlib
import io
import json
import queue
import sys
import threading
import unittest
import wave
from pathlib import Path
from unittest.mock import mock_open, patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _wire(source, target):
    return {"source": source, "target": target}


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


def _wav_payload(encoded):
    with wave.open(io.BytesIO(base64.b64decode(encoded)), "rb") as wav_file:
        params = (
            wav_file.getnchannels(),
            wav_file.getsampwidth(),
            wav_file.getframerate(),
        )
        frames = wav_file.getnframes()
        pcm = wav_file.readframes(frames)
    return params, frames, pcm


class _Logger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


class _FakeTTSCaller:
    chunks = (
        _wav_bytes(sample_value=120),
        _wav_bytes(sample_value=-340),
    )
    instances = []

    def __init__(self, config, logger):
        self.calls = []
        self.__class__.instances.append(self)

    def call_stream(self, prompt, language="auto", speaker="", duration=None):
        self.calls.append((prompt, language, speaker, duration))
        for chunk in self.chunks:
            yield {"audio": chunk}


class _FakeMotionWebSocketSession:
    instances = []

    def __init__(self, url, frames_per_chunk, framerate, **kwargs):
        self.url = url
        self.frames_per_chunk = frames_per_chunk
        self.framerate = framerate
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
        return {
            "motion": [
                {"frame": index} for index in range(self.frames_per_chunk)
            ]
        }

    def finish(self):
        self.finish_count += 1

    def abort(self):
        self.abort_count += 1

    def close(self):
        self.close_count += 1


class TTSMotionStreamUnitTest(unittest.TestCase):
    def test_stream_boundaries_chunks_context_and_cleanup(self):
        tts_module = importlib.import_module(
            "Modules.tts_openai.OpenaiTTSStep"
        )
        motion_module = importlib.import_module(
            "Modules.motion_generation.MotionChunkGenerationStep"
        )

        tts_config = {
            "input_vars": [
                _wire("request_text", "text"),
                _wire(None, "language"),
                _wire(None, "speaker"),
                _wire(None, "duration"),
            ],
            "pass_vars": [
                _wire("requested_motion", "motion_hint"),
                _wire("request_id", "item_id"),
            ],
            "output_vars": [
                _wire("audio_file", "audio_chunk"),
                _wire("duration", None),
            ],
            "catch_signals": [],
            "pass_signals": [],
            "emit_signals": [
                _wire("SoS", "audio_start"),
                _wire("EoS", "audio_end"),
            ],
            "next_nodes": [2],
            "stream": True,
            "stream_chunk_ms": 200,
            "exact_chunk": True,
        }
        motion_config = {
            "input_vars": [_wire("audio_chunk", "audio_data")],
            "pass_vars": [_wire("audio_chunk", "audio_chunk")],
            "output_vars": [_wire("motion", "motion_chunk")],
            "catch_signals": [
                _wire("audio_start", "stream_start"),
                _wire("audio_end", "stream_end"),
            ],
            "pass_signals": [
                _wire("audio_start", "item_start"),
                _wire("audio_end", "item_end"),
            ],
            "emit_signals": [],
            "next_nodes": [-1],
            "model": "unit_motion",
            "ws_url": "ws://unit.invalid:18084",
            "framerate": 30,
            "chunk_duration_ms": 200,
            "init_test_text": "probe motion",
            "init_test_duration_ms": 1000,
            "humanoid_output": True,
        }

        ingress = queue.Queue()
        between = queue.Queue()
        output = queue.Queue()
        tts_cancel = queue.Queue()
        motion_cancel = queue.Queue()
        threads = []
        _FakeTTSCaller.instances.clear()
        _FakeMotionWebSocketSession.instances.clear()
        settings_file = mock_open(
            read_data=json.dumps({"unit_motion": {"extra": {}}})
        )

        with patch.object(
            tts_module, "OpenaiTTSCaller", _FakeTTSCaller
        ), patch.object(
            motion_module,
            "MotionWebSocketSession",
            _FakeMotionWebSocketSession,
        ), patch.object(
            motion_module, "open", settings_file, create=True
        ):
            tts = tts_module.OpenaiTTSStep(
                1,
                "unit",
                _Logger(),
                queue.Queue(),
                ingress,
                between,
                tts_cancel,
                tts_config,
            )
            motion = motion_module.MotionChunkGenerationStep(
                2,
                "unit",
                _Logger(),
                queue.Queue(),
                between,
                output,
                motion_cancel,
                motion_config,
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
                    "request_text": "hello",
                    "requested_motion": "step left",
                    "request_id": "request-1",
                }))

                start, first, second, end = [
                    json.loads(output.get(timeout=2)) for _ in range(4)
                ]
                self.assertEqual(start["signal"], "item_start")
                self.assertEqual(end["signal"], "item_end")
                self.assertEqual(start["pass_data"], {
                    "motion_hint": "step left",
                    "item_id": "request-1",
                })
                self.assertEqual(start["timestamp"], 10)
                self.assertEqual(end["timestamp"], 10)

                chunks = (first, second)
                expected_audio = [
                    _wav_payload(base64.b64encode(chunk).decode("utf-8"))
                    for chunk in _FakeTTSCaller.chunks
                ]
                actual_audio = [
                    _wav_payload(chunk["audio_chunk"]) for chunk in chunks
                ]
                self.assertEqual(actual_audio, expected_audio)
                for chunk, (_, frames, _) in zip(chunks, actual_audio):
                    self.assertEqual(frames, 4800)
                    self.assertEqual(len(chunk["motion_chunk"]), 6)

                # Only an absent/None hint bypasses Motion.
                ingress.put(json.dumps({
                    "timestamp": 11,
                    "request_text": "audio only",
                    "request_id": "request-11",
                }))
                item_start, audio_1, audio_2, item_end = [
                    json.loads(output.get(timeout=2)) for _ in range(4)
                ]
                self.assertEqual(item_start["signal"], "item_start")
                self.assertEqual(item_end["signal"], "item_end")
                self.assertEqual(item_start["timestamp"], 11)
                self.assertEqual(item_end["timestamp"], 11)
                passthrough_audio = [
                    _wav_payload(chunk["audio_chunk"])
                    for chunk in (audio_1, audio_2)
                ]
                self.assertEqual(passthrough_audio, expected_audio)
                self.assertNotIn("motion_chunk", audio_1)
                self.assertNotIn("motion_chunk", audio_2)

                # Empty and whitespace strings are present hints and are sent
                # to Motion unchanged.
                hinted_chunks = None
                for timestamp, requested_motion in ((12, ""), (13, "   ")):
                    ingress.put(json.dumps({
                        "timestamp": timestamp,
                        "request_text": "blank motion prompt",
                        "requested_motion": requested_motion,
                        "request_id": f"request-{timestamp}",
                    }))
                    item_start, hinted_1, hinted_2, item_end = [
                        json.loads(output.get(timeout=2)) for _ in range(4)
                    ]
                    self.assertEqual(item_start["signal"], "item_start")
                    self.assertEqual(item_end["signal"], "item_end")
                    self.assertEqual(len(hinted_1["motion_chunk"]), 6)
                    self.assertEqual(len(hinted_2["motion_chunk"]), 6)
                    hinted_chunks = (hinted_1, hinted_2)

                tts_caller = _FakeTTSCaller.instances[0]
                self.assertEqual(
                    tts_caller.calls,
                    [("hello", "auto", "", None),
                     ("audio only", "auto", "", None),
                     ("blank motion prompt", "auto", "", None),
                     ("blank motion prompt", "auto", "", None)],
                )

                session = _FakeMotionWebSocketSession.instances[0]
                self.assertEqual(session.frames_per_chunk, 6)
                self.assertEqual(session.framerate, 30)
                self.assertEqual(
                    [context["motion_hint"]
                     for context in session.reset_contexts],
                    ["probe motion", "step left", "", "   "],
                )
                self.assertEqual(
                    [index for _, index in session.generated],
                    [0, 1, 2, 3, 4, 0, 1, 0, 1, 0, 1],
                )
                self.assertEqual(
                    [inputs["audio_data"]
                     for inputs, _ in session.generated[-2:]],
                    [hinted_chunks[0]["audio_chunk"],
                     hinted_chunks[1]["audio_chunk"]],
                )
                self.assertEqual(
                    [inputs["motion_hint"]
                     for inputs, _ in session.generated[-2:]],
                    ["   ", "   "],
                )
                self.assertEqual(session.finish_count, 4)
                self.assertEqual(session.abort_count, 0)
                self.assertTrue(output.empty())
            finally:
                for cancel_queue in (tts_cancel, motion_cancel):
                    cancel_queue.put(json.dumps({
                        "signal": "kill",
                        "timestamp": 999,
                    }))
                for thread in threads:
                    thread.join(timeout=3)
                    self.assertFalse(thread.is_alive())

            session = _FakeMotionWebSocketSession.instances[0]
            self.assertEqual(session.close_count, 1)
            self.assertEqual(session.abort_count, 0)
            settings_file.assert_called_once()


if __name__ == "__main__":
    unittest.main()
